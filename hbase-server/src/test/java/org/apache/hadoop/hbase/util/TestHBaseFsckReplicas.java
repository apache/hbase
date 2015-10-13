package org.apache.hadoop.hbase.util;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hbase.util.hbck.HbckTestingUtil.*;
import static org.junit.Assert.*;

@Category({MiscTests.class, LargeTests.class})
public class TestHBaseFsckReplicas extends BaseTestHBaseFsck {
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        MasterSyncObserver.class.getName());

    conf.setInt("hbase.regionserver.handler.count", 2);
    conf.setInt("hbase.regionserver.metahandler.count", 30);

    conf.setInt("hbase.htable.threads.max", POOL_SIZE);
    conf.setInt("hbase.hconnection.threads.max", 2 * POOL_SIZE);
    conf.setInt("hbase.hconnection.threads.core", POOL_SIZE);
    conf.setInt("hbase.hbck.close.timeout", 2 * REGION_ONLINE_TIMEOUT);
    conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 8 * REGION_ONLINE_TIMEOUT);
    TEST_UTIL.startMiniCluster(3);

    tableExecutorService = new ThreadPoolExecutor(1, POOL_SIZE, 60, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(), Threads.newDaemonThreadFactory("testhbck"));

    hbfsckExecutorService = new ScheduledThreadPoolExecutor(POOL_SIZE);

    AssignmentManager assignmentManager =
        TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager();
    regionStates = assignmentManager.getRegionStates();

    connection = (ClusterConnection) TEST_UTIL.getConnection();

    admin = connection.getAdmin();
    admin.setBalancerRunning(false, true);

    TEST_UTIL.waitUntilAllRegionsAssigned(TableName.META_TABLE_NAME);
    TEST_UTIL.waitUntilAllRegionsAssigned(TableName.NAMESPACE_TABLE_NAME);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    tableExecutorService.shutdown();
    hbfsckExecutorService.shutdown();
    admin.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() {
    EnvironmentEdgeManager.reset();
  }

  /*
 * This creates a table with region_replica > 1 and verifies hbck runs
 * successfully
 */
  @Test(timeout=180000)
  public void testHbckWithRegionReplica() throws Exception {
    TableName table =
        TableName.valueOf("testHbckWithRegionReplica");
    try {
      setupTableWithRegionReplica(table, 2);
      admin.flush(table);
      assertNoErrors(doFsck(conf, false));
    } finally {
      cleanupTable(table);
    }
  }

  @Test (timeout=180000)
  public void testHbckWithFewerReplica() throws Exception {
    TableName table =
        TableName.valueOf("testHbckWithFewerReplica");
    try {
      setupTableWithRegionReplica(table, 2);
      admin.flush(table);
      assertNoErrors(doFsck(conf, false));
      assertEquals(ROWKEYS.length, countRows());
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("B"), Bytes.toBytes("C"), true,
          false, false, false, 1); // unassign one replica
      // check that problem exists
      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] { HBaseFsck.ErrorReporter.ERROR_CODE.NOT_DEPLOYED });
      // fix the problem
      hbck = doFsck(conf, true);
      // run hbck again to make sure we don't see any errors
      hbck = doFsck(conf, false);
      assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] {});
    } finally {
      cleanupTable(table);
    }
  }

  @Test (timeout=180000)
  public void testHbckWithExcessReplica() throws Exception {
    TableName table =
        TableName.valueOf("testHbckWithExcessReplica");
    try {
      setupTableWithRegionReplica(table, 2);
      admin.flush(table);
      assertNoErrors(doFsck(conf, false));
      assertEquals(ROWKEYS.length, countRows());
      // the next few lines inject a location in meta for a replica, and then
      // asks the master to assign the replica (the meta needs to be injected
      // for the master to treat the request for assignment as valid; the master
      // checks the region is valid either from its memory or meta)
      Table meta = connection.getTable(TableName.META_TABLE_NAME, tableExecutorService);
      List<HRegionInfo> regions = admin.getTableRegions(table);
      byte[] startKey = Bytes.toBytes("B");
      byte[] endKey = Bytes.toBytes("C");
      byte[] metaKey = null;
      HRegionInfo newHri = null;
      for (HRegionInfo h : regions) {
        if (Bytes.compareTo(h.getStartKey(), startKey) == 0  &&
            Bytes.compareTo(h.getEndKey(), endKey) == 0 &&
            h.getReplicaId() == HRegionInfo.DEFAULT_REPLICA_ID) {
          metaKey = h.getRegionName();
          //create a hri with replicaId as 2 (since we already have replicas with replicaid 0 and 1)
          newHri = RegionReplicaUtil.getRegionInfoForReplica(h, 2);
          break;
        }
      }
      Put put = new Put(metaKey);
      Collection<ServerName> var = admin.getClusterStatus().getServers();
      ServerName sn = var.toArray(new ServerName[var.size()])[0];
      //add a location with replicaId as 2 (since we already have replicas with replicaid 0 and 1)
      MetaTableAccessor.addLocation(put, sn, sn.getStartcode(), -1, 2);
      meta.put(put);
      // assign the new replica
      HBaseFsckRepair.fixUnassigned(admin, newHri);
      HBaseFsckRepair.waitUntilAssigned(admin, newHri);
      // now reset the meta row to its original value
      Delete delete = new Delete(metaKey);
      delete.addColumns(HConstants.CATALOG_FAMILY, MetaTableAccessor.getServerColumn(2));
      delete.addColumns(HConstants.CATALOG_FAMILY, MetaTableAccessor.getStartCodeColumn(2));
      delete.addColumns(HConstants.CATALOG_FAMILY, MetaTableAccessor.getSeqNumColumn(2));
      meta.delete(delete);
      meta.close();
      // check that problem exists
      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[]{HBaseFsck.ErrorReporter.ERROR_CODE.NOT_IN_META});
      // fix the problem
      hbck = doFsck(conf, true);
      // run hbck again to make sure we don't see any errors
      hbck = doFsck(conf, false);
      assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[]{});
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * This creates and fixes a bad table with a region that is in meta but has
   * no deployment or data hdfs. The table has region_replication set to 2.
   */
  @Test (timeout=180000)
  public void testNotInHdfsWithReplicas() throws Exception {
    TableName table =
        TableName.valueOf("tableNotInHdfs");
    try {
      HRegionInfo[] oldHris = new HRegionInfo[2];
      setupTableWithRegionReplica(table, 2);
      assertEquals(ROWKEYS.length, countRows());
      NavigableMap<HRegionInfo, ServerName> map =
          MetaTableAccessor.allTableRegions(TEST_UTIL.getConnection(),
              tbl.getName());
      int i = 0;
      // store the HRIs of the regions we will mess up
      for (Map.Entry<HRegionInfo, ServerName> m : map.entrySet()) {
        if (m.getKey().getStartKey().length > 0 &&
            m.getKey().getStartKey()[0] == Bytes.toBytes("B")[0]) {
          LOG.debug("Initially server hosting " + m.getKey() + " is " + m.getValue());
          oldHris[i++] = m.getKey();
        }
      }
      // make sure data in regions
      admin.flush(table);

      // Mess it up by leaving a hole in the hdfs data
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("B"), Bytes.toBytes("C"), false,
          false, true); // don't rm meta

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] { HBaseFsck.ErrorReporter.ERROR_CODE.NOT_IN_HDFS });

      // fix hole
      doFsck(conf, true);

      // check that hole fixed
      assertNoErrors(doFsck(conf, false));
      assertEquals(ROWKEYS.length - 2, countRows());

      // the following code checks whether the old primary/secondary has
      // been unassigned and the new primary/secondary has been assigned
      i = 0;
      HRegionInfo[] newHris = new HRegionInfo[2];
      // get all table's regions from meta
      map = MetaTableAccessor.allTableRegions(TEST_UTIL.getConnection(), tbl.getName());
      // get the HRIs of the new regions (hbck created new regions for fixing the hdfs mess-up)
      for (Map.Entry<HRegionInfo, ServerName> m : map.entrySet()) {
        if (m.getKey().getStartKey().length > 0 &&
            m.getKey().getStartKey()[0] == Bytes.toBytes("B")[0]) {
          newHris[i++] = m.getKey();
        }
      }
      // get all the online regions in the regionservers
      Collection<ServerName> servers = admin.getClusterStatus().getServers();
      Set<HRegionInfo> onlineRegions = new HashSet<HRegionInfo>();
      for (ServerName s : servers) {
        List<HRegionInfo> list = admin.getOnlineRegions(s);
        onlineRegions.addAll(list);
      }
      // the new HRIs must be a subset of the online regions
      assertTrue(onlineRegions.containsAll(Arrays.asList(newHris)));
      // the old HRIs must not be part of the set (removeAll would return false if
      // the set didn't change)
      assertFalse(onlineRegions.removeAll(Arrays.asList(oldHris)));
    } finally {
      cleanupTable(table);
      admin.close();
    }
  }

}
