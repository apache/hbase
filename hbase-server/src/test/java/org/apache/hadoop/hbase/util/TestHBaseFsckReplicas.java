/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.util;

import static org.apache.hadoop.hbase.util.hbck.HbckTestingUtil.assertErrors;
import static org.apache.hadoop.hbase.util.hbck.HbckTestingUtil.assertNoErrors;
import static org.apache.hadoop.hbase.util.hbck.HbckTestingUtil.doFsck;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.ClusterStatus.Option;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Ignore
@Category({MiscTests.class, LargeTests.class})
public class TestHBaseFsckReplicas extends BaseTestHBaseFsck {
  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        MasterSyncCoprocessor.class.getName());

    conf.setInt("hbase.regionserver.handler.count", 2);
    conf.setInt("hbase.regionserver.metahandler.count", 30);

    conf.setInt("hbase.htable.threads.max", POOL_SIZE);
    conf.setInt("hbase.hconnection.threads.max", 2 * POOL_SIZE);
    conf.setInt("hbase.hbck.close.timeout", 2 * REGION_ONLINE_TIMEOUT);
    conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 8 * REGION_ONLINE_TIMEOUT);
    TEST_UTIL.startMiniCluster(3);

    tableExecutorService = new ThreadPoolExecutor(1, POOL_SIZE, 60, TimeUnit.SECONDS,
        new SynchronousQueue<>(), Threads.newDaemonThreadFactory("testhbck"));

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
 * This creates a table with region_replica > 1, do a split, check
 * that hbck will not report split replica parent as lingering split parent
 */
  @Test public void testHbckReportReplicaLingeringSplitParent() throws Exception {
    TableName table = TableName.valueOf("testHbckReportReplicaLingeringSplitParent");

    try {
      setupTableWithRegionReplica(table, 2);
      admin.flush(table);

      // disable catalog janitor
      admin.enableCatalogJanitor(false);
      admin.split(table, Bytes.toBytes("A1"));

      Thread.sleep(1000);
      // run hbck again to make sure we don't see any errors
      assertNoErrors(doFsck(conf, false));
    } finally {
      cleanupTable(table);
      // enable catalog janitor
      admin.enableCatalogJanitor(true);
    }
  }

  /*
 * This creates a table with region_replica > 1 and verifies hbck runs
 * successfully
 */
  @Test(timeout=180000)
  public void testHbckWithRegionReplica() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try {
      setupTableWithRegionReplica(tableName, 2);
      admin.flush(tableName);
      assertNoErrors(doFsck(conf, false));
    } finally {
      cleanupTable(tableName);
    }
  }

  @Test (timeout=180000)
  public void testHbckWithFewerReplica() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try {
      setupTableWithRegionReplica(tableName, 2);
      admin.flush(tableName);
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
      cleanupTable(tableName);
    }
  }

  @Test (timeout=180000)
  public void testHbckWithExcessReplica() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try {
      setupTableWithRegionReplica(tableName, 2);
      admin.flush(tableName);
      assertNoErrors(doFsck(conf, false));
      assertEquals(ROWKEYS.length, countRows());
      // the next few lines inject a location in meta for a replica, and then
      // asks the master to assign the replica (the meta needs to be injected
      // for the master to treat the request for assignment as valid; the master
      // checks the region is valid either from its memory or meta)
      Table meta = connection.getTable(TableName.META_TABLE_NAME, tableExecutorService);
      List<RegionInfo> regions = admin.getRegions(tableName);
      byte[] startKey = Bytes.toBytes("B");
      byte[] endKey = Bytes.toBytes("C");
      byte[] metaKey = null;
      RegionInfo newHri = null;
      for (RegionInfo h : regions) {
        if (Bytes.compareTo(h.getStartKey(), startKey) == 0  &&
            Bytes.compareTo(h.getEndKey(), endKey) == 0 &&
            h.getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID) {
          metaKey = h.getRegionName();
          //create a hri with replicaId as 2 (since we already have replicas with replicaid 0 and 1)
          newHri = RegionReplicaUtil.getRegionInfoForReplica(h, 2);
          break;
        }
      }
      Put put = new Put(metaKey);
      Collection<ServerName> var = admin.getClusterStatus(EnumSet.of(Option.LIVE_SERVERS)).getServers();
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
      cleanupTable(tableName);
    }
  }

  /**
   * This creates and fixes a bad table with a region that is in meta but has
   * no deployment or data hdfs. The table has region_replication set to 2.
   */
  @Test (timeout=180000)
  public void testNotInHdfsWithReplicas() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try {
      RegionInfo[] oldHris = new RegionInfo[2];
      setupTableWithRegionReplica(tableName, 2);
      assertEquals(ROWKEYS.length, countRows());
      NavigableMap<RegionInfo, ServerName> map =
          MetaTableAccessor.allTableRegions(TEST_UTIL.getConnection(),
              tbl.getName());
      int i = 0;
      // store the HRIs of the regions we will mess up
      for (Map.Entry<RegionInfo, ServerName> m : map.entrySet()) {
        if (m.getKey().getStartKey().length > 0 &&
            m.getKey().getStartKey()[0] == Bytes.toBytes("B")[0]) {
          LOG.debug("Initially server hosting " + m.getKey() + " is " + m.getValue());
          oldHris[i++] = m.getKey();
        }
      }
      // make sure data in regions
      admin.flush(tableName);

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
      RegionInfo[] newHris = new RegionInfo[2];
      // get all table's regions from meta
      map = MetaTableAccessor.allTableRegions(TEST_UTIL.getConnection(), tbl.getName());
      // get the HRIs of the new regions (hbck created new regions for fixing the hdfs mess-up)
      for (Map.Entry<RegionInfo, ServerName> m : map.entrySet()) {
        if (m.getKey().getStartKey().length > 0 &&
            m.getKey().getStartKey()[0] == Bytes.toBytes("B")[0]) {
          newHris[i++] = m.getKey();
        }
      }
      // get all the online regions in the regionservers
      Collection<ServerName> servers =
          admin.getClusterStatus(EnumSet.of(Option.LIVE_SERVERS)).getServers();
      Set<RegionInfo> onlineRegions = new HashSet<>();
      for (ServerName s : servers) {
        List<RegionInfo> list = admin.getRegions(s);
        onlineRegions.addAll(list);
      }
      // the new HRIs must be a subset of the online regions
      assertTrue(onlineRegions.containsAll(Arrays.asList(newHris)));
      // the old HRIs must not be part of the set (removeAll would return false if
      // the set didn't change)
      assertFalse(onlineRegions.removeAll(Arrays.asList(oldHris)));
    } finally {
      cleanupTable(tableName);
      admin.close();
    }
  }

  /**
   * Creates and fixes a bad table with a successful split that have a deployed
   * start and end keys and region replicas enabled
   */
  @Test (timeout=180000)
  public void testSplitAndDupeRegionWithRegionReplica() throws Exception {
    TableName table =
      TableName.valueOf("testSplitAndDupeRegionWithRegionReplica");
    Table meta = null;

    try {
      setupTableWithRegionReplica(table, 2);

      assertNoErrors(doFsck(conf, false));
      assertEquals(ROWKEYS.length, countRows());

      // No Catalog Janitor running
      admin.enableCatalogJanitor(false);
      meta = connection.getTable(TableName.META_TABLE_NAME, tableExecutorService);
      HRegionLocation loc = this.connection.getRegionLocation(table, SPLITS[0], false);
      RegionInfo hriParent = loc.getRegionInfo();

      // Split Region A just before B
      this.connection.getAdmin().split(table, Bytes.toBytes("A@"));
      Thread.sleep(1000);

      // We need to make sure the parent region is not in a split state, so we put it in CLOSED state.
      regionStates.updateRegionState(hriParent, RegionState.State.CLOSED);
      TEST_UTIL.assignRegion(hriParent);
      MetaTableAccessor.addRegionToMeta(meta, hriParent);
      ServerName server = regionStates.getRegionServerOfRegion(hriParent);

      if (server != null)
        TEST_UTIL.assertRegionOnServer(hriParent, server, REGION_ONLINE_TIMEOUT);

      while (findDeployedHSI(getDeployedHRIs((HBaseAdmin) admin), hriParent) == null) {
        Thread.sleep(250);
      }

      LOG.debug("Finished assignment of parent region");

      // TODO why is dupe region different from dupe start keys?
      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new HBaseFsck.ErrorReporter.ERROR_CODE[] { HBaseFsck.ErrorReporter.ERROR_CODE.NOT_DEPLOYED,
        HBaseFsck.ErrorReporter.ERROR_CODE.DUPE_STARTKEYS,
        HBaseFsck.ErrorReporter.ERROR_CODE.DUPE_STARTKEYS, HBaseFsck.ErrorReporter.ERROR_CODE.OVERLAP_IN_REGION_CHAIN});
      assertEquals(3, hbck.getOverlapGroups(table).size());

      // fix the degenerate region.
      hbck = new HBaseFsck(conf, hbfsckExecutorService);
      hbck.setDisplayFullReport(); // i.e. -details
      hbck.setTimeLag(0);
      hbck.setFixHdfsOverlaps(true);
      hbck.setRemoveParents(true);
      hbck.setFixReferenceFiles(true);
      hbck.setFixHFileLinks(true);
      hbck.connect();
      hbck.onlineHbck();
      hbck.close();

      hbck = doFsck(conf, false);

      assertNoErrors(hbck);
      assertEquals(0, hbck.getOverlapGroups(table).size());
      assertEquals(ROWKEYS.length, countRows());
    } finally {
      cleanupTable(table);
    }
  }
}
