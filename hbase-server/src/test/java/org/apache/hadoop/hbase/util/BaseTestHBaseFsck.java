/**
 *
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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.mob.MobFileName;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.SplitTransactionFactory;
import org.apache.hadoop.hbase.regionserver.SplitTransactionImpl;
import org.apache.hadoop.hbase.util.HBaseFsck.ErrorReporter;
import org.apache.hadoop.hbase.util.HBaseFsck.ErrorReporter.ERROR_CODE;
import org.apache.hadoop.hbase.util.HBaseFsck.HbckInfo;
import org.apache.hadoop.hbase.util.HBaseFsck.TableInfo;
import org.apache.hadoop.hbase.util.hbck.HFileCorruptionChecker;
import org.apache.hadoop.hbase.util.hbck.HbckTestingUtil;
import org.apache.zookeeper.KeeperException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TestName;


/**
 * This is the base class for  HBaseFsck's ability to detect reasons for inconsistent tables.
 *
 * Actual tests are in :
 * TestHBaseFsckTwoRS
 * TestHBaseFsckOneRS
 * TestHBaseFsckMOB
 * TestHBaseFsckReplicas
 */
public class BaseTestHBaseFsck {
  static final int POOL_SIZE = 7;
  protected static final Log LOG = LogFactory.getLog(BaseTestHBaseFsck.class);
  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  protected final static Configuration conf = TEST_UTIL.getConfiguration();
  protected final static String FAM_STR = "fam";
  protected final static byte[] FAM = Bytes.toBytes(FAM_STR);
  protected final static int REGION_ONLINE_TIMEOUT = 800;
  protected static RegionStates regionStates;
  protected static ExecutorService tableExecutorService;
  protected static ScheduledThreadPoolExecutor hbfsckExecutorService;
  protected static ClusterConnection connection;
  protected static Admin admin;

  // for the instance, reset every test run
  protected Table tbl;
  protected final static byte[][] SPLITS = new byte[][] { Bytes.toBytes("A"),
    Bytes.toBytes("B"), Bytes.toBytes("C") };
  // one row per region.
  protected final static byte[][] ROWKEYS= new byte[][] {
    Bytes.toBytes("00"), Bytes.toBytes("50"), Bytes.toBytes("A0"), Bytes.toBytes("A5"),
    Bytes.toBytes("B0"), Bytes.toBytes("B5"), Bytes.toBytes("C0"), Bytes.toBytes("C5") };


  /**
   * Create a new region in META.
   */
  protected HRegionInfo createRegion(final HTableDescriptor
      htd, byte[] startKey, byte[] endKey)
      throws IOException {
    Table meta = connection.getTable(TableName.META_TABLE_NAME, tableExecutorService);
    HRegionInfo hri = new HRegionInfo(htd.getTableName(), startKey, endKey);
    MetaTableAccessor.addRegionToMeta(meta, hri);
    meta.close();
    return hri;
  }

  /**
   * Debugging method to dump the contents of meta.
   */
  protected void dumpMeta(TableName tableName) throws IOException {
    List<byte[]> metaRows = TEST_UTIL.getMetaTableRows(tableName);
    for (byte[] row : metaRows) {
      LOG.info(Bytes.toString(row));
    }
  }

  /**
   * This method is used to undeploy a region -- close it and attempt to
   * remove its state from the Master.
   */
  protected void undeployRegion(Connection conn, ServerName sn,
      HRegionInfo hri) throws IOException, InterruptedException {
    try {
      HBaseFsckRepair.closeRegionSilentlyAndWait((HConnection) conn, sn, hri);
      if (!hri.isMetaTable()) {
        admin.offline(hri.getRegionName());
      }
    } catch (IOException ioe) {
      LOG.warn("Got exception when attempting to offline region "
          + Bytes.toString(hri.getRegionName()), ioe);
    }
  }
  /**
   * Delete a region from assignments, meta, or completely from hdfs.
   * @param unassign if true unassign region if assigned
   * @param metaRow  if true remove region's row from META
   * @param hdfs if true remove region's dir in HDFS
   */
  protected void deleteRegion(Configuration conf, final HTableDescriptor htd,
      byte[] startKey, byte[] endKey, boolean unassign, boolean metaRow,
      boolean hdfs) throws IOException, InterruptedException {
    deleteRegion(conf, htd, startKey, endKey, unassign, metaRow, hdfs, false,
        HRegionInfo.DEFAULT_REPLICA_ID);
  }

  /**
   * Delete a region from assignments, meta, or completely from hdfs.
   * @param unassign if true unassign region if assigned
   * @param metaRow  if true remove region's row from META
   * @param hdfs if true remove region's dir in HDFS
   * @param regionInfoOnly if true remove a region dir's .regioninfo file
   * @param replicaId replica id
   */
  protected void deleteRegion(Configuration conf, final HTableDescriptor htd,
      byte[] startKey, byte[] endKey, boolean unassign, boolean metaRow,
      boolean hdfs, boolean regionInfoOnly, int replicaId)
          throws IOException, InterruptedException {
    LOG.info("** Before delete:");
    dumpMeta(htd.getTableName());

    List<HRegionLocation> locations;
    try(RegionLocator rl = connection.getRegionLocator(tbl.getName())) {
      locations = rl.getAllRegionLocations();
    }

    for (HRegionLocation location : locations) {
      HRegionInfo hri = location.getRegionInfo();
      ServerName hsa = location.getServerName();
      if (Bytes.compareTo(hri.getStartKey(), startKey) == 0
          && Bytes.compareTo(hri.getEndKey(), endKey) == 0
          && hri.getReplicaId() == replicaId) {

        LOG.info("RegionName: " +hri.getRegionNameAsString());
        byte[] deleteRow = hri.getRegionName();

        if (unassign) {
          LOG.info("Undeploying region " + hri + " from server " + hsa);
          undeployRegion(connection, hsa, hri);
        }

        if (regionInfoOnly) {
          LOG.info("deleting hdfs .regioninfo data: " + hri.toString() + hsa.toString());
          Path rootDir = FSUtils.getRootDir(conf);
          FileSystem fs = rootDir.getFileSystem(conf);
          Path p = new Path(FSUtils.getTableDir(rootDir, htd.getTableName()),
              hri.getEncodedName());
          Path hriPath = new Path(p, HRegionFileSystem.REGION_INFO_FILE);
          fs.delete(hriPath, true);
        }

        if (hdfs) {
          LOG.info("deleting hdfs data: " + hri.toString() + hsa.toString());
          Path rootDir = FSUtils.getRootDir(conf);
          FileSystem fs = rootDir.getFileSystem(conf);
          Path p = new Path(FSUtils.getTableDir(rootDir, htd.getTableName()),
              hri.getEncodedName());
          HBaseFsck.debugLsr(conf, p);
          boolean success = fs.delete(p, true);
          LOG.info("Deleted " + p + " sucessfully? " + success);
          HBaseFsck.debugLsr(conf, p);
        }

        if (metaRow) {
          try (Table meta = connection.getTable(TableName.META_TABLE_NAME, tableExecutorService)) {
            Delete delete = new Delete(deleteRow);
            meta.delete(delete);
          }
        }
      }
      LOG.info(hri.toString() + hsa.toString());
    }

    TEST_UTIL.getMetaTableRows(htd.getTableName());
    LOG.info("*** After delete:");
    dumpMeta(htd.getTableName());
  }

  /**
   * Setup a clean table before we start mucking with it.
   *
   * It will set tbl which needs to be closed after test
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   */
  void setupTable(TableName tablename) throws Exception {
    setupTableWithRegionReplica(tablename, 1);
  }

  /**
   * Setup a clean table with a certain region_replica count
   *
   * It will set tbl which needs to be closed after test
   *
   * @param tableName
   * @param replicaCount
   * @throws Exception
   */
  void setupTableWithRegionReplica(TableName tablename, int replicaCount) throws Exception {
    HTableDescriptor desc = new HTableDescriptor(tablename);
    desc.setRegionReplication(replicaCount);
    HColumnDescriptor hcd = new HColumnDescriptor(Bytes.toString(FAM));
    desc.addFamily(hcd); // If a table has no CF's it doesn't get checked
    createTable(TEST_UTIL, desc, SPLITS);

    tbl = connection.getTable(tablename, tableExecutorService);
    List<Put> puts = new ArrayList<Put>();
    for (byte[] row : ROWKEYS) {
      Put p = new Put(row);
      p.addColumn(FAM, Bytes.toBytes("val"), row);
      puts.add(p);
    }
    tbl.put(puts);
  }

  /**
   * Setup a clean table with a mob-enabled column.
   *
   * @param tableName The name of a table to be created.
   * @throws Exception
   */
  void setupMobTable(TableName tablename) throws Exception {
    HTableDescriptor desc = new HTableDescriptor(tablename);
    HColumnDescriptor hcd = new HColumnDescriptor(Bytes.toString(FAM));
    hcd.setMobEnabled(true);
    hcd.setMobThreshold(0);
    desc.addFamily(hcd); // If a table has no CF's it doesn't get checked
    createTable(TEST_UTIL, desc, SPLITS);

    tbl = connection.getTable(tablename, tableExecutorService);
    List<Put> puts = new ArrayList<Put>();
    for (byte[] row : ROWKEYS) {
      Put p = new Put(row);
      p.add(FAM, Bytes.toBytes("val"), row);
      puts.add(p);
    }
    tbl.put(puts);
  }

  /**
   * Counts the number of row to verify data loss or non-dataloss.
   */
  int countRows() throws IOException {
     Scan s = new Scan();
     ResultScanner rs = tbl.getScanner(s);
     int i = 0;
     while(rs.next() !=null) {
       i++;
     }
     return i;
  }

  /**
   * delete table in preparation for next test
   *
   * @param tablename
   * @throws IOException
   */
  void cleanupTable(TableName tablename) throws Exception {
    if (tbl != null) {
      tbl.close();
      tbl = null;
    }

    ((ClusterConnection) connection).clearRegionCache();
    deleteTable(TEST_UTIL, tablename);
  }

  /**
   * Get region info from local cluster.
   */
  Map<ServerName, List<String>> getDeployedHRIs(final HBaseAdmin admin) throws IOException {
    ClusterStatus status = admin.getClusterStatus();
    Collection<ServerName> regionServers = status.getServers();
    Map<ServerName, List<String>> mm =
        new HashMap<ServerName, List<String>>();
    for (ServerName hsi : regionServers) {
      AdminProtos.AdminService.BlockingInterface server = ((HConnection) connection).getAdmin(hsi);

      // list all online regions from this region server
      List<HRegionInfo> regions = ProtobufUtil.getOnlineRegions(server);
      List<String> regionNames = new ArrayList<String>();
      for (HRegionInfo hri : regions) {
        regionNames.add(hri.getRegionNameAsString());
      }
      mm.put(hsi, regionNames);
    }
    return mm;
  }

  /**
   * Returns the HSI a region info is on.
   */
  ServerName findDeployedHSI(Map<ServerName, List<String>> mm, HRegionInfo hri) {
    for (Map.Entry<ServerName,List <String>> e : mm.entrySet()) {
      if (e.getValue().contains(hri.getRegionNameAsString())) {
        return e.getKey();
      }
    }
    return null;
  }




  /**
   * This creates and fixes a bad table with a missing region -- hole in meta
   * and data present but .regioinfino missing (an orphan hdfs region)in the fs.
   */
  @Test (timeout=180000)
  public void testHDFSRegioninfoMissing() throws Exception {
    TableName table = TableName.valueOf("tableHDFSRegioninfoMissing");
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by leaving a hole in the meta data
      admin.disableTable(table);
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("B"), Bytes.toBytes("C"), true,
          true, false, true, HRegionInfo.DEFAULT_REPLICA_ID);
      admin.enableTable(table);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck,
          new ERROR_CODE[] { ERROR_CODE.ORPHAN_HDFS_REGION, ERROR_CODE.NOT_IN_META_OR_DEPLOYED,
              ERROR_CODE.HOLE_IN_REGION_CHAIN });
      // holes are separate from overlap groups
      assertEquals(0, hbck.getOverlapGroups(table).size());

      // fix hole
      doFsck(conf, true);

      // check that hole fixed
      assertNoErrors(doFsck(conf, false));
      assertEquals(ROWKEYS.length, countRows());
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * This creates and fixes a bad table with a region that is missing meta and
   * not assigned to a region server.
   */
  @Test (timeout=180000)
  public void testNotInMetaOrDeployedHole() throws Exception {
    TableName table =
        TableName.valueOf("tableNotInMetaOrDeployedHole");
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by leaving a hole in the meta data
      admin.disableTable(table);
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("B"), Bytes.toBytes("C"), true,
          true, false); // don't rm from fs
      admin.enableTable(table);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck,
          new ERROR_CODE[] { ERROR_CODE.NOT_IN_META_OR_DEPLOYED, ERROR_CODE.HOLE_IN_REGION_CHAIN });
      // holes are separate from overlap groups
      assertEquals(0, hbck.getOverlapGroups(table).size());

      // fix hole
      assertErrors(doFsck(conf, true),
          new ERROR_CODE[] { ERROR_CODE.NOT_IN_META_OR_DEPLOYED, ERROR_CODE.HOLE_IN_REGION_CHAIN });

      // check that hole fixed
      assertNoErrors(doFsck(conf, false));
      assertEquals(ROWKEYS.length, countRows());
    } finally {
      cleanupTable(table);
    }
  }

  @Test (timeout=180000)
  public void testCleanUpDaughtersNotInMetaAfterFailedSplit() throws Exception {
    TableName table = TableName.valueOf("testCleanUpDaughtersNotInMetaAfterFailedSplit");
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    try {
      HTableDescriptor desc = new HTableDescriptor(table);
      desc.addFamily(new HColumnDescriptor(Bytes.toBytes("f")));
      createTable(TEST_UTIL, desc, null);

      tbl = connection.getTable(desc.getTableName());
      for (int i = 0; i < 5; i++) {
        Put p1 = new Put(("r" + i).getBytes());
        p1.add(Bytes.toBytes("f"), "q1".getBytes(), "v".getBytes());
        tbl.put(p1);
      }
      admin.flush(desc.getTableName());
      List<HRegion> regions = cluster.getRegions(desc.getTableName());
      int serverWith = cluster.getServerWith(regions.get(0).getRegionInfo().getRegionName());
      HRegionServer regionServer = cluster.getRegionServer(serverWith);
      cluster.getServerWith(regions.get(0).getRegionInfo().getRegionName());
      SplitTransactionImpl st = (SplitTransactionImpl)
        new SplitTransactionFactory(TEST_UTIL.getConfiguration())
          .create(regions.get(0), Bytes.toBytes("r3"));
      st.prepare();
      st.stepsBeforePONR(regionServer, regionServer, false);
      AssignmentManager am = cluster.getMaster().getAssignmentManager();
      Map<String, RegionState> regionsInTransition = am.getRegionStates().getRegionsInTransition();
      for (RegionState state : regionsInTransition.values()) {
        am.regionOffline(state.getRegion());
      }
      Map<HRegionInfo, ServerName> regionsMap = new HashMap<HRegionInfo, ServerName>();
      regionsMap.put(regions.get(0).getRegionInfo(), regionServer.getServerName());
      am.assign(regionsMap);
      am.waitForAssignment(regions.get(0).getRegionInfo());
      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] { ERROR_CODE.NOT_IN_META_OR_DEPLOYED,
          ERROR_CODE.NOT_IN_META_OR_DEPLOYED });
      // holes are separate from overlap groups
      assertEquals(0, hbck.getOverlapGroups(table).size());

      // fix hole
      assertErrors(
          doFsck(conf, false, true, false, false, false, false, false, false, false, false, false,
            null),
          new ERROR_CODE[] { ERROR_CODE.NOT_IN_META_OR_DEPLOYED,
              ERROR_CODE.NOT_IN_META_OR_DEPLOYED });

      // check that hole fixed
      assertNoErrors(doFsck(conf, false));
      assertEquals(5, countRows());
    } finally {
      if (tbl != null) {
        tbl.close();
        tbl = null;
      }
      cleanupTable(table);
    }
  }

  /**
   * This creates fixes a bad table with a hole in meta.
   */
  @Test (timeout=180000)
  public void testNotInMetaHole() throws Exception {
    TableName table =
        TableName.valueOf("tableNotInMetaHole");
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // Mess it up by leaving a hole in the meta data
      admin.disableTable(table);
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("B"), Bytes.toBytes("C"), false,
          true, false); // don't rm from fs
      admin.enableTable(table);

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck,
          new ERROR_CODE[] { ERROR_CODE.NOT_IN_META_OR_DEPLOYED, ERROR_CODE.HOLE_IN_REGION_CHAIN });
      // holes are separate from overlap groups
      assertEquals(0, hbck.getOverlapGroups(table).size());

      // fix hole
      assertErrors(doFsck(conf, true),
          new ERROR_CODE[] { ERROR_CODE.NOT_IN_META_OR_DEPLOYED, ERROR_CODE.HOLE_IN_REGION_CHAIN });

      // check that hole fixed
      assertNoErrors(doFsck(conf, false));
      assertEquals(ROWKEYS.length, countRows());
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * This creates and fixes a bad table with a region that is in meta but has
   * no deployment or data hdfs
   */
  @Test (timeout=180000)
  public void testNotInHdfs() throws Exception {
    TableName table =
        TableName.valueOf("tableNotInHdfs");
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());

      // make sure data in regions, if in wal only there is no data loss
      admin.flush(table);

      // Mess it up by leaving a hole in the hdfs data
      deleteRegion(conf, tbl.getTableDescriptor(), Bytes.toBytes("B"), Bytes.toBytes("C"), false,
          false, true); // don't rm meta

      HBaseFsck hbck = doFsck(conf, false);
      assertErrors(hbck, new ERROR_CODE[] {ERROR_CODE.NOT_IN_HDFS});
      // holes are separate from overlap groups
      assertEquals(0, hbck.getOverlapGroups(table).size());

      // fix hole
      doFsck(conf, true);

      // check that hole fixed
      assertNoErrors(doFsck(conf,false));
      assertEquals(ROWKEYS.length - 2, countRows());
    } finally {
      cleanupTable(table);
    }
  }


  public void deleteTableDir(TableName table) throws IOException {
    Path rootDir = FSUtils.getRootDir(conf);
    FileSystem fs = rootDir.getFileSystem(conf);
    Path p = FSUtils.getTableDir(rootDir, table);
    HBaseFsck.debugLsr(conf, p);
    boolean success = fs.delete(p, true);
    LOG.info("Deleted " + p + " sucessfully? " + success);
  }





  /**
   * We don't have an easy way to verify that a flush completed, so we loop until we find a
   * legitimate hfile and return it.
   * @param fs
   * @param table
   * @return Path of a flushed hfile.
   * @throws IOException
   */
  Path getFlushedHFile(FileSystem fs, TableName table) throws IOException {
    Path tableDir= FSUtils.getTableDir(FSUtils.getRootDir(conf), table);
    Path regionDir = FSUtils.getRegionDirs(fs, tableDir).get(0);
    Path famDir = new Path(regionDir, FAM_STR);

    // keep doing this until we get a legit hfile
    while (true) {
      FileStatus[] hfFss = fs.listStatus(famDir);
      if (hfFss.length == 0) {
        continue;
      }
      for (FileStatus hfs : hfFss) {
        if (!hfs.isDirectory()) {
          return hfs.getPath();
        }
      }
    }
  }

  /**
   * Gets flushed mob files.
   * @param fs The current file system.
   * @param table The current table name.
   * @return Path of a flushed hfile.
   * @throws IOException
   */
  Path getFlushedMobFile(FileSystem fs, TableName table) throws IOException {
    Path famDir = MobUtils.getMobFamilyPath(conf, table, FAM_STR);

    // keep doing this until we get a legit hfile
    while (true) {
      FileStatus[] hfFss = fs.listStatus(famDir);
      if (hfFss.length == 0) {
        continue;
      }
      for (FileStatus hfs : hfFss) {
        if (!hfs.isDirectory()) {
          return hfs.getPath();
        }
      }
    }
  }

  /**
   * Creates a new mob file name by the old one.
   * @param oldFileName The old mob file name.
   * @return The new mob file name.
   */
  String createMobFileName(String oldFileName) {
    MobFileName mobFileName = MobFileName.create(oldFileName);
    String startKey = mobFileName.getStartKey();
    String date = mobFileName.getDate();
    return MobFileName.create(startKey, date, UUID.randomUUID().toString().replaceAll("-", ""))
      .getFileName();
  }




  /**
   * Test that use this should have a timeout, because this method could potentially wait forever.
  */
  protected void doQuarantineTest(TableName table, HBaseFsck hbck, int check,
                                  int corrupt, int fail, int quar, int missing) throws Exception {
    try {
      setupTable(table);
      assertEquals(ROWKEYS.length, countRows());
      admin.flush(table); // flush is async.

      // Mess it up by leaving a hole in the assignment, meta, and hdfs data
      admin.disableTable(table);

      String[] args = {"-sidelineCorruptHFiles", "-repairHoles", "-ignorePreCheckPermission",
          table.getNameAsString()};
      HBaseFsck res = hbck.exec(hbfsckExecutorService, args);

      HFileCorruptionChecker hfcc = res.getHFilecorruptionChecker();
      assertEquals(hfcc.getHFilesChecked(), check);
      assertEquals(hfcc.getCorrupted().size(), corrupt);
      assertEquals(hfcc.getFailures().size(), fail);
      assertEquals(hfcc.getQuarantined().size(), quar);
      assertEquals(hfcc.getMissing().size(), missing);

      // its been fixed, verify that we can enable
      admin.enableTableAsync(table);
      while (!admin.isTableEnabled(table)) {
        try {
          Thread.sleep(250);
        } catch (InterruptedException e) {
          e.printStackTrace();
          fail("Interrupted when trying to enable table " + table);
        }
      }
    } finally {
      cleanupTable(table);
    }
  }

  /**
   * This creates a table and simulates the race situation where a concurrent compaction or split
   * has removed an colfam dir before the corruption checker got to it.
   */
  // Disabled because fails sporadically.  Is this test right?  Timing-wise, there could be no
  // files in a column family on initial creation -- as suggested by Matteo.
  @Ignore @Test(timeout=180000)
  public void testQuarantineMissingFamdir() throws Exception {
    TableName table = TableName.valueOf(name.getMethodName());
    // inject a fault in the hfcc created.
    final FileSystem fs = FileSystem.get(conf);
    HBaseFsck hbck = new HBaseFsck(conf, hbfsckExecutorService) {
      @Override
      public HFileCorruptionChecker createHFileCorruptionChecker(boolean sidelineCorruptHFiles)
          throws IOException {
        return new HFileCorruptionChecker(conf, executor, sidelineCorruptHFiles) {
          AtomicBoolean attemptedFirstHFile = new AtomicBoolean(false);
          @Override
          protected void checkColFamDir(Path p) throws IOException {
            if (attemptedFirstHFile.compareAndSet(false, true)) {
              assertTrue(fs.delete(p, true)); // make sure delete happened.
            }
            super.checkColFamDir(p);
          }
        };
      }
    };
    doQuarantineTest(table, hbck, 3, 0, 0, 0, 1);
    hbck.close();
  }

  /**
   * This creates a table and simulates the race situation where a concurrent compaction or split
   * has removed a region dir before the corruption checker got to it.
   */
  @Test(timeout=180000)
  public void testQuarantineMissingRegionDir() throws Exception {
    TableName table = TableName.valueOf(name.getMethodName());
    // inject a fault in the hfcc created.
    final FileSystem fs = FileSystem.get(conf);
    HBaseFsck hbck = new HBaseFsck(conf, hbfsckExecutorService) {
      @Override
      public HFileCorruptionChecker createHFileCorruptionChecker(boolean sidelineCorruptHFiles)
      throws IOException {
        return new HFileCorruptionChecker(conf, executor, sidelineCorruptHFiles) {
          AtomicBoolean attemptedFirstHFile = new AtomicBoolean(false);
          @Override
          protected void checkRegionDir(Path p) throws IOException {
            if (attemptedFirstHFile.compareAndSet(false, true)) {
              assertTrue(fs.delete(p, true)); // make sure delete happened.
            }
            super.checkRegionDir(p);
          }
        };
      }
    };
    doQuarantineTest(table, hbck, 3, 0, 0, 0, 1);
    hbck.close();
  }





  static class MockErrorReporter implements ErrorReporter {
    static int calledCount = 0;

    @Override
    public void clear() {
      calledCount++;
    }

    @Override
    public void report(String message) {
      calledCount++;
    }

    @Override
    public void reportError(String message) {
      calledCount++;
    }

    @Override
    public void reportError(ERROR_CODE errorCode, String message) {
      calledCount++;
    }

    @Override
    public void reportError(ERROR_CODE errorCode, String message, TableInfo table) {
      calledCount++;
    }

    @Override
    public void reportError(ERROR_CODE errorCode,
        String message, TableInfo table, HbckInfo info) {
      calledCount++;
    }

    @Override
    public void reportError(ERROR_CODE errorCode, String message,
        TableInfo table, HbckInfo info1, HbckInfo info2) {
      calledCount++;
    }

    @Override
    public int summarize() {
      return ++calledCount;
    }

    @Override
    public void detail(String details) {
      calledCount++;
    }

    @Override
    public ArrayList<ERROR_CODE> getErrorList() {
      calledCount++;
      return new ArrayList<ERROR_CODE>();
    }

    @Override
    public void progress() {
      calledCount++;
    }

    @Override
    public void print(String message) {
      calledCount++;
    }

    @Override
    public void resetErrors() {
      calledCount++;
    }

    @Override
    public boolean tableHasErrors(TableInfo table) {
      calledCount++;
      return false;
    }
  }


  protected void deleteMetaRegion(Configuration conf, boolean unassign, boolean hdfs,
                                  boolean regionInfoOnly) throws IOException, InterruptedException {
    HRegionLocation metaLocation = connection.getRegionLocator(TableName.META_TABLE_NAME)
        .getRegionLocation(HConstants.EMPTY_START_ROW);
    ServerName hsa = metaLocation.getServerName();
    HRegionInfo hri = metaLocation.getRegionInfo();
    if (unassign) {
      LOG.info("Undeploying meta region " + hri + " from server " + hsa);
      try (Connection unmanagedConnection = ConnectionFactory.createConnection(conf)) {
        undeployRegion(unmanagedConnection, hsa, hri);
      }
    }

    if (regionInfoOnly) {
      LOG.info("deleting hdfs .regioninfo data: " + hri.toString() + hsa.toString());
      Path rootDir = FSUtils.getRootDir(conf);
      FileSystem fs = rootDir.getFileSystem(conf);
      Path p = new Path(rootDir + "/" + TableName.META_TABLE_NAME.getNameAsString(),
          hri.getEncodedName());
      Path hriPath = new Path(p, HRegionFileSystem.REGION_INFO_FILE);
      fs.delete(hriPath, true);
    }

    if (hdfs) {
      LOG.info("deleting hdfs data: " + hri.toString() + hsa.toString());
      Path rootDir = FSUtils.getRootDir(conf);
      FileSystem fs = rootDir.getFileSystem(conf);
      Path p = new Path(rootDir + "/" + TableName.META_TABLE_NAME.getNameAsString(),
          hri.getEncodedName());
      HBaseFsck.debugLsr(conf, p);
      boolean success = fs.delete(p, true);
      LOG.info("Deleted " + p + " sucessfully? " + success);
      HBaseFsck.debugLsr(conf, p);
    }
  }







  @org.junit.Rule
  public TestName name = new TestName();



  public static class MasterSyncObserver extends BaseMasterObserver {
    volatile CountDownLatch tableCreationLatch = null;
    volatile CountDownLatch tableDeletionLatch = null;

    @Override
    public void postCreateTableHandler(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
      // the AccessController test, some times calls only and directly the postCreateTableHandler()
      if (tableCreationLatch != null) {
        tableCreationLatch.countDown();
      }
    }

    @Override
    public void postDeleteTableHandler(final ObserverContext<MasterCoprocessorEnvironment> ctx,
                                       TableName tableName)
    throws IOException {
      // the AccessController test, some times calls only and directly the postDeleteTableHandler()
      if (tableDeletionLatch != null) {
        tableDeletionLatch.countDown();
      }
    }
  }

  public static void createTable(HBaseTestingUtility testUtil, HTableDescriptor htd,
    byte [][] splitKeys) throws Exception {
    // NOTE: We need a latch because admin is not sync,
    // so the postOp coprocessor method may be called after the admin operation returned.
    MasterSyncObserver observer = (MasterSyncObserver)testUtil.getHBaseCluster().getMaster()
      .getMasterCoprocessorHost().findCoprocessor(MasterSyncObserver.class.getName());
    observer.tableCreationLatch = new CountDownLatch(1);
    if (splitKeys != null) {
      admin.createTable(htd, splitKeys);
    } else {
      admin.createTable(htd);
    }
    observer.tableCreationLatch.await();
    observer.tableCreationLatch = null;
    testUtil.waitUntilAllRegionsAssigned(htd.getTableName());
  }

  public static void deleteTable(HBaseTestingUtility testUtil, TableName tableName)
    throws Exception {
    // NOTE: We need a latch because admin is not sync,
    // so the postOp coprocessor method may be called after the admin operation returned.
    MasterSyncObserver observer = (MasterSyncObserver)testUtil.getHBaseCluster().getMaster()
      .getMasterCoprocessorHost().findCoprocessor(MasterSyncObserver.class.getName());
    observer.tableDeletionLatch = new CountDownLatch(1);
    try {
      admin.disableTable(tableName);
    } catch (Exception e) {
      LOG.debug("Table: " + tableName + " already disabled, so just deleting it.");
    }
    admin.deleteTable(tableName);
    observer.tableDeletionLatch.await();
    observer.tableDeletionLatch = null;
  }
}
