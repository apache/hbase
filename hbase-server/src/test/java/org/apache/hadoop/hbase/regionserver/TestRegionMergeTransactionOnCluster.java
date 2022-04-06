/*
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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CatalogFamilyFormat;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.DoNotRetryRegionException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.exceptions.MergeRegionException;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterRpcServices;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.base.Joiner;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionResponse;

@Category({RegionServerTests.class, LargeTests.class})
public class TestRegionMergeTransactionOnCluster {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionMergeTransactionOnCluster.class);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRegionMergeTransactionOnCluster.class);

  @Rule public TestName name = new TestName();

  private static final int NB_SERVERS = 3;

  private static final byte[] FAMILYNAME = Bytes.toBytes("fam");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");

  private static byte[] ROW = Bytes.toBytes("testRow");
  private static final int INITIAL_REGION_NUM = 10;
  private static final int ROWSIZE = 200;
  private static byte[][] ROWS = makeN(ROW, ROWSIZE);

  private static int waitTime = 60 * 1000;

  static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private static HMaster MASTER;
  private static Admin ADMIN;

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    // Start a cluster
    StartTestingClusterOption option = StartTestingClusterOption.builder()
        .masterClass(MyMaster.class).numRegionServers(NB_SERVERS).numDataNodes(NB_SERVERS).build();
    TEST_UTIL.startMiniCluster(option);
    SingleProcessHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    MASTER = cluster.getMaster();
    MASTER.balanceSwitch(false);
    ADMIN = TEST_UTIL.getConnection().getAdmin();
  }

  @AfterClass
  public static void afterAllTests() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    if (ADMIN != null) {
      ADMIN.close();
    }
  }

  @Test
  public void testWholesomeMerge() throws Exception {
    LOG.info("Starting " + name.getMethodName());
    final TableName tableName = TableName.valueOf(name.getMethodName());

    try {
      // Create table and load data.
      Table table = createTableAndLoadData(MASTER, tableName);
      // Merge 1st and 2nd region
      mergeRegionsAndVerifyRegionNum(MASTER, tableName, 0, 1, INITIAL_REGION_NUM - 1);

      // Merge 2nd and 3th region
      PairOfSameType<RegionInfo> mergedRegions =
        mergeRegionsAndVerifyRegionNum(MASTER, tableName, 1, 2, INITIAL_REGION_NUM - 2);

      verifyRowCount(table, ROWSIZE);

      // Randomly choose one of the two merged regions
      RegionInfo hri = ThreadLocalRandom.current().nextBoolean() ? mergedRegions.getFirst() :
        mergedRegions.getSecond();
      SingleProcessHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
      AssignmentManager am = cluster.getMaster().getAssignmentManager();
      RegionStates regionStates = am.getRegionStates();

      // We should not be able to assign it again
      am.assign(hri);
      assertFalse("Merged region can't be assigned", regionStates.isRegionInTransition(hri));

      // We should not be able to unassign it either
      am.unassign(hri);
      assertFalse("Merged region can't be unassigned", regionStates.isRegionInTransition(hri));

      table.close();
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }

  /**
   * Not really restarting the master. Simulate it by clear of new region
   * state since it is not persisted, will be lost after master restarts.
   */
  @Test
  public void testMergeAndRestartingMaster() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());

    try {
      // Create table and load data.
      Table table = createTableAndLoadData(MASTER, tableName);

      try {
        MyMasterRpcServices.enabled.set(true);

        // Merge 1st and 2nd region
        mergeRegionsAndVerifyRegionNum(MASTER, tableName, 0, 1, INITIAL_REGION_NUM - 1);
      } finally {
        MyMasterRpcServices.enabled.set(false);
      }

      table.close();
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }

  @Test
  public void testCleanMergeReference() throws Exception {
    LOG.info("Starting " + name.getMethodName());
    ADMIN.catalogJanitorSwitch(false);
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try {
      // Create table and load data.
      Table table = createTableAndLoadData(MASTER, tableName);
      // Merge 1st and 2nd region
      mergeRegionsAndVerifyRegionNum(MASTER, tableName, 0, 1, INITIAL_REGION_NUM - 1);
      verifyRowCount(table, ROWSIZE);
      table.close();

      List<Pair<RegionInfo, ServerName>> tableRegions = MetaTableAccessor
          .getTableRegionsAndLocations(MASTER.getConnection(), tableName);
      RegionInfo mergedRegionInfo = tableRegions.get(0).getFirst();
      TableDescriptor tableDescriptor = MASTER.getTableDescriptors().get(
          tableName);
      Result mergedRegionResult = MetaTableAccessor.getRegionResult(
        MASTER.getConnection(), mergedRegionInfo.getRegionName());

      // contains merge reference in META
      assertTrue(CatalogFamilyFormat.hasMergeRegions(mergedRegionResult.rawCells()));

      // merging regions' directory are in the file system all the same
      List<RegionInfo> p = CatalogFamilyFormat.getMergeRegions(mergedRegionResult.rawCells());
      RegionInfo regionA = p.get(0);
      RegionInfo regionB = p.get(1);
      FileSystem fs = MASTER.getMasterFileSystem().getFileSystem();
      Path rootDir = MASTER.getMasterFileSystem().getRootDir();

      Path tabledir = CommonFSUtils.getTableDir(rootDir, mergedRegionInfo.getTable());
      Path regionAdir = new Path(tabledir, regionA.getEncodedName());
      Path regionBdir = new Path(tabledir, regionB.getEncodedName());
      assertTrue(fs.exists(regionAdir));
      assertTrue(fs.exists(regionBdir));

      ColumnFamilyDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
      HRegionFileSystem hrfs = new HRegionFileSystem(
        TEST_UTIL.getConfiguration(), fs, tabledir, mergedRegionInfo);
      int count = 0;
      for(ColumnFamilyDescriptor colFamily : columnFamilies) {
        count += hrfs.getStoreFiles(colFamily.getNameAsString()).size();
      }
      ADMIN.compactRegion(mergedRegionInfo.getRegionName());
      // clean up the merged region store files
      // wait until merged region have reference file
      long timeout = EnvironmentEdgeManager.currentTime() + waitTime;
      int newcount = 0;
      while (EnvironmentEdgeManager.currentTime() < timeout) {
        for(ColumnFamilyDescriptor colFamily : columnFamilies) {
          newcount += hrfs.getStoreFiles(colFamily.getNameAsString()).size();
        }
        if(newcount > count) {
          break;
        }
        Thread.sleep(50);
      }
      assertTrue(newcount > count);
      List<RegionServerThread> regionServerThreads = TEST_UTIL.getHBaseCluster()
          .getRegionServerThreads();
      for (RegionServerThread rs : regionServerThreads) {
        CompactedHFilesDischarger cleaner = new CompactedHFilesDischarger(100, null,
            rs.getRegionServer(), false);
        cleaner.chore();
        Thread.sleep(1000);
      }
      while (EnvironmentEdgeManager.currentTime() < timeout) {
        int newcount1 = 0;
        for(ColumnFamilyDescriptor colFamily : columnFamilies) {
          newcount1 += hrfs.getStoreFiles(colFamily.getNameAsString()).size();
        }
        if(newcount1 <= 1) {
          break;
        }
        Thread.sleep(50);
      }
      // run CatalogJanitor to clean merge references in hbase:meta and archive the
      // files of merging regions
      int cleaned = 0;
      while (cleaned == 0) {
        cleaned = ADMIN.runCatalogJanitor();
        LOG.debug("catalog janitor returned " + cleaned);
        Thread.sleep(50);
        // Cleanup is async so wait till all procedures are done running.
        ProcedureTestingUtility.waitNoProcedureRunning(
            TEST_UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor());
      }
      // We used to check for existence of region in fs but sometimes the region dir was
      // cleaned up by the time we got here making the test sometimes flakey.
      assertTrue(cleaned > 0);

      // Wait around a bit to give stuff a chance to complete.
      while (true) {
        mergedRegionResult = MetaTableAccessor
          .getRegionResult(TEST_UTIL.getConnection(), mergedRegionInfo.getRegionName());
        if (CatalogFamilyFormat.hasMergeRegions(mergedRegionResult.rawCells())) {
          LOG.info("Waiting on cleanup of merge columns {}",
            Arrays.asList(mergedRegionResult.rawCells()).stream().
              map(c -> c.toString()).collect(Collectors.joining(",")));
          Threads.sleep(50);
        } else {
          break;
        }
      }
      assertFalse(CatalogFamilyFormat.hasMergeRegions(mergedRegionResult.rawCells()));
    } finally {
      ADMIN.catalogJanitorSwitch(true);
      TEST_UTIL.deleteTable(tableName);
    }
  }

  /**
   * This test tests 1, merging region not online;
   * 2, merging same two regions; 3, merging unknown regions.
   * They are in one test case so that we don't have to create
   * many tables, and these tests are simple.
   */
  @Test
  public void testMerge() throws Exception {
    LOG.info("Starting " + name.getMethodName());
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final Admin admin = TEST_UTIL.getAdmin();

    try {
      // Create table and load data.
      Table table = createTableAndLoadData(MASTER, tableName);
      AssignmentManager am = MASTER.getAssignmentManager();
      List<RegionInfo> regions = am.getRegionStates().getRegionsOfTable(tableName);
      // Fake offline one region
      RegionInfo a = regions.get(0);
      RegionInfo b = regions.get(1);
      am.unassign(b);
      am.offlineRegion(b);
      try {
        // Merge offline region. Region a is offline here
        FutureUtils.get(
          admin.mergeRegionsAsync(a.getEncodedNameAsBytes(), b.getEncodedNameAsBytes(), false));
        fail("Offline regions should not be able to merge");
      } catch (DoNotRetryRegionException ie) {
        System.out.println(ie);
        assertTrue(ie instanceof MergeRegionException);
      }

      try {
        // Merge the same region: b and b.
        FutureUtils
          .get(admin.mergeRegionsAsync(b.getEncodedNameAsBytes(), b.getEncodedNameAsBytes(), true));
        fail("A region should not be able to merge with itself, even forcfully");
      } catch (IOException ie) {
        assertTrue("Exception should mention regions not online",
          StringUtils.stringifyException(ie).contains("region to itself") &&
            ie instanceof MergeRegionException);
      }

      try {
        // Merge unknown regions
        FutureUtils.get(admin.mergeRegionsAsync(Bytes.toBytes("-f1"), Bytes.toBytes("-f2"), true));
        fail("Unknown region could not be merged");
      } catch (IOException ie) {
        assertTrue("UnknownRegionException should be thrown", ie instanceof UnknownRegionException);
      }
      table.close();
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }

  @Test
  public void testMergeWithReplicas() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try {
      // Create table and load data.
      Table table = createTableAndLoadData(MASTER, tableName, 5, 2);
      List<Pair<RegionInfo, ServerName>> initialRegionToServers =
        MetaTableAccessor.getTableRegionsAndLocations(TEST_UTIL.getConnection(), tableName);
      // Merge 1st and 2nd region
      PairOfSameType<RegionInfo> mergedRegions =
        mergeRegionsAndVerifyRegionNum(MASTER, tableName, 0, 2, 5 * 2 - 2);
      List<Pair<RegionInfo, ServerName>> currentRegionToServers =
        MetaTableAccessor.getTableRegionsAndLocations(TEST_UTIL.getConnection(), tableName);
      List<RegionInfo> initialRegions = new ArrayList<>();
      for (Pair<RegionInfo, ServerName> p : initialRegionToServers) {
        initialRegions.add(p.getFirst());
      }
      List<RegionInfo> currentRegions = new ArrayList<>();
      for (Pair<RegionInfo, ServerName> p : currentRegionToServers) {
        currentRegions.add(p.getFirst());
      }
      // this is the first region
      assertTrue(initialRegions.contains(mergedRegions.getFirst()));
      // this is the replica of the first region
      assertTrue(initialRegions
        .contains(RegionReplicaUtil.getRegionInfoForReplica(mergedRegions.getFirst(), 1)));
      // this is the second region
      assertTrue(initialRegions.contains(mergedRegions.getSecond()));
      // this is the replica of the second region
      assertTrue(initialRegions
        .contains(RegionReplicaUtil.getRegionInfoForReplica(mergedRegions.getSecond(), 1)));
      // this is the new region
      assertTrue(!initialRegions.contains(currentRegions.get(0)));
      // replica of the new region
      assertTrue(!initialRegions
        .contains(RegionReplicaUtil.getRegionInfoForReplica(currentRegions.get(0), 1)));
      // replica of the new region
      assertTrue(currentRegions
        .contains(RegionReplicaUtil.getRegionInfoForReplica(currentRegions.get(0), 1)));
      // replica of the merged region
      assertTrue(!currentRegions
        .contains(RegionReplicaUtil.getRegionInfoForReplica(mergedRegions.getFirst(), 1)));
      // replica of the merged region
      assertTrue(!currentRegions
        .contains(RegionReplicaUtil.getRegionInfoForReplica(mergedRegions.getSecond(), 1)));
      table.close();
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }

  private PairOfSameType<RegionInfo> mergeRegionsAndVerifyRegionNum(
      HMaster master, TableName tablename,
      int regionAnum, int regionBnum, int expectedRegionNum) throws Exception {
    PairOfSameType<RegionInfo> mergedRegions =
      requestMergeRegion(master, tablename, regionAnum, regionBnum);
    waitAndVerifyRegionNum(master, tablename, expectedRegionNum);
    return mergedRegions;
  }

  private PairOfSameType<RegionInfo> requestMergeRegion(
      HMaster master, TableName tablename,
      int regionAnum, int regionBnum) throws Exception {
    List<Pair<RegionInfo, ServerName>> tableRegions = MetaTableAccessor
        .getTableRegionsAndLocations(
            TEST_UTIL.getConnection(), tablename);
    RegionInfo regionA = tableRegions.get(regionAnum).getFirst();
    RegionInfo regionB = tableRegions.get(regionBnum).getFirst();
    ADMIN.mergeRegionsAsync(
      regionA.getEncodedNameAsBytes(),
      regionB.getEncodedNameAsBytes(), false);
    return new PairOfSameType<>(regionA, regionB);
  }

  private void waitAndVerifyRegionNum(HMaster master, TableName tablename,
      int expectedRegionNum) throws Exception {
    List<Pair<RegionInfo, ServerName>> tableRegionsInMeta;
    List<RegionInfo> tableRegionsInMaster;
    long timeout = EnvironmentEdgeManager.currentTime() + waitTime;
    while (EnvironmentEdgeManager.currentTime() < timeout) {
      tableRegionsInMeta =
          MetaTableAccessor.getTableRegionsAndLocations(TEST_UTIL.getConnection(), tablename);
      tableRegionsInMaster =
          master.getAssignmentManager().getRegionStates().getRegionsOfTable(tablename);
      LOG.info(Objects.toString(tableRegionsInMaster));
      LOG.info(Objects.toString(tableRegionsInMeta));
      int tableRegionsInMetaSize = tableRegionsInMeta.size();
      int tableRegionsInMasterSize = tableRegionsInMaster.size();
      if (tableRegionsInMetaSize == expectedRegionNum
          && tableRegionsInMasterSize == expectedRegionNum) {
        break;
      }
      Thread.sleep(250);
    }

    tableRegionsInMeta = MetaTableAccessor.getTableRegionsAndLocations(
        TEST_UTIL.getConnection(), tablename);
    LOG.info("Regions after merge:" + Joiner.on(',').join(tableRegionsInMeta));
    assertEquals(expectedRegionNum, tableRegionsInMeta.size());
  }

  private Table createTableAndLoadData(HMaster master, TableName tablename)
      throws Exception {
    return createTableAndLoadData(master, tablename, INITIAL_REGION_NUM, 1);
  }

  private Table createTableAndLoadData(HMaster master, TableName tablename,
      int numRegions, int replication) throws Exception {
    assertTrue("ROWSIZE must > numregions:" + numRegions, ROWSIZE > numRegions);
    byte[][] splitRows = new byte[numRegions - 1][];
    for (int i = 0; i < splitRows.length; i++) {
      splitRows[i] = ROWS[(i + 1) * ROWSIZE / numRegions];
    }

    Table table = TEST_UTIL.createTable(tablename, FAMILYNAME, splitRows);
    LOG.info("Created " + table.getName());
    if (replication > 1) {
      HBaseTestingUtil.setReplicas(ADMIN, tablename, replication);
      LOG.info("Set replication of " + replication + " on " + table.getName());
    }
    loadData(table);
    LOG.info("Loaded " + table.getName());
    verifyRowCount(table, ROWSIZE);
    LOG.info("Verified " + table.getName());

    List<Pair<RegionInfo, ServerName>> tableRegions;
    TEST_UTIL.waitUntilAllRegionsAssigned(tablename);
    LOG.info("All regions assigned for table - " + table.getName());
    tableRegions = MetaTableAccessor.getTableRegionsAndLocations(
        TEST_UTIL.getConnection(), tablename);
    assertEquals("Wrong number of regions in table " + tablename,
        numRegions * replication, tableRegions.size());
    LOG.info(tableRegions.size() + "Regions after load: " + Joiner.on(',').join(tableRegions));
    assertEquals(numRegions * replication, tableRegions.size());
    return table;
  }

  private static byte[][] makeN(byte[] base, int n) {
    byte[][] ret = new byte[n][];
    for (int i = 0; i < n; i++) {
      ret[i] = Bytes.add(base, Bytes.toBytes(String.format("%04d", i)));
    }
    return ret;
  }

  private void loadData(Table table) throws IOException {
    for (int i = 0; i < ROWSIZE; i++) {
      Put put = new Put(ROWS[i]);
      put.addColumn(FAMILYNAME, QUALIFIER, Bytes.toBytes(i));
      table.put(put);
    }
  }

  private void verifyRowCount(Table table, int expectedRegionNum)
      throws IOException {
    ResultScanner scanner = table.getScanner(new Scan());
    int rowCount = 0;
    while (scanner.next() != null) {
      rowCount++;
    }
    assertEquals(expectedRegionNum, rowCount);
    scanner.close();
  }

  // Make it public so that JVMClusterUtil can access it.
  public static class MyMaster extends HMaster {
    public MyMaster(Configuration conf) throws IOException, KeeperException, InterruptedException {
      super(conf);
    }

    @Override
    protected MasterRpcServices createRpcServices() throws IOException {
      return new MyMasterRpcServices(this);
    }
  }

  static class MyMasterRpcServices extends MasterRpcServices {
    static AtomicBoolean enabled = new AtomicBoolean(false);

    private HMaster myMaster;
    public MyMasterRpcServices(HMaster master) throws IOException {
      super(master);
      myMaster = master;
    }

    @Override
    public ReportRegionStateTransitionResponse reportRegionStateTransition(RpcController c,
        ReportRegionStateTransitionRequest req) throws ServiceException {
      ReportRegionStateTransitionResponse resp = super.reportRegionStateTransition(c, req);
      if (enabled.get() && req.getTransition(0).getTransitionCode()
          == TransitionCode.READY_TO_MERGE && !resp.hasErrorMessage()) {
        RegionStates regionStates = myMaster.getAssignmentManager().getRegionStates();
        for (RegionState regionState: regionStates.getRegionsStateInTransition()) {
          // Find the merging_new region and remove it
          if (regionState.isMergingNew()) {
            regionStates.deleteRegion(regionState.getRegion());
          }
        }
      }
      return resp;
    }
  }
}
