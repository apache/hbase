/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CategoryBasedTimeout;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.exceptions.MergeRegionException;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterRpcServices;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionResponse;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;

import com.google.common.base.Joiner;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Like {@link TestRegionMergeTransaction} in that we're testing
 * {@link RegionMergeTransactionImpl} only the below tests are against a running
 * cluster where {@link TestRegionMergeTransaction} is tests against bare
 * {@link HRegion}.
 */
@Category({RegionServerTests.class, LargeTests.class})
public class TestRegionMergeTransactionOnCluster {
  private static final Log LOG = LogFactory
      .getLog(TestRegionMergeTransactionOnCluster.class);
  @Rule public TestName name = new TestName();
  @Rule public final TestRule timeout = CategoryBasedTimeout.builder().withTimeout(this.getClass()).
      withLookingForStuckThread(true).build();
  private static final int NB_SERVERS = 3;

  private static final byte[] FAMILYNAME = Bytes.toBytes("fam");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");

  private static byte[] ROW = Bytes.toBytes("testRow");
  private static final int INITIAL_REGION_NUM = 10;
  private static final int ROWSIZE = 200;
  private static byte[][] ROWS = makeN(ROW, ROWSIZE);

  private static int waitTime = 60 * 1000;

  static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static HMaster MASTER;
  private static Admin ADMIN;

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    // Start a cluster
    TEST_UTIL.startMiniCluster(1, NB_SERVERS, null, MyMaster.class, null);
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    MASTER = cluster.getMaster();
    MASTER.balanceSwitch(false);
    ADMIN = TEST_UTIL.getConnection().getAdmin();
  }

  @AfterClass
  public static void afterAllTests() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    if (ADMIN != null) ADMIN.close();
  }

  @Test
  public void testWholesomeMerge() throws Exception {
    LOG.info("Starting testWholesomeMerge");
    final TableName tableName =
        TableName.valueOf("testWholesomeMerge");

    // Create table and load data.
    Table table = createTableAndLoadData(MASTER, tableName);
    // Merge 1st and 2nd region
    mergeRegionsAndVerifyRegionNum(MASTER, tableName, 0, 1,
        INITIAL_REGION_NUM - 1);

    // Merge 2nd and 3th region
    PairOfSameType<HRegionInfo> mergedRegions =
      mergeRegionsAndVerifyRegionNum(MASTER, tableName, 1, 2,
        INITIAL_REGION_NUM - 2);

    verifyRowCount(table, ROWSIZE);

    // Randomly choose one of the two merged regions
    HRegionInfo hri = RandomUtils.nextBoolean() ?
      mergedRegions.getFirst() : mergedRegions.getSecond();
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    AssignmentManager am = cluster.getMaster().getAssignmentManager();
    RegionStates regionStates = am.getRegionStates();
    long start = EnvironmentEdgeManager.currentTime();
    while (!regionStates.isRegionInState(hri, State.MERGED)) {
      assertFalse("Timed out in waiting one merged region to be in state MERGED",
        EnvironmentEdgeManager.currentTime() - start > 60000);
      Thread.sleep(500);
    }

    // We should not be able to assign it again
    am.assign(hri, true);
    assertFalse("Merged region can't be assigned",
      regionStates.isRegionInTransition(hri));
    assertTrue(regionStates.isRegionInState(hri, State.MERGED));

    // We should not be able to unassign it either
    am.unassign(hri, null);
    assertFalse("Merged region can't be unassigned",
      regionStates.isRegionInTransition(hri));
    assertTrue(regionStates.isRegionInState(hri, State.MERGED));

    table.close();
  }

  /**
   * Not really restarting the master. Simulate it by clear of new region
   * state since it is not persisted, will be lost after master restarts.
   */
  @Test
  public void testMergeAndRestartingMaster() throws Exception {
    final TableName tableName = TableName.valueOf("testMergeAndRestartingMaster");

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
  }

  @Test
  public void testCleanMergeReference() throws Exception {
    LOG.info("Starting testCleanMergeReference");
    ADMIN.enableCatalogJanitor(false);
    try {
      final TableName tableName =
          TableName.valueOf("testCleanMergeReference");
      // Create table and load data.
      Table table = createTableAndLoadData(MASTER, tableName);
      // Merge 1st and 2nd region
      mergeRegionsAndVerifyRegionNum(MASTER, tableName, 0, 1,
          INITIAL_REGION_NUM - 1);
      verifyRowCount(table, ROWSIZE);
      table.close();

      List<Pair<HRegionInfo, ServerName>> tableRegions = MetaTableAccessor
          .getTableRegionsAndLocations(MASTER.getConnection(), tableName);
      HRegionInfo mergedRegionInfo = tableRegions.get(0).getFirst();
      HTableDescriptor tableDescriptor = MASTER.getTableDescriptors().get(
          tableName);
      Result mergedRegionResult = MetaTableAccessor.getRegionResult(
        MASTER.getConnection(), mergedRegionInfo.getRegionName());

      // contains merge reference in META
      assertTrue(mergedRegionResult.getValue(HConstants.CATALOG_FAMILY,
          HConstants.MERGEA_QUALIFIER) != null);
      assertTrue(mergedRegionResult.getValue(HConstants.CATALOG_FAMILY,
          HConstants.MERGEB_QUALIFIER) != null);

      // merging regions' directory are in the file system all the same
      PairOfSameType<HRegionInfo> p = MetaTableAccessor.getMergeRegions(mergedRegionResult);
      HRegionInfo regionA = p.getFirst();
      HRegionInfo regionB = p.getSecond();
      FileSystem fs = MASTER.getMasterFileSystem().getFileSystem();
      Path rootDir = MASTER.getMasterFileSystem().getRootDir();

      Path tabledir = FSUtils.getTableDir(rootDir, mergedRegionInfo.getTable());
      Path regionAdir = new Path(tabledir, regionA.getEncodedName());
      Path regionBdir = new Path(tabledir, regionB.getEncodedName());
      assertTrue(fs.exists(regionAdir));
      assertTrue(fs.exists(regionBdir));

      HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
      HRegionFileSystem hrfs = new HRegionFileSystem(
        TEST_UTIL.getConfiguration(), fs, tabledir, mergedRegionInfo);
      int count = 0;
      for(HColumnDescriptor colFamily : columnFamilies) {
        count += hrfs.getStoreFiles(colFamily.getName()).size();
      }
      ADMIN.compactRegion(mergedRegionInfo.getRegionName());
      // clean up the merged region store files
      // wait until merged region have reference file
      long timeout = System.currentTimeMillis() + waitTime;
      int newcount = 0;
      while (System.currentTimeMillis() < timeout) {
        for(HColumnDescriptor colFamily : columnFamilies) {
          newcount += hrfs.getStoreFiles(colFamily.getName()).size();
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
      while (System.currentTimeMillis() < timeout) {
        int newcount1 = 0;
        for(HColumnDescriptor colFamily : columnFamilies) {
          newcount1 += hrfs.getStoreFiles(colFamily.getName()).size();
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
        cleaned = ADMIN.runCatalogScan();
        LOG.debug("catalog janitor returned " + cleaned);
        Thread.sleep(50);
      }
      assertFalse(regionAdir.toString(), fs.exists(regionAdir));
      assertFalse(regionBdir.toString(), fs.exists(regionBdir));
      assertTrue(cleaned > 0);

      mergedRegionResult = MetaTableAccessor.getRegionResult(
        TEST_UTIL.getConnection(), mergedRegionInfo.getRegionName());
      assertFalse(mergedRegionResult.getValue(HConstants.CATALOG_FAMILY,
          HConstants.MERGEA_QUALIFIER) != null);
      assertFalse(mergedRegionResult.getValue(HConstants.CATALOG_FAMILY,
          HConstants.MERGEB_QUALIFIER) != null);

    } finally {
      ADMIN.enableCatalogJanitor(true);
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
    LOG.info("Starting testMerge");
    final TableName tableName = TableName.valueOf("testMerge");

    try {
      // Create table and load data.
      Table table = createTableAndLoadData(MASTER, tableName);
      RegionStates regionStates = MASTER.getAssignmentManager().getRegionStates();
      List<HRegionInfo> regions = regionStates.getRegionsOfTable(tableName);
      // Fake offline one region
      HRegionInfo a = regions.get(0);
      HRegionInfo b = regions.get(1);
      regionStates.regionOffline(a);
      try {
        // Merge offline region. Region a is offline here
        ADMIN.mergeRegions(a.getEncodedNameAsBytes(), b.getEncodedNameAsBytes(), false);
        fail("Offline regions should not be able to merge");
      } catch (IOException ie) {
        System.out.println(ie);
        assertTrue("Exception should mention regions not online",
          StringUtils.stringifyException(ie).contains("regions not online")
            && ie instanceof MergeRegionException);
      }
      try {
        // Merge the same region: b and b.
        ADMIN.mergeRegions(b.getEncodedNameAsBytes(), b.getEncodedNameAsBytes(), true);
        fail("A region should not be able to merge with itself, even forcifully");
      } catch (IOException ie) {
        assertTrue("Exception should mention regions not online",
          StringUtils.stringifyException(ie).contains("region to itself")
            && ie instanceof MergeRegionException);
      }
      try {
        // Merge unknown regions
        ADMIN.mergeRegions(Bytes.toBytes("-f1"), Bytes.toBytes("-f2"), true);
        fail("Unknown region could not be merged");
      } catch (IOException ie) {
        assertTrue("UnknownRegionException should be thrown",
          ie instanceof UnknownRegionException);
      }
      table.close();
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }

  @Test
  public void testMergeWithReplicas() throws Exception {
    final TableName tableName = TableName.valueOf("testMergeWithReplicas");
    // Create table and load data.
    createTableAndLoadData(MASTER, tableName, 5, 2);
    List<Pair<HRegionInfo, ServerName>> initialRegionToServers =
        MetaTableAccessor.getTableRegionsAndLocations(
            TEST_UTIL.getConnection(), tableName);
    // Merge 1st and 2nd region
    PairOfSameType<HRegionInfo> mergedRegions = mergeRegionsAndVerifyRegionNum(MASTER, tableName,
        0, 2, 5 * 2 - 2);
    List<Pair<HRegionInfo, ServerName>> currentRegionToServers =
        MetaTableAccessor.getTableRegionsAndLocations(
            TEST_UTIL.getConnection(), tableName);
    List<HRegionInfo> initialRegions = new ArrayList<HRegionInfo>();
    for (Pair<HRegionInfo, ServerName> p : initialRegionToServers) {
      initialRegions.add(p.getFirst());
    }
    List<HRegionInfo> currentRegions = new ArrayList<HRegionInfo>();
    for (Pair<HRegionInfo, ServerName> p : currentRegionToServers) {
      currentRegions.add(p.getFirst());
    }
    assertTrue(initialRegions.contains(mergedRegions.getFirst())); //this is the first region
    assertTrue(initialRegions.contains(RegionReplicaUtil.getRegionInfoForReplica(
        mergedRegions.getFirst(), 1))); //this is the replica of the first region
    assertTrue(initialRegions.contains(mergedRegions.getSecond())); //this is the second region
    assertTrue(initialRegions.contains(RegionReplicaUtil.getRegionInfoForReplica(
        mergedRegions.getSecond(), 1))); //this is the replica of the second region
    assertTrue(!initialRegions.contains(currentRegions.get(0))); //this is the new region
    assertTrue(!initialRegions.contains(RegionReplicaUtil.getRegionInfoForReplica(
        currentRegions.get(0), 1))); //replica of the new region
    assertTrue(currentRegions.contains(RegionReplicaUtil.getRegionInfoForReplica(
        currentRegions.get(0), 1))); //replica of the new region
    assertTrue(!currentRegions.contains(RegionReplicaUtil.getRegionInfoForReplica(
        mergedRegions.getFirst(), 1))); //replica of the merged region
    assertTrue(!currentRegions.contains(RegionReplicaUtil.getRegionInfoForReplica(
        mergedRegions.getSecond(), 1))); //replica of the merged region
  }

  private PairOfSameType<HRegionInfo> mergeRegionsAndVerifyRegionNum(
      HMaster master, TableName tablename,
      int regionAnum, int regionBnum, int expectedRegionNum) throws Exception {
    PairOfSameType<HRegionInfo> mergedRegions =
      requestMergeRegion(master, tablename, regionAnum, regionBnum);
    waitAndVerifyRegionNum(master, tablename, expectedRegionNum);
    return mergedRegions;
  }

  private PairOfSameType<HRegionInfo> requestMergeRegion(
      HMaster master, TableName tablename,
      int regionAnum, int regionBnum) throws Exception {
    List<Pair<HRegionInfo, ServerName>> tableRegions = MetaTableAccessor
        .getTableRegionsAndLocations(
            TEST_UTIL.getConnection(), tablename);
    HRegionInfo regionA = tableRegions.get(regionAnum).getFirst();
    HRegionInfo regionB = tableRegions.get(regionBnum).getFirst();
    ADMIN.mergeRegions(
      regionA.getEncodedNameAsBytes(),
      regionB.getEncodedNameAsBytes(), false);
    return new PairOfSameType<HRegionInfo>(regionA, regionB);
  }

  private void waitAndVerifyRegionNum(HMaster master, TableName tablename,
      int expectedRegionNum) throws Exception {
    List<Pair<HRegionInfo, ServerName>> tableRegionsInMeta;
    List<HRegionInfo> tableRegionsInMaster;
    long timeout = System.currentTimeMillis() + waitTime;
    while (System.currentTimeMillis() < timeout) {
      tableRegionsInMeta = MetaTableAccessor.getTableRegionsAndLocations(
        TEST_UTIL.getConnection(), tablename);
      tableRegionsInMaster = master.getAssignmentManager().getRegionStates()
          .getRegionsOfTable(tablename);
      if (tableRegionsInMeta.size() == expectedRegionNum
          && tableRegionsInMaster.size() == expectedRegionNum) {
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
      HBaseTestingUtility.setReplicas(ADMIN, tablename, replication);
      LOG.info("Set replication of " + replication + " on " + table.getName());
    }
    loadData(table);
    LOG.info("Loaded " + table.getName());
    verifyRowCount(table, ROWSIZE);
    LOG.info("Verified " + table.getName());

    // sleep here is an ugly hack to allow region transitions to finish
    long timeout = System.currentTimeMillis() + waitTime;
    List<Pair<HRegionInfo, ServerName>> tableRegions;
    while (System.currentTimeMillis() < timeout) {
      tableRegions = MetaTableAccessor.getTableRegionsAndLocations(
          TEST_UTIL.getConnection(), tablename);
      if (tableRegions.size() == numRegions * replication)
        break;
      Thread.sleep(250);
    }
    LOG.info("Getting regions of " + table.getName());
    tableRegions = MetaTableAccessor.getTableRegionsAndLocations(
        TEST_UTIL.getConnection(), tablename);
    LOG.info("Regions after load: " + Joiner.on(',').join(tableRegions));
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
    public MyMaster(Configuration conf, CoordinatedStateManager cp)
      throws IOException, KeeperException,
        InterruptedException {
      super(conf, cp);
    }

    @Override
    protected RSRpcServices createRpcServices() throws IOException {
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
        for (RegionState regionState: regionStates.getRegionsInTransition()) {
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