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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.RegionTransition;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.exceptions.MergeRegionException;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.base.Joiner;

/**
 * Like {@link TestRegionMergeTransaction} in that we're testing
 * {@link RegionMergeTransactionImpl} only the below tests are against a running
 * cluster where {@link TestRegionMergeTransaction} is tests against bare
 * {@link HRegion}.
 */
@Category(LargeTests.class)
public class TestRegionMergeTransactionOnCluster {
  private static final Log LOG = LogFactory
      .getLog(TestRegionMergeTransactionOnCluster.class);
  private static final int NB_SERVERS = 3;

  private static final byte[] FAMILYNAME = Bytes.toBytes("fam");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");

  private static byte[] ROW = Bytes.toBytes("testRow");
  private static final int INITIAL_REGION_NUM = 10;
  private static final int ROWSIZE = 200;
  private static byte[][] ROWS = makeN(ROW, ROWSIZE);

  private static int waitTime = 60 * 1000;

  static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static HMaster master;
  private static Admin admin;
  static MiniHBaseCluster cluster;
  static Configuration conf;

  static void setupOnce() throws Exception {
    // Start a cluster
    TEST_UTIL.startMiniCluster(NB_SERVERS);
    cluster = TEST_UTIL.getHBaseCluster();
    master = cluster.getMaster();
    master.balanceSwitch(false);
    admin = TEST_UTIL.getHBaseAdmin();
  }

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    // Use ZK for region assignment
    conf.setBoolean("hbase.assignment.usezk", true);
    setupOnce();
  }

  @AfterClass
  public static void afterAllTests() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testWholesomeMerge() throws Exception {
    LOG.info("Starting testWholesomeMerge");
    final TableName tableName =
        TableName.valueOf("testWholesomeMerge");

    // Create table and load data.
    Table table = createTableAndLoadData(master, tableName);
    // Merge 1st and 2nd region
    mergeRegionsAndVerifyRegionNum(master, tableName, 0, 1,
        INITIAL_REGION_NUM - 1);

    // Merge 2nd and 3th region
    PairOfSameType<HRegionInfo> mergedRegions =
      mergeRegionsAndVerifyRegionNum(master, tableName, 1, 2,
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
    am.assign(hri, true, true);
    assertFalse("Merged region can't be assigned",
      regionStates.isRegionInTransition(hri));
    assertTrue(regionStates.isRegionInState(hri, State.MERGED));

    // We should not be able to unassign it either
    am.unassign(hri, true, null);
    assertFalse("Merged region can't be unassigned",
      regionStates.isRegionInTransition(hri));
    assertTrue(regionStates.isRegionInState(hri, State.MERGED));

    table.close();
  }

  @Test
  public void testCleanMergeReference() throws Exception {
    LOG.info("Starting testCleanMergeReference");
    admin.enableCatalogJanitor(false);
    try {
      final TableName tableName =
          TableName.valueOf("testCleanMergeReference");
      // Create table and load data.
      Table table = createTableAndLoadData(master, tableName);
      // Merge 1st and 2nd region
      mergeRegionsAndVerifyRegionNum(master, tableName, 0, 1,
          INITIAL_REGION_NUM - 1);
      verifyRowCount(table, ROWSIZE);
      table.close();

      List<Pair<HRegionInfo, ServerName>> tableRegions = MetaTableAccessor
          .getTableRegionsAndLocations(master.getZooKeeper(), master.getConnection(), tableName);
      HRegionInfo mergedRegionInfo = tableRegions.get(0).getFirst();
      HTableDescriptor tableDescritor = master.getTableDescriptors().get(
          tableName);
      Result mergedRegionResult = MetaTableAccessor.getRegionResult(
        master.getConnection(), mergedRegionInfo.getRegionName());

      // contains merge reference in META
      assertTrue(mergedRegionResult.getValue(HConstants.CATALOG_FAMILY,
          HConstants.MERGEA_QUALIFIER) != null);
      assertTrue(mergedRegionResult.getValue(HConstants.CATALOG_FAMILY,
          HConstants.MERGEB_QUALIFIER) != null);

      // merging regions' directory are in the file system all the same
      HRegionInfo regionA = HRegionInfo.getHRegionInfo(mergedRegionResult,
          HConstants.MERGEA_QUALIFIER);
      HRegionInfo regionB = HRegionInfo.getHRegionInfo(mergedRegionResult,
          HConstants.MERGEB_QUALIFIER);
      FileSystem fs = master.getMasterFileSystem().getFileSystem();
      Path rootDir = master.getMasterFileSystem().getRootDir();

      Path tabledir = FSUtils.getTableDir(rootDir, mergedRegionInfo.getTable());
      Path regionAdir = new Path(tabledir, regionA.getEncodedName());
      Path regionBdir = new Path(tabledir, regionB.getEncodedName());
      assertTrue(fs.exists(regionAdir));
      assertTrue(fs.exists(regionBdir));

      admin.compactRegion(mergedRegionInfo.getRegionName());
      // wait until merged region doesn't have reference file
      long timeout = System.currentTimeMillis() + waitTime;
      HRegionFileSystem hrfs = new HRegionFileSystem(
          TEST_UTIL.getConfiguration(), fs, tabledir, mergedRegionInfo);
      while (System.currentTimeMillis() < timeout) {
        if (!hrfs.hasReferences(tableDescritor)) {
          break;
        }
        Thread.sleep(50);
      }
      assertFalse(hrfs.hasReferences(tableDescritor));

      // run CatalogJanitor to clean merge references in hbase:meta and archive the
      // files of merging regions
      int cleaned = admin.runCatalogScan();
      assertTrue(cleaned > 0);
      assertFalse(fs.exists(regionAdir));
      assertFalse(fs.exists(regionBdir));

      mergedRegionResult = MetaTableAccessor.getRegionResult(
        master.getConnection(), mergedRegionInfo.getRegionName());
      assertFalse(mergedRegionResult.getValue(HConstants.CATALOG_FAMILY,
          HConstants.MERGEA_QUALIFIER) != null);
      assertFalse(mergedRegionResult.getValue(HConstants.CATALOG_FAMILY,
          HConstants.MERGEB_QUALIFIER) != null);

    } finally {
      admin.enableCatalogJanitor(true);
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
      Table table = createTableAndLoadData(master, tableName);
      RegionStates regionStates = master.getAssignmentManager().getRegionStates();
      List<HRegionInfo> regions = regionStates.getRegionsOfTable(tableName);
      // Fake offline one region
      HRegionInfo a = regions.get(0);
      HRegionInfo b = regions.get(1);
      regionStates.regionOffline(a);
      try {
        // Merge offline region. Region a is offline here
        admin.mergeRegions(a.getEncodedNameAsBytes(), b.getEncodedNameAsBytes(), false);
        fail("Offline regions should not be able to merge");
      } catch (IOException ie) {
        System.out.println(ie);
        assertTrue("Exception should mention regions not online",
          StringUtils.stringifyException(ie).contains("regions not online")
            && ie instanceof MergeRegionException);
      }
      try {
        // Merge the same region: b and b.
        admin.mergeRegions(b.getEncodedNameAsBytes(), b.getEncodedNameAsBytes(), true);
        fail("A region should not be able to merge with itself, even forcifully");
      } catch (IOException ie) {
        assertTrue("Exception should mention regions not online",
          StringUtils.stringifyException(ie).contains("region to itself")
            && ie instanceof MergeRegionException);
      }
      try {
        // Merge unknown regions
        admin.mergeRegions(Bytes.toBytes("-f1"), Bytes.toBytes("-f2"), true);
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
    createTableAndLoadData(master, tableName, 5, 2);
    List<Pair<HRegionInfo, ServerName>> initialRegionToServers =
        MetaTableAccessor.getTableRegionsAndLocations(master.getZooKeeper(), master.getConnection(),
           tableName);
    // Merge 1st and 2nd region
    PairOfSameType<HRegionInfo> mergedRegions = mergeRegionsAndVerifyRegionNum(master, tableName,
        0, 2, 5 * 2 - 2);
    List<Pair<HRegionInfo, ServerName>> currentRegionToServers =
        MetaTableAccessor.getTableRegionsAndLocations(master.getZooKeeper(), master.getConnection(),
           tableName);
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
        .getTableRegionsAndLocations(master.getZooKeeper(),
          master.getConnection(), tablename);
    HRegionInfo regionA = tableRegions.get(regionAnum).getFirst();
    HRegionInfo regionB = tableRegions.get(regionBnum).getFirst();
    TEST_UTIL.getHBaseAdmin().mergeRegions(
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
      tableRegionsInMeta = MetaTableAccessor.getTableRegionsAndLocations(master.getZooKeeper(),
        master.getConnection(), tablename);
      tableRegionsInMaster = master.getAssignmentManager().getRegionStates()
          .getRegionsOfTable(tablename);
      if (tableRegionsInMeta.size() == expectedRegionNum
          && tableRegionsInMaster.size() == expectedRegionNum) {
        break;
      }
      Thread.sleep(250);
    }

    tableRegionsInMeta = MetaTableAccessor.getTableRegionsAndLocations(master.getZooKeeper(),
      master.getConnection(), tablename);
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
    if (replication > 1) {
      HBaseTestingUtility.setReplicas(admin, tablename, replication);
    }
    loadData(table);
    verifyRowCount(table, ROWSIZE);

    // sleep here is an ugly hack to allow region transitions to finish
    long timeout = System.currentTimeMillis() + waitTime;
    List<Pair<HRegionInfo, ServerName>> tableRegions;
    while (System.currentTimeMillis() < timeout) {
      tableRegions = MetaTableAccessor.getTableRegionsAndLocations(master.getZooKeeper(),
        master.getConnection(), tablename);
      if (tableRegions.size() == numRegions * replication)
        break;
      Thread.sleep(250);
    }

    tableRegions = MetaTableAccessor.getTableRegionsAndLocations(
      master.getZooKeeper(),
      master.getConnection(), tablename);
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
      put.add(FAMILYNAME, QUALIFIER, Bytes.toBytes(i));
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

  /**
   * A test that intentionally has master fail the processing of the merge message.
   * Tests that the regionserver merge ephemeral node gets cleaned up if it
   * crashes and that after we process server shutdown, the parent regions are online and
   * merged region is cleaned up.
   */
  @Test (timeout = 60000)
  public void testMergeIsRolledBackOnMergeFailure() throws Exception {

    final RegionStates regionStates = master.getAssignmentManager().getRegionStates();
    final ZooKeeperWatcher zkw = TEST_UTIL.getZooKeeperWatcher();

    final TableName tableName = TableName.valueOf("testMergeIsRolledBackOnMergeFailure");
    // Create table with 2 regions as its easy for us to merge.
    createTableAndLoadData(master, tableName, 2, 1);
    List<HRegion> regions = cluster.getRegions(tableName);

    assertEquals("Table shudn't have more than 2 regions, " + regions, 2, regions.size());
    final HRegionInfo regionA = regions.get(0).getRegionInfo();
    final HRegionInfo regionB = regions.get(1).getRegionInfo();

    // Turn off balancer so it doesn't cut in and mess up our placements.
    admin.setBalancerRunning(false, true);
    // Turn off the meta scanner so it don't remove parent on us.
    master.setCatalogJanitorEnabled(false);

    // Start a server and move both the regions to it. We kill this server later.
    HRegionServer regionServer = cluster.startRegionServer().getRegionServer();
    moveRegionToServer(regionA, regionServer);
    moveRegionToServer(regionB, regionServer);

    int serverIndex = cluster.getServerWith(regionA.getRegionName());

    // This helps with server aborts later.
    TEST_UTIL.compact(tableName, true);

    try {
      printOutRegions(regionServer, "Initial regions: ");

      // Now, before we merge, set special flag in master, a flag that has
      // it FAIL the processing of merge.
      AssignmentManager.setTestSkipMergeHandling(true);
      admin.mergeRegions(regionA.getRegionName(), regionB.getRegionName(), false);

      // Lets wait until we have a merge region.
      TEST_UTIL.waitFor(60000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return regionStates.getRegionByStateOfTable(tableName).get(State.MERGING_NEW).size() > 0;
        }
      });

      List<HRegionInfo> mergedRegions =
          regionStates.getRegionByStateOfTable(tableName).get(State.MERGING_NEW);
      assertEquals("Only one region should be in MERGING_NEW state", 1, mergedRegions.size());
      final HRegionInfo merge = mergedRegions.get(0);

      // Lets double check if we have the merge Znode with the appr. state.
      final String path = ZKAssign.getNodeName(zkw, merge.getEncodedName());
      // Wait till the znode moved to MERGED
      TEST_UTIL.waitFor(60000, new Waiter.Predicate<Exception>() {
        @Override public boolean evaluate() throws Exception {
          Stat stats = zkw.getRecoverableZooKeeper().exists(path, false);
          RegionTransition rt =
              RegionTransition.parseFrom(ZKAssign.getData(zkw, merge.getEncodedName()));
          return stats != null && rt.getEventType().equals(EventType.RS_ZK_REGION_MERGED);
        }
      });

      // Now crash the server
      abortServerAndWaitForProcessingToComplete(serverIndex);
      waitUntilRegionServerDead();

      TEST_UTIL.waitUntilNoRegionsInTransition(waitTime);

      // Lets wait until merge parents are online.
      TEST_UTIL.waitFor(60000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() {
          return cluster.getRegions(tableName).size() == 2;
        }
      });

      // Check if merge regions is cleaned up.
      List<HRegionInfo> tableRegions = MetaTableAccessor.getTableRegions(zkw,
          cluster.getMaster().getConnection(), tableName);
      assertEquals("Only parent regions should be present, but we have: " + tableRegions,
          2, tableRegions.size());
      assertTrue("Merge A not present? " + regionA, tableRegions.contains(regionA));
      assertTrue("Merge B not present? " + regionB, tableRegions.contains(regionB));

      // Are both merge parents online?
      assertTrue("region should be online, " + regionA, regionStates.isRegionOnline(regionA));
      assertTrue("region should be online, " + regionB, regionStates.isRegionOnline(regionB));

      // Have HDFS dirs been cleaned up?
      Path tableDir = FSUtils.getTableDir(FSUtils.getRootDir(conf), tableName);
      List<Path> regionDirs =
          FSUtils.getRegionDirs(cluster.getMaster().getFileSystem(), tableDir);
      assertEquals("Only two region dir should be present, we have, dirs: " + regionDirs,
          2, regionDirs.size());

      assertTrue("Region dir doesn't belong to region: " + regionA + " dir: " + regionDirs,
          regionDirs.get(0).getName().endsWith(regionA.getEncodedName())
              || regionDirs.get(1).getName().endsWith(regionA.getEncodedName()));
      assertTrue("Region dir doesn't belong to region: " + regionB + " dir: " + regionDirs,
          regionDirs.get(0).getName().endsWith(regionB.getEncodedName())
              || regionDirs.get(1).getName().endsWith(regionB.getEncodedName()));

      // The merged Znode should have been cleaned up.
      Stat stat = zkw.getRecoverableZooKeeper().exists(path, false);
      assertNull("Merged znode shouldn't exist, but we have stat: " + stat, stat);

    } finally {
      // Set this flag back.
      AssignmentManager.setTestSkipMergeHandling(false);
      admin.setBalancerRunning(true, false);
      master.setCatalogJanitorEnabled(true);
      TEST_UTIL.deleteTable(tableName);
    }
  }

  private void moveRegionToServer(final HRegionInfo region, final HRegionServer rs)
      throws Exception {
    admin.move(region.getEncodedNameAsBytes(), rs.getServerName().toString().getBytes());
    TEST_UTIL.waitUntilNoRegionsInTransition(waitTime);
    TEST_UTIL.waitFor(60000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return rs.getOnlineRegion(region.getRegionName()) != null;
      }
    });
  }

  private void waitUntilRegionServerDead() throws InterruptedException, IOException {
    // Wait until the master processes the RS shutdown
    for (int i=0; cluster.getMaster().getClusterStatus().
        getServers().size() > NB_SERVERS && i<100; i++) {
      LOG.info("Waiting on server to go down");
      Thread.sleep(100);
    }
    assertFalse("Waited too long for RS to die", cluster.getMaster().getClusterStatus().
        getServers().size() > NB_SERVERS);
  }

  private void printOutRegions(final HRegionServer hrs, final String prefix)
      throws IOException {
    List<HRegionInfo> regions = ProtobufUtil.getOnlineRegions(hrs.getRSRpcServices());
    for (HRegionInfo region: regions) {
      LOG.info(prefix + region.getRegionNameAsString());
    }
  }

  private void abortServerAndWaitForProcessingToComplete(int serverIndex) throws Exception {

    final HMaster master = TEST_UTIL.getMiniHBaseCluster().getMaster();
    cluster.abortRegionServer(serverIndex);
    TEST_UTIL.waitFor(60000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return master.getServerManager().areDeadServersInProgress();
      }
    });
  }
}
