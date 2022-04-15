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

import static org.apache.hadoop.hbase.client.TableDescriptorBuilder.SPLIT_POLICY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.DoNotRetryRegionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.TestReplicasClient.SlowMeCopro;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterRpcServices;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.AssignmentTestingUtil;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.throttle.NoLimitThroughputController;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.util.HBaseFsck;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.io.Closeables;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionResponse;

/**
 * The below tests are testing split region against a running cluster
 */
@Category({RegionServerTests.class, LargeTests.class})
public class TestSplitTransactionOnCluster {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSplitTransactionOnCluster.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestSplitTransactionOnCluster.class);
  private Admin admin = null;
  private SingleProcessHBaseCluster cluster = null;
  private static final int NB_SERVERS = 3;

  static final HBaseTestingUtil TESTING_UTIL = new HBaseTestingUtil();

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void before() throws Exception {
    TESTING_UTIL.getConfiguration().setInt(HConstants.HBASE_BALANCER_PERIOD, 60000);
    StartTestingClusterOption option = StartTestingClusterOption.builder()
        .masterClass(MyMaster.class).numRegionServers(NB_SERVERS).
            numDataNodes(NB_SERVERS).build();
    TESTING_UTIL.startMiniCluster(option);
  }

  @AfterClass
  public static void after() throws Exception {
    TESTING_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setup() throws IOException {
    TESTING_UTIL.ensureSomeNonStoppedRegionServersAvailable(NB_SERVERS);
    this.admin = TESTING_UTIL.getAdmin();
    this.cluster = TESTING_UTIL.getMiniHBaseCluster();
  }

  @After
  public void tearDown() throws Exception {
    this.admin.close();
    for (TableDescriptor htd: this.admin.listTableDescriptors()) {
      LOG.info("Tear down, remove table=" + htd.getTableName());
      TESTING_UTIL.deleteTable(htd.getTableName());
    }
  }

  private RegionInfo getAndCheckSingleTableRegion(final List<HRegion> regions)
      throws IOException, InterruptedException {
    assertEquals(1, regions.size());
    RegionInfo hri = regions.get(0).getRegionInfo();
    AssignmentTestingUtil.waitForAssignment(cluster.getMaster().getAssignmentManager(), hri);
    return hri;
  }

  private void requestSplitRegion(
      final HRegionServer rsServer,
      final Region region,
      final byte[] midKey) throws IOException {
    long procId = cluster.getMaster().splitRegion(region.getRegionInfo(), midKey, 0, 0);
    // wait for the split to complete or get interrupted.  If the split completes successfully,
    // the procedure will return true; if the split fails, the procedure would throw exception.
    ProcedureTestingUtility.waitProcedure(cluster.getMaster().getMasterProcedureExecutor(), procId);
  }

  @Test
  public void testRITStateForRollback() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final HMaster master = cluster.getMaster();
    try {
      // Create table then get the single region for our new table.
      Table t = createTableAndWait(tableName, Bytes.toBytes("cf"));
      final List<HRegion> regions = cluster.getRegions(tableName);
      final RegionInfo hri = getAndCheckSingleTableRegion(regions);
      insertData(tableName, admin, t);
      t.close();

      // Turn off balancer so it doesn't cut in and mess up our placements.
      this.admin.balancerSwitch(false, true);
      // Turn off the meta scanner so it don't remove parent on us.
      master.setCatalogJanitorEnabled(false);

      // find a splittable region
      final HRegion region = findSplittableRegion(regions);
      assertTrue("not able to find a splittable region", region != null);

      // install master co-processor to fail splits
      master.getMasterCoprocessorHost().load(
        FailingSplitMasterObserver.class,
        Coprocessor.PRIORITY_USER,
        master.getConfiguration());

      // split async
      this.admin.splitRegionAsync(region.getRegionInfo().getRegionName(), new byte[] { 42 });

      // we have to wait until the SPLITTING state is seen by the master
      FailingSplitMasterObserver observer =
          master.getMasterCoprocessorHost().findCoprocessor(FailingSplitMasterObserver.class);
      assertNotNull(observer);
      observer.latch.await();

      LOG.info("Waiting for region to come out of RIT");
      while (!cluster.getMaster().getAssignmentManager().getRegionStates().isRegionOnline(hri)) {
        Threads.sleep(100);
      }
      assertTrue(cluster.getMaster().getAssignmentManager().getRegionStates().isRegionOnline(hri));
    } finally {
      admin.balancerSwitch(true, false);
      master.setCatalogJanitorEnabled(true);
      abortAndWaitForMaster();
      TESTING_UTIL.deleteTable(tableName);
    }
  }

  @Test
  public void testSplitFailedCompactionAndSplit() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    // Create table then get the single region for our new table.
    byte[] cf = Bytes.toBytes("cf");
    TableDescriptor htd = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(cf)).build();
    admin.createTable(htd);

    for (int i = 0; cluster.getRegions(tableName).isEmpty() && i < 100; i++) {
      Thread.sleep(100);
    }
    assertEquals(1, cluster.getRegions(tableName).size());

    HRegion region = cluster.getRegions(tableName).get(0);
    HStore store = region.getStore(cf);
    int regionServerIndex = cluster.getServerWith(region.getRegionInfo().getRegionName());
    HRegionServer regionServer = cluster.getRegionServer(regionServerIndex);

    Table t = TESTING_UTIL.getConnection().getTable(tableName);
    // insert data
    insertData(tableName, admin, t);
    insertData(tableName, admin, t);

    int fileNum = store.getStorefiles().size();
    // 0, Compaction Request
    store.triggerMajorCompaction();
    Optional<CompactionContext> cc = store.requestCompaction();
    assertTrue(cc.isPresent());
    // 1, A timeout split
    // 1.1 close region
    assertEquals(2, region.close(false).get(cf).size());
    // 1.2 rollback and Region initialize again
    region.initialize();

    // 2, Run Compaction cc
    assertFalse(region.compact(cc.get(), store, NoLimitThroughputController.INSTANCE));
    assertTrue(fileNum > store.getStorefiles().size());

    // 3, Split
    requestSplitRegion(regionServer, region, Bytes.toBytes("row3"));
    assertEquals(2, cluster.getRegions(tableName).size());
  }

  @Test
  public void testSplitCompactWithPriority() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    // Create table then get the single region for our new table.
    byte[] cf = Bytes.toBytes("cf");
    TableDescriptor htd = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(cf)).build();
    admin.createTable(htd);

    assertNotEquals("Unable to retrieve regions of the table", -1,
      TESTING_UTIL.waitFor(10000, () -> cluster.getRegions(tableName).size() == 1));

    HRegion region = cluster.getRegions(tableName).get(0);
    HStore store = region.getStore(cf);
    int regionServerIndex = cluster.getServerWith(region.getRegionInfo().getRegionName());
    HRegionServer regionServer = cluster.getRegionServer(regionServerIndex);

    Table table = TESTING_UTIL.getConnection().getTable(tableName);
    // insert data
    insertData(tableName, admin, table);
    insertData(tableName, admin, table, 20);
    insertData(tableName, admin, table, 40);

    // Compaction Request
    store.triggerMajorCompaction();
    Optional<CompactionContext> compactionContext = store.requestCompaction();
    assertTrue(compactionContext.isPresent());
    assertFalse(compactionContext.get().getRequest().isAfterSplit());
    assertEquals(compactionContext.get().getRequest().getPriority(), 13);

    // Split
    long procId =
      cluster.getMaster().splitRegion(region.getRegionInfo(), Bytes.toBytes("row4"), 0, 0);

    // wait for the split to complete or get interrupted.  If the split completes successfully,
    // the procedure will return true; if the split fails, the procedure would throw exception.
    ProcedureTestingUtility.waitProcedure(cluster.getMaster().getMasterProcedureExecutor(),
      procId);
    Thread.sleep(3000);
    assertNotEquals("Table is not split properly?", -1,
      TESTING_UTIL.waitFor(3000,
        () -> cluster.getRegions(tableName).size() == 2));
    // we have 2 daughter regions
    HRegion hRegion1 = cluster.getRegions(tableName).get(0);
    HRegion hRegion2 = cluster.getRegions(tableName).get(1);
    HStore hStore1 = hRegion1.getStore(cf);
    HStore hStore2 = hRegion2.getStore(cf);

    // For hStore1 && hStore2, set mock reference to one of the storeFiles
    StoreFileInfo storeFileInfo1 = new ArrayList<>(hStore1.getStorefiles()).get(0).getFileInfo();
    StoreFileInfo storeFileInfo2 = new ArrayList<>(hStore2.getStorefiles()).get(0).getFileInfo();
    Field field = StoreFileInfo.class.getDeclaredField("reference");
    field.setAccessible(true);
    field.set(storeFileInfo1, Mockito.mock(Reference.class));
    field.set(storeFileInfo2, Mockito.mock(Reference.class));
    hStore1.triggerMajorCompaction();
    hStore2.triggerMajorCompaction();

    compactionContext = hStore1.requestCompaction();
    assertTrue(compactionContext.isPresent());
    // since we set mock reference to one of the storeFiles, we will get isAfterSplit=true &&
    // highest priority for hStore1's compactionContext
    assertTrue(compactionContext.get().getRequest().isAfterSplit());
    assertEquals(compactionContext.get().getRequest().getPriority(), Integer.MIN_VALUE + 1000);

    compactionContext =
      hStore2.requestCompaction(Integer.MIN_VALUE + 10, CompactionLifeCycleTracker.DUMMY, null);
    assertTrue(compactionContext.isPresent());
    // compaction request contains higher priority than default priority of daughter region
    // compaction (Integer.MIN_VALUE + 1000), hence we are expecting request priority to
    // be accepted.
    assertTrue(compactionContext.get().getRequest().isAfterSplit());
    assertEquals(compactionContext.get().getRequest().getPriority(), Integer.MIN_VALUE + 10);
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }

  @Test
  public void testContinuousSplitUsingLinkFile() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    // Create table then get the single region for our new table.
    byte[] cf = Bytes.toBytes("cf");
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(cf));
    String splitPolicy = ConstantSizeRegionSplitPolicy.class.getName();
    builder.setValue(SPLIT_POLICY, splitPolicy);

    admin.createTable(builder.build());
    admin.compactionSwitch(false, new ArrayList<>());

    assertNotEquals("Unable to retrieve regions of the table", -1,
      TESTING_UTIL.waitFor(10000, () -> cluster.getRegions(tableName).size() == 1));
    Table table = TESTING_UTIL.getConnection().getTable(tableName);
    // insert data
    insertData(tableName, admin, table, 10);
    insertData(tableName, admin, table, 20);
    insertData(tableName, admin, table, 40);
    int rowCount = 3 * 4;
    Scan scan = new Scan();
    scanValidate(scan, rowCount, table);

    // Split
    admin.splitRegionAsync(cluster.getRegions(tableName).get(0).getRegionInfo().getRegionName(),
      Bytes.toBytes("row14"));
    // wait for the split to complete or get interrupted.  If the split completes successfully,
    // the procedure will return true; if the split fails, the procedure would throw exception.
    Thread.sleep(3000);
    assertNotEquals("Table is not split properly?", -1,
      TESTING_UTIL.waitFor(3000, () -> cluster.getRegions(tableName).size() == 2));
    // we have 2 daughter regions
    HRegion hRegion1 = cluster.getRegions(tableName).get(0);
    HRegion hRegion2 = cluster.getRegions(tableName).get(1);
    HStore hStore1 = hRegion1.getStore(cf);
    HStore hStore2 = hRegion2.getStore(cf);
    // the sum of store files of the two children should be equal to their parent
    assertEquals(3, hStore1.getStorefilesCount() + hStore2.getStorefilesCount());
    // both the two children should have link files
    for (StoreFile sf : hStore1.getStorefiles()) {
      assertTrue(HFileLink.isHFileLink(sf.getPath()));
    }
    for (StoreFile sf : hStore2.getStorefiles()) {
      assertTrue(HFileLink.isHFileLink(sf.getPath()));
    }
    // validate children data
    scan = new Scan();
    scanValidate(scan, rowCount, table);

    //Continuous Split
    findRegionToSplit(tableName, "row24");
    Thread.sleep(3000);
    assertNotEquals("Table is not split properly?", -1,
      TESTING_UTIL.waitFor(3000, () -> cluster.getRegions(tableName).size() == 3));
    // now table has 3 region, each region should have one link file
    for (HRegion newRegion : cluster.getRegions(tableName)) {
      assertEquals(1, newRegion.getStore(cf).getStorefilesCount());
      assertTrue(
        HFileLink.isHFileLink(newRegion.getStore(cf).getStorefiles().iterator().next().getPath()));
    }

    scan = new Scan();
    scanValidate(scan, rowCount, table);

    //Continuous Split, random split HFileLink, generate Reference files.
    //After this, can not continuous split, because there are reference files.
    findRegionToSplit(tableName, "row11");
    Thread.sleep(3000);
    assertNotEquals("Table is not split properly?", -1,
      TESTING_UTIL.waitFor(3000, () -> cluster.getRegions(tableName).size() == 4));

    scan = new Scan();
    scanValidate(scan, rowCount, table);
  }

  private void findRegionToSplit(TableName tableName, String splitRowKey) throws Exception {
    HRegion toSplit = null;
    byte[] toSplitKey = Bytes.toBytes(splitRowKey);
    for(HRegion rg : cluster.getRegions(tableName)) {
      LOG.debug("startKey=" +
        Bytes.toStringBinary(rg.getRegionInfo().getStartKey()) + ", getEndKey()=" +
        Bytes.toStringBinary(rg.getRegionInfo().getEndKey()) + ", row=" + splitRowKey);
      if((rg.getRegionInfo().getStartKey().length==0||
        CellComparator.getInstance().compare(
          PrivateCellUtil.createFirstOnRow(rg.getRegionInfo().getStartKey()),
          PrivateCellUtil.createFirstOnRow(toSplitKey)) <= 0) &&(
        rg.getRegionInfo().getEndKey().length==0||
          CellComparator.getInstance().compare(
            PrivateCellUtil.createFirstOnRow(rg.getRegionInfo().getEndKey()),
            PrivateCellUtil.createFirstOnRow(toSplitKey)) >= 0)){
        toSplit = rg;
      }
    }
    assertNotNull(toSplit);
    admin.splitRegionAsync(toSplit.getRegionInfo().getRegionName(), toSplitKey);
  }

  private static void scanValidate(Scan scan, int expectedRowCount, Table table) throws IOException{
    ResultScanner scanner = table.getScanner(scan);
    int rows = 0;
    for (Result result : scanner) {
      rows++;
    }
    scanner.close();
    assertEquals(expectedRowCount, rows);
  }

  public static class FailingSplitMasterObserver implements MasterCoprocessor, MasterObserver {
    volatile CountDownLatch latch;

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
      latch = new CountDownLatch(1);
    }

    @Override
    public Optional<MasterObserver> getMasterObserver() {
      return Optional.of(this);
    }

    @Override
    public void preSplitRegionBeforeMETAAction(
        final ObserverContext<MasterCoprocessorEnvironment> ctx,
        final byte[] splitKey,
        final List<Mutation> metaEntries) throws IOException {
      latch.countDown();
      throw new IOException("Causing rollback of region split");
    }
  }

  @Test
  public void testSplitRollbackOnRegionClosing() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());

    // Create table then get the single region for our new table.
    Table t = createTableAndWait(tableName, HConstants.CATALOG_FAMILY);
    List<HRegion> regions = cluster.getRegions(tableName);
    RegionInfo hri = getAndCheckSingleTableRegion(regions);

    int tableRegionIndex = ensureTableRegionNotOnSameServerAsMeta(admin, hri);

    RegionStates regionStates = cluster.getMaster().getAssignmentManager().getRegionStates();

    // Turn off balancer so it doesn't cut in and mess up our placements.
    this.admin.balancerSwitch(false, true);
    // Turn off the meta scanner so it don't remove parent on us.
    cluster.getMaster().setCatalogJanitorEnabled(false);
    try {
      // Add a bit of load up into the table so splittable.
      TESTING_UTIL.loadTable(t, HConstants.CATALOG_FAMILY, false);
      // Get region pre-split.
      HRegionServer server = cluster.getRegionServer(tableRegionIndex);
      printOutRegions(server, "Initial regions: ");
      int regionCount = cluster.getRegions(hri.getTable()).size();
      regionStates.updateRegionState(hri, RegionState.State.CLOSING);

      // Now try splitting.... should fail.  And each should successfully
      // rollback.
      // We don't roll back here anymore. Instead we fail-fast on construction of the
      // split transaction. Catch the exception instead.
      try {
        FutureUtils.get(this.admin.splitRegionAsync(hri.getRegionName()));
        fail();
      } catch (DoNotRetryRegionException e) {
        // Expected
      }
      // Wait around a while and assert count of regions remains constant.
      for (int i = 0; i < 10; i++) {
        Thread.sleep(100);
        assertEquals(regionCount, cluster.getRegions(hri.getTable()).size());
      }
      regionStates.updateRegionState(hri, State.OPEN);
      // Now try splitting and it should work.
      admin.splitRegionAsync(hri.getRegionName()).get(2, TimeUnit.MINUTES);
      // Get daughters
      checkAndGetDaughters(tableName);
      // OK, so split happened after we cleared the blocking node.
    } finally {
      admin.balancerSwitch(true, false);
      cluster.getMaster().setCatalogJanitorEnabled(true);
      t.close();
    }
  }

  /**
   * Test that if daughter split on us, we won't do the shutdown handler fixup just because we can't
   * find the immediate daughter of an offlined parent.
   */
  @Test
  public void testShutdownFixupWhenDaughterHasSplit() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());

    // Create table then get the single region for our new table.
    Table t = createTableAndWait(tableName, HConstants.CATALOG_FAMILY); List<HRegion> regions =
      cluster.getRegions(tableName); RegionInfo hri = getAndCheckSingleTableRegion(regions);
    int tableRegionIndex = ensureTableRegionNotOnSameServerAsMeta(admin, hri);

    // Turn off balancer so it doesn't cut in and mess up our placements.
    this.admin.balancerSwitch(false, true);
    // Turn off the meta scanner so it don't remove parent on us.
    cluster.getMaster().setCatalogJanitorEnabled(false);
    try {
      // Add a bit of load up into the table so splittable.
      TESTING_UTIL.loadTable(t, HConstants.CATALOG_FAMILY);
      // Get region pre-split.
      HRegionServer server = cluster.getRegionServer(tableRegionIndex);
      printOutRegions(server, "Initial regions: ");
      // Now split.
      admin.splitRegionAsync(hri.getRegionName()).get(2, TimeUnit.MINUTES);
      // Get daughters
      List<HRegion> daughters = checkAndGetDaughters(tableName);
      // Now split one of the daughters.
      HRegion daughterRegion = daughters.get(0);
      RegionInfo daughter = daughterRegion.getRegionInfo();
      LOG.info("Daughter we are going to split: " + daughter);
      clearReferences(daughterRegion);
      LOG.info("Finished {} references={}", daughterRegion, daughterRegion.hasReferences());
      admin.splitRegionAsync(daughter.getRegionName()).get(2, TimeUnit.MINUTES);
      // Get list of daughters
      daughters = cluster.getRegions(tableName);
      for (HRegion d: daughters) {
        LOG.info("Regions before crash: " + d);
      }
      // Now crash the server
      cluster.abortRegionServer(tableRegionIndex);
      waitUntilRegionServerDead();
      awaitDaughters(tableName, daughters.size());
      // Assert daughters are online and ONLY the original daughters -- that
      // fixup didn't insert one during server shutdown recover.
      regions = cluster.getRegions(tableName);
      for (HRegion d: daughters) {
        LOG.info("Regions after crash: " + d);
      }
      if (daughters.size() != regions.size()) {
        LOG.info("Daughters=" + daughters.size() + ", regions=" + regions.size());
      }
      assertEquals(daughters.size(), regions.size());
      for (HRegion r: regions) {
        LOG.info("Regions post crash " + r + ", contains=" + daughters.contains(r));
        assertTrue("Missing region post crash " + r, daughters.contains(r));
      }
    } finally {
      LOG.info("EXITING");
      admin.balancerSwitch(true, false);
      cluster.getMaster().setCatalogJanitorEnabled(true);
      t.close();
    }
  }

  private void clearReferences(HRegion region) throws IOException {
    // Presumption.
    assertEquals(1, region.getStores().size());
    HStore store = region.getStores().get(0);
    while (store.hasReferences()) {
      while (store.storeEngine.getCompactor().isCompacting()) {
        Threads.sleep(100);
      }
      // Run new compaction. Shoudn't be any others running.
      region.compact(true);
      store.closeAndArchiveCompactedFiles();
    }
  }

  @Test
  public void testSplitShouldNotThrowNPEEvenARegionHasEmptySplitFiles() throws Exception {
    TableName userTableName = TableName.valueOf(name.getMethodName());
    TableDescriptor htd = TableDescriptorBuilder.newBuilder(userTableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("col")).build();
    admin.createTable(htd);
    Table table = TESTING_UTIL.getConnection().getTable(userTableName);
    try {
      for (int i = 0; i <= 5; i++) {
        String row = "row" + i;
        Put p = new Put(Bytes.toBytes(row));
        String val = "Val" + i;
        p.addColumn(Bytes.toBytes("col"), Bytes.toBytes("ql"), Bytes.toBytes(val));
        table.put(p);
        admin.flush(userTableName);
        Delete d = new Delete(Bytes.toBytes(row));
        // Do a normal delete
        table.delete(d);
        admin.flush(userTableName);
      }
      admin.majorCompact(userTableName);
      List<RegionInfo> regionsOfTable =
          cluster.getMaster().getAssignmentManager().getRegionStates()
          .getRegionsOfTable(userTableName);
      assertEquals(1, regionsOfTable.size());
      RegionInfo hRegionInfo = regionsOfTable.get(0);
      Put p = new Put(Bytes.toBytes("row6"));
      p.addColumn(Bytes.toBytes("col"), Bytes.toBytes("ql"), Bytes.toBytes("val"));
      table.put(p);
      p = new Put(Bytes.toBytes("row7"));
      p.addColumn(Bytes.toBytes("col"), Bytes.toBytes("ql"), Bytes.toBytes("val"));
      table.put(p);
      p = new Put(Bytes.toBytes("row8"));
      p.addColumn(Bytes.toBytes("col"), Bytes.toBytes("ql"), Bytes.toBytes("val"));
      table.put(p);
      admin.flush(userTableName);
      admin.splitRegionAsync(hRegionInfo.getRegionName(), Bytes.toBytes("row7"));
      regionsOfTable = cluster.getMaster()
          .getAssignmentManager().getRegionStates()
          .getRegionsOfTable(userTableName);

      while (regionsOfTable.size() != 2) {
        Thread.sleep(1000);
        regionsOfTable = cluster.getMaster()
            .getAssignmentManager().getRegionStates()
            .getRegionsOfTable(userTableName);
        LOG.debug("waiting 2 regions to be available, got " + regionsOfTable.size() +
          ": " + regionsOfTable);

      }
      Assert.assertEquals(2, regionsOfTable.size());

      Scan s = new Scan();
      ResultScanner scanner = table.getScanner(s);
      int mainTableCount = 0;
      for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
        mainTableCount++;
      }
      Assert.assertEquals(3, mainTableCount);
    } finally {
      table.close();
    }
  }

  /**
   * Verifies HBASE-5806. Here the case is that splitting is completed but before the CJ could
   * remove the parent region the master is killed and restarted.
   */
  @Test
  public void testMasterRestartAtRegionSplitPendingCatalogJanitor()
      throws IOException, InterruptedException, NodeExistsException, KeeperException,
      ServiceException, ExecutionException, TimeoutException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    // Create table then get the single region for our new table.
    try (Table t = createTableAndWait(tableName, HConstants.CATALOG_FAMILY)) {
      List<HRegion> regions = cluster.getRegions(tableName);
      RegionInfo hri = getAndCheckSingleTableRegion(regions);

      int tableRegionIndex = ensureTableRegionNotOnSameServerAsMeta(admin, hri);

      // Turn off balancer so it doesn't cut in and mess up our placements.
      this.admin.balancerSwitch(false, true);
      // Turn off the meta scanner so it don't remove parent on us.
      cluster.getMaster().setCatalogJanitorEnabled(false);
      // Add a bit of load up into the table so splittable.
      TESTING_UTIL.loadTable(t, HConstants.CATALOG_FAMILY, false);
      // Get region pre-split.
      HRegionServer server = cluster.getRegionServer(tableRegionIndex);
      printOutRegions(server, "Initial regions: ");
      // Call split.
      this.admin.splitRegionAsync(hri.getRegionName()).get(2, TimeUnit.MINUTES);
      List<HRegion> daughters = checkAndGetDaughters(tableName);

      // Before cleanup, get a new master.
      HMaster master = abortAndWaitForMaster();
      // Now call compact on the daughters and clean up any references.
      for (HRegion daughter : daughters) {
        clearReferences(daughter);
        assertFalse(daughter.hasReferences());
      }
      // BUT calling compact on the daughters is not enough. The CatalogJanitor looks
      // in the filesystem, and the filesystem content is not same as what the Region
      // is reading from. Compacted-away files are picked up later by the compacted
      // file discharger process. It runs infrequently. Make it run so CatalogJanitor
      // doens't find any references.
      for (RegionServerThread rst : cluster.getRegionServerThreads()) {
        boolean oldSetting = rst.getRegionServer().compactedFileDischarger.setUseExecutor(false);
        rst.getRegionServer().compactedFileDischarger.run();
        rst.getRegionServer().compactedFileDischarger.setUseExecutor(oldSetting);
      }
      cluster.getMaster().setCatalogJanitorEnabled(true);
      ProcedureTestingUtility.waitAllProcedures(cluster.getMaster().getMasterProcedureExecutor());
      LOG.info("Starting run of CatalogJanitor");
      cluster.getMaster().getCatalogJanitor().run();
      ProcedureTestingUtility.waitAllProcedures(cluster.getMaster().getMasterProcedureExecutor());
      RegionStates regionStates = master.getAssignmentManager().getRegionStates();
      ServerName regionServerOfRegion = regionStates.getRegionServerOfRegion(hri);
      assertEquals(null, regionServerOfRegion);
    } finally {
      TESTING_UTIL.getAdmin().balancerSwitch(true, false);
      cluster.getMaster().setCatalogJanitorEnabled(true);
    }
  }

  @Test
  public void testSplitWithRegionReplicas() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    TableDescriptor htd = TESTING_UTIL
      .createModifyableTableDescriptor(TableName.valueOf(name.getMethodName()),
        ColumnFamilyDescriptorBuilder.DEFAULT_MIN_VERSIONS, 3, HConstants.FOREVER,
        ColumnFamilyDescriptorBuilder.DEFAULT_KEEP_DELETED)
      .setRegionReplication(2).setCoprocessor(SlowMeCopro.class.getName()).build();
    // Create table then get the single region for our new table.
    Table t = TESTING_UTIL.createTable(htd, new byte[][]{Bytes.toBytes("cf")}, null);
    List<HRegion> oldRegions;
    do {
      oldRegions = cluster.getRegions(tableName);
      Thread.sleep(10);
    } while (oldRegions.size() != 2);
    for (HRegion h : oldRegions) LOG.debug("OLDREGION " + h.getRegionInfo());
    try {
      int regionServerIndex = cluster.getServerWith(oldRegions.get(0).getRegionInfo()
        .getRegionName());
      HRegionServer regionServer = cluster.getRegionServer(regionServerIndex);
      insertData(tableName, admin, t);
      // Turn off balancer so it doesn't cut in and mess up our placements.
      admin.balancerSwitch(false, true);
      // Turn off the meta scanner so it don't remove parent on us.
      cluster.getMaster().setCatalogJanitorEnabled(false);
      boolean tableExists = TESTING_UTIL.getAdmin().tableExists(tableName);
      assertEquals("The specified table should be present.", true, tableExists);
      final HRegion region = findSplittableRegion(oldRegions);
      regionServerIndex = cluster.getServerWith(region.getRegionInfo().getRegionName());
      regionServer = cluster.getRegionServer(regionServerIndex);
      assertTrue("not able to find a splittable region", region != null);
      try {
        requestSplitRegion(regionServer, region, Bytes.toBytes("row2"));
      } catch (IOException e) {
        e.printStackTrace();
        fail("Split execution should have succeeded with no exceptions thrown " + e);
      }
      //TESTING_UTIL.waitUntilAllRegionsAssigned(tableName);
      List<HRegion> newRegions;
      do {
        newRegions = cluster.getRegions(tableName);
        for (HRegion h : newRegions) LOG.debug("NEWREGION " + h.getRegionInfo());
        Thread.sleep(1000);
      } while ((newRegions.contains(oldRegions.get(0)) || newRegions.contains(oldRegions.get(1)))
          || newRegions.size() != 4);
      tableExists = TESTING_UTIL.getAdmin().tableExists(tableName);
      assertEquals("The specified table should be present.", true, tableExists);
      // exists works on stale and we see the put after the flush
      byte[] b1 = Bytes.toBytes("row1");
      Get g = new Get(b1);
      g.setConsistency(Consistency.STRONG);
      // The following GET will make a trip to the meta to get the new location of the 1st daughter
      // In the process it will also get the location of the replica of the daughter (initially
      // pointing to the parent's replica)
      Result r = t.get(g);
      Assert.assertFalse(r.isStale());
      LOG.info("exists stale after flush done");

      SlowMeCopro.getPrimaryCdl().set(new CountDownLatch(1));
      g = new Get(b1);
      g.setConsistency(Consistency.TIMELINE);
      // This will succeed because in the previous GET we get the location of the replica
      r = t.get(g);
      Assert.assertTrue(r.isStale());
      SlowMeCopro.getPrimaryCdl().get().countDown();
    } finally {
      SlowMeCopro.getPrimaryCdl().get().countDown();
      admin.balancerSwitch(true, false);
      cluster.getMaster().setCatalogJanitorEnabled(true);
      t.close();
    }
  }

  private void insertData(final TableName tableName, Admin admin, Table t) throws IOException {
    insertData(tableName, admin, t, 1);
  }

  private void insertData(TableName tableName, Admin admin, Table t, int i) throws IOException {
    Put p = new Put(Bytes.toBytes("row" + i));
    p.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q1"), Bytes.toBytes("1"));
    t.put(p);
    p = new Put(Bytes.toBytes("row" + (i + 1)));
    p.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q1"), Bytes.toBytes("2"));
    t.put(p);
    p = new Put(Bytes.toBytes("row" + (i + 2)));
    p.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q1"), Bytes.toBytes("3"));
    t.put(p);
    p = new Put(Bytes.toBytes("row" + (i + 3)));
    p.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q1"), Bytes.toBytes("4"));
    t.put(p);
    admin.flush(tableName);
  }

  /**
   * If a table has regions that have no store files in a region, they should split successfully
   * into two regions with no store files.
   */
  @Test
  public void testSplitRegionWithNoStoreFiles() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    // Create table then get the single region for our new table.
    createTableAndWait(tableName, HConstants.CATALOG_FAMILY);
    List<HRegion> regions = cluster.getRegions(tableName);
    RegionInfo hri = getAndCheckSingleTableRegion(regions);
    ensureTableRegionNotOnSameServerAsMeta(admin, hri);
    int regionServerIndex = cluster.getServerWith(regions.get(0).getRegionInfo()
      .getRegionName());
    HRegionServer regionServer = cluster.getRegionServer(regionServerIndex);
    // Turn off balancer so it doesn't cut in and mess up our placements.
    this.admin.balancerSwitch(false, true);
    // Turn off the meta scanner so it don't remove parent on us.
    cluster.getMaster().setCatalogJanitorEnabled(false);
    try {
      // Precondition: we created a table with no data, no store files.
      printOutRegions(regionServer, "Initial regions: ");
      Configuration conf = cluster.getConfiguration();
      HBaseFsck.debugLsr(conf, new Path("/"));
      Path rootDir = CommonFSUtils.getRootDir(conf);
      FileSystem fs = TESTING_UTIL.getDFSCluster().getFileSystem();
      Map<String, Path> storefiles =
          FSUtils.getTableStoreFilePathMap(null, fs, rootDir, tableName);
      assertEquals("Expected nothing but found " + storefiles.toString(), 0, storefiles.size());

      // find a splittable region.  Refresh the regions list
      regions = cluster.getRegions(tableName);
      final HRegion region = findSplittableRegion(regions);
      assertTrue("not able to find a splittable region", region != null);

      // Now split.
      try {
        requestSplitRegion(regionServer, region, Bytes.toBytes("row2"));
      } catch (IOException e) {
        fail("Split execution should have succeeded with no exceptions thrown");
      }

      // Postcondition: split the table with no store files into two regions, but still have no
      // store files
      List<HRegion> daughters = cluster.getRegions(tableName);
      assertEquals(2, daughters.size());

      // check dirs
      HBaseFsck.debugLsr(conf, new Path("/"));
      Map<String, Path> storefilesAfter =
          FSUtils.getTableStoreFilePathMap(null, fs, rootDir, tableName);
      assertEquals("Expected nothing but found " + storefilesAfter.toString(), 0,
          storefilesAfter.size());

      hri = region.getRegionInfo(); // split parent
      AssignmentManager am = cluster.getMaster().getAssignmentManager();
      RegionStates regionStates = am.getRegionStates();
      long start = EnvironmentEdgeManager.currentTime();
      while (!regionStates.isRegionInState(hri, State.SPLIT)) {
        LOG.debug("Waiting for SPLIT state on: " + hri);
        assertFalse("Timed out in waiting split parent to be in state SPLIT",
          EnvironmentEdgeManager.currentTime() - start > 60000);
        Thread.sleep(500);
      }
      assertTrue(regionStates.isRegionInState(daughters.get(0).getRegionInfo(), State.OPEN));
      assertTrue(regionStates.isRegionInState(daughters.get(1).getRegionInfo(), State.OPEN));

      // We should not be able to assign it again
      try {
        am.assign(hri);
      } catch (DoNotRetryIOException e) {
        // Expected
      }
      assertFalse("Split region can't be assigned", regionStates.isRegionInTransition(hri));
      assertTrue(regionStates.isRegionInState(hri, State.SPLIT));

      // We should not be able to unassign it either
      try {
        am.unassign(hri);
        fail("Should have thrown exception");
      } catch (DoNotRetryIOException e) {
        // Expected
      }
      assertFalse("Split region can't be unassigned", regionStates.isRegionInTransition(hri));
      assertTrue(regionStates.isRegionInState(hri, State.SPLIT));
    } finally {
      admin.balancerSwitch(true, false);
      cluster.getMaster().setCatalogJanitorEnabled(true);
    }
  }

  @Test
  public void testStoreFileReferenceCreationWhenSplitPolicySaysToSkipRangeCheck()
      throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try {
      byte[] cf = Bytes.toBytes("f");
      byte[] cf1 = Bytes.toBytes("i_f");
      TableDescriptor htd = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(cf))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(cf1))
        .setRegionSplitPolicyClassName(CustomSplitPolicy.class.getName()).build();
      admin.createTable(htd);
      List<HRegion> regions = awaitTableRegions(tableName);
      HRegion region = regions.get(0);
      for(int i = 3;i<9;i++) {
        Put p = new Put(Bytes.toBytes("row"+i));
        p.addColumn(cf, Bytes.toBytes("q"), Bytes.toBytes("value" + i));
        p.addColumn(cf1, Bytes.toBytes("q"), Bytes.toBytes("value" + i));
        region.put(p);
      }
      region.flush(true);
      HStore store = region.getStore(cf);
      Collection<HStoreFile> storefiles = store.getStorefiles();
      assertEquals(1, storefiles.size());
      assertFalse(region.hasReferences());
      Path referencePath =
          region.getRegionFileSystem().splitStoreFile(region.getRegionInfo(), "f",
            storefiles.iterator().next(), Bytes.toBytes("row1"), false, region.getSplitPolicy());
      assertNull(referencePath);
      referencePath =
          region.getRegionFileSystem().splitStoreFile(region.getRegionInfo(), "i_f",
            storefiles.iterator().next(), Bytes.toBytes("row1"), false, region.getSplitPolicy());
      assertNotNull(referencePath);
    } finally {
      TESTING_UTIL.deleteTable(tableName);
    }
  }

  private HRegion findSplittableRegion(final List<HRegion> regions) throws InterruptedException {
    for (int i = 0; i < 5; ++i) {
      for (HRegion r: regions) {
        if (r.isSplittable() && r.getRegionInfo().getReplicaId() == 0) {
          return(r);
        }
      }
      Thread.sleep(100);
    }
    return null;
  }

  private List<HRegion> checkAndGetDaughters(TableName tableName) throws InterruptedException {
    List<HRegion> daughters = null;
    // try up to 10s
    for (int i = 0; i < 100; i++) {
      daughters = cluster.getRegions(tableName);
      if (daughters.size() >= 2) {
        break;
      }
      Thread.sleep(100);
    }
    assertTrue(daughters.size() >= 2);
    return daughters;
  }

  private HMaster abortAndWaitForMaster() throws IOException, InterruptedException {
    cluster.abortMaster(0);
    cluster.waitOnMaster(0);
    HMaster master = cluster.startMaster().getMaster();
    cluster.waitForActiveAndReadyMaster();
    // reset the connections
    Closeables.close(admin, true);
    TESTING_UTIL.invalidateConnection();
    admin = TESTING_UTIL.getAdmin();
    return master;
  }

  /**
   * Ensure single table region is not on same server as the single hbase:meta table
   * region.
   * @return Index of the server hosting the single table region
   * @throws UnknownRegionException
   * @throws MasterNotRunningException
   * @throws org.apache.hadoop.hbase.ZooKeeperConnectionException
   * @throws InterruptedException
   */
  private int ensureTableRegionNotOnSameServerAsMeta(final Admin admin,
      final RegionInfo hri)
  throws IOException, MasterNotRunningException,
  ZooKeeperConnectionException, InterruptedException {
    // Now make sure that the table region is not on same server as that hosting
    // hbase:meta  We don't want hbase:meta replay polluting our test when we later crash
    // the table region serving server.
    int metaServerIndex = cluster.getServerWithMeta();
    HRegionServer metaRegionServer = cluster.getRegionServer(metaServerIndex);
    int tableRegionIndex = cluster.getServerWith(hri.getRegionName());
    assertTrue(tableRegionIndex != -1);
    HRegionServer tableRegionServer = cluster.getRegionServer(tableRegionIndex);
    LOG.info("MetaRegionServer=" + metaRegionServer.getServerName() +
      ", other=" + tableRegionServer.getServerName());
    if (metaRegionServer.getServerName().equals(tableRegionServer.getServerName())) {
      HRegionServer hrs = getOtherRegionServer(cluster, metaRegionServer);
      assertNotNull(hrs);
      assertNotNull(hri);
      LOG.info("Moving " + hri.getRegionNameAsString() + " from " +
        metaRegionServer.getServerName() + " to " +
        hrs.getServerName() + "; metaServerIndex=" + metaServerIndex);
      admin.move(hri.getEncodedNameAsBytes(), hrs.getServerName());
    }
    // Wait till table region is up on the server that is NOT carrying hbase:meta.
    for (int i = 0; i < 100; i++) {
      tableRegionIndex = cluster.getServerWith(hri.getRegionName());
      if (tableRegionIndex != -1 && tableRegionIndex != metaServerIndex) break;
      LOG.debug("Waiting on region move off the hbase:meta server; current index " +
        tableRegionIndex + " and metaServerIndex=" + metaServerIndex);
      Thread.sleep(100);
    }
    assertTrue("Region not moved off hbase:meta server, tableRegionIndex=" + tableRegionIndex,
      tableRegionIndex != -1 && tableRegionIndex != metaServerIndex);
    // Verify for sure table region is not on same server as hbase:meta
    tableRegionIndex = cluster.getServerWith(hri.getRegionName());
    assertTrue(tableRegionIndex != -1);
    assertNotSame(metaServerIndex, tableRegionIndex);
    return tableRegionIndex;
  }

  /**
   * Find regionserver other than the one passed.
   * Can't rely on indexes into list of regionservers since crashed servers
   * occupy an index.
   * @param cluster
   * @param notThisOne
   * @return A regionserver that is not <code>notThisOne</code> or null if none
   * found
   */
  private HRegionServer getOtherRegionServer(final SingleProcessHBaseCluster cluster,
      final HRegionServer notThisOne) {
    for (RegionServerThread rst: cluster.getRegionServerThreads()) {
      HRegionServer hrs = rst.getRegionServer();
      if (hrs.getServerName().equals(notThisOne.getServerName())) continue;
      if (hrs.isStopping() || hrs.isStopped()) continue;
      return hrs;
    }
    return null;
  }

  private void printOutRegions(final HRegionServer hrs, final String prefix)
      throws IOException {
    List<RegionInfo> regions = ProtobufUtil.getOnlineRegions(hrs.getRSRpcServices());
    for (RegionInfo region: regions) {
      LOG.info(prefix + region.getRegionNameAsString());
    }
  }

  private void waitUntilRegionServerDead() throws InterruptedException, IOException {
    // Wait until the master processes the RS shutdown
    for (int i=0; (cluster.getMaster().getClusterMetrics()
        .getLiveServerMetrics().size() > NB_SERVERS
        || cluster.getLiveRegionServerThreads().size() > NB_SERVERS) && i<100; i++) {
      LOG.info("Waiting on server to go down");
      Thread.sleep(100);
    }
    assertFalse("Waited too long for RS to die",
      cluster.getMaster().getClusterMetrics(). getLiveServerMetrics().size() > NB_SERVERS
        || cluster.getLiveRegionServerThreads().size() > NB_SERVERS);
  }

  private void awaitDaughters(TableName tableName, int numDaughters) throws InterruptedException {
    // Wait till regions are back on line again.
    for (int i = 0; cluster.getRegions(tableName).size() < numDaughters && i < 60; i++) {
      LOG.info("Waiting for repair to happen");
      Thread.sleep(1000);
    }
    if (cluster.getRegions(tableName).size() < numDaughters) {
      fail("Waiting too long for daughter regions");
    }
  }

  private List<HRegion> awaitTableRegions(final TableName tableName) throws InterruptedException {
    List<HRegion> regions = null;
    for (int i = 0; i < 100; i++) {
      regions = cluster.getRegions(tableName);
      if (regions.size() > 0) break;
      Thread.sleep(100);
    }
    return regions;
  }

  private Table createTableAndWait(TableName tableName, byte[] cf) throws IOException,
      InterruptedException {
    Table t = TESTING_UTIL.createTable(tableName, cf);
    awaitTableRegions(tableName);
    assertTrue("Table not online: " + tableName,
      cluster.getRegions(tableName).size() != 0);
    return t;
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
      if (enabled.get() && req.getTransition(0).getTransitionCode().equals(
          TransitionCode.READY_TO_SPLIT) && !resp.hasErrorMessage()) {
        RegionStates regionStates = myMaster.getAssignmentManager().getRegionStates();
        for (RegionStateNode regionState:
          regionStates.getRegionsInTransition()) {
          /* TODO!!!!
          // Find the merging_new region and remove it
          if (regionState.isSplittingNew()) {
            regionStates.deleteRegion(regionState.getRegion());
          }
          */
        }
      }
      return resp;
    }
  }

  static class CustomSplitPolicy extends IncreasingToUpperBoundRegionSplitPolicy {

    @Override
    protected boolean shouldSplit() {
      return true;
    }

    @Override
    public boolean skipStoreFileRangeCheck(String familyName) {
      if(familyName.startsWith("i_")) {
        return true;
      } else {
        return false;
      }
    }
  }
}

