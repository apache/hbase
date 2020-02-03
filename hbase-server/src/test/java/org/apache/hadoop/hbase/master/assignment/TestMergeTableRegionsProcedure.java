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
package org.apache.hadoop.hbase.master.assignment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureConstants;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureMetrics;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MasterTests.class, LargeTests.class})
public class TestMergeTableRegionsProcedure {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMergeTableRegionsProcedure.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestMergeTableRegionsProcedure.class);
  @Rule
  public final TestName name = new TestName();

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final int initialRegionCount = 4;
  private final static byte[] FAMILY = Bytes.toBytes("FAMILY");
  private static Admin admin;

  private ProcedureMetrics mergeProcMetrics;
  private ProcedureMetrics assignProcMetrics;
  private ProcedureMetrics unassignProcMetrics;
  private long mergeSubmittedCount = 0;
  private long mergeFailedCount = 0;
  private long assignSubmittedCount = 0;
  private long assignFailedCount = 0;
  private long unassignSubmittedCount = 0;
  private long unassignFailedCount = 0;

  private static void setupConf(Configuration conf) {
    // Reduce the maximum attempts to speed up the test
    conf.setInt("hbase.assignment.maximum.attempts", 3);
    conf.setInt("hbase.master.maximum.ping.server.attempts", 3);
    conf.setInt("hbase.master.ping.server.retry.sleep.interval", 1);
    conf.setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, 1);
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(1);
    admin = UTIL.getAdmin();
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void setup() throws Exception {
    resetProcExecutorTestingKillFlag();
    MasterProcedureTestingUtility.generateNonceGroup(UTIL.getHBaseCluster().getMaster());
    MasterProcedureTestingUtility.generateNonce(UTIL.getHBaseCluster().getMaster());
    // Turn off balancer so it doesn't cut in and mess up our placements.
    admin.balancerSwitch(false, true);
    // Turn off the meta scanner so it don't remove parent on us.
    UTIL.getHBaseCluster().getMaster().setCatalogJanitorEnabled(false);
    resetProcExecutorTestingKillFlag();
    AssignmentManager am = UTIL.getHBaseCluster().getMaster().getAssignmentManager();
    mergeProcMetrics = am.getAssignmentManagerMetrics().getMergeProcMetrics();
    assignProcMetrics = am.getAssignmentManagerMetrics().getAssignProcMetrics();
    unassignProcMetrics = am.getAssignmentManagerMetrics().getUnassignProcMetrics();
  }

  @After
  public void tearDown() throws Exception {
    resetProcExecutorTestingKillFlag();
    for (TableDescriptor htd: admin.listTableDescriptors()) {
      LOG.info("Tear down, remove table=" + htd.getTableName());
      UTIL.deleteTable(htd.getTableName());
    }
  }

  private void resetProcExecutorTestingKillFlag() {
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, false);
    assertTrue("expected executor to be running", procExec.isRunning());
  }

  private int loadARowPerRegion(final Table t, List<RegionInfo> ris)
      throws IOException {
    List<Put> puts = new ArrayList<>();
    for (RegionInfo ri: ris) {
      Put put = new Put(ri.getStartKey() == null || ri.getStartKey().length == 0?
          new byte [] {'a'}: ri.getStartKey());
      put.addColumn(HConstants.CATALOG_FAMILY, HConstants.CATALOG_FAMILY,
          HConstants.CATALOG_FAMILY);
      puts.add(put);
    }
    t.put(puts);
    return puts.size();
  }


  /**
   * This tests two region merges
   */
  @Test
  public void testMergeTwoRegions() throws Exception {
    final TableName tableName = TableName.valueOf(this.name.getMethodName());
    UTIL.createTable(tableName, new byte[][]{HConstants.CATALOG_FAMILY},
        new byte[][]{new byte[]{'b'}, new byte[]{'c'}, new byte[]{'d'}, new byte[]{'e'}});
    testMerge(tableName, 2);
  }

  private void testMerge(TableName tableName, int mergeCount) throws IOException {
    List<RegionInfo> ris = MetaTableAccessor.getTableRegions(UTIL.getConnection(), tableName);
    int originalRegionCount = ris.size();
    assertTrue(originalRegionCount > mergeCount);
    RegionInfo[] regionsToMerge = ris.subList(0, mergeCount).toArray(new RegionInfo [] {});
    int countOfRowsLoaded = 0;
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      countOfRowsLoaded = loadARowPerRegion(table, ris);
    }
    assertEquals(countOfRowsLoaded, UTIL.countRows(tableName));

    // collect AM metrics before test
    collectAssignmentManagerMetrics();
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    MergeTableRegionsProcedure proc =
        new MergeTableRegionsProcedure(procExec.getEnvironment(), regionsToMerge, true);
    long procId = procExec.submitProcedure(proc);
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);
    MetaTableAccessor.fullScanMetaAndPrint(UTIL.getConnection());
    assertEquals(originalRegionCount - mergeCount + 1,
        MetaTableAccessor.getTableRegions(UTIL.getConnection(), tableName).size());

    assertEquals(mergeSubmittedCount + 1, mergeProcMetrics.getSubmittedCounter().getCount());
    assertEquals(mergeFailedCount, mergeProcMetrics.getFailedCounter().getCount());
    assertEquals(assignSubmittedCount + 1, assignProcMetrics.getSubmittedCounter().getCount());
    assertEquals(assignFailedCount, assignProcMetrics.getFailedCounter().getCount());
    assertEquals(unassignSubmittedCount + mergeCount,
        unassignProcMetrics.getSubmittedCounter().getCount());
    assertEquals(unassignFailedCount, unassignProcMetrics.getFailedCounter().getCount());

    // Need to get the references cleaned out. Close of region will move them
    // to archive so disable and reopen just to get rid of references to later
    // when the catalogjanitor runs, it can do merged region cleanup.
    admin.disableTable(tableName);
    admin.enableTable(tableName);

    // Can I purge the merged regions from hbase:meta? Check that all went
    // well by looking at the merged row up in hbase:meta. It should have no
    // more mention of the merged regions; they are purged as last step in
    // the merged regions cleanup.
    UTIL.getHBaseCluster().getMaster().setCatalogJanitorEnabled(true);
    UTIL.getHBaseCluster().getMaster().getCatalogJanitor().triggerNow();
    byte [] mergedRegion = proc.getMergedRegion().getRegionName();
    while (ris != null && ris.get(0) != null && ris.get(1) != null) {
      ris = MetaTableAccessor.getMergeRegions(UTIL.getConnection(), mergedRegion);
      LOG.info("{} {}", Bytes.toStringBinary(mergedRegion), ris);
      Threads.sleep(1000);
    }
    assertEquals(countOfRowsLoaded, UTIL.countRows(tableName));
  }

  /**
   * This tests ten region merges in one go.
   */
  @Test
  public void testMergeTenRegions() throws Exception {
    final TableName tableName = TableName.valueOf(this.name.getMethodName());
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    UTIL.createMultiRegionTable(tableName, HConstants.CATALOG_FAMILY);
    testMerge(tableName, 10);
  }

  /**
   * This tests two concurrent region merges
   */
  @Test
  public void testMergeRegionsConcurrently() throws Exception {
    final TableName tableName = TableName.valueOf("testMergeRegionsConcurrently");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    List<RegionInfo> tableRegions = createTable(tableName);

    RegionInfo[] regionsToMerge1 = new RegionInfo[2];
    RegionInfo[] regionsToMerge2 = new RegionInfo[2];
    regionsToMerge1[0] = tableRegions.get(0);
    regionsToMerge1[1] = tableRegions.get(1);
    regionsToMerge2[0] = tableRegions.get(2);
    regionsToMerge2[1] = tableRegions.get(3);

    // collect AM metrics before test
    collectAssignmentManagerMetrics();

    long procId1 = procExec.submitProcedure(new MergeTableRegionsProcedure(
      procExec.getEnvironment(), regionsToMerge1, true));
    long procId2 = procExec.submitProcedure(new MergeTableRegionsProcedure(
      procExec.getEnvironment(), regionsToMerge2, true));
    ProcedureTestingUtility.waitProcedure(procExec, procId1);
    ProcedureTestingUtility.waitProcedure(procExec, procId2);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId1);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId2);
    assertRegionCount(tableName, initialRegionCount - 2);

    assertEquals(mergeSubmittedCount + 2, mergeProcMetrics.getSubmittedCounter().getCount());
    assertEquals(mergeFailedCount, mergeProcMetrics.getFailedCounter().getCount());
    assertEquals(assignSubmittedCount + 2, assignProcMetrics.getSubmittedCounter().getCount());
    assertEquals(assignFailedCount, assignProcMetrics.getFailedCounter().getCount());
    assertEquals(unassignSubmittedCount + 4, unassignProcMetrics.getSubmittedCounter().getCount());
    assertEquals(unassignFailedCount, unassignProcMetrics.getFailedCounter().getCount());
  }

  @Test
  public void testRecoveryAndDoubleExecution() throws Exception {
    final TableName tableName = TableName.valueOf("testRecoveryAndDoubleExecution");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    List<RegionInfo> tableRegions = createTable(tableName);

    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillIfHasParent(procExec, false);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    RegionInfo[] regionsToMerge = new RegionInfo[2];
    regionsToMerge[0] = tableRegions.get(0);
    regionsToMerge[1] = tableRegions.get(1);

    long procId = procExec.submitProcedure(
      new MergeTableRegionsProcedure(procExec.getEnvironment(), regionsToMerge, true));

    // Restart the executor and execute the step twice
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);

    assertRegionCount(tableName, initialRegionCount - 1);
  }

  @Test
  public void testRollbackAndDoubleExecution() throws Exception {
    final TableName tableName = TableName.valueOf("testRollbackAndDoubleExecution");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    List<RegionInfo> tableRegions = createTable(tableName);

    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    RegionInfo[] regionsToMerge = new RegionInfo[2];
    regionsToMerge[0] = tableRegions.get(0);
    regionsToMerge[1] = tableRegions.get(1);

    long procId = procExec.submitProcedure(
      new MergeTableRegionsProcedure(procExec.getEnvironment(), regionsToMerge, true));

    // Failing before MERGE_TABLE_REGIONS_UPDATE_META we should trigger the rollback
    // NOTE: the 8 (number of MERGE_TABLE_REGIONS_UPDATE_META step) is
    // hardcoded, so you have to look at this test at least once when you add a new step.
    int lastStep = 8;
    MasterProcedureTestingUtility.testRollbackAndDoubleExecution(procExec, procId, lastStep, true);
    assertEquals(initialRegionCount, UTIL.getAdmin().getRegions(tableName).size());
    UTIL.waitUntilAllRegionsAssigned(tableName);
    List<HRegion> regions = UTIL.getMiniHBaseCluster().getRegions(tableName);
    assertEquals(initialRegionCount, regions.size());
  }

  @Test
  public void testMergeWithoutPONR() throws Exception {
    final TableName tableName = TableName.valueOf("testMergeWithoutPONR");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    List<RegionInfo> tableRegions = createTable(tableName);

    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    RegionInfo[] regionsToMerge = new RegionInfo[2];
    regionsToMerge[0] = tableRegions.get(0);
    regionsToMerge[1] = tableRegions.get(1);

    long procId = procExec.submitProcedure(
      new MergeTableRegionsProcedure(procExec.getEnvironment(), regionsToMerge, true));

    // Execute until step 9 of split procedure
    // NOTE: step 9 is after step MERGE_TABLE_REGIONS_UPDATE_META
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(procExec, procId, 9, false);

    // Unset Toggle Kill and make ProcExec work correctly
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, false);
    MasterProcedureTestingUtility.restartMasterProcedureExecutor(procExec);
    ProcedureTestingUtility.waitProcedure(procExec, procId);

    assertRegionCount(tableName, initialRegionCount - 1);
  }

  private List<RegionInfo> createTable(final TableName tableName) throws Exception {
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build();
    byte[][] splitRows = new byte[initialRegionCount - 1][];
    for (int i = 0; i < splitRows.length; ++i) {
      splitRows[i] = Bytes.toBytes(String.format("%d", i));
    }
    admin.createTable(desc, splitRows);
    return assertRegionCount(tableName, initialRegionCount);
  }

  public List<RegionInfo> assertRegionCount(final TableName tableName, final int nregions)
      throws Exception {
    UTIL.waitUntilNoRegionsInTransition();
    List<RegionInfo> tableRegions = admin.getRegions(tableName);
    assertEquals(nregions, tableRegions.size());
    return tableRegions;
  }

  private ProcedureExecutor<MasterProcedureEnv> getMasterProcedureExecutor() {
    return UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
  }

  private void collectAssignmentManagerMetrics() {
    mergeSubmittedCount = mergeProcMetrics.getSubmittedCounter().getCount();
    mergeFailedCount = mergeProcMetrics.getFailedCounter().getCount();

    assignSubmittedCount = assignProcMetrics.getSubmittedCounter().getCount();
    assignFailedCount = assignProcMetrics.getFailedCounter().getCount();
    unassignSubmittedCount = unassignProcMetrics.getSubmittedCounter().getCount();
    unassignFailedCount = unassignProcMetrics.getFailedCounter().getCount();
  }
}
