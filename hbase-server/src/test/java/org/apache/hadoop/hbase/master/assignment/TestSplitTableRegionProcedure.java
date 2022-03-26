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
package org.apache.hadoop.hbase.master.assignment;

import static org.apache.hadoop.hbase.master.assignment.AssignmentTestingUtil.insertData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureConstants;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureMetrics;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
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

@Category({MasterTests.class, MediumTests.class})
public class TestSplitTableRegionProcedure {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSplitTableRegionProcedure.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestSplitTableRegionProcedure.class);

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static String columnFamilyName1 = "cf1";
  private static String columnFamilyName2 = "cf2";

  private static final int startRowNum = 11;
  private static final int rowCount = 60;

  private AssignmentManager am;

  private ProcedureMetrics splitProcMetrics;
  private ProcedureMetrics assignProcMetrics;
  private ProcedureMetrics unassignProcMetrics;

  private long splitSubmittedCount = 0;
  private long splitFailedCount = 0;
  private long assignSubmittedCount = 0;
  private long assignFailedCount = 0;
  private long unassignSubmittedCount = 0;
  private long unassignFailedCount = 0;

  @Rule
  public TestName name = new TestName();

  private static void setupConf(Configuration conf) {
    conf.setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, 1);
    conf.setLong(HConstants.MAJOR_COMPACTION_PERIOD, 0);
    conf.set("hbase.coprocessor.region.classes",
      RegionServerHostingReplicaSlowOpenCopro.class.getName());
    conf.setInt("hbase.client.sync.wait.timeout.msec", 1500);
  }

  /**
   * This copro is used to slow down opening of the replica regions.
   */
  public static class RegionServerHostingReplicaSlowOpenCopro
    implements RegionCoprocessor, RegionObserver {
    static int countForReplica = 0;
    static boolean slowDownReplicaOpen = false;

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void preOpen(ObserverContext<RegionCoprocessorEnvironment> c) throws IOException {
      int replicaId = c.getEnvironment().getRegion().getRegionInfo().getReplicaId();
      if ((replicaId != RegionInfo.DEFAULT_REPLICA_ID) && (countForReplica == 0)) {
        countForReplica ++;
        while (slowDownReplicaOpen) {
          LOG.info("Slow down replica region open a bit");
          try {
            Thread.sleep(100);
          } catch (InterruptedException ie) {
            // Ingore
          }
        }
      }
    }
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(3);
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    try {
      UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      LOG.warn("failure shutting down cluster", e);
    }
  }

  @Before
  public void setup() throws Exception {
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(getMasterProcedureExecutor(), false);

    // Turn off balancer so it doesn't cut in and mess up our placements.
    UTIL.getAdmin().balancerSwitch(false, true);
    // Turn off the meta scanner so it don't remove parent on us.
    UTIL.getHBaseCluster().getMaster().setCatalogJanitorEnabled(false);
    am = UTIL.getHBaseCluster().getMaster().getAssignmentManager();
    splitProcMetrics = am.getAssignmentManagerMetrics().getSplitProcMetrics();
    assignProcMetrics = am.getAssignmentManagerMetrics().getAssignProcMetrics();
    unassignProcMetrics = am.getAssignmentManagerMetrics().getUnassignProcMetrics();
  }

  @After
  public void tearDown() throws Exception {
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(getMasterProcedureExecutor(), false);
    for (TableDescriptor htd : UTIL.getAdmin().listTableDescriptors()) {
      UTIL.deleteTable(htd.getTableName());
    }
  }


  @Test
  public void testRollbackForSplitTableRegionWithReplica() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();


    RegionServerHostingReplicaSlowOpenCopro.slowDownReplicaOpen = true;
    RegionInfo [] regions = MasterProcedureTestingUtility.createTable(
      procExec, tableName, null, columnFamilyName1);

    try {
      HBaseTestingUtility.setReplicas(UTIL.getAdmin(), tableName, 2);
    } catch (IOException ioe) {

    }

    // wait until the primary region is online.
    HBaseTestingUtility.await(2000, () -> {
      try {
        AssignmentManager am = UTIL.getHBaseCluster().getMaster().getAssignmentManager();
        if (am == null) return false;
        if (am.getRegionStates().getRegionState(regions[0]).isOpened()) {
          return true;
        }
        return false;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    // Split region of the table, it will fail and rollback as replica parent region
    // is still at OPENING state.
    long procId = procExec.submitProcedure(new SplitTableRegionProcedure(
      procExec.getEnvironment(), regions[0], HConstants.CATALOG_FAMILY));
    // Wait for the completion.
    ProcedureTestingUtility.waitProcedure(procExec, procId);

    // Let replica parent region open.
    RegionServerHostingReplicaSlowOpenCopro.slowDownReplicaOpen = false;

    // wait until the replica region is online.
    HBaseTestingUtility.await(2000, () -> {
      try {
        AssignmentManager am = UTIL.getHBaseCluster().getMaster().getAssignmentManager();
        if (am == null) return false;
        RegionInfo replicaRegion = RegionReplicaUtil.getRegionInfoForReplica(regions[0], 1);
        if (am.getRegionStates().getRegionState(replicaRegion).isOpened()) {
          return true;
        }
        return false;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    ProcedureTestingUtility.assertProcFailed(procExec, procId);
    // There should not be any active OpenRegionProcedure
    procExec.getActiveProceduresNoCopy().forEach(p -> assertFalse(p instanceof OpenRegionProcedure));
  }

  @Test
  public void testSplitTableRegion() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    RegionInfo [] regions = MasterProcedureTestingUtility.createTable(
      procExec, tableName, null, columnFamilyName1, columnFamilyName2);
    insertData(UTIL, tableName, rowCount, startRowNum, columnFamilyName1, columnFamilyName2);
    int splitRowNum = startRowNum + rowCount / 2;
    byte[] splitKey = Bytes.toBytes("" + splitRowNum);

    assertTrue("not able to find a splittable region", regions != null);
    assertTrue("not able to find a splittable region", regions.length == 1);

    // collect AM metrics before test
    collectAssignmentManagerMetrics();

    // Split region of the table
    long procId = procExec.submitProcedure(
      new SplitTableRegionProcedure(procExec.getEnvironment(), regions[0], splitKey));
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);

    verify(tableName, splitRowNum);

    assertEquals(splitSubmittedCount + 1, splitProcMetrics.getSubmittedCounter().getCount());
    assertEquals(splitFailedCount, splitProcMetrics.getFailedCounter().getCount());
    assertEquals(assignSubmittedCount + 2, assignProcMetrics.getSubmittedCounter().getCount());
    assertEquals(assignFailedCount, assignProcMetrics.getFailedCounter().getCount());
    assertEquals(unassignSubmittedCount + 1, unassignProcMetrics.getSubmittedCounter().getCount());
    assertEquals(unassignFailedCount, unassignProcMetrics.getFailedCounter().getCount());
}

  @Test
  public void testSplitTableRegionNoStoreFile() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    RegionInfo [] regions = MasterProcedureTestingUtility.createTable(
      procExec, tableName, null, columnFamilyName1, columnFamilyName2);
    int splitRowNum = startRowNum + rowCount / 2;
    byte[] splitKey = Bytes.toBytes("" + splitRowNum);

    assertTrue("not able to find a splittable region", regions != null);
    assertTrue("not able to find a splittable region", regions.length == 1);

    // collect AM metrics before test
    collectAssignmentManagerMetrics();

    // Split region of the table
    long procId = procExec.submitProcedure(
      new SplitTableRegionProcedure(procExec.getEnvironment(), regions[0], splitKey));
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);

    assertTrue(UTIL.getMiniHBaseCluster().getRegions(tableName).size() == 2);
    assertTrue(UTIL.countRows(tableName) == 0);

    assertEquals(splitSubmittedCount + 1, splitProcMetrics.getSubmittedCounter().getCount());
    assertEquals(splitFailedCount, splitProcMetrics.getFailedCounter().getCount());
  }

  @Test
  public void testSplitTableRegionUnevenDaughter() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    RegionInfo [] regions = MasterProcedureTestingUtility.createTable(
      procExec, tableName, null, columnFamilyName1, columnFamilyName2);
    insertData(UTIL, tableName, rowCount, startRowNum, columnFamilyName1, columnFamilyName2);
    // Split to two daughters with one of them only has 1 row
    int splitRowNum = startRowNum + rowCount / 4;
    byte[] splitKey = Bytes.toBytes("" + splitRowNum);

    assertTrue("not able to find a splittable region", regions != null);
    assertTrue("not able to find a splittable region", regions.length == 1);

    // collect AM metrics before test
    collectAssignmentManagerMetrics();

    // Split region of the table
    long procId = procExec.submitProcedure(
      new SplitTableRegionProcedure(procExec.getEnvironment(), regions[0], splitKey));
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);

    verify(tableName, splitRowNum);

    assertEquals(splitSubmittedCount + 1, splitProcMetrics.getSubmittedCounter().getCount());
    assertEquals(splitFailedCount, splitProcMetrics.getFailedCounter().getCount());
  }

  @Test
  public void testSplitTableRegionEmptyDaughter() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    RegionInfo [] regions = MasterProcedureTestingUtility.createTable(
      procExec, tableName, null, columnFamilyName1, columnFamilyName2);
    insertData(UTIL, tableName, rowCount, startRowNum, columnFamilyName1, columnFamilyName2);
    // Split to two daughters with one of them only has 1 row
    int splitRowNum = startRowNum + rowCount;
    byte[] splitKey = Bytes.toBytes("" + splitRowNum);

    assertTrue("not able to find a splittable region", regions != null);
    assertTrue("not able to find a splittable region", regions.length == 1);

    // collect AM metrics before test
    collectAssignmentManagerMetrics();

    // Split region of the table
    long procId = procExec.submitProcedure(
      new SplitTableRegionProcedure(procExec.getEnvironment(), regions[0], splitKey));
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);

    // Make sure one daughter has 0 rows.
    List<HRegion> daughters = UTIL.getMiniHBaseCluster().getRegions(tableName);
    assertTrue(daughters.size() == 2);
    assertTrue(UTIL.countRows(tableName) == rowCount);
    assertTrue(UTIL.countRows(daughters.get(0)) == 0 || UTIL.countRows(daughters.get(1)) == 0);

    assertEquals(splitSubmittedCount + 1,
        splitProcMetrics.getSubmittedCounter().getCount());
    assertEquals(splitFailedCount, splitProcMetrics.getFailedCounter().getCount());
  }

  @Test
  public void testSplitTableRegionDeletedRowsDaughter() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    RegionInfo [] regions = MasterProcedureTestingUtility.createTable(
      procExec, tableName, null, columnFamilyName1, columnFamilyName2);
    insertData(UTIL, tableName, rowCount, startRowNum, columnFamilyName1, columnFamilyName2);
    // Split to two daughters with one of them only has 1 row
    int splitRowNum = rowCount;
    deleteData(tableName, splitRowNum);
    byte[] splitKey = Bytes.toBytes("" + splitRowNum);

    assertTrue("not able to find a splittable region", regions != null);
    assertTrue("not able to find a splittable region", regions.length == 1);

    // collect AM metrics before test
    collectAssignmentManagerMetrics();

    // Split region of the table
    long procId = procExec.submitProcedure(
      new SplitTableRegionProcedure(procExec.getEnvironment(), regions[0], splitKey));
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);

    UTIL.getAdmin().majorCompact(tableName);
    // waiting for the major compaction to complete
    UTIL.waitFor(6000, new Waiter.Predicate<IOException>() {
      @Override
      public boolean evaluate() throws IOException {
        return UTIL.getAdmin().getCompactionState(tableName) == CompactionState.NONE;
      }
    });

    // Make sure one daughter has 0 rows.
    List<HRegion> daughters = UTIL.getMiniHBaseCluster().getRegions(tableName);
    assertTrue(daughters.size() == 2);
    final int currentRowCount = splitRowNum - startRowNum;
    assertTrue(UTIL.countRows(tableName) == currentRowCount);
    assertTrue(UTIL.countRows(daughters.get(0)) == 0 || UTIL.countRows(daughters.get(1)) == 0);

    assertEquals(splitSubmittedCount + 1, splitProcMetrics.getSubmittedCounter().getCount());
    assertEquals(splitFailedCount, splitProcMetrics.getFailedCounter().getCount());
  }

  @Test
  public void testInvalidSplitKey() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    RegionInfo [] regions = MasterProcedureTestingUtility.createTable(
      procExec, tableName, null, columnFamilyName1, columnFamilyName2);
    insertData(UTIL, tableName, rowCount, startRowNum, columnFamilyName1, columnFamilyName2);

    assertTrue("not able to find a splittable region", regions != null);
    assertTrue("not able to find a splittable region", regions.length == 1);

    // collect AM metrics before test
    collectAssignmentManagerMetrics();

    // Split region of the table with null split key
    try {
      long procId1 = procExec.submitProcedure(
        new SplitTableRegionProcedure(procExec.getEnvironment(), regions[0], null));
      ProcedureTestingUtility.waitProcedure(procExec, procId1);
      fail("unexpected procedure start with invalid split-key");
    } catch (DoNotRetryIOException e) {
      LOG.debug("Expected Split procedure construction failure: " + e.getMessage());
    }

    assertEquals(splitSubmittedCount, splitProcMetrics.getSubmittedCounter().getCount());
    assertEquals(splitFailedCount, splitProcMetrics.getFailedCounter().getCount());
  }

  @Test
  public void testRollbackAndDoubleExecution() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    RegionInfo[] regions = MasterProcedureTestingUtility.createTable(
      procExec, tableName, null, columnFamilyName1, columnFamilyName2);
    insertData(UTIL, tableName, rowCount, startRowNum, columnFamilyName1, columnFamilyName2);
    int splitRowNum = startRowNum + rowCount / 2;
    byte[] splitKey = Bytes.toBytes("" + splitRowNum);

    assertTrue("not able to find a splittable region", regions != null);
    assertTrue("not able to find a splittable region", regions.length == 1);
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // collect AM metrics before test
    collectAssignmentManagerMetrics();

    // Split region of the table
    long procId = procExec.submitProcedure(
      new SplitTableRegionProcedure(procExec.getEnvironment(), regions[0], splitKey));

    // Failing before SPLIT_TABLE_REGION_UPDATE_META we should trigger the
    // rollback
    // NOTE: the 7 (number of SPLIT_TABLE_REGION_UPDATE_META step) is
    // hardcoded, so you have to look at this test at least once when you add a new step.
    int lastStep = 7;
    MasterProcedureTestingUtility.testRollbackAndDoubleExecution(procExec, procId, lastStep,
        true);
    // check that we have only 1 region
    assertEquals(1, UTIL.getAdmin().getRegions(tableName).size());
    UTIL.waitUntilAllRegionsAssigned(tableName);
    List<HRegion> newRegions = UTIL.getMiniHBaseCluster().getRegions(tableName);
    assertEquals(1, newRegions.size());
    verifyData(newRegions.get(0), startRowNum, rowCount,
    Bytes.toBytes(columnFamilyName1), Bytes.toBytes(columnFamilyName2));

    assertEquals(splitSubmittedCount + 1, splitProcMetrics.getSubmittedCounter().getCount());
    assertEquals(splitFailedCount + 1, splitProcMetrics.getFailedCounter().getCount());
  }

  @Test
  public void testRecoveryAndDoubleExecution() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    RegionInfo [] regions = MasterProcedureTestingUtility.createTable(
      procExec, tableName, null, columnFamilyName1, columnFamilyName2);
    insertData(UTIL, tableName, rowCount, startRowNum, columnFamilyName1, columnFamilyName2);
    int splitRowNum = startRowNum + rowCount / 2;
    byte[] splitKey = Bytes.toBytes("" + splitRowNum);

    assertTrue("not able to find a splittable region", regions != null);
    assertTrue("not able to find a splittable region", regions.length == 1);
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillIfHasParent(procExec, false);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // collect AM metrics before test
    collectAssignmentManagerMetrics();

    // Split region of the table
    long procId = procExec.submitProcedure(
      new SplitTableRegionProcedure(procExec.getEnvironment(), regions[0], splitKey));

    // Restart the executor and execute the step twice
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);

    verify(tableName, splitRowNum);

    assertEquals(splitSubmittedCount + 1, splitProcMetrics.getSubmittedCounter().getCount());
    assertEquals(splitFailedCount, splitProcMetrics.getFailedCounter().getCount());
  }

  @Test
  public void testSplitWithoutPONR() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    RegionInfo [] regions = MasterProcedureTestingUtility.createTable(
        procExec, tableName, null, columnFamilyName1, columnFamilyName2);
    insertData(UTIL, tableName, rowCount, startRowNum, columnFamilyName1, columnFamilyName2);
    int splitRowNum = startRowNum + rowCount / 2;
    byte[] splitKey = Bytes.toBytes("" + splitRowNum);

    assertTrue("not able to find a splittable region", regions != null);
    assertTrue("not able to find a splittable region", regions.length == 1);
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Split region of the table
    long procId = procExec.submitProcedure(
        new SplitTableRegionProcedure(procExec.getEnvironment(), regions[0], splitKey));

    // Execute until step 7 of split procedure
    // NOTE: the 7 (number after SPLIT_TABLE_REGION_UPDATE_META step)
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(procExec, procId, 7, false);

    // Unset Toggle Kill and make ProcExec work correctly
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, false);
    MasterProcedureTestingUtility.restartMasterProcedureExecutor(procExec);
    ProcedureTestingUtility.waitProcedure(procExec, procId);

    // Even split failed after step 4, it should still works fine
    verify(tableName, splitRowNum);
  }

  private void deleteData(
      final TableName tableName,
      final int startDeleteRowNum) throws IOException, InterruptedException {
    Table t = UTIL.getConnection().getTable(tableName);
    final int numRows = rowCount + startRowNum - startDeleteRowNum;
    Delete d;
    for (int i= startDeleteRowNum; i <= numRows + startDeleteRowNum; i++) {
      d = new Delete(Bytes.toBytes("" + i));
      t.delete(d);
      if (i % 5 == 0) {
        UTIL.getAdmin().flush(tableName);
      }
    }
  }

  private void verify(final TableName tableName, final int splitRowNum) throws IOException {
    List<HRegion> daughters = UTIL.getMiniHBaseCluster().getRegions(tableName);
    assertTrue(daughters.size() == 2);
    LOG.info("Row Count = " + UTIL.countRows(tableName));
    assertTrue(UTIL.countRows(tableName) == rowCount);
    int startRow;
    int numRows;
    for (int i = 0; i < daughters.size(); i++) {
      if (Bytes.compareTo(
        daughters.get(i).getRegionInfo().getStartKey(), HConstants.EMPTY_BYTE_ARRAY) == 0) {
        startRow = startRowNum; // first region
        numRows = splitRowNum - startRowNum;
      } else {
        startRow = splitRowNum;
        numRows = rowCount + startRowNum - splitRowNum;
      }
      verifyData(
        daughters.get(i),
        startRow,
        numRows,
        Bytes.toBytes(columnFamilyName1),
        Bytes.toBytes(columnFamilyName2));
    }
  }

  private void verifyData(
      final HRegion newReg,
      final int startRow,
      final int numRows,
      final byte[]... families)
      throws IOException {
    for (int i = startRow; i < startRow + numRows; i++) {
      byte[] row = Bytes.toBytes("" + i);
      Get get = new Get(row);
      Result result = newReg.get(get);
      Cell[] raw = result.rawCells();
      assertEquals(families.length, result.size());
      for (int j = 0; j < families.length; j++) {
        assertTrue(CellUtil.matchingRows(raw[j], row));
        assertTrue(CellUtil.matchingFamily(raw[j], families[j]));
      }
    }
  }

  private ProcedureExecutor<MasterProcedureEnv> getMasterProcedureExecutor() {
    return UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
  }

  private void collectAssignmentManagerMetrics() {
    splitSubmittedCount = splitProcMetrics.getSubmittedCounter().getCount();
    splitFailedCount = splitProcMetrics.getFailedCounter().getCount();
    assignSubmittedCount = assignProcMetrics.getSubmittedCounter().getCount();
    assignFailedCount = assignProcMetrics.getFailedCounter().getCount();
    unassignSubmittedCount = unassignProcMetrics.getSubmittedCounter().getCount();
    unassignFailedCount = unassignProcMetrics.getFailedCounter().getCount();
  }
}
