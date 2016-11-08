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

package org.apache.hadoop.hbase.master.procedure;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.SplitTableRegionState;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MasterTests.class, MediumTests.class})
public class TestSplitTableRegionProcedure {
  private static final Log LOG = LogFactory.getLog(TestSplitTableRegionProcedure.class);

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static long nonceGroup = HConstants.NO_NONCE;
  private static long nonce = HConstants.NO_NONCE;

  private static String ColumnFamilyName1 = "cf1";
  private static String ColumnFamilyName2 = "cf2";

  private static final int startRowNum = 11;
  private static final int rowCount = 60;

  private static void setupConf(Configuration conf) {
    conf.setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, 1);
    conf.setLong(HConstants.MAJOR_COMPACTION_PERIOD, 0);
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
    nonceGroup =
        MasterProcedureTestingUtility.generateNonceGroup(UTIL.getHBaseCluster().getMaster());
    nonce = MasterProcedureTestingUtility.generateNonce(UTIL.getHBaseCluster().getMaster());

    // Turn off balancer so it doesn't cut in and mess up our placements.
    UTIL.getHBaseAdmin().setBalancerRunning(false, true);
    // Turn off the meta scanner so it don't remove parent on us.
    UTIL.getHBaseCluster().getMaster().setCatalogJanitorEnabled(false);
  }

  @After
  public void tearDown() throws Exception {
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(getMasterProcedureExecutor(), false);
    for (HTableDescriptor htd: UTIL.getHBaseAdmin().listTables()) {
      LOG.info("Tear down, remove table=" + htd.getTableName());
      UTIL.deleteTable(htd.getTableName());
    }
  }

  @Test(timeout=60000)
  public void testSplitTableRegion() throws Exception {
    final TableName tableName = TableName.valueOf("testSplitTableRegion");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    HRegionInfo [] regions = MasterProcedureTestingUtility.createTable(
      procExec, tableName, null, ColumnFamilyName1, ColumnFamilyName2);
    insertData(tableName);
    int splitRowNum = startRowNum + rowCount / 2;
    byte[] splitKey = Bytes.toBytes("" + splitRowNum);

    assertTrue("not able to find a splittable region", regions != null);
    assertTrue("not able to find a splittable region", regions.length == 1);

    // Split region of the table
    long procId = procExec.submitProcedure(
      new SplitTableRegionProcedure(procExec.getEnvironment(), regions[0], splitKey),
      nonceGroup,
      nonce);
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);

    verify(tableName, splitRowNum);
  }

  @Test(timeout=60000)
  public void testSplitTableRegionNoStoreFile() throws Exception {
    final TableName tableName = TableName.valueOf("testSplitTableRegionNoStoreFile");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    HRegionInfo [] regions = MasterProcedureTestingUtility.createTable(
      procExec, tableName, null, ColumnFamilyName1, ColumnFamilyName2);
    int splitRowNum = startRowNum + rowCount / 2;
    byte[] splitKey = Bytes.toBytes("" + splitRowNum);

    assertTrue("not able to find a splittable region", regions != null);
    assertTrue("not able to find a splittable region", regions.length == 1);

    // Split region of the table
    long procId = procExec.submitProcedure(
      new SplitTableRegionProcedure(procExec.getEnvironment(), regions[0], splitKey),
      nonceGroup,
      nonce);
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);

    assertTrue(UTIL.getMiniHBaseCluster().getRegions(tableName).size() == 2);
    assertTrue(UTIL.countRows(tableName) == 0);
  }

  @Test(timeout=60000)
  public void testSplitTableRegionUnevenDaughter() throws Exception {
    final TableName tableName = TableName.valueOf("testSplitTableRegionUnevenDaughter");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    HRegionInfo [] regions = MasterProcedureTestingUtility.createTable(
      procExec, tableName, null, ColumnFamilyName1, ColumnFamilyName2);
    insertData(tableName);
    // Split to two daughters with one of them only has 1 row
    int splitRowNum = startRowNum + rowCount / 4;
    byte[] splitKey = Bytes.toBytes("" + splitRowNum);

    assertTrue("not able to find a splittable region", regions != null);
    assertTrue("not able to find a splittable region", regions.length == 1);

    // Split region of the table
    long procId = procExec.submitProcedure(
      new SplitTableRegionProcedure(procExec.getEnvironment(), regions[0], splitKey),
      nonceGroup,
      nonce);
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);

    verify(tableName, splitRowNum);
  }

  @Test(timeout=60000)
  public void testSplitTableRegionEmptyDaughter() throws Exception {
    final TableName tableName = TableName.valueOf("testSplitTableRegionEmptyDaughter");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    HRegionInfo [] regions = MasterProcedureTestingUtility.createTable(
      procExec, tableName, null, ColumnFamilyName1, ColumnFamilyName2);
    insertData(tableName);
    // Split to two daughters with one of them only has 1 row
    int splitRowNum = startRowNum + rowCount;
    byte[] splitKey = Bytes.toBytes("" + splitRowNum);

    assertTrue("not able to find a splittable region", regions != null);
    assertTrue("not able to find a splittable region", regions.length == 1);

    // Split region of the table
    long procId = procExec.submitProcedure(
      new SplitTableRegionProcedure(procExec.getEnvironment(), regions[0], splitKey),
      nonceGroup,
      nonce);
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);

    // Make sure one daughter has 0 rows.
    List<HRegion> daughters = UTIL.getMiniHBaseCluster().getRegions(tableName);
    assertTrue(daughters.size() == 2);
    assertTrue(UTIL.countRows(tableName) == rowCount);
    assertTrue(UTIL.countRows(daughters.get(0)) == 0 || UTIL.countRows(daughters.get(1)) == 0);
  }

  @Test(timeout=60000)
  public void testSplitTableRegionDeletedRowsDaughter() throws Exception {
    final TableName tableName = TableName.valueOf("testSplitTableRegionDeletedRowsDaughter");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    HRegionInfo [] regions = MasterProcedureTestingUtility.createTable(
      procExec, tableName, null, ColumnFamilyName1, ColumnFamilyName2);
    insertData(tableName);
    // Split to two daughters with one of them only has 1 row
    int splitRowNum = rowCount;
    deleteData(tableName, splitRowNum);
    byte[] splitKey = Bytes.toBytes("" + splitRowNum);

    assertTrue("not able to find a splittable region", regions != null);
    assertTrue("not able to find a splittable region", regions.length == 1);

    // Split region of the table
    long procId = procExec.submitProcedure(
      new SplitTableRegionProcedure(procExec.getEnvironment(), regions[0], splitKey),
      nonceGroup,
      nonce);
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);

    UTIL.getHBaseAdmin().majorCompact(tableName);
    // waiting for the major compaction to complete
    UTIL.waitFor(6000, new Waiter.Predicate<IOException>() {
      @Override
      public boolean evaluate() throws IOException {
        return UTIL.getHBaseAdmin().getCompactionState(tableName) == CompactionState.NONE;
      }
    });

    // Make sure one daughter has 0 rows.
    List<HRegion> daughters = UTIL.getMiniHBaseCluster().getRegions(tableName);
    assertTrue(daughters.size() == 2);
    final int currentRowCount = splitRowNum - startRowNum;
    assertTrue(UTIL.countRows(tableName) == currentRowCount);
    assertTrue(UTIL.countRows(daughters.get(0)) == 0 || UTIL.countRows(daughters.get(1)) == 0);
  }

  @Test(timeout=60000)
  public void testSplitTableRegionTwiceWithSameNonce() throws Exception {
    final TableName tableName = TableName.valueOf("testSplitTableRegionTwiceWithSameNonce");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    HRegionInfo [] regions = MasterProcedureTestingUtility.createTable(
      procExec, tableName, null, ColumnFamilyName1, ColumnFamilyName2);
    insertData(tableName);
    int splitRowNum = startRowNum + rowCount / 2;
    byte[] splitKey = Bytes.toBytes("" + splitRowNum);

    assertTrue("not able to find a splittable region", regions != null);
    assertTrue("not able to find a splittable region", regions.length == 1);

    // Split region of the table
    long procId1 = procExec.submitProcedure(
      new SplitTableRegionProcedure(procExec.getEnvironment(), regions[0], splitKey),
      nonceGroup,
      nonce);
    // Split region of the table with the same nonce
    long procId2 = procExec.submitProcedure(
      new SplitTableRegionProcedure(procExec.getEnvironment(), regions[0], splitKey),
      nonceGroup,
      nonce);

    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId1);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId1);
    // The second proc should succeed too - because it is the same proc.
    ProcedureTestingUtility.waitProcedure(procExec, procId2);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId2);
    assertTrue(procId1 == procId2);

    verify(tableName, splitRowNum);
  }

  @Test(timeout=60000)
  public void testInvalidSplitKey() throws Exception {
    final TableName tableName = TableName.valueOf("testInvalidSplitKey");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    HRegionInfo [] regions = MasterProcedureTestingUtility.createTable(
      procExec, tableName, null, ColumnFamilyName1, ColumnFamilyName2);
    insertData(tableName);
    int splitRowNum = startRowNum + rowCount / 2;
    byte[] splitKey = Bytes.toBytes("" + splitRowNum);

    assertTrue("not able to find a splittable region", regions != null);
    assertTrue("not able to find a splittable region", regions.length == 1);

    // Split region of the table with null split key
    try {
      long procId1 = procExec.submitProcedure(
        new SplitTableRegionProcedure(procExec.getEnvironment(), regions[0], null),
        nonceGroup,
        nonce);
      ProcedureTestingUtility.waitProcedure(procExec, procId1);
      fail("unexpected procedure start with invalid split-key");
    } catch (DoNotRetryIOException e) {
      LOG.debug("Expected Split procedure construction failure: " + e.getMessage());
    }
  }

  @Test(timeout = 600000)
  public void testRollbackAndDoubleExecution() throws Exception {
    final TableName tableName = TableName.valueOf("testRollbackAndDoubleExecution");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    HRegionInfo [] regions = MasterProcedureTestingUtility.createTable(
      procExec, tableName, null, ColumnFamilyName1, ColumnFamilyName2);
    insertData(tableName);
    int splitRowNum = startRowNum + rowCount / 2;
    byte[] splitKey = Bytes.toBytes("" + splitRowNum);

    assertTrue("not able to find a splittable region", regions != null);
    assertTrue("not able to find a splittable region", regions.length == 1);
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Split region of the table
    long procId = procExec.submitProcedure(
      new SplitTableRegionProcedure(procExec.getEnvironment(), regions[0], splitKey),
      nonceGroup,
      nonce);

    // Failing before SPLIT_TABLE_REGION_UPDATE_META we should trigger the
    // rollback
    // NOTE: the 5 (number before SPLIT_TABLE_REGION_UPDATE_META step) is
    // hardcoded, so you have to look at this test at least once when you add a new step.
    int numberOfSteps = 5;
    MasterProcedureTestingUtility.testRollbackAndDoubleExecution(
      procExec,
      procId,
      numberOfSteps);
  }

  @Test(timeout=60000)
  public void testRecoveryAndDoubleExecution() throws Exception {
    final TableName tableName = TableName.valueOf("testRecoveryAndDoubleExecution");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    HRegionInfo [] regions = MasterProcedureTestingUtility.createTable(
      procExec, tableName, null, ColumnFamilyName1, ColumnFamilyName2);
    insertData(tableName);
    int splitRowNum = startRowNum + rowCount / 2;
    byte[] splitKey = Bytes.toBytes("" + splitRowNum);

    assertTrue("not able to find a splittable region", regions != null);
    assertTrue("not able to find a splittable region", regions.length == 1);
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Split region of the table
    long procId = procExec.submitProcedure(
      new SplitTableRegionProcedure(procExec.getEnvironment(), regions[0], splitKey),
      nonceGroup,
      nonce);

    // Restart the executor and execute the step twice
    int numberOfSteps = SplitTableRegionState.values().length;
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(procExec, procId, numberOfSteps);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);

    verify(tableName, splitRowNum);
  }

  private void insertData(final TableName tableName) throws IOException, InterruptedException {
    Table t = UTIL.getConnection().getTable(tableName);
    Put p;
    for (int i= 0; i < rowCount / 2; i++) {
      p = new Put(Bytes.toBytes("" + (startRowNum + i)));
      p.addColumn(Bytes.toBytes(ColumnFamilyName1), Bytes.toBytes("q1"), Bytes.toBytes(i));
      p.addColumn(Bytes.toBytes(ColumnFamilyName2), Bytes.toBytes("q2"), Bytes.toBytes(i));
      t.put(p);
      p = new Put(Bytes.toBytes("" + (startRowNum + rowCount - i - 1)));
      p.addColumn(Bytes.toBytes(ColumnFamilyName1), Bytes.toBytes("q1"), Bytes.toBytes(i));
      p.addColumn(Bytes.toBytes(ColumnFamilyName2), Bytes.toBytes("q2"), Bytes.toBytes(i));
      t.put(p);
      if (i % 5 == 0) {
        UTIL.getHBaseAdmin().flush(tableName);
      }
    }
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
        UTIL.getHBaseAdmin().flush(tableName);
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
        ColumnFamilyName1.getBytes(),
        ColumnFamilyName2.getBytes());
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
        assertTrue(CellUtil.matchingRow(raw[j], row));
        assertTrue(CellUtil.matchingFamily(raw[j], families[j]));
      }
    }
  }

  private ProcedureExecutor<MasterProcedureEnv> getMasterProcedureExecutor() {
    return UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
  }
}
