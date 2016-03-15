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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos.ModifyTableState;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MasterTests.class, MediumTests.class})
public class TestModifyTableProcedure {
  private static final Log LOG = LogFactory.getLog(TestModifyTableProcedure.class);

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static long nonceGroup = HConstants.NO_NONCE;
  private static long nonce = HConstants.NO_NONCE;

  private static void setupConf(Configuration conf) {
    conf.setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, 1);
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(1);
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
  public void testModifyTable() throws Exception {
    final TableName tableName = TableName.valueOf("testModifyTable");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "cf");
    UTIL.getHBaseAdmin().disableTable(tableName);

    // Modify the table descriptor
    HTableDescriptor htd = new HTableDescriptor(UTIL.getHBaseAdmin().getTableDescriptor(tableName));

    // Test 1: Modify 1 property
    long newMaxFileSize = htd.getMaxFileSize() * 2;
    htd.setMaxFileSize(newMaxFileSize);
    htd.setRegionReplication(3);

    long procId1 = ProcedureTestingUtility.submitAndWait(
        procExec, new ModifyTableProcedure(procExec.getEnvironment(), htd));
    ProcedureTestingUtility.assertProcNotFailed(procExec.getResult(procId1));

    HTableDescriptor currentHtd = UTIL.getHBaseAdmin().getTableDescriptor(tableName);
    assertEquals(newMaxFileSize, currentHtd.getMaxFileSize());

    // Test 2: Modify multiple properties
    boolean newReadOnlyOption = htd.isReadOnly() ? false : true;
    long newMemStoreFlushSize = htd.getMemStoreFlushSize() * 2;
    htd.setReadOnly(newReadOnlyOption);
    htd.setMemStoreFlushSize(newMemStoreFlushSize);

    long procId2 = ProcedureTestingUtility.submitAndWait(
        procExec, new ModifyTableProcedure(procExec.getEnvironment(), htd));
    ProcedureTestingUtility.assertProcNotFailed(procExec.getResult(procId2));

    currentHtd = UTIL.getHBaseAdmin().getTableDescriptor(tableName);
    assertEquals(newReadOnlyOption, currentHtd.isReadOnly());
    assertEquals(newMemStoreFlushSize, currentHtd.getMemStoreFlushSize());
  }

  @Test(timeout = 60000)
  public void testModifyTableAddCF() throws Exception {
    final TableName tableName = TableName.valueOf("testModifyTableAddCF");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "cf1");
    HTableDescriptor currentHtd = UTIL.getHBaseAdmin().getTableDescriptor(tableName);
    assertEquals(1, currentHtd.getFamiliesKeys().size());

    // Test 1: Modify the table descriptor online
    String cf2 = "cf2";
    HTableDescriptor htd = new HTableDescriptor(UTIL.getHBaseAdmin().getTableDescriptor(tableName));
    htd.addFamily(new HColumnDescriptor(cf2));

    long procId = ProcedureTestingUtility.submitAndWait(
        procExec, new ModifyTableProcedure(procExec.getEnvironment(), htd));
    ProcedureTestingUtility.assertProcNotFailed(procExec.getResult(procId));

    currentHtd = UTIL.getHBaseAdmin().getTableDescriptor(tableName);
    assertEquals(2, currentHtd.getFamiliesKeys().size());
    assertTrue(currentHtd.hasFamily(cf2.getBytes()));

    // Test 2: Modify the table descriptor offline
    UTIL.getHBaseAdmin().disableTable(tableName);
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    String cf3 = "cf3";
    HTableDescriptor htd2 =
        new HTableDescriptor(UTIL.getHBaseAdmin().getTableDescriptor(tableName));
    htd2.addFamily(new HColumnDescriptor(cf3));

    long procId2 =
        ProcedureTestingUtility.submitAndWait(procExec,
          new ModifyTableProcedure(procExec.getEnvironment(), htd2));
    ProcedureTestingUtility.assertProcNotFailed(procExec.getResult(procId2));

    currentHtd = UTIL.getHBaseAdmin().getTableDescriptor(tableName);
    assertTrue(currentHtd.hasFamily(cf3.getBytes()));
    assertEquals(3, currentHtd.getFamiliesKeys().size());
  }

  @Test(timeout = 60000)
  public void testModifyTableDeleteCF() throws Exception {
    final TableName tableName = TableName.valueOf("testModifyTableDeleteCF");
    final String cf1 = "cf1";
    final String cf2 = "cf2";
    final String cf3 = "cf3";
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    MasterProcedureTestingUtility.createTable(procExec, tableName, null, cf1, cf2, cf3);
    HTableDescriptor currentHtd = UTIL.getHBaseAdmin().getTableDescriptor(tableName);
    assertEquals(3, currentHtd.getFamiliesKeys().size());

    // Test 1: Modify the table descriptor
    HTableDescriptor htd = new HTableDescriptor(UTIL.getHBaseAdmin().getTableDescriptor(tableName));
    htd.removeFamily(cf2.getBytes());

    long procId = ProcedureTestingUtility.submitAndWait(
        procExec, new ModifyTableProcedure(procExec.getEnvironment(), htd));
    ProcedureTestingUtility.assertProcNotFailed(procExec.getResult(procId));

    currentHtd = UTIL.getHBaseAdmin().getTableDescriptor(tableName);
    assertEquals(2, currentHtd.getFamiliesKeys().size());
    assertFalse(currentHtd.hasFamily(cf2.getBytes()));

    // Test 2: Modify the table descriptor offline
    UTIL.getHBaseAdmin().disableTable(tableName);
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);

    HTableDescriptor htd2 =
        new HTableDescriptor(UTIL.getHBaseAdmin().getTableDescriptor(tableName));
    htd2.removeFamily(cf3.getBytes());
    // Disable Sanity check
    htd2.setConfiguration("hbase.table.sanity.checks", Boolean.FALSE.toString());

    long procId2 =
        ProcedureTestingUtility.submitAndWait(procExec,
          new ModifyTableProcedure(procExec.getEnvironment(), htd2));
    ProcedureTestingUtility.assertProcNotFailed(procExec.getResult(procId2));

    currentHtd = UTIL.getHBaseAdmin().getTableDescriptor(tableName);
    assertEquals(1, currentHtd.getFamiliesKeys().size());
    assertFalse(currentHtd.hasFamily(cf3.getBytes()));

    //Removing the last family will fail
    HTableDescriptor htd3 =
        new HTableDescriptor(UTIL.getHBaseAdmin().getTableDescriptor(tableName));
    htd3.removeFamily(cf1.getBytes());
    long procId3 =
        ProcedureTestingUtility.submitAndWait(procExec,
            new ModifyTableProcedure(procExec.getEnvironment(), htd3));
    final ProcedureInfo result = procExec.getResult(procId3);
    assertEquals(true, result.isFailed());
    Throwable cause = ProcedureTestingUtility.getExceptionCause(result);
    assertTrue("expected DoNotRetryIOException, got " + cause,
        cause instanceof DoNotRetryIOException);
    assertEquals(1, currentHtd.getFamiliesKeys().size());
    assertTrue(currentHtd.hasFamily(cf1.getBytes()));
  }

  @Test(timeout=60000)
  public void testRecoveryAndDoubleExecutionOffline() throws Exception {
    final TableName tableName = TableName.valueOf("testRecoveryAndDoubleExecutionOffline");
    final String cf2 = "cf2";
    final String cf3 = "cf3";
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    // create the table
    HRegionInfo[] regions = MasterProcedureTestingUtility.createTable(
      procExec, tableName, null, "cf1", cf3);
    UTIL.getHBaseAdmin().disableTable(tableName);

    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Modify multiple properties of the table.
    HTableDescriptor htd = new HTableDescriptor(UTIL.getHBaseAdmin().getTableDescriptor(tableName));
    boolean newCompactionEnableOption = htd.isCompactionEnabled() ? false : true;
    htd.setCompactionEnabled(newCompactionEnableOption);
    htd.addFamily(new HColumnDescriptor(cf2));
    htd.removeFamily(cf3.getBytes());
    htd.setRegionReplication(3);

    // Start the Modify procedure && kill the executor
    long procId = procExec.submitProcedure(
      new ModifyTableProcedure(procExec.getEnvironment(), htd), nonceGroup, nonce);

    // Restart the executor and execute the step twice
    int numberOfSteps = ModifyTableState.values().length;
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(
      procExec,
      procId,
      numberOfSteps,
      ModifyTableState.values());

    // Validate descriptor
    HTableDescriptor currentHtd = UTIL.getHBaseAdmin().getTableDescriptor(tableName);
    assertEquals(newCompactionEnableOption, currentHtd.isCompactionEnabled());
    assertEquals(2, currentHtd.getFamiliesKeys().size());

    // cf2 should be added cf3 should be removed
    MasterProcedureTestingUtility.validateTableCreation(UTIL.getHBaseCluster().getMaster(),
      tableName, regions, false, "cf1", cf2);
  }

  @Test(timeout = 60000)
  public void testRecoveryAndDoubleExecutionOnline() throws Exception {
    final TableName tableName = TableName.valueOf("testRecoveryAndDoubleExecutionOnline");
    final String cf2 = "cf2";
    final String cf3 = "cf3";
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    // create the table
    HRegionInfo[] regions = MasterProcedureTestingUtility.createTable(
      procExec, tableName, null, "cf1", cf3);

    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Modify multiple properties of the table.
    HTableDescriptor htd = new HTableDescriptor(UTIL.getHBaseAdmin().getTableDescriptor(tableName));
    boolean newCompactionEnableOption = htd.isCompactionEnabled() ? false : true;
    htd.setCompactionEnabled(newCompactionEnableOption);
    htd.addFamily(new HColumnDescriptor(cf2));
    htd.removeFamily(cf3.getBytes());

    // Start the Modify procedure && kill the executor
    long procId = procExec.submitProcedure(
      new ModifyTableProcedure(procExec.getEnvironment(), htd), nonceGroup, nonce);

    // Restart the executor and execute the step twice
    int numberOfSteps = ModifyTableState.values().length;
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(procExec, procId, numberOfSteps,
      ModifyTableState.values());

    // Validate descriptor
    HTableDescriptor currentHtd = UTIL.getHBaseAdmin().getTableDescriptor(tableName);
    assertEquals(newCompactionEnableOption, currentHtd.isCompactionEnabled());
    assertEquals(2, currentHtd.getFamiliesKeys().size());
    assertTrue(currentHtd.hasFamily(cf2.getBytes()));
    assertFalse(currentHtd.hasFamily(cf3.getBytes()));

    // cf2 should be added cf3 should be removed
    MasterProcedureTestingUtility.validateTableCreation(UTIL.getHBaseCluster().getMaster(),
      tableName, regions, "cf1", cf2);
  }

  @Test(timeout = 60000)
  public void testRollbackAndDoubleExecutionOnline() throws Exception {
    final TableName tableName = TableName.valueOf("testRollbackAndDoubleExecution");
    final String familyName = "cf2";
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    // create the table
    HRegionInfo[] regions = MasterProcedureTestingUtility.createTable(
      procExec, tableName, null, "cf1");

    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    HTableDescriptor htd = new HTableDescriptor(UTIL.getHBaseAdmin().getTableDescriptor(tableName));
    boolean newCompactionEnableOption = htd.isCompactionEnabled() ? false : true;
    htd.setCompactionEnabled(newCompactionEnableOption);
    htd.addFamily(new HColumnDescriptor(familyName));

    // Start the Modify procedure && kill the executor
    long procId = procExec.submitProcedure(
      new ModifyTableProcedure(procExec.getEnvironment(), htd), nonceGroup, nonce);

    // Restart the executor and rollback the step twice
    int numberOfSteps = ModifyTableState.values().length - 4; // failing in the middle of proc
    MasterProcedureTestingUtility.testRollbackAndDoubleExecution(
      procExec,
      procId,
      numberOfSteps,
      ModifyTableState.values());

    // cf2 should not be present
    MasterProcedureTestingUtility.validateTableCreation(UTIL.getHBaseCluster().getMaster(),
      tableName, regions, "cf1");
  }

  @Test(timeout = 60000)
  public void testRollbackAndDoubleExecutionOffline() throws Exception {
    final TableName tableName = TableName.valueOf("testRollbackAndDoubleExecution");
    final String familyName = "cf2";
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    // create the table
    HRegionInfo[] regions = MasterProcedureTestingUtility.createTable(
      procExec, tableName, null, "cf1");
    UTIL.getHBaseAdmin().disableTable(tableName);

    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    HTableDescriptor htd = new HTableDescriptor(UTIL.getHBaseAdmin().getTableDescriptor(tableName));
    boolean newCompactionEnableOption = htd.isCompactionEnabled() ? false : true;
    htd.setCompactionEnabled(newCompactionEnableOption);
    htd.addFamily(new HColumnDescriptor(familyName));
    htd.setRegionReplication(3);

    // Start the Modify procedure && kill the executor
    long procId = procExec.submitProcedure(
      new ModifyTableProcedure(procExec.getEnvironment(), htd), nonceGroup, nonce);

    // Restart the executor and rollback the step twice
    int numberOfSteps = ModifyTableState.values().length - 4; // failing in the middle of proc
    MasterProcedureTestingUtility.testRollbackAndDoubleExecution(
      procExec,
      procId,
      numberOfSteps,
      ModifyTableState.values());

    // cf2 should not be present
    MasterProcedureTestingUtility.validateTableCreation(UTIL.getHBaseCluster().getMaster(),
      tableName, regions, "cf1");
  }

  @Test(timeout = 60000)
  public void testRollbackAndDoubleExecutionAfterPONR() throws Exception {
    final TableName tableName = TableName.valueOf("testRollbackAndDoubleExecutionAfterPONR");
    final String familyToAddName = "cf2";
    final String familyToRemove = "cf1";
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    // create the table
    HRegionInfo[] regions = MasterProcedureTestingUtility.createTable(
      procExec, tableName, null, familyToRemove);
    UTIL.getHBaseAdmin().disableTable(tableName);

    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    HTableDescriptor htd = new HTableDescriptor(UTIL.getHBaseAdmin().getTableDescriptor(tableName));
    htd.setCompactionEnabled(!htd.isCompactionEnabled());
    htd.addFamily(new HColumnDescriptor(familyToAddName));
    htd.removeFamily(familyToRemove.getBytes());
    htd.setRegionReplication(3);

    // Start the Modify procedure && kill the executor
    long procId = procExec.submitProcedure(
      new ModifyTableProcedure(procExec.getEnvironment(), htd), nonceGroup, nonce);

    // Failing after MODIFY_TABLE_DELETE_FS_LAYOUT we should not trigger the rollback.
    // NOTE: the 5 (number of MODIFY_TABLE_DELETE_FS_LAYOUT + 1 step) is hardcoded,
    //       so you have to look at this test at least once when you add a new step.
    int numberOfSteps = 5;
    MasterProcedureTestingUtility.testRollbackAndDoubleExecutionAfterPONR(
      procExec,
      procId,
      numberOfSteps,
      ModifyTableState.values());

    // "cf2" should be added and "cf1" should be removed
    MasterProcedureTestingUtility.validateTableCreation(UTIL.getHBaseCluster().getMaster(),
      tableName, regions, false, familyToAddName);
  }

  private ProcedureExecutor<MasterProcedureEnv> getMasterProcedureExecutor() {
    return UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
  }
}
