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

import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureResult;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos.ModifyColumnFamilyState;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestModifyColumnFamilyProcedure {
  private static final Log LOG = LogFactory.getLog(TestModifyColumnFamilyProcedure.class);

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

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
  }

  @After
  public void tearDown() throws Exception {
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(getMasterProcedureExecutor(), false);
    for (HTableDescriptor htd: UTIL.getHBaseAdmin().listTables()) {
      LOG.info("Tear down, remove table=" + htd.getTableName());
      UTIL.deleteTable(htd.getTableName());
    }
  }

  @Test(timeout = 60000)
  public void testModifyColumnFamily() throws Exception {
    final TableName tableName = TableName.valueOf("testModifyColumnFamily");
    final String cf1 = "cf1";
    final HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf1);
    int oldBlockSize = columnDescriptor.getBlocksize();
    int newBlockSize = 3 * oldBlockSize;

    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    MasterProcedureTestingUtility.createTable(procExec, tableName, null, cf1, "f2");

    // Test 1: modify the column family online
    columnDescriptor.setBlocksize(newBlockSize);
    long procId1 = procExec.submitProcedure(new ModifyColumnFamilyProcedure(
        procExec.getEnvironment(), tableName, columnDescriptor));
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId1);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId1);
    MasterProcedureTestingUtility.validateColumnFamilyModification(UTIL.getHBaseCluster()
        .getMaster(), tableName, cf1, columnDescriptor);

    // Test 2: modify the column family offline
    UTIL.getHBaseAdmin().disableTable(tableName);
    columnDescriptor.setBlocksize(newBlockSize * 2);
    long procId2 =
        procExec.submitProcedure(new ModifyColumnFamilyProcedure(procExec.getEnvironment(),
            tableName, columnDescriptor));
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId2);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId2);
    MasterProcedureTestingUtility.validateColumnFamilyModification(UTIL.getHBaseCluster()
        .getMaster(), tableName, cf1, columnDescriptor);
  }

  @Test(timeout=60000)
  public void testModifyNonExistingColumnFamily() throws Exception {
    final TableName tableName = TableName.valueOf("testModifyExistingColumnFamily");
    final String cf2 = "cf2";
    final HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf2);
    int oldBlockSize = columnDescriptor.getBlocksize();
    int newBlockSize = 2 * oldBlockSize;

    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f1");

    // Modify the column family that does not exist
    columnDescriptor.setBlocksize(newBlockSize);
    long procId1 = procExec.submitProcedure(new ModifyColumnFamilyProcedure(
        procExec.getEnvironment(), tableName, columnDescriptor));
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId1);

    ProcedureResult result = procExec.getResult(procId1);
    assertTrue(result.isFailed());
    LOG.debug("Modify failed with exception: " + result.getException());
    assertTrue(result.getException().getCause() instanceof InvalidFamilyOperationException);
  }

  @Test(timeout=60000)
  public void testRecoveryAndDoubleExecutionOffline() throws Exception {
    final TableName tableName = TableName.valueOf("testRecoveryAndDoubleExecutionOffline");
    final String cf3 = "cf3";
    final HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf3);
    int oldBlockSize = columnDescriptor.getBlocksize();
    int newBlockSize = 4 * oldBlockSize;

    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    // create the table
    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f1", "f2", cf3);
    UTIL.getHBaseAdmin().disableTable(tableName);
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the Modify procedure && kill the executor
    columnDescriptor.setBlocksize(newBlockSize);
    long procId = procExec.submitProcedure(new ModifyColumnFamilyProcedure(
        procExec.getEnvironment(), tableName, columnDescriptor));

    // Restart the executor and execute the step twice
    int numberOfSteps = ModifyColumnFamilyState.values().length;
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(
      procExec,
      procId,
      numberOfSteps,
      ModifyColumnFamilyState.values());

    MasterProcedureTestingUtility.validateColumnFamilyModification(UTIL.getHBaseCluster()
        .getMaster(), tableName, cf3, columnDescriptor);
  }

  @Test(timeout = 60000)
  public void testRecoveryAndDoubleExecutionOnline() throws Exception {
    final TableName tableName = TableName.valueOf("testRecoveryAndDoubleExecutionOnline");
    final String cf4 = "cf4";
    final HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf4);
    int oldBlockSize = columnDescriptor.getBlocksize();
    int newBlockSize = 4 * oldBlockSize;

    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    // create the table
    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f1", "f2", cf4);
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the Modify procedure && kill the executor
    columnDescriptor.setBlocksize(newBlockSize);
    long procId =
        procExec.submitProcedure(new ModifyColumnFamilyProcedure(procExec.getEnvironment(),
            tableName, columnDescriptor));

    // Restart the executor and execute the step twice
    int numberOfSteps = ModifyColumnFamilyState.values().length;
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(procExec, procId, numberOfSteps,
      ModifyColumnFamilyState.values());

    MasterProcedureTestingUtility.validateColumnFamilyModification(UTIL.getHBaseCluster()
        .getMaster(), tableName, cf4, columnDescriptor);
  }

  @Test(timeout = 60000)
  public void testRollbackAndDoubleExecution() throws Exception {
    final TableName tableName = TableName.valueOf("testRollbackAndDoubleExecution");
    final String cf3 = "cf3";
    final HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf3);
    int oldBlockSize = columnDescriptor.getBlocksize();
    int newBlockSize = 4 * oldBlockSize;

    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    // create the table
    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f1", "f2", cf3);
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the Modify procedure && kill the executor
    columnDescriptor.setBlocksize(newBlockSize);
    long procId = procExec.submitProcedure(new ModifyColumnFamilyProcedure(
        procExec.getEnvironment(), tableName, columnDescriptor));

    // Failing in the middle of proc
    int numberOfSteps = ModifyColumnFamilyState.values().length - 2;
    MasterProcedureTestingUtility.testRollbackAndDoubleExecution(
      procExec,
      procId,
      numberOfSteps,
      ModifyColumnFamilyState.values());
  }

  private ProcedureExecutor<MasterProcedureEnv> getMasterProcedureExecutor() {
    return UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
  }
}
