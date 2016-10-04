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
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.EnableTableState;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MasterTests.class, MediumTests.class})
public class TestEnableTableProcedure extends TestTableDDLProcedureBase {
  private static final Log LOG = LogFactory.getLog(TestEnableTableProcedure.class);

  @Test(timeout = 60000)
  public void testEnableTable() throws Exception {
    final TableName tableName = TableName.valueOf("testEnableTable");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f1", "f2");
    UTIL.getHBaseAdmin().disableTable(tableName);

    // Enable the table
    long procId = procExec.submitProcedure(
      new EnableTableProcedure(procExec.getEnvironment(), tableName, false), nonceGroup, nonce);
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);
    MasterProcedureTestingUtility.validateTableIsEnabled(UTIL.getHBaseCluster().getMaster(),
      tableName);
  }

  @Test(timeout = 60000)
  public void testEnableTableTwiceWithSameNonce() throws Exception {
    final TableName tableName = TableName.valueOf("testEnableTableTwiceWithSameNonce");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f1", "f2");
    UTIL.getHBaseAdmin().disableTable(tableName);

    // Enable the table
    long procId1 = procExec.submitProcedure(
      new EnableTableProcedure(procExec.getEnvironment(), tableName, false), nonceGroup, nonce);
    long procId2 = procExec.submitProcedure(
      new EnableTableProcedure(procExec.getEnvironment(), tableName, false), nonceGroup, nonce);

    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId1);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId1);
    // The second proc should succeed too - because it is the same proc.
    ProcedureTestingUtility.waitProcedure(procExec, procId2);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId2);
    assertTrue(procId1 == procId2);
  }

  @Test(timeout=60000, expected=TableNotDisabledException.class)
  public void testEnableNonDisabledTable() throws Exception {
    final TableName tableName = TableName.valueOf("testEnableNonExistingTable");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f1", "f2");

    // Enable the table - expect failure
    long procId1 = procExec.submitProcedure(
        new EnableTableProcedure(procExec.getEnvironment(), tableName, false), nonceGroup, nonce);
    ProcedureTestingUtility.waitProcedure(procExec, procId1);

    ProcedureInfo result = procExec.getResult(procId1);
    assertTrue(result.isFailed());
    LOG.debug("Enable failed with exception: " + result.getExceptionFullMessage());
    assertTrue(
      ProcedureTestingUtility.getExceptionCause(result) instanceof TableNotDisabledException);

    // Enable the table with skipping table state check flag (simulate recovery scenario)
    long procId2 = procExec.submitProcedure(
        new EnableTableProcedure(procExec.getEnvironment(), tableName, true),
        nonceGroup + 1,
        nonce + 1);
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId2);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId2);

    // Enable the table - expect failure from ProcedurePrepareLatch
    final ProcedurePrepareLatch prepareLatch = new ProcedurePrepareLatch.CompatibilityLatch();
    long procId3 = procExec.submitProcedure(
        new EnableTableProcedure(procExec.getEnvironment(), tableName, false, prepareLatch),
        nonceGroup + 2,
        nonce + 2);
    prepareLatch.await();
    Assert.fail("Enable should throw exception through latch.");
  }

  @Test(timeout = 60000)
  public void testRecoveryAndDoubleExecution() throws Exception {
    final TableName tableName = TableName.valueOf("testRecoveryAndDoubleExecution");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    final byte[][] splitKeys = new byte[][] {
      Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("c")
    };
    MasterProcedureTestingUtility.createTable(procExec, tableName, splitKeys, "f1", "f2");
    UTIL.getHBaseAdmin().disableTable(tableName);
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the Enable procedure && kill the executor
    long procId = procExec.submitProcedure(
        new EnableTableProcedure(procExec.getEnvironment(), tableName, false), nonceGroup, nonce);

    // Restart the executor and execute the step twice
    int numberOfSteps = EnableTableState.values().length;
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(procExec, procId, numberOfSteps);
    MasterProcedureTestingUtility.validateTableIsEnabled(UTIL.getHBaseCluster().getMaster(),
      tableName);
  }

  @Test(timeout = 60000)
  public void testRollbackAndDoubleExecution() throws Exception {
    final TableName tableName = TableName.valueOf("testRollbackAndDoubleExecution");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    final byte[][] splitKeys = new byte[][] {
      Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("c")
    };
    MasterProcedureTestingUtility.createTable(procExec, tableName, splitKeys, "f1", "f2");
    UTIL.getHBaseAdmin().disableTable(tableName);
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the Enable procedure && kill the executor
    long procId = procExec.submitProcedure(
        new EnableTableProcedure(procExec.getEnvironment(), tableName, false), nonceGroup, nonce);

    int numberOfSteps = 1; // failing at pre operation
    MasterProcedureTestingUtility.testRollbackAndDoubleExecution(procExec, procId, numberOfSteps);
    MasterProcedureTestingUtility.validateTableIsDisabled(UTIL.getHBaseCluster().getMaster(),
      tableName);
  }
}
