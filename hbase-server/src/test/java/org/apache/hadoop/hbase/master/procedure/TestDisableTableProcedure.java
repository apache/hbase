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
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos.DisableTableState;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MasterTests.class, MediumTests.class})
public class TestDisableTableProcedure extends TestTableDDLProcedureBase {
  private static final Log LOG = LogFactory.getLog(TestDisableTableProcedure.class);

  @Test(timeout = 60000)
  public void testDisableTable() throws Exception {
    final TableName tableName = TableName.valueOf("testDisableTable");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f1", "f2");

    // Disable the table
    long procId = procExec.submitProcedure(
      new DisableTableProcedure(procExec.getEnvironment(), tableName, false), nonceGroup, nonce);
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);
    MasterProcedureTestingUtility.validateTableIsDisabled(UTIL.getHBaseCluster().getMaster(),
      tableName);
  }

  @Test(timeout = 60000)
  public void testDisableTableMultipleTimes() throws Exception {
    final TableName tableName = TableName.valueOf("testDisableTableMultipleTimes");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f1", "f2");

    // Disable the table
    long procId1 = procExec.submitProcedure(new DisableTableProcedure(
        procExec.getEnvironment(), tableName, false), nonceGroup, nonce);
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId1);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId1);
    MasterProcedureTestingUtility.validateTableIsDisabled(UTIL.getHBaseCluster().getMaster(),
      tableName);

    // Disable the table again - expect failure
    long procId2 = procExec.submitProcedure(new DisableTableProcedure(
        procExec.getEnvironment(), tableName, false), nonceGroup + 1, nonce + 1);
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId2);
    ProcedureInfo result = procExec.getResult(procId2);
    assertTrue(result.isFailed());
    LOG.debug("Disable failed with exception: " + result.getExceptionFullMessage());
    assertTrue(
      ProcedureTestingUtility.getExceptionCause(result) instanceof TableNotEnabledException);

    // Disable the table - expect failure from ProcedurePrepareLatch
    try {
      final ProcedurePrepareLatch prepareLatch = new ProcedurePrepareLatch.CompatibilityLatch();

      long procId3 = procExec.submitProcedure(new DisableTableProcedure(
          procExec.getEnvironment(), tableName, false, prepareLatch), nonceGroup + 2, nonce + 2);
      prepareLatch.await();
      Assert.fail("Disable should throw exception through latch.");
    } catch (TableNotEnabledException tnee) {
      // Expected
      LOG.debug("Disable failed with expected exception.");
    }

    // Disable the table again with skipping table state check flag (simulate recovery scenario)
    long procId4 = procExec.submitProcedure(new DisableTableProcedure(
        procExec.getEnvironment(), tableName, true));
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId4);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId4);
    MasterProcedureTestingUtility.validateTableIsDisabled(UTIL.getHBaseCluster().getMaster(),
      tableName);
  }

  @Test(timeout = 60000)
  public void testDisableTableTwiceWithSameNonce() throws Exception {
    final TableName tableName = TableName.valueOf("testDisableTableTwiceWithSameNonce");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f1", "f2");

    // Disable the table
    long procId1 = procExec.submitProcedure(new DisableTableProcedure(
        procExec.getEnvironment(), tableName, false), nonceGroup, nonce);
    long procId2 = procExec.submitProcedure(new DisableTableProcedure(
      procExec.getEnvironment(), tableName, false), nonceGroup, nonce);
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId1);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId1);
    MasterProcedureTestingUtility.validateTableIsDisabled(UTIL.getHBaseCluster().getMaster(),
      tableName);

    ProcedureTestingUtility.waitProcedure(procExec, procId2);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId2);
    assertTrue(procId1 == procId2);
  }

  @Test(timeout=60000)
  public void testRecoveryAndDoubleExecution() throws Exception {
    final TableName tableName = TableName.valueOf("testRecoveryAndDoubleExecution");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    final byte[][] splitKeys = new byte[][] {
      Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("c")
    };
    MasterProcedureTestingUtility.createTable(procExec, tableName, splitKeys, "f1", "f2");

    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the Disable procedure && kill the executor
    long procId = procExec.submitProcedure(
      new DisableTableProcedure(procExec.getEnvironment(), tableName, false), nonceGroup, nonce);

    // Restart the executor and execute the step twice
    int numberOfSteps = DisableTableState.values().length;
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(procExec, procId, numberOfSteps);
    MasterProcedureTestingUtility.validateTableIsDisabled(UTIL.getHBaseCluster().getMaster(),
      tableName);
  }
}
