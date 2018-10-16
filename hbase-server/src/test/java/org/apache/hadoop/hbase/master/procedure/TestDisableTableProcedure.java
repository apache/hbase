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

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MasterTests.class, MediumTests.class})
public class TestDisableTableProcedure extends TestTableDDLProcedureBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestDisableTableProcedure.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestDisableTableProcedure.class);

  @Rule public TestName name = new TestName();

  @Test
  public void testDisableTable() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f1", "f2");

    // Disable the table
    long procId = procExec.submitProcedure(
      new DisableTableProcedure(procExec.getEnvironment(), tableName, false));
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);
    MasterProcedureTestingUtility.validateTableIsDisabled(getMaster(), tableName);
  }

  @Test
  public void testDisableTableMultipleTimes() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f1", "f2");

    // Disable the table
    long procId1 = procExec.submitProcedure(new DisableTableProcedure(
        procExec.getEnvironment(), tableName, false));
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId1);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId1);
    MasterProcedureTestingUtility.validateTableIsDisabled(getMaster(), tableName);

    // Disable the table again - expect failure. We used to get it via procExec#getResult but we
    // added fail fast so now happens on construction.
    Throwable e = null;
    Throwable cause = null;
    try {
      long procId2 = procExec.submitProcedure(new DisableTableProcedure(
          procExec.getEnvironment(), tableName, false));
      // Wait the completion
      ProcedureTestingUtility.waitProcedure(procExec, procId2);
      Procedure<?> result = procExec.getResult(procId2);
      assertTrue(result.isFailed());
      cause = ProcedureTestingUtility.getExceptionCause(result);
      e = result.getException();
    } catch (TableNotEnabledException tnde) {
      // Expected.
      e = tnde;
      cause = tnde;
    }
    LOG.debug("Disable failed with exception {}" + e);
    assertTrue(cause instanceof TableNotEnabledException);

    // Disable the table - expect failure from ProcedurePrepareLatch
    try {
      final ProcedurePrepareLatch prepareLatch = new ProcedurePrepareLatch.CompatibilityLatch();

      long procId3 = procExec.submitProcedure(new DisableTableProcedure(
          procExec.getEnvironment(), tableName, false, prepareLatch));
      prepareLatch.await();
      Assert.fail("Disable should throw exception through latch.");
    } catch (TableNotEnabledException tnee) {
      // Expected
      LOG.debug("Disable failed with expected exception {}", tnee);
    }

    // Disable the table again with skipping table state check flag (simulate recovery scenario)
    try {
      long procId4 = procExec.submitProcedure(new DisableTableProcedure(
        procExec.getEnvironment(), tableName, true));
      // Wait the completion
      ProcedureTestingUtility.waitProcedure(procExec, procId4);
      ProcedureTestingUtility.assertProcNotFailed(procExec, procId4);
    } catch (TableNotEnabledException tnee) {
      // Expected
      LOG.debug("Disable failed with expected exception {}", tnee);
    }
    MasterProcedureTestingUtility.validateTableIsDisabled(getMaster(), tableName);
  }

  @Test
  public void testRecoveryAndDoubleExecution() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    final byte[][] splitKeys = new byte[][] {
      Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("c")
    };
    MasterProcedureTestingUtility.createTable(procExec, tableName, splitKeys, "f1", "f2");

    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the Disable procedure && kill the executor
    long procId = procExec.submitProcedure(
      new DisableTableProcedure(procExec.getEnvironment(), tableName, false));

    // Restart the executor and execute the step twice
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(procExec, procId);

    MasterProcedureTestingUtility.validateTableIsDisabled(getMaster(), tableName);
  }
}
