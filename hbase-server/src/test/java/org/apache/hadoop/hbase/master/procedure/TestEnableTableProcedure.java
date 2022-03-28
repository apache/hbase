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
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MasterTests.class, MediumTests.class })
public class TestEnableTableProcedure extends TestTableDDLProcedureBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestEnableTableProcedure.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestEnableTableProcedure.class);

  @Rule
  public TestName name = new TestName();

  @Test
  public void testEnableTable() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f1", "f2");
    UTIL.getAdmin().disableTable(tableName);

    // Enable the table
    long procId =
      procExec.submitProcedure(new EnableTableProcedure(procExec.getEnvironment(), tableName));
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);
    MasterProcedureTestingUtility.validateTableIsEnabled(getMaster(), tableName);
  }

  @Test(expected = TableNotDisabledException.class)
  public void testEnableNonDisabledTable() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f1", "f2");

    // Enable the table - expect failure
    long procId1 =
      procExec.submitProcedure(new EnableTableProcedure(procExec.getEnvironment(), tableName));
    ProcedureTestingUtility.waitProcedure(procExec, procId1);

    Procedure<?> result = procExec.getResult(procId1);
    assertTrue(result.isFailed());
    LOG.debug("Enable failed with exception: " + result.getException());
    assertTrue(
      ProcedureTestingUtility.getExceptionCause(result) instanceof TableNotDisabledException);

    // Enable the table - expect failure from ProcedurePrepareLatch
    final ProcedurePrepareLatch prepareLatch = new ProcedurePrepareLatch.CompatibilityLatch();
    procExec.submitProcedure(
      new EnableTableProcedure(procExec.getEnvironment(), tableName, prepareLatch));
    prepareLatch.await();
  }

  @Test
  public void testRecoveryAndDoubleExecution() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    final byte[][] splitKeys =
      new byte[][] { Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("c") };
    MasterProcedureTestingUtility.createTable(procExec, tableName, splitKeys, "f1", "f2");
    UTIL.getAdmin().disableTable(tableName);
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillIfHasParent(procExec, false);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the Enable procedure && kill the executor
    long procId =
      procExec.submitProcedure(new EnableTableProcedure(procExec.getEnvironment(), tableName));

    // Restart the executor and execute the step twice
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(procExec, procId);

    MasterProcedureTestingUtility.validateTableIsEnabled(getMaster(), tableName);
  }

  @Test
  public void testRollbackAndDoubleExecution() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    final byte[][] splitKeys =
      new byte[][] { Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("c") };
    MasterProcedureTestingUtility.createTable(procExec, tableName, splitKeys, "f1", "f2");
    UTIL.getAdmin().disableTable(tableName);
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the Enable procedure && kill the executor
    long procId =
      procExec.submitProcedure(new EnableTableProcedure(procExec.getEnvironment(), tableName));

    int lastStep = 3; // fail before ENABLE_TABLE_SET_ENABLING_TABLE_STATE
    MasterProcedureTestingUtility.testRollbackAndDoubleExecution(procExec, procId, lastStep);
    MasterProcedureTestingUtility.validateTableIsDisabled(getMaster(), tableName);
  }
}
