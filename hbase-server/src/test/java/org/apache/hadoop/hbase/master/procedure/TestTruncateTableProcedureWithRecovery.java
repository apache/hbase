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
package org.apache.hadoop.hbase.master.procedure;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.TruncateTableState;

@Category({ MasterTests.class, LargeTests.class })
public class TestTruncateTableProcedureWithRecovery extends TestTableDDLProcedureBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestTruncateTableProcedureWithRecovery.class);

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setupCluster() throws Exception {
    // Enable recovery snapshots
    UTIL.getConfiguration().setBoolean(HConstants.SNAPSHOT_BEFORE_DESTRUCTIVE_ACTION_ENABLED_KEY,
      true);
    TestTableDDLProcedureBase.setupCluster();
  }

  @Test
  public void testRecoverySnapshotRollback() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final String[] families = new String[] { "f1", "f2" };
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    // Create table with data
    MasterProcedureTestingUtility.createTable(getMasterProcedureExecutor(), tableName, null,
      families);
    MasterProcedureTestingUtility.loadData(UTIL.getConnection(), tableName, 100, new byte[0][],
      families);
    assertEquals(100, UTIL.countRows(tableName));

    // Disable the table
    UTIL.getAdmin().disableTable(tableName);

    // Submit the failing procedure
    long procId = procExec.submitProcedure(
      new FailingTruncateTableProcedure(procExec.getEnvironment(), tableName, false));

    // Wait for procedure to complete (should fail)
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    Procedure<MasterProcedureEnv> result = procExec.getResult(procId);
    assertTrue("Procedure should have failed", result.isFailed());

    // Verify no recovery snapshots remain after rollback
    boolean snapshotFound = false;
    for (SnapshotDescription snapshot : UTIL.getAdmin().listSnapshots()) {
      if (snapshot.getName().startsWith("auto_" + tableName.getNameAsString())) {
        snapshotFound = true;
        break;
      }
    }
    assertTrue("Recovery snapshot should have been cleaned up during rollback", !snapshotFound);
  }

  @Test
  public void testRecoverySnapshotAndRestore() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final TableName restoredTableName = TableName.valueOf(name.getMethodName() + "_restored");
    final String[] families = new String[] { "f1", "f2" };

    // Create table with data
    MasterProcedureTestingUtility.createTable(getMasterProcedureExecutor(), tableName, null,
      families);
    MasterProcedureTestingUtility.loadData(UTIL.getConnection(), tableName, 100, new byte[0][],
      families);
    assertEquals(100, UTIL.countRows(tableName));

    // Disable the table
    UTIL.getAdmin().disableTable(tableName);

    // Truncate the table (this should create a recovery snapshot)
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    long procId = ProcedureTestingUtility.submitAndWait(procExec,
      new TruncateTableProcedure(procExec.getEnvironment(), tableName, false));
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);

    // Verify table is truncated
    UTIL.waitUntilAllRegionsAssigned(tableName);
    assertEquals(0, UTIL.countRows(tableName));

    // Find the recovery snapshot
    String recoverySnapshotName = null;
    for (SnapshotDescription snapshot : UTIL.getAdmin().listSnapshots()) {
      if (snapshot.getName().startsWith("auto_" + tableName.getNameAsString())) {
        recoverySnapshotName = snapshot.getName();
        break;
      }
    }
    assertTrue("Recovery snapshot should exist", recoverySnapshotName != null);

    // Restore from snapshot by cloning to a new table
    UTIL.getAdmin().cloneSnapshot(recoverySnapshotName, restoredTableName);
    UTIL.waitUntilAllRegionsAssigned(restoredTableName);

    // Verify restored table has original data
    assertEquals(100, UTIL.countRows(restoredTableName));

    // Clean up the cloned table
    UTIL.getAdmin().disableTable(restoredTableName);
    UTIL.getAdmin().deleteTable(restoredTableName);
  }

  public static class FailingTruncateTableProcedure extends TruncateTableProcedure {
    private boolean failOnce = false;

    public FailingTruncateTableProcedure() {
      super();
    }

    public FailingTruncateTableProcedure(MasterProcedureEnv env, TableName tableName,
      boolean preserveSplits) throws HBaseIOException {
      super(env, tableName, preserveSplits);
    }

    @Override
    protected Flow executeFromState(MasterProcedureEnv env, TruncateTableState state)
      throws InterruptedException {
      if (!failOnce && state == TruncateTableState.TRUNCATE_TABLE_CLEAR_FS_LAYOUT) {
        failOnce = true;
        throw new RuntimeException("Simulated failure");
      }
      return super.executeFromState(env, state);
    }
  }
}
