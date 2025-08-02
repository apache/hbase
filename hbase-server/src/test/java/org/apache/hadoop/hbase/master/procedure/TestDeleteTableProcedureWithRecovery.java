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
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.DeleteTableState;

@Category({ MasterTests.class, MediumTests.class })
public class TestDeleteTableProcedureWithRecovery extends TestTableDDLProcedureBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestDeleteTableProcedureWithRecovery.class);

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
    MasterProcedureTestingUtility.createTable(procExec, tableName, null, families);
    MasterProcedureTestingUtility.loadData(UTIL.getConnection(), tableName, 100, new byte[0][],
      families);
    UTIL.getAdmin().disableTable(tableName);

    // Submit the failing procedure
    long procId = procExec
      .submitProcedure(new FailingDeleteTableProcedure(procExec.getEnvironment(), tableName));

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

    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    // Create table with data
    MasterProcedureTestingUtility.createTable(procExec, tableName, null, families);
    MasterProcedureTestingUtility.loadData(UTIL.getConnection(), tableName, 100, new byte[0][],
      families);
    UTIL.getAdmin().disableTable(tableName);

    // Delete the table (this should create a recovery snapshot)
    long procId = ProcedureTestingUtility.submitAndWait(procExec,
      new DeleteTableProcedure(procExec.getEnvironment(), tableName));
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);

    // Verify table is deleted
    MasterProcedureTestingUtility.validateTableDeletion(getMaster(), tableName);

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

  // Create a procedure that will fail after snapshot creation
  public static class FailingDeleteTableProcedure extends DeleteTableProcedure {
    private boolean failOnce = false;

    public FailingDeleteTableProcedure() {
      super();
    }

    public FailingDeleteTableProcedure(MasterProcedureEnv env, TableName tableName) {
      super(env, tableName);
    }

    @Override
    protected Flow executeFromState(MasterProcedureEnv env, DeleteTableState state)
      throws InterruptedException, ProcedureSuspendedException {
      if (!failOnce && state == DeleteTableState.DELETE_TABLE_CLEAR_FS_LAYOUT) {
        failOnce = true;
        throw new RuntimeException("Simulated failure");
      }
      return super.executeFromState(env, state);
    }
  }

}
