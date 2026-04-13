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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.ModifyTableState;

@Tag(MasterTests.TAG)
@Tag(LargeTests.TAG)
public class TestModifyTableProcedureWithRecovery extends TestTableDDLProcedureBase {
  private String testMethodName;

  @BeforeEach
  public void setTestMethod(TestInfo testInfo) {
    testMethodName = testInfo.getTestMethod().get().getName();
  }

  @BeforeAll
  public static void setupCluster() throws Exception {
    // Enable recovery snapshots
    TestTableDDLProcedureBase.setupConf(UTIL.getConfiguration());
    UTIL.getConfiguration().setBoolean(HConstants.SNAPSHOT_BEFORE_DESTRUCTIVE_ACTION_ENABLED_KEY,
      true);
    UTIL.startMiniCluster(1);
  }

  @AfterAll
  public static void cleanupTest() throws Exception {
    TestTableDDLProcedureBase.cleanupTest();
  }

  @Test
  public void testRecoverySnapshotRollback() throws Exception {
    final TableName tableName = TableName.valueOf(testMethodName);
    final String cf1 = "cf1";
    final String cf2 = "cf2";
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    // Create table with multiple column families
    MasterProcedureTestingUtility.createTable(procExec, tableName, null, cf1, cf2);
    MasterProcedureTestingUtility.loadData(UTIL.getConnection(), tableName, 100, new byte[0][],
      new String[] { cf1, cf2 });
    UTIL.getAdmin().disableTable(tableName);

    // Create a procedure that will fail - modify to delete a column family
    // but simulate failure after snapshot creation
    // Modify table to remove cf2 (which should trigger recovery snapshot)
    TableDescriptor originalHtd = UTIL.getAdmin().getDescriptor(tableName);
    TableDescriptor modifiedHtd =
      TableDescriptorBuilder.newBuilder(originalHtd).removeColumnFamily(cf2.getBytes()).build();

    // Submit the failing procedure
    long procId = procExec
      .submitProcedure(new FailingModifyTableProcedure(procExec.getEnvironment(), modifiedHtd));

    // Wait for procedure to complete (should fail)
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    Procedure<MasterProcedureEnv> result = procExec.getResult(procId);
    assertTrue(result.isFailed(), "Procedure should have failed");

    // Verify no recovery snapshots remain after rollback
    boolean snapshotFound = false;
    for (SnapshotDescription snapshot : UTIL.getAdmin().listSnapshots()) {
      if (snapshot.getName().startsWith("auto_" + tableName.getNameAsString())) {
        snapshotFound = true;
        break;
      }
    }
    assertTrue(!snapshotFound, "Recovery snapshot should have been cleaned up during rollback");
  }

  @Test
  public void testRecoverySnapshotAndRestore() throws Exception {
    final TableName tableName = TableName.valueOf(testMethodName);
    final TableName restoredTableName = TableName.valueOf(testMethodName + "_restored");
    final String cf1 = "cf1";
    final String cf2 = "cf2";
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    // Create table with multiple column families
    MasterProcedureTestingUtility.createTable(procExec, tableName, null, cf1, cf2);
    MasterProcedureTestingUtility.loadData(UTIL.getConnection(), tableName, 100, new byte[0][],
      new String[] { cf1, cf2 });
    UTIL.getAdmin().disableTable(tableName);

    // Modify table to remove cf2 (which should trigger recovery snapshot)
    TableDescriptor originalHtd = UTIL.getAdmin().getDescriptor(tableName);
    TableDescriptor modifiedHtd =
      TableDescriptorBuilder.newBuilder(originalHtd).removeColumnFamily(cf2.getBytes()).build();

    long procId = ProcedureTestingUtility.submitAndWait(procExec,
      new ModifyTableProcedure(procExec.getEnvironment(), modifiedHtd));
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);

    // Verify table modification was successful
    TableDescriptor currentHtd = UTIL.getAdmin().getDescriptor(tableName);
    assertEquals(1, currentHtd.getColumnFamilyNames().size(), "Should have one column family");
    assertTrue(currentHtd.hasColumnFamily(cf1.getBytes()), "Should only have cf1");

    // Find the recovery snapshot
    String recoverySnapshotName = null;
    for (SnapshotDescription snapshot : UTIL.getAdmin().listSnapshots()) {
      if (snapshot.getName().startsWith("auto_" + tableName.getNameAsString())) {
        recoverySnapshotName = snapshot.getName();
        break;
      }
    }
    assertTrue(recoverySnapshotName != null, "Recovery snapshot should exist");

    // Restore from snapshot by cloning to a new table
    UTIL.getAdmin().cloneSnapshot(recoverySnapshotName, restoredTableName);
    UTIL.waitUntilAllRegionsAssigned(restoredTableName);

    // Verify restored table has original structure with both column families
    TableDescriptor restoredHtd = UTIL.getAdmin().getDescriptor(restoredTableName);
    assertEquals(2, restoredHtd.getColumnFamilyNames().size(), "Should have two column families");
    assertTrue(restoredHtd.hasColumnFamily(cf1.getBytes()), "Should have cf1");
    assertTrue(restoredHtd.hasColumnFamily(cf2.getBytes()), "Should have cf2");

    // Clean up the cloned table
    UTIL.getAdmin().disableTable(restoredTableName);
    UTIL.getAdmin().deleteTable(restoredTableName);
  }

  public static class FailingModifyTableProcedure extends ModifyTableProcedure {
    private boolean failOnce = false;

    public FailingModifyTableProcedure() {
      super();
    }

    public FailingModifyTableProcedure(MasterProcedureEnv env, TableDescriptor newTableDescriptor)
      throws HBaseIOException {
      super(env, newTableDescriptor);
    }

    @Override
    protected Flow executeFromState(MasterProcedureEnv env, ModifyTableState state)
      throws InterruptedException {
      if (!failOnce && state == ModifyTableState.MODIFY_TABLE_CLOSE_EXCESS_REPLICAS) {
        failOnce = true;
        throw new RuntimeException("Simulated failure");
      }
      return super.executeFromState(env, state);
    }
  }
}
