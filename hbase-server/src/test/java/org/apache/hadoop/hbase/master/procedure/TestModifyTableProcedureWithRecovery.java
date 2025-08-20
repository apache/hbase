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
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
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

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.ModifyTableState;

@Category({ MasterTests.class, LargeTests.class })
public class TestModifyTableProcedureWithRecovery extends TestTableDDLProcedureBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestModifyTableProcedureWithRecovery.class);

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
    assertEquals("Should have one column family", 1, currentHtd.getColumnFamilyNames().size());
    assertTrue("Should only have cf1", currentHtd.hasColumnFamily(cf1.getBytes()));

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

    // Verify restored table has original structure with both column families
    TableDescriptor restoredHtd = UTIL.getAdmin().getDescriptor(restoredTableName);
    assertEquals("Should have two column families", 2, restoredHtd.getColumnFamilyNames().size());
    assertTrue("Should have cf1", restoredHtd.hasColumnFamily(cf1.getBytes()));
    assertTrue("Should have cf2", restoredHtd.hasColumnFamily(cf2.getBytes()));

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
