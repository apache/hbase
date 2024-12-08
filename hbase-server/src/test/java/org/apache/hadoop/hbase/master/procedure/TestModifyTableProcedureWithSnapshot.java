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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.snapshot.UnknownSnapshotException;
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

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.ModifyTableState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;

@Category({ MasterTests.class, MediumTests.class })
public class TestModifyTableProcedureWithSnapshot extends TestSnapshottingTableDDLProcedureBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestModifyTableProcedureWithSnapshot.class);

  @Rule
  public TestName name = new TestName();

  private static final Logger LOG =
    LoggerFactory.getLogger(TestModifyTableProcedureWithSnapshot.class);

  @Test
  public void testModifyTableAddCF() throws Exception {
    final String cf1 = "cf1";
    final String cf2 = "cf2";
    // Create the test table
    TableName tableName = TableName.valueOf(name.getMethodName());
    ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    LOG.info("Creating {}", tableName);
    MasterProcedureTestingUtility.createTable(procExec, tableName, null, cf1);
    // Prepare the procedure and set a snapshot name for testing that we will check for later
    List<ColumnFamilyDescriptor> families = new ArrayList<>();
    families.add(ColumnFamilyDescriptorBuilder.of(cf1));
    families.add(ColumnFamilyDescriptorBuilder.of(cf2));
    TableDescriptor newTd =
      TableDescriptorBuilder.newBuilder(tableName).setColumnFamilies(families).build();
    ProcedurePrepareLatch latch = new ProcedurePrepareLatch.CompatibilityLatch();
    ModifyTableProcedure proc = new ModifyTableProcedure(procExec.getEnvironment(), newTd, latch);
    String snapshotName = makeSnapshotName(name);
    proc.setSnapshotName(snapshotName);
    // Execute the procedure
    LOG.info("Submitting ModifyTableProcedure for {}", tableName);
    ProcedureTestingUtility.submitAndWait(procExec, proc);
    latch.await();
    // Validate that we really did add the CF
    TableDescriptor currentTd = UTIL.getAdmin().getDescriptor(tableName);
    assertTrue("cf2 should exist", currentTd.hasColumnFamily(Bytes.toBytes(cf2)));
    // We did not take a destructive action so there should not be a snapshot
    assertSnapshotAbsent(snapshotName);
  }

  @Test
  public void testModifyTableRemoveCF() throws Exception {
    final String cf1 = "cf1";
    final String cf2 = "cf2";
    // Create the test table
    TableName tableName = TableName.valueOf(name.getMethodName());
    ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    LOG.info("Creating {}", tableName);
    MasterProcedureTestingUtility.createTable(procExec, tableName, null, cf1, cf2);
    // Prepare the procedure and set a snapshot name for testing that we will check for later
    List<ColumnFamilyDescriptor> families = new ArrayList<>();
    families.add(ColumnFamilyDescriptorBuilder.of(cf1)); // Only cf1, we will drop cf2
    TableDescriptor newTd =
      TableDescriptorBuilder.newBuilder(tableName).setColumnFamilies(families).build();
    ProcedurePrepareLatch latch = new ProcedurePrepareLatch.CompatibilityLatch();
    ModifyTableProcedure proc = new ModifyTableProcedure(procExec.getEnvironment(), newTd, latch);
    String snapshotName = makeSnapshotName(name);
    proc.setSnapshotName(snapshotName);
    // Execute the procedure
    LOG.info("Submitting ModifyTableProcedure for {}", tableName);
    ProcedureTestingUtility.submitAndWait(procExec, proc);
    latch.await();
    // Validate that we really did remove the CF
    TableDescriptor currentTd = UTIL.getAdmin().getDescriptor(tableName);
    assertFalse("cf2 should not exist", currentTd.hasColumnFamily(Bytes.toBytes(cf2)));
    // We took a destructive action so there should be a recovery snapshot
    assertSnapshotExists(snapshotName);
    // The recovery snapshot should have the correct TTL
    assertSnapshotHasTtl(snapshotName, ttlForTest);
  }

  @Test
  public void testModifyTableRemoveCFSnapshotRollback() throws Exception {
    final String cf1 = "cf1";
    final String cf2 = "cf2";
    // Create the test table
    TableName tableName = TableName.valueOf(name.getMethodName());
    ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    LOG.info("Creating {}", tableName);
    MasterProcedureTestingUtility.createTable(procExec, tableName, null, cf1, cf2);
    // Prepare the procedure and set a snapshot name for testing that we will check for later.
    // FaultyModifyTableProcedure faults in MODIFY_TABLE_SNAPSHOT, triggering rollback from that
    // state.
    List<ColumnFamilyDescriptor> families = new ArrayList<>();
    families.add(ColumnFamilyDescriptorBuilder.of(cf1)); // Only cf1, we will drop cf2
    TableDescriptor newTd =
      TableDescriptorBuilder.newBuilder(tableName).setColumnFamilies(families).build();
    ProcedurePrepareLatch latch = new ProcedurePrepareLatch.CompatibilityLatch();
    ModifyTableProcedure proc =
      new FaultyModifyTableProcedure(procExec.getEnvironment(), newTd, latch);
    String snapshotName = makeSnapshotName(name);
    proc.setSnapshotName(snapshotName);
    // Execute the procedure
    LOG.info("Submitting FaultyModifyTableProcedure for {}", tableName);
    ProcedureTestingUtility.submitAndWait(procExec, proc);
    try {
      latch.await();
    } catch (Exception e) {
      // Expected
    }
    // We should have aborted and rolled back the ModifyTableProcedure
    TableDescriptor currentTd = UTIL.getAdmin().getDescriptor(tableName);
    assertTrue("cf2 should exist", currentTd.hasColumnFamily(Bytes.toBytes(cf2)));
    // Rollback should have deleted the snapshot
    assertSnapshotAbsent(snapshotName);
  }

  @Test
  public void testRecoveryAfterRemoveCF() throws Exception {
    final String cf1 = "cf1";
    final String cf2 = "cf2";
    final int rows = 100;
    // Create the test table
    TableName tableName = TableName.valueOf(name.getMethodName());
    ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    LOG.info("Creating {}", tableName);
    MasterProcedureTestingUtility.createTable(procExec, tableName, null, cf1, cf2);
    // Load data
    insertData(tableName, rows, 1, cf1, cf2);
    assertEquals("Table load failed", rows, countRows(tableName, cf1, cf2));
    // Prepare the procedure and set a snapshot name for testing that we will check for later
    List<ColumnFamilyDescriptor> families = new ArrayList<>();
    families.add(ColumnFamilyDescriptorBuilder.of(cf1)); // Only cf1, we will drop cf2
    TableDescriptor newTd =
      TableDescriptorBuilder.newBuilder(tableName).setColumnFamilies(families).build();
    ProcedurePrepareLatch latch = new ProcedurePrepareLatch.CompatibilityLatch();
    ModifyTableProcedure proc = new ModifyTableProcedure(procExec.getEnvironment(), newTd, latch);
    String snapshotName = makeSnapshotName(name);
    proc.setSnapshotName(snapshotName);
    // Execute the procedure
    LOG.info("Submitting ModifyTableProcedure for {}", tableName);
    ProcedureTestingUtility.submitAndWait(procExec, proc);
    latch.await();
    // Validate that we really did remove the CF
    TableDescriptor currentTd = UTIL.getAdmin().getDescriptor(tableName);
    assertFalse("cf2 should not exist", currentTd.hasColumnFamily(Bytes.toBytes(cf2)));
    // We took a destructive action so there should be a recovery snapshot
    assertSnapshotExists(snapshotName);
    // The data in cf2 no longer exists but we should still check cf1
    assertEquals("Data is also missing from cf1?", rows, countRows(tableName, cf1));
    // Restore from snapshot
    LOG.info("Disabling {}", tableName);
    UTIL.getAdmin().disableTable(tableName);
    LOG.info("Restoring {}", tableName);
    UTIL.getAdmin().restoreSnapshot(snapshotName, false);
    // Validate we recovered the data
    LOG.info("Enabling {}", tableName);
    UTIL.getAdmin().enableTable(tableName);
    assertEquals("Restore from snapshot failed", rows, countRows(tableName, cf1));
  }

  public static class FaultyModifyTableProcedure extends ModifyTableProcedure {

    public FaultyModifyTableProcedure() {
      super();
    }

    public FaultyModifyTableProcedure(MasterProcedureEnv env, TableDescriptor descriptor,
      ProcedurePrepareLatch latch) throws HBaseIOException {
      super(env, descriptor, latch);
    }

    @Override
    protected Flow executeFromState(MasterProcedureEnv env, ModifyTableState state)
      throws InterruptedException, ProcedureSuspendedException {
      switch (state) {
        case MODIFY_TABLE_SNAPSHOT:
          SnapshotDescription snapshot =
            SnapshotDescription.newBuilder().setName(getSnapshotName()).build();
          boolean isDone;
          try {
            isDone = env.getMasterServices().getSnapshotManager().isSnapshotDone(snapshot);
          } catch (UnknownSnapshotException e) {
            isDone = false;
          } catch (IOException e) {
            // Unexpected IOException
            LOG.error("Unexpected exception", e);
            throw new RuntimeException(e);
          }
          // Situation normal until we've taken the snapshot
          if (!isDone) {
            return super.executeFromState(env, state);
          } else {
            throw new RuntimeException("Injected fault");
          }
        default:
          return super.executeFromState(env, state);
      }
    }
  }

}
