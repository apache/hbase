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
import static org.junit.Assert.fail;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.procedure.AbstractSnapshottingStateMachineTableProcedure.NoLockDisabledTableSnapshotHandler;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.snapshot.UnknownSnapshotException;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.DeleteTableState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;

@Category({ MasterTests.class, MediumTests.class })
public class TestDeleteTableProcedureWithSnapshot extends TestSnapshottingTableDDLProcedureBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestDeleteTableProcedureWithSnapshot.class);

  @Rule
  public TestName name = new TestName();

  private static final Logger LOG =
    LoggerFactory.getLogger(TestDeleteTableProcedureWithSnapshot.class);

  @Test
  public void testDeleteNotExistentTable() throws Exception {
    TableName tableName = TableName.valueOf(name.getMethodName());
    ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    // Prepare the procedure and set a snapshot name for testing that we will check for later
    ProcedurePrepareLatch latch = new ProcedurePrepareLatch.CompatibilityLatch();
    DeleteTableProcedure proc =
      new DeleteTableProcedure(procExec.getEnvironment(), tableName, latch);
    String snapshotName = makeSnapshotName(name);
    proc.setSnapshotName(snapshotName);
    ProcedureTestingUtility.submitAndWait(procExec, proc);
    try {
      latch.await();
      fail("TableNotFoundException not thrown");
    } catch (TableNotFoundException e) {
      // Expected
    } catch (IOException e) {
      LOG.error("Unexpected IOException", e);
      fail("Unexpected exception");
    }
    // We failed in preflight, there should be no recovery snapshot
    assertSnapshotAbsent(snapshotName);
  }

  @Test
  public void testDelete() throws Exception {
    final String cf1 = "cf1";
    // Create the test table
    TableName tableName = TableName.valueOf(name.getMethodName());
    ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    LOG.info("Creating {}", tableName);
    MasterProcedureTestingUtility.createTable(procExec, tableName, null, cf1);
    // Prepare the procedure and set a snapshot name for testing that we will check for later
    ProcedurePrepareLatch latch = new ProcedurePrepareLatch.CompatibilityLatch();
    DeleteTableProcedure proc =
      new DeleteTableProcedure(procExec.getEnvironment(), tableName, latch);
    String snapshotName = makeSnapshotName(name);
    proc.setSnapshotName(snapshotName);
    // Delete table procedure requires the table to be disabled
    LOG.info("Disabling {}", tableName);
    UTIL.getAdmin().disableTable(tableName);
    // Execute the procedure
    LOG.info("Submitting DeleteTableProcedure for {}", tableName);
    ProcedureTestingUtility.submitAndWait(procExec, proc);
    latch.await();
    // Validate that the table was deleted
    MasterProcedureTestingUtility.validateTableDeletion(getMaster(), tableName);
    // There should be a recovery snapshot
    assertSnapshotExists(snapshotName);
    // The recovery snapshot should have the correct TTL
    assertSnapshotHasTtl(snapshotName, ttlForTest);
  }

  @Test
  public void testDeleteSnapshotRollback() throws Exception {
    final String cf1 = "cf1";
    // Create the test table
    TableName tableName = TableName.valueOf(name.getMethodName());
    ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    LOG.info("Creating {}", tableName);
    MasterProcedureTestingUtility.createTable(procExec, tableName, null, cf1);
    // Prepare the procedure and set a snapshot name for testing that we will check for later.
    // FaultyDeleteTableProcedure faults in DELETE_TABLE_SNAPSHOT, triggering rollback from that
    // state.
    ProcedurePrepareLatch latch = new ProcedurePrepareLatch.CompatibilityLatch();
    DeleteTableProcedure proc =
      new FaultyDeleteTableProcedure(procExec.getEnvironment(), tableName, latch);
    String snapshotName = makeSnapshotName(name);
    proc.setSnapshotName(snapshotName);
    // Delete table procedure requires the table to be disabled
    LOG.info("Disabling {}", tableName);
    UTIL.getAdmin().disableTable(tableName);
    // Execute the procedure
    LOG.info("Submitting FaultyDeleteTableProcedure for {}", tableName);
    ProcedureTestingUtility.submitAndWait(procExec, proc);
    try {
      latch.await();
    } catch (IOException e) {
      // Expected
    }
    // We should have aborted and rolled back the DeleteTableProcedure
    // The table exists and is still disabled
    MasterProcedureTestingUtility.validateTableIsDisabled(getMaster(), tableName);
    // It should be possible to enable it
    LOG.info("Enabling {}", tableName);
    UTIL.getAdmin().enableTable(tableName);
    // Rollback should have deleted the snapshot
    assertSnapshotAbsent(snapshotName);
  }

  @Test
  public void testRecoveryAfterDelete() throws Exception {
    final String cf1 = "cf1";
    final int rows = 100;
    // Create the test table
    final TableName tableName = TableName.valueOf(name.getMethodName());
    ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    LOG.info("Creating {}", tableName);
    MasterProcedureTestingUtility.createTable(procExec, tableName, null, cf1);
    // Load data
    insertData(tableName, rows, 1, cf1);
    assertEquals("Table load failed", rows, countRows(tableName, cf1));
    // Prepare the procedure and set a snapshot name for testing that we will check for later.
    ProcedurePrepareLatch latch = new ProcedurePrepareLatch.CompatibilityLatch();
    DeleteTableProcedure proc =
      new DeleteTableProcedure(procExec.getEnvironment(), tableName, latch);
    String snapshotName = makeSnapshotName(name);
    proc.setSnapshotName(snapshotName);
    // Delete table procedure requires the table to be disabled
    LOG.info("Disabling {}", tableName);
    UTIL.getAdmin().disableTable(tableName);
    // Execute the procedure
    LOG.info("Submitting DeleteTableProcedure for {}", tableName);
    ProcedureTestingUtility.submitAndWait(procExec, proc);
    latch.await();
    // Validate that the table was deleted
    MasterProcedureTestingUtility.validateTableDeletion(getMaster(), tableName);
    // There should be a recovery snapshot
    assertSnapshotExists(snapshotName);
    // Restore the table
    LOG.info("Restoring {} from {}", tableName, snapshotName);
    UTIL.getAdmin().restoreSnapshot(snapshotName, false);
    // The data should have been restored
    assertEquals("Restore from snapshot failed", rows, countRows(tableName, cf1));
  }

  @Test
  public void testDeleteWithSlowSnapshot() throws Exception {
    final String cf1 = "cf1";
    // Create the test table
    TableName tableName = TableName.valueOf(name.getMethodName());
    ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    LOG.info("Creating {}", tableName);
    MasterProcedureTestingUtility.createTable(procExec, tableName, null, cf1);
    // Prepare the procedure and set a snapshot name for testing that we will check for later
    ProcedurePrepareLatch latch = new ProcedurePrepareLatch.CompatibilityLatch();
    DeleteTableProcedure proc =
      new SlowSnapshotDeleteTableProcedure(procExec.getEnvironment(), tableName, latch);
    String snapshotName = makeSnapshotName(name);
    proc.setSnapshotName(snapshotName);
    // Delete table procedure requires the table to be disabled
    LOG.info("Disabling {}", tableName);
    UTIL.getAdmin().disableTable(tableName);
    // Execute the procedure
    LOG.info("Submitting DeleteTableProcedure for {}", tableName);
    ProcedureTestingUtility.submitAndWait(procExec, proc);
    latch.await();
    // Validate that the table was deleted
    MasterProcedureTestingUtility.validateTableDeletion(getMaster(), tableName);
    // There should be a recovery snapshot
    assertSnapshotExists(snapshotName);
    // The recovery snapshot should have the correct TTL
    assertSnapshotHasTtl(snapshotName, ttlForTest);
  }

  /**
   * A DeleteTableProcedure for testing that will throw an exception at the end of the
   * DELETE_STATE_SNAPSHOT state processing just after the snapshot is completed, so we can test
   * rollback from that state.
   */
  public static class FaultyDeleteTableProcedure extends DeleteTableProcedure {

    public FaultyDeleteTableProcedure() {
      super();
    }

    public FaultyDeleteTableProcedure(MasterProcedureEnv env, TableName tableName,
      ProcedurePrepareLatch latch) {
      super(env, tableName, latch);
    }

    @Override
    protected Flow executeFromState(final MasterProcedureEnv env, DeleteTableState state)
      throws InterruptedException, ProcedureSuspendedException {
      switch (state) {
        case DELETE_TABLE_SNAPSHOT:
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

  /**
   * A DeleteTableProcedure that will pause for 10 seconds during snapshot execution, to simulate
   * the taking of a snapshot that requires some time to complete.
   */
  public static class SlowSnapshotDeleteTableProcedure extends DeleteTableProcedure {

    public SlowSnapshotDeleteTableProcedure() {
      super();
    }

    public SlowSnapshotDeleteTableProcedure(MasterProcedureEnv env, TableName tableName,
      ProcedurePrepareLatch latch) {
      super(env, tableName, latch);
    }

    @Override
    protected void takeSnapshot(SnapshotDescription snapshot)
      throws ProcedureSuspendedException, IOException {
      // Set our snapshot handler to the test type below that will delay the snapshot for a while.
      setSnapshotHandler(SlowNoLockDisabledTableSnapshotHandler.class, snapshot);
      super.takeSnapshot(snapshot);
    }

  }

  /**
   * A snapshot handler that will introduce a 10 second delay in process() to simulate the taking of
   * a snapshot that requires some time to complete.
   */
  public static class SlowNoLockDisabledTableSnapshotHandler
    extends NoLockDisabledTableSnapshotHandler {

    public SlowNoLockDisabledTableSnapshotHandler(SnapshotDescription snapshot,
      MasterServices masterServices, SnapshotManager snapshotManager) throws IOException {
      super(snapshot, masterServices, snapshotManager);
    }

    @Override
    public void process() {
      // First, let's sleep 10 seconds. We want to delay the procedure for long enough so it will
      // go through a few suspend and wakeup cycles.
      try {
        Thread.sleep(10 * 1000);
      } catch (InterruptedException e) {
        // Restore interrupt status
        Thread.currentThread().interrupt();
      }
      // Then we can let the snapshot handler do its thing
      super.process();
    }

  }

}
