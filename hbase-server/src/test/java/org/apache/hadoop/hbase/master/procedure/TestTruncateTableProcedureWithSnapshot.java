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

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.TableName;
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

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.TruncateTableState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;

@Category({ MasterTests.class, MediumTests.class })
public class TestTruncateTableProcedureWithSnapshot extends TestSnapshottingTableDDLProcedureBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestTruncateTableProcedureWithSnapshot.class);

  @Rule
  public TestName name = new TestName();

  private static final Logger LOG =
    LoggerFactory.getLogger(TestTruncateTableProcedureWithSnapshot.class);

  @Test
  public void testTruncate() throws Exception {
    final String cf1 = "cf1";
    final int rows = 100;
    // Create the test table
    TableName tableName = TableName.valueOf(name.getMethodName());
    ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    LOG.info("Creating {}", tableName);
    MasterProcedureTestingUtility.createTable(procExec, tableName, null, cf1);
    // Load data
    insertData(tableName, rows, 1, cf1);
    assertEquals("Table load failed", rows, countRows(tableName, cf1));
    // Truncate table procedure requires the table to be disabled
    LOG.info("Disabling {}", tableName);
    UTIL.getAdmin().disableTable(tableName);
    // Prepare the procedure and set a snapshot name for testing that we will check for later
    ProcedurePrepareLatch latch = new ProcedurePrepareLatch.CompatibilityLatch();
    TruncateTableProcedure proc =
      new TruncateTableProcedure(procExec.getEnvironment(), tableName, false, latch);
    String snapshotName = makeSnapshotName(name);
    proc.setSnapshotName(snapshotName);
    // Execute the procedure
    LOG.info("Submitting TruncateTableProcedure for {}", tableName);
    ProcedureTestingUtility.submitAndWait(procExec, proc);
    latch.await();
    // The table should have been truncated
    assertEquals("Table was not truncated", 0, countRows(tableName, cf1));
    // There should be a recovery snapshot
    assertSnapshotExists(snapshotName);
    // The recovery snapshot should have the correct TTL
    assertSnapshotHasTtl(snapshotName, ttlForTest);
  }

  @Test
  public void testTruncateSnapshotRollback() throws Exception {
    final String cf1 = "cf1";
    final int rows = 100;
    // Create the test table
    TableName tableName = TableName.valueOf(name.getMethodName());
    ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    LOG.info("Creating {}", tableName);
    MasterProcedureTestingUtility.createTable(procExec, tableName, null, cf1);
    // Load data
    insertData(tableName, rows, 1, cf1);
    assertEquals("Table load failed", rows, countRows(tableName, cf1));
    // Truncate table procedure requires the table to be disabled
    LOG.info("Disabling {}", tableName);
    UTIL.getAdmin().disableTable(tableName);
    // Prepare the procedure and set a snapshot name for testing that we will check for later.
    // FaultyTruncateTableProcedure faults in TRUNCATE_TABLE_SNAPSHOT, triggering rollback from
    // that state.
    ProcedurePrepareLatch latch = new ProcedurePrepareLatch.CompatibilityLatch();
    TruncateTableProcedure proc =
      new FaultyTruncateTableProcedure(procExec.getEnvironment(), tableName, false, latch);
    String snapshotName = makeSnapshotName(name);
    // Execute the procedure
    LOG.info("Submitting FaultyTruncateTableProcedure for {}", tableName);
    ProcedureTestingUtility.submitAndWait(procExec, proc);
    try {
      latch.await();
    } catch (IOException e) {
      // Expected
    }
    // We should have aborted and rolled back the TruncateTableProcedure
    // The table should not have been truncated
    UTIL.getAdmin().enableTable(tableName);
    assertEquals("Table was truncated", rows, countRows(tableName, cf1));
    // Rollback should have deleted the snapshot
    assertSnapshotAbsent(snapshotName);
  }

  @Test
  public void testRecoveryAfterTruncate() throws Exception {
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
    // Truncate table procedure requires the table to be disabled
    LOG.info("Disabling {}", tableName);
    UTIL.getAdmin().disableTable(tableName);
    // Prepare the procedure and set a snapshot name for testing that we will check for later.
    ProcedurePrepareLatch latch = new ProcedurePrepareLatch.CompatibilityLatch();
    TruncateTableProcedure proc =
      new TruncateTableProcedure(procExec.getEnvironment(), tableName, false, latch);
    String snapshotName = makeSnapshotName(name);
    proc.setSnapshotName(snapshotName);
    // Execute the procedure
    LOG.info("Submitting TruncateTableProcedure for {}", tableName);
    ProcedureTestingUtility.submitAndWait(procExec, proc);
    latch.await();
    // There should be a recovery snapshot
    assertSnapshotExists(snapshotName);
    // The table should have been truncated
    assertEquals("Table was not truncated", 0, countRows(tableName, cf1));
    // Restore the table
    LOG.info("Disabling {}", tableName);
    UTIL.getAdmin().disableTable(tableName);
    LOG.info("Restoring snapshot {} for {}", snapshotName, tableName);
    UTIL.getAdmin().restoreSnapshot(snapshotName, false);
    LOG.info("Enabling {}", tableName);
    UTIL.getAdmin().enableTable(tableName);
    // The data should have been restored
    assertEquals("Restore from snapshot failed", rows, countRows(tableName, cf1));
  }

  public static class FaultyTruncateTableProcedure extends TruncateTableProcedure {

    public FaultyTruncateTableProcedure() {
      super();
    }

    public FaultyTruncateTableProcedure(MasterProcedureEnv env, TableName tableName,
      boolean preserveSplits, ProcedurePrepareLatch latch) throws HBaseIOException {
      super(env, tableName, preserveSplits, latch);
    }

    @Override
    protected Flow executeFromState(final MasterProcedureEnv env, TruncateTableState state)
      throws InterruptedException, ProcedureSuspendedException {
      switch (state) {
        case TRUNCATE_TABLE_SNAPSHOT:
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
