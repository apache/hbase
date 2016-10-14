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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CategoryBasedTimeout;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos.CreateTableState;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos.DeleteTableState;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos.DisableTableState;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos.EnableTableState;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos.TruncateTableState;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;

@Category({MasterTests.class, LargeTests.class})
public class TestMasterFailoverWithProcedures {
  private static final Log LOG = LogFactory.getLog(TestMasterFailoverWithProcedures.class);

  @ClassRule
  public static final TestRule timeout =
      CategoryBasedTimeout.forClass(TestMasterFailoverWithProcedures.class);

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static void setupConf(Configuration conf) {
    // don't waste time retrying with the roll, the test is already slow enough.
    conf.setInt(WALProcedureStore.MAX_RETRIES_BEFORE_ROLL_CONF_KEY, 1);
    conf.setInt(WALProcedureStore.WAIT_BEFORE_ROLL_CONF_KEY, 0);
    conf.setInt(WALProcedureStore.ROLL_RETRIES_CONF_KEY, 1);
    conf.setInt(WALProcedureStore.MAX_SYNC_FAILURE_ROLL_CONF_KEY, 1);
  }

  @Before
  public void setup() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(2, 1);

    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    ProcedureTestingUtility.setToggleKillBeforeStoreUpdate(procExec, false);
    ProcedureTestingUtility.setKillBeforeStoreUpdate(procExec, false);
  }

  @After
  public void tearDown() throws Exception {
    try {
      UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      LOG.warn("failure shutting down cluster", e);
    }
  }

  // ==========================================================================
  //  Test Create Table
  // ==========================================================================
  @Test
  public void testCreateWithFailover() throws Exception {
    // TODO: Should we try every step? (master failover takes long time)
    // It is already covered by TestCreateTableProcedure
    // but without the master restart, only the executor/store is restarted.
    // Without Master restart we may not find bug in the procedure code
    // like missing "wait" for resources to be available (e.g. RS)
    testCreateWithFailoverAtStep(CreateTableState.CREATE_TABLE_ASSIGN_REGIONS.ordinal());
  }

  private void testCreateWithFailoverAtStep(final int step) throws Exception {
    final TableName tableName = TableName.valueOf("testCreateWithFailoverAtStep" + step);

    // create the table
    ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    ProcedureTestingUtility.setKillBeforeStoreUpdate(procExec, true);
    ProcedureTestingUtility.setToggleKillBeforeStoreUpdate(procExec, true);

    // Start the Create procedure && kill the executor
    byte[][] splitKeys = null;
    HTableDescriptor htd = MasterProcedureTestingUtility.createHTD(tableName, "f1", "f2");
    HRegionInfo[] regions = ModifyRegionUtils.createHRegionInfos(htd, splitKeys);
    long procId = procExec.submitProcedure(
        new CreateTableProcedure(procExec.getEnvironment(), htd, regions));
    testRecoveryAndDoubleExecution(UTIL, procId, step, CreateTableState.values());

    MasterProcedureTestingUtility.validateTableCreation(
        UTIL.getHBaseCluster().getMaster(), tableName, regions, "f1", "f2");
  }

  // ==========================================================================
  //  Test Delete Table
  // ==========================================================================
  @Test
  public void testDeleteWithFailover() throws Exception {
    // TODO: Should we try every step? (master failover takes long time)
    // It is already covered by TestDeleteTableProcedure
    // but without the master restart, only the executor/store is restarted.
    // Without Master restart we may not find bug in the procedure code
    // like missing "wait" for resources to be available (e.g. RS)
    testDeleteWithFailoverAtStep(DeleteTableState.DELETE_TABLE_UNASSIGN_REGIONS.ordinal());
  }

  private void testDeleteWithFailoverAtStep(final int step) throws Exception {
    final TableName tableName = TableName.valueOf("testDeleteWithFailoverAtStep" + step);

    // create the table
    byte[][] splitKeys = null;
    HRegionInfo[] regions = MasterProcedureTestingUtility.createTable(
        getMasterProcedureExecutor(), tableName, splitKeys, "f1", "f2");
//    Path tableDir = FSUtils.getTableDir(getRootDir(), tableName);
    MasterProcedureTestingUtility.validateTableCreation(
        UTIL.getHBaseCluster().getMaster(), tableName, regions, "f1", "f2");
    UTIL.getHBaseAdmin().disableTable(tableName);

    ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    ProcedureTestingUtility.setKillBeforeStoreUpdate(procExec, true);
    ProcedureTestingUtility.setToggleKillBeforeStoreUpdate(procExec, true);

    // Start the Delete procedure && kill the executor
    long procId = procExec.submitProcedure(
        new DeleteTableProcedure(procExec.getEnvironment(), tableName));
    testRecoveryAndDoubleExecution(UTIL, procId, step, DeleteTableState.values());

//    MasterProcedureTestingUtility.validateTableDeletion(
//        UTIL.getHBaseCluster().getMaster(), tableName);
  }

  // ==========================================================================
  //  Test Truncate Table
  // ==========================================================================
  @Test
  public void testTruncateWithFailover() throws Exception {
    // TODO: Should we try every step? (master failover takes long time)
    // It is already covered by TestTruncateTableProcedure
    // but without the master restart, only the executor/store is restarted.
    // Without Master restart we may not find bug in the procedure code
    // like missing "wait" for resources to be available (e.g. RS)
    testTruncateWithFailoverAtStep(true, TruncateTableState.TRUNCATE_TABLE_ADD_TO_META.ordinal());
  }

  private void testTruncateWithFailoverAtStep(final boolean preserveSplits, final int step)
      throws Exception {
    final TableName tableName = TableName.valueOf("testTruncateWithFailoverAtStep" + step);

    // create the table
    final String[] families = new String[] { "f1", "f2" };
    final byte[][] splitKeys = new byte[][] {
        Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("c")
    };
    HRegionInfo[] regions = MasterProcedureTestingUtility.createTable(
        getMasterProcedureExecutor(), tableName, splitKeys, families);
    // load and verify that there are rows in the table
    MasterProcedureTestingUtility.loadData(
        UTIL.getConnection(), tableName, 100, splitKeys, families);
    assertEquals(100, UTIL.countRows(tableName));
    // disable the table
    UTIL.getHBaseAdmin().disableTable(tableName);

    ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the Truncate procedure && kill the executor
    long procId = procExec.submitProcedure(
        new TruncateTableProcedure(procExec.getEnvironment(), tableName, preserveSplits));
    testRecoveryAndDoubleExecution(UTIL, procId, step, TruncateTableState.values());

    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, false);
    UTIL.waitUntilAllRegionsAssigned(tableName);

    // validate the table regions and layout
    regions = UTIL.getHBaseAdmin().getTableRegions(tableName).toArray(new HRegionInfo[0]);
    if (preserveSplits) {
      assertEquals(1 + splitKeys.length, regions.length);
    } else {
      assertEquals(1, regions.length);
    }
    MasterProcedureTestingUtility.validateTableCreation(
        UTIL.getHBaseCluster().getMaster(), tableName, regions, families);

    // verify that there are no rows in the table
    assertEquals(0, UTIL.countRows(tableName));

    // verify that the table is read/writable
    MasterProcedureTestingUtility.loadData(
        UTIL.getConnection(), tableName, 50, splitKeys, families);
    assertEquals(50, UTIL.countRows(tableName));
  }

  // ==========================================================================
  //  Test Disable Table
  // ==========================================================================
  @Test
  public void testDisableTableWithFailover() throws Exception {
    // TODO: Should we try every step? (master failover takes long time)
    // It is already covered by TestDisableTableProcedure
    // but without the master restart, only the executor/store is restarted.
    // Without Master restart we may not find bug in the procedure code
    // like missing "wait" for resources to be available (e.g. RS)
    testDisableTableWithFailoverAtStep(
        DisableTableState.DISABLE_TABLE_MARK_REGIONS_OFFLINE.ordinal());
  }

  private void testDisableTableWithFailoverAtStep(final int step) throws Exception {
    final TableName tableName = TableName.valueOf("testDisableTableWithFailoverAtStep" + step);

    // create the table
    final byte[][] splitKeys = new byte[][] {
        Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("c")
    };
    MasterProcedureTestingUtility.createTable(
        getMasterProcedureExecutor(), tableName, splitKeys, "f1", "f2");

    ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the Delete procedure && kill the executor
    long procId = procExec.submitProcedure(
        new DisableTableProcedure(procExec.getEnvironment(), tableName, false));
    testRecoveryAndDoubleExecution(UTIL, procId, step, DisableTableState.values());

    MasterProcedureTestingUtility.validateTableIsDisabled(
        UTIL.getHBaseCluster().getMaster(), tableName);
  }

  // ==========================================================================
  //  Test Enable Table
  // ==========================================================================
  @Test
  public void testEnableTableWithFailover() throws Exception {
    // TODO: Should we try every step? (master failover takes long time)
    // It is already covered by TestEnableTableProcedure
    // but without the master restart, only the executor/store is restarted.
    // Without Master restart we may not find bug in the procedure code
    // like missing "wait" for resources to be available (e.g. RS)
    testEnableTableWithFailoverAtStep(
        EnableTableState.ENABLE_TABLE_MARK_REGIONS_ONLINE.ordinal());
  }

  private void testEnableTableWithFailoverAtStep(final int step) throws Exception {
    final TableName tableName = TableName.valueOf("testEnableTableWithFailoverAtStep" + step);

    // create the table
    final byte[][] splitKeys = new byte[][] {
        Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("c")
    };
    MasterProcedureTestingUtility.createTable(
        getMasterProcedureExecutor(), tableName, splitKeys, "f1", "f2");
    UTIL.getHBaseAdmin().disableTable(tableName);

    ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the Delete procedure && kill the executor
    long procId = procExec.submitProcedure(
        new EnableTableProcedure(procExec.getEnvironment(), tableName, false));
    testRecoveryAndDoubleExecution(UTIL, procId, step, EnableTableState.values());

    MasterProcedureTestingUtility.validateTableIsEnabled(
        UTIL.getHBaseCluster().getMaster(), tableName);
  }

  // ==========================================================================
  //  Test Helpers
  // ==========================================================================
  public static <TState> void testRecoveryAndDoubleExecution(final HBaseTestingUtility testUtil,
      final long procId, final int lastStepBeforeFailover, TState[] states) throws Exception {
    ProcedureExecutor<MasterProcedureEnv> procExec =
        testUtil.getHBaseCluster().getMaster().getMasterProcedureExecutor();
    ProcedureTestingUtility.waitProcedure(procExec, procId);

    for (int i = 0; i < lastStepBeforeFailover; ++i) {
      LOG.info("Restart "+ i +" exec state: " + states[i]);
      ProcedureTestingUtility.assertProcNotYetCompleted(procExec, procId);
      ProcedureTestingUtility.restart(procExec);
      ProcedureTestingUtility.waitProcedure(procExec, procId);
    }
    ProcedureTestingUtility.assertProcNotYetCompleted(procExec, procId);

    LOG.info("Trigger master failover");
    MasterProcedureTestingUtility.masterFailover(testUtil);

    procExec = testUtil.getHBaseCluster().getMaster().getMasterProcedureExecutor();
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);
  }

  // ==========================================================================
  //  Helpers
  // ==========================================================================
  private ProcedureExecutor<MasterProcedureEnv> getMasterProcedureExecutor() {
    return UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
  }

//  private Path getRootDir() {
//    return UTIL.getHBaseCluster().getMaster().getMasterStorage().getRootDir();
//  }
}
