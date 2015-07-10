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

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.TestProcedure;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;
import org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos.CreateTableState;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos.DeleteTableState;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos.DisableTableState;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos.EnableTableState;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos.TruncateTableState;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(LargeTests.class)
public class TestMasterFailoverWithProcedures {
  private static final Log LOG = LogFactory.getLog(TestMasterFailoverWithProcedures.class);

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static void setupConf(Configuration conf) {
    // don't waste time retrying with the roll, the test is already slow enough.
    conf.setInt("hbase.procedure.store.wal.max.retries.before.roll", 1);
    conf.setInt("hbase.procedure.store.wal.wait.before.roll", 0);
    conf.setInt("hbase.procedure.store.wal.max.roll.retries", 1);
    conf.setInt("hbase.procedure.store.wal.sync.failure.roll.max", 1);
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

  @Test(timeout=60000)
  public void testWalRecoverLease() throws Exception {
    final ProcedureStore masterStore = getMasterProcedureExecutor().getStore();
    assertTrue("expected WALStore for this test", masterStore instanceof WALProcedureStore);

    HMaster firstMaster = UTIL.getHBaseCluster().getMaster();
    // Abort Latch for the master store
    final CountDownLatch masterStoreAbort = new CountDownLatch(1);
    masterStore.registerListener(new ProcedureStore.ProcedureStoreListener() {
      @Override
      public void postSync() {}

      @Override
      public void abortProcess() {
        LOG.debug("Abort store of Master");
        masterStoreAbort.countDown();
      }
    });

    // startup a fake master the new WAL store will take the lease
    // and the active master should abort.
    HMaster backupMaster3 = Mockito.mock(HMaster.class);
    Mockito.doReturn(firstMaster.getConfiguration()).when(backupMaster3).getConfiguration();
    Mockito.doReturn(true).when(backupMaster3).isActiveMaster();
    final WALProcedureStore backupStore3 = new WALProcedureStore(firstMaster.getConfiguration(),
        firstMaster.getMasterFileSystem().getFileSystem(),
        ((WALProcedureStore)masterStore).getLogDir(),
        new MasterProcedureEnv.WALStoreLeaseRecovery(backupMaster3));
    // Abort Latch for the test store
    final CountDownLatch backupStore3Abort = new CountDownLatch(1);
    backupStore3.registerListener(new ProcedureStore.ProcedureStoreListener() {
      @Override
      public void postSync() {}

      @Override
      public void abortProcess() {
        LOG.debug("Abort store of backupMaster3");
        backupStore3Abort.countDown();
        backupStore3.stop(true);
      }
    });
    backupStore3.start(1);
    backupStore3.recoverLease();

    // Try to trigger a command on the master (WAL lease expired on the active one)
    HTableDescriptor htd = MasterProcedureTestingUtility.createHTD(TableName.valueOf("mtb"), "f");
    HRegionInfo[] regions = ModifyRegionUtils.createHRegionInfos(htd, null);
    LOG.debug("submit proc");
    try {
      getMasterProcedureExecutor().submitProcedure(
        new CreateTableProcedure(getMasterProcedureExecutor().getEnvironment(), htd, regions));
      fail("expected RuntimeException 'sync aborted'");
    } catch (RuntimeException e) {
      LOG.info("got " + e.getMessage());
    }
    LOG.debug("wait master store abort");
    masterStoreAbort.await();

    // Now the real backup master should start up
    LOG.debug("wait backup master to startup");
    waitBackupMaster(UTIL, firstMaster);
    assertEquals(true, firstMaster.isStopped());

    // wait the store in here to abort (the test will fail due to timeout if it doesn't)
    LOG.debug("wait the store to abort");
    backupStore3.getStoreTracker().setDeleted(1, false);
    try {
      backupStore3.delete(1);
      fail("expected RuntimeException 'sync aborted'");
    } catch (RuntimeException e) {
      LOG.info("got " + e.getMessage());
    }
    backupStore3Abort.await();
  }

  /**
   * Tests proper fencing in case the current WAL store is fenced
   */
  @Test
  public void testWALfencingWithoutWALRolling() throws IOException {
    testWALfencing(false);
  }

  /**
   * Tests proper fencing in case the current WAL store does not receive writes until after the
   * new WAL does a couple of WAL rolls.
   */
  @Test
  public void testWALfencingWithWALRolling() throws IOException {
    testWALfencing(true);
  }

  public void testWALfencing(boolean walRolls) throws IOException {
    final ProcedureStore procStore = getMasterProcedureExecutor().getStore();
    assertTrue("expected WALStore for this test", procStore instanceof WALProcedureStore);

    HMaster firstMaster = UTIL.getHBaseCluster().getMaster();

    // cause WAL rolling after a delete in WAL:
    firstMaster.getConfiguration().setLong("hbase.procedure.store.wal.roll.threshold", 1);

    HMaster backupMaster3 = Mockito.mock(HMaster.class);
    Mockito.doReturn(firstMaster.getConfiguration()).when(backupMaster3).getConfiguration();
    Mockito.doReturn(true).when(backupMaster3).isActiveMaster();
    final WALProcedureStore procStore2 = new WALProcedureStore(firstMaster.getConfiguration(),
        firstMaster.getMasterFileSystem().getFileSystem(),
        ((WALProcedureStore)procStore).getLogDir(),
        new MasterProcedureEnv.WALStoreLeaseRecovery(backupMaster3));

    // start a second store which should fence the first one out
    LOG.info("Starting new WALProcedureStore");
    procStore2.start(1);
    procStore2.recoverLease();

    // before writing back to the WAL store, optionally do a couple of WAL rolls (which causes
    // to delete the old WAL files).
    if (walRolls) {
      LOG.info("Inserting into second WALProcedureStore, causing WAL rolls");
      for (int i = 0; i < 512; i++) {
        // insert something to the second store then delete it, causing a WAL roll(s)
        Procedure proc2 = new TestProcedure(i);
        procStore2.insert(proc2, null);
        procStore2.delete(proc2.getProcId()); // delete the procedure so that the WAL is removed later
      }
    }

    // Now, insert something to the first store, should fail.
    // If the store does a WAL roll and continue with another logId without checking higher logIds
    // it will incorrectly succeed.
    LOG.info("Inserting into first WALProcedureStore");
    try {
      procStore.insert(new TestProcedure(11), null);
      fail("Inserting into Procedure Store should have failed");
    } catch (Exception ex) {
      LOG.info("Received expected exception", ex);
    }
  }

  // ==========================================================================
  //  Test Create Table
  // ==========================================================================
  @Test(timeout=60000)
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
  @Test(timeout=60000)
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
    Path tableDir = FSUtils.getTableDir(getRootDir(), tableName);
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

    MasterProcedureTestingUtility.validateTableDeletion(
      UTIL.getHBaseCluster().getMaster(), tableName, regions, "f1", "f2");
  }

  // ==========================================================================
  //  Test Truncate Table
  // ==========================================================================
  @Test(timeout=90000)
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
    if (preserveSplits) {
      assertEquals(1 + splitKeys.length, UTIL.getHBaseAdmin().getTableRegions(tableName).size());
    } else {
      regions = UTIL.getHBaseAdmin().getTableRegions(tableName).toArray(new HRegionInfo[1]);
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
  @Test(timeout=60000)
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
  @Test(timeout=60000)
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
    masterFailover(testUtil);

    procExec = testUtil.getHBaseCluster().getMaster().getMasterProcedureExecutor();
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);
  }

  // ==========================================================================
  //  Master failover utils
  // ==========================================================================
  public static void masterFailover(final HBaseTestingUtility testUtil)
      throws Exception {
    MiniHBaseCluster cluster = testUtil.getMiniHBaseCluster();

    // Kill the master
    HMaster oldMaster = cluster.getMaster();
    cluster.killMaster(cluster.getMaster().getServerName());

    // Wait the secondary
    waitBackupMaster(testUtil, oldMaster);
  }

  public static void waitBackupMaster(final HBaseTestingUtility testUtil,
      final HMaster oldMaster) throws Exception {
    MiniHBaseCluster cluster = testUtil.getMiniHBaseCluster();

    HMaster newMaster = cluster.getMaster();
    while (newMaster == null || newMaster == oldMaster) {
      Thread.sleep(250);
      newMaster = cluster.getMaster();
    }

    while (!(newMaster.isActiveMaster() && newMaster.isInitialized())) {
      Thread.sleep(250);
    }
  }

  // ==========================================================================
  //  Helpers
  // ==========================================================================
  private MasterProcedureEnv getMasterProcedureEnv() {
    return getMasterProcedureExecutor().getEnvironment();
  }

  private ProcedureExecutor<MasterProcedureEnv> getMasterProcedureExecutor() {
    return UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
  }

  private FileSystem getFileSystem() {
    return UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getFileSystem();
  }

  private Path getRootDir() {
    return UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();
  }

  private Path getTempDir() {
    return UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getTempDir();
  }
}
