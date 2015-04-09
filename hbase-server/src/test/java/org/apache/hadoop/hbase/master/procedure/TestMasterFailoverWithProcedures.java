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
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;
import org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos.CreateTableState;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos.DeleteTableState;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({MasterTests.class, LargeTests.class})
public class TestMasterFailoverWithProcedures {
  private static final Log LOG = LogFactory.getLog(TestMasterFailoverWithProcedures.class);

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static void setupConf(Configuration conf) {
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
    getMasterProcedureExecutor().submitProcedure(
      new CreateTableProcedure(getMasterProcedureExecutor().getEnvironment(), htd, regions));
    LOG.debug("wait master store abort");
    masterStoreAbort.await();

    // Now the real backup master should start up
    LOG.debug("wait backup master to startup");
    waitBackupMaster(UTIL, firstMaster);
    assertEquals(true, firstMaster.isStopped());

    // wait the store in here to abort (the test will fail due to timeout if it doesn't)
    LOG.debug("wait the store to abort");
    backupStore3.getStoreTracker().setDeleted(1, false);
    backupStore3.delete(1);
    backupStore3Abort.await();
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
