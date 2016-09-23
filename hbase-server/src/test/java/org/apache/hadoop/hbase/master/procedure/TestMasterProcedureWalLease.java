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

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CategoryBasedTimeout;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.TestProcedure;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;
import org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;
import org.mockito.Mockito;

@Category({MasterTests.class, LargeTests.class})
public class TestMasterProcedureWalLease {
  private static final Log LOG = LogFactory.getLog(TestMasterProcedureWalLease.class);

  @ClassRule
  public static final TestRule timeout =
      CategoryBasedTimeout.forClass(TestMasterProcedureWalLease.class);

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
  }

  @After
  public void tearDown() throws Exception {
    try {
      UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      LOG.warn("failure shutting down cluster", e);
    }
  }

  @Test
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
    MasterProcedureTestingUtility.waitBackupMaster(UTIL, firstMaster);
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
    firstMaster.getConfiguration().setLong(WALProcedureStore.ROLL_THRESHOLD_CONF_KEY, 1);

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
  //  Helpers
  // ==========================================================================
  private ProcedureExecutor<MasterProcedureEnv> getMasterProcedureExecutor() {
    return UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
  }
}