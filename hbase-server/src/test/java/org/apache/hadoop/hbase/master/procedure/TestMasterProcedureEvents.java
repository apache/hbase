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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({MasterTests.class, MediumTests.class})
public class TestMasterProcedureEvents {
  private static final Log LOG = LogFactory.getLog(TestCreateTableProcedure.class);

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static void setupConf(Configuration conf) {
    conf.setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, 1);
    conf.setBoolean(WALProcedureStore.USE_HSYNC_CONF_KEY, false);
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(2);
    UTIL.waitUntilNoRegionsInTransition();
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    try {
      UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      LOG.warn("failure shutting down cluster", e);
    }
  }

  @After
  public void tearDown() throws Exception {
    for (HTableDescriptor htd: UTIL.getHBaseAdmin().listTables()) {
      LOG.info("Tear down, remove table=" + htd.getTableName());
      UTIL.deleteTable(htd.getTableName());
    }
  }

  @Test(timeout = 30000)
  public void testMasterInitializedEvent() throws Exception {
    TableName tableName = TableName.valueOf("testMasterInitializedEvent");
    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();

    HRegionInfo hri = new HRegionInfo(tableName);
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor("f"));

    while (!master.isInitialized()) Thread.sleep(250);
    master.setInitialized(false); // fake it, set back later

    // check event wait/wake
    testProcedureEventWaitWake(master, master.getInitializedEvent(),
      new CreateTableProcedure(procExec.getEnvironment(), htd, new HRegionInfo[] { hri }));
  }

  @Test(timeout = 30000)
  public void testServerCrashProcedureEvent() throws Exception {
    TableName tableName = TableName.valueOf("testServerCrashProcedureEventTb");
    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();

    while (!master.isServerCrashProcessingEnabled() || !master.isInitialized() ||
        master.getAssignmentManager().getRegionStates().isRegionsInTransition()) {
      Thread.sleep(25);
    }

    UTIL.createTable(tableName, HBaseTestingUtility.COLUMNS[0]);
    try (Table t = UTIL.getConnection().getTable(tableName)) {
      // Load the table with a bit of data so some logs to split and some edits in each region.
      UTIL.loadTable(t, HBaseTestingUtility.COLUMNS[0]);
    }

    master.setServerCrashProcessingEnabled(false);  // fake it, set back later

    // Kill a server. Master will notice but do nothing other than add it to list of dead servers.
    HRegionServer hrs = getServerWithRegions();
    boolean carryingMeta = master.getAssignmentManager().isCarryingMeta(hrs.getServerName());
    UTIL.getHBaseCluster().killRegionServer(hrs.getServerName());
    hrs.join();

    // Wait until the expiration of the server has arrived at the master. We won't process it
    // by queuing a ServerCrashProcedure because we have disabled crash processing... but wait
    // here so ServerManager gets notice and adds expired server to appropriate queues.
    while (!master.getServerManager().isServerDead(hrs.getServerName())) Thread.sleep(10);

    // Do some of the master processing of dead servers so when SCP runs, it has expected 'state'.
    master.getServerManager().moveFromOnelineToDeadServers(hrs.getServerName());

    // check event wait/wake
    testProcedureEventWaitWake(master, master.getServerCrashProcessingEnabledEvent(),
      new ServerCrashProcedure(procExec.getEnvironment(), hrs.getServerName(), true, carryingMeta));
  }

  private void testProcedureEventWaitWake(final HMaster master, final ProcedureEvent event,
      final Procedure proc) throws Exception {
    final ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
    final MasterProcedureScheduler procSched = procExec.getEnvironment().getProcedureQueue();

    final long startPollCalls = procSched.getPollCalls();
    final long startNullPollCalls = procSched.getNullPollCalls();

    // check that nothing is in the event queue
    LOG.debug("checking " + event);
    assertEquals(false, event.isReady());
    assertEquals(0, event.size());

    // submit the procedure
    LOG.debug("submit " + proc);
    long procId = procExec.submitProcedure(proc);

    // wait until the event is in the queue (proc executed and got into suspended state)
    LOG.debug("wait procedure suspended on " + event);
    while (event.size() < 1) Thread.sleep(25);

    // check that the proc is in the event queue
    LOG.debug("checking " + event + " size=" + event.size());
    assertEquals(false, event.isReady());
    assertEquals(1, event.size());

    // wake the event
    LOG.debug("wake " + event);
    procSched.wakeEvent(event);
    assertEquals(true, event.isReady());

    // wait until proc completes
    LOG.debug("waiting " + proc);
    ProcedureTestingUtility.waitProcedure(procExec, procId);

    // check that nothing is in the event queue and the event is not suspended
    assertEquals(true, event.isReady());
    assertEquals(0, event.size());
    LOG.debug("completed execution of " + proc +
      " pollCalls=" + (procSched.getPollCalls() - startPollCalls) +
      " nullPollCalls=" + (procSched.getNullPollCalls() - startNullPollCalls));
  }

  private HRegionServer getServerWithRegions() {
    for (int i = 0; i < 3; ++i) {
      HRegionServer hrs = UTIL.getHBaseCluster().getRegionServer(i);
      if (hrs.getNumberOfOnlineRegions() > 0) {
        return hrs;
      }
    }
    return null;
  }
}
