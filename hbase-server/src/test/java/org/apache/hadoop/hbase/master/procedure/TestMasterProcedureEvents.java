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
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
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
    conf.setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, 8);
    conf.setBoolean(WALProcedureStore.USE_HSYNC_CONF_KEY, false);
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(3);
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    try {
      UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      LOG.warn("failure shutting down cluster", e);
    }
  }

  @Test
  public void testMasterInitializedEvent() throws Exception {
    TableName tableName = TableName.valueOf("testMasterInitializedEvent");
    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
    MasterProcedureScheduler procSched = procExec.getEnvironment().getProcedureQueue();

    HRegionInfo hri = new HRegionInfo(tableName);
    HTableDescriptor htd = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor("f");
    htd.addFamily(hcd);

    while (!master.isInitialized()) Thread.sleep(250);
    master.setInitialized(false); // fake it, set back later

    CreateTableProcedure proc = new CreateTableProcedure(
      procExec.getEnvironment(), htd, new HRegionInfo[] { hri });

    long pollCalls = procSched.getPollCalls();
    long nullPollCalls = procSched.getNullPollCalls();

    long procId = procExec.submitProcedure(proc);
    for (int i = 0; i < 10; ++i) {
      Thread.sleep(100);
      assertEquals(pollCalls + 1, procSched.getPollCalls());
      assertEquals(nullPollCalls, procSched.getNullPollCalls());
    }

    master.setInitialized(true);
    ProcedureTestingUtility.waitProcedure(procExec, procId);

    assertEquals(pollCalls + 2, procSched.getPollCalls());
    assertEquals(nullPollCalls, procSched.getNullPollCalls());
  }

  @Test
  public void testServerCrashProcedureEvent() throws Exception {
    TableName tableName = TableName.valueOf("testServerCrashProcedureEventTb");
    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
    MasterProcedureScheduler procSched = procExec.getEnvironment().getProcedureQueue();

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

    long pollCalls = procSched.getPollCalls();
    long nullPollCalls = procSched.getNullPollCalls();

    // Kill a server. Master will notice but do nothing other than add it to list of dead servers.
    HRegionServer hrs = getServerWithRegions();
    boolean carryingMeta = master.getAssignmentManager()
        .isCarryingMeta(hrs.getServerName()) == AssignmentManager.ServerHostRegion.HOSTING_REGION;
    UTIL.getHBaseCluster().killRegionServer(hrs.getServerName());
    hrs.join();

    // Wait until the expiration of the server has arrived at the master. We won't process it
    // by queuing a ServerCrashProcedure because we have disabled crash processing... but wait
    // here so ServerManager gets notice and adds expired server to appropriate queues.
    while (!master.getServerManager().isServerDead(hrs.getServerName())) Thread.sleep(10);

    // Do some of the master processing of dead servers so when SCP runs, it has expected 'state'.
    master.getServerManager().moveFromOnelineToDeadServers(hrs.getServerName());

    long procId = procExec.submitProcedure(
      new ServerCrashProcedure(procExec.getEnvironment(), hrs.getServerName(), true, carryingMeta));

    for (int i = 0; i < 10; ++i) {
      Thread.sleep(100);
      assertEquals(pollCalls + 1, procSched.getPollCalls());
      assertEquals(nullPollCalls, procSched.getNullPollCalls());
    }

    // Now, reenable processing else we can't get a lock on the ServerCrashProcedure.
    master.setServerCrashProcessingEnabled(true);
    ProcedureTestingUtility.waitProcedure(procExec, procId);

    LOG.debug("server crash processing poll calls: " + procSched.getPollCalls());
    assertTrue(procSched.getPollCalls() >= (pollCalls + 2));
    assertEquals(nullPollCalls, procSched.getNullPollCalls());

    UTIL.deleteTable(tableName);
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
