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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * It used to first run with DLS and then DLR but HBASE-12751 broke DLR so we disabled it here.
 */
@Category(LargeTests.class)
@RunWith(Parameterized.class)
public class TestServerCrashProcedure {
  // Ugly junit parameterization. I just want to pass false and then true but seems like needs
  // to return sequences of two-element arrays.
  @Parameters(name = "{index}: setting={0}")
  public static Collection<Object []> data() {
    return Arrays.asList(new Object[] [] {{Boolean.FALSE, -1}});
  }

  private final HBaseTestingUtility util = new HBaseTestingUtility();

  @Before
  public void setup() throws Exception {
    this.util.startMiniCluster(3);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(
      this.util.getHBaseCluster().getMaster().getMasterProcedureExecutor(), false);
  }

  @After
  public void tearDown() throws Exception {
    MiniHBaseCluster cluster = this.util.getHBaseCluster();
    HMaster master = cluster == null? null: cluster.getMaster();
    if (master != null && master.getMasterProcedureExecutor() != null) {
      ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(master.getMasterProcedureExecutor(),
        false);
    }
    this.util.shutdownMiniCluster();
  }

  public TestServerCrashProcedure(final Boolean b, final int ignore) {
    this.util.getConfiguration().setBoolean("hbase.master.distributed.log.replay", b);
    this.util.getConfiguration().setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, 1);
  }

  /**
   * Run server crash procedure steps twice to test idempotency and that we are persisting all
   * needed state.
   * @throws Exception
   */
  @Test(timeout = 300000)
  public void testRecoveryAndDoubleExecutionOnline() throws Exception {
    final TableName tableName = TableName.valueOf("testRecoveryAndDoubleExecutionOnline");
    this.util.createTable(tableName, HBaseTestingUtility.COLUMNS,
      HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE);
    try (Table t = this.util.getConnection().getTable(tableName)) {
      // Load the table with a bit of data so some logs to split and some edits in each region.
      this.util.loadTable(t, HBaseTestingUtility.COLUMNS[0]);
      int count = countRows(t);
      // Run the procedure executor outside the master so we can mess with it. Need to disable
      // Master's running of the server crash processing.
      HMaster master = this.util.getHBaseCluster().getMaster();
      final ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
      master.setServerCrashProcessingEnabled(false);
      // Kill a server. Master will notice but do nothing other than add it to list of dead servers.
      HRegionServer hrs = this.util.getHBaseCluster().getRegionServer(0);
      boolean carryingMeta = (master.getAssignmentManager().isCarryingMeta(hrs.getServerName()) ==
          AssignmentManager.ServerHostRegion.HOSTING_REGION);
      this.util.getHBaseCluster().killRegionServer(hrs.getServerName());
      hrs.join();
      // Wait until the expiration of the server has arrived at the master. We won't process it
      // by queuing a ServerCrashProcedure because we have disabled crash processing... but wait
      // here so ServerManager gets notice and adds expired server to appropriate queues.
      while (!master.getServerManager().isServerDead(hrs.getServerName())) Threads.sleep(10);
      // Now, reenable processing else we can't get a lock on the ServerCrashProcedure.
      master.setServerCrashProcessingEnabled(true);
      // Do some of the master processing of dead servers so when SCP runs, it has expected 'state'.
      master.getServerManager().moveFromOnelineToDeadServers(hrs.getServerName());
      // Enable test flags and then queue the crash procedure.
      ProcedureTestingUtility.waitNoProcedureRunning(procExec);
      ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);
      long procId =
        procExec.submitProcedure(new ServerCrashProcedure(hrs.getServerName(), true, carryingMeta));
      // Now run through the procedure twice crashing the executor on each step...
      MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(procExec, procId);
      // Assert all data came back.
      assertEquals(count, countRows(t));
    }
  }

  int countRows(final Table t) throws IOException {
    int count = 0;
    try (ResultScanner scanner = t.getScanner(new Scan())) {
      while(scanner.next() != null) count++;
    }
    return count;
  }
}