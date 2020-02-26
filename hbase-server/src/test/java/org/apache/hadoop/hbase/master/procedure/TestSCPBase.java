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
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaTestHelper;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.assignment.AssignmentTestingUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSCPBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestSCPBase.class);
  static final int RS_COUNT = 3;

  protected HBaseTestingUtility util;

  protected void setupConf(Configuration conf) {
    conf.setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, 1);
  }

  @Before
  public void setup() throws Exception {
    this.util = new HBaseTestingUtility();
    setupConf(this.util.getConfiguration());
    startMiniCluster();
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(
      this.util.getHBaseCluster().getMaster().getMasterProcedureExecutor(), false);
  }

  protected void startMiniCluster() throws Exception {
    this.util.startMiniCluster(RS_COUNT);
  }

  @After
  public void tearDown() throws Exception {
    MiniHBaseCluster cluster = this.util.getHBaseCluster();
    HMaster master = cluster == null ? null : cluster.getMaster();
    if (master != null && master.getMasterProcedureExecutor() != null) {
      ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(master.getMasterProcedureExecutor(),
        false);
    }
    this.util.shutdownMiniCluster();
  }

  /**
   * Run server crash procedure steps twice to test idempotency and that we are persisting all
   * needed state.
   */
  protected void testRecoveryAndDoubleExecution(boolean carryingMeta, boolean doubleExecution)
      throws Exception {
    final TableName tableName = TableName.valueOf("testRecoveryAndDoubleExecution-carryingMeta-" +
      carryingMeta + "-doubleExecution-" + doubleExecution);
    try (Table t = createTable(tableName)) {
      // Load the table with a bit of data so some logs to split and some edits in each region.
      this.util.loadTable(t, HBaseTestingUtility.COLUMNS[0]);
      final int count = util.countRows(t);
      assertTrue("expected some rows", count > 0);
      final String checksum = util.checksumRows(t);
      // Run the procedure executor outside the master so we can mess with it. Need to disable
      // Master's running of the server crash processing.
      final HMaster master = this.util.getHBaseCluster().getMaster();
      final ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
      // find the first server that match the request and executes the test
      ServerName rsToKill = null;
      for (RegionInfo hri : util.getAdmin().getRegions(tableName)) {
        final ServerName serverName = AssignmentTestingUtil.getServerHoldingRegion(util, hri);
        if (AssignmentTestingUtil.isServerHoldingMeta(util, serverName) == carryingMeta) {
          rsToKill = serverName;
          break;
        }
      }
      // Enable test flags and then queue the crash procedure.
      ProcedureTestingUtility.waitNoProcedureRunning(procExec);
      if (doubleExecution) {
        // For SCP, if you enable this then we will enter an infinite loop, as we will crash between
        // queue and open for TRSP, and then going back to queue, as we will use the crash rs as the
        // target server since it is recored in hbase:meta.
        ProcedureTestingUtility.setKillIfHasParent(procExec, false);
        ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);
        // kill the RS
        AssignmentTestingUtil.killRs(util, rsToKill);
        long procId = getSCPProcId(procExec);
        // Now run through the procedure twice crashing the executor on each step...
        MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(procExec, procId);
      } else {
        // kill the RS
        AssignmentTestingUtil.killRs(util, rsToKill);
        long procId = getSCPProcId(procExec);
        ProcedureTestingUtility.waitProcedure(procExec, procId);
      }
      RegionReplicaTestHelper.assertReplicaDistributed(util, t);
      assertEquals(count, util.countRows(t));
      assertEquals(checksum, util.checksumRows(t));
    }
  }

  protected long getSCPProcId(ProcedureExecutor<?> procExec) {
    util.waitFor(30000, () -> !procExec.getProcedures().isEmpty());
    return procExec.getActiveProcIds().stream().mapToLong(Long::longValue).min().getAsLong();
  }

  protected Table createTable(final TableName tableName) throws IOException {
    final Table t = this.util.createTable(tableName, HBaseTestingUtility.COLUMNS,
      HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE, getRegionReplication());
    return t;
  }

  protected int getRegionReplication() {
    return 1;
  }
}
