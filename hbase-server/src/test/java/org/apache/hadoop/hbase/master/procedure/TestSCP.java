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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.assignment.AssignmentTestingUtil;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MasterTests.class, LargeTests.class})
public class TestSCP {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestSCP.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestSCP.class);

  protected HBaseTestingUtility util;

  protected void setupConf(Configuration conf) {
    conf.setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, 1);
    conf.set("hbase.balancer.tablesOnMaster", "none");
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 3);
    conf.setInt(HConstants.HBASE_CLIENT_SERVERSIDE_RETRIES_MULTIPLIER, 3);
    conf.setBoolean("hbase.split.writer.creation.bounded", true);
    conf.setInt("hbase.regionserver.hlog.splitlog.writer.threads", 8);
    conf.setBoolean(HConstants.HBASE_SPLIT_WAL_COORDINATED_BY_ZK, true);
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
    this.util.startMiniCluster(3);
  }

  @After
  public void tearDown() throws Exception {
    MiniHBaseCluster cluster = this.util.getHBaseCluster();
    HMaster master = cluster == null? null: cluster.getMaster();
    if (master != null && master.getMasterProcedureExecutor() != null) {
      ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(
        master.getMasterProcedureExecutor(), false);
    }
    this.util.shutdownMiniCluster();
  }


  @Test
  public void testCrashTargetRs() throws Exception {
    testRecoveryAndDoubleExecution(false, false);
  }

  @Test
  public void testRecoveryAndDoubleExecutionOnRsWithMeta() throws Exception {
    testRecoveryAndDoubleExecution(true, true);
  }

  @Test
  public void testRecoveryAndDoubleExecutionOnRsWithoutMeta() throws Exception {
    testRecoveryAndDoubleExecution(false, true);
  }

  private long getSCPProcId(ProcedureExecutor<?> procExec) {
    util.waitFor(30000, () -> !procExec.getProcedures().isEmpty());
    return procExec.getActiveProcIds().stream().mapToLong(Long::longValue).min().getAsLong();
  }

  /**
   * Run server crash procedure steps twice to test idempotency and that we are persisting all
   * needed state.
   */
  private void testRecoveryAndDoubleExecution(boolean carryingMeta, boolean doubleExecution)
      throws Exception {
    final TableName tableName = TableName.valueOf("testRecoveryAndDoubleExecution-carryingMeta-"
        + carryingMeta + "-doubleExecution-" + doubleExecution);
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
      assertReplicaDistributed(t);
      assertEquals(count, util.countRows(t));
      assertEquals(checksum, util.checksumRows(t));
    }
  }

  @Test
  public void testConcurrentSCPForSameServer() throws Exception {
    final TableName tableName =
        TableName.valueOf("testConcurrentSCPForSameServer");
    try (Table t = createTable(tableName)) {
      // Load the table with a bit of data so some logs to split and some edits in each region.
      this.util.loadTable(t, HBaseTestingUtility.COLUMNS[0]);
      final int count = util.countRows(t);
      assertTrue("expected some rows", count > 0);
      // find the first server that match the request and executes the test
      ServerName rsToKill = null;
      for (RegionInfo hri : util.getAdmin().getRegions(tableName)) {
        final ServerName serverName = AssignmentTestingUtil.getServerHoldingRegion(util, hri);
        if (AssignmentTestingUtil.isServerHoldingMeta(util, serverName) == true) {
          rsToKill = serverName;
          break;
        }
      }
      HMaster master = util.getHBaseCluster().getMaster();
      final ProcedureExecutor<MasterProcedureEnv> pExecutor = master.getMasterProcedureExecutor();
      ServerCrashProcedure procB =
          new ServerCrashProcedure(pExecutor.getEnvironment(), rsToKill, false, false);
      AssignmentTestingUtil.killRs(util, rsToKill);
      long procId = getSCPProcId(pExecutor);
      Procedure procA = pExecutor.getProcedure(procId);
      LOG.info("submit SCP procedureA");
      util.waitFor(5000, () -> procA.hasLock());
      LOG.info("procedureA acquired the lock");
      assertEquals(Procedure.LockState.LOCK_EVENT_WAIT,
          procB.acquireLock(pExecutor.getEnvironment()));
      LOG.info("procedureB should not be able to get the lock");
      util.waitFor(60000,
        () -> procB.acquireLock(pExecutor.getEnvironment()) == Procedure.LockState.LOCK_ACQUIRED);
      LOG.info("when procedure B get the lock, procedure A should be finished");
      assertTrue(procA.isFinished());
    }
  }

  protected void assertReplicaDistributed(final Table t) {
    return;
  }

  protected Table createTable(final TableName tableName) throws IOException {
    final Table t = this.util.createTable(tableName, HBaseTestingUtility.COLUMNS,
        HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE);
    return t;
  }
}
