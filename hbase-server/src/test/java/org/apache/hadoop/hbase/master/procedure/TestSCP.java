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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.assignment.AssignmentTestingUtil;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(MasterTests.TAG)
@Tag(LargeTests.TAG)
public class TestSCP extends TestSCPBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestSCP.class);

  @Test
  public void testCrashTargetRs() throws Exception {
    testRecoveryAndDoubleExecution(false, false);
  }

  @Test
  public void testConcurrentSCPForSameServer() throws Exception {
    final TableName tableName = TableName.valueOf("testConcurrentSCPForSameServer");
    try (Table t = createTable(tableName)) {
      // Load the table with a bit of data so some logs to split and some edits in each region.
      this.util.loadTable(t, HBaseTestingUtil.COLUMNS[0]);
      final int count = HBaseTestingUtil.countRows(t);
      assertTrue(count > 0, "expected some rows");
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
      Procedure<?> procA = pExecutor.getProcedure(procId);
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
}
