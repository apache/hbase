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
package org.apache.hadoop.hbase.master.replication;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureConstants;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.hadoop.hbase.replication.SyncReplicationTestBase;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, LargeTests.class })
public class TestTransitPeerSyncReplicationStateProcedureRetry extends SyncReplicationTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestTransitPeerSyncReplicationStateProcedureRetry.class);

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL2.getConfiguration().setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, 1);
    SyncReplicationTestBase.setUp();
  }

  @Test
  public void testRecoveryAndDoubleExecution() throws Exception {
    UTIL2.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
      SyncReplicationState.STANDBY);
    UTIL1.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
      SyncReplicationState.ACTIVE);

    UTIL1.getAdmin().disableReplicationPeer(PEER_ID);
    write(UTIL1, 0, 100);
    Thread.sleep(2000);
    // peer is disabled so no data have been replicated
    verifyNotReplicatedThroughRegion(UTIL2, 0, 100);

    // transit the A to DA first to avoid too many error logs.
    UTIL1.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
      SyncReplicationState.DOWNGRADE_ACTIVE);
    HMaster master = UTIL2.getHBaseCluster().getMaster();
    ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
    // Enable test flags and then queue the procedure.
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);
    Thread t = new Thread() {

      @Override
      public void run() {
        try {
          UTIL2.getAdmin().transitReplicationPeerSyncReplicationState(PEER_ID,
            SyncReplicationState.DOWNGRADE_ACTIVE);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    };
    t.start();
    UTIL2.waitFor(30000, () -> procExec.getProcedures().stream()
      .anyMatch(p -> p instanceof TransitPeerSyncReplicationStateProcedure && !p.isFinished()));
    long procId = procExec.getProcedures().stream()
      .filter(p -> p instanceof TransitPeerSyncReplicationStateProcedure && !p.isFinished())
      .mapToLong(Procedure::getProcId).min().getAsLong();
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(procExec, procId);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, false);
    assertEquals(SyncReplicationState.DOWNGRADE_ACTIVE,
      UTIL2.getAdmin().getReplicationPeerSyncReplicationState(PEER_ID));
    verify(UTIL2, 0, 100);
  }
}
