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

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ProcedureTestUtil;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, LargeTests.class })
public class TestTransitPeerSyncReplicationStateProcedureBackoff {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestTransitPeerSyncReplicationStateProcedureBackoff.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static boolean FAIL = true;

  public static class TestTransitPeerSyncReplicationStateProcedure
      extends TransitPeerSyncReplicationStateProcedure {

    public TestTransitPeerSyncReplicationStateProcedure() {
    }

    public TestTransitPeerSyncReplicationStateProcedure(String peerId, SyncReplicationState state) {
      super(peerId, state);
    }

    private void tryFail() throws ReplicationException {
      synchronized (TestTransitPeerSyncReplicationStateProcedureBackoff.class) {
        if (FAIL) {
          throw new ReplicationException("Inject error");
        }
        FAIL = true;
      }
    }

    @Override
    protected <T extends Procedure<MasterProcedureEnv>> void addChildProcedure(
        @SuppressWarnings("unchecked") T... subProcedure) {
      // Make it a no-op
    }

    @Override
    protected void preTransit(MasterProcedureEnv env) throws IOException {
      fromState = SyncReplicationState.DOWNGRADE_ACTIVE;
    }

    @Override
    protected void setPeerNewSyncReplicationState(MasterProcedureEnv env)
        throws ReplicationException {
      tryFail();
    }

    @Override
    protected void removeAllReplicationQueues(MasterProcedureEnv env) throws ReplicationException {
      tryFail();
    }

    @Override
    protected void reopenRegions(MasterProcedureEnv env) {
      // do nothing;
    }

    @Override
    protected void transitPeerSyncReplicationState(MasterProcedureEnv env)
        throws ReplicationException {
      tryFail();
    }

    @Override
    protected void createDirForRemoteWAL(MasterProcedureEnv env) throws IOException {
      try {
        tryFail();
      } catch (ReplicationException e) {
        throw new IOException(e);
      }
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  private void assertBackoffIncrease() throws IOException, InterruptedException {
    ProcedureTestUtil.waitUntilProcedureWaitingTimeout(UTIL,
      TestTransitPeerSyncReplicationStateProcedure.class, 30000);
    ProcedureTestUtil.waitUntilProcedureTimeoutIncrease(UTIL,
      TestTransitPeerSyncReplicationStateProcedure.class, 2);
    synchronized (TestTransitPeerSyncReplicationStateProcedure.class) {
      FAIL = false;
    }
    UTIL.waitFor(30000, () -> FAIL);
  }

  @Test
  public void testDowngradeActiveToActive() throws IOException, InterruptedException {
    ProcedureExecutor<MasterProcedureEnv> procExec =
        UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();
    // Test procedure: DOWNGRADE_ACTIVE ==> ACTIVE
    long procId = procExec.submitProcedure(
      new TestTransitPeerSyncReplicationStateProcedure("1", SyncReplicationState.ACTIVE));
    // No retry for PRE_PEER_SYNC_REPLICATION_STATE_TRANSITION
    // SET_PEER_NEW_SYNC_REPLICATION_STATE
    assertBackoffIncrease();
    // No retry for REFRESH_PEER_SYNC_REPLICATION_STATE_ON_RS_BEGIN
    // No retry for REOPEN_ALL_REGIONS_IN_PEER
    // TRANSIT_PEER_NEW_SYNC_REPLICATION_STATE
    assertBackoffIncrease();
    // No retry for REFRESH_PEER_SYNC_REPLICATION_STATE_ON_RS_END
    // No retry for POST_PEER_SYNC_REPLICATION_STATE_TRANSITION
    UTIL.waitFor(30000, () -> procExec.isFinished(procId));
  }

  @Test
  public void testDowngradeActiveToStandby() throws IOException, InterruptedException {
    ProcedureExecutor<MasterProcedureEnv> procExec =
        UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();
    // Test procedure: DOWNGRADE_ACTIVE ==> ACTIVE
    long procId = procExec.submitProcedure(
      new TestTransitPeerSyncReplicationStateProcedure("2", SyncReplicationState.STANDBY));
    // No retry for PRE_PEER_SYNC_REPLICATION_STATE_TRANSITION
    // SET_PEER_NEW_SYNC_REPLICATION_STATE
    assertBackoffIncrease();
    // No retry for REFRESH_PEER_SYNC_REPLICATION_STATE_ON_RS_BEGIN
    // REMOVE_ALL_REPLICATION_QUEUES_IN_PEER
    assertBackoffIncrease();
    // TRANSIT_PEER_NEW_SYNC_REPLICATION_STATE
    assertBackoffIncrease();
    // No retry for REFRESH_PEER_SYNC_REPLICATION_STATE_ON_RS_END
    // CREATE_DIR_FOR_REMOTE_WAL
    assertBackoffIncrease();
    // No retry for POST_PEER_SYNC_REPLICATION_STATE_TRANSITION
    UTIL.waitFor(30000, () -> procExec.isFinished(procId));
  }
}