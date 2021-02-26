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
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.PeerModificationState;

@Category({ MasterTests.class, LargeTests.class })
public class TestModifyPeerProcedureRetryBackoff {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestModifyPeerProcedureRetryBackoff.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static boolean FAIL = true;

  public static class TestModifyPeerProcedure extends ModifyPeerProcedure {

    public TestModifyPeerProcedure() {
    }

    public TestModifyPeerProcedure(String peerId) {
      super(peerId);
    }

    @Override
    public PeerOperationType getPeerOperationType() {
      return PeerOperationType.ADD;
    }

    private void tryFail() throws ReplicationException {
      synchronized (TestModifyPeerProcedureRetryBackoff.class) {
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
    protected PeerModificationState nextStateAfterRefresh() {
      return PeerModificationState.SERIAL_PEER_REOPEN_REGIONS;
    }

    @Override
    protected boolean enablePeerBeforeFinish() {
      return true;
    }

    @Override
    protected void updateLastPushedSequenceIdForSerialPeer(MasterProcedureEnv env)
        throws IOException, ReplicationException {
      tryFail();
    }

    @Override
    protected void reopenRegions(MasterProcedureEnv env) throws IOException {
      try {
        tryFail();
      } catch (ReplicationException e) {
        throw new IOException(e);
      }
    }

    @Override
    protected void enablePeer(MasterProcedureEnv env) throws ReplicationException {
      tryFail();
    }

    @Override
    protected void prePeerModification(MasterProcedureEnv env)
        throws IOException, ReplicationException {
      tryFail();
    }

    @Override
    protected void updatePeerStorage(MasterProcedureEnv env) throws ReplicationException {
      tryFail();
    }

    @Override
    protected void postPeerModification(MasterProcedureEnv env)
        throws IOException, ReplicationException {
      tryFail();
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
    ProcedureTestUtil.waitUntilProcedureWaitingTimeout(UTIL, TestModifyPeerProcedure.class, 30000);
    ProcedureTestUtil.waitUntilProcedureTimeoutIncrease(UTIL, TestModifyPeerProcedure.class, 2);
    synchronized (TestModifyPeerProcedureRetryBackoff.class) {
      FAIL = false;
    }
    UTIL.waitFor(30000, () -> FAIL);
  }

  @Test
  public void test() throws IOException, InterruptedException {
    ProcedureExecutor<MasterProcedureEnv> procExec =
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();
    long procId = procExec.submitProcedure(new TestModifyPeerProcedure("1"));
    // PRE_PEER_MODIFICATION
    assertBackoffIncrease();
    // UPDATE_PEER_STORAGE
    assertBackoffIncrease();
    // No retry for REFRESH_PEER_ON_RS
    // SERIAL_PEER_REOPEN_REGIONS
    assertBackoffIncrease();
    // SERIAL_PEER_UPDATE_LAST_PUSHED_SEQ_ID
    assertBackoffIncrease();
    // SERIAL_PEER_SET_PEER_ENABLED
    assertBackoffIncrease();
    // No retry for SERIAL_PEER_ENABLE_PEER_REFRESH_PEER_ON_RS
    // POST_PEER_MODIFICATION
    assertBackoffIncrease();
    UTIL.waitFor(30000, () -> procExec.isFinished(procId));
  }
}
