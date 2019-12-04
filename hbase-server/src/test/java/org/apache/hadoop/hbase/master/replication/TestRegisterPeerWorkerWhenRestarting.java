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

import static org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RecoverStandbyState.DISPATCH_WALS_VALUE;
import static org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RecoverStandbyState.UNREGISTER_PEER_FROM_WORKER_STORAGE_VALUE;

import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.hadoop.hbase.replication.SyncReplicationTestBase;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Testcase for HBASE-21494.
 */
@Category({ MasterTests.class, LargeTests.class })
public class TestRegisterPeerWorkerWhenRestarting extends SyncReplicationTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegisterPeerWorkerWhenRestarting.class);

  private static volatile boolean FAIL = false;

  public static final class HMasterForTest extends HMaster {

    public HMasterForTest(Configuration conf) throws IOException {
      super(conf);
    }

    @Override
    public void remoteProcedureCompleted(long procId) {
      if (FAIL && getMasterProcedureExecutor()
        .getProcedure(procId) instanceof SyncReplicationReplayWALRemoteProcedure) {
        throw new RuntimeException("Inject error");
      }
      super.remoteProcedureCompleted(procId);
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL2.getConfiguration().setClass(HConstants.MASTER_IMPL, HMasterForTest.class, HMaster.class);
    SyncReplicationTestBase.setUp();
  }

  @Test
  public void testRestart() throws Exception {
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
    // make sure the transiting can not succeed
    FAIL = true;
    ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
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
    // wait until we are in the states where we need to register peer worker when restarting
    UTIL2.waitFor(60000,
      () -> procExec.getProcedures().stream().filter(p -> p instanceof RecoverStandbyProcedure)
        .map(p -> (RecoverStandbyProcedure) p)
        .anyMatch(p -> p.getCurrentStateId() == DISPATCH_WALS_VALUE ||
          p.getCurrentStateId() == UNREGISTER_PEER_FROM_WORKER_STORAGE_VALUE));
    // failover to another master
    MasterThread mt = UTIL2.getMiniHBaseCluster().getMasterThread();
    mt.getMaster().abort("for testing");
    mt.join();
    FAIL = false;
    t.join();
    // make sure the new master can finish the transition
    UTIL2.waitFor(60000, () -> UTIL2.getAdmin()
      .getReplicationPeerSyncReplicationState(PEER_ID) == SyncReplicationState.DOWNGRADE_ACTIVE);
    verify(UTIL2, 0, 100);
  }
}
