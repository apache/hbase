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
package org.apache.hadoop.hbase.master.replication;

import static org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.MigrateReplicationQueueFromZkToTableState.MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_DISABLE_CLEANER;
import static org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.MigrateReplicationQueueFromZkToTableState.MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_WAIT_UPGRADING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionServerList;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.PeerProcedureInterface;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.master.ReplicationLogCleanerBarrier;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;

@Category({ MasterTests.class, MediumTests.class })
public class TestMigrateReplicationQueueFromZkToTableProcedure {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMigrateReplicationQueueFromZkToTableProcedure.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  public static final class HMasterForTest extends HMaster {

    public HMasterForTest(Configuration conf) throws IOException {
      super(conf);
    }

    @Override
    protected ServerManager createServerManager(MasterServices master, RegionServerList storage)
      throws IOException {
      setupClusterConnection();
      return new ServerManagerForTest(master, storage);
    }
  }

  private static final ConcurrentMap<ServerName, ServerMetrics> EXTRA_REGION_SERVERS =
    new ConcurrentHashMap<>();

  public static final class ServerManagerForTest extends ServerManager {

    public ServerManagerForTest(MasterServices master, RegionServerList storage) {
      super(master, storage);
    }

    @Override
    public Map<ServerName, ServerMetrics> getOnlineServers() {
      Map<ServerName, ServerMetrics> map = new HashMap<>(super.getOnlineServers());
      map.putAll(EXTRA_REGION_SERVERS);
      return map;
    }
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    // one hour, to make sure it will not run during the test
    UTIL.getConfiguration().setInt(HMaster.HBASE_MASTER_CLEANER_INTERVAL, 60 * 60 * 1000);
    UTIL.startMiniCluster(
      StartTestingClusterOption.builder().masterClass(HMasterForTest.class).build());
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  private ProcedureExecutor<MasterProcedureEnv> getMasterProcedureExecutor() {
    return UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
  }

  @After
  public void tearDown() throws Exception {
    Admin admin = UTIL.getAdmin();
    for (ReplicationPeerDescription pd : admin.listReplicationPeers()) {
      admin.removeReplicationPeer(pd.getPeerId());
    }
  }

  private static CountDownLatch PEER_PROC_ARRIVE;

  private static CountDownLatch PEER_PROC_RESUME;

  public static final class FakePeerProcedure extends Procedure<MasterProcedureEnv>
    implements PeerProcedureInterface {

    private String peerId;

    public FakePeerProcedure() {
    }

    public FakePeerProcedure(String peerId) {
      this.peerId = peerId;
    }

    @Override
    public String getPeerId() {
      return peerId;
    }

    @Override
    public PeerOperationType getPeerOperationType() {
      return PeerOperationType.UPDATE_CONFIG;
    }

    @Override
    protected Procedure<MasterProcedureEnv>[] execute(MasterProcedureEnv env)
      throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
      PEER_PROC_ARRIVE.countDown();
      PEER_PROC_RESUME.await();
      return null;
    }

    @Override
    protected void rollback(MasterProcedureEnv env) throws IOException, InterruptedException {
      throw new UnsupportedOperationException();
    }

    @Override
    protected boolean abort(MasterProcedureEnv env) {
      return false;
    }

    @Override
    protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    }

    @Override
    protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    }
  }

  @Test
  public void testWaitUntilNoPeerProcedure() throws Exception {
    PEER_PROC_ARRIVE = new CountDownLatch(1);
    PEER_PROC_RESUME = new CountDownLatch(1);
    ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    procExec.submitProcedure(new FakePeerProcedure("1"));
    PEER_PROC_ARRIVE.await();
    MigrateReplicationQueueFromZkToTableProcedure proc =
      new MigrateReplicationQueueFromZkToTableProcedure();
    procExec.submitProcedure(proc);
    // make sure we will wait until there is no peer related procedures before proceeding
    UTIL.waitFor(30000, () -> proc.getState() == ProcedureState.WAITING_TIMEOUT);
    // continue and make sure we can finish successfully
    PEER_PROC_RESUME.countDown();
    UTIL.waitFor(30000, () -> proc.isSuccess());
  }

  // make sure we will disable replication peers while migrating
  // and also tests disable/enable replication log cleaner and wait for region server upgrading
  @Test
  public void testDisablePeerAndWaitStates() throws Exception {
    String peerId = "2";
    ReplicationPeerConfig rpc = ReplicationPeerConfig.newBuilder()
      .setClusterKey(UTIL.getZkCluster().getAddress().toString() + ":/testhbase")
      .setReplicateAllUserTables(true).build();
    UTIL.getAdmin().addReplicationPeer(peerId, rpc);
    // put a fake region server to simulate that there are still region servers with older version
    ServerMetrics metrics = mock(ServerMetrics.class);
    when(metrics.getVersion()).thenReturn("2.5.0");
    EXTRA_REGION_SERVERS
      .put(ServerName.valueOf("localhost", 54321, EnvironmentEdgeManager.currentTime()), metrics);

    ReplicationLogCleanerBarrier barrier =
      UTIL.getHBaseCluster().getMaster().getReplicationLogCleanerBarrier();
    assertTrue(barrier.start());

    ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    MigrateReplicationQueueFromZkToTableProcedure proc =
      new MigrateReplicationQueueFromZkToTableProcedure();
    procExec.submitProcedure(proc);

    Thread.sleep(5000);
    // make sure we are still waiting for replication log cleaner quit
    assertEquals(MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_DISABLE_CLEANER.getNumber(),
      proc.getCurrentStateId());
    barrier.stop();

    // wait until we reach the wait upgrading state
    UTIL.waitFor(30000,
      () -> proc.getCurrentStateId()
          == MIGRATE_REPLICATION_QUEUE_FROM_ZK_TO_TABLE_WAIT_UPGRADING.getNumber()
        && proc.getState() == ProcedureState.WAITING_TIMEOUT);
    // make sure the peer is disabled for migrating
    assertFalse(UTIL.getAdmin().isReplicationPeerEnabled(peerId));
    // make sure the replication log cleaner is disabled
    assertFalse(barrier.start());

    // the procedure should finish successfully
    EXTRA_REGION_SERVERS.clear();
    UTIL.waitFor(30000, () -> proc.isSuccess());

    // make sure the peer is enabled again
    assertTrue(UTIL.getAdmin().isReplicationPeerEnabled(peerId));
    // make sure the replication log cleaner is enabled again
    assertTrue(barrier.start());
    barrier.stop();
  }
}
