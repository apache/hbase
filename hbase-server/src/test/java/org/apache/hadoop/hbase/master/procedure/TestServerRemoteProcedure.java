/**
 *
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

import static org.apache.hadoop.hbase.master.procedure.ServerProcedureInterface.ServerOperationType.SWITCH_RPC_THROTTLE;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.MockMasterServices;
import org.apache.hadoop.hbase.master.assignment.OpenRegionProcedure;
import org.apache.hadoop.hbase.master.assignment.TransitRegionStateProcedure;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureException;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;

@Category({ MasterTests.class, MediumTests.class })
public class TestServerRemoteProcedure {
  private static final Logger LOG = LoggerFactory.getLogger(TestServerRemoteProcedure.class);
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestServerRemoteProcedure.class);
  @Rule
  public TestName name = new TestName();
  @Rule
  public final ExpectedException exception = ExpectedException.none();
  protected HBaseTestingUtility util;
  protected MockRSProcedureDispatcher rsDispatcher;
  protected MockMasterServices master;
  protected AssignmentManager am;
  protected NavigableMap<ServerName, SortedSet<byte[]>> regionsToRegionServers =
      new ConcurrentSkipListMap<>();
  // Simple executor to run some simple tasks.
  protected ScheduledExecutorService executor;

  @Before
  public void setUp() throws Exception {
    util = new HBaseTestingUtility();
    this.executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
        .setUncaughtExceptionHandler((t, e) -> LOG.warn("Uncaught: ", e)).build());
    master = new MockMasterServices(util.getConfiguration(), this.regionsToRegionServers);
    rsDispatcher = new MockRSProcedureDispatcher(master);
    rsDispatcher.setMockRsExecutor(new NoopRSExecutor());
    master.start(2, rsDispatcher);
    am = master.getAssignmentManager();
    master.getServerManager().getOnlineServersList().stream()
        .forEach(serverName -> am.getRegionStates().getOrCreateServer(serverName));
  }

  @After
  public void tearDown() throws Exception {
    master.stop("tearDown");
    this.executor.shutdownNow();
  }

  @Test
  public void testSplitWALAndCrashBeforeResponse() throws Exception {
    ServerName worker = master.getServerManager().getOnlineServersList().get(0);
    ServerName crashedWorker = master.getServerManager().getOnlineServersList().get(1);
    ServerRemoteProcedure splitWALRemoteProcedure =
        new SplitWALRemoteProcedure(worker, crashedWorker, "test");
    Future<byte[]> future = submitProcedure(splitWALRemoteProcedure);
    Thread.sleep(2000);
    master.getServerManager().expireServer(worker);
    // if remoteCallFailed is called for this procedure, this procedure should be finished.
    future.get(5000, TimeUnit.MILLISECONDS);
    Assert.assertTrue(splitWALRemoteProcedure.isSuccess());
  }

  @Test
  public void testRemoteCompleteAndFailedAtTheSameTime() throws Exception {
    ServerName worker = master.getServerManager().getOnlineServersList().get(0);
    ServerRemoteProcedure noopServerRemoteProcedure = new NoopServerRemoteProcedure(worker);
    Future<byte[]> future = submitProcedure(noopServerRemoteProcedure);
    Thread.sleep(2000);
    // complete the process and fail the process at the same time
    ExecutorService threadPool = Executors.newFixedThreadPool(2);
    threadPool.execute(() -> noopServerRemoteProcedure
        .remoteOperationDone(master.getMasterProcedureExecutor().getEnvironment(), null));
    threadPool.execute(() -> noopServerRemoteProcedure.remoteCallFailed(
      master.getMasterProcedureExecutor().getEnvironment(), worker, new IOException()));
    future.get(2000, TimeUnit.MILLISECONDS);
    Assert.assertTrue(noopServerRemoteProcedure.isSuccess());
  }

  @Test
  public void testRegionOpenProcedureIsNotHandledByDispatcher() throws Exception {
    TableName tableName = TableName.valueOf("testRegionOpenProcedureIsNotHandledByDisPatcher");
    RegionInfo hri = RegionInfoBuilder.newBuilder(tableName).setStartKey(Bytes.toBytes(1))
      .setEndKey(Bytes.toBytes(2)).setSplit(false).setRegionId(0).build();
    MasterProcedureEnv env = master.getMasterProcedureExecutor().getEnvironment();
    env.getAssignmentManager().getRegionStates().getOrCreateRegionStateNode(hri);
    TransitRegionStateProcedure proc = TransitRegionStateProcedure.assign(env, hri, null);
    ServerName worker = master.getServerManager().getOnlineServersList().get(0);
    OpenRegionProcedure openRegionProcedure = new OpenRegionProcedure(proc, hri, worker);
    Future<byte[]> future = submitProcedure(openRegionProcedure);
    Thread.sleep(2000);
    rsDispatcher.removeNode(worker);
    try {
      future.get(2000, TimeUnit.MILLISECONDS);
      fail();
    } catch (TimeoutException e) {
      LOG.info("timeout is expected");
    }
    Assert.assertFalse(openRegionProcedure.isFinished());
  }

  private Future<byte[]> submitProcedure(final Procedure<MasterProcedureEnv> proc) {
    return ProcedureSyncWait.submitProcedure(master.getMasterProcedureExecutor(), proc);
  }

  private static class NoopServerRemoteProcedure extends ServerRemoteProcedure
      implements ServerProcedureInterface {

    public NoopServerRemoteProcedure(ServerName targetServer) {
      this.targetServer = targetServer;
    }

    @Override
    protected void rollback(MasterProcedureEnv env) throws IOException, InterruptedException {
      return;
    }

    @Override
    protected boolean abort(MasterProcedureEnv env) {
      return false;
    }

    @Override
    protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
      return;
    }

    @Override
    protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
      return;
    }

    @Override
    public Optional<RemoteProcedureDispatcher.RemoteOperation> remoteCallBuild(
        MasterProcedureEnv env, ServerName serverName) {
      return Optional
          .of(new RSProcedureDispatcher.ServerOperation(null, 0L, this.getClass(), new byte[0]));
    }

    @Override
    public synchronized void remoteOperationCompleted(MasterProcedureEnv env) {
      complete(env, null);
    }

    @Override
    public synchronized void remoteOperationFailed(MasterProcedureEnv env,
        RemoteProcedureException error) {
      complete(env, error);
    }

    @Override
    public void complete(MasterProcedureEnv env, Throwable error) {
      this.succ = true;
      return;
    }

    @Override
    public ServerName getServerName() {
      return targetServer;
    }

    @Override
    public boolean hasMetaTableRegion() {
      return false;
    }

    @Override
    public ServerOperationType getServerOperationType() {
      return SWITCH_RPC_THROTTLE;
    }

  }

  protected interface MockRSExecutor {
    AdminProtos.ExecuteProceduresResponse sendRequest(ServerName server,
        AdminProtos.ExecuteProceduresRequest req) throws IOException;
  }

  protected static class NoopRSExecutor implements MockRSExecutor {
    @Override
    public AdminProtos.ExecuteProceduresResponse sendRequest(ServerName server,
        AdminProtos.ExecuteProceduresRequest req) throws IOException {
      if (req.getOpenRegionCount() > 0) {
        for (AdminProtos.OpenRegionRequest request : req.getOpenRegionList()) {
          for (AdminProtos.OpenRegionRequest.RegionOpenInfo openReq : request.getOpenInfoList()) {
            execOpenRegion(server, openReq);
          }
        }
      }
      return AdminProtos.ExecuteProceduresResponse.getDefaultInstance();
    }

    protected AdminProtos.OpenRegionResponse.RegionOpeningState execOpenRegion(ServerName server,
        AdminProtos.OpenRegionRequest.RegionOpenInfo regionInfo) throws IOException {
      return null;
    }
  }

  protected static class MockRSProcedureDispatcher extends RSProcedureDispatcher {
    private MockRSExecutor mockRsExec;

    public MockRSProcedureDispatcher(final MasterServices master) {
      super(master);
    }

    public void setMockRsExecutor(final MockRSExecutor mockRsExec) {
      this.mockRsExec = mockRsExec;
    }

    @Override
    protected void remoteDispatch(ServerName serverName,
        @SuppressWarnings("rawtypes") Set<RemoteProcedure> remoteProcedures) {
      submitTask(new MockRSProcedureDispatcher.MockRemoteCall(serverName, remoteProcedures));
    }

    private class MockRemoteCall extends ExecuteProceduresRemoteCall {
      public MockRemoteCall(final ServerName serverName,
          @SuppressWarnings("rawtypes") final Set<RemoteProcedure> operations) {
        super(serverName, operations);
      }

      @Override
      protected AdminProtos.ExecuteProceduresResponse sendRequest(final ServerName serverName,
          final AdminProtos.ExecuteProceduresRequest request) throws IOException {
        return mockRsExec.sendRequest(serverName, request);
      }
    }
  }
}
