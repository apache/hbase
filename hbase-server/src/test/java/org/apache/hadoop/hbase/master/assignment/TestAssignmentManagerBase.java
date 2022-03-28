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
package org.apache.hadoop.hbase.master.assignment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CallQueueTooBigException;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.ServerMetricsBuilder;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.YouAreDeadException;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.ipc.CallTimeoutException;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureConstants;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.ProcedureSyncWait;
import org.apache.hadoop.hbase.master.procedure.RSProcedureDispatcher;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureMetrics;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore;
import org.apache.hadoop.hbase.regionserver.RegionServerAbortedException;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.ipc.RemoteException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CloseRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CloseRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ExecuteProceduresRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ExecuteProceduresResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.OpenRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.OpenRegionRequest.RegionOpenInfo;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.OpenRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.OpenRegionResponse.RegionOpeningState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionRequest;

/**
 * Base class for AM test.
 */
public abstract class TestAssignmentManagerBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestAssignmentManagerBase.class);

  @Rule
  public TestName name = new TestName();
  @Rule
  public final ExpectedException exception = ExpectedException.none();

  protected static final int PROC_NTHREADS = 64;
  protected static final int NREGIONS = 1 * 1000;
  protected static final int NSERVERS = Math.max(1, NREGIONS / 100);

  protected HBaseTestingUtil util;
  protected MockRSProcedureDispatcher rsDispatcher;
  protected MockMasterServices master;
  protected AssignmentManager am;
  protected NavigableMap<ServerName, SortedSet<byte[]>> regionsToRegionServers =
    new ConcurrentSkipListMap<ServerName, SortedSet<byte[]>>();
  // Simple executor to run some simple tasks.
  protected ScheduledExecutorService executor;

  protected ProcedureMetrics assignProcMetrics;
  protected ProcedureMetrics unassignProcMetrics;
  protected ProcedureMetrics moveProcMetrics;
  protected ProcedureMetrics reopenProcMetrics;
  protected ProcedureMetrics openProcMetrics;
  protected ProcedureMetrics closeProcMetrics;

  protected long assignSubmittedCount = 0;
  protected long assignFailedCount = 0;
  protected long unassignSubmittedCount = 0;
  protected long unassignFailedCount = 0;
  protected long moveSubmittedCount = 0;
  protected long moveFailedCount = 0;
  protected long reopenSubmittedCount = 0;
  protected long reopenFailedCount = 0;
  protected long openSubmittedCount = 0;
  protected long openFailedCount = 0;
  protected long closeSubmittedCount = 0;
  protected long closeFailedCount = 0;

  protected int newRsAdded;

  protected int getAssignMaxAttempts() {
    // Have many so we succeed eventually.
    return 1000;
  }

  protected void setupConfiguration(Configuration conf) throws Exception {
    CommonFSUtils.setRootDir(conf, util.getDataTestDir());
    conf.setBoolean(WALProcedureStore.USE_HSYNC_CONF_KEY, false);
    conf.setInt(WALProcedureStore.SYNC_WAIT_MSEC_CONF_KEY, 10);
    conf.setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, PROC_NTHREADS);
    conf.setInt(RSProcedureDispatcher.RS_RPC_STARTUP_WAIT_TIME_CONF_KEY, 1000);
    conf.setInt(AssignmentManager.ASSIGN_MAX_ATTEMPTS, getAssignMaxAttempts());
    // make retry for TRSP more frequent
    conf.setLong(ProcedureUtil.PROCEDURE_RETRY_SLEEP_INTERVAL_MS, 10);
    conf.setLong(ProcedureUtil.PROCEDURE_RETRY_MAX_SLEEP_TIME_MS, 100);
  }

  @Before
  public void setUp() throws Exception {
    util = new HBaseTestingUtil();
    this.executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
      .setUncaughtExceptionHandler((t, e) -> LOG.warn("Uncaught: ", e)).build());
    setupConfiguration(util.getConfiguration());
    master = new MockMasterServices(util.getConfiguration(), this.regionsToRegionServers);
    rsDispatcher = new MockRSProcedureDispatcher(master);
    master.start(NSERVERS, rsDispatcher);
    newRsAdded = 0;
    am = master.getAssignmentManager();
    assignProcMetrics = am.getAssignmentManagerMetrics().getAssignProcMetrics();
    unassignProcMetrics = am.getAssignmentManagerMetrics().getUnassignProcMetrics();
    moveProcMetrics = am.getAssignmentManagerMetrics().getMoveProcMetrics();
    reopenProcMetrics = am.getAssignmentManagerMetrics().getReopenProcMetrics();
    openProcMetrics = am.getAssignmentManagerMetrics().getOpenProcMetrics();
    closeProcMetrics = am.getAssignmentManagerMetrics().getCloseProcMetrics();
    setUpMeta();
  }

  protected void setUpMeta() throws Exception {
    rsDispatcher.setMockRsExecutor(new GoodRsExecutor());
    am.assign(RegionInfoBuilder.FIRST_META_REGIONINFO);
    am.wakeMetaLoadedEvent();
  }

  @After
  public void tearDown() throws Exception {
    master.stop("tearDown");
    this.executor.shutdownNow();
  }

  protected class NoopRsExecutor implements MockRSExecutor {
    @Override
    public ExecuteProceduresResponse sendRequest(ServerName server,
        ExecuteProceduresRequest request) throws IOException {
      if (request.getOpenRegionCount() > 0) {
        for (OpenRegionRequest req : request.getOpenRegionList()) {
          for (RegionOpenInfo openReq : req.getOpenInfoList()) {
            execOpenRegion(server, openReq);
          }
        }
      }
      if (request.getCloseRegionCount() > 0) {
        for (CloseRegionRequest req : request.getCloseRegionList()) {
          execCloseRegion(server, req.getRegion().getValue().toByteArray());
        }
      }
      return ExecuteProceduresResponse.newBuilder().build();
    }

    protected RegionOpeningState execOpenRegion(ServerName server, RegionOpenInfo regionInfo)
        throws IOException {
      return null;
    }

    protected CloseRegionResponse execCloseRegion(ServerName server, byte[] regionName)
        throws IOException {
      return null;
    }
  }

  protected Future<byte[]> submitProcedure(final Procedure<MasterProcedureEnv> proc) {
    return ProcedureSyncWait.submitProcedure(master.getMasterProcedureExecutor(), proc);
  }

  protected byte[] waitOnFuture(final Future<byte[]> future) throws Exception {
    try {
      return future.get(3, TimeUnit.MINUTES);
    } catch (ExecutionException e) {
      LOG.info("ExecutionException", e);
      Exception ee = (Exception) e.getCause();
      if (ee instanceof InterruptedIOException) {
        for (Procedure<?> p : this.master.getMasterProcedureExecutor().getProcedures()) {
          LOG.info(p.toStringDetails());
        }
      }
      throw (Exception) e.getCause();
    }
  }

  // ============================================================================================
  // Helpers
  // ============================================================================================
  protected void bulkSubmit(TransitRegionStateProcedure[] procs) throws Exception {
    Thread[] threads = new Thread[PROC_NTHREADS];
    for (int i = 0; i < threads.length; ++i) {
      final int threadId = i;
      threads[i] = new Thread() {
        @Override
        public void run() {
          TableName tableName = TableName.valueOf("table-" + threadId);
          int n = (procs.length / threads.length);
          int start = threadId * n;
          int stop = start + n;
          for (int j = start; j < stop; ++j) {
            procs[j] = createAndSubmitAssign(tableName, j);
          }
        }
      };
      threads[i].start();
    }
    for (int i = 0; i < threads.length; ++i) {
      threads[i].join();
    }
    for (int i = procs.length - 1; i >= 0 && procs[i] == null; --i) {
      procs[i] = createAndSubmitAssign(TableName.valueOf("table-sync"), i);
    }
  }

  protected TransitRegionStateProcedure createAndSubmitAssign(TableName tableName, int regionId) {
    RegionInfo hri = createRegionInfo(tableName, regionId);
    TransitRegionStateProcedure proc = createAssignProcedure(hri);
    master.getMasterProcedureExecutor().submitProcedure(proc);
    return proc;
  }

  protected RegionInfo createRegionInfo(final TableName tableName, final long regionId) {
    return RegionInfoBuilder.newBuilder(tableName).setStartKey(Bytes.toBytes(regionId))
      .setEndKey(Bytes.toBytes(regionId + 1)).setSplit(false).setRegionId(0).build();
  }

  protected TransitRegionStateProcedure createAssignProcedure(RegionInfo hri) {
    return am.createAssignProcedures(Arrays.asList(hri))[0];
  }

  protected TransitRegionStateProcedure createUnassignProcedure(RegionInfo hri) {
    RegionStateNode regionNode = am.getRegionStates().getRegionStateNode(hri);
    TransitRegionStateProcedure proc;
    regionNode.lock();
    try {
      assertFalse(regionNode.isInTransition());
      proc = TransitRegionStateProcedure
        .unassign(master.getMasterProcedureExecutor().getEnvironment(), hri);
      regionNode.setProcedure(proc);
    } finally {
      regionNode.unlock();
    }
    return proc;
  }

  protected void sendTransitionReport(final ServerName serverName,
      final org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionInfo regionInfo,
      final TransitionCode state, long seqId) throws IOException {
    ReportRegionStateTransitionRequest.Builder req =
      ReportRegionStateTransitionRequest.newBuilder();
    req.setServer(ProtobufUtil.toServerName(serverName));
    req.addTransition(RegionStateTransition.newBuilder().addRegionInfo(regionInfo)
      .setTransitionCode(state).setOpenSeqNum(seqId).build());
    am.reportRegionStateTransition(req.build());
  }

  protected void doCrash(final ServerName serverName) {
    this.master.getServerManager().moveFromOnlineToDeadServers(serverName);
    this.am.submitServerCrash(serverName, false/* No WALs here */, false);
    // add a new server to avoid killing all the region servers which may hang the UTs
    ServerName newSn = ServerName.valueOf("localhost", 10000 + newRsAdded, 1);
    newRsAdded++;
    try {
      this.master.getServerManager().regionServerReport(newSn, ServerMetricsBuilder
        .newBuilder(newSn).setLastReportTimestamp(EnvironmentEdgeManager.currentTime()).build());
    } catch (YouAreDeadException e) {
      // should not happen
      throw new UncheckedIOException(e);
    }
  }

  protected void doRestart(final ServerName serverName) {
    try {
      this.master.restartRegionServer(serverName);
    } catch (IOException e) {
      LOG.warn("Can not restart RS with new startcode");
    }
  }

  protected class GoodRsExecutor extends NoopRsExecutor {
    @Override
    protected RegionOpeningState execOpenRegion(ServerName server, RegionOpenInfo openReq)
        throws IOException {
      RegionInfo hri = ProtobufUtil.toRegionInfo(openReq.getRegion());
      long previousOpenSeqNum =
        am.getRegionStates().getOrCreateRegionStateNode(hri).getOpenSeqNum();
      sendTransitionReport(server, openReq.getRegion(), TransitionCode.OPENED,
        previousOpenSeqNum + 2);
      // Concurrency?
      // Now update the state of our cluster in regionsToRegionServers.
      SortedSet<byte[]> regions = regionsToRegionServers.get(server);
      if (regions == null) {
        regions = new ConcurrentSkipListSet<byte[]>(Bytes.BYTES_COMPARATOR);
        regionsToRegionServers.put(server, regions);
      }
      if (regions.contains(hri.getRegionName())) {
        throw new UnsupportedOperationException(hri.getRegionNameAsString());
      }
      regions.add(hri.getRegionName());
      return RegionOpeningState.OPENED;
    }

    @Override
    protected CloseRegionResponse execCloseRegion(ServerName server, byte[] regionName)
        throws IOException {
      RegionInfo hri = am.getRegionInfo(regionName);
      sendTransitionReport(server, ProtobufUtil.toRegionInfo(hri), TransitionCode.CLOSED, -1);
      return CloseRegionResponse.newBuilder().setClosed(true).build();
    }
  }

  protected static class ServerNotYetRunningRsExecutor implements MockRSExecutor {
    @Override
    public ExecuteProceduresResponse sendRequest(ServerName server, ExecuteProceduresRequest req)
        throws IOException {
      throw new ServerNotRunningYetException("wait on server startup");
    }
  }

  protected static class FaultyRsExecutor implements MockRSExecutor {
    private final IOException exception;

    public FaultyRsExecutor(final IOException exception) {
      this.exception = exception;
    }

    @Override
    public ExecuteProceduresResponse sendRequest(ServerName server, ExecuteProceduresRequest req)
        throws IOException {
      throw exception;
    }
  }

  protected class SocketTimeoutRsExecutor extends GoodRsExecutor {
    private final int timeoutTimes;

    private ServerName lastServer;
    private int retries;

    public SocketTimeoutRsExecutor(int timeoutTimes) {
      this.timeoutTimes = timeoutTimes;
    }

    @Override
    public ExecuteProceduresResponse sendRequest(ServerName server, ExecuteProceduresRequest req)
        throws IOException {
      // SocketTimeoutException should be a temporary problem
      // unless the server will be declared dead.
      retries++;
      if (retries == 1) {
        lastServer = server;
      }
      if (retries <= timeoutTimes) {
        LOG.debug("Socket timeout for server=" + server + " retries=" + retries);
        // should not change the server if the server is not dead yet.
        assertEquals(lastServer, server);
        if (retries == timeoutTimes) {
          LOG.info("Mark server=" + server + " as dead. retries=" + retries);
          master.getServerManager().moveFromOnlineToDeadServers(server);
          executor.schedule(new Runnable() {
            @Override
            public void run() {
              LOG.info("Sending in CRASH of " + server);
              doCrash(server);
            }
          }, 1, TimeUnit.SECONDS);
        }
        throw new SocketTimeoutException("simulate socket timeout");
      } else {
        // should select another server
        assertNotEquals(lastServer, server);
        return super.sendRequest(server, req);
      }
    }
  }

  protected class CallQueueTooBigOnceRsExecutor extends GoodRsExecutor {

    private boolean invoked = false;

    private ServerName lastServer;

    @Override
    public ExecuteProceduresResponse sendRequest(ServerName server, ExecuteProceduresRequest req)
        throws IOException {
      if (!invoked) {
        lastServer = server;
        invoked = true;
        throw new CallQueueTooBigException("simulate queue full");
      }
      // better select another server since the server is over loaded, but anyway, it is fine to
      // still select the same server since it is not dead yet...
      if (lastServer.equals(server)) {
        LOG.warn("We still select the same server, which is not good.");
      }
      return super.sendRequest(server, req);
    }
  }

  protected class TimeoutThenCallQueueTooBigRsExecutor extends GoodRsExecutor {

    private final int queueFullTimes;

    private int retries;

    private ServerName lastServer;

    public TimeoutThenCallQueueTooBigRsExecutor(int queueFullTimes) {
      this.queueFullTimes = queueFullTimes;
    }

    @Override
    public ExecuteProceduresResponse sendRequest(ServerName server, ExecuteProceduresRequest req)
        throws IOException {
      retries++;
      if (retries == 1) {
        lastServer = server;
        throw new CallTimeoutException("simulate call timeout");
      }
      // should always retry on the same server
      assertEquals(lastServer, server);
      if (retries < queueFullTimes) {
        throw new CallQueueTooBigException("simulate queue full");
      }
      return super.sendRequest(server, req);
    }
  }

  /**
   * Takes open request and then returns nothing so acts like a RS that went zombie. No response (so
   * proc is stuck/suspended on the Master and won't wake up.). We then send in a crash for this
   * server after a few seconds; crash is supposed to take care of the suspended procedures.
   */
  protected class HangThenRSCrashExecutor extends GoodRsExecutor {
    private int invocations;

    @Override
    protected RegionOpeningState execOpenRegion(final ServerName server, RegionOpenInfo openReq)
        throws IOException {
      if (this.invocations++ > 0) {
        // Return w/o problem the second time through here.
        return super.execOpenRegion(server, openReq);
      }
      // The procedure on master will just hang forever because nothing comes back
      // from the RS in this case.
      LOG.info("Return null response from serverName=" + server + "; means STUCK...TODO timeout");
      executor.schedule(new Runnable() {
        @Override
        public void run() {
          LOG.info("Sending in CRASH of " + server);
          doCrash(server);
        }
      }, 1, TimeUnit.SECONDS);
      return null;
    }
  }

  /**
   * Takes open request and then returns nothing so acts like a RS that went zombie. No response (so
   * proc is stuck/suspended on the Master and won't wake up.). Different with
   * HangThenRSCrashExecutor, HangThenRSCrashExecutor will create ServerCrashProcedure to handle the
   * server crash. However, this HangThenRSRestartExecutor will restart RS directly, situation for
   * RS crashed when SCP is not enabled.
   */
  protected class HangThenRSRestartExecutor extends GoodRsExecutor {
    private int invocations;

    @Override
    protected RegionOpeningState execOpenRegion(final ServerName server, RegionOpenInfo openReq)
        throws IOException {
      if (this.invocations++ > 0) {
        // Return w/o problem the second time through here.
        return super.execOpenRegion(server, openReq);
      }
      // The procedure on master will just hang forever because nothing comes back
      // from the RS in this case.
      LOG.info("Return null response from serverName=" + server + "; means STUCK...TODO timeout");
      executor.schedule(new Runnable() {
        @Override
        public void run() {
          LOG.info("Restarting RS of " + server);
          doRestart(server);
        }
      }, 1, TimeUnit.SECONDS);
      return null;
    }
  }

  protected class HangOnCloseThenRSCrashExecutor extends GoodRsExecutor {
    public static final int TYPES_OF_FAILURE = 6;
    private int invocations;

    @Override
    protected CloseRegionResponse execCloseRegion(ServerName server, byte[] regionName)
        throws IOException {
      switch (this.invocations++) {
        case 0:
          throw new NotServingRegionException("Fake");
        case 1:
          executor.schedule(new Runnable() {
            @Override
            public void run() {
              LOG.info("Sending in CRASH of " + server);
              doCrash(server);
            }
          }, 1, TimeUnit.SECONDS);
          throw new RegionServerAbortedException("Fake!");
        case 2:
          executor.schedule(new Runnable() {
            @Override
            public void run() {
              LOG.info("Sending in CRASH of " + server);
              doCrash(server);
            }
          }, 1, TimeUnit.SECONDS);
          throw new RegionServerStoppedException("Fake!");
        case 3:
          throw new ServerNotRunningYetException("Fake!");
        case 4:
          LOG.info("Returned null from serverName={}; means STUCK...TODO timeout", server);
          executor.schedule(new Runnable() {
            @Override
            public void run() {
              LOG.info("Sending in CRASH of " + server);
              doCrash(server);
            }
          }, 1, TimeUnit.SECONDS);
          return null;
        default:
          return super.execCloseRegion(server, regionName);
      }
    }
  }

  protected class RandRsExecutor extends NoopRsExecutor {
    @Override
    public ExecuteProceduresResponse sendRequest(ServerName server, ExecuteProceduresRequest req)
        throws IOException {
      switch (ThreadLocalRandom.current().nextInt(5)) {
        case 0:
          throw new ServerNotRunningYetException("wait on server startup");
        case 1:
          throw new SocketTimeoutException("simulate socket timeout");
        case 2:
          throw new RemoteException("java.io.IOException", "unexpected exception");
        default:
          // fall out
      }
      return super.sendRequest(server, req);
    }

    @Override
    protected RegionOpeningState execOpenRegion(final ServerName server, RegionOpenInfo openReq)
        throws IOException {
      RegionInfo hri = ProtobufUtil.toRegionInfo(openReq.getRegion());
      long previousOpenSeqNum =
        am.getRegionStates().getOrCreateRegionStateNode(hri).getOpenSeqNum();
      switch (ThreadLocalRandom.current().nextInt(3)) {
        case 0:
          LOG.info("Return OPENED response");
          sendTransitionReport(server, openReq.getRegion(), TransitionCode.OPENED,
            previousOpenSeqNum + 2);
          return OpenRegionResponse.RegionOpeningState.OPENED;
        case 1:
          LOG.info("Return transition report that FAILED_OPEN/FAILED_OPENING response");
          sendTransitionReport(server, openReq.getRegion(), TransitionCode.FAILED_OPEN, -1);
          return OpenRegionResponse.RegionOpeningState.FAILED_OPENING;
        default:
          // fall out
      }
      // The procedure on master will just hang forever because nothing comes back
      // from the RS in this case.
      LOG.info("Return null as response; means proc stuck so we send in a crash report after" +
        " a few seconds...");
      executor.schedule(new Runnable() {
        @Override
        public void run() {
          LOG.info("Delayed CRASHING of " + server);
          doCrash(server);
        }
      }, 5, TimeUnit.SECONDS);
      return null;
    }

    @Override
    protected CloseRegionResponse execCloseRegion(ServerName server, byte[] regionName)
        throws IOException {
      CloseRegionResponse.Builder resp = CloseRegionResponse.newBuilder();
      boolean closed = ThreadLocalRandom.current().nextBoolean();
      if (closed) {
        RegionInfo hri = am.getRegionInfo(regionName);
        sendTransitionReport(server, ProtobufUtil.toRegionInfo(hri), TransitionCode.CLOSED, -1);
      }
      resp.setClosed(closed);
      return resp.build();
    }
  }

  protected interface MockRSExecutor {
    ExecuteProceduresResponse sendRequest(ServerName server, ExecuteProceduresRequest req)
        throws IOException;
  }

  protected class MockRSProcedureDispatcher extends RSProcedureDispatcher {
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
      submitTask(new MockRemoteCall(serverName, remoteProcedures));
    }

    private class MockRemoteCall extends ExecuteProceduresRemoteCall {
      public MockRemoteCall(final ServerName serverName,
          @SuppressWarnings("rawtypes") final Set<RemoteProcedure> operations) {
        super(serverName, operations);
      }

      @Override
      protected ExecuteProceduresResponse sendRequest(final ServerName serverName,
          final ExecuteProceduresRequest request) throws IOException {
        return mockRsExec.sendRequest(serverName, request);
      }
    }
  }

  protected final void collectAssignmentManagerMetrics() {
    assignSubmittedCount = assignProcMetrics.getSubmittedCounter().getCount();
    assignFailedCount = assignProcMetrics.getFailedCounter().getCount();
    unassignSubmittedCount = unassignProcMetrics.getSubmittedCounter().getCount();
    unassignFailedCount = unassignProcMetrics.getFailedCounter().getCount();
    moveSubmittedCount = moveProcMetrics.getSubmittedCounter().getCount();
    moveFailedCount = moveProcMetrics.getFailedCounter().getCount();
    reopenSubmittedCount = reopenProcMetrics.getSubmittedCounter().getCount();
    reopenFailedCount = reopenProcMetrics.getFailedCounter().getCount();
    openSubmittedCount = openProcMetrics.getSubmittedCounter().getCount();
    openFailedCount = openProcMetrics.getFailedCounter().getCount();
    closeSubmittedCount = closeProcMetrics.getSubmittedCounter().getCount();
    closeFailedCount = closeProcMetrics.getFailedCounter().getCount();
  }
}
