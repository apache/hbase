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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.exceptions.UnexpectedStateException;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureConstants;
import org.apache.hadoop.hbase.master.procedure.ProcedureSyncWait;
import org.apache.hadoop.hbase.master.procedure.RSProcedureDispatcher;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureMetrics;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.regionserver.RegionServerAbortedException;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

@Category({MasterTests.class, LargeTests.class})
public class TestAssignmentManager {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAssignmentManager.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestAssignmentManager.class);

  @Rule public TestName name = new TestName();
  @Rule public final ExpectedException exception = ExpectedException.none();

  private static final int PROC_NTHREADS = 64;
  private static final int NREGIONS = 1 * 1000;
  private static final int NSERVERS = Math.max(1, NREGIONS / 100);

  private HBaseTestingUtility UTIL;
  private MockRSProcedureDispatcher rsDispatcher;
  private MockMasterServices master;
  private AssignmentManager am;
  private NavigableMap<ServerName, SortedSet<byte []>> regionsToRegionServers =
      new ConcurrentSkipListMap<ServerName, SortedSet<byte []>>();
  // Simple executor to run some simple tasks.
  private ScheduledExecutorService executor;

  private ProcedureMetrics assignProcMetrics;
  private ProcedureMetrics unassignProcMetrics;

  private long assignSubmittedCount = 0;
  private long assignFailedCount = 0;
  private long unassignSubmittedCount = 0;
  private long unassignFailedCount = 0;

  private void setupConfiguration(Configuration conf) throws Exception {
    FSUtils.setRootDir(conf, UTIL.getDataTestDir());
    conf.setBoolean(WALProcedureStore.USE_HSYNC_CONF_KEY, false);
    conf.setInt(WALProcedureStore.SYNC_WAIT_MSEC_CONF_KEY, 10);
    conf.setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, PROC_NTHREADS);
    conf.setInt(RSProcedureDispatcher.RS_RPC_STARTUP_WAIT_TIME_CONF_KEY, 1000);
    conf.setInt(AssignmentManager.ASSIGN_MAX_ATTEMPTS, 100); // Have many so we succeed eventually.
  }

  @Before
  public void setUp() throws Exception {
    UTIL = new HBaseTestingUtility();
    this.executor = Executors.newSingleThreadScheduledExecutor();
    setupConfiguration(UTIL.getConfiguration());
    master = new MockMasterServices(UTIL.getConfiguration(), this.regionsToRegionServers);
    rsDispatcher = new MockRSProcedureDispatcher(master);
    master.start(NSERVERS, rsDispatcher);
    am = master.getAssignmentManager();
    assignProcMetrics = am.getAssignmentManagerMetrics().getAssignProcMetrics();
    unassignProcMetrics = am.getAssignmentManagerMetrics().getUnassignProcMetrics();
    setUpMeta();
  }

  private void setUpMeta() throws Exception {
    rsDispatcher.setMockRsExecutor(new GoodRsExecutor());
    am.assign(RegionInfoBuilder.FIRST_META_REGIONINFO);
    am.wakeMetaLoadedEvent();
    am.setFailoverCleanupDone(true);
  }

  @After
  public void tearDown() throws Exception {
    master.stop("tearDown");
    this.executor.shutdownNow();
  }

  @Test (expected=NullPointerException.class)
  public void testWaitServerReportEventWithNullServer() throws UnexpectedStateException {
    // Test what happens if we pass in null server. I'd expect it throws NPE.
    if (this.am.waitServerReportEvent(null, null)) throw new UnexpectedStateException();
  }

  @Test
  public void testAssignWithGoodExec() throws Exception {
    // collect AM metrics before test
    collectAssignmentManagerMetrics();

    testAssign(new GoodRsExecutor());

    assertEquals(assignSubmittedCount + NREGIONS,
        assignProcMetrics.getSubmittedCounter().getCount());
    assertEquals(assignFailedCount, assignProcMetrics.getFailedCounter().getCount());
  }

  @Test
  public void testAssignAndCrashBeforeResponse() throws Exception {
    final TableName tableName = TableName.valueOf("testAssignAndCrashBeforeResponse");
    final RegionInfo hri = createRegionInfo(tableName, 1);
    rsDispatcher.setMockRsExecutor(new HangThenRSCrashExecutor());
    AssignProcedure proc = am.createAssignProcedure(hri);
    waitOnFuture(submitProcedure(proc));
  }

  @Test
  public void testUnassignAndCrashBeforeResponse() throws Exception {
    final TableName tableName = TableName.valueOf("testAssignAndCrashBeforeResponse");
    final RegionInfo hri = createRegionInfo(tableName, 1);
    rsDispatcher.setMockRsExecutor(new HangOnCloseThenRSCrashExecutor());
    for (int i = 0; i < HangOnCloseThenRSCrashExecutor.TYPES_OF_FAILURE; i++) {
      AssignProcedure assign = am.createAssignProcedure(hri);
      waitOnFuture(submitProcedure(assign));
      UnassignProcedure unassign = am.createUnassignProcedure(hri,
          am.getRegionStates().getRegionServerOfRegion(hri), false);
      waitOnFuture(submitProcedure(unassign));
    }
  }

  @Test
  public void testAssignWithRandExec() throws Exception {
    final TableName tableName = TableName.valueOf("testAssignWithRandExec");
    final RegionInfo hri = createRegionInfo(tableName, 1);

    rsDispatcher.setMockRsExecutor(new RandRsExecutor());
    // Loop a bunch of times so we hit various combos of exceptions.
    for (int i = 0; i < 10; i++) {
      LOG.info("ROUND=" + i);
      AssignProcedure proc = am.createAssignProcedure(hri);
      waitOnFuture(submitProcedure(proc));
    }
  }

  @Ignore @Test // Disabled for now. Since HBASE-18551, this mock is insufficient.
  public void testSocketTimeout() throws Exception {
    final TableName tableName = TableName.valueOf(this.name.getMethodName());
    final RegionInfo hri = createRegionInfo(tableName, 1);

    // collect AM metrics before test
    collectAssignmentManagerMetrics();

    rsDispatcher.setMockRsExecutor(new SocketTimeoutRsExecutor(20, 3));
    waitOnFuture(submitProcedure(am.createAssignProcedure(hri)));

    rsDispatcher.setMockRsExecutor(new SocketTimeoutRsExecutor(20, 1));
    // exception.expect(ServerCrashException.class);
    waitOnFuture(submitProcedure(am.createUnassignProcedure(hri, null, false)));

    assertEquals(assignSubmittedCount + 1, assignProcMetrics.getSubmittedCounter().getCount());
    assertEquals(assignFailedCount, assignProcMetrics.getFailedCounter().getCount());
    assertEquals(unassignSubmittedCount + 1, unassignProcMetrics.getSubmittedCounter().getCount());
    assertEquals(unassignFailedCount + 1, unassignProcMetrics.getFailedCounter().getCount());
  }

  @Test
  public void testServerNotYetRunning() throws Exception {
    testRetriesExhaustedFailure(TableName.valueOf(this.name.getMethodName()),
      new ServerNotYetRunningRsExecutor());
  }

  private void testRetriesExhaustedFailure(final TableName tableName,
      final MockRSExecutor executor) throws Exception {
    final RegionInfo hri = createRegionInfo(tableName, 1);

    // collect AM metrics before test
    collectAssignmentManagerMetrics();

    // Test Assign operation failure
    rsDispatcher.setMockRsExecutor(executor);
    try {
      waitOnFuture(submitProcedure(am.createAssignProcedure(hri)));
      fail("unexpected assign completion");
    } catch (RetriesExhaustedException e) {
      // expected exception
      LOG.info("expected exception from assign operation: " + e.getMessage(), e);
    }

    // Assign the region (without problems)
    rsDispatcher.setMockRsExecutor(new GoodRsExecutor());
    waitOnFuture(submitProcedure(am.createAssignProcedure(hri)));

    // TODO: Currently unassign just keeps trying until it sees a server crash.
    // There is no count on unassign.
    /*
    // Test Unassign operation failure
    rsDispatcher.setMockRsExecutor(executor);
    waitOnFuture(submitProcedure(am.createUnassignProcedure(hri, null, false)));

    assertEquals(assignSubmittedCount + 2, assignProcMetrics.getSubmittedCounter().getCount());
    assertEquals(assignFailedCount + 1, assignProcMetrics.getFailedCounter().getCount());
    assertEquals(unassignSubmittedCount + 1, unassignProcMetrics.getSubmittedCounter().getCount());

    // TODO: We supposed to have 1 failed assign, 1 successful assign and a failed unassign
    // operation. But ProcV2 framework marks aborted unassign operation as success. Fix it!
    assertEquals(unassignFailedCount, unassignProcMetrics.getFailedCounter().getCount());
    */
  }


  @Test
  public void testIOExceptionOnAssignment() throws Exception {
    // collect AM metrics before test
    collectAssignmentManagerMetrics();

    testFailedOpen(TableName.valueOf("testExceptionOnAssignment"),
      new FaultyRsExecutor(new IOException("test fault")));

    assertEquals(assignSubmittedCount + 1, assignProcMetrics.getSubmittedCounter().getCount());
    assertEquals(assignFailedCount + 1, assignProcMetrics.getFailedCounter().getCount());
  }

  @Test
  public void testDoNotRetryExceptionOnAssignment() throws Exception {
    // collect AM metrics before test
    collectAssignmentManagerMetrics();

    testFailedOpen(TableName.valueOf("testDoNotRetryExceptionOnAssignment"),
      new FaultyRsExecutor(new DoNotRetryIOException("test do not retry fault")));

    assertEquals(assignSubmittedCount + 1, assignProcMetrics.getSubmittedCounter().getCount());
    assertEquals(assignFailedCount + 1, assignProcMetrics.getFailedCounter().getCount());
  }

  private void testFailedOpen(final TableName tableName,
      final MockRSExecutor executor) throws Exception {
    final RegionInfo hri = createRegionInfo(tableName, 1);

    // Test Assign operation failure
    rsDispatcher.setMockRsExecutor(executor);
    try {
      waitOnFuture(submitProcedure(am.createAssignProcedure(hri)));
      fail("unexpected assign completion");
    } catch (RetriesExhaustedException e) {
      // expected exception
      LOG.info("REGION STATE " + am.getRegionStates().getRegionStateNode(hri));
      LOG.info("expected exception from assign operation: " + e.getMessage(), e);
      assertEquals(true, am.getRegionStates().getRegionState(hri).isFailedOpen());
    }
  }

  private void testAssign(final MockRSExecutor executor) throws Exception {
    testAssign(executor, NREGIONS);
  }

  private void testAssign(final MockRSExecutor executor, final int nregions) throws Exception {
    rsDispatcher.setMockRsExecutor(executor);

    AssignProcedure[] assignments = new AssignProcedure[nregions];

    long st = System.currentTimeMillis();
    bulkSubmit(assignments);

    for (int i = 0; i < assignments.length; ++i) {
      ProcedureTestingUtility.waitProcedure(
        master.getMasterProcedureExecutor(), assignments[i]);
      assertTrue(assignments[i].toString(), assignments[i].isSuccess());
    }
    long et = System.currentTimeMillis();
    float sec = ((et - st) / 1000.0f);
    LOG.info(String.format("[T] Assigning %dprocs in %s (%.2fproc/sec)",
        assignments.length, StringUtils.humanTimeDiff(et - st), assignments.length / sec));
  }

  @Test
  public void testAssignAnAssignedRegion() throws Exception {
    final TableName tableName = TableName.valueOf("testAssignAnAssignedRegion");
    final RegionInfo hri = createRegionInfo(tableName, 1);

    // collect AM metrics before test
    collectAssignmentManagerMetrics();

    rsDispatcher.setMockRsExecutor(new GoodRsExecutor());

    final Future<byte[]> futureA = submitProcedure(am.createAssignProcedure(hri));

    // wait first assign
    waitOnFuture(futureA);
    am.getRegionStates().isRegionInState(hri, State.OPEN);
    // Second should be a noop. We should recognize region is already OPEN internally
    // and skip out doing nothing.
    // wait second assign
    final Future<byte[]> futureB = submitProcedure(am.createAssignProcedure(hri));
    waitOnFuture(futureB);
    am.getRegionStates().isRegionInState(hri, State.OPEN);
    // TODO: What else can we do to ensure just a noop.

    // TODO: Though second assign is noop, it's considered success, can noop be handled in a
    // better way?
    assertEquals(assignSubmittedCount + 2, assignProcMetrics.getSubmittedCounter().getCount());
    assertEquals(assignFailedCount, assignProcMetrics.getFailedCounter().getCount());

  }

  @Test
  public void testUnassignAnUnassignedRegion() throws Exception {
    final TableName tableName = TableName.valueOf("testUnassignAnUnassignedRegion");
    final RegionInfo hri = createRegionInfo(tableName, 1);

    // collect AM metrics before test
    collectAssignmentManagerMetrics();

    rsDispatcher.setMockRsExecutor(new GoodRsExecutor());

    // assign the region first
    waitOnFuture(submitProcedure(am.createAssignProcedure(hri)));

    final Future<byte[]> futureA = submitProcedure(am.createUnassignProcedure(hri, null, false));

    // Wait first unassign.
    waitOnFuture(futureA);
    am.getRegionStates().isRegionInState(hri, State.CLOSED);
    // Second should be a noop. We should recognize region is already CLOSED internally
    // and skip out doing nothing.
    final Future<byte[]> futureB =
        submitProcedure(am.createUnassignProcedure(hri,
            ServerName.valueOf("example.org,1234,1"), false));
    waitOnFuture(futureB);
    // Ensure we are still CLOSED.
    am.getRegionStates().isRegionInState(hri, State.CLOSED);
    // TODO: What else can we do to ensure just a noop.

    assertEquals(assignSubmittedCount + 1, assignProcMetrics.getSubmittedCounter().getCount());
    assertEquals(assignFailedCount, assignProcMetrics.getFailedCounter().getCount());
    // TODO: Though second unassign is noop, it's considered success, can noop be handled in a
    // better way?
    assertEquals(unassignSubmittedCount + 2, unassignProcMetrics.getSubmittedCounter().getCount());
    assertEquals(unassignFailedCount, unassignProcMetrics.getFailedCounter().getCount());
  }

  /**
   * It is possible that when AM send assign meta request to a RS successfully,
   * but RS can not send back any response, which cause master startup hangs forever
   */
  @Test
  public void testAssignMetaAndCrashBeforeResponse() throws Exception {
    tearDown();
    // See setUp(), start HBase until set up meta
    UTIL = new HBaseTestingUtility();
    this.executor = Executors.newSingleThreadScheduledExecutor();
    setupConfiguration(UTIL.getConfiguration());
    master = new MockMasterServices(UTIL.getConfiguration(), this.regionsToRegionServers);
    rsDispatcher = new MockRSProcedureDispatcher(master);
    master.start(NSERVERS, rsDispatcher);
    am = master.getAssignmentManager();

    // Assign meta
    master.setServerCrashProcessingEnabled(false);
    rsDispatcher.setMockRsExecutor(new HangThenRSRestartExecutor());
    am.assign(RegionInfoBuilder.FIRST_META_REGIONINFO);
    assertEquals(true, am.isMetaInitialized());

    // set it back as default, see setUpMeta()
    master.setServerCrashProcessingEnabled(true);
    am.wakeMetaLoadedEvent();
    am.setFailoverCleanupDone(true);
  }

  private Future<byte[]> submitProcedure(final Procedure proc) {
    return ProcedureSyncWait.submitProcedure(master.getMasterProcedureExecutor(), proc);
  }

  private byte[] waitOnFuture(final Future<byte[]> future) throws Exception {
    try {
      return future.get(5, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      LOG.info("ExecutionException", e);
      Exception ee = (Exception)e.getCause();
      if (ee instanceof InterruptedIOException) {
        for (Procedure p: this.master.getMasterProcedureExecutor().getProcedures()) {
          LOG.info(p.toStringDetails());
        }
      }
      throw (Exception)e.getCause();
    }
  }

  // ============================================================================================
  //  Helpers
  // ============================================================================================
  private void bulkSubmit(final AssignProcedure[] procs) throws Exception {
    final Thread[] threads = new Thread[PROC_NTHREADS];
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

  private AssignProcedure createAndSubmitAssign(TableName tableName, int regionId) {
    RegionInfo hri = createRegionInfo(tableName, regionId);
    AssignProcedure proc = am.createAssignProcedure(hri);
    master.getMasterProcedureExecutor().submitProcedure(proc);
    return proc;
  }

  private UnassignProcedure createAndSubmitUnassign(TableName tableName, int regionId) {
    RegionInfo hri = createRegionInfo(tableName, regionId);
    UnassignProcedure proc = am.createUnassignProcedure(hri, null, false);
    master.getMasterProcedureExecutor().submitProcedure(proc);
    return proc;
  }

  private RegionInfo createRegionInfo(final TableName tableName, final long regionId) {
    return RegionInfoBuilder.newBuilder(tableName)
        .setStartKey(Bytes.toBytes(regionId))
        .setEndKey(Bytes.toBytes(regionId + 1))
        .setSplit(false)
        .setRegionId(0)
        .build();
  }

  private void sendTransitionReport(final ServerName serverName,
      final org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionInfo regionInfo,
      final TransitionCode state) throws IOException {
    ReportRegionStateTransitionRequest.Builder req =
      ReportRegionStateTransitionRequest.newBuilder();
    req.setServer(ProtobufUtil.toServerName(serverName));
    req.addTransition(RegionStateTransition.newBuilder()
      .addRegionInfo(regionInfo)
      .setTransitionCode(state)
      .setOpenSeqNum(1)
      .build());
    am.reportRegionStateTransition(req.build());
  }

  private void doCrash(final ServerName serverName) {
    this.am.submitServerCrash(serverName, false/*No WALs here*/);
  }

  private void doRestart(final ServerName serverName) {
    try {
      this.master.restartRegionServer(serverName);
    } catch (IOException e) {
      LOG.warn("Can not restart RS with new startcode");
    }
  }

  private class NoopRsExecutor implements MockRSExecutor {
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

  private class GoodRsExecutor extends NoopRsExecutor {
    @Override
    protected RegionOpeningState execOpenRegion(ServerName server, RegionOpenInfo openReq)
        throws IOException {
      sendTransitionReport(server, openReq.getRegion(), TransitionCode.OPENED);
      // Concurrency?
      // Now update the state of our cluster in regionsToRegionServers.
      SortedSet<byte []> regions = regionsToRegionServers.get(server);
      if (regions == null) {
        regions = new ConcurrentSkipListSet<byte[]>(Bytes.BYTES_COMPARATOR);
        regionsToRegionServers.put(server, regions);
      }
      RegionInfo hri = ProtobufUtil.toRegionInfo(openReq.getRegion());
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
      sendTransitionReport(server, ProtobufUtil.toRegionInfo(hri), TransitionCode.CLOSED);
      return CloseRegionResponse.newBuilder().setClosed(true).build();
    }
  }

  private static class ServerNotYetRunningRsExecutor implements MockRSExecutor {
    @Override
    public ExecuteProceduresResponse sendRequest(ServerName server, ExecuteProceduresRequest req)
        throws IOException {
      throw new ServerNotRunningYetException("wait on server startup");
    }
  }

  private static class FaultyRsExecutor implements MockRSExecutor {
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

  private class SocketTimeoutRsExecutor extends GoodRsExecutor {
    private final int maxSocketTimeoutRetries;
    private final int maxServerRetries;

    private ServerName lastServer;
    private int sockTimeoutRetries;
    private int serverRetries;

    public SocketTimeoutRsExecutor(int maxSocketTimeoutRetries, int maxServerRetries) {
      this.maxServerRetries = maxServerRetries;
      this.maxSocketTimeoutRetries = maxSocketTimeoutRetries;
    }

    @Override
    public ExecuteProceduresResponse sendRequest(ServerName server, ExecuteProceduresRequest req)
        throws IOException {
      // SocketTimeoutException should be a temporary problem
      // unless the server will be declared dead.
      if (sockTimeoutRetries++ < maxSocketTimeoutRetries) {
        if (sockTimeoutRetries == 1) assertNotEquals(lastServer, server);
        lastServer = server;
        LOG.debug("Socket timeout for server=" + server + " retries=" + sockTimeoutRetries);
        throw new SocketTimeoutException("simulate socket timeout");
      } else if (serverRetries++ < maxServerRetries) {
        LOG.info("Mark server=" + server + " as dead. serverRetries=" + serverRetries);
        master.getServerManager().moveFromOnlineToDeadServers(server);
        sockTimeoutRetries = 0;
        throw new SocketTimeoutException("simulate socket timeout");
      } else {
        return super.sendRequest(server, req);
      }
    }
  }

  /**
   * Takes open request and then returns nothing so acts like a RS that went zombie.
   * No response (so proc is stuck/suspended on the Master and won't wake up.). We
   * then send in a crash for this server after a few seconds; crash is supposed to
   * take care of the suspended procedures.
   */
  private class HangThenRSCrashExecutor extends GoodRsExecutor {
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
   * Takes open request and then returns nothing so acts like a RS that went zombie.
   * No response (so proc is stuck/suspended on the Master and won't wake up.).
   * Different with HangThenRSCrashExecutor,  HangThenRSCrashExecutor will create
   * ServerCrashProcedure to handle the server crash. However, this HangThenRSRestartExecutor
   * will restart RS directly, situation for RS crashed when SCP is not enabled.
   */
  private class HangThenRSRestartExecutor extends GoodRsExecutor {
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

  private class HangOnCloseThenRSCrashExecutor extends GoodRsExecutor {
    public static final int TYPES_OF_FAILURE = 6;
    private int invocations;

    @Override
    protected CloseRegionResponse execCloseRegion(ServerName server, byte[] regionName)
        throws IOException {
      switch (this.invocations++) {
      case 0: throw new NotServingRegionException("Fake");
      case 1: throw new RegionServerAbortedException("Fake!");
      case 2: throw new RegionServerStoppedException("Fake!");
      case 3: throw new ServerNotRunningYetException("Fake!");
      case 4:
        LOG.info("Return null response from serverName=" + server + "; means STUCK...TODO timeout");
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

  private class RandRsExecutor extends NoopRsExecutor {
    private final Random rand = new Random();

    @Override
    public ExecuteProceduresResponse sendRequest(ServerName server, ExecuteProceduresRequest req)
        throws IOException {
      switch (rand.nextInt(5)) {
        case 0: throw new ServerNotRunningYetException("wait on server startup");
        case 1: throw new SocketTimeoutException("simulate socket timeout");
        case 2: throw new RemoteException("java.io.IOException", "unexpected exception");
      }
      return super.sendRequest(server, req);
    }

    @Override
    protected RegionOpeningState execOpenRegion(final ServerName server, RegionOpenInfo openReq)
        throws IOException {
      switch (rand.nextInt(6)) {
        case 0:
          LOG.info("Return OPENED response");
          sendTransitionReport(server, openReq.getRegion(), TransitionCode.OPENED);
          return OpenRegionResponse.RegionOpeningState.OPENED;
        case 1:
          LOG.info("Return transition report that OPENED/ALREADY_OPENED response");
          sendTransitionReport(server, openReq.getRegion(), TransitionCode.OPENED);
          return OpenRegionResponse.RegionOpeningState.ALREADY_OPENED;
        case 2:
          LOG.info("Return transition report that FAILED_OPEN/FAILED_OPENING response");
          sendTransitionReport(server, openReq.getRegion(), TransitionCode.FAILED_OPEN);
          return OpenRegionResponse.RegionOpeningState.FAILED_OPENING;
      }
      // The procedure on master will just hang forever because nothing comes back
      // from the RS in this case.
      LOG.info("Return null as response; means proc stuck so we send in a crash report after a few seconds...");
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
      boolean closed = rand.nextBoolean();
      if (closed) {
        RegionInfo hri = am.getRegionInfo(regionName);
        sendTransitionReport(server, ProtobufUtil.toRegionInfo(hri), TransitionCode.CLOSED);
      }
      resp.setClosed(closed);
      return resp.build();
    }
  }

  private interface MockRSExecutor {
    ExecuteProceduresResponse sendRequest(ServerName server, ExecuteProceduresRequest req)
        throws IOException;
  }

  private class MockRSProcedureDispatcher extends RSProcedureDispatcher {
    private MockRSExecutor mockRsExec;

    public MockRSProcedureDispatcher(final MasterServices master) {
      super(master);
    }

    public void setMockRsExecutor(final MockRSExecutor mockRsExec) {
      this.mockRsExec = mockRsExec;
    }

    @Override
    protected void remoteDispatch(ServerName serverName, Set<RemoteProcedure> remoteProcedures) {
      submitTask(new MockRemoteCall(serverName, remoteProcedures));
    }

    private class MockRemoteCall extends ExecuteProceduresRemoteCall {
      public MockRemoteCall(final ServerName serverName,
          final Set<RemoteProcedure> operations) {
        super(serverName, operations);
      }

      @Override
      protected ExecuteProceduresResponse sendRequest(final ServerName serverName,
          final ExecuteProceduresRequest request) throws IOException {
        return mockRsExec.sendRequest(serverName, request);
      }
    }
  }

  private void collectAssignmentManagerMetrics() {
    assignSubmittedCount = assignProcMetrics.getSubmittedCounter().getCount();
    assignFailedCount = assignProcMetrics.getFailedCounter().getCount();
    unassignSubmittedCount = unassignProcMetrics.getSubmittedCounter().getCount();
    unassignFailedCount = unassignProcMetrics.getFailedCounter().getCount();
  }
}
