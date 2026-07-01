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
package org.apache.hadoop.hbase.master.assignment;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BalanceRequest;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureConstants;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureTestingUtility;
import org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure;
import org.apache.hadoop.hbase.master.region.MasterRegion;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionResponse;

/**
 * SCP does not support rollback actually, here we just want to simulate that when there is a code
 * bug, SCP and its sub procedures will not hang there forever, and it will not mess up the
 * procedure store.
 */
@Tag(MasterTests.TAG)
@Tag(LargeTests.TAG)
public class TestRollbackSCP {

  private static final Logger LOG = LoggerFactory.getLogger(TestRollbackSCP.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static final TableName TABLE_NAME = TableName.valueOf("test");

  private static final byte[] FAMILY = Bytes.toBytes("family");

  private static final AtomicBoolean INJECTED = new AtomicBoolean(false);

  // HBASE-29555: guards the in-place procedure-executor reload against region servers concurrently
  // reporting region state transitions. See restartMasterProcedureExecutorLikeFailover(). Fair so
  // that, while the reload is waiting for / holding the write lock, new reports are rejected rather
  // than barging in. A region state report takes the read lock; the reload takes the write lock.
  private static final ReentrantReadWriteLock RELOAD_LOCK = new ReentrantReadWriteLock(true);

  private static final class AssignmentManagerForTest extends AssignmentManager {

    public AssignmentManagerForTest(MasterServices master, MasterRegion masterRegion) {
      super(master, masterRegion);
    }

    @Override
    public ReportRegionStateTransitionResponse reportRegionStateTransition(
      ReportRegionStateTransitionRequest req) throws PleaseHoldException {
      // HBASE-29555 (Gap #2): in production a starting/recovering master rejects region state
      // reports (via HMaster.checkServiceStarted) until it has finished initializing, which happens
      // only after the procedure executor has loaded. The test instead reloads the executor in place
      // under a live master, so a report can wake a procedure and re-populate the scheduler in the
      // window between clearing it and ProcedureExecutor.load() asserting it is empty. Mirror
      // production: while the reload holds the write lock, reject reports with PleaseHoldException
      // (the region server retries). tryLock (non-blocking) is used so a report never blocks the
      // reload; acquiring the write lock in the reload still waits for any already in-flight report.
      boolean locked = false;
      try {
        locked = RELOAD_LOCK.readLock().tryLock(0, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      if (!locked) {
        throw new PleaseHoldException("master procedure executor is reloading");
      }
      try {
        return super.reportRegionStateTransition(req);
      } finally {
        RELOAD_LOCK.readLock().unlock();
      }
    }

    @Override
    CompletableFuture<Void> persistToMeta(RegionStateNode regionNode) {
      TransitRegionStateProcedure proc = regionNode.getProcedure();
      if (!regionNode.getRegionInfo().isMetaRegion() && proc.hasParent()) {
        Procedure<?> p =
          getMaster().getMasterProcedureExecutor().getProcedure(proc.getRootProcId());
        // fail the procedure if it is a sub procedure for SCP
        if (p instanceof ServerCrashProcedure) {
          if (INJECTED.compareAndSet(false, true)) {
            ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdateInRollback(
              getMaster().getMasterProcedureExecutor(), true);
          }
          return FutureUtils.failedFuture(new RuntimeException("inject code bug"));
        }
      }
      return super.persistToMeta(regionNode);
    }
  }

  public static final class HMasterForTest extends HMaster {

    public HMasterForTest(Configuration conf) throws IOException {
      super(conf);
    }

    @Override
    protected AssignmentManager createAssignmentManager(MasterServices master,
      MasterRegion masterRegion) {
      return new AssignmentManagerForTest(master, masterRegion);
    }
  }

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    UTIL.getConfiguration().setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, 1);
    UTIL.startMiniCluster(StartTestingClusterOption.builder().numDataNodes(3).numRegionServers(3)
      .masterClass(HMasterForTest.class).build());
    UTIL.createMultiRegionTable(TABLE_NAME, FAMILY);
    UTIL.waitTableAvailable(TABLE_NAME);
    UTIL.getAdmin().balance(BalanceRequest.newBuilder().setIgnoreRegionsInTransition(true).build());
    UTIL.waitUntilNoRegionsInTransition();
    UTIL.getAdmin().balancerSwitch(false, true);
  }

  @AfterAll
  public static void tearDownAfterClass() throws IOException {
    UTIL.shutdownMiniCluster();
  }

  @BeforeEach
  public void setUp() throws IOException {
    UTIL.ensureSomeNonStoppedRegionServersAvailable(2);
  }

  private ServerCrashProcedure getSCPForServer(ServerName serverName) throws IOException {
    return UTIL.getMiniHBaseCluster().getMaster().getProcedures().stream()
      .filter(p -> p instanceof ServerCrashProcedure).map(p -> (ServerCrashProcedure) p)
      .filter(p -> p.getServerName().equals(serverName)).findFirst().orElse(null);
  }

  private Matcher<Procedure<MasterProcedureEnv>> subProcOf(Procedure<MasterProcedureEnv> proc) {
    return new BaseMatcher<Procedure<MasterProcedureEnv>>() {

      @Override
      public boolean matches(Object item) {
        if (!(item instanceof Procedure)) {
          return false;
        }
        Procedure<?> p = (Procedure<?>) item;
        return p.hasParent() && p.getRootProcId() == proc.getProcId();
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("sub procedure of(").appendValue(proc).appendText(")");
      }
    };
  }

  /**
   * Wait for the (already shutdown) asyncTaskExecutor of the given executor to fully terminate.
   * HBASE-29555 (Gap #1): callbacks that wake a procedure after an async operation (e.g.
   * AssignmentManager#persistToMeta run on the executor's asyncTaskExecutor, not on a
   * PEWorker. In production a master restart is a fresh process, so no such callback can survive
   * into the reloaded executor. Here we reuse the same executor in-place, so a pending callback can
   * call scheduler.addFront during the reload. ProcedureExecutor shuts the
   * asyncTaskExecutor down; waiting for it to terminate guarantees any pending callback has run
   * before we clear the scheduler, reproducing the "no async work survives the restart" guarantee.
   */
  private static void awaitAsyncTaskExecutorTermination(ProcedureExecutor<?> procExec) {
    ExecutorService asyncTaskExecutor = procExec.getAsyncTaskExecutor();
    if (asyncTaskExecutor == null) {
      return;
    }
    long deadline = System.currentTimeMillis() + 60000;
    try {
      while (!asyncTaskExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
        if (System.currentTimeMillis() > deadline) {
          LOG.warn("asyncTaskExecutor did not terminate in time while reloading the executor");
          return;
        }
      }
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while waiting for asyncTaskExecutor to terminate while reloading", e);
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Reload the master procedure executor in place (delegating the actual stop/clear/reload to
   * MasterProcedureTestingUtility.restartMasterProcedureExecutor), but closing the two gaps
   * that make the in-place reload diverge from a real master failover (HBASE-29555), so that the
   * test reproduces production rather than relying on a retry:
   * Gap #1 (no async work survives a real failover): stop the executor and wait for its
   * asyncTaskExecutor to fully terminate before the reload, so a pending meta-update wake-up cannot
   * re-populate the scheduler during load(). A real failover gets this for free because the
   * old process (and its asyncTaskExecutor) is gone.
   * Gap #2 (no region report during load): hold the reload write lock for the whole
   * reload so that reportRegionStateTransition is rejected (and any in-flight report is
   * waited for) -- exactly like a real master rejects reports via checkServiceStarted until
   * it finishes initializing, which is after load()
   */
  private void restartMasterProcedureExecutorLikeFailover(
    ProcedureExecutor<MasterProcedureEnv> procExec) throws Exception {
    // Gap #2: block region reports for the duration of the reload. Acquiring the write lock waits for
    // any in-flight report to finish first.
    RELOAD_LOCK.writeLock().lock();
    try {
      // Gap #1: stop the executor (which shuts down the asyncTaskExecutor) and wait for the
      // asyncTaskExecutor to fully terminate, so any pending wake-up has run before the reload
      // clears the scheduler. The subsequent restartMasterProcedureExecutor will clear/reload, and
      // its stop()/join() are safe to call again.
      procExec.stop();
      awaitAsyncTaskExecutorTermination(procExec);
      MasterProcedureTestingUtility.restartMasterProcedureExecutor(procExec);
    } finally {
      RELOAD_LOCK.writeLock().unlock();
    }
  }

  @Test
  public void testFailAndRollback() throws Exception {
    HRegionServer rsWithMeta = UTIL.getRSForFirstRegionInTable(TableName.META_TABLE_NAME);
    UTIL.getMiniHBaseCluster().killRegionServer(rsWithMeta.getServerName());
    UTIL.waitFor(15000, () -> getSCPForServer(rsWithMeta.getServerName()) != null);
    ServerCrashProcedure scp = getSCPForServer(rsWithMeta.getServerName());
    ProcedureExecutor<MasterProcedureEnv> procExec =
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();
    // wait for the procedure to stop, as we inject a code bug and also set kill before store update
    UTIL.waitFor(30000, () -> !procExec.isRunning());
    // make sure that finally we could successfully rollback the procedure
    while (scp.getState() != ProcedureState.FAILED || !procExec.isRunning()) {
      restartMasterProcedureExecutorLikeFailover(procExec);
      ProcedureTestingUtility.waitProcedure(procExec, scp);
    }
    assertEquals(scp.getState(), ProcedureState.FAILED);
    assertThat(scp.getException().getMessage(), containsString("inject code bug"));
    // make sure all sub procedures are cleaned up
    assertThat(procExec.getProcedures(), everyItem(not(subProcOf(scp))));
  }
}
