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

import static org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RegionStateTransitionState.REGION_STATE_TRANSITION_CONFIRM_OPENED_VALUE;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureTestingUtility;
import org.apache.hadoop.hbase.master.region.MasterRegion;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionResponse;

/**
 * HBASE-30357: OpenRegionProcedure#restoreSucceedState() must not force the region state to OPEN on
 * master-failover restore when the persisted transition code is actually FAILED_OPEN.
 * <p/>
 * To reproduce the exact crash window without racing a genuinely reporting RS, we intercept the
 * RS's real OPENED report on the master side and rewrite it to FAILED_OPEN before it is persisted.
 * While still on the RPC handler thread (i.e. before the woken child OpenRegionProcedure gets a
 * chance to run its own execute() and persist anything to meta), we lock the RegionStateNode and
 * perform a genuine restart of the master's ProcedureExecutor/AssignmentManager, forcing a real
 * reload from the WALProcedureStore and hbase:meta - exactly the mechanism restoreSucceedState() is
 * meant to handle.
 */
@Tag(MasterTests.TAG)
@Tag(MediumTests.TAG)
public class TestOpenRegionProcedureRestoreFailedOpen {

  private static final long AWAIT_TIMEOUT_SECONDS = 30;

  private static final AtomicReference<CountDownLatch> ARRIVE = new AtomicReference<>();

  private static final AtomicReference<CountDownLatch> PROCEED = new AtomicReference<>();

  private static final class AssignmentManagerForTest extends AssignmentManager {

    public AssignmentManagerForTest(MasterServices master, MasterRegion masterRegion) {
      super(master, masterRegion);
    }

    @Override
    public ReportRegionStateTransitionResponse reportRegionStateTransition(
      ReportRegionStateTransitionRequest req) throws PleaseHoldException {
      RegionStateTransition transition = req.getTransition(0);
      RegionInfo hri = ProtobufUtil.toRegionInfo(transition.getRegionInfo(0));
      if (transition.getTransitionCode() != TransitionCode.OPENED || !hri.getTable().equals(NAME)) {
        return super.reportRegionStateTransition(req);
      }
      CountDownLatch arrive = ARRIVE.getAndSet(null);
      if (arrive == null) {
        return super.reportRegionStateTransition(req);
      }
      ReportRegionStateTransitionRequest failedOpenReq = req.toBuilder()
        .setTransition(0, transition.toBuilder().setTransitionCode(TransitionCode.FAILED_OPEN)
          .setOpenSeqNum(HConstants.NO_SEQNUM).build())
        .build();
      RegionStateNode regionNode = getRegionStates().getRegionStateNode(hri);
      // AssignmentManager#updateRegionTransition() (called from super.reportRegionStateTransition
      // below) also locks this same RegionStateNode; that only works here because the lock is
      // reentrant for the same thread (see RegionStateNodeLock#lock0).
      regionNode.lock();
      try {
        // persists REPORT_SUCCEED/FAILED_OPEN to the real WALProcedureStore and wakes the child
        // OpenRegionProcedure, but since we still hold the RegionStateNode lock here (reentrant,
        // same thread), the woken child can not resume and complete its own meta update - this is
        // exactly the window a real master crash would leave us in.
        ReportRegionStateTransitionResponse resp = super.reportRegionStateTransition(failedOpenReq);
        arrive.countDown();
        CountDownLatch proceed = PROCEED.get();
        if (!proceed.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
          throw new RuntimeException("Timed out waiting for PROCEED");
        }
        return resp;
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } finally {
        regionNode.unlock();
      }
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

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static final TableName NAME =
    TableName.valueOf("TestOpenRegionProcedureRestoreFailedOpen");

  private static final byte[] CF = Bytes.toBytes("cf");

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    UTIL.getConfiguration().setClass(HConstants.MASTER_IMPL, HMasterForTest.class, HMaster.class);
    UTIL.startMiniCluster(1);
    UTIL.createTable(NAME, CF);
    UTIL.waitTableAvailable(NAME);
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testRestoreDoesNotForceOpenAfterFailedOpen() throws Exception {
    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
    AssignmentManager am = master.getAssignmentManager();
    RegionInfo region = UTIL.getAdmin().getRegions(NAME).get(0);
    RegionStateNode regionNode = am.getRegionStates().getRegionStateNode(region);

    CountDownLatch arrive = new CountDownLatch(1);
    CountDownLatch proceed = new CountDownLatch(1);
    ARRIVE.set(arrive);
    PROCEED.set(proceed);
    Future<byte[]> future = am.moveAsync(
      new RegionPlan(region, regionNode.getRegionLocation(), regionNode.getRegionLocation()));
    TransitRegionStateProcedure proc =
      procExec.getProcedures().stream().filter(p -> p instanceof TransitRegionStateProcedure)
        .filter(p -> !p.isFinished()).map(p -> (TransitRegionStateProcedure) p).findAny().get();
    // wait until we are suspended waiting on the RS to report the open, then let it report; the
    // AssignmentManagerForTest above rewrites that report to FAILED_OPEN and blocks the RPC handler
    // thread, still holding the RegionStateNode lock, until we tell it to proceed
    UTIL.waitFor(30000,
      () -> proc.getCurrentStateId() == REGION_STATE_TRANSITION_CONFIRM_OPENED_VALUE);
    assertTrue(arrive.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));

    MasterProcedureTestingUtility.restartMasterProcedureExecutor(procExec);
    RegionStateNode reloaded = am.getRegionStates().getRegionStateNode(region);
    assertNotEquals(RegionState.State.OPEN, reloaded.getState());

    proceed.countDown();
    future.get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }
}
