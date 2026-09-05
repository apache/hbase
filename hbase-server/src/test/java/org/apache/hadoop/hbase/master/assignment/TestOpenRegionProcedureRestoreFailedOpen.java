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

import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.OpenRegionRequest.RegionOpenInfo;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.OpenRegionResponse.RegionOpeningState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;

/**
 * HBASE-30357: OpenRegionProcedure#restoreSucceedState() must not force the region state to OPEN on
 * master-failover restore when the persisted transition code is actually FAILED_OPEN.
 */
@Tag(MasterTests.TAG)
@Tag(LargeTests.TAG)
public class TestOpenRegionProcedureRestoreFailedOpen extends TestAssignmentManagerBase {

  /**
   * On the first open attempt, reports FAILED_OPEN and then immediately simulates a master restart
   * happening before the child OpenRegionProcedure gets to run its own execute() (i.e. before it
   * can persist anything to meta), by directly invoking the package-private stateLoaded() hook that
   * a real restart would trigger. Records the region state right after that simulated restore.
   * Subsequent attempts behave like a normal healthy RS so the assign procedure can converge and
   * the test does not hang.
   */
  private class FailedOpenThenRestoreRsExecutor extends GoodRsExecutor {

    private final AtomicBoolean firstAttempt = new AtomicBoolean(true);

    final AtomicReference<RegionState.State> stateAfterRestore = new AtomicReference<>();

    @Override
    protected RegionOpeningState execOpenRegion(ServerName server, RegionOpenInfo openReq)
      throws IOException {
      if (!firstAttempt.compareAndSet(true, false)) {
        return super.execOpenRegion(server, openReq);
      }
      RegionInfo hri = ProtobufUtil.toRegionInfo(openReq.getRegion());
      RegionStateNode regionNode = am.getRegionStates().getRegionStateNode(hri);
      TransitRegionStateProcedure trsp = (TransitRegionStateProcedure) regionNode.getProcedure();
      regionNode.lock();
      try {
        sendTransitionReport(server, openReq.getRegion(), TransitionCode.FAILED_OPEN,
          HConstants.NO_SEQNUM);
        trsp.stateLoaded(am, regionNode);
        stateAfterRestore.set(regionNode.getState());
      } finally {
        regionNode.unlock();
      }
      return RegionOpeningState.FAILED_OPENING;
    }
  }

  @Test
  public void testRestoreDoesNotForceOpenAfterFailedOpen() throws Exception {
    TableName tableName = TableName.valueOf(testMethodName);
    RegionInfo hri = createRegionInfo(tableName, 1);
    FailedOpenThenRestoreRsExecutor executor = new FailedOpenThenRestoreRsExecutor();
    rsDispatcher.setMockRsExecutor(executor);
    TransitRegionStateProcedure proc = createAssignProcedure(hri);
    waitOnFuture(submitProcedure(proc));

    assertNotEquals(RegionState.State.OPEN, executor.stateAfterRestore.get());
  }
}
