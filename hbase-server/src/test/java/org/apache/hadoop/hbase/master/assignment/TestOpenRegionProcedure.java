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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;

/**
 * Coverage for {@link OpenRegionProcedure#restoreSucceedState}, which is invoked while loading
 * hbase:meta on master failover to re-apply a transition that was reported by the RegionServer but
 * not yet persisted before the old master went away.
 * <p>
 * Regression test for HBASE-29364: a recorded {@code FAILED_OPEN} report must NOT be restored as
 * {@code OPEN} (doing so could leave the region marked open on a now-dead server); it must be left
 * in transition for the parent {@link TransitRegionStateProcedure} to retry. A recorded
 * {@code OPENED} must still be restored as {@code OPEN}.
 */
@Tag(MasterTests.TAG)
@Tag(MediumTests.TAG)
public class TestOpenRegionProcedure extends TestAssignmentManagerBase {

  @Test
  public void testRestoreFailedOpenDoesNotMarkRegionOpen() throws Exception {
    RegionStateNode regionNode = newOpeningRegion("failed-open");
    OpenRegionProcedure orp = newReportedOpenProc(regionNode, TransitionCode.FAILED_OPEN);

    orp.restoreSucceedState(am, regionNode, -1);

    // FAILED_OPEN must not become OPEN; the region stays in transition so it can be retried.
    assertEquals(State.OPENING, regionNode.getState());
  }

  @Test
  public void testRestoreOpenedMarksRegionOpen() throws Exception {
    RegionStateNode regionNode = newOpeningRegion("opened");
    OpenRegionProcedure orp = newReportedOpenProc(regionNode, TransitionCode.OPENED);

    orp.restoreSucceedState(am, regionNode, 100);

    assertEquals(State.OPEN, regionNode.getState());
  }

  private RegionStateNode newOpeningRegion(String suffix) {
    ServerName target = ServerName.valueOf("rs-" + suffix + ".example.org", 16020, 1);
    am.getRegionStates().createServer(target);
    RegionInfo hri = createRegionInfo(TableName.valueOf(testMethodName), 1);
    RegionStateNode regionNode = am.getRegionStates().getOrCreateRegionStateNode(hri);
    regionNode.setState(State.OPENING);
    regionNode.setRegionLocation(target);
    return regionNode;
  }

  private OpenRegionProcedure newReportedOpenProc(RegionStateNode regionNode, TransitionCode code) {
    MasterProcedureEnv env = master.getMasterProcedureExecutor().getEnvironment();
    TransitRegionStateProcedure parent =
      TransitRegionStateProcedure.assign(env, regionNode.getRegionInfo(), regionNode.getRegionLocation());
    OpenRegionProcedure orp =
      new OpenRegionProcedure(parent, regionNode.getRegionInfo(), regionNode.getRegionLocation());
    // Simulate the report the RegionServer sent before the master failed over.
    orp.transitionCode = code;
    return orp;
  }
}
