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
package org.apache.hadoop.hbase.master.procedure;

import static org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.SnapshotState.SNAPSHOT_CONSOLIDATE_SNAPSHOT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.assignment.MergeTableRegionsProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;

@Tag(MasterTests.TAG)
@Tag(MediumTests.TAG)
public class TestSnapshotProcedureRIT extends TestSnapshotProcedure {

  public static final class DelayConsolidateSnapshotProcedure extends SnapshotProcedure {
    private static volatile boolean consolidating = false;

    public DelayConsolidateSnapshotProcedure() {
    }

    public DelayConsolidateSnapshotProcedure(final MasterProcedureEnv env,
      final SnapshotProtos.SnapshotDescription snapshot) {
      super(env, snapshot);
    }

    @Override
    protected Flow executeFromState(MasterProcedureEnv env,
      MasterProcedureProtos.SnapshotState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
      if (state == SNAPSHOT_CONSOLIDATE_SNAPSHOT) {
        consolidating = true;
        TimeUnit.SECONDS.sleep(60);
      }
      return super.executeFromState(env, state);
    }
  }

  @Test
  public void testTableInMergeWhileTakingSnapshot() throws Exception {
    ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
    List<RegionInfo> regions = master.getAssignmentManager().getTableRegions(TABLE_NAME, true)
      .stream().sorted(RegionInfo.COMPARATOR).collect(Collectors.toList());
    MergeTableRegionsProcedure mergeProc = new MergeTableRegionsProcedure(procExec.getEnvironment(),
      new RegionInfo[] { regions.get(0), regions.get(1) }, false);
    long mergeProcId = procExec.submitProcedure(mergeProc);
    // wait until merge region procedure running
    TEST_UTIL.waitFor(10000,
      () -> procExec.getProcedure(mergeProcId).getState() == ProcedureState.RUNNABLE);
    SnapshotProcedure sp = new SnapshotProcedure(procExec.getEnvironment(), snapshotProto);
    long snapshotProcId = procExec.submitProcedure(sp);
    TEST_UTIL.waitFor(2000, 1000, () -> procExec.getProcedure(snapshotProcId) != null
      && procExec.getProcedure(snapshotProcId).getState() == ProcedureState.WAITING_TIMEOUT);
    ProcedureTestingUtility.waitProcedure(procExec, snapshotProcId);
    SnapshotTestingUtils.confirmSnapshotValid(TEST_UTIL, snapshotProto, TABLE_NAME, CF);
  }

  @Test
  public void testRegionMoveWhileConsolidatingSnapshot() throws Exception {
    ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
    SnapshotProcedure sp =
      new DelayConsolidateSnapshotProcedure(procExec.getEnvironment(), snapshotProto);
    long snapshotProcId = procExec.submitProcedure(sp);
    TEST_UTIL.waitFor(60000, () -> DelayConsolidateSnapshotProcedure.consolidating);

    RegionInfo region = master.getAssignmentManager().getTableRegions(TABLE_NAME, true).get(0);
    ServerName source =
      master.getAssignmentManager().getRegionStates().getRegionServerOfRegion(region);
    ServerName dest = master.getServerManager().getOnlineServersList().stream()
      .filter(sn -> !sn.equals(source)).findFirst().get();
    master.getAssignmentManager().moveAsync(new RegionPlan(region, source, dest)).get(45,
      TimeUnit.SECONDS);

    assertFalse(procExec.isFinished(snapshotProcId));
    assertEquals(dest,
      master.getAssignmentManager().getRegionStates().getRegionServerOfRegion(region));
    ProcedureTestingUtility.waitProcedure(procExec, snapshotProcId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, snapshotProcId);
    SnapshotTestingUtils.confirmSnapshotValid(TEST_UTIL, snapshotProto, TABLE_NAME, CF);
  }
}
