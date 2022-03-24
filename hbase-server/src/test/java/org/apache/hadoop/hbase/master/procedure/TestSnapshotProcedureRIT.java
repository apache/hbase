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

package org.apache.hadoop.hbase.master.procedure;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.assignment.MergeTableRegionsProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;

@Category({ MasterTests.class, MediumTests.class })
public class TestSnapshotProcedureRIT extends TestSnapshotProcedure {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSnapshotProcedureRIT.class);

  @Test
  public void testTableInMergeWhileTakingSnapshot() throws Exception {
    ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
    List<RegionInfo> regions = master.getAssignmentManager().getTableRegions(TABLE_NAME, true)
      .stream().sorted(RegionInfo.COMPARATOR).collect(Collectors.toList());
    MergeTableRegionsProcedure mergeProc = new MergeTableRegionsProcedure(
      procExec.getEnvironment(), new RegionInfo[] {regions.get(0), regions.get(1)}, false);
    long mergeProcId = procExec.submitProcedure(mergeProc);
    // wait until merge region procedure running
    TEST_UTIL.waitFor(10000, () ->
      procExec.getProcedure(mergeProcId).getState() == ProcedureState.RUNNABLE);
    SnapshotProcedure sp = new SnapshotProcedure(procExec.getEnvironment(), snapshotProto);
    long snapshotProcId = procExec.submitProcedure(sp);
    TEST_UTIL.waitFor(2000, 1000, () -> procExec.getProcedure(snapshotProcId) != null &&
      procExec.getProcedure(snapshotProcId).getState() == ProcedureState.WAITING_TIMEOUT);
    ProcedureTestingUtility.waitProcedure(procExec, snapshotProcId);
    SnapshotTestingUtils.confirmSnapshotValid(TEST_UTIL, snapshotProto, TABLE_NAME, CF);
  }
}
