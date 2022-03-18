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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.SnapshotState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;

@Category({ MasterTests.class, MediumTests.class })
public class TestSnapshotProcedureMasterRestarts extends TestSnapshotProcedure {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSnapshotProcedureMasterRestarts.class);

  @Test
  public void testMasterRestarts() throws Exception {
    ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
    MasterProcedureEnv env = procExec.getEnvironment();
    SnapshotProcedure sp = new SnapshotProcedure(env, snapshotProto);
    SnapshotProcedure spySp = getDelayedOnSpecificStateSnapshotProcedure(sp,
      procExec.getEnvironment(), SnapshotState.SNAPSHOT_SNAPSHOT_ONLINE_REGIONS);

    long procId = procExec.submitProcedure(spySp);

    TEST_UTIL.waitFor(2000, () -> env.getMasterServices().getProcedures()
      .stream().map(Procedure::getProcId).collect(Collectors.toList()).contains(procId));
    TEST_UTIL.getHBaseCluster().killMaster(master.getServerName());
    TEST_UTIL.getHBaseCluster().waitForMasterToStop(master.getServerName(), 30000);
    TEST_UTIL.getHBaseCluster().startMaster();
    TEST_UTIL.getHBaseCluster().waitForActiveAndReadyMaster();

    master = TEST_UTIL.getHBaseCluster().getMaster();
    assertTrue(master.getSnapshotManager().isTakingAnySnapshot());
    assertTrue(master.getSnapshotManager().isTableTakingAnySnapshot(TABLE_NAME));

    List<SnapshotProcedure> unfinishedProcedures = master
      .getMasterProcedureExecutor().getProcedures().stream()
      .filter(p -> p instanceof SnapshotProcedure)
      .filter(p -> !p.isFinished()).map(p -> (SnapshotProcedure) p)
      .collect(Collectors.toList());
    assertEquals(unfinishedProcedures.size(), 1);
    long newProcId = unfinishedProcedures.get(0).getProcId();
    assertEquals(procId, newProcId);

    ProcedureTestingUtility.waitProcedure(master.getMasterProcedureExecutor(), newProcId);
    assertFalse(master.getSnapshotManager().isTableTakingAnySnapshot(TABLE_NAME));

    List<SnapshotProtos.SnapshotDescription> snapshots
      = master.getSnapshotManager().getCompletedSnapshots();
    assertEquals(1, snapshots.size());
    assertEquals(SNAPSHOT_NAME, snapshots.get(0).getName());
    assertEquals(TABLE_NAME, TableName.valueOf(snapshots.get(0).getTable()));
    SnapshotTestingUtils.confirmSnapshotValid(TEST_UTIL, snapshotProto, TABLE_NAME, CF);
  }
}
