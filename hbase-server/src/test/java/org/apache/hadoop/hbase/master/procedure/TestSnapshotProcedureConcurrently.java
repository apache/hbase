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

import static org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.EnableTableState.ENABLE_TABLE_MARK_REGIONS_ONLINE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.SnapshotType;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;

@Category({ MasterTests.class, MediumTests.class })
public class TestSnapshotProcedureConcurrently extends TestSnapshotProcedure {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSnapshotProcedureConcurrently.class);

  @Test
  public void testRunningTwoSnapshotProcedureOnSameTable() throws Exception {
    String newSnapshotName = SNAPSHOT_NAME + "_2";
    SnapshotProtos.SnapshotDescription snapshotProto2 =
      SnapshotProtos.SnapshotDescription.newBuilder(snapshotProto).setName(newSnapshotName).build();

    ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
    MasterProcedureEnv env = procExec.getEnvironment();

    SnapshotProcedure sp1 = new SnapshotProcedure(env, snapshotProto);
    SnapshotProcedure sp2 = new SnapshotProcedure(env, snapshotProto2);
    SnapshotProcedure spySp1 =
      getDelayedOnSpecificStateSnapshotProcedure(sp1, procExec.getEnvironment(),
        MasterProcedureProtos.SnapshotState.SNAPSHOT_SNAPSHOT_ONLINE_REGIONS);
    SnapshotProcedure spySp2 =
      getDelayedOnSpecificStateSnapshotProcedure(sp2, procExec.getEnvironment(),
        MasterProcedureProtos.SnapshotState.SNAPSHOT_SNAPSHOT_ONLINE_REGIONS);

    long procId1 = procExec.submitProcedure(spySp1);
    long procId2 = procExec.submitProcedure(spySp2);
    TEST_UTIL.waitFor(2000,
      () -> env.getMasterServices().getProcedures().stream().map(Procedure::getProcId)
        .collect(Collectors.toList()).containsAll(Arrays.asList(procId1, procId2)));

    assertFalse(procExec.isFinished(procId1));
    assertFalse(procExec.isFinished(procId2));

    ProcedureTestingUtility.waitProcedure(master.getMasterProcedureExecutor(), procId1);
    ProcedureTestingUtility.waitProcedure(master.getMasterProcedureExecutor(), procId2);

    List<SnapshotProtos.SnapshotDescription> snapshots =
      master.getSnapshotManager().getCompletedSnapshots();
    assertEquals(2, snapshots.size());
    snapshots.sort(Comparator.comparing(SnapshotProtos.SnapshotDescription::getName));
    assertEquals(SNAPSHOT_NAME, snapshots.get(0).getName());
    assertEquals(newSnapshotName, snapshots.get(1).getName());
    SnapshotTestingUtils.confirmSnapshotValid(TEST_UTIL, snapshotProto, TABLE_NAME, CF);
    SnapshotTestingUtils.confirmSnapshotValid(TEST_UTIL, snapshotProto2, TABLE_NAME, CF);
  }

  @Test
  public void testTakeZkCoordinatedSnapshotAndProcedureCoordinatedSnapshotBoth() throws Exception {
    String newSnapshotName = SNAPSHOT_NAME + "_2";
    Thread first = new Thread("procedure-snapshot") {
      @Override
      public void run() {
        try {
          TEST_UTIL.getAdmin().snapshot(snapshot);
        } catch (IOException e) {
          LOG.error("procedure snapshot failed", e);
          fail("procedure snapshot failed");
        }
      }
    };
    first.start();
    Thread.sleep(1000);

    SnapshotManager sm = master.getSnapshotManager();
    TEST_UTIL.waitFor(2000, 50,
      () -> !sm.isTakingSnapshot(TABLE_NAME) && sm.isTableTakingAnySnapshot(TABLE_NAME));

    TEST_UTIL.getConfiguration().setBoolean("hbase.snapshot.zk.coordinated", true);
    SnapshotDescription snapshotOnSameTable =
      new SnapshotDescription(newSnapshotName, TABLE_NAME, SnapshotType.SKIPFLUSH);
    SnapshotProtos.SnapshotDescription snapshotOnSameTableProto =
      ProtobufUtil.createHBaseProtosSnapshotDesc(snapshotOnSameTable);
    Thread second = new Thread("zk-snapshot") {
      @Override
      public void run() {
        try {
          master.getSnapshotManager().takeSnapshot(snapshotOnSameTableProto);
        } catch (IOException e) {
          LOG.error("zk snapshot failed", e);
          fail("zk snapshot failed");
        }
      }
    };
    second.start();

    TEST_UTIL.waitFor(2000, () -> sm.isTakingSnapshot(TABLE_NAME));
    TEST_UTIL.waitFor(60000,
      () -> sm.isSnapshotDone(snapshotOnSameTableProto) && !sm.isTakingAnySnapshot());
    SnapshotTestingUtils.confirmSnapshotValid(TEST_UTIL, snapshotProto, TABLE_NAME, CF);
    SnapshotTestingUtils.confirmSnapshotValid(TEST_UTIL, snapshotOnSameTableProto, TABLE_NAME, CF);
  }

  @Test
  public void testItFailsIfTableIsNotDisabledOrEnabled() throws Exception {
    ProcedureExecutor<MasterProcedureEnv> executor = master.getMasterProcedureExecutor();
    MasterProcedureEnv env = executor.getEnvironment();
    TEST_UTIL.getAdmin().disableTable(TABLE_NAME);

    TestEnableTableProcedure enableTable = new TestEnableTableProcedure(
      master.getMasterProcedureExecutor().getEnvironment(), TABLE_NAME);
    long enableProcId = executor.submitProcedure(enableTable);
    TEST_UTIL.waitFor(60000, () -> {
      Procedure<MasterProcedureEnv> proc = executor.getProcedure(enableProcId);
      if (proc == null) {
        return false;
      }
      return ((TestEnableTableProcedure) proc).getProcedureState()
          == ENABLE_TABLE_MARK_REGIONS_ONLINE;
    });

    // Using a delayed spy ensures we hit the problem state while the table enable procedure
    // is waiting to run
    SnapshotProcedure snapshotProc = new SnapshotProcedure(env, snapshotProto);
    long snapshotProcId = executor.submitProcedure(snapshotProc);
    TEST_UTIL.waitTableEnabled(TABLE_NAME);
    // Wait for procedure to run and finish
    TEST_UTIL.waitFor(60000, () -> executor.getProcedure(snapshotProcId) != null);
    TEST_UTIL.waitFor(60000, () -> executor.getProcedure(snapshotProcId) == null);

    SnapshotTestingUtils.confirmSnapshotValid(TEST_UTIL, snapshotProto, TABLE_NAME, CF);
  }

  // Needs to be publicly accessible for Procedure validation
  public static class TestEnableTableProcedure extends EnableTableProcedure {
    // Necessary for Procedure validation
    public TestEnableTableProcedure() {
    }

    public TestEnableTableProcedure(MasterProcedureEnv env, TableName tableName) {
      super(env, tableName);
    }

    public MasterProcedureProtos.EnableTableState getProcedureState() {
      return getState(stateCount);
    }

    @Override
    protected Flow executeFromState(MasterProcedureEnv env,
      MasterProcedureProtos.EnableTableState state) throws InterruptedException {
      if (state == ENABLE_TABLE_MARK_REGIONS_ONLINE) {
        Thread.sleep(10000);
      }

      return super.executeFromState(env, state);
    }
  }
}
