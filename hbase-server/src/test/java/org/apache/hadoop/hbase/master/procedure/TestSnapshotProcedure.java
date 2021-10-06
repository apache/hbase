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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.SnapshotType;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.answers.AnswersWithDelay;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.SnapshotState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;

@Category({ MasterTests.class, MediumTests.class })
public class TestSnapshotProcedure {
  private static final Logger LOG = LoggerFactory.getLogger(TestSnapshotProcedure.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSnapshotProcedure.class);

  private static HBaseTestingUtil TEST_UTIL;
  private HMaster master;
  private TableName TABLE_NAME;
  private byte[] CF;
  private String SNAPSHOT_NAME;
  private SnapshotDescription snapshot;
  private SnapshotProtos.SnapshotDescription snapshotProto;

  @Before
  public void setup() throws Exception {
    TEST_UTIL = new HBaseTestingUtil();
    Configuration config = TEST_UTIL.getConfiguration();
    config.setBoolean("hbase.snapshot.zk.coordinated", false);
    // using SnapshotVerifyProcedure to verify snapshot
    config.setInt("hbase.snapshot.remote.verify.threshold", 1);
    config.setInt(HConstants.MASTER_INFO_PORT, 8080);
    // delay dispatch so that we can do something, for example kill a target server
    config.setInt(RemoteProcedureDispatcher.DISPATCH_DELAY_CONF_KEY, 10000);
    config.setInt(RemoteProcedureDispatcher.DISPATCH_MAX_QUEUE_SIZE_CONF_KEY, 128);
    TEST_UTIL.startMiniCluster(3);
    master = TEST_UTIL.getHBaseCluster().getMaster();
    TABLE_NAME = TableName.valueOf(Bytes.toBytes("SPTestTable"));
    CF = Bytes.toBytes("cf");
    SNAPSHOT_NAME = "SnapshotProcedureTest";
    snapshot = new SnapshotDescription(SNAPSHOT_NAME, TABLE_NAME, SnapshotType.FLUSH);
    snapshotProto = ProtobufUtil.createHBaseProtosSnapshotDesc(snapshot);
    snapshotProto = SnapshotDescriptionUtils.validate(snapshotProto, master.getConfiguration());
    final byte[][] splitKeys = new RegionSplitter.HexStringSplit().split(10);
    Table table = TEST_UTIL.createTable(TABLE_NAME, CF, splitKeys);
    TEST_UTIL.loadTable(table, CF, false);
  }

  @Test
  public void testSimpleSnapshot() throws Exception {
    TEST_UTIL.getAdmin().snapshot(snapshot);
    SnapshotTestingUtils.assertOneSnapshotThatMatches(TEST_UTIL.getAdmin(), snapshotProto);
    SnapshotTestingUtils.confirmSnapshotValid(TEST_UTIL, snapshotProto, TABLE_NAME, CF);
  }

  @Test
  public void testMasterRestart() throws Exception {
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
    assertTrue(master.getSnapshotManager().isTakingSnapshot(TABLE_NAME, true));

    List<SnapshotProcedure> unfinishedProcedures = master
      .getMasterProcedureExecutor().getProcedures().stream()
      .filter(p -> p instanceof SnapshotProcedure)
      .filter(p -> !p.isFinished()).map(p -> (SnapshotProcedure) p)
      .collect(Collectors.toList());
    assertEquals(unfinishedProcedures.size(), 1);
    long newProcId = unfinishedProcedures.get(0).getProcId();
    assertEquals(procId, newProcId);

    ProcedureTestingUtility.waitProcedure(master.getMasterProcedureExecutor(), newProcId);
    assertFalse(master.getSnapshotManager().isTakingSnapshot(TABLE_NAME, true));

    List<SnapshotProtos.SnapshotDescription> snapshots
      = master.getSnapshotManager().getCompletedSnapshots();
    assertEquals(1, snapshots.size());
    assertEquals(SNAPSHOT_NAME, snapshots.get(0).getName());
    assertEquals(TABLE_NAME, TableName.valueOf(snapshots.get(0).getTable()));
    SnapshotTestingUtils.confirmSnapshotValid(TEST_UTIL, snapshotProto, TABLE_NAME, CF);
  }

  @Test
  public void testRegionServerCrashWhileTakingSnapshot() throws Exception {
    ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
    MasterProcedureEnv env = procExec.getEnvironment();
    SnapshotProcedure sp = new SnapshotProcedure(env, snapshotProto);
    long procId = procExec.submitProcedure(sp);

    SnapshotRegionProcedure snp = waitProcedureRunnableAndGetFirst(
      SnapshotRegionProcedure.class, 60000);
    ServerName targetServer = env.getAssignmentManager().getRegionStates()
      .getRegionStateNode(snp.getRegion()).getRegionLocation();
    TEST_UTIL.getHBaseCluster().killRegionServer(targetServer);

    TEST_UTIL.waitFor(60000, () -> snp.inRetrying());
    ProcedureTestingUtility.waitProcedure(procExec, procId);

    SnapshotTestingUtils.assertOneSnapshotThatMatches(TEST_UTIL.getAdmin(), snapshotProto);
    SnapshotTestingUtils.confirmSnapshotValid(TEST_UTIL, snapshotProto, TABLE_NAME, CF);
  }

  @Test
  public void testRegionServerCrashWhileVerifyingSnapshot() throws Exception {
    ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
    MasterProcedureEnv env = procExec.getEnvironment();
    SnapshotProcedure sp = new SnapshotProcedure(env, snapshotProto);
    long procId = procExec.submitProcedure(sp);

    SnapshotVerifyProcedure svp = waitProcedureRunnableAndGetFirst(
      SnapshotVerifyProcedure.class, 60000);
    ServerName previousTargetServer = svp.getServerName();

    HRegionServer rs = TEST_UTIL.getHBaseCluster().getRegionServer(previousTargetServer);
    TEST_UTIL.getHBaseCluster().killRegionServer(rs.getServerName());
    TEST_UTIL.waitFor(60000, () -> svp.getServerName() != previousTargetServer);
    ProcedureTestingUtility.waitProcedure(procExec, procId);

    SnapshotTestingUtils.assertOneSnapshotThatMatches(TEST_UTIL.getAdmin(), snapshotProto);
    SnapshotTestingUtils.confirmSnapshotValid(TEST_UTIL, snapshotProto, TABLE_NAME, CF);
  }

  public <T extends Procedure<MasterProcedureEnv>> T waitProcedureRunnableAndGetFirst(
      Class<T> clazz, long timeout) throws IOException {
    TEST_UTIL.waitFor(timeout, () -> master.getProcedures().stream()
      .anyMatch(clazz::isInstance));
    Optional<T> procOpt =  master.getMasterProcedureExecutor().getProcedures().stream()
      .filter(clazz::isInstance).map(clazz::cast).findFirst();
    assertTrue(procOpt.isPresent());
    return procOpt.get();
  }

  @Test(expected = org.apache.hadoop.hbase.snapshot.SnapshotCreationException.class)
  public void testClientTakingTwoSnapshotOnSameTable() throws Exception {
    Thread first = new Thread("first-client") {
      @Override
      public void run() {
        try {
          TEST_UTIL.getAdmin().snapshot(snapshot);
        } catch (IOException e) {
          LOG.error("first client failed taking snapshot", e);
          fail("first client failed taking snapshot");
        }
      }
    };
    first.start();
    Thread.sleep(1000);
    // we don't allow different snapshot with same name
    SnapshotDescription snapshotWithSameName =
      new SnapshotDescription(SNAPSHOT_NAME, TABLE_NAME, SnapshotType.SKIPFLUSH);
    TEST_UTIL.getAdmin().snapshot(snapshotWithSameName);
  }

  @Test(expected = org.apache.hadoop.hbase.snapshot.SnapshotCreationException.class)
  public void testClientTakeSameSnapshotTwice() throws IOException, InterruptedException {
    Thread first = new Thread("first-client") {
      @Override
      public void run() {
        try {
          TEST_UTIL.getAdmin().snapshot(snapshot);
        } catch (IOException e) {
          LOG.error("first client failed taking snapshot", e);
          fail("first client failed taking snapshot");
        }
      }
    };
    first.start();
    Thread.sleep(1000);
    TEST_UTIL.getAdmin().snapshot(snapshot);
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
    TEST_UTIL.waitFor(2000,50, () -> !sm.isTakingSnapshot(TABLE_NAME)
      && sm.isTakingSnapshot(TABLE_NAME, true));

    TEST_UTIL.getConfiguration().setBoolean("hbase.snapshot.zk.coordinated", true);
    SnapshotDescription snapshotOnSameTable =
      new SnapshotDescription(newSnapshotName, TABLE_NAME, SnapshotType.SKIPFLUSH);
    SnapshotProtos.SnapshotDescription snapshotOnSameTableProto = ProtobufUtil
      .createHBaseProtosSnapshotDesc(snapshotOnSameTable);
    Thread second = new Thread("zk-snapshot") {
      @Override
      public void run() {
        try {
          TEST_UTIL.getAdmin().snapshot(snapshotOnSameTable);
        } catch (IOException e) {
          LOG.error("zk snapshot failed", e);
          fail("zk snapshot failed");
        }
      }
    };
    second.start();

    TEST_UTIL.waitFor(2000, () -> sm.isTakingSnapshot(TABLE_NAME));
    TEST_UTIL.waitFor(60000, () -> sm.isSnapshotDone(snapshotOnSameTableProto)
      && !sm.isTakingAnySnapshot());
    SnapshotTestingUtils.confirmSnapshotValid(TEST_UTIL, snapshotProto, TABLE_NAME, CF);
    SnapshotTestingUtils.confirmSnapshotValid(TEST_UTIL, snapshotOnSameTableProto, TABLE_NAME, CF);
  }

  @Test
  public void testRunningTowSnapshotProcedureOnSameTable() throws Exception {
    String newSnapshotName = SNAPSHOT_NAME + "_2";
    SnapshotProtos.SnapshotDescription snapshotProto2 = SnapshotProtos.SnapshotDescription
      .newBuilder(snapshotProto).setName(newSnapshotName).build();

    ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
    MasterProcedureEnv env = procExec.getEnvironment();

    SnapshotProcedure sp1 = new SnapshotProcedure(env, snapshotProto);
    SnapshotProcedure sp2 = new SnapshotProcedure(env, snapshotProto2);
    SnapshotProcedure spySp1 = getDelayedOnSpecificStateSnapshotProcedure(sp1,
      procExec.getEnvironment(), SnapshotState.SNAPSHOT_SNAPSHOT_ONLINE_REGIONS);
    SnapshotProcedure spySp2 = getDelayedOnSpecificStateSnapshotProcedure(sp2,
      procExec.getEnvironment(), SnapshotState.SNAPSHOT_SNAPSHOT_ONLINE_REGIONS);

    long procId1 = procExec.submitProcedure(spySp1);
    long procId2 = procExec.submitProcedure(spySp2);
    TEST_UTIL.waitFor(2000, () -> env.getMasterServices().getProcedures()
      .stream().map(Procedure::getProcId).collect(Collectors.toList())
      .containsAll(Arrays.asList(procId1, procId2)));

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


  private SnapshotProcedure getDelayedOnSpecificStateSnapshotProcedure(
      SnapshotProcedure sp, MasterProcedureEnv env, SnapshotState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    SnapshotProcedure spySp = Mockito.spy(sp);
    Mockito.doAnswer(new AnswersWithDelay(60000, new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        return invocation.callRealMethod();
      }
    })).when(spySp).executeFromState(env, state);
    return spySp;
  }

  @After
  public void teardown() throws Exception {
    if (this.master != null) {
      ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(
        master.getMasterProcedureExecutor(), false);
    }
    TEST_UTIL.shutdownMiniCluster();
  }
}
