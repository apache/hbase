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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.TableProcedureInterface;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.io.Closeables;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

/**
 * Class to test HBaseHbck. Spins up the minicluster once at test start and then takes it down
 * afterward. Add any testing of HBaseHbck functionality here.
 */
@RunWith(Parameterized.class)
@Category({ LargeTests.class, ClientTests.class })
public class TestHbck {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestHbck.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestHbck.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @Rule
  public TestName name = new TestName();

  @SuppressWarnings("checkstyle:VisibilityModifier") @Parameter
  public boolean async;

  private static final TableName TABLE_NAME = TableName.valueOf(TestHbck.class.getSimpleName());

  private static ProcedureExecutor<MasterProcedureEnv> procExec;

  private static AsyncConnection ASYNC_CONN;

  @Parameters(name = "{index}: async={0}")
  public static List<Object[]> params() {
    return Arrays.asList(new Object[] { false }, new Object[] { true });
  }

  private Hbck getHbck() throws Exception {
    if (async) {
      return ASYNC_CONN.getHbck().get();
    } else {
      return TEST_UTIL.getHbck();
    }
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(3);
    TEST_UTIL.createMultiRegionTable(TABLE_NAME, Bytes.toBytes("family1"), 5);
    procExec = TEST_UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();
    ASYNC_CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();
    TEST_UTIL.getHBaseCluster().getMaster().getMasterCoprocessorHost().load(
      FailingMergeAfterMetaUpdatedMasterObserver.class, Coprocessor.PRIORITY_USER,
      TEST_UTIL.getHBaseCluster().getMaster().getConfiguration());
    TEST_UTIL.getHBaseCluster().getMaster().getMasterCoprocessorHost().load(
      FailingSplitAfterMetaUpdatedMasterObserver.class, Coprocessor.PRIORITY_USER,
      TEST_UTIL.getHBaseCluster().getMaster().getConfiguration());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Closeables.close(ASYNC_CONN, true);
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws IOException {
    TEST_UTIL.ensureSomeRegionServersAvailable(3);
  }

  public static class SuspendProcedure extends
      ProcedureTestingUtility.NoopProcedure<MasterProcedureEnv> implements TableProcedureInterface {
    public SuspendProcedure() {
      super();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    protected Procedure[] execute(final MasterProcedureEnv env) throws ProcedureSuspendedException {
      // Always suspend the procedure
      throw new ProcedureSuspendedException();
    }

    @Override
    public TableName getTableName() {
      return TABLE_NAME;
    }

    @Override
    public TableOperationType getTableOperationType() {
      return TableOperationType.READ;
    }
  }

  @Test
  public void testBypassProcedure() throws Exception {
    // SuspendProcedure
    final SuspendProcedure proc = new SuspendProcedure();
    long procId = procExec.submitProcedure(proc);
    Thread.sleep(500);

    // bypass the procedure
    List<Long> pids = Arrays.<Long> asList(procId);
    List<Boolean> results = getHbck().bypassProcedure(pids, 30000, false, false);
    assertTrue("Failed to by pass procedure!", results.get(0));
    TEST_UTIL.waitFor(5000, () -> proc.isSuccess() && proc.isBypass());
    LOG.info("{} finished", proc);
  }

  @Test
  public void testSetTableStateInMeta() throws Exception {
    Hbck hbck = getHbck();
    // set table state to DISABLED
    hbck.setTableStateInMeta(new TableState(TABLE_NAME, TableState.State.DISABLED));
    // Method {@link Hbck#setTableStateInMeta()} returns previous state, which in this case
    // will be DISABLED
    TableState prevState =
      hbck.setTableStateInMeta(new TableState(TABLE_NAME, TableState.State.ENABLED));
    assertTrue("Incorrect previous state! expeced=DISABLED, found=" + prevState.getState(),
      prevState.isDisabled());
  }

  @Test
  public void testSetRegionStateInMeta() throws Exception {
    Hbck hbck = getHbck();
    Admin admin = TEST_UTIL.getAdmin();
    final List<RegionInfo> regions = admin.getRegions(TABLE_NAME);
    final AssignmentManager am = TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager();
    Map<String, RegionState.State> prevStates = new HashMap<>();
    Map<String, RegionState.State> newStates = new HashMap<>();
    final Map<String, Pair<RegionState.State, RegionState.State>> regionsMap = new HashMap<>();
    regions.forEach(r -> {
      RegionState prevState = am.getRegionStates().getRegionState(r);
      prevStates.put(r.getEncodedName(), prevState.getState());
      newStates.put(r.getEncodedName(), RegionState.State.CLOSED);
      regionsMap.put(r.getEncodedName(),
        new Pair<>(prevState.getState(), RegionState.State.CLOSED));
    });
    final Map<String, RegionState.State> result = hbck.setRegionStateInMeta(newStates);
    result.forEach((k, v) -> {
      RegionState.State prevState = regionsMap.get(k).getFirst();
      assertEquals(prevState, v);
    });
    regions.forEach(r -> {
      RegionState cachedState = am.getRegionStates().getRegionState(r.getEncodedName());
      RegionState.State newState = regionsMap.get(r.getEncodedName()).getSecond();
      assertEquals(newState, cachedState.getState());
    });
    hbck.setRegionStateInMeta(prevStates);
  }

  @Test
  public void testAssigns() throws Exception {
    Hbck hbck = getHbck();
    try (Admin admin = TEST_UTIL.getConnection().getAdmin()) {
      List<RegionInfo> regions = admin.getRegions(TABLE_NAME);
      for (RegionInfo ri : regions) {
        RegionState rs = TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager()
          .getRegionStates().getRegionState(ri.getEncodedName());
        LOG.info("RS: {}", rs.toString());
      }
      List<Long> pids =
        hbck.unassigns(regions.stream().map(r -> r.getEncodedName()).collect(Collectors.toList()));
      waitOnPids(pids);
      // Rerun the unassign. Should fail for all Regions since they already unassigned; failed
      // unassign will manifest as all pids being -1 (ever since HBASE-24885).
      pids =
        hbck.unassigns(regions.stream().map(r -> r.getEncodedName()).collect(Collectors.toList()));
      waitOnPids(pids);
      for (long pid: pids) {
        assertEquals(Procedure.NO_PROC_ID, pid);
      }
      // If we pass override, then we should be able to unassign EVEN THOUGH Regions already
      // unassigned.... makes for a mess but operator might want to do this at an extreme when
      // doing fixup of broke cluster.
      pids =
        hbck.unassigns(regions.stream().map(r -> r.getEncodedName()).collect(Collectors.toList()),
          true);
      waitOnPids(pids);
      for (long pid: pids) {
        assertNotEquals(Procedure.NO_PROC_ID, pid);
      }
      // Clean-up by bypassing all the unassigns we just made so tests can continue.
      hbck.bypassProcedure(pids, 10000, true, true);
      for (RegionInfo ri : regions) {
        RegionState rs = TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager()
          .getRegionStates().getRegionState(ri.getEncodedName());
        LOG.info("RS: {}", rs.toString());
        assertTrue(rs.toString(), rs.isClosed());
      }
      pids =
        hbck.assigns(regions.stream().map(r -> r.getEncodedName()).collect(Collectors.toList()));
      waitOnPids(pids);
      // Rerun the assign. Should fail for all Regions since they already assigned; failed
      // assign will manifest as all pids being -1 (ever since HBASE-24885).
      pids =
        hbck.assigns(regions.stream().map(r -> r.getEncodedName()).collect(Collectors.toList()));
      for (long pid: pids) {
        assertEquals(Procedure.NO_PROC_ID, pid);
      }
      for (RegionInfo ri : regions) {
        RegionState rs = TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager()
          .getRegionStates().getRegionState(ri.getEncodedName());
        LOG.info("RS: {}", rs.toString());
        assertTrue(rs.toString(), rs.isOpened());
      }
      // What happens if crappy region list passed?
      pids = hbck.assigns(
        Arrays.stream(new String[] { "a", "some rubbish name" }).collect(Collectors.toList()));
      for (long pid : pids) {
        assertEquals(Procedure.NO_PROC_ID, pid);
      }
    }
  }

  @Test
  public void testScheduleSCP() throws Exception {
    HRegionServer testRs = TEST_UTIL.getRSForFirstRegionInTable(TABLE_NAME);
    TEST_UTIL.loadTable(TEST_UTIL.getConnection().getTable(TABLE_NAME), Bytes.toBytes("family1"),
      true);
    ServerName serverName = testRs.getServerName();
    Hbck hbck = getHbck();
    List<Long> pids =
      hbck.scheduleServerCrashProcedure(Arrays.asList(ProtobufUtil.toServerName(serverName)));
    assertTrue(pids.get(0) > 0);
    LOG.info("pid is {}", pids.get(0));

    List<Long> newPids =
      hbck.scheduleServerCrashProcedure(Arrays.asList(ProtobufUtil.toServerName(serverName)));
    assertTrue(newPids.get(0) < 0);
    LOG.info("pid is {}", newPids.get(0));
    waitOnPids(pids);
  }

  @Test
  public void testRunHbckChore() throws Exception {
    HMaster master = TEST_UTIL.getMiniHBaseCluster().getMaster();
    long endTimestamp = master.getHbckChore().getCheckingEndTimestamp();
    Hbck hbck = getHbck();
    boolean ran = false;
    while (!ran) {
      ran = hbck.runHbckChore();
      if (ran) {
        assertTrue(master.getHbckChore().getCheckingEndTimestamp() > endTimestamp);
      }
    }
  }

  public static class FailingSplitAfterMetaUpdatedMasterObserver
      implements MasterCoprocessor, MasterObserver {
    @SuppressWarnings("checkstyle:VisibilityModifier") public volatile CountDownLatch latch;

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
      resetLatch();
    }

    @Override
    public Optional<MasterObserver> getMasterObserver() {
      return Optional.of(this);
    }

    @Override
    public void preSplitRegionAfterMETAAction(ObserverContext<MasterCoprocessorEnvironment> ctx)
        throws IOException {
      LOG.info("I'm here");
      latch.countDown();
      throw new IOException("this procedure will fail at here forever");
    }

    public void resetLatch() {
      this.latch = new CountDownLatch(1);
    }
  }

  public static class FailingMergeAfterMetaUpdatedMasterObserver
      implements MasterCoprocessor, MasterObserver {
    @SuppressWarnings("checkstyle:VisibilityModifier") public volatile CountDownLatch latch;

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
      resetLatch();
    }

    @Override
    public Optional<MasterObserver> getMasterObserver() {
      return Optional.of(this);
    }

    public void resetLatch() {
      this.latch = new CountDownLatch(1);
    }

    @Override
    public void postMergeRegionsCommitAction(
        final ObserverContext<MasterCoprocessorEnvironment> ctx, final RegionInfo[] regionsToMerge,
        final RegionInfo mergedRegion) throws IOException {
      latch.countDown();
      throw new IOException("this procedure will fail at here forever");
    }
  }

  private void waitOnPids(List<Long> pids) {
    TEST_UTIL.waitFor(60000, () -> pids.stream().allMatch(procExec::isFinished));
  }
}
