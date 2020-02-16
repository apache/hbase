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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
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

  @Parameter
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
    try(Admin admin = TEST_UTIL.getAdmin()){
      final List<RegionInfo> regions = admin.getRegions(TABLE_NAME);
      final AssignmentManager am = TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager();
      final List<RegionState> prevStates = new ArrayList<>();
      final List<RegionState> newStates = new ArrayList<>();
      final Map<String, Pair<RegionState, RegionState>> regionsMap = new HashMap<>();
      regions.forEach(r -> {
        RegionState prevState = am.getRegionStates().getRegionState(r);
        prevStates.add(prevState);
        RegionState newState = RegionState.createForTesting(r, RegionState.State.CLOSED);
        newStates.add(newState);
        regionsMap.put(r.getEncodedName(), new Pair<>(prevState, newState));
      });
      final List<RegionState> result = hbck.setRegionStateInMeta(newStates);
      result.forEach(r -> {
        RegionState prevState = regionsMap.get(r.getRegion().getEncodedName()).getFirst();
        assertEquals(prevState.getState(), r.getState());
      });
      regions.forEach(r -> {
        RegionState cachedState = am.getRegionStates().getRegionState(r.getEncodedName());
        RegionState newState = regionsMap.get(r.getEncodedName()).getSecond();
        assertEquals(newState.getState(), cachedState.getState());
      });
      hbck.setRegionStateInMeta(prevStates);
    }
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
      for (RegionInfo ri : regions) {
        RegionState rs = TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager()
          .getRegionStates().getRegionState(ri.getEncodedName());
        LOG.info("RS: {}", rs.toString());
        assertTrue(rs.toString(), rs.isClosed());
      }
      pids =
        hbck.assigns(regions.stream().map(r -> r.getEncodedName()).collect(Collectors.toList()));
      waitOnPids(pids);
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
        assertEquals(org.apache.hadoop.hbase.procedure2.Procedure.NO_PROC_ID, pid);
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

  @Test
  public void testMergeRegions() throws Exception {
    Hbck hbck = getHbck();
    TableName tn = TableName.valueOf(async ? "testMergeRegionsAsync" : "testMergeRegionsSync");
    TEST_UTIL.createMultiRegionTable(tn, Bytes.toBytes("family1"), 4);
    try (Admin admin = TEST_UTIL.getAdmin()) {
      List<RegionInfo> regions = admin.getRegions(tn);
      List<byte[]> regionsList = new ArrayList<>();
      List<Long> pids = new ArrayList<>();

      // test num of regions < 2
      regionsList.add(regions.get(0).getEncodedNameAsBytes());
      try{
        hbck.mergeRegions(regionsList, false, HConstants.NO_NONCE, HConstants.NO_NONCE);
        fail("Should have failed with : Need two Regions at least to run a Merge");
      } catch (IOException ioe) {
      }

      // test dummy region
      regionsList.add("dummy_region".getBytes());
      try{
        hbck.mergeRegions(regionsList, false, HConstants.NO_NONCE, HConstants.NO_NONCE);
        fail("Should have failed with : UnknownRegionException");
      } catch (IOException ioe) {
      }

      // test duplicate region
      regionsList.set(1,regionsList.get(0));
      try{
        hbck.mergeRegions(regionsList, false, HConstants.NO_NONCE, HConstants.NO_NONCE);
        fail("Should have failed MergeRegionException: Duplicate regions specified; cannot merge a region to itself");
      } catch (IOException ioe) {
      }

      // test non-adjacent region merge
      regionsList.set(1,regions.get(2).getEncodedNameAsBytes());
      try{
        hbck.mergeRegions(regionsList, false, HConstants.NO_NONCE, HConstants.NO_NONCE);
        fail("Should have failed with MergeRegionException: Unable to merge non-adjacent or non-overlapping regions");
      } catch (IOException ioe) {
      }

      // test multiple regions (>2) & non adjacent regions with force and without force
      regionsList.add(regions.get(1).getEncodedNameAsBytes());
      regionsList.add(regions.get(3).getEncodedNameAsBytes());
      // regionsList now contains r0, r2, r1, r3

      try{
        // without force, hence should fail
        hbck.mergeRegions(regionsList, false, HConstants.NO_NONCE, HConstants.NO_NONCE);
        fail("Should have failed with MergeRegionException: Unable to merge non-adjacent or non-overlapping regions");
      } catch (IOException ioe) {
      }

      // with force, hence should pass and result in exactly 1 region
      pids.add(hbck.mergeRegions(regionsList, true, HConstants.NO_NONCE, HConstants.NO_NONCE));
      LOG.info("pid for the mergeRegion created is {}", pids.get(0));
      waitOnPids(pids);
      regions = admin.getRegions(tn);
      assertEquals(1, regions.size());
      RegionState rs =
        TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStates()
          .getRegionState(regions.get(0).getEncodedName());
      assertTrue(rs.toString() + "not yet in opened state", rs.isOpened());
    }
  }

  private void waitOnPids(List<Long> pids) {
    TEST_UTIL.waitFor(60000, () -> pids.stream().allMatch(procExec::isFinished));
  }
}
