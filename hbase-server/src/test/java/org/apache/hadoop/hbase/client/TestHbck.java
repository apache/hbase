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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
import org.apache.hadoop.hbase.master.assignment.MergeTableRegionsProcedure;
import org.apache.hadoop.hbase.master.assignment.SplitTableRegionProcedure;
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
import org.junit.AfterClass;
import org.junit.Assert;
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
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos;

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
  public void testRecoverMergeAfterMetaUpdated() throws Exception {
    String testTable = async ? "mergeTestAsync" : "mergeTestSync";
    TEST_UTIL.createMultiRegionTable(TableName.valueOf(testTable), Bytes.toBytes("family1"), 5);
    TEST_UTIL.loadTable(TEST_UTIL.getConnection().getTable(TableName.valueOf(testTable)),
      Bytes.toBytes("family1"), true);
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    Hbck hbck = getHbck();
    FailingMergeAfterMetaUpdatedMasterObserver observer = master.getMasterCoprocessorHost()
        .findCoprocessor(FailingMergeAfterMetaUpdatedMasterObserver.class);
    try (Admin admin = TEST_UTIL.getConnection().getAdmin()) {
      List<RegionInfo> regions = admin.getRegions(TableName.valueOf(testTable));
      admin.mergeRegionsAsync(regions.get(0).getRegionName(), regions.get(1).getRegionName(), true);
      assertNotNull(observer);
      observer.latch.await(5000, TimeUnit.MILLISECONDS);
      Map<String, MasterProtos.RegionErrorType> result =
          hbck.getFailedSplitMergeLegacyRegions(Arrays.asList(TableName.valueOf(testTable)));
      Assert.assertEquals(0, result.size());
      Optional<Procedure<?>> procedure = TEST_UTIL.getHBaseCluster().getMaster().getProcedures()
          .stream().filter(p -> p instanceof MergeTableRegionsProcedure).findAny();
      Assert.assertTrue(procedure.isPresent());
      hbck.bypassProcedure(Arrays.asList(procedure.get().getProcId()), 5, true, false);
      result = hbck.getFailedSplitMergeLegacyRegions(Arrays.asList(TableName.valueOf(testTable)));
      Assert.assertEquals(1, result.size());
      hbck.assigns(Arrays.asList(result.keySet().toArray(new String[0])).stream()
          .map(regionName -> regionName.split("\\.")[1]).collect(Collectors.toList()));
      ProcedureTestingUtility.waitAllProcedures(master.getMasterProcedureExecutor());
      // now the state should be fixed
      result = hbck.getFailedSplitMergeLegacyRegions(Arrays.asList(TableName.valueOf(testTable)));
      Assert.assertEquals(0, result.size());
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    } finally {
      observer.resetLatch();
    }
  }

  @Test
  public void testRecoverSplitAfterMetaUpdated() throws Exception {
    String testTable = async ? "splitTestAsync" : "splitTestSync";
    TEST_UTIL.createMultiRegionTable(TableName.valueOf(testTable), Bytes.toBytes("family1"), 5);
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    Hbck hbck = getHbck();
    FailingSplitAfterMetaUpdatedMasterObserver observer = master.getMasterCoprocessorHost()
        .findCoprocessor(FailingSplitAfterMetaUpdatedMasterObserver.class);
    assertNotNull(observer);
    try {
      AsyncAdmin admin = TEST_UTIL.getAsyncConnection().getAdmin();
      byte[] splitKey = Bytes.toBytes("bcd");
      admin.split(TableName.valueOf(testTable), splitKey);
      observer.latch.await(5000, TimeUnit.MILLISECONDS);
      Map<String, MasterProtos.RegionErrorType> result =
          hbck.getFailedSplitMergeLegacyRegions(Arrays.asList(TableName.valueOf(testTable)));
      // since there is a split procedure work on the region, thus this check should return a empty
      // map.
      Assert.assertEquals(0, result.size());
      Optional<Procedure<?>> procedure = TEST_UTIL.getHBaseCluster().getMaster().getProcedures()
          .stream().filter(p -> p instanceof SplitTableRegionProcedure).findAny();
      Assert.assertTrue(procedure.isPresent());
      hbck.bypassProcedure(Arrays.asList(procedure.get().getProcId()), 5, true, false);
      result = hbck.getFailedSplitMergeLegacyRegions(Arrays.asList(TableName.valueOf(testTable)));
      Assert.assertEquals(2, result.size());
      hbck.assigns(Arrays.asList(result.keySet().toArray(new String[0])).stream()
          .map(regionName -> regionName.split("\\.")[1]).collect(Collectors.toList()));
      ProcedureTestingUtility.waitAllProcedures(master.getMasterProcedureExecutor());
      // now the state should be fixed
      result = hbck.getFailedSplitMergeLegacyRegions(Arrays.asList(TableName.valueOf(testTable)));
      Assert.assertEquals(0, result.size());

      //split one of the daughter region again
      observer.resetLatch();
      byte[] splitKey2 = Bytes.toBytes("bcde");

      admin.split(TableName.valueOf(testTable), splitKey2);
      observer.latch.await(5000, TimeUnit.MILLISECONDS);

      procedure = TEST_UTIL.getHBaseCluster().getMaster().getProcedures()
          .stream().filter(p -> p instanceof SplitTableRegionProcedure).findAny();
      Assert.assertTrue(procedure.isPresent());
      hbck.bypassProcedure(Arrays.asList(procedure.get().getProcId()), 5, true, false);
      result = hbck.getFailedSplitMergeLegacyRegions(Arrays.asList(TableName.valueOf(testTable)));
      Assert.assertEquals(2, result.size());
      hbck.assigns(Arrays.asList(result.keySet().toArray(new String[0])).stream()
          .map(regionName -> regionName.split("\\.")[1]).collect(Collectors.toList()));
      ProcedureTestingUtility.waitAllProcedures(master.getMasterProcedureExecutor());
      // now the state should be fixed
      result = hbck.getFailedSplitMergeLegacyRegions(Arrays.asList(TableName.valueOf(testTable)));
      Assert.assertEquals(0, result.size());
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    } finally {
      observer.resetLatch();
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

  public static class FailingSplitAfterMetaUpdatedMasterObserver
      implements MasterCoprocessor, MasterObserver {
    public volatile CountDownLatch latch;

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
    public volatile CountDownLatch latch;

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
