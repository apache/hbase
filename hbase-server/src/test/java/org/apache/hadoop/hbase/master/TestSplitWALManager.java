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
package org.apache.hadoop.hbase.master;

import static org.apache.hadoop.hbase.HConstants.HBASE_SPLIT_WAL_COORDINATED_BY_ZK;
import static org.apache.hadoop.hbase.HConstants.HBASE_SPLIT_WAL_MAX_SPLITTER;
import static org.apache.hadoop.hbase.master.procedure.ServerProcedureInterface.ServerOperationType.SPLIT_WAL;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.ServerProcedureInterface;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MasterTests.class, LargeTests.class })

public class TestSplitWALManager {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSplitWALManager.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestSplitWALManager.class);
  private static HBaseTestingUtility TEST_UTIL;
  private HMaster master;
  private SplitWALManager splitWALManager;
  private TableName TABLE_NAME;
  private byte[] FAMILY;

  @Before
  public void setup() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    TEST_UTIL.getConfiguration().setBoolean(HBASE_SPLIT_WAL_COORDINATED_BY_ZK, false);
    TEST_UTIL.getConfiguration().setInt(HBASE_SPLIT_WAL_MAX_SPLITTER, 1);
    TEST_UTIL.startMiniCluster(3);
    master = TEST_UTIL.getHBaseCluster().getMaster();
    splitWALManager = master.getSplitWALManager();
    TABLE_NAME = TableName.valueOf(Bytes.toBytes("TestSplitWALManager"));
    FAMILY = Bytes.toBytes("test");
  }

  @After
  public void teardown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testAcquireAndRelease() throws Exception {
    List<FakeServerProcedure> testProcedures = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      testProcedures.add(new FakeServerProcedure(
          TEST_UTIL.getHBaseCluster().getServerHoldingMeta()));
    }
    ServerName server = splitWALManager.acquireSplitWALWorker(testProcedures.get(0));
    Assert.assertNotNull(server);
    Assert.assertNotNull(splitWALManager.acquireSplitWALWorker(testProcedures.get(1)));
    Assert.assertNotNull(splitWALManager.acquireSplitWALWorker(testProcedures.get(2)));

    Exception e = null;
    try {
      splitWALManager.acquireSplitWALWorker(testProcedures.get(3));
    } catch (ProcedureSuspendedException suspendException) {
      e = suspendException;
    }
    Assert.assertNotNull(e);
    Assert.assertTrue(e instanceof ProcedureSuspendedException);

    splitWALManager.releaseSplitWALWorker(server, TEST_UTIL.getHBaseCluster().getMaster()
        .getMasterProcedureExecutor().getEnvironment().getProcedureScheduler());
    Assert.assertNotNull(splitWALManager.acquireSplitWALWorker(testProcedures.get(3)));
  }

  @Test
  public void testAddNewServer() throws Exception {
    List<FakeServerProcedure> testProcedures = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      testProcedures.add(new FakeServerProcedure(
          TEST_UTIL.getHBaseCluster().getServerHoldingMeta()));
    }
    ServerName server = splitWALManager.acquireSplitWALWorker(testProcedures.get(0));
    Assert.assertNotNull(server);
    Assert.assertNotNull(splitWALManager.acquireSplitWALWorker(testProcedures.get(1)));
    Assert.assertNotNull(splitWALManager.acquireSplitWALWorker(testProcedures.get(2)));

    Exception e = null;
    try {
      splitWALManager.acquireSplitWALWorker(testProcedures.get(3));
    } catch (ProcedureSuspendedException suspendException) {
      e = suspendException;
    }
    Assert.assertNotNull(e);
    Assert.assertTrue(e instanceof ProcedureSuspendedException);

    JVMClusterUtil.RegionServerThread newServer = TEST_UTIL.getHBaseCluster().startRegionServer();
    newServer.waitForServerOnline();
    Assert.assertNotNull(splitWALManager.acquireSplitWALWorker(testProcedures.get(3)));
  }

  @Test
  public void testCreateSplitWALProcedures() throws Exception {
    TEST_UTIL.createTable(TABLE_NAME, FAMILY, TEST_UTIL.KEYS_FOR_HBA_CREATE_TABLE);
    // load table
    TEST_UTIL.loadTable(TEST_UTIL.getConnection().getTable(TABLE_NAME), FAMILY);
    ProcedureExecutor<MasterProcedureEnv> masterPE = master.getMasterProcedureExecutor();
    ServerName metaServer = TEST_UTIL.getHBaseCluster().getServerHoldingMeta();
    Path metaWALDir = new Path(TEST_UTIL.getDefaultRootDirPath(),
        AbstractFSWALProvider.getWALDirectoryName(metaServer.toString()));
    // Test splitting meta wal
    FileStatus[] wals =
        TEST_UTIL.getTestFileSystem().listStatus(metaWALDir, MasterWalManager.META_FILTER);
    Assert.assertEquals(1, wals.length);
    List<Procedure> testProcedures =
        splitWALManager.createSplitWALProcedures(Lists.newArrayList(wals[0]), metaServer);
    Assert.assertEquals(1, testProcedures.size());
    ProcedureTestingUtility.submitAndWait(masterPE, testProcedures.get(0));
    Assert.assertFalse(TEST_UTIL.getTestFileSystem().exists(wals[0].getPath()));

    // Test splitting wal
    wals = TEST_UTIL.getTestFileSystem().listStatus(metaWALDir, MasterWalManager.NON_META_FILTER);
    Assert.assertEquals(1, wals.length);
    testProcedures =
        splitWALManager.createSplitWALProcedures(Lists.newArrayList(wals[0]), metaServer);
    Assert.assertEquals(1, testProcedures.size());
    ProcedureTestingUtility.submitAndWait(masterPE, testProcedures.get(0));
    Assert.assertFalse(TEST_UTIL.getTestFileSystem().exists(wals[0].getPath()));
  }

  @Test
  public void testAcquireAndReleaseSplitWALWorker() throws Exception {
    ProcedureExecutor<MasterProcedureEnv> masterPE = master.getMasterProcedureExecutor();
    List<FakeServerProcedure> testProcedures = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      FakeServerProcedure procedure =
          new FakeServerProcedure(TEST_UTIL.getHBaseCluster().getRegionServer(i).getServerName());
      testProcedures.add(procedure);
      ProcedureTestingUtility.submitProcedure(masterPE, procedure, HConstants.NO_NONCE,
          HConstants.NO_NONCE);
    }
    TEST_UTIL.waitFor(10000, () -> testProcedures.get(2).isWorkerAcquired());
    FakeServerProcedure failedProcedure =
        new FakeServerProcedure(TEST_UTIL.getHBaseCluster().getServerHoldingMeta());
    ProcedureTestingUtility.submitProcedure(masterPE, failedProcedure, HConstants.NO_NONCE,
        HConstants.NO_NONCE);
    TEST_UTIL.waitFor(20000, () -> failedProcedure.isTriedToAcquire());
    Assert.assertFalse(failedProcedure.isWorkerAcquired());
    // let one procedure finish and release worker
    testProcedures.get(0).countDown();
    TEST_UTIL.waitFor(10000, () -> failedProcedure.isWorkerAcquired());
    Assert.assertTrue(testProcedures.get(0).isSuccess());
  }

  @Test
  public void testGetWALsToSplit() throws Exception {
    TEST_UTIL.createTable(TABLE_NAME, FAMILY, TEST_UTIL.KEYS_FOR_HBA_CREATE_TABLE);
    // load table
    TEST_UTIL.loadTable(TEST_UTIL.getConnection().getTable(TABLE_NAME), FAMILY);
    ServerName metaServer = TEST_UTIL.getHBaseCluster().getServerHoldingMeta();
    List<FileStatus> metaWals = splitWALManager.getWALsToSplit(metaServer, true);
    Assert.assertEquals(1, metaWals.size());
    List<FileStatus> wals = splitWALManager.getWALsToSplit(metaServer, false);
    Assert.assertEquals(1, wals.size());
    ServerName testServer = TEST_UTIL.getHBaseCluster().getRegionServerThreads().stream()
        .map(rs -> rs.getRegionServer().getServerName()).filter(rs -> rs != metaServer).findAny()
        .get();
    metaWals = splitWALManager.getWALsToSplit(testServer, true);
    Assert.assertEquals(0, metaWals.size());
  }

  private void splitLogsTestHelper(HBaseTestingUtility testUtil) throws Exception {
    HMaster hmaster = testUtil.getHBaseCluster().getMaster();
    SplitWALManager splitWALManager = hmaster.getSplitWALManager();
    LOG.info("The Master FS is pointing to: " + hmaster.getMasterFileSystem()
      .getFileSystem().getUri());
    LOG.info("The WAL FS is pointing to: " + hmaster.getMasterFileSystem()
      .getWALFileSystem().getUri());

    testUtil.createTable(TABLE_NAME, FAMILY, testUtil.KEYS_FOR_HBA_CREATE_TABLE);
    // load table
    testUtil.loadTable(testUtil.getConnection().getTable(TABLE_NAME), FAMILY);
    ProcedureExecutor<MasterProcedureEnv> masterPE = hmaster.getMasterProcedureExecutor();
    ServerName metaServer = testUtil.getHBaseCluster().getServerHoldingMeta();
    ServerName testServer = testUtil.getHBaseCluster().getRegionServerThreads().stream()
      .map(rs -> rs.getRegionServer().getServerName()).filter(rs -> rs != metaServer).findAny()
      .get();
    List<Procedure> procedures = splitWALManager.splitWALs(testServer, false);
    Assert.assertEquals(1, procedures.size());
    ProcedureTestingUtility.submitAndWait(masterPE, procedures.get(0));
    Assert.assertEquals(0, splitWALManager.getWALsToSplit(testServer, false).size());

    // Validate the old WAL file archive dir
    Path walRootDir = hmaster.getMasterFileSystem().getWALRootDir();
    Path walArchivePath = new Path(walRootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    FileSystem walFS = hmaster.getMasterFileSystem().getWALFileSystem();
    int archiveFileCount = walFS.listStatus(walArchivePath).length;

    procedures = splitWALManager.splitWALs(metaServer, true);
    Assert.assertEquals(1, procedures.size());
    ProcedureTestingUtility.submitAndWait(masterPE, procedures.get(0));
    Assert.assertEquals(0, splitWALManager.getWALsToSplit(metaServer, true).size());
    Assert.assertEquals(1, splitWALManager.getWALsToSplit(metaServer, false).size());
    // There should be archiveFileCount + 1 WALs after SplitWALProcedure finish
    Assert.assertEquals("Splitted WAL files should be archived", archiveFileCount + 1,
      walFS.listStatus(walArchivePath).length);
  }

  @Test
  public void testSplitLogs() throws Exception {
    splitLogsTestHelper(TEST_UTIL);
  }

  @Test
  public void testSplitLogsWithDifferentWalAndRootFS() throws Exception{
    HBaseTestingUtility testUtil2 = new HBaseTestingUtility();
    testUtil2.getConfiguration().setBoolean(HBASE_SPLIT_WAL_COORDINATED_BY_ZK, false);
    testUtil2.getConfiguration().setInt(HBASE_SPLIT_WAL_MAX_SPLITTER, 1);
    Path dir = TEST_UTIL.getDataTestDirOnTestFS("testWalDir");
    testUtil2.getConfiguration().set(CommonFSUtils.HBASE_WAL_DIR, dir.toString());
    CommonFSUtils.setWALRootDir(testUtil2.getConfiguration(), dir);
    testUtil2.startMiniCluster(3);
    splitLogsTestHelper(testUtil2);
    testUtil2.shutdownMiniCluster();
  }

  @Test
  public void testWorkerReloadWhenMasterRestart() throws Exception {
    List<FakeServerProcedure> testProcedures = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      FakeServerProcedure procedure =
          new FakeServerProcedure(TEST_UTIL.getHBaseCluster().getRegionServer(i).getServerName());
      testProcedures.add(procedure);
      ProcedureTestingUtility.submitProcedure(master.getMasterProcedureExecutor(), procedure,
        HConstants.NO_NONCE, HConstants.NO_NONCE);
    }
    TEST_UTIL.waitFor(10000, () -> testProcedures.get(2).isWorkerAcquired());
    // Kill master
    TEST_UTIL.getHBaseCluster().killMaster(master.getServerName());
    TEST_UTIL.getHBaseCluster().waitForMasterToStop(master.getServerName(), 20000);
    // restart master
    TEST_UTIL.getHBaseCluster().startMaster();
    TEST_UTIL.getHBaseCluster().waitForActiveAndReadyMaster();
    this.master = TEST_UTIL.getHBaseCluster().getMaster();

    FakeServerProcedure failedProcedure =
        new FakeServerProcedure(TEST_UTIL.getHBaseCluster().getServerHoldingMeta());
    ProcedureTestingUtility.submitProcedure(master.getMasterProcedureExecutor(), failedProcedure,
      HConstants.NO_NONCE, HConstants.NO_NONCE);
    TEST_UTIL.waitFor(20000, () -> failedProcedure.isTriedToAcquire());
    Assert.assertFalse(failedProcedure.isWorkerAcquired());
    for (int i = 0; i < 3; i++) {
      testProcedures.get(i).countDown();
    }
    failedProcedure.countDown();
  }

  public static final class FakeServerProcedure
      extends StateMachineProcedure<MasterProcedureEnv, MasterProcedureProtos.SplitWALState>
      implements ServerProcedureInterface {

    private ServerName serverName;
    private ServerName worker;
    private CountDownLatch barrier = new CountDownLatch(1);
    private boolean triedToAcquire = false;

    public FakeServerProcedure() {
    }

    public FakeServerProcedure(ServerName serverName) {
      this.serverName = serverName;
    }

    public ServerName getServerName() {
      return serverName;
    }

    @Override
    public boolean hasMetaTableRegion() {
      return false;
    }

    @Override
    public ServerOperationType getServerOperationType() {
      return SPLIT_WAL;
    }

    @Override
    protected Flow executeFromState(MasterProcedureEnv env,
                                    MasterProcedureProtos.SplitWALState state)
        throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
      SplitWALManager splitWALManager = env.getMasterServices().getSplitWALManager();
      switch (state) {
        case ACQUIRE_SPLIT_WAL_WORKER:
          triedToAcquire = true;
          worker = splitWALManager.acquireSplitWALWorker(this);
          setNextState(MasterProcedureProtos.SplitWALState.DISPATCH_WAL_TO_WORKER);
          return Flow.HAS_MORE_STATE;
        case DISPATCH_WAL_TO_WORKER:
          barrier.await();
          setNextState(MasterProcedureProtos.SplitWALState.RELEASE_SPLIT_WORKER);
          return Flow.HAS_MORE_STATE;
        case RELEASE_SPLIT_WORKER:
          splitWALManager.releaseSplitWALWorker(worker, env.getProcedureScheduler());
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException("unhandled state=" + state);
      }
    }

    public boolean isWorkerAcquired() {
      return worker != null;
    }

    public boolean isTriedToAcquire() {
      return triedToAcquire;
    }

    public void countDown() {
      this.barrier.countDown();
    }

    @Override
    protected void rollbackState(MasterProcedureEnv env, MasterProcedureProtos.SplitWALState state)
        throws IOException, InterruptedException {

    }

    @Override
    protected MasterProcedureProtos.SplitWALState getState(int stateId) {
      return MasterProcedureProtos.SplitWALState.forNumber(stateId);
    }

    @Override
    protected int getStateId(MasterProcedureProtos.SplitWALState state) {
      return state.getNumber();
    }

    @Override
    protected MasterProcedureProtos.SplitWALState getInitialState() {
      return MasterProcedureProtos.SplitWALState.ACQUIRE_SPLIT_WAL_WORKER;
    }

    @Override
    protected boolean holdLock(MasterProcedureEnv env) {
      return true;
    }

    @Override
    protected void rollback(MasterProcedureEnv env) throws IOException, InterruptedException {

    }

    @Override
    protected boolean abort(MasterProcedureEnv env) {
      return false;
    }

    @Override
    protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
      MasterProcedureProtos.SplitWALData.Builder builder =
          MasterProcedureProtos.SplitWALData.newBuilder();
      builder.setWalPath("test").setCrashedServer(ProtobufUtil.toServerName(serverName));
      serializer.serialize(builder.build());
    }

    @Override
    protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
      MasterProcedureProtos.SplitWALData data =
          serializer.deserialize(MasterProcedureProtos.SplitWALData.class);
      serverName = ProtobufUtil.toServerName(data.getCrashedServer());
    }
  }
}
