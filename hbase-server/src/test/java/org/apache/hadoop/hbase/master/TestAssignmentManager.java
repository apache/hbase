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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoordinatedStateException;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.CoordinatedStateManagerFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaMockingUtil;
import org.apache.hadoop.hbase.RegionException;
import org.apache.hadoop.hbase.RegionTransition;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.HConnectionTestingUtility;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.coordination.BaseCoordinatedStateManager;
import org.apache.hadoop.hbase.coordination.OpenRegionCoordination;
import org.apache.hadoop.hbase.coordination.ZkCoordinatedStateManager;
import org.apache.hadoop.hbase.coordination.ZkOpenRegionCoordination;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.executor.ExecutorType;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.TableLockManager.NullTableLockManager;
import org.apache.hadoop.hbase.master.balancer.LoadBalancerFactory;
import org.apache.hadoop.hbase.master.balancer.SimpleLoadBalancer;
import org.apache.hadoop.hbase.master.handler.EnableTableHandler;
import org.apache.hadoop.hbase.master.handler.ServerShutdownHandler;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanResponse;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.SplitLogTask.RecoveryMode;
import org.apache.hadoop.hbase.regionserver.RegionOpeningState;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Watcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * Test {@link AssignmentManager}
 */
@Category(MediumTests.class)
public class TestAssignmentManager {
  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();
  private static final ServerName SERVERNAME_A =
      ServerName.valueOf("example.org", 1234, 5678);
  private static final ServerName SERVERNAME_B =
      ServerName.valueOf("example.org", 0, 5678);
  private static final HRegionInfo REGIONINFO =
    new HRegionInfo(TableName.valueOf("t"),
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_START_ROW);
  private static int assignmentCount;
  private static boolean enabling = false;

  // Mocked objects or; get redone for each test.
  private MasterServices server;
  private ServerManager serverManager;
  private ZooKeeperWatcher watcher;
  private CoordinatedStateManager cp;
  private MetaTableLocator mtl;
  private LoadBalancer balancer;
  private HMaster master;
  private ClusterConnection connection;
  private TableStateManager tableStateManager;

  @BeforeClass
  public static void beforeClass() throws Exception {
    HTU.getConfiguration().setBoolean("hbase.assignment.usezk", true);
    HTU.startMiniZKCluster();
  }

  @AfterClass
  public static void afterClass() throws IOException {
    HTU.shutdownMiniZKCluster();
  }

  @Before
  public void before() throws ZooKeeperConnectionException, IOException {
    // TODO: Make generic versions of what we do below and put up in a mocking
    // utility class or move up into HBaseTestingUtility.

    // Mock a Server.  Have it return a legit Configuration and ZooKeeperWatcher.
    // If abort is called, be sure to fail the test (don't just swallow it
    // silently as is mockito default).
    this.tableStateManager = new TableStateManager.InMemoryTableStateManager();
    this.server = Mockito.mock(MasterServices.class);
    Mockito.when(server.getServerName()).thenReturn(ServerName.valueOf("master,1,1"));
    Mockito.when(server.getConfiguration()).thenReturn(HTU.getConfiguration());
    Mockito.when(server.getTableStateManager()).thenReturn(tableStateManager);
    this.watcher =
      new ZooKeeperWatcher(HTU.getConfiguration(), "mockedServer", this.server, true);
    Mockito.when(server.getZooKeeper()).thenReturn(this.watcher);
    Mockito.doThrow(new RuntimeException("Aborted")).
      when(server).abort(Mockito.anyString(), (Throwable)Mockito.anyObject());

    cp = new ZkCoordinatedStateManager();
    cp.initialize(this.server);
    cp.start();

    mtl = Mockito.mock(MetaTableLocator.class);

    Mockito.when(server.getCoordinatedStateManager()).thenReturn(cp);
    Mockito.when(server.getMetaTableLocator()).thenReturn(mtl);

    // Get a connection w/ mocked up common methods.
    this.connection =
      (ClusterConnection)HConnectionTestingUtility.getMockedConnection(HTU.getConfiguration());

    // Make it so we can get a catalogtracker from servermanager.. .needed
    // down in guts of server shutdown handler.
    Mockito.when(server.getConnection()).thenReturn(connection);

    // Mock a ServerManager.  Say server SERVERNAME_{A,B} are online.  Also
    // make it so if close or open, we return 'success'.
    this.serverManager = Mockito.mock(ServerManager.class);
    Mockito.when(this.serverManager.isServerOnline(SERVERNAME_A)).thenReturn(true);
    Mockito.when(this.serverManager.isServerOnline(SERVERNAME_B)).thenReturn(true);
    Mockito.when(this.serverManager.getDeadServers()).thenReturn(new DeadServer());
    final Map<ServerName, ServerLoad> onlineServers = new HashMap<ServerName, ServerLoad>();
    onlineServers.put(SERVERNAME_B, ServerLoad.EMPTY_SERVERLOAD);
    onlineServers.put(SERVERNAME_A, ServerLoad.EMPTY_SERVERLOAD);
    Mockito.when(this.serverManager.getOnlineServersList()).thenReturn(
        new ArrayList<ServerName>(onlineServers.keySet()));
    Mockito.when(this.serverManager.getOnlineServers()).thenReturn(onlineServers);

    List<ServerName> avServers = new ArrayList<ServerName>();
    avServers.addAll(onlineServers.keySet());
    Mockito.when(this.serverManager.createDestinationServersList()).thenReturn(avServers);
    Mockito.when(this.serverManager.createDestinationServersList(null)).thenReturn(avServers);

    Mockito.when(this.serverManager.sendRegionClose(SERVERNAME_A, REGIONINFO, -1)).
      thenReturn(true);
    Mockito.when(this.serverManager.sendRegionClose(SERVERNAME_B, REGIONINFO, -1)).
      thenReturn(true);
    // Ditto on open.
    Mockito.when(this.serverManager.sendRegionOpen(SERVERNAME_A, REGIONINFO, -1, null)).
      thenReturn(RegionOpeningState.OPENED);
    Mockito.when(this.serverManager.sendRegionOpen(SERVERNAME_B, REGIONINFO, -1, null)).
      thenReturn(RegionOpeningState.OPENED);
    this.master = Mockito.mock(HMaster.class);

    Mockito.when(this.master.getServerManager()).thenReturn(serverManager);
  }

  @After public void after() throws KeeperException, IOException {
    if (this.watcher != null) {
      // Clean up all znodes
      ZKAssign.deleteAllNodes(this.watcher);
      this.watcher.close();
      this.cp.stop();
    }
    if (this.connection != null) this.connection.close();
  }

  /**
   * Test a balance going on at same time as a master failover
   *
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   * @throws DeserializationException
   */
  @Test(timeout = 60000)
  public void testBalanceOnMasterFailoverScenarioWithOpenedNode()
      throws IOException, KeeperException, InterruptedException, ServiceException,
      DeserializationException, CoordinatedStateException {
    AssignmentManagerWithExtrasForTesting am =
        setUpMockedAssignmentManager(this.server, this.serverManager,
            tableStateManager);
    try {
      createRegionPlanAndBalance(am, SERVERNAME_A, SERVERNAME_B, REGIONINFO);
      startFakeFailedOverMasterAssignmentManager(am, this.watcher);
      while (!am.processRITInvoked) Thread.sleep(1);
      // As part of the failover cleanup, the balancing region plan is removed.
      // So a random server will be used to open the region. For testing purpose,
      // let's assume it is going to open on server b:
      am.addPlan(REGIONINFO.getEncodedName(), new RegionPlan(REGIONINFO, null, SERVERNAME_B));

      Mocking.waitForRegionFailedToCloseAndSetToPendingClose(am, REGIONINFO);

      // Now fake the region closing successfully over on the regionserver; the
      // regionserver will have set the region in CLOSED state. This will
      // trigger callback into AM. The below zk close call is from the RS close
      // region handler duplicated here because its down deep in a private
      // method hard to expose.
      int versionid =
        ZKAssign.transitionNodeClosed(this.watcher, REGIONINFO, SERVERNAME_A, -1);
      assertNotSame(versionid, -1);
      Mocking.waitForRegionPendingOpenInRIT(am, REGIONINFO.getEncodedName());

      // Get current versionid else will fail on transition from OFFLINE to
      // OPENING below
      versionid = ZKAssign.getVersion(this.watcher, REGIONINFO);
      assertNotSame(-1, versionid);
      // This uglyness below is what the openregionhandler on RS side does.
      versionid = ZKAssign.transitionNode(server.getZooKeeper(), REGIONINFO,
        SERVERNAME_B, EventType.M_ZK_REGION_OFFLINE,
        EventType.RS_ZK_REGION_OPENING, versionid);
      assertNotSame(-1, versionid);
      // Move znode from OPENING to OPENED as RS does on successful open.
      versionid = ZKAssign.transitionNodeOpened(this.watcher, REGIONINFO,
        SERVERNAME_B, versionid);
      assertNotSame(-1, versionid);
      am.gate.set(false);
      // Block here until our znode is cleared or until this test times out.
      ZKAssign.blockUntilNoRIT(watcher);
    } finally {
      am.getExecutorService().shutdown();
      am.shutdown();
    }
  }

  @Test(timeout = 60000)
  public void testBalanceOnMasterFailoverScenarioWithClosedNode()
      throws IOException, KeeperException, InterruptedException, ServiceException,
        DeserializationException, CoordinatedStateException {
    AssignmentManagerWithExtrasForTesting am =
        setUpMockedAssignmentManager(this.server, this.serverManager,
            tableStateManager);
    try {
      createRegionPlanAndBalance(am, SERVERNAME_A, SERVERNAME_B, REGIONINFO);
      startFakeFailedOverMasterAssignmentManager(am, this.watcher);
      while (!am.processRITInvoked) Thread.sleep(1);
      // As part of the failover cleanup, the balancing region plan is removed.
      // So a random server will be used to open the region. For testing purpose,
      // let's assume it is going to open on server b:
      am.addPlan(REGIONINFO.getEncodedName(), new RegionPlan(REGIONINFO, null, SERVERNAME_B));

      Mocking.waitForRegionFailedToCloseAndSetToPendingClose(am, REGIONINFO);

      // Now fake the region closing successfully over on the regionserver; the
      // regionserver will have set the region in CLOSED state. This will
      // trigger callback into AM. The below zk close call is from the RS close
      // region handler duplicated here because its down deep in a private
      // method hard to expose.
      int versionid =
        ZKAssign.transitionNodeClosed(this.watcher, REGIONINFO, SERVERNAME_A, -1);
      assertNotSame(versionid, -1);
      am.gate.set(false);
      Mocking.waitForRegionPendingOpenInRIT(am, REGIONINFO.getEncodedName());

      // Get current versionid else will fail on transition from OFFLINE to
      // OPENING below
      versionid = ZKAssign.getVersion(this.watcher, REGIONINFO);
      assertNotSame(-1, versionid);
      // This uglyness below is what the openregionhandler on RS side does.
      versionid = ZKAssign.transitionNode(server.getZooKeeper(), REGIONINFO,
          SERVERNAME_B, EventType.M_ZK_REGION_OFFLINE,
          EventType.RS_ZK_REGION_OPENING, versionid);
      assertNotSame(-1, versionid);
      // Move znode from OPENING to OPENED as RS does on successful open.
      versionid = ZKAssign.transitionNodeOpened(this.watcher, REGIONINFO,
          SERVERNAME_B, versionid);
      assertNotSame(-1, versionid);

      // Block here until our znode is cleared or until this test timesout.
      ZKAssign.blockUntilNoRIT(watcher);
    } finally {
      am.getExecutorService().shutdown();
      am.shutdown();
    }
  }

  @Test(timeout = 60000)
  public void testBalanceOnMasterFailoverScenarioWithOfflineNode()
      throws IOException, KeeperException, InterruptedException, ServiceException,
      DeserializationException, CoordinatedStateException {
    AssignmentManagerWithExtrasForTesting am =
        setUpMockedAssignmentManager(this.server, this.serverManager,
            tableStateManager);
    try {
      createRegionPlanAndBalance(am, SERVERNAME_A, SERVERNAME_B, REGIONINFO);
      startFakeFailedOverMasterAssignmentManager(am, this.watcher);
      while (!am.processRITInvoked) Thread.sleep(1);
      // As part of the failover cleanup, the balancing region plan is removed.
      // So a random server will be used to open the region. For testing purpose,
      // let's assume it is going to open on server b:
      am.addPlan(REGIONINFO.getEncodedName(), new RegionPlan(REGIONINFO, null, SERVERNAME_B));

      Mocking.waitForRegionFailedToCloseAndSetToPendingClose(am, REGIONINFO);

      // Now fake the region closing successfully over on the regionserver; the
      // regionserver will have set the region in CLOSED state. This will
      // trigger callback into AM. The below zk close call is from the RS close
      // region handler duplicated here because its down deep in a private
      // method hard to expose.
      int versionid =
        ZKAssign.transitionNodeClosed(this.watcher, REGIONINFO, SERVERNAME_A, -1);
      assertNotSame(versionid, -1);
      Mocking.waitForRegionPendingOpenInRIT(am, REGIONINFO.getEncodedName());

      am.gate.set(false);
      // Get current versionid else will fail on transition from OFFLINE to
      // OPENING below
      versionid = ZKAssign.getVersion(this.watcher, REGIONINFO);
      assertNotSame(-1, versionid);
      // This uglyness below is what the openregionhandler on RS side does.
      versionid = ZKAssign.transitionNode(server.getZooKeeper(), REGIONINFO,
          SERVERNAME_B, EventType.M_ZK_REGION_OFFLINE,
          EventType.RS_ZK_REGION_OPENING, versionid);
      assertNotSame(-1, versionid);
      // Move znode from OPENING to OPENED as RS does on successful open.
      versionid = ZKAssign.transitionNodeOpened(this.watcher, REGIONINFO,
          SERVERNAME_B, versionid);
      assertNotSame(-1, versionid);
      // Block here until our znode is cleared or until this test timesout.
      ZKAssign.blockUntilNoRIT(watcher);
    } finally {
      am.getExecutorService().shutdown();
      am.shutdown();
    }
  }

  private void createRegionPlanAndBalance(
      final AssignmentManager am, final ServerName from,
      final ServerName to, final HRegionInfo hri) throws RegionException {
    // Call the balance function but fake the region being online first at
    // servername from.
    am.regionOnline(hri, from);
    // Balance region from 'from' to 'to'. It calls unassign setting CLOSING state
    // up in zk.  Create a plan and balance
    am.balance(new RegionPlan(hri, from, to));
  }

  /**
   * Tests AssignmentManager balance function.  Runs a balance moving a region
   * from one server to another mocking regionserver responding over zk.
   * @throws IOException
   * @throws KeeperException
   * @throws DeserializationException
   */
  @Test (timeout=180000)
  public void testBalance() throws IOException, KeeperException, DeserializationException,
      InterruptedException, CoordinatedStateException {
    // Create and startup an executor.  This is used by AssignmentManager
    // handling zk callbacks.
    ExecutorService executor = startupMasterExecutor("testBalanceExecutor");

    // We need a mocked catalog tracker.
    LoadBalancer balancer = LoadBalancerFactory.getLoadBalancer(server
        .getConfiguration());
    // Create an AM.
    AssignmentManager am = new AssignmentManager(this.server,
        this.serverManager, balancer, executor, null, master.getTableLockManager(),
        tableStateManager);
    am.failoverCleanupDone.set(true);
    try {
      // Make sure our new AM gets callbacks; once registered, can't unregister.
      // Thats ok because we make a new zk watcher for each test.
      this.watcher.registerListenerFirst(am);
      // Call the balance function but fake the region being online first at
      // SERVERNAME_A.  Create a balance plan.
      am.regionOnline(REGIONINFO, SERVERNAME_A);
      // Balance region from A to B.
      RegionPlan plan = new RegionPlan(REGIONINFO, SERVERNAME_A, SERVERNAME_B);
      am.balance(plan);

      RegionStates regionStates = am.getRegionStates();
      // Must be failed to close since the server is fake
      assertTrue(regionStates.isRegionInTransition(REGIONINFO)
        && regionStates.isRegionInState(REGIONINFO, State.FAILED_CLOSE));
      // Move it back to pending_close
      regionStates.updateRegionState(REGIONINFO, State.PENDING_CLOSE);

      // Now fake the region closing successfully over on the regionserver; the
      // regionserver will have set the region in CLOSED state.  This will
      // trigger callback into AM. The below zk close call is from the RS close
      // region handler duplicated here because its down deep in a private
      // method hard to expose.
      int versionid =
        ZKAssign.transitionNodeClosed(this.watcher, REGIONINFO, SERVERNAME_A, -1);
      assertNotSame(versionid, -1);
      // AM is going to notice above CLOSED and queue up a new assign.  The
      // assign will go to open the region in the new location set by the
      // balancer.  The zk node will be OFFLINE waiting for regionserver to
      // transition it through OPENING, OPENED.  Wait till we see the OFFLINE
      // zk node before we proceed.
      Mocking.waitForRegionPendingOpenInRIT(am, REGIONINFO.getEncodedName());

      // Get current versionid else will fail on transition from OFFLINE to OPENING below
      versionid = ZKAssign.getVersion(this.watcher, REGIONINFO);
      assertNotSame(-1, versionid);
      // This uglyness below is what the openregionhandler on RS side does.
      versionid = ZKAssign.transitionNode(server.getZooKeeper(), REGIONINFO,
        SERVERNAME_B, EventType.M_ZK_REGION_OFFLINE,
        EventType.RS_ZK_REGION_OPENING, versionid);
      assertNotSame(-1, versionid);
      // Move znode from OPENING to OPENED as RS does on successful open.
      versionid =
        ZKAssign.transitionNodeOpened(this.watcher, REGIONINFO, SERVERNAME_B, versionid);
      assertNotSame(-1, versionid);
      // Wait on the handler removing the OPENED znode.
      while(regionStates.isRegionInTransition(REGIONINFO)) Threads.sleep(1);
    } finally {
      executor.shutdown();
      am.shutdown();
      // Clean up all znodes
      ZKAssign.deleteAllNodes(this.watcher);
    }
  }

  /**
   * Run a simple server shutdown handler.
   * @throws KeeperException
   * @throws IOException
   */
  @Test (timeout=180000)
  public void testShutdownHandler()
      throws KeeperException, IOException, CoordinatedStateException, ServiceException {
    // Create and startup an executor.  This is used by AssignmentManager
    // handling zk callbacks.
    ExecutorService executor = startupMasterExecutor("testShutdownHandler");

    // Create an AM.
    AssignmentManagerWithExtrasForTesting am = setUpMockedAssignmentManager(
        this.server, this.serverManager,
        tableStateManager);
    try {
      processServerShutdownHandler(am, false);
    } finally {
      executor.shutdown();
      am.shutdown();
      // Clean up all znodes
      ZKAssign.deleteAllNodes(this.watcher);
    }
  }

  /**
   * To test closed region handler to remove rit and delete corresponding znode
   * if region in pending close or closing while processing shutdown of a region
   * server.(HBASE-5927).
   *
   * @throws KeeperException
   * @throws IOException
   * @throws ServiceException
   */
  @Test (timeout=180000)
  public void testSSHWhenDisableTableInProgress() throws KeeperException, IOException,
    CoordinatedStateException, ServiceException {
    testCaseWithPartiallyDisabledState(TableState.State.DISABLING);
    testCaseWithPartiallyDisabledState(TableState.State.DISABLED);
  }


  /**
   * To test if the split region is removed from RIT if the region was in SPLITTING state but the RS
   * has actually completed the splitting in hbase:meta but went down. See HBASE-6070 and also HBASE-5806
   *
   * @throws KeeperException
   * @throws IOException
   */
  @Test (timeout=180000)
  public void testSSHWhenSplitRegionInProgress() throws KeeperException, IOException, Exception {
    // true indicates the region is split but still in RIT
    testCaseWithSplitRegionPartial(true);
    // false indicate the region is not split
    testCaseWithSplitRegionPartial(false);
  }

  private void testCaseWithSplitRegionPartial(boolean regionSplitDone) throws KeeperException,
      IOException, InterruptedException,
    CoordinatedStateException, ServiceException {
    // Create and startup an executor. This is used by AssignmentManager
    // handling zk callbacks.
    ExecutorService executor = startupMasterExecutor("testSSHWhenSplitRegionInProgress");
    // We need a mocked catalog tracker.
    ZKAssign.deleteAllNodes(this.watcher);

    // Create an AM.
    AssignmentManagerWithExtrasForTesting am = setUpMockedAssignmentManager(
        this.server, this.serverManager,
        tableStateManager);
    // adding region to regions and servers maps.
    am.regionOnline(REGIONINFO, SERVERNAME_A);
    // adding region in pending close.
    am.getRegionStates().updateRegionState(
      REGIONINFO, State.SPLITTING, SERVERNAME_A);
    am.getTableStateManager().setTableState(REGIONINFO.getTable(),
      TableState.State.ENABLED);
    RegionTransition data = RegionTransition.createRegionTransition(EventType.RS_ZK_REGION_SPLITTING,
        REGIONINFO.getRegionName(), SERVERNAME_A);
    String node = ZKAssign.getNodeName(this.watcher, REGIONINFO.getEncodedName());
    // create znode in M_ZK_REGION_CLOSING state.
    ZKUtil.createAndWatch(this.watcher, node, data.toByteArray());

    try {
      processServerShutdownHandler(am, regionSplitDone);
      // check znode deleted or not.
      // In both cases the znode should be deleted.

      if (regionSplitDone) {
        assertFalse("Region state of region in SPLITTING should be removed from rit.",
            am.getRegionStates().isRegionsInTransition());
      } else {
        while (!am.assignInvoked) {
          Thread.sleep(1);
        }
        assertTrue("Assign should be invoked.", am.assignInvoked);
      }
    } finally {
      REGIONINFO.setOffline(false);
      REGIONINFO.setSplit(false);
      executor.shutdown();
      am.shutdown();
      // Clean up all znodes
      ZKAssign.deleteAllNodes(this.watcher);
    }
  }

  private void testCaseWithPartiallyDisabledState(TableState.State state) throws KeeperException,
      IOException, CoordinatedStateException, ServiceException {
    // Create and startup an executor. This is used by AssignmentManager
    // handling zk callbacks.
    ExecutorService executor = startupMasterExecutor("testSSHWhenDisableTableInProgress");
    LoadBalancer balancer = LoadBalancerFactory.getLoadBalancer(server.getConfiguration());
    ZKAssign.deleteAllNodes(this.watcher);

    // Create an AM.
    AssignmentManager am = new AssignmentManager(this.server,
        this.serverManager, balancer, executor, null, master.getTableLockManager(),
        tableStateManager);
    // adding region to regions and servers maps.
    am.regionOnline(REGIONINFO, SERVERNAME_A);
    // adding region in pending close.
    am.getRegionStates().updateRegionState(REGIONINFO, State.PENDING_CLOSE);
    if (state == TableState.State.DISABLING) {
      am.getTableStateManager().setTableState(REGIONINFO.getTable(),
        TableState.State.DISABLING);
    } else {
      am.getTableStateManager().setTableState(REGIONINFO.getTable(),
        TableState.State.DISABLED);
    }
    RegionTransition data = RegionTransition.createRegionTransition(EventType.M_ZK_REGION_CLOSING,
        REGIONINFO.getRegionName(), SERVERNAME_A);
    // RegionTransitionData data = new
    // RegionTransitionData(EventType.M_ZK_REGION_CLOSING,
    // REGIONINFO.getRegionName(), SERVERNAME_A);
    String node = ZKAssign.getNodeName(this.watcher, REGIONINFO.getEncodedName());
    // create znode in M_ZK_REGION_CLOSING state.
    ZKUtil.createAndWatch(this.watcher, node, data.toByteArray());

    try {
      processServerShutdownHandler(am, false);
      // check znode deleted or not.
      // In both cases the znode should be deleted.
      assertTrue("The znode should be deleted.", ZKUtil.checkExists(this.watcher, node) == -1);
      // check whether in rit or not. In the DISABLING case also the below
      // assert will be true but the piece of code added for HBASE-5927 will not
      // do that.
      if (state == TableState.State.DISABLED) {
        assertFalse("Region state of region in pending close should be removed from rit.",
            am.getRegionStates().isRegionsInTransition());
      }
    } finally {
      am.setEnabledTable(REGIONINFO.getTable());
      executor.shutdown();
      am.shutdown();
      // Clean up all znodes
      ZKAssign.deleteAllNodes(this.watcher);
    }
  }

  private void processServerShutdownHandler(AssignmentManager am, boolean splitRegion)
      throws IOException, ServiceException {
    // Make sure our new AM gets callbacks; once registered, can't unregister.
    // Thats ok because we make a new zk watcher for each test.
    this.watcher.registerListenerFirst(am);

    // Need to set up a fake scan of meta for the servershutdown handler
    // Make an RS Interface implementation.  Make it so a scanner can go against it.
    ClientProtos.ClientService.BlockingInterface implementation =
      Mockito.mock(ClientProtos.ClientService.BlockingInterface.class);
    // Get a meta row result that has region up on SERVERNAME_A

    Result r;
    if (splitRegion) {
      r = MetaMockingUtil.getMetaTableRowResultAsSplitRegion(REGIONINFO, SERVERNAME_A);
    } else {
      r = MetaMockingUtil.getMetaTableRowResult(REGIONINFO, SERVERNAME_A);
    }

    final ScanResponse.Builder builder = ScanResponse.newBuilder();
    builder.setMoreResults(true);
    builder.addCellsPerResult(r.size());
    final List<CellScannable> cellScannables = new ArrayList<CellScannable>(1);
    cellScannables.add(r);
    Mockito.when(implementation.scan(
      (RpcController)Mockito.any(), (ScanRequest)Mockito.any())).
      thenAnswer(new Answer<ScanResponse>() {
          @Override
          public ScanResponse answer(InvocationOnMock invocation) throws Throwable {
            PayloadCarryingRpcController controller = (PayloadCarryingRpcController) invocation
                .getArguments()[0];
            if (controller != null) {
              controller.setCellScanner(CellUtil.createCellScanner(cellScannables));
            }
            return builder.build();
          }
      });

    // Get a connection w/ mocked up common methods.
    ClusterConnection connection =
      HConnectionTestingUtility.getMockedConnectionAndDecorate(HTU.getConfiguration(),
          null, implementation, SERVERNAME_B, REGIONINFO);
    // These mocks were done up when all connections were managed.  World is different now we
    // moved to unmanaged connections.  It messes up the intercepts done in these tests.
    // Just mark connections as marked and then down in MetaTableAccessor, it will go the path
    // that picks up the above mocked up 'implementation' so 'scans' of meta return the expected
    // result.  Redo in new realm of unmanaged connections.
    Mockito.when(connection.isManaged()).thenReturn(true);
    try {
      // Make it so we can get a catalogtracker from servermanager.. .needed
      // down in guts of server shutdown handler.
      Mockito.when(this.server.getConnection()).thenReturn(connection);

      // Now make a server shutdown handler instance and invoke process.
      // Have it that SERVERNAME_A died.
      DeadServer deadServers = new DeadServer();
      deadServers.add(SERVERNAME_A);
      // I need a services instance that will return the AM
      MasterFileSystem fs = Mockito.mock(MasterFileSystem.class);
      Mockito.doNothing().when(fs).setLogRecoveryMode();
      Mockito.when(fs.getLogRecoveryMode()).thenReturn(RecoveryMode.LOG_REPLAY);
      MasterServices services = Mockito.mock(MasterServices.class);
      Mockito.when(services.getAssignmentManager()).thenReturn(am);
      Mockito.when(services.getServerManager()).thenReturn(this.serverManager);
      Mockito.when(services.getZooKeeper()).thenReturn(this.watcher);
      Mockito.when(services.getMasterFileSystem()).thenReturn(fs);
      Mockito.when(services.getConnection()).thenReturn(connection);
      Configuration conf = server.getConfiguration();
      Mockito.when(services.getConfiguration()).thenReturn(conf);
      ServerShutdownHandler handler = new ServerShutdownHandler(this.server,
          services, deadServers, SERVERNAME_A, false);
      am.failoverCleanupDone.set(true);
      handler.process();
      // The region in r will have been assigned.  It'll be up in zk as unassigned.
    } finally {
      if (connection != null) connection.close();
    }
  }

  /**
   * Create and startup executor pools. Start same set as master does (just
   * run a few less).
   * @param name Name to give our executor
   * @return Created executor (be sure to call shutdown when done).
   */
  private ExecutorService startupMasterExecutor(final String name) {
    // TODO: Move up into HBaseTestingUtility?  Generally useful.
    ExecutorService executor = new ExecutorService(name);
    executor.startExecutorService(ExecutorType.MASTER_OPEN_REGION, 3);
    executor.startExecutorService(ExecutorType.MASTER_CLOSE_REGION, 3);
    executor.startExecutorService(ExecutorType.MASTER_SERVER_OPERATIONS, 3);
    executor.startExecutorService(ExecutorType.MASTER_META_SERVER_OPERATIONS, 3);
    return executor;
  }

  @Test (timeout=180000)
  public void testUnassignWithSplitAtSameTime() throws KeeperException,
      IOException, CoordinatedStateException {
    // Region to use in test.
    final HRegionInfo hri = HRegionInfo.FIRST_META_REGIONINFO;
    // First amend the servermanager mock so that when we do send close of the
    // first meta region on SERVERNAME_A, it will return true rather than
    // default null.
    Mockito.when(this.serverManager.sendRegionClose(SERVERNAME_A, hri, -1)).thenReturn(true);
    // Need a mocked catalog tracker.
    LoadBalancer balancer = LoadBalancerFactory.getLoadBalancer(server
        .getConfiguration());
    // Create an AM.
    AssignmentManager am = new AssignmentManager(this.server,
        this.serverManager, balancer, null, null, master.getTableLockManager(),
        tableStateManager);
    try {
      // First make sure my mock up basically works.  Unassign a region.
      unassign(am, SERVERNAME_A, hri);
      // This delete will fail if the previous unassign did wrong thing.
      ZKAssign.deleteClosingNode(this.watcher, hri, SERVERNAME_A);
      // Now put a SPLITTING region in the way.  I don't have to assert it
      // go put in place.  This method puts it in place then asserts it still
      // owns it by moving state from SPLITTING to SPLITTING.
      int version = createNodeSplitting(this.watcher, hri, SERVERNAME_A);
      // Now, retry the unassign with the SPLTTING in place.  It should just
      // complete without fail; a sort of 'silent' recognition that the
      // region to unassign has been split and no longer exists: TOOD: what if
      // the split fails and the parent region comes back to life?
      unassign(am, SERVERNAME_A, hri);
      // This transition should fail if the znode has been messed with.
      ZKAssign.transitionNode(this.watcher, hri, SERVERNAME_A,
        EventType.RS_ZK_REGION_SPLITTING, EventType.RS_ZK_REGION_SPLITTING, version);
      assertFalse(am.getRegionStates().isRegionInTransition(hri));
    } finally {
      am.shutdown();
    }
  }

  /**
   * Tests the processDeadServersAndRegionsInTransition should not fail with NPE
   * when it failed to get the children. Let's abort the system in this
   * situation
   * @throws ServiceException
   */
  @Test(timeout = 60000)
  public void testProcessDeadServersAndRegionsInTransitionShouldNotFailWithNPE()
      throws IOException, KeeperException, CoordinatedStateException,
      InterruptedException, ServiceException {
    final RecoverableZooKeeper recoverableZk = Mockito
        .mock(RecoverableZooKeeper.class);
    AssignmentManagerWithExtrasForTesting am = setUpMockedAssignmentManager(
        this.server, this.serverManager,
        tableStateManager);
    Watcher zkw = new ZooKeeperWatcher(HBaseConfiguration.create(), "unittest",
        null) {
      @Override
      public RecoverableZooKeeper getRecoverableZooKeeper() {
        return recoverableZk;
      }
    };
    ((ZooKeeperWatcher) zkw).registerListener(am);
    Mockito.doThrow(new InterruptedException()).when(recoverableZk)
        .getChildren("/hbase/region-in-transition", null);
    am.setWatcher((ZooKeeperWatcher) zkw);
    try {
      am.processDeadServersAndRegionsInTransition(null);
      fail("Expected to abort");
    } catch (NullPointerException e) {
      fail("Should not throw NPE");
    } catch (RuntimeException e) {
      assertEquals("Aborted", e.getLocalizedMessage());
    } finally {
      am.shutdown();
    }
  }
  /**
   * TestCase verifies that the regionPlan is updated whenever a region fails to open
   * and the master tries to process RS_ZK_FAILED_OPEN state.(HBASE-5546).
   */
  @Test(timeout = 60000)
  public void testRegionPlanIsUpdatedWhenRegionFailsToOpen() throws IOException, KeeperException,
      ServiceException, InterruptedException, CoordinatedStateException {
    this.server.getConfiguration().setClass(
      HConstants.HBASE_MASTER_LOADBALANCER_CLASS, MockedLoadBalancer.class,
      LoadBalancer.class);
    AssignmentManagerWithExtrasForTesting am = setUpMockedAssignmentManager(
        this.server, this.serverManager,
        tableStateManager);
    try {
      // Boolean variable used for waiting until randomAssignment is called and
      // new
      // plan is generated.
      AtomicBoolean gate = new AtomicBoolean(false);
      if (balancer instanceof MockedLoadBalancer) {
        ((MockedLoadBalancer) balancer).setGateVariable(gate);
      }
      ZKAssign.createNodeOffline(this.watcher, REGIONINFO, SERVERNAME_A);
      int v = ZKAssign.getVersion(this.watcher, REGIONINFO);
      ZKAssign.transitionNode(this.watcher, REGIONINFO, SERVERNAME_A,
          EventType.M_ZK_REGION_OFFLINE, EventType.RS_ZK_REGION_FAILED_OPEN, v);
      String path = ZKAssign.getNodeName(this.watcher, REGIONINFO
          .getEncodedName());
      am.getRegionStates().updateRegionState(
        REGIONINFO, State.OPENING, SERVERNAME_A);
      // a dummy plan inserted into the regionPlans. This plan is cleared and
      // new one is formed
      am.regionPlans.put(REGIONINFO.getEncodedName(), new RegionPlan(
          REGIONINFO, null, SERVERNAME_A));
      RegionPlan regionPlan = am.regionPlans.get(REGIONINFO.getEncodedName());
      List<ServerName> serverList = new ArrayList<ServerName>(2);
      serverList.add(SERVERNAME_B);
      Mockito.when(
          this.serverManager.createDestinationServersList(SERVERNAME_A))
          .thenReturn(serverList);
      am.nodeDataChanged(path);
      // here we are waiting until the random assignment in the load balancer is
      // called.
      while (!gate.get()) {
        Thread.sleep(10);
      }
      // new region plan may take some time to get updated after random
      // assignment is called and
      // gate is set to true.
      RegionPlan newRegionPlan = am.regionPlans
          .get(REGIONINFO.getEncodedName());
      while (newRegionPlan == null) {
        Thread.sleep(10);
        newRegionPlan = am.regionPlans.get(REGIONINFO.getEncodedName());
      }
      // the new region plan created may contain the same RS as destination but
      // it should
      // be new plan.
      assertNotSame("Same region plan should not come", regionPlan,
          newRegionPlan);
      assertTrue("Destination servers should be different.", !(regionPlan
          .getDestination().equals(newRegionPlan.getDestination())));

      Mocking.waitForRegionPendingOpenInRIT(am, REGIONINFO.getEncodedName());
    } finally {
      this.server.getConfiguration().setClass(
          HConstants.HBASE_MASTER_LOADBALANCER_CLASS, SimpleLoadBalancer.class,
          LoadBalancer.class);
      am.getExecutorService().shutdown();
      am.shutdown();
    }
  }

  /**
   * Mocked load balancer class used in the testcase to make sure that the testcase waits until
   * random assignment is called and the gate variable is set to true.
   */
  public static class MockedLoadBalancer extends SimpleLoadBalancer {
    private AtomicBoolean gate;

    public void setGateVariable(AtomicBoolean gate) {
      this.gate = gate;
    }

    @Override
    public ServerName randomAssignment(HRegionInfo regionInfo, List<ServerName> servers) {
      ServerName randomServerName = super.randomAssignment(regionInfo, servers);
      this.gate.set(true);
      return randomServerName;
    }

    @Override
    public Map<ServerName, List<HRegionInfo>> retainAssignment(
        Map<HRegionInfo, ServerName> regions, List<ServerName> servers) {
      this.gate.set(true);
      return super.retainAssignment(regions, servers);
    }
  }

  /**
   * Test the scenario when the master is in failover and trying to process a
   * region which is in Opening state on a dead RS. Master will force offline the
   * region and put it in transition. AM relies on SSH to reassign it.
   */
  @Test(timeout = 60000)
  public void testRegionInOpeningStateOnDeadRSWhileMasterFailover() throws IOException,
      KeeperException, ServiceException, CoordinatedStateException, InterruptedException {
    AssignmentManagerWithExtrasForTesting am = setUpMockedAssignmentManager(
        this.server, this.serverManager,
        tableStateManager);
    ZKAssign.createNodeOffline(this.watcher, REGIONINFO, SERVERNAME_A);
    int version = ZKAssign.getVersion(this.watcher, REGIONINFO);
    ZKAssign.transitionNode(this.watcher, REGIONINFO, SERVERNAME_A, EventType.M_ZK_REGION_OFFLINE,
        EventType.RS_ZK_REGION_OPENING, version);
    RegionTransition rt = RegionTransition.createRegionTransition(EventType.RS_ZK_REGION_OPENING,
        REGIONINFO.getRegionName(), SERVERNAME_A, HConstants.EMPTY_BYTE_ARRAY);
    version = ZKAssign.getVersion(this.watcher, REGIONINFO);
    Mockito.when(this.serverManager.isServerOnline(SERVERNAME_A)).thenReturn(false);
    am.getRegionStates().logSplit(SERVERNAME_A); // Assume log splitting is done
    am.getRegionStates().createRegionState(REGIONINFO);
    am.gate.set(false);

    BaseCoordinatedStateManager cp = new ZkCoordinatedStateManager();
    cp.initialize(server);
    cp.start();

    OpenRegionCoordination orc = cp.getOpenRegionCoordination();
    ZkOpenRegionCoordination.ZkOpenRegionDetails zkOrd =
      new ZkOpenRegionCoordination.ZkOpenRegionDetails();
    zkOrd.setServerName(server.getServerName());
    zkOrd.setVersion(version);

    assertFalse(am.processRegionsInTransition(rt, REGIONINFO, orc, zkOrd));
    am.getTableStateManager().setTableState(REGIONINFO.getTable(), TableState.State.ENABLED);
    processServerShutdownHandler(am, false);
    // Waiting for the assignment to get completed.
    while (!am.gate.get()) {
      Thread.sleep(10);
    }
    assertTrue("The region should be assigned immediately.", null != am.regionPlans.get(REGIONINFO
        .getEncodedName()));
    am.shutdown();
  }

  /**
   * Test verifies whether assignment is skipped for regions of tables in DISABLING state during
   * clean cluster startup. See HBASE-6281.
   *
   * @throws KeeperException
   * @throws IOException
   * @throws Exception
   */
  @Test(timeout = 60000)
  public void testDisablingTableRegionsAssignmentDuringCleanClusterStartup()
      throws KeeperException, IOException, Exception {
    this.server.getConfiguration().setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
        MockedLoadBalancer.class, LoadBalancer.class);
    Mockito.when(this.serverManager.getOnlineServers()).thenReturn(
        new HashMap<ServerName, ServerLoad>(0));
    List<ServerName> destServers = new ArrayList<ServerName>(1);
    destServers.add(SERVERNAME_A);
    Mockito.when(this.serverManager.createDestinationServersList()).thenReturn(destServers);
    // To avoid cast exception in DisableTableHandler process.
    HTU.getConfiguration().setInt(HConstants.MASTER_PORT, 0);

    CoordinatedStateManager csm = CoordinatedStateManagerFactory.getCoordinatedStateManager(
      HTU.getConfiguration());
    HMaster server = new HMaster(HTU.getConfiguration(), csm);
    Whitebox.setInternalState(server, "tableStateManager", tableStateManager);

    AssignmentManagerWithExtrasForTesting am = setUpMockedAssignmentManager(server,
        this.serverManager,
        tableStateManager);

    Whitebox.setInternalState(server, "metaTableLocator", Mockito.mock(MetaTableLocator.class));

    // Make it so we can get a catalogtracker from servermanager.. .needed
    // down in guts of server shutdown handler.
    Whitebox.setInternalState(server, "clusterConnection", am.getConnection());

    AtomicBoolean gate = new AtomicBoolean(false);
    if (balancer instanceof MockedLoadBalancer) {
      ((MockedLoadBalancer) balancer).setGateVariable(gate);
    }
    try{
      // set table in disabling state.
      am.getTableStateManager().setTableState(REGIONINFO.getTable(),
        TableState.State.DISABLING);
      am.joinCluster();
      // should not call retainAssignment if we get empty regions in assignAllUserRegions.
      assertFalse(
          "Assign should not be invoked for disabling table regions during clean cluster startup.",
          gate.get());
      // need to change table state from disabling to disabled.
      assertTrue("Table should be disabled.",
          am.getTableStateManager().isTableState(REGIONINFO.getTable(),
            TableState.State.DISABLED));
    } finally {
      this.server.getConfiguration().setClass(
        HConstants.HBASE_MASTER_LOADBALANCER_CLASS, SimpleLoadBalancer.class,
        LoadBalancer.class);
      am.getTableStateManager().setTableState(REGIONINFO.getTable(),
        TableState.State.ENABLED);
      am.shutdown();
    }
  }

  /**
   * Test verifies whether all the enabling table regions assigned only once during master startup.
   *
   * @throws KeeperException
   * @throws IOException
   * @throws Exception
   */
  @Test (timeout=180000)
  public void testMasterRestartWhenTableInEnabling() throws KeeperException, IOException, Exception {
    enabling = true;
    List<ServerName> destServers = new ArrayList<ServerName>(1);
    destServers.add(SERVERNAME_A);
    Mockito.when(this.serverManager.createDestinationServersList()).thenReturn(destServers);
    Mockito.when(this.serverManager.isServerOnline(SERVERNAME_A)).thenReturn(true);
    HTU.getConfiguration().setInt(HConstants.MASTER_PORT, 0);
    CoordinatedStateManager csm = CoordinatedStateManagerFactory.getCoordinatedStateManager(
      HTU.getConfiguration());
    HMaster server = new HMaster(HTU.getConfiguration(), csm);
    Whitebox.setInternalState(server, "tableStateManager", this.tableStateManager);
    Whitebox.setInternalState(server, "serverManager", this.serverManager);
    TableDescriptors tableDescriptors = Mockito.mock(TableDescriptors.class);
    Mockito.when(tableDescriptors.get(REGIONINFO.getTable()))
        .thenReturn(new HTableDescriptor(REGIONINFO.getTable()));
    Whitebox.setInternalState(server, "tableDescriptors", tableDescriptors);
    AssignmentManagerWithExtrasForTesting am = setUpMockedAssignmentManager(server,
        this.serverManager,
        this.tableStateManager);

    Whitebox.setInternalState(server, "metaTableLocator", Mockito.mock(MetaTableLocator.class));

    // Make it so we can get a catalogtracker from servermanager.. .needed
    // down in guts of server shutdown handler.
    Whitebox.setInternalState(server, "clusterConnection", am.getConnection());

    try {
      // set table in enabling state.
      am.getTableStateManager().setTableState(REGIONINFO.getTable(),
        TableState.State.ENABLING);
      new EnableTableHandler(server, REGIONINFO.getTable(),
          am, new NullTableLockManager(), true).prepare()
          .process();
      assertEquals("Number of assignments should be 1.", 1, assignmentCount);
      assertTrue("Table should be enabled.",
          am.getTableStateManager().isTableState(REGIONINFO.getTable(),
            TableState.State.ENABLED));
    } finally {
      enabling = false;
      assignmentCount = 0;
      am.getTableStateManager().setTableState(REGIONINFO.getTable(),
        TableState.State.ENABLED);
      am.shutdown();
      ZKAssign.deleteAllNodes(this.watcher);
    }
  }

  /**
   * When a region is in transition, if the region server opening the region goes down,
   * the region assignment takes a long time normally (waiting for timeout monitor to trigger assign).
   * This test is to make sure SSH reassigns it right away.
   */
  @Test (timeout=180000)
  public void testSSHTimesOutOpeningRegionTransition()
      throws KeeperException, IOException, CoordinatedStateException, ServiceException {
    // Create an AM.
    AssignmentManagerWithExtrasForTesting am =
        setUpMockedAssignmentManager(this.server, this.serverManager,
            tableStateManager);
    // adding region in pending open.
    RegionState state = new RegionState(REGIONINFO,
      State.OPENING, System.currentTimeMillis(), SERVERNAME_A);
    am.getRegionStates().regionOnline(REGIONINFO, SERVERNAME_B);
    am.getRegionStates().regionsInTransition.put(REGIONINFO.getEncodedName(), state);
    // adding region plan
    am.regionPlans.put(REGIONINFO.getEncodedName(),
      new RegionPlan(REGIONINFO, SERVERNAME_B, SERVERNAME_A));
    am.getTableStateManager().setTableState(REGIONINFO.getTable(),
      TableState.State.ENABLED);

    try {
      am.assignInvoked = false;
      processServerShutdownHandler(am, false);
      assertTrue(am.assignInvoked);
    } finally {
      am.getRegionStates().regionsInTransition.remove(REGIONINFO.getEncodedName());
      am.regionPlans.remove(REGIONINFO.getEncodedName());
      am.shutdown();
    }
  }

  /**
   * Scenario:<ul>
   *  <li> master starts a close, and creates a znode</li>
   *  <li> it fails just at this moment, before contacting the RS</li>
   *  <li> while the second master is coming up, the targeted RS dies. But it's before ZK timeout so
   *    we don't know, and we have an exception.</li>
   *  <li> the master must handle this nicely and reassign.
   *  </ul>
   */
  @Test (timeout=180000)
  public void testClosingFailureDuringRecovery() throws Exception {

    AssignmentManagerWithExtrasForTesting am =
        setUpMockedAssignmentManager(this.server, this.serverManager,
            tableStateManager);
    ZKAssign.createNodeClosing(this.watcher, REGIONINFO, SERVERNAME_A);
    try {
      am.getRegionStates().createRegionState(REGIONINFO);

      assertFalse( am.getRegionStates().isRegionsInTransition() );

      am.processRegionInTransition(REGIONINFO.getEncodedName(), REGIONINFO);

      assertTrue( am.getRegionStates().isRegionsInTransition() );
    } finally {
      am.shutdown();
    }
  }

  /**
   * Creates a new ephemeral node in the SPLITTING state for the specified region.
   * Create it ephemeral in case regionserver dies mid-split.
   *
   * <p>Does not transition nodes from other states.  If a node already exists
   * for this region, a {@link NodeExistsException} will be thrown.
   *
   * @param zkw zk reference
   * @param region region to be created as offline
   * @param serverName server event originates from
   * @return Version of znode created.
   * @throws KeeperException
   * @throws IOException
   */
  // Copied from SplitTransaction rather than open the method over there in
  // the regionserver package.
  private static int createNodeSplitting(final ZooKeeperWatcher zkw,
      final HRegionInfo region, final ServerName serverName)
  throws KeeperException, IOException {
    RegionTransition rt =
      RegionTransition.createRegionTransition(EventType.RS_ZK_REGION_SPLITTING,
        region.getRegionName(), serverName);

    String node = ZKAssign.getNodeName(zkw, region.getEncodedName());
    if (!ZKUtil.createEphemeralNodeAndWatch(zkw, node, rt.toByteArray())) {
      throw new IOException("Failed create of ephemeral " + node);
    }
    // Transition node from SPLITTING to SPLITTING and pick up version so we
    // can be sure this znode is ours; version is needed deleting.
    return transitionNodeSplitting(zkw, region, serverName, -1);
  }

  // Copied from SplitTransaction rather than open the method over there in
  // the regionserver package.
  private static int transitionNodeSplitting(final ZooKeeperWatcher zkw,
      final HRegionInfo parent,
      final ServerName serverName, final int version)
  throws KeeperException, IOException {
    return ZKAssign.transitionNode(zkw, parent, serverName,
      EventType.RS_ZK_REGION_SPLITTING, EventType.RS_ZK_REGION_SPLITTING, version);
  }

  private void unassign(final AssignmentManager am, final ServerName sn,
      final HRegionInfo hri) throws RegionException {
    // Before I can unassign a region, I need to set it online.
    am.regionOnline(hri, sn);
    // Unassign region.
    am.unassign(hri);
  }

  /**
   * Create an {@link AssignmentManagerWithExtrasForTesting} that has mocked
   * {@link CatalogTracker} etc.
   * @param server
   * @param manager
   * @return An AssignmentManagerWithExtras with mock connections, etc.
   * @throws IOException
   * @throws KeeperException
   */
  private AssignmentManagerWithExtrasForTesting setUpMockedAssignmentManager(
      final MasterServices server,
      final ServerManager manager,
      final TableStateManager stateManager) throws IOException, KeeperException,
        ServiceException, CoordinatedStateException {
    // Make an RS Interface implementation. Make it so a scanner can go against
    // it and a get to return the single region, REGIONINFO, this test is
    // messing with. Needed when "new master" joins cluster. AM will try and
    // rebuild its list of user regions and it will also get the HRI that goes
    // with an encoded name by doing a Get on hbase:meta
    ClientProtos.ClientService.BlockingInterface ri =
      Mockito.mock(ClientProtos.ClientService.BlockingInterface.class);
    // Get a meta row result that has region up on SERVERNAME_A for REGIONINFO
    Result r = MetaMockingUtil.getMetaTableRowResult(REGIONINFO, SERVERNAME_A);
    final ScanResponse.Builder builder = ScanResponse.newBuilder();
    builder.setMoreResults(true);
    builder.addCellsPerResult(r.size());
    final List<CellScannable> rows = new ArrayList<CellScannable>(1);
    rows.add(r);
    Answer<ScanResponse> ans = new Answer<ClientProtos.ScanResponse>() {
      @Override
      public ScanResponse answer(InvocationOnMock invocation) throws Throwable {
        PayloadCarryingRpcController controller = (PayloadCarryingRpcController) invocation
            .getArguments()[0];
        if (controller != null) {
          controller.setCellScanner(CellUtil.createCellScanner(rows));
        }
        return builder.build();
      }
    };
    if (enabling) {
      Mockito.when(ri.scan((RpcController) Mockito.any(), (ScanRequest) Mockito.any()))
          .thenAnswer(ans).thenAnswer(ans).thenAnswer(ans).thenAnswer(ans).thenAnswer(ans)
          .thenReturn(ScanResponse.newBuilder().setMoreResults(false).build());
    } else {
      Mockito.when(ri.scan((RpcController) Mockito.any(), (ScanRequest) Mockito.any())).thenAnswer(
          ans);
    }
    // If a get, return the above result too for REGIONINFO
    GetResponse.Builder getBuilder = GetResponse.newBuilder();
    getBuilder.setResult(ProtobufUtil.toResult(r));
    Mockito.when(ri.get((RpcController)Mockito.any(), (GetRequest) Mockito.any())).
      thenReturn(getBuilder.build());
    // Get a connection w/ mocked up common methods.
    ClusterConnection connection = (ClusterConnection)HConnectionTestingUtility.
      getMockedConnectionAndDecorate(HTU.getConfiguration(), null,
        ri, SERVERNAME_B, REGIONINFO);
    // These mocks were done up when all connections were managed.  World is different now we
    // moved to unmanaged connections.  It messes up the intercepts done in these tests.
    // Just mark connections as marked and then down in MetaTableAccessor, it will go the path
    // that picks up the above mocked up 'implementation' so 'scans' of meta return the expected
    // result.  Redo in new realm of unmanaged connections.
    Mockito.when(connection.isManaged()).thenReturn(true);
    // Make it so we can get the connection from our mocked catalogtracker
    // Create and startup an executor. Used by AM handling zk callbacks.
    ExecutorService executor = startupMasterExecutor("mockedAMExecutor");
    this.balancer = LoadBalancerFactory.getLoadBalancer(server.getConfiguration());
    AssignmentManagerWithExtrasForTesting am = new AssignmentManagerWithExtrasForTesting(
        server, connection, manager, this.balancer, executor, new NullTableLockManager(),
        tableStateManager);
    return am;
  }

  /**
   * An {@link AssignmentManager} with some extra facility used testing
   */
  class AssignmentManagerWithExtrasForTesting extends AssignmentManager {
    // Keep a reference so can give it out below in {@link #getExecutorService}
    private final ExecutorService es;
    boolean processRITInvoked = false;
    boolean assignInvoked = false;
    AtomicBoolean gate = new AtomicBoolean(true);
    private ClusterConnection connection;

    public AssignmentManagerWithExtrasForTesting(
        final MasterServices master, ClusterConnection connection, final ServerManager serverManager,
        final LoadBalancer balancer,
        final ExecutorService service, final TableLockManager tableLockManager,
        final TableStateManager tableStateManager)
            throws KeeperException, IOException, CoordinatedStateException {
      super(master, serverManager, balancer, service, null, tableLockManager,
          tableStateManager);
      this.es = service;
      this.connection = connection;
    }

    @Override
    boolean processRegionInTransition(String encodedRegionName,
        HRegionInfo regionInfo) throws KeeperException, IOException {
      this.processRITInvoked = true;
      return super.processRegionInTransition(encodedRegionName, regionInfo);
    }

    @Override
    public void assign(HRegionInfo region, boolean setOfflineInZK, boolean forceNewPlan) {
      if (enabling) {
        assignmentCount++;
        this.regionOnline(region, SERVERNAME_A);
      } else {
        super.assign(region, setOfflineInZK, forceNewPlan);
        this.gate.set(true);
      }
    }

    @Override
    boolean assign(ServerName destination, List<HRegionInfo> regions)
        throws InterruptedException {
      if (enabling) {
        for (HRegionInfo region : regions) {
          assignmentCount++;
          this.regionOnline(region, SERVERNAME_A);
        }
        return true;
      }
      return super.assign(destination, regions);
    }

    @Override
    public void assign(List<HRegionInfo> regions)
        throws IOException, InterruptedException {
      assignInvoked = (regions != null && regions.size() > 0);
      super.assign(regions);
      this.gate.set(true);
    }

    /** reset the watcher */
    void setWatcher(ZooKeeperWatcher watcher) {
      this.watcher = watcher;
    }

    /**
     * @return ExecutorService used by this instance.
     */
    ExecutorService getExecutorService() {
      return this.es;
    }

    /*
     * Convenient method to retrieve mocked up connection
     */
    ClusterConnection getConnection() {
      return this.connection;
    }

    @Override
    public void shutdown() {
      super.shutdown();
      if (this.connection != null)
        try {
          this.connection.close();
        } catch (IOException e) {
          fail("Failed to close connection");
        }
    }
  }

  /**
   * Call joinCluster on the passed AssignmentManager.  Do it in a thread
   * so it runs independent of what all else is going on.  Try to simulate
   * an AM running insided a failed over master by clearing all in-memory
   * AM state first.
  */
  private void startFakeFailedOverMasterAssignmentManager(final AssignmentManager am,
      final ZooKeeperWatcher watcher) {
    // Make sure our new AM gets callbacks; once registered, we can't unregister.
    // Thats ok because we make a new zk watcher for each test.
    watcher.registerListenerFirst(am);
    Thread t = new Thread("RunAmJoinCluster") {
      @Override
      public void run() {
        // Call the joinCluster function as though we were doing a master
        // failover at this point. It will stall just before we go to add
        // the RIT region to our RIT Map in AM at processRegionsInTransition.
        // First clear any inmemory state from AM so it acts like a new master
        // coming on line.
        am.getRegionStates().regionsInTransition.clear();
        am.regionPlans.clear();
        try {
          am.joinCluster();
        } catch (IOException e) {
          throw new RuntimeException(e);
        } catch (KeeperException e) {
          throw new RuntimeException(e);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        } catch (CoordinatedStateException e) {
          throw new RuntimeException(e);
        }
      }
    };
    t.start();
    while (!t.isAlive()) Threads.sleep(1);
  }

  @Test (timeout=180000)
  public void testForceAssignMergingRegion() throws Exception {
    // Region to use in test.
    final HRegionInfo hri = HRegionInfo.FIRST_META_REGIONINFO;
    // Need a mocked catalog tracker.
    LoadBalancer balancer = LoadBalancerFactory.getLoadBalancer(
      server.getConfiguration());
    // Create an AM.
    AssignmentManager am = new AssignmentManager(this.server,
        this.serverManager, balancer, null, null, master.getTableLockManager(),
        tableStateManager);
    RegionStates regionStates = am.getRegionStates();
    try {
      // First set the state of the region to merging
      regionStates.updateRegionState(hri, RegionState.State.MERGING);
      // Now, try to assign it with force new plan
      am.assign(hri, true, true);
      assertEquals("The region should be still in merging state",
        RegionState.State.MERGING, regionStates.getRegionState(hri).getState());
    } finally {
      am.shutdown();
    }
  }

  /**
   * Test assignment related ZK events are ignored by AM if the region is not known
   * by AM to be in transition. During normal operation, all assignments are started
   * by AM (not considering split/merge), if an event is received but the region
   * is not in transition, the event must be a very late one. So it can be ignored.
   * During master failover, since AM watches assignment znodes after failover cleanup
   * is completed, when an event comes in, AM should already have the region in transition
   * if ZK is used during the assignment action (only hbck doesn't use ZK for region
   * assignment). So during master failover, we can ignored such events too.
   */
  @Test (timeout=180000)
  public void testAssignmentEventIgnoredIfNotExpected() throws KeeperException, IOException,
      CoordinatedStateException {
    // Region to use in test.
    final HRegionInfo hri = HRegionInfo.FIRST_META_REGIONINFO;
    LoadBalancer balancer = LoadBalancerFactory.getLoadBalancer(
      server.getConfiguration());
    final AtomicBoolean zkEventProcessed = new AtomicBoolean(false);
    // Create an AM.
    AssignmentManager am = new AssignmentManager(this.server,
        this.serverManager, balancer, null, null, master.getTableLockManager(),
        tableStateManager) {

      @Override
      void handleRegion(final RegionTransition rt, OpenRegionCoordination coordination,
                        OpenRegionCoordination.OpenRegionDetails ord) {
        super.handleRegion(rt, coordination, ord);
        if (rt != null && Bytes.equals(hri.getRegionName(),
          rt.getRegionName()) && rt.getEventType() == EventType.RS_ZK_REGION_OPENING) {
          zkEventProcessed.set(true);
        }
      }
    };
    try {
      // First make sure the region is not in transition
      am.getRegionStates().regionOffline(hri);
      zkEventProcessed.set(false); // Reset it before faking zk transition
      this.watcher.registerListenerFirst(am);
      assertFalse("The region should not be in transition",
        am.getRegionStates().isRegionInTransition(hri));
      ZKAssign.createNodeOffline(this.watcher, hri, SERVERNAME_A);
      // Trigger a transition event
      ZKAssign.transitionNodeOpening(this.watcher, hri, SERVERNAME_A);
      long startTime = EnvironmentEdgeManager.currentTime();
      while (!zkEventProcessed.get()) {
        assertTrue("Timed out in waiting for ZK event to be processed",
          EnvironmentEdgeManager.currentTime() - startTime < 30000);
        Threads.sleepWithoutInterrupt(100);
      }
      assertFalse(am.getRegionStates().isRegionInTransition(hri));
    } finally {
      am.shutdown();
    }
  }

  /**
   * If a table is deleted, we should not be able to balance it anymore.
   * Otherwise, the region will be brought back.
   * @throws Exception
   */
  @Test (timeout=180000)
  public void testBalanceRegionOfDeletedTable() throws Exception {
    AssignmentManager am = new AssignmentManager(this.server, this.serverManager,
        balancer, null, null, master.getTableLockManager(),
        tableStateManager);
    RegionStates regionStates = am.getRegionStates();
    HRegionInfo hri = REGIONINFO;
    regionStates.createRegionState(hri);
    assertFalse(regionStates.isRegionInTransition(hri));
    RegionPlan plan = new RegionPlan(hri, SERVERNAME_A, SERVERNAME_B);
    // Fake table is deleted
    regionStates.tableDeleted(hri.getTable());
    am.balance(plan);
    assertFalse("The region should not in transition",
      regionStates.isRegionInTransition(hri));
    am.shutdown();
  }

  /**
   * Tests an on-the-fly RPC that was scheduled for the earlier RS on the same port
   * for openRegion. AM should assign this somewhere else. (HBASE-9721)
   */
  @SuppressWarnings("unchecked")
  @Test (timeout=180000)
  public void testOpenCloseRegionRPCIntendedForPreviousServer() throws Exception {
    Mockito.when(this.serverManager.sendRegionOpen(Mockito.eq(SERVERNAME_B), Mockito.eq(REGIONINFO),
      Mockito.anyInt(), (List<ServerName>)Mockito.any()))
      .thenThrow(new DoNotRetryIOException());
    this.server.getConfiguration().setInt("hbase.assignment.maximum.attempts", 100);

    HRegionInfo hri = REGIONINFO;
    LoadBalancer balancer = LoadBalancerFactory.getLoadBalancer(
      server.getConfiguration());
    // Create an AM.
    AssignmentManager am = new AssignmentManager(this.server,
        this.serverManager, balancer, null, null, master.getTableLockManager(),
        tableStateManager);
    RegionStates regionStates = am.getRegionStates();
    try {
      am.regionPlans.put(REGIONINFO.getEncodedName(),
        new RegionPlan(REGIONINFO, null, SERVERNAME_B));

      // Should fail once, but succeed on the second attempt for the SERVERNAME_A
      am.assign(hri, true, false);
    } finally {
      assertEquals(SERVERNAME_A, regionStates.getRegionState(REGIONINFO).getServerName());
      am.shutdown();
    }
  }
}
