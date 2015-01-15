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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionTestingUtility;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.executor.EventHandler.EventType;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.executor.ExecutorService.ExecutorType;
import org.apache.hadoop.hbase.executor.RegionTransitionData;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.master.AssignmentManager.RegionState;
import org.apache.hadoop.hbase.master.AssignmentManager.RegionState.State;
import org.apache.hadoop.hbase.master.handler.ServerShutdownHandler;
import org.apache.hadoop.hbase.regionserver.RegionOpeningState;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKTable.TableState;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Watcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;

import com.google.protobuf.ServiceException;


/**
 * Test {@link AssignmentManager}
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Category(MediumTests.class)
public class TestAssignmentManager {
  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();
  private static final ServerName SERVERNAME_A =
    new ServerName("example.org", 1234, 5678);
  private static final ServerName SERVERNAME_B =
    new ServerName("example.org", 0, 5678);
  private static final ServerName SERVERNAME_C =
      new ServerName("example.org", 123, 5678);
  private static final HRegionInfo REGIONINFO =
    new HRegionInfo(Bytes.toBytes("t"),
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_START_ROW);
  private static final HRegionInfo REGIONINFO_2 = new HRegionInfo(Bytes.toBytes("t"),
      Bytes.toBytes("a"),Bytes.toBytes( "b"));
  private static int assignmentCount;
  private static boolean enabling = false;  

  // Mocked objects or; get redone for each test.
  private Server server;
  private ServerManager serverManager;
  private ZooKeeperWatcher watcher;
  private LoadBalancer balancer;

  @BeforeClass
  public static void beforeClass() throws Exception {
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
    this.server = Mockito.mock(Server.class);
    Mockito.when(server.getConfiguration()).thenReturn(HTU.getConfiguration());
    this.watcher =
      new ZooKeeperWatcher(HTU.getConfiguration(), "mockedServer", this.server, true);
    Mockito.when(server.getZooKeeper()).thenReturn(this.watcher);
    Mockito.doThrow(new RuntimeException("Aborted")).
      when(server).abort(Mockito.anyString(), (Throwable)Mockito.anyObject());

    // Mock a ServerManager.  Say server SERVERNAME_{A,B} are online.  Also
    // make it so if close or open, we return 'success'.
    this.serverManager = mockManager(SERVERNAME_A, SERVERNAME_B);
  }

  private ServerManager mockManager(ServerName... servers) throws IOException {
    ServerManager serverManager = Mockito.mock(ServerManager.class);
    final Map<ServerName, HServerLoad> onlineServers = new HashMap<ServerName, HServerLoad>();
    for (ServerName server : servers) {
      Mockito.when(serverManager.isServerOnline(server)).thenReturn(true);
      onlineServers.put(server, new HServerLoad());
      Mockito.when(serverManager.sendRegionClose(server, REGIONINFO, -1)).thenReturn(true);
      Mockito.when(serverManager.sendRegionOpen(server, REGIONINFO, -1)).
          thenReturn(RegionOpeningState.OPENED);
    }
    Mockito.when(serverManager.getOnlineServersList()).thenReturn(
        new ArrayList<ServerName>(onlineServers.keySet()));
    Mockito.when(serverManager.getOnlineServers()).thenReturn(onlineServers);
    return serverManager;
  }

  @After
    public void after() throws KeeperException {
    if (this.watcher != null) {
      // Clean up all znodes
      ZKAssign.deleteAllNodes(this.watcher);
      this.watcher.close();
    }
  }

  /**
   * Test a balance going on at same time as a master failover
   *
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  @Test(timeout = 60000)
  public void testBalanceOnMasterFailoverScenarioWithOpenedNode()
      throws IOException, KeeperException, InterruptedException {
    AssignmentManagerWithExtrasForTesting am =
      setUpMockedAssignmentManager(this.server, this.serverManager);
    try {
      createRegionPlanAndBalance(am, SERVERNAME_A, SERVERNAME_B, REGIONINFO);
      startFakeFailedOverMasterAssignmentManager(am, this.watcher);
      while (!am.processRITInvoked) Thread.sleep(1);
      // Now fake the region closing successfully over on the regionserver; the
      // regionserver will have set the region in CLOSED state. This will
      // trigger callback into AM. The below zk close call is from the RS close
      // region handler duplicated here because its down deep in a private
      // method hard to expose.
      int versionid =
        ZKAssign.transitionNodeClosed(this.watcher, REGIONINFO, SERVERNAME_A, -1);
      assertNotSame(versionid, -1);
      Mocking.waitForRegionOfflineInRIT(am, REGIONINFO.getEncodedName());

      // Get the OFFLINE version id.  May have to wait some for it to happen.
      // OPENING below
      while (true) {
        int vid = ZKAssign.getVersion(this.watcher, REGIONINFO);
        if (vid != versionid) {
          versionid = vid;
          break;
        }
      }
      assertNotSame(-1, versionid);
      // This uglyness below is what the openregionhandler on RS side does.
      versionid = ZKAssign.transitionNode(server.getZooKeeper(), REGIONINFO,
        SERVERNAME_A, EventType.M_ZK_REGION_OFFLINE,
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
      throws IOException, KeeperException, InterruptedException {
    AssignmentManagerWithExtrasForTesting am =
      setUpMockedAssignmentManager(this.server, this.serverManager);
    try {
      createRegionPlanAndBalance(am, SERVERNAME_A, SERVERNAME_B, REGIONINFO);
      startFakeFailedOverMasterAssignmentManager(am, this.watcher);
      while (!am.processRITInvoked) Thread.sleep(1);
      // Now fake the region closing successfully over on the regionserver; the
      // regionserver will have set the region in CLOSED state. This will
      // trigger callback into AM. The below zk close call is from the RS close
      // region handler duplicated here because its down deep in a private
      // method hard to expose.
      int versionid =
        ZKAssign.transitionNodeClosed(this.watcher, REGIONINFO, SERVERNAME_A, -1);
      assertNotSame(versionid, -1);
      am.gate.set(false);
      Mocking.waitForRegionOfflineInRIT(am, REGIONINFO.getEncodedName());

      // Get current versionid else will fail on transition from OFFLINE to
      // OPENING below
      while (true) {
        int vid = ZKAssign.getVersion(this.watcher, REGIONINFO);
        if (vid != versionid) {
          versionid = vid;
          break;
        }
      }
      assertNotSame(-1, versionid);
      // This uglyness below is what the openregionhandler on RS side does.
      versionid = ZKAssign.transitionNode(server.getZooKeeper(), REGIONINFO,
          SERVERNAME_A, EventType.M_ZK_REGION_OFFLINE,
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
      throws IOException, KeeperException, InterruptedException {
    AssignmentManagerWithExtrasForTesting am =
      setUpMockedAssignmentManager(this.server, this.serverManager);
    try {
      createRegionPlanAndBalance(am, SERVERNAME_A, SERVERNAME_B, REGIONINFO);
      startFakeFailedOverMasterAssignmentManager(am, this.watcher);
      while (!am.processRITInvoked) Thread.sleep(1);
      // Now fake the region closing successfully over on the regionserver; the
      // regionserver will have set the region in CLOSED state. This will
      // trigger callback into AM. The below zk close call is from the RS close
      // region handler duplicated here because its down deep in a private
      // method hard to expose.
      int versionid =
        ZKAssign.transitionNodeClosed(this.watcher, REGIONINFO, SERVERNAME_A, -1);
      assertNotSame(versionid, -1);
      Mocking.waitForRegionOfflineInRIT(am, REGIONINFO.getEncodedName());

      am.gate.set(false);
      // Get current versionid else will fail on transition from OFFLINE to
      // OPENING below
      while (true) {
        int vid = ZKAssign.getVersion(this.watcher, REGIONINFO);
        if (vid != versionid) {
          versionid = vid;
          break;
        }
      }
      assertNotSame(-1, versionid);
      // This uglyness below is what the openregionhandler on RS side does.
      versionid = ZKAssign.transitionNode(server.getZooKeeper(), REGIONINFO,
          SERVERNAME_A, EventType.M_ZK_REGION_OFFLINE,
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

  private void createRegionPlanAndBalance(final AssignmentManager am,
      final ServerName from, final ServerName to, final HRegionInfo hri) {
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
   * @throws InterruptedException
   */
  @Test(timeout = 60000)
  public void testBalance()
  throws IOException, KeeperException, InterruptedException {
    // Create and startup an executor.  This is used by AssignmentManager
    // handling zk callbacks.
    ExecutorService executor = startupMasterExecutor("testBalanceExecutor");

    // We need a mocked catalog tracker.
    CatalogTracker ct = Mockito.mock(CatalogTracker.class);
    LoadBalancer balancer = LoadBalancerFactory.getLoadBalancer(server
        .getConfiguration());
    // Create an AM.
    AssignmentManager am = new AssignmentManager(this.server,
        this.serverManager, ct, balancer, executor);
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
      // transition it through OPENING, OPENED.  Wait till we see the RIT
      // before we proceed.
      Mocking.waitForRegionOfflineInRIT(am, REGIONINFO.getEncodedName());
      // Get current versionid else will fail on transition from OFFLINE to OPENING below
      while (true) {
        int vid = ZKAssign.getVersion(this.watcher, REGIONINFO);
        if (vid != versionid) {
          versionid = vid;
          break;
        }
      }
      assertNotSame(-1, versionid);
      // This uglyness below is what the openregionhandler on RS side does.
      versionid = ZKAssign.transitionNode(server.getZooKeeper(), REGIONINFO,
        SERVERNAME_A, EventType.M_ZK_REGION_OFFLINE,
        EventType.RS_ZK_REGION_OPENING, versionid);
      assertNotSame(-1, versionid);
      // Move znode from OPENING to OPENED as RS does on successful open.
      versionid =
        ZKAssign.transitionNodeOpened(this.watcher, REGIONINFO, SERVERNAME_B, versionid);
      assertNotSame(-1, versionid);
      // Wait on the handler removing the OPENED znode.
      while(am.isRegionInTransition(REGIONINFO) != null) Threads.sleep(1);
    } finally {
      executor.shutdown();
      am.shutdown();
      // Clean up all znodes
      ZKAssign.deleteAllNodes(this.watcher);
    }
  }

  @Test
  public void testGettingAssignmentsExcludesDrainingServers() throws Exception {
    List<ServerName> availableServers =
        Lists.newArrayList(SERVERNAME_A, SERVERNAME_B, SERVERNAME_C);
    ServerManager serverManager = mockManager(availableServers.toArray(new ServerName[0]));


    ExecutorService executor = startupMasterExecutor("testAssignmentsWithRSInDraining");
    CatalogTracker ct = Mockito.mock(CatalogTracker.class);

    LoadBalancer balancer = LoadBalancerFactory.getLoadBalancer(server.getConfiguration());

    Mockito.when(serverManager.getDrainingServersList()).thenReturn(
        Lists.newArrayList(SERVERNAME_C));
    AssignmentManager am = new AssignmentManager(this.server, serverManager, ct, balancer, executor);

    for (ServerName availableServer : availableServers) {
      HRegionInfo info = Mockito.mock(HRegionInfo.class);
      Mockito.when(info.getEncodedName()).thenReturn(UUID.randomUUID().toString());
      am.regionOnline(info, availableServer);
    }

    Map<String, Map<ServerName, List<HRegionInfo>>> result = am.getAssignmentsByTable();
    for (Map<ServerName, List<HRegionInfo>> map : result.values()) {
      System.out.println(map.keySet());
      assertFalse(map.containsKey(SERVERNAME_C));
    }
  }

  /**
   * Run a simple server shutdown handler.
   * @throws KeeperException
   * @throws IOException
   */
  @Test
  public void testShutdownHandler() throws KeeperException, IOException {
    // Create and startup an executor.  This is used by AssignmentManager
    // handling zk callbacks.
    ExecutorService executor = startupMasterExecutor("testShutdownHandler");

    // We need a mocked catalog tracker.
    CatalogTracker ct = Mockito.mock(CatalogTracker.class);
    LoadBalancer balancer = LoadBalancerFactory.getLoadBalancer(server
        .getConfiguration());
    // Create an AM.
    AssignmentManager am =
      new AssignmentManager(this.server, this.serverManager, ct, balancer, executor);
    try {
      processServerShutdownHandler(ct, am, false, null);
    } finally {
      executor.shutdown();
      am.shutdown();
      // Clean up all znodes
      ZKAssign.deleteAllNodes(this.watcher);
    }
  }

  /**
   * To test closed region handler to remove rit and delete corresponding znode if region in pending
   * close or closing while processing shutdown of a region server.(HBASE-5927).
   * @throws KeeperException
   * @throws IOException
   */
  @Test
  public void testSSHWhenDisableTableInProgress()
      throws KeeperException, IOException {
    testCaseWithPartiallyDisabledState(TableState.DISABLING, false);
    testCaseWithPartiallyDisabledState(TableState.DISABLED, false);
  }

  @Test
  public void testSSHWhenDisablingTableRegionsInOpeningState()
      throws KeeperException, IOException {
    testCaseWithPartiallyDisabledState(TableState.DISABLING, true);
    testCaseWithPartiallyDisabledState(TableState.DISABLED, true);
  }

  
  /**
   * To test if the split region is removed from RIT if the region was in SPLITTING state
   * but the RS has actually completed the splitting in META but went down. See HBASE-6070
   * and also HBASE-5806
   * @throws KeeperException
   * @throws IOException
   */
  @Test
  public void testSSHWhenSplitRegionInProgress()
      throws KeeperException, IOException, Exception {
    // true indicates the region is split but still in RIT
    testCaseWithSplitRegionPartial(true);
    // false indicate the region is not split
    testCaseWithSplitRegionPartial(false);

  }

  private void testCaseWithSplitRegionPartial(boolean regionSplitDone) throws KeeperException, IOException,
      NodeExistsException, InterruptedException {
    // Create and startup an executor. This is used by AssignmentManager
    // handling zk callbacks.
    ExecutorService executor = startupMasterExecutor("testSSHWhenSplitRegionInProgress");

    // We need a mocked catalog tracker.
    CatalogTracker ct = Mockito.mock(CatalogTracker.class);
    // Create an AM.
    AssignmentManagerWithExtrasForTesting am = setUpMockedAssignmentManager(this.server, this.serverManager);
    // adding region to regions and servers maps.
    am.regionOnline(REGIONINFO, SERVERNAME_A);
    // adding region in pending close.
    am.regionsInTransition.put(REGIONINFO.getEncodedName(), new RegionState(REGIONINFO,
        State.SPLITTING, System.currentTimeMillis(), SERVERNAME_A));
    am.getZKTable().setEnabledTable(REGIONINFO.getTableNameAsString());

    RegionTransitionData data = new RegionTransitionData(EventType.RS_ZK_REGION_SPLITTING,
        REGIONINFO.getRegionName(), SERVERNAME_A);
    String node = ZKAssign.getNodeName(this.watcher, REGIONINFO.getEncodedName());
    // create znode in M_ZK_REGION_CLOSING state.
    ZKUtil.createAndWatch(this.watcher, node, data.getBytes());

    try {
      processServerShutdownHandler(ct, am, regionSplitDone, null);
      // check znode deleted or not.
      // In both cases the znode should be deleted.

      if(regionSplitDone){
        assertTrue("Region state of region in SPLITTING should be removed from rit.",
            am.regionsInTransition.isEmpty());
      }
      else{
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

  private void testCaseWithPartiallyDisabledState(TableState state, boolean opening)
      throws KeeperException, IOException, NodeExistsException {
    // Create and startup an executor. This is used by AssignmentManager
    // handling zk callbacks.
    ExecutorService executor = startupMasterExecutor("testSSHWhenDisableTableInProgress");

    // We need a mocked catalog tracker.
    CatalogTracker ct = Mockito.mock(CatalogTracker.class);
    LoadBalancer balancer = LoadBalancerFactory.getLoadBalancer(server.getConfiguration());
    // Create an AM.
    AssignmentManager am = new AssignmentManager(this.server, this.serverManager, ct, balancer,
        executor);
    if (opening) {
      am.regionsInTransition.put(REGIONINFO.getEncodedName(), new RegionState(REGIONINFO,
          State.OPENING, System.currentTimeMillis(), SERVERNAME_A));
    } else {
      // adding region to regions and servers maps.
      am.regionOnline(REGIONINFO, SERVERNAME_A);
      // adding region in pending close.
      am.regionsInTransition.put(REGIONINFO.getEncodedName(), new RegionState(REGIONINFO,
          State.PENDING_CLOSE, System.currentTimeMillis(), SERVERNAME_A));
    }
    if (state == TableState.DISABLING) {
      am.getZKTable().setDisablingTable(REGIONINFO.getTableNameAsString());
    } else {
      am.getZKTable().setDisabledTable(REGIONINFO.getTableNameAsString());
    }
    RegionTransitionData data = null;
    if (opening) {
      data =
          new RegionTransitionData(EventType.RS_ZK_REGION_OPENING, REGIONINFO.getRegionName(),
              SERVERNAME_A);

    } else {
      data =
          new RegionTransitionData(EventType.M_ZK_REGION_CLOSING, REGIONINFO.getRegionName(),
              SERVERNAME_A);
    }
    String node = ZKAssign.getNodeName(this.watcher, REGIONINFO.getEncodedName());
    // create znode in M_ZK_REGION_CLOSING state.
    ZKUtil.createAndWatch(this.watcher, node, data.getBytes());

    try {
      processServerShutdownHandler(ct, am, false, null);
      // check znode deleted or not.
      // In both cases the znode should be deleted.
      assertTrue("The znode should be deleted.",ZKUtil.checkExists(this.watcher, node) == -1);
      assertTrue("Region state of region in pending close should be removed from rit.",
        am.regionsInTransition.isEmpty());
    } finally {
      executor.shutdown();
      am.shutdown();
      // Clean up all znodes
      ZKAssign.deleteAllNodes(this.watcher);
    }
  }

  private void processServerShutdownHandler(CatalogTracker ct, AssignmentManager am,
    boolean splitRegion, ServerName sn)
      throws IOException {
    // Make sure our new AM gets callbacks; once registered, can't unregister.
    // Thats ok because we make a new zk watcher for each test.
    this.watcher.registerListenerFirst(am);
    // Need to set up a fake scan of meta for the servershutdown handler
    // Make an RS Interface implementation.  Make it so a scanner can go against it.
    HRegionInterface implementation = Mockito.mock(HRegionInterface.class);
    // Get a meta row result that has region up on SERVERNAME_A

    Result r = null;
    if (sn == null) {
      if (splitRegion) {
        r = getMetaTableRowResultAsSplitRegion(REGIONINFO, SERVERNAME_A);
      } else {
        r = getMetaTableRowResult(REGIONINFO, SERVERNAME_A);
      }
    } else {
      if (sn.equals(SERVERNAME_A)) {
        r = getMetaTableRowResult(REGIONINFO, SERVERNAME_A);
      } else if (sn.equals(SERVERNAME_B)) {
        r = new Result(new KeyValue[0]);
      }
    }

    Mockito.when(implementation.openScanner((byte [])Mockito.any(), (Scan)Mockito.any())).
      thenReturn(System.currentTimeMillis());
    // Return a good result first and then return null to indicate end of scan
    Mockito.when(implementation.next(Mockito.anyLong(), Mockito.anyInt(), Mockito.anyInt())).
      thenReturn(new Result [] {r}, (Result [])null);

    // Get a connection w/ mocked up common methods.
    HConnection connection =
      HConnectionTestingUtility.getMockedConnectionAndDecorate(HTU.getConfiguration(),
        implementation, SERVERNAME_B, REGIONINFO);

    // Make it so we can get a catalogtracker from servermanager.. .needed
    // down in guts of server shutdown handler.
    Mockito.when(ct.getConnection()).thenReturn(connection);
    Mockito.when(this.server.getCatalogTracker()).thenReturn(ct);

    // Now make a server shutdown handler instance and invoke process.
    // Have it that SERVERNAME_A died.
    DeadServer deadServers = new DeadServer();
    deadServers.add(SERVERNAME_A);
    // I need a services instance that will return the AM
    MasterServices services = Mockito.mock(MasterServices.class);
    Mockito.when(services.getAssignmentManager()).thenReturn(am);
    Mockito.when(services.getZooKeeper()).thenReturn(this.watcher);
    ServerShutdownHandler handler = null;
    if (sn != null) {
      handler = new ServerShutdownHandler(this.server, services, deadServers, sn, false);
    } else {
      handler = new ServerShutdownHandler(this.server, services, deadServers, SERVERNAME_A, false);
    }
    handler.process();
    // The region in r will have been assigned.  It'll be up in zk as unassigned.
  }

  /**
   * @param sn ServerName to use making startcode and server in meta
   * @param hri Region to serialize into HRegionInfo
   * @return A mocked up Result that fakes a Get on a row in the
   * <code>.META.</code> table.
   * @throws IOException
   */
  private Result getMetaTableRowResult(final HRegionInfo hri,
      final ServerName sn)
  throws IOException {
    // TODO: Move to a utilities class.  More than one test case can make use
    // of this facility.
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    kvs.add(new KeyValue(HConstants.EMPTY_BYTE_ARRAY,
      HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
      Writables.getBytes(hri)));
    kvs.add(new KeyValue(HConstants.EMPTY_BYTE_ARRAY,
      HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER,
      Bytes.toBytes(sn.getHostAndPort())));
    kvs.add(new KeyValue(HConstants.EMPTY_BYTE_ARRAY,
      HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER,
      Bytes.toBytes(sn.getStartcode())));
    return new Result(kvs);
  }

  /**
   * @param sn ServerName to use making startcode and server in meta
   * @param hri Region to serialize into HRegionInfo
   * @return A mocked up Result that fakes a Get on a row in the
   * <code>.META.</code> table.
   * @throws IOException
   */
  private Result getMetaTableRowResultAsSplitRegion(final HRegionInfo hri, final ServerName sn)
      throws IOException {
    hri.setOffline(true);
    hri.setSplit(true);
    return getMetaTableRowResult(hri, sn);
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

  @Test
  public void testUnassignWithSplitAtSameTime() throws KeeperException, IOException {
    // Region to use in test.
    final HRegionInfo hri = HRegionInfo.FIRST_META_REGIONINFO;
    // First amend the servermanager mock so that when we do send close of the
    // first meta region on SERVERNAME_A, it will return true rather than
    // default null.
    Mockito.when(this.serverManager.sendRegionClose(SERVERNAME_A, hri, -1)).thenReturn(true);
    // Need a mocked catalog tracker.
    CatalogTracker ct = Mockito.mock(CatalogTracker.class);
    LoadBalancer balancer = LoadBalancerFactory.getLoadBalancer(server
        .getConfiguration());
    // Create an AM.
    AssignmentManager am =
      new AssignmentManager(this.server, this.serverManager, ct, balancer, null);
    try {
      // First make sure my mock up basically works.  Unassign a region.
      unassign(am, SERVERNAME_A, hri);
      // This delete will fail if the previous unassign did wrong thing.
      ZKAssign.deleteClosingNode(this.watcher, hri);
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
      assertTrue(am.isRegionInTransition(hri) == null);
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
      throws IOException, KeeperException, InterruptedException, ServiceException {
    final RecoverableZooKeeper recoverableZk = Mockito
        .mock(RecoverableZooKeeper.class);
    AssignmentManagerWithExtrasForTesting am = setUpMockedAssignmentManager(
        this.server, this.serverManager);
    Watcher zkw = new ZooKeeperWatcher(HBaseConfiguration.create(), "unittest",
        null) {
      public RecoverableZooKeeper getRecoverableZooKeeper() {
        return recoverableZk;
      }
    };
    ((ZooKeeperWatcher) zkw).registerListener(am);
    Mockito.doThrow(new InterruptedException()).when(recoverableZk)
        .getChildren("/hbase/unassigned", zkw);
    am.setWatcher((ZooKeeperWatcher) zkw);
    try {
      am.processDeadServersAndRegionsInTransition();
      fail("Expected to abort");
    } catch (NullPointerException e) {
      fail("Should not throw NPE");
    } catch (RuntimeException e) {
      assertEquals("Aborted", e.getLocalizedMessage());
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
    RegionTransitionData data =
      new RegionTransitionData(EventType.RS_ZK_REGION_SPLITTING,
        region.getRegionName(), serverName);

    String node = ZKAssign.getNodeName(zkw, region.getEncodedName());
    if (!ZKUtil.createEphemeralNodeAndWatch(zkw, node, data.getBytes())) {
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
      final HRegionInfo hri) {
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
  private AssignmentManagerWithExtrasForTesting setUpMockedAssignmentManager(final Server server,
      final ServerManager manager)
  throws IOException, KeeperException {
    // We need a mocked catalog tracker. Its used by our AM instance.
    CatalogTracker ct = Mockito.mock(CatalogTracker.class);
    // Make an RS Interface implementation. Make it so a scanner can go against
    // it and a get to return the single region, REGIONINFO, this test is
    // messing with. Needed when "new master" joins cluster. AM will try and
    // rebuild its list of user regions and it will also get the HRI that goes
    // with an encoded name by doing a Get on .META.
    HRegionInterface ri = Mockito.mock(HRegionInterface.class);
    // Get a meta row result that has region up on SERVERNAME_A for REGIONINFO
    Result[] result = null;
    if (enabling) {
      result = new Result[2];
      result[0] = getMetaTableRowResult(REGIONINFO, SERVERNAME_A);
      result[1] = getMetaTableRowResult(REGIONINFO_2, SERVERNAME_A);
    }
    Result r = getMetaTableRowResult(REGIONINFO, SERVERNAME_A);
    Mockito.when(ri .openScanner((byte[]) Mockito.any(), (Scan) Mockito.any())).
      thenReturn(System.currentTimeMillis());
   if (enabling) {
      Mockito.when(ri.next(Mockito.anyLong(), Mockito.anyInt(), Mockito.anyInt())).thenReturn(result, result, result,
          (Result[]) null);
      // If a get, return the above result too for REGIONINFO_2
      Mockito.when(ri.get((byte[]) Mockito.any(), (Get) Mockito.any())).thenReturn(
          getMetaTableRowResult(REGIONINFO_2, SERVERNAME_A));
    } else {
      // Return good result 'r' first and then return null to indicate end of scan
      Mockito.when(ri.next(Mockito.anyLong(), Mockito.anyInt(), Mockito.anyInt())).thenReturn(new Result[] { r });
      // If a get, return the above result too for REGIONINFO
      Mockito.when(ri.get((byte[]) Mockito.any(), (Get) Mockito.any())).thenReturn(r);
    }
    // Get a connection w/ mocked up common methods.
    HConnection connection = HConnectionTestingUtility.
      getMockedConnectionAndDecorate(HTU.getConfiguration(), ri, SERVERNAME_B,
        REGIONINFO);
    // Make it so we can get the connection from our mocked catalogtracker
    Mockito.when(ct.getConnection()).thenReturn(connection);
    // Create and startup an executor. Used by AM handling zk callbacks.
    ExecutorService executor = startupMasterExecutor("mockedAMExecutor");
    this.balancer = LoadBalancerFactory.getLoadBalancer(server.getConfiguration());
    AssignmentManagerWithExtrasForTesting am = new AssignmentManagerWithExtrasForTesting(
        server, manager, ct, balancer, executor);
    return am;
  }

  /**
   * TestCase verifies that the regionPlan is updated whenever a region fails to open
   * and the master tries to process RS_ZK_FAILED_OPEN state.(HBASE-5546).
   */
  @Test
  public void testRegionPlanIsUpdatedWhenRegionFailsToOpen() throws IOException, KeeperException,
      ServiceException, InterruptedException {
    this.server.getConfiguration().setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
        MockedLoadBalancer.class, LoadBalancer.class);
    AssignmentManagerWithExtrasForTesting am = setUpMockedAssignmentManager(this.server,
        this.serverManager);
    try {
      // Boolean variable used for waiting until randomAssignment is called and new
      // plan is generated.
      AtomicBoolean gate = new AtomicBoolean(false);
      if (balancer instanceof MockedLoadBalancer) {
        ((MockedLoadBalancer) balancer).setGateVariable(gate);
      }
      ZKAssign.createNodeOffline(this.watcher, REGIONINFO, SERVERNAME_A);
      int v = ZKAssign.getVersion(this.watcher, REGIONINFO);
      ZKAssign.transitionNode(this.watcher, REGIONINFO, SERVERNAME_A, EventType.M_ZK_REGION_OFFLINE,
          EventType.RS_ZK_REGION_FAILED_OPEN, v);
      String path = ZKAssign.getNodeName(this.watcher, REGIONINFO.getEncodedName());
      RegionState state = new RegionState(REGIONINFO, State.OPENING, System.currentTimeMillis(),
          SERVERNAME_A);
      am.regionsInTransition.put(REGIONINFO.getEncodedName(), state);
      // a dummy plan inserted into the regionPlans. This plan is cleared and new one is formed
      am.regionPlans.put(REGIONINFO.getEncodedName(), new RegionPlan(REGIONINFO, null, SERVERNAME_A));
      RegionPlan regionPlan = am.regionPlans.get(REGIONINFO.getEncodedName());
      List<ServerName> serverList = new ArrayList<ServerName>(2);
      serverList.add(SERVERNAME_B);
      Mockito.when(this.serverManager.getOnlineServersList()).thenReturn(serverList);
      am.nodeDataChanged(path);
      // here we are waiting until the random assignment in the load balancer is called.
      while (!gate.get()) {
        Thread.sleep(10);
      }
      // new region plan may take some time to get updated after random assignment is called and
      // gate is set to true.
      RegionPlan newRegionPlan = am.regionPlans.get(REGIONINFO.getEncodedName());
      while (newRegionPlan == null) {
        Thread.sleep(10);
        newRegionPlan = am.regionPlans.get(REGIONINFO.getEncodedName());
      }
      // the new region plan created may contain the same RS as destination but it should
      // be new plan.
      assertNotSame("Same region plan should not come", regionPlan, newRegionPlan);
      assertTrue("Destnation servers should be different.", !(regionPlan.getDestination().equals(
        newRegionPlan.getDestination())));
      Mocking.waitForRegionOfflineInRIT(am, REGIONINFO.getEncodedName());
    } finally {
      this.server.getConfiguration().setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
        DefaultLoadBalancer.class, LoadBalancer.class);
      am.shutdown();
    }
  }

  /**
   * Test verifies whether assignment is skipped for regions of tables in DISABLING state during
   * clean cluster startup. See HBASE-6281.
   *
   * @throws KeeperException
   * @throws IOException
   * @throws Exception
   */
  @Test
  public void testDisablingTableRegionsAssignmentDuringCleanClusterStartup()
      throws KeeperException, IOException, Exception {
    this.server.getConfiguration().setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
        MockedLoadBalancer.class, LoadBalancer.class);
    Mockito.when(this.serverManager.getOnlineServers()).thenReturn(
        new HashMap<ServerName, HServerLoad>(0));
    List<ServerName> destServers = new ArrayList<ServerName>(1);
    destServers.add(SERVERNAME_A);
    Mockito.when(this.serverManager.getDrainingServersList()).thenReturn(destServers);
    // To avoid cast exception in DisableTableHandler process.
    //Server server = new HMaster(HTU.getConfiguration());
    AssignmentManagerWithExtrasForTesting am = setUpMockedAssignmentManager(server,
        this.serverManager);
    AtomicBoolean gate = new AtomicBoolean(false);
    if (balancer instanceof MockedLoadBalancer) {
      ((MockedLoadBalancer) balancer).setGateVariable(gate);
    }
    try{
      // set table in disabling state.
      am.getZKTable().setDisablingTable(REGIONINFO.getTableNameAsString());
      am.joinCluster();
      // should not call retainAssignment if we get empty regions in assignAllUserRegions.
      assertFalse(
          "Assign should not be invoked for disabling table regions during clean cluster startup.",
          gate.get());
      // need to change table state from disabling to disabled.
      assertTrue("Table should be disabled.",
          am.getZKTable().isDisabledTable(REGIONINFO.getTableNameAsString()));
    } finally {
      this.server.getConfiguration().setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
        DefaultLoadBalancer.class, LoadBalancer.class);
      am.getZKTable().setEnabledTable(REGIONINFO.getTableNameAsString());
      am.shutdown();
    }
  }

  /**
   * Test verifies whether stale znodes of unknown tables as for the hbase:meta will be removed or
   * not.
   * @throws KeeperException
   * @throws IOException
   * @throws Exception
   */
  @Test
  public void testMasterRestartShouldRemoveStaleZnodesOfUnknownTableAsForMeta()
      throws KeeperException, IOException, Exception {
    List<ServerName> destServers = new ArrayList<ServerName>(1);
    destServers.add(SERVERNAME_A);
    Mockito.when(this.serverManager.getOnlineServersList()).thenReturn(destServers);
    Mockito.when(this.serverManager.isServerOnline(SERVERNAME_A)).thenReturn(true);
    HTU.getConfiguration().setInt(HConstants.MASTER_PORT, 0);
    Server server = new HMaster(HTU.getConfiguration());
    Whitebox.setInternalState(server, "serverManager", this.serverManager);
    AssignmentManagerWithExtrasForTesting am = setUpMockedAssignmentManager(server,
        this.serverManager);
    try {
      String tableName = "dummyTable";
      am.enablingTables.put(tableName, null);
      // set table in enabling state.
      am.getZKTable().setEnablingTable(tableName);
      am.joinCluster();
      assertFalse("Table should not be present in zookeeper.",
        am.getZKTable().isTablePresent(tableName));
    } finally {
    }
  }

  /**
   * Test verifies whether all the enabling table regions assigned only once during master startup.
   * 
   * @throws KeeperException
   * @throws IOException
   * @throws Exception
   */
  @Test
  public void testMasterRestartWhenTableInEnabling() throws KeeperException, IOException, Exception {
    enabling = true;
    this.server.getConfiguration().setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
        DefaultLoadBalancer.class, LoadBalancer.class);
    Map<ServerName, HServerLoad> serverAndLoad = new HashMap<ServerName, HServerLoad>();
    serverAndLoad.put(SERVERNAME_A, null);
    Mockito.when(this.serverManager.getOnlineServers()).thenReturn(serverAndLoad);
    Mockito.when(this.serverManager.isServerOnline(SERVERNAME_B)).thenReturn(false);
    Mockito.when(this.serverManager.isServerOnline(SERVERNAME_A)).thenReturn(true);
    HTU.getConfiguration().setInt(HConstants.MASTER_PORT, 0);
    Server server = new HMaster(HTU.getConfiguration());
    Whitebox.setInternalState(server, "serverManager", this.serverManager);
    assignmentCount = 0;
    AssignmentManagerWithExtrasForTesting am = setUpMockedAssignmentManager(server,
        this.serverManager);
    am.regionOnline(new HRegionInfo("t1".getBytes(), HConstants.EMPTY_START_ROW,
        HConstants.EMPTY_END_ROW), SERVERNAME_A);
    am.gate.set(false);
    try {
      // set table in enabling state.
      am.getZKTable().setEnablingTable(REGIONINFO.getTableNameAsString());
      ZKAssign.createNodeOffline(this.watcher, REGIONINFO_2, SERVERNAME_B);

      am.joinCluster();
      while (!am.getZKTable().isEnabledTable(REGIONINFO.getTableNameAsString())) {
        Thread.sleep(10);
      }
      assertEquals("Number of assignments should be equal.", 2, assignmentCount);
      assertTrue("Table should be enabled.",
          am.getZKTable().isEnabledTable(REGIONINFO.getTableNameAsString()));
    } finally {
      enabling = false;
      am.getZKTable().setEnabledTable(REGIONINFO.getTableNameAsString());
      am.shutdown();
      ZKAssign.deleteAllNodes(this.watcher);
      assignmentCount = 0;
    }
  }



  /**
   * When region in transition if region server opening the region gone down then region assignment
   * taking long time(Waiting for timeout monitor to trigger assign). HBASE-5396(HBASE-6060) fixes this
   * scenario. This test case verifies whether SSH calling assign for the region in transition or not.
   *
   * @throws KeeperException
   * @throws IOException
   * @throws ServiceException
   */
  @Test
  public void testSSHWhenSourceRSandDestRSInRegionPlanGoneDown() throws KeeperException, IOException,
      ServiceException {
    testSSHWhenSourceRSandDestRSInRegionPlanGoneDown(true);
    testSSHWhenSourceRSandDestRSInRegionPlanGoneDown(false);
  }

  private void testSSHWhenSourceRSandDestRSInRegionPlanGoneDown(boolean regionInOffline)
      throws IOException, KeeperException, ServiceException {
    // We need a mocked catalog tracker.
    CatalogTracker ct = Mockito.mock(CatalogTracker.class);
    // Create an AM.
    AssignmentManagerWithExtrasForTesting am =
        setUpMockedAssignmentManager(this.server, this.serverManager);
    // adding region in pending open.
    if (regionInOffline) {
      ServerName MASTER_SERVERNAME = new ServerName("example.org", 1111, 1111);
      am.regionsInTransition.put(REGIONINFO.getEncodedName(), new RegionState(REGIONINFO,
          State.OFFLINE, System.currentTimeMillis(), MASTER_SERVERNAME));
    } else {
      am.regionsInTransition.put(REGIONINFO.getEncodedName(), new RegionState(REGIONINFO,
          State.OPENING, System.currentTimeMillis(), SERVERNAME_B));
    }
    // adding region plan
    am.regionPlans.put(REGIONINFO.getEncodedName(), new RegionPlan(REGIONINFO, SERVERNAME_A, SERVERNAME_B));
    am.getZKTable().setEnabledTable(REGIONINFO.getTableNameAsString());

    try {
      processServerShutdownHandler(ct, am, false, SERVERNAME_A);
      processServerShutdownHandler(ct, am, false, SERVERNAME_B);
      if(regionInOffline){
        assertFalse("Assign should not be invoked.", am.assignInvoked);
      } else {
        assertTrue("Assign should be invoked.", am.assignInvoked);
      }
    } finally {
      am.regionsInTransition.remove(REGIONINFO.getEncodedName());
      am.regionPlans.remove(REGIONINFO.getEncodedName());
    }
  }

  /**
   * Mocked load balancer class used in the testcase to make sure that the testcase waits until
   * random assignment is called and the gate variable is set to true.
   */
  public static class MockedLoadBalancer extends DefaultLoadBalancer {
    private AtomicBoolean gate;

    public void setGateVariable(AtomicBoolean gate) {
      this.gate = gate;
    }

    @Override
    public ServerName randomAssignment(List<ServerName> servers) {
      ServerName randomServerName = super.randomAssignment(servers);
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
   * An {@link AssignmentManager} with some extra facility used testing
   */
  class AssignmentManagerWithExtrasForTesting extends AssignmentManager {
    // Keep a reference so can give it out below in {@link #getExecutorService}
    private final ExecutorService es;
    // Ditto for ct
    private final CatalogTracker ct;
    boolean processRITInvoked = false;
    boolean assignInvoked = false;
    AtomicBoolean gate = new AtomicBoolean(true);

    public AssignmentManagerWithExtrasForTesting(final Server master,
        final ServerManager serverManager, final CatalogTracker catalogTracker,
        final LoadBalancer balancer, final ExecutorService service)
    throws KeeperException, IOException {
      super(master, serverManager, catalogTracker, balancer, service);
      this.es = service;
      this.ct = catalogTracker;
    }

    @Override
    boolean processRegionInTransition(String encodedRegionName,
        HRegionInfo regionInfo,
        Map<ServerName, List<Pair<HRegionInfo, Result>>> deadServers)
        throws KeeperException, IOException {
      this.processRITInvoked = true;
      return super.processRegionInTransition(encodedRegionName, regionInfo,
          deadServers);
    }
    @Override
    void processRegionsInTransition(final RegionTransitionData data,
        final HRegionInfo regionInfo,
        final Map<ServerName, List<Pair<HRegionInfo, Result>>> deadServers,
        final int expectedVersion) throws KeeperException {
      while (this.gate.get()) Threads.sleep(1);
      super.processRegionsInTransition(data, regionInfo, deadServers, expectedVersion);
    }
    
    @Override
    public void assign(HRegionInfo region, boolean setOfflineInZK, boolean forceNewPlan,
        boolean hijack) {
      if (enabling) {
        assignmentCount++;
        this.regionOnline(region, SERVERNAME_A);
      } else {
        assignInvoked = true;
        super.assign(region, setOfflineInZK, forceNewPlan, hijack);
      }
    }
    
    @Override
    public ServerName getRegionServerOfRegion(HRegionInfo hri) {
      return SERVERNAME_A;
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

    /**
     * @return CatalogTracker used by this AM (Its a mock).
     */
    CatalogTracker getCatalogTracker() {
      return this.ct;
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
      public void run() {
        // Call the joinCluster function as though we were doing a master
        // failover at this point. It will stall just before we go to add
        // the RIT region to our RIT Map in AM at processRegionsInTransition.
        // First clear any inmemory state from AM so it acts like a new master
        // coming on line.
        am.regionsInTransition.clear();
        am.regionPlans.clear();
        try {
          am.joinCluster();
        } catch (IOException e) {
          throw new RuntimeException(e);
        } catch (KeeperException e) {
          throw new RuntimeException(e);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      };
    };
    t.start();
    while (!t.isAlive()) Threads.sleep(1);
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}
