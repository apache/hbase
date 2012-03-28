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

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SmallTests;
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
import org.apache.hadoop.hbase.master.handler.ServerShutdownHandler;
import org.apache.hadoop.hbase.regionserver.RegionOpeningState;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;


/**
 * Test {@link AssignmentManager}
 */
@Category(SmallTests.class)
public class TestAssignmentManager {
  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();
  private static final ServerName SERVERNAME_A =
    new ServerName("example.org", 1234, 5678);
  private static final ServerName SERVERNAME_B =
    new ServerName("example.org", 0, 5678);
  private static final HRegionInfo REGIONINFO =
    new HRegionInfo(Bytes.toBytes("t"),
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_START_ROW);

  // Mocked objects or; get redone for each test.
  private Server server;
  private ServerManager serverManager;
  private ZooKeeperWatcher watcher;

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
    this.serverManager = Mockito.mock(ServerManager.class);
    Mockito.when(this.serverManager.isServerOnline(SERVERNAME_A)).thenReturn(true);
    Mockito.when(this.serverManager.isServerOnline(SERVERNAME_B)).thenReturn(true);
    final Map<ServerName, HServerLoad> onlineServers = new HashMap<ServerName, HServerLoad>();
    onlineServers.put(SERVERNAME_B, new HServerLoad());
    onlineServers.put(SERVERNAME_A, new HServerLoad());
    Mockito.when(this.serverManager.getOnlineServersList()).thenReturn(
        new ArrayList<ServerName>(onlineServers.keySet()));
    Mockito.when(this.serverManager.getOnlineServers()).thenReturn(onlineServers);
    Mockito.when(this.serverManager.sendRegionClose(SERVERNAME_A, REGIONINFO, -1)).
      thenReturn(true);
    Mockito.when(this.serverManager.sendRegionClose(SERVERNAME_B, REGIONINFO, -1)).
      thenReturn(true);
    // Ditto on open.
    Mockito.when(this.serverManager.sendRegionOpen(SERVERNAME_A, REGIONINFO, -1)).
      thenReturn(RegionOpeningState.OPENED);
    Mockito.when(this.serverManager.sendRegionOpen(SERVERNAME_B, REGIONINFO, -1)).
    thenReturn(RegionOpeningState.OPENED);
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
  @Test(timeout = 5000)
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
      while (!ZKAssign.verifyRegionState(this.watcher, REGIONINFO,
          EventType.M_ZK_REGION_OFFLINE)) {
        Threads.sleep(1);
      }
      // Get current versionid else will fail on transition from OFFLINE to
      // OPENING below
      versionid = ZKAssign.getVersion(this.watcher, REGIONINFO);
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

  @Test(timeout = 5000)
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
      while (!ZKAssign.verifyRegionState(this.watcher, REGIONINFO,
          EventType.M_ZK_REGION_OFFLINE)) {
        Threads.sleep(1);
      }
      // Get current versionid else will fail on transition from OFFLINE to
      // OPENING below
      versionid = ZKAssign.getVersion(this.watcher, REGIONINFO);
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

  @Test(timeout = 5000)
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
      while (!ZKAssign.verifyRegionState(this.watcher, REGIONINFO,
          EventType.M_ZK_REGION_OFFLINE)) {
        Threads.sleep(1);
      }
      am.gate.set(false);
      // Get current versionid else will fail on transition from OFFLINE to
      // OPENING below
      versionid = ZKAssign.getVersion(this.watcher, REGIONINFO);
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
   */
  @Test
  public void testBalance()
  throws IOException, KeeperException {
    // Create and startup an executor.  This is used by AssignmentManager
    // handling zk callbacks.
    ExecutorService executor = startupMasterExecutor("testBalanceExecutor");

    // We need a mocked catalog tracker.
    CatalogTracker ct = Mockito.mock(CatalogTracker.class);
    // Create an AM.
    AssignmentManager am =
      new AssignmentManager(this.server, this.serverManager, ct, executor);
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
      // transition it through OPENING, OPENED.  Wait till we see the OFFLINE
      // zk node before we proceed.
      while (!ZKAssign.verifyRegionState(this.watcher, REGIONINFO, EventType.M_ZK_REGION_OFFLINE)) {
        Threads.sleep(1);
      }
      // Get current versionid else will fail on transition from OFFLINE to OPENING below
      versionid = ZKAssign.getVersion(this.watcher, REGIONINFO);
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
    // Create an AM.
    AssignmentManager am =
      new AssignmentManager(this.server, this.serverManager, ct, executor);
    try {
      // Make sure our new AM gets callbacks; once registered, can't unregister.
      // Thats ok because we make a new zk watcher for each test.
      this.watcher.registerListenerFirst(am);

      // Need to set up a fake scan of meta for the servershutdown handler
      // Make an RS Interface implementation.  Make it so a scanner can go against it.
      HRegionInterface implementation = Mockito.mock(HRegionInterface.class);
      // Get a meta row result that has region up on SERVERNAME_A
      Result r = getMetaTableRowResult(REGIONINFO, SERVERNAME_A);
      Mockito.when(implementation.openScanner((byte [])Mockito.any(), (Scan)Mockito.any())).
        thenReturn(System.currentTimeMillis());
      // Return a good result first and then return null to indicate end of scan
      Mockito.when(implementation.next(Mockito.anyLong(), Mockito.anyInt())).
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
      ServerShutdownHandler handler = new ServerShutdownHandler(this.server,
        services, deadServers, SERVERNAME_A, false);
      handler.process();
      // The region in r will have been assigned.  It'll be up in zk as unassigned.
    } finally {
      executor.shutdown();
      am.shutdown();
      // Clean up all znodes
      ZKAssign.deleteAllNodes(this.watcher);
    }
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
    // Create an AM.
    AssignmentManager am =
      new AssignmentManager(this.server, this.serverManager, ct, null);
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
    Result r = getMetaTableRowResult(REGIONINFO, SERVERNAME_A);
    Mockito.when(ri .openScanner((byte[]) Mockito.any(), (Scan) Mockito.any())).
      thenReturn(System.currentTimeMillis());
    // Return good result 'r' first and then return null to indicate end of scan
    Mockito.when(ri.next(Mockito.anyLong(), Mockito.anyInt())).
      thenReturn(new Result[] { r }, (Result[]) null);
    // If a get, return the above result too for REGIONINFO
    Mockito.when(ri.get((byte[]) Mockito.any(), (Get) Mockito.any())).
      thenReturn(r);
    // Get a connection w/ mocked up common methods.
    HConnection connection = HConnectionTestingUtility.
      getMockedConnectionAndDecorate(HTU.getConfiguration(), ri, SERVERNAME_B,
        REGIONINFO);
    // Make it so we can get the connection from our mocked catalogtracker
    Mockito.when(ct.getConnection()).thenReturn(connection);
    // Create and startup an executor. Used by AM handling zk callbacks.
    ExecutorService executor = startupMasterExecutor("mockedAMExecutor");
    AssignmentManagerWithExtrasForTesting am =
      new AssignmentManagerWithExtrasForTesting(server, manager, ct, executor);
    return am;
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
    AtomicBoolean gate = new AtomicBoolean(true);

    public AssignmentManagerWithExtrasForTesting(final Server master,
        final ServerManager serverManager,
        final CatalogTracker catalogTracker, final ExecutorService service)
    throws KeeperException, IOException {
      super(master, serverManager, catalogTracker, service);
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
