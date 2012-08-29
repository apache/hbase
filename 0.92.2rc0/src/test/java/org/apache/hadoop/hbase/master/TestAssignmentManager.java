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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HConnectionTestingUtility;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.executor.ExecutorService.ExecutorType;
import org.apache.hadoop.hbase.executor.RegionTransitionData;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.regionserver.RegionOpeningState;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;


/**
 * Test {@link AssignmentManager}
 */
public class TestAssignmentManager {
  private static final Log LOG = LogFactory.getLog(TestAssignmentManager.class);
  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();
  private static final ServerName SERVERNAME_A =
    new ServerName("example.org", 1234, 5678);
  private static final ServerName SERVERNAME_B =
    new ServerName("example.org", 0, 5678);
  private static final HRegionInfo REGIONINFO =
    new HRegionInfo(Bytes.toBytes("t"),
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_START_ROW);
  private static final Abortable ABORTABLE = new Abortable() {
    boolean aborted = false;
    @Override
    public void abort(String why, Throwable e) {
      LOG.info(why, e);
      this.aborted = true;
      throw new RuntimeException(e);
    }
    @Override
    public boolean isAborted()  {
      return this.aborted;
    }
  };

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
   * Test verifies whether assignment is skipped for regions of tables in
   * DISABLING state during clean cluster startup. See HBASE-6281.
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
      CatalogTracker ct = am.getCatalogTracker();
      if (ct != null) {
        ct.stop();
      }
      HConnectionManager.deleteConnection(HTU.getConfiguration(), true);
      am.getZKTable().setEnabledTable(REGIONINFO.getTableNameAsString());
      am.shutdown();
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

  /**
   * Create an {@link AssignmentManagerWithExtrasForTesting} that has mocked {@link CatalogTracker}
   * etc.
   * 
   * @param server
   * @param manager
   * @return An AssignmentManagerWithExtras with mock connections, etc.
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  private AssignmentManagerWithExtrasForTesting setUpMockedAssignmentManager(final Server server,
      final ServerManager manager) throws IOException, KeeperException, InterruptedException {
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(HTU.getConfiguration(), this.getClass()
        .getSimpleName(), ABORTABLE, true);
    HConnection connection = null;
    CatalogTracker ct = null;
    // Make an RS Interface implementation. Make it so a scanner can go against
    // it and a get to return the single region, REGIONINFO, this test is
    // messing with. Needed when "new master" joins cluster. AM will try and
    // rebuild its list of user regions and it will also get the HRI that goes
    // with an encoded name by doing a Get on .META.
    HRegionInterface ri = Mockito.mock(HRegionInterface.class);
    // Get a meta row result that has region up on SERVERNAME_A for REGIONINFO
    Result r = getMetaTableRowResult(REGIONINFO, SERVERNAME_A);

    final long scannerid = 123L;
    Mockito.when(ri.openScanner((byte[]) Mockito.any(), (Scan) Mockito.any()))
        .thenReturn(scannerid);
    // Make it so a verifiable answer comes back when next is called. Return
    // the verifiable answer and then a null so we stop scanning. Our
    // verifiable answer is something that looks like a row in META with
    // a server and startcode that is that of the above defined servername.
    Mockito.when(ri.next(Mockito.anyLong(), Mockito.anyInt()))
        .thenReturn(new Result[] { r }, (Result[]) null)
        .thenReturn(new Result[] { r }, (Result[]) null)
        .thenReturn(new Result[] { r }, (Result[]) null);

    // Associate a spied-upon HConnection with UTIL.getConfiguration. Need
    // to shove this in here first so it gets picked up all over; e.g. by
    // HTable.
    connection = HConnectionTestingUtility.getSpiedConnection(HTU.getConfiguration());
    Mockito.doNothing().when(connection).close();
    // Fix the location lookup so it 'works' though no network. First
    // make an 'any location' object.
    final HRegionLocation anyLocation = new HRegionLocation(HRegionInfo.FIRST_META_REGIONINFO,
        SERVERNAME_A.getHostname(), SERVERNAME_A.getPort());
    // Return the any location object when locateRegion is called in HTable
    // constructor and when its called by ServerCallable (it uses getRegionLocation).

    Mockito.doReturn(anyLocation).when(connection)
        .locateRegion((byte[]) Mockito.any(), (byte[]) Mockito.any());
    Mockito.doReturn(anyLocation).when(connection)
        .getRegionLocation((byte[]) Mockito.any(), (byte[]) Mockito.any(), Mockito.anyBoolean());

    // Now shove our HRI implementation into the spied-upon connection.
    Mockito.doReturn(ri).when(connection)
        .getHRegionConnection(Mockito.anyString(), Mockito.anyInt());

    // Mockito.when(connection.getHRegionConnection(Mockito.anyString(),
    // Mockito.anyInt())).thenReturn(ri, ri);

    // Now start up the catalogtracker with our doctored Connection.
    ct = new CatalogTracker(zkw, null, connection, ABORTABLE, 0);
    ct.start();

    // Create and startup an executor. Used by AM handling zk callbacks.
    ExecutorService executor = startupMasterExecutor("mockedAMExecutor");
    this.balancer = LoadBalancerFactory.getLoadBalancer(server.getConfiguration());
    AssignmentManagerWithExtrasForTesting am = new AssignmentManagerWithExtrasForTesting(server,
        manager, ct, balancer, executor);
    return am;
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
      super(master, serverManager, catalogTracker,service);
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
      assignInvoked = true;
      super.assign(region, setOfflineInZK, forceNewPlan, hijack);
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

}
