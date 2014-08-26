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
package org.apache.hadoop.hbase;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.executor.ExecutorType;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.balancer.LoadBalancerFactory;
import org.apache.hadoop.hbase.regionserver.RegionOpeningState;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;


/**
 * Test the draining servers feature.
 */
@Category(MediumTests.class)
public class TestDrainingServer {
  private static final Log LOG = LogFactory.getLog(TestDrainingServer.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private Abortable abortable = new Abortable() {
    @Override
    public boolean isAborted() {
      return false;
    }

    @Override
    public void abort(String why, Throwable e) {
    }
  };

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
  }
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean("hbase.assignment.usezk", true);
    TEST_UTIL.startMiniZKCluster();
  }

  @Test
  public void testAssignmentManagerDoesntUseDrainingServer() throws Exception {
    AssignmentManager am;
    Configuration conf = TEST_UTIL.getConfiguration();
    final HMaster master = Mockito.mock(HMaster.class);
    final Server server = Mockito.mock(Server.class);
    final ServerManager serverManager = Mockito.mock(ServerManager.class);
    final ServerName SERVERNAME_A = ServerName.valueOf("mockserver_a.org", 1000, 8000);
    final ServerName SERVERNAME_B = ServerName.valueOf("mockserver_b.org", 1001, 8000);
    LoadBalancer balancer = LoadBalancerFactory.getLoadBalancer(conf);
    CatalogTracker catalogTracker = Mockito.mock(CatalogTracker.class);
    final HRegionInfo REGIONINFO = new HRegionInfo(TableName.valueOf("table_test"),
        HConstants.EMPTY_START_ROW, HConstants.EMPTY_START_ROW);

    ZooKeeperWatcher zkWatcher = new ZooKeeperWatcher(TEST_UTIL.getConfiguration(),
      "zkWatcher-Test", abortable, true);

    Map<ServerName, ServerLoad> onlineServers = new HashMap<ServerName, ServerLoad>();

    onlineServers.put(SERVERNAME_A, ServerLoad.EMPTY_SERVERLOAD);
    onlineServers.put(SERVERNAME_B, ServerLoad.EMPTY_SERVERLOAD);

    Mockito.when(server.getConfiguration()).thenReturn(conf);
    Mockito.when(server.getServerName()).thenReturn(ServerName.valueOf("masterMock,1,1"));
    Mockito.when(server.getZooKeeper()).thenReturn(zkWatcher);

    Mockito.when(serverManager.getOnlineServers()).thenReturn(onlineServers);
    Mockito.when(serverManager.getOnlineServersList())
    .thenReturn(new ArrayList<ServerName>(onlineServers.keySet()));
    
    Mockito.when(serverManager.createDestinationServersList())
        .thenReturn(new ArrayList<ServerName>(onlineServers.keySet()));
    Mockito.when(serverManager.createDestinationServersList(null))
        .thenReturn(new ArrayList<ServerName>(onlineServers.keySet()));
    
    for (ServerName sn : onlineServers.keySet()) {
      Mockito.when(serverManager.isServerOnline(sn)).thenReturn(true);
      Mockito.when(serverManager.sendRegionClose(sn, REGIONINFO, -1)).thenReturn(true);
      Mockito.when(serverManager.sendRegionClose(sn, REGIONINFO, -1, null, false)).thenReturn(true);
      Mockito.when(serverManager.sendRegionOpen(sn, REGIONINFO, -1, new ArrayList<ServerName>()))
      .thenReturn(RegionOpeningState.OPENED);
      Mockito.when(serverManager.sendRegionOpen(sn, REGIONINFO, -1, null))
      .thenReturn(RegionOpeningState.OPENED);
      Mockito.when(serverManager.addServerToDrainList(sn)).thenReturn(true);
    }

    Mockito.when(master.getServerManager()).thenReturn(serverManager);

    am = new AssignmentManager(server, serverManager, catalogTracker,
        balancer, startupMasterExecutor("mockExecutorService"), null, null);

    Mockito.when(master.getAssignmentManager()).thenReturn(am);
    Mockito.when(master.getZooKeeperWatcher()).thenReturn(zkWatcher);
    Mockito.when(master.getZooKeeper()).thenReturn(zkWatcher);
    
    am.addPlan(REGIONINFO.getEncodedName(), new RegionPlan(REGIONINFO, null, SERVERNAME_A));

    zkWatcher.registerListenerFirst(am);

    addServerToDrainedList(SERVERNAME_A, onlineServers, serverManager);

    am.assign(REGIONINFO, true);

    setRegionOpenedOnZK(zkWatcher, SERVERNAME_A, REGIONINFO);
    setRegionOpenedOnZK(zkWatcher, SERVERNAME_B, REGIONINFO);

    am.waitForAssignment(REGIONINFO);

    assertTrue(am.getRegionStates().isRegionOnline(REGIONINFO));
    assertNotEquals(am.getRegionStates().getRegionServerOfRegion(REGIONINFO), SERVERNAME_A);
  }

  @Test
  public void testAssignmentManagerDoesntUseDrainedServerWithBulkAssign() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    LoadBalancer balancer = LoadBalancerFactory.getLoadBalancer(conf);
    CatalogTracker catalogTracker = Mockito.mock(CatalogTracker.class);
    AssignmentManager am;
    final HMaster master = Mockito.mock(HMaster.class);
    final Server server = Mockito.mock(Server.class);
    final ServerManager serverManager = Mockito.mock(ServerManager.class);
    final ServerName SERVERNAME_A = ServerName.valueOf("mockserverbulk_a.org", 1000, 8000);
    final ServerName SERVERNAME_B = ServerName.valueOf("mockserverbulk_b.org", 1001, 8000);
    final ServerName SERVERNAME_C = ServerName.valueOf("mockserverbulk_c.org", 1002, 8000);
    final ServerName SERVERNAME_D = ServerName.valueOf("mockserverbulk_d.org", 1003, 8000);
    final ServerName SERVERNAME_E = ServerName.valueOf("mockserverbulk_e.org", 1004, 8000);
    final Map<HRegionInfo, ServerName> bulk = new HashMap<HRegionInfo, ServerName>();

    Set<ServerName> bunchServersAssigned = new HashSet<ServerName>();
    
    HRegionInfo REGIONINFO_A = new HRegionInfo(TableName.valueOf("table_A"),
        HConstants.EMPTY_START_ROW, HConstants.EMPTY_START_ROW);
    HRegionInfo REGIONINFO_B = new HRegionInfo(TableName.valueOf("table_B"),
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_START_ROW);
    HRegionInfo REGIONINFO_C = new HRegionInfo(TableName.valueOf("table_C"),
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_START_ROW);
    HRegionInfo REGIONINFO_D = new HRegionInfo(TableName.valueOf("table_D"),
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_START_ROW);
    HRegionInfo REGIONINFO_E = new HRegionInfo(TableName.valueOf("table_E"),
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_START_ROW);

    Map<ServerName, ServerLoad> onlineServers = new HashMap<ServerName, ServerLoad>();
    List<ServerName> drainedServers = new ArrayList<ServerName>();

    onlineServers.put(SERVERNAME_A, ServerLoad.EMPTY_SERVERLOAD);
    onlineServers.put(SERVERNAME_B, ServerLoad.EMPTY_SERVERLOAD);
    onlineServers.put(SERVERNAME_C, ServerLoad.EMPTY_SERVERLOAD);
    onlineServers.put(SERVERNAME_D, ServerLoad.EMPTY_SERVERLOAD);
    onlineServers.put(SERVERNAME_E, ServerLoad.EMPTY_SERVERLOAD);

    bulk.put(REGIONINFO_A, SERVERNAME_A);
    bulk.put(REGIONINFO_B, SERVERNAME_B);
    bulk.put(REGIONINFO_C, SERVERNAME_C);
    bulk.put(REGIONINFO_D, SERVERNAME_D);
    bulk.put(REGIONINFO_E, SERVERNAME_E);

    ZooKeeperWatcher zkWatcher = new ZooKeeperWatcher(TEST_UTIL.getConfiguration(),
        "zkWatcher-BulkAssignTest", abortable, true);

    Mockito.when(server.getConfiguration()).thenReturn(conf);
    Mockito.when(server.getServerName()).thenReturn(ServerName.valueOf("masterMock,1,1"));
    Mockito.when(server.getZooKeeper()).thenReturn(zkWatcher);

    Mockito.when(serverManager.getOnlineServers()).thenReturn(onlineServers);
    Mockito.when(serverManager.getOnlineServersList()).thenReturn(
      new ArrayList<ServerName>(onlineServers.keySet()));
    
    Mockito.when(serverManager.createDestinationServersList()).thenReturn(
      new ArrayList<ServerName>(onlineServers.keySet()));
    Mockito.when(serverManager.createDestinationServersList(null)).thenReturn(
      new ArrayList<ServerName>(onlineServers.keySet()));
    
    for (Entry<HRegionInfo, ServerName> entry : bulk.entrySet()) {
      Mockito.when(serverManager.isServerOnline(entry.getValue())).thenReturn(true);
      Mockito.when(serverManager.sendRegionClose(entry.getValue(), 
        entry.getKey(), -1)).thenReturn(true);
      Mockito.when(serverManager.sendRegionOpen(entry.getValue(), 
        entry.getKey(), -1, null)).thenReturn(RegionOpeningState.OPENED);  
      Mockito.when(serverManager.addServerToDrainList(entry.getValue())).thenReturn(true);
    }
    
    Mockito.when(master.getServerManager()).thenReturn(serverManager);

    drainedServers.add(SERVERNAME_A);
    drainedServers.add(SERVERNAME_B);
    drainedServers.add(SERVERNAME_C);
    drainedServers.add(SERVERNAME_D);

    am = new AssignmentManager(server, serverManager, catalogTracker,
      balancer, startupMasterExecutor("mockExecutorServiceBulk"), null, null);
    
    Mockito.when(master.getAssignmentManager()).thenReturn(am);

    zkWatcher.registerListener(am);
    
    for (ServerName drained : drainedServers) {
      addServerToDrainedList(drained, onlineServers, serverManager);
    }
    
    am.assign(bulk);

    Map<String, RegionState> regionsInTransition = am.getRegionStates().getRegionsInTransition();
    for (Entry<String, RegionState> entry : regionsInTransition.entrySet()) {
      setRegionOpenedOnZK(zkWatcher, entry.getValue().getServerName(), 
        entry.getValue().getRegion());
    }
    
    am.waitForAssignment(REGIONINFO_A);
    am.waitForAssignment(REGIONINFO_B);
    am.waitForAssignment(REGIONINFO_C);
    am.waitForAssignment(REGIONINFO_D);
    am.waitForAssignment(REGIONINFO_E);
    
    Map<HRegionInfo, ServerName> regionAssignments = am.getRegionStates().getRegionAssignments();
    for (Entry<HRegionInfo, ServerName> entry : regionAssignments.entrySet()) {
      LOG.info("Region Assignment: " 
          + entry.getKey().getRegionNameAsString() + " Server: " + entry.getValue());
      bunchServersAssigned.add(entry.getValue());
    }
    
    for (ServerName sn : drainedServers) {
      assertFalse(bunchServersAssigned.contains(sn));
    }
  }

  private void addServerToDrainedList(ServerName serverName, 
      Map<ServerName, ServerLoad> onlineServers, ServerManager serverManager) {
    onlineServers.remove(serverName);
    List<ServerName> availableServers = new ArrayList<ServerName>(onlineServers.keySet());
    Mockito.when(serverManager.createDestinationServersList()).thenReturn(availableServers);
    Mockito.when(serverManager.createDestinationServersList(null)).thenReturn(availableServers);
  }

  private void setRegionOpenedOnZK(final ZooKeeperWatcher zkWatcher, final ServerName serverName,
                                   HRegionInfo hregionInfo) throws Exception {
    int version = ZKAssign.getVersion(zkWatcher, hregionInfo);
    int versionTransition = ZKAssign.transitionNode(zkWatcher,
        hregionInfo, serverName, EventType.M_ZK_REGION_OFFLINE,
        EventType.RS_ZK_REGION_OPENING, version);
    ZKAssign.transitionNodeOpened(zkWatcher, hregionInfo, serverName, versionTransition);
  }

  private ExecutorService startupMasterExecutor(final String name) {
    ExecutorService executor = new ExecutorService(name);
    executor.startExecutorService(ExecutorType.MASTER_OPEN_REGION, 3);
    executor.startExecutorService(ExecutorType.MASTER_CLOSE_REGION, 3);
    executor.startExecutorService(ExecutorType.MASTER_SERVER_OPERATIONS, 3);
    executor.startExecutorService(ExecutorType.MASTER_META_SERVER_OPERATIONS, 3);
    return executor;
  }
}