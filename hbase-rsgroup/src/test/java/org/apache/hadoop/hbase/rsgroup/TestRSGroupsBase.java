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
package org.apache.hadoop.hbase.rsgroup;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.HBaseCluster;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.net.Address;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

public abstract class TestRSGroupsBase {
  protected static final Logger LOG = LoggerFactory.getLogger(TestRSGroupsBase.class);

  //shared
  protected final static String groupPrefix = "Group";
  protected final static String tablePrefix = "Group";
  protected final static Random rand = new Random();

  //shared, cluster type specific
  protected static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  protected static Admin admin;
  protected static HBaseCluster cluster;
  protected static RSGroupAdmin rsGroupAdmin;
  protected static HMaster master;
  protected boolean INIT = false;
  protected static RSGroupAdminEndpoint rsGroupAdminEndpoint;
  protected static CPMasterObserver observer;

  public final static long WAIT_TIMEOUT = 60000;
  public final static int NUM_SLAVES_BASE = 4; //number of slaves for the smallest cluster
  public static int NUM_DEAD_SERVERS = 0;

  // Per test variables
  @Rule
  public TestName name = new TestName();
  protected TableName tableName;

  public static void setUpTestBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setFloat(
            "hbase.master.balancer.stochastic.tableSkewCost", 6000);
    TEST_UTIL.getConfiguration().set(
        HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
        RSGroupBasedLoadBalancer.class.getName());
    TEST_UTIL.getConfiguration().set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        RSGroupAdminEndpoint.class.getName() + "," + CPMasterObserver.class.getName());
    TEST_UTIL.startMiniCluster(NUM_SLAVES_BASE - 1);
    TEST_UTIL.getConfiguration().setInt(
        ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART,
        NUM_SLAVES_BASE - 1);
    TEST_UTIL.getConfiguration().setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    initialize();
  }

  protected static void initialize() throws Exception {
    admin = TEST_UTIL.getAdmin();
    cluster = TEST_UTIL.getHBaseCluster();
    master = TEST_UTIL.getMiniHBaseCluster().getMaster();

    //wait for balancer to come online
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return master.isInitialized() &&
            ((RSGroupBasedLoadBalancer) master.getLoadBalancer()).isOnline();
      }
    });
    admin.balancerSwitch(false, true);
    rsGroupAdmin = new VerifyingRSGroupAdminClient(
        new RSGroupAdminClient(TEST_UTIL.getConnection()), TEST_UTIL.getConfiguration());
    MasterCoprocessorHost host = master.getMasterCoprocessorHost();
    observer = (CPMasterObserver) host.findCoprocessor(CPMasterObserver.class.getName());
    rsGroupAdminEndpoint = (RSGroupAdminEndpoint)
        host.findCoprocessor(RSGroupAdminEndpoint.class.getName());
  }

  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  public void setUpBeforeMethod() throws Exception {
    LOG.info(name.getMethodName());
    tableName = TableName.valueOf(tablePrefix + "_" + name.getMethodName());
    if (!INIT) {
      INIT = true;
      tearDownAfterMethod();
    }
    observer.resetFlags();
  }

  public void tearDownAfterMethod() throws Exception {
    deleteTableIfNecessary();
    deleteNamespaceIfNecessary();
    deleteGroups();

    for(ServerName sn : admin.listDecommissionedRegionServers()){
      admin.recommissionRegionServer(sn, null);
    }
    assertTrue(admin.listDecommissionedRegionServers().isEmpty());

    int missing = NUM_SLAVES_BASE - getNumServers();
    LOG.info("Restoring servers: "+missing);
    for(int i=0; i<missing; i++) {
      ((MiniHBaseCluster)cluster).startRegionServer();
    }

    rsGroupAdmin.addRSGroup("master");
    ServerName masterServerName =
        ((MiniHBaseCluster)cluster).getMaster().getServerName();

    try {
      rsGroupAdmin.moveServers(Sets.newHashSet(masterServerName.getAddress()), "master");
    } catch (Exception ex) {
      LOG.warn("Got this on setup, FYI", ex);
    }
    assertTrue(observer.preMoveServersCalled);
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        LOG.info("Waiting for cleanup to finish " + rsGroupAdmin.listRSGroups());
        //Might be greater since moving servers back to default
        //is after starting a server

        return rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers().size()
            == NUM_SLAVES_BASE;
      }
    });
  }

  protected RSGroupInfo addGroup(String groupName, int serverCount)
      throws IOException, InterruptedException {
    RSGroupInfo defaultInfo = rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP);
    rsGroupAdmin.addRSGroup(groupName);
    Set<Address> set = new HashSet<>();
    for(Address server: defaultInfo.getServers()) {
      if(set.size() == serverCount) {
        break;
      }
      set.add(server);
    }
    rsGroupAdmin.moveServers(set, groupName);
    RSGroupInfo result = rsGroupAdmin.getRSGroupInfo(groupName);
    return result;
  }

  protected void removeGroup(String groupName) throws IOException {
    RSGroupInfo groupInfo = rsGroupAdmin.getRSGroupInfo(groupName);
    rsGroupAdmin.moveTables(groupInfo.getTables(), RSGroupInfo.DEFAULT_GROUP);
    rsGroupAdmin.moveServers(groupInfo.getServers(), RSGroupInfo.DEFAULT_GROUP);
    rsGroupAdmin.removeRSGroup(groupName);
  }

  protected void deleteTableIfNecessary() throws IOException {
    for (TableDescriptor desc : TEST_UTIL.getAdmin()
      .listTableDescriptors(Pattern.compile(tablePrefix + ".*"))) {
      TEST_UTIL.deleteTable(desc.getTableName());
    }
  }

  protected void deleteNamespaceIfNecessary() throws IOException {
    for (NamespaceDescriptor desc : TEST_UTIL.getAdmin().listNamespaceDescriptors()) {
      if(desc.getName().startsWith(tablePrefix)) {
        admin.deleteNamespace(desc.getName());
      }
    }
  }

  protected void deleteGroups() throws IOException {
    RSGroupAdmin groupAdmin = new RSGroupAdminClient(TEST_UTIL.getConnection());
    for(RSGroupInfo group: groupAdmin.listRSGroups()) {
      if(!group.getName().equals(RSGroupInfo.DEFAULT_GROUP)) {
        groupAdmin.moveTables(group.getTables(), RSGroupInfo.DEFAULT_GROUP);
        groupAdmin.moveServers(group.getServers(), RSGroupInfo.DEFAULT_GROUP);
        groupAdmin.removeRSGroup(group.getName());
      }
    }
  }

  protected Map<TableName, List<String>> getTableRegionMap() throws IOException {
    Map<TableName, List<String>> map = Maps.newTreeMap();
    Map<TableName, Map<ServerName, List<String>>> tableServerRegionMap
        = getTableServerRegionMap();
    for(TableName tableName : tableServerRegionMap.keySet()) {
      if(!map.containsKey(tableName)) {
        map.put(tableName, new LinkedList<>());
      }
      for(List<String> subset: tableServerRegionMap.get(tableName).values()) {
        map.get(tableName).addAll(subset);
      }
    }
    return map;
  }

  protected Map<TableName, Map<ServerName, List<String>>> getTableServerRegionMap()
    throws IOException {
    Map<TableName, Map<ServerName, List<String>>> map = Maps.newTreeMap();
    Admin admin = TEST_UTIL.getAdmin();
    ClusterMetrics metrics =
      admin.getClusterMetrics(EnumSet.of(ClusterMetrics.Option.SERVERS_NAME));
    for (ServerName serverName : metrics.getServersName()) {
      for (RegionInfo region : admin.getRegions(serverName)) {
        TableName tableName = region.getTable();
        map.computeIfAbsent(tableName, k -> new TreeMap<>())
          .computeIfAbsent(serverName, k -> new ArrayList<>()).add(region.getRegionNameAsString());
      }
    }
    return map;
  }

  // return the real number of region servers, excluding the master embedded region server in 2.0+
  protected int getNumServers() throws IOException {
    ClusterMetrics status =
        admin.getClusterMetrics(EnumSet.of(Option.MASTER, Option.LIVE_SERVERS));
    ServerName masterName = status.getMasterName();
    int count = 0;
    for (ServerName sn : status.getLiveServerMetrics().keySet()) {
      if (!sn.equals(masterName)) {
        count++;
      }
    }
    return count;
  }

  protected String getGroupName(String baseName) {
    return groupPrefix + "_" + baseName + "_" + rand.nextInt(Integer.MAX_VALUE);
  }

  /**
   * The server name in group does not contain the start code, this method will find out the start
   * code and construct the ServerName object.
   */
  protected ServerName getServerName(Address addr) {
    return TEST_UTIL.getMiniHBaseCluster().getRegionServerThreads().stream()
      .map(t -> t.getRegionServer().getServerName()).filter(sn -> sn.getAddress().equals(addr))
      .findFirst().get();
  }

  public static class CPMasterObserver implements MasterCoprocessor, MasterObserver {
    boolean preBalanceRSGroupCalled = false;
    boolean postBalanceRSGroupCalled = false;
    boolean preMoveServersCalled = false;
    boolean postMoveServersCalled = false;
    boolean preMoveTablesCalled = false;
    boolean postMoveTablesCalled = false;
    boolean preAddRSGroupCalled = false;
    boolean postAddRSGroupCalled = false;
    boolean preRemoveRSGroupCalled = false;
    boolean postRemoveRSGroupCalled = false;
    boolean preRemoveServersCalled = false;
    boolean postRemoveServersCalled = false;
    boolean preMoveServersAndTables = false;
    boolean postMoveServersAndTables = false;
    boolean preRenameRSGroupCalled = false;
    boolean postRenameRSGroupCalled = false;

    public void resetFlags() {
      preBalanceRSGroupCalled = false;
      postBalanceRSGroupCalled = false;
      preMoveServersCalled = false;
      postMoveServersCalled = false;
      preMoveTablesCalled = false;
      postMoveTablesCalled = false;
      preAddRSGroupCalled = false;
      postAddRSGroupCalled = false;
      preRemoveRSGroupCalled = false;
      postRemoveRSGroupCalled = false;
      preRemoveServersCalled = false;
      postRemoveServersCalled = false;
      preMoveServersAndTables = false;
      postMoveServersAndTables = false;
      preRenameRSGroupCalled = false;
      postRenameRSGroupCalled = false;
    }

    @Override
    public Optional<MasterObserver> getMasterObserver() {
      return Optional.of(this);
    }

    @Override
    public void preMoveServersAndTables(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        Set<Address> servers, Set<TableName> tables, String targetGroup) throws IOException {
      preMoveServersAndTables = true;
    }

    @Override
    public void postMoveServersAndTables(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        Set<Address> servers, Set<TableName> tables, String targetGroup) throws IOException {
      postMoveServersAndTables = true;
    }

    @Override
    public void preRemoveServers(
        final ObserverContext<MasterCoprocessorEnvironment> ctx,
        Set<Address> servers) throws IOException {
      preRemoveServersCalled = true;
    }

    @Override
    public void postRemoveServers(
        final ObserverContext<MasterCoprocessorEnvironment> ctx,
        Set<Address> servers) throws IOException {
      postRemoveServersCalled = true;
    }

    @Override
    public void preRemoveRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        String name) throws IOException {
      preRemoveRSGroupCalled = true;
    }

    @Override
    public void postRemoveRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        String name) throws IOException {
      postRemoveRSGroupCalled = true;
    }

    @Override
    public void preAddRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        String name) throws IOException {
      preAddRSGroupCalled = true;
    }

    @Override
    public void postAddRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        String name) throws IOException {
      postAddRSGroupCalled = true;
    }

    @Override
    public void preMoveTables(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        Set<TableName> tables, String targetGroup) throws IOException {
      preMoveTablesCalled = true;
    }

    @Override
    public void postMoveTables(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        Set<TableName> tables, String targetGroup) throws IOException {
      postMoveTablesCalled = true;
    }

    @Override
    public void preMoveServers(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        Set<Address> servers, String targetGroup) throws IOException {
      preMoveServersCalled = true;
    }

    @Override
    public void postMoveServers(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        Set<Address> servers, String targetGroup) throws IOException {
      postMoveServersCalled = true;
    }

    @Override
    public void preBalanceRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        String groupName) throws IOException {
      preBalanceRSGroupCalled = true;
    }

    @Override
    public void postBalanceRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx,
        String groupName, boolean balancerRan) throws IOException {
      postBalanceRSGroupCalled = true;
    }

    @Override
    public void preRenameRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx,
        String oldName, String newName) throws IOException {
      preRenameRSGroupCalled = true;
    }

    @Override
    public void postRenameRSGroup(ObserverContext<MasterCoprocessorEnvironment> ctx,
        String oldName, String newName) throws IOException {
      postRenameRSGroupCalled = true;
    }
  }

}
