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
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.HBaseCluster;
import org.apache.hadoop.hbase.HBaseTestingUtility;
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
import org.apache.hadoop.hbase.quotas.QuotaUtil;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

public abstract class TestRSGroupsBase {
  protected static final Logger LOG = LoggerFactory.getLogger(TestRSGroupsBase.class);

  // shared
  protected static final String GROUP_PREFIX = "Group";
  protected static final String TABLE_PREFIX = "Group";

  // shared, cluster type specific
  protected static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  protected static Admin ADMIN;
  protected static HBaseCluster CLUSTER;
  protected static HMaster MASTER;
  protected boolean INIT = false;
  protected static CPMasterObserver OBSERVER;
  protected static RSGroupAdminClient RS_GROUP_ADMIN_CLIENT;

  public final static long WAIT_TIMEOUT = 60000;
  public final static int NUM_SLAVES_BASE = 4; // number of slaves for the smallest cluster
  public static int NUM_DEAD_SERVERS = 0;

  // Per test variables
  @Rule
  public TestName name = new TestName();
  protected TableName tableName;

  public static String getNameWithoutIndex(String name) {
    return name.split("\\[")[0];
  }

  public static void setUpTestBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setFloat("hbase.master.balancer.stochastic.tableSkewCost", 6000);
    TEST_UTIL.getConfiguration().setBoolean(RSGroupInfoManager.RS_GROUP_ENABLED, true);
    TEST_UTIL.getConfiguration().set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
      RSGroupAdminEndpoint.class.getName() + "," + CPMasterObserver.class.getName());
    TEST_UTIL.getConfiguration().setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART,
      NUM_SLAVES_BASE - 1);
    TEST_UTIL.getConfiguration().setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    TEST_UTIL.getConfiguration().setInt("hbase.rpc.timeout", 100000);

    TEST_UTIL.startMiniCluster(NUM_SLAVES_BASE - 1);
    initialize();
  }

  protected static void initialize() throws Exception {
    ADMIN = new VerifyingRSGroupAdmin(TEST_UTIL.getConfiguration());
    CLUSTER = TEST_UTIL.getHBaseCluster();
    MASTER = TEST_UTIL.getMiniHBaseCluster().getMaster();

    // wait for balancer to come online
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return MASTER.isInitialized() &&
          ((RSGroupBasedLoadBalancer) MASTER.getLoadBalancer()).isOnline();
      }
    });
    ADMIN.balancerSwitch(false, true);
    MasterCoprocessorHost host = MASTER.getMasterCoprocessorHost();
    OBSERVER = (CPMasterObserver) host.findCoprocessor(CPMasterObserver.class.getName());
    RS_GROUP_ADMIN_CLIENT = new RSGroupAdminClient(TEST_UTIL.getConnection());
  }

  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  public void setUpBeforeMethod() throws Exception {
    LOG.info(name.getMethodName());
    tableName = TableName.valueOf(TABLE_PREFIX + "_" + name.getMethodName().split("\\[")[0]);
    if (!INIT) {
      INIT = true;
      tearDownAfterMethod();
    }
    OBSERVER.resetFlags();
  }

  public void tearDownAfterMethod() throws Exception {
    deleteTableIfNecessary();
    deleteNamespaceIfNecessary();
    deleteGroups();

    for (ServerName sn : ADMIN.listDecommissionedRegionServers()) {
      ADMIN.recommissionRegionServer(sn, null);
    }
    assertTrue(ADMIN.listDecommissionedRegionServers().isEmpty());

    int missing = NUM_SLAVES_BASE - getNumServers();
    LOG.info("Restoring servers: " + missing);
    for (int i = 0; i < missing; i++) {
      ((MiniHBaseCluster) CLUSTER).startRegionServer();
    }
    ADMIN.addRSGroup("master");
    ServerName masterServerName = ((MiniHBaseCluster) CLUSTER).getMaster().getServerName();
    try {
      ADMIN.moveServersToRSGroup(Sets.newHashSet(masterServerName.getAddress()), "master");
    } catch (Exception ex) {
      LOG.warn("Got this on setup, FYI", ex);
    }
    assertTrue(OBSERVER.preMoveServersCalled);
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        LOG.info("Waiting for cleanup to finish " + ADMIN.listRSGroups());
        // Might be greater since moving servers back to default
        // is after starting a server

        return ADMIN.getRSGroup(RSGroupInfo.DEFAULT_GROUP).getServers().size() == NUM_SLAVES_BASE;
      }
    });
  }

  protected final RSGroupInfo addGroup(String groupName, int serverCount)
    throws IOException, InterruptedException {
    RSGroupInfo defaultInfo = ADMIN.getRSGroup(RSGroupInfo.DEFAULT_GROUP);
    ADMIN.addRSGroup(groupName);
    Set<Address> set = new HashSet<>();
    for (Address server : defaultInfo.getServers()) {
      if (set.size() == serverCount) {
        break;
      }
      set.add(server);
    }
    ADMIN.moveServersToRSGroup(set, groupName);
    RSGroupInfo result = ADMIN.getRSGroup(groupName);
    return result;
  }

  protected final void removeGroup(String groupName) throws IOException {
    Set<TableName> tables = new HashSet<>();
    for (TableDescriptor td : ADMIN.listTableDescriptors(true)) {
      RSGroupInfo groupInfo = ADMIN.getRSGroup(td.getTableName());
      if (groupInfo != null && groupInfo.getName().equals(groupName)) {
        tables.add(td.getTableName());
      }
    }
    ADMIN.setRSGroup(tables, RSGroupInfo.DEFAULT_GROUP);
    RSGroupInfo groupInfo = ADMIN.getRSGroup(groupName);
    ADMIN.moveServersToRSGroup(groupInfo.getServers(), RSGroupInfo.DEFAULT_GROUP);
    ADMIN.removeRSGroup(groupName);
  }

  protected final void deleteTableIfNecessary() throws IOException {
    for (TableDescriptor desc : TEST_UTIL.getAdmin()
      .listTableDescriptors(Pattern.compile(TABLE_PREFIX + ".*"))) {
      TEST_UTIL.deleteTable(desc.getTableName());
    }
  }

  protected final void deleteNamespaceIfNecessary() throws IOException {
    for (NamespaceDescriptor desc : TEST_UTIL.getAdmin().listNamespaceDescriptors()) {
      if (desc.getName().startsWith(TABLE_PREFIX)) {
        ADMIN.deleteNamespace(desc.getName());
      }
    }
  }

  protected final void deleteGroups() throws IOException {
    for (RSGroupInfo groupInfo : ADMIN.listRSGroups()) {
      if (!groupInfo.getName().equals(RSGroupInfo.DEFAULT_GROUP)) {
        removeGroup(groupInfo.getName());
      }
    }
  }

  protected Map<TableName, List<String>> getTableRegionMap() throws IOException {
    Map<TableName, List<String>> map = Maps.newTreeMap();
    Map<TableName, Map<ServerName, List<String>>> tableServerRegionMap = getTableServerRegionMap();
    for (TableName tableName : tableServerRegionMap.keySet()) {
      if (!map.containsKey(tableName)) {
        map.put(tableName, new LinkedList<>());
      }
      for (List<String> subset : tableServerRegionMap.get(tableName).values()) {
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
    ClusterMetrics status = ADMIN.getClusterMetrics(EnumSet.of(Option.MASTER, Option.LIVE_SERVERS));
    ServerName masterName = status.getMasterName();
    int count = 0;
    for (ServerName sn : status.getLiveServerMetrics().keySet()) {
      if (!sn.equals(masterName)) {
        count++;
      }
    }
    return count;
  }

  protected final String getGroupName(String baseName) {
    return GROUP_PREFIX + "_" + getNameWithoutIndex(baseName) + "_" +
      ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);
  }

  /**
   * The server name in group does not contain the start code, this method will find out the start
   * code and construct the ServerName object.
   */
  protected final ServerName getServerName(Address addr) {
    return TEST_UTIL.getMiniHBaseCluster().getRegionServerThreads().stream()
      .map(t -> t.getRegionServer().getServerName()).filter(sn -> sn.getAddress().equals(addr))
      .findFirst().get();
  }

  protected final void toggleQuotaCheckAndRestartMiniCluster(boolean enable) throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    TEST_UTIL.getConfiguration().setBoolean(QuotaUtil.QUOTA_CONF_KEY, enable);
    TEST_UTIL.startMiniCluster(NUM_SLAVES_BASE - 1);
    TEST_UTIL.getConfiguration().setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART,
      NUM_SLAVES_BASE - 1);
    TEST_UTIL.getConfiguration().setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    initialize();
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
    boolean preGetRSGroupInfoCalled = false;
    boolean postGetRSGroupInfoCalled = false;
    boolean preGetRSGroupInfoOfTableCalled = false;
    boolean postGetRSGroupInfoOfTableCalled = false;
    boolean preListRSGroupsCalled = false;
    boolean postListRSGroupsCalled = false;
    boolean preGetRSGroupInfoOfServerCalled = false;
    boolean postGetRSGroupInfoOfServerCalled = false;
    boolean preSetRSGroupForTablesCalled = false;
    boolean postSetRSGroupForTablesCalled = false;

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
      preGetRSGroupInfoCalled = false;
      postGetRSGroupInfoCalled = false;
      preGetRSGroupInfoOfTableCalled = false;
      postGetRSGroupInfoOfTableCalled = false;
      preListRSGroupsCalled = false;
      postListRSGroupsCalled = false;
      preGetRSGroupInfoOfServerCalled = false;
      postGetRSGroupInfoOfServerCalled = false;
      preSetRSGroupForTablesCalled = false;
      postSetRSGroupForTablesCalled = false;
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
    public void preRemoveServers(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      Set<Address> servers) throws IOException {
      preRemoveServersCalled = true;
    }

    @Override
    public void postRemoveServers(final ObserverContext<MasterCoprocessorEnvironment> ctx,
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
    public void preAddRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx, String name)
      throws IOException {
      preAddRSGroupCalled = true;
    }

    @Override
    public void postAddRSGroup(final ObserverContext<MasterCoprocessorEnvironment> ctx, String name)
      throws IOException {
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
    public void preGetRSGroupInfo(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String groupName) throws IOException {
      preGetRSGroupInfoCalled = true;
    }

    @Override
    public void postGetRSGroupInfo(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final String groupName) throws IOException {
      postGetRSGroupInfoCalled = true;
    }

    @Override
    public void preGetRSGroupInfoOfTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {
      preGetRSGroupInfoOfTableCalled = true;
    }

    @Override
    public void postGetRSGroupInfoOfTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName) throws IOException {
      postGetRSGroupInfoOfTableCalled = true;
    }

    @Override
    public void preListRSGroups(final ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
      preListRSGroupsCalled = true;
    }

    @Override
    public void postListRSGroups(final ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
      postListRSGroupsCalled = true;
    }

    @Override
    public void preGetRSGroupInfoOfServer(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final Address server) throws IOException {
      preGetRSGroupInfoOfServerCalled = true;
    }

    @Override
    public void postGetRSGroupInfoOfServer(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final Address server) throws IOException {
      postGetRSGroupInfoOfServerCalled = true;
    }
  }

}
