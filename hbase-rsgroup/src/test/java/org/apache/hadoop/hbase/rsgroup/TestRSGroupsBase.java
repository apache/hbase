/**
 * Copyright The Apache Software Foundation
 *
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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseCluster;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.util.Bytes;

public abstract class TestRSGroupsBase {
  protected static final Log LOG = LogFactory.getLog(TestRSGroupsBase.class);

  //shared
  protected final static String groupPrefix = "Group";
  protected final static String tablePrefix = "Group";
  protected final static Random rand = new Random();

  //shared, cluster type specific
  protected static HBaseTestingUtility TEST_UTIL;
  protected static HBaseAdmin admin;
  protected static HBaseCluster cluster;
  protected static RSGroupAdmin rsGroupAdmin;
  protected static HMaster master;
  protected static boolean init = false;
  protected static RSGroupAdminEndpoint RSGroupAdminEndpoint;
  protected static CPMasterObserver observer;

  public final static long WAIT_TIMEOUT = 60000*5;
  public final static int NUM_SLAVES_BASE = 4; //number of slaves for the smallest cluster

  public static void setUpTestBeforeClass() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    TEST_UTIL.getConfiguration().setFloat(
            "hbase.master.balancer.stochastic.tableSkewCost", 6000);
    TEST_UTIL.getConfiguration().set(
        HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
        RSGroupBasedLoadBalancer.class.getName());
    TEST_UTIL.getConfiguration().set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        RSGroupAdminEndpoint.class.getName() + "," + CPMasterObserver.class.getName());
    TEST_UTIL.getConfiguration().setBoolean(
        HConstants.ZOOKEEPER_USEMULTI,
        true);
    TEST_UTIL.startMiniCluster(NUM_SLAVES_BASE - 1);
    TEST_UTIL.getConfiguration().setInt(
        ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART,
        NUM_SLAVES_BASE - 1);
    TEST_UTIL.getConfiguration().setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    initialize();
  }

  protected static void initialize() throws Exception {
    admin = TEST_UTIL.getHBaseAdmin();
    cluster = TEST_UTIL.getHBaseCluster();
    master = ((MiniHBaseCluster)cluster).getMaster();

    //wait for balancer to come online
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return master.isInitialized() &&
            ((RSGroupBasedLoadBalancer) master.getLoadBalancer()).isOnline();
      }
    });
    admin.setBalancerRunning(false,true);
    rsGroupAdmin = new VerifyingRSGroupAdminClient(new RSGroupAdminClient(TEST_UTIL.getConnection()),
        TEST_UTIL.getConfiguration());
    MasterCoprocessorHost host = master.getMasterCoprocessorHost();
    observer = (CPMasterObserver) host.findCoprocessor(CPMasterObserver.class.getName());
    RSGroupAdminEndpoint =
        host.findCoprocessors(RSGroupAdminEndpoint.class).get(0);
  }

  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  public void setUpBeforeMethod() throws Exception {
    if(!init) {
      init = true;
      tearDownAfterMethod();
    }
    observer.resetFlags();
  }

  public void tearDownAfterMethod() throws Exception {
    deleteTableIfNecessary();
    deleteNamespaceIfNecessary();
    deleteGroups();

    int missing = NUM_SLAVES_BASE - getNumServers();
    LOG.info("Restoring servers: "+missing);
    for(int i=0; i<missing; i++) {
      ((MiniHBaseCluster)cluster).startRegionServer();
    }

    rsGroupAdmin.addRSGroup("master");
    ServerName masterServerName =
        ((MiniHBaseCluster)cluster).getMaster().getServerName();

    try {
      rsGroupAdmin.moveServers(
          Sets.newHashSet(masterServerName.getAddress()),
          "master");
    } catch (Exception ex) {
      // ignore
    }

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

  // return the real number of region servers, excluding the master embedded region server in 2.0+
  protected int getNumServers() throws IOException {
    ClusterStatus status = admin.getClusterStatus();
    ServerName master = status.getMaster();
    int count = 0;
    for (ServerName sn : status.getServers()) {
      if (!sn.equals(master)) {
        count++;
      }
    }
    return count;
  }

  protected RSGroupInfo addGroup(RSGroupAdmin gAdmin, String groupName,
                                 int serverCount) throws IOException, InterruptedException {
    RSGroupInfo defaultInfo = gAdmin
        .getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP);
    gAdmin.addRSGroup(groupName);

    Set<Address> set = new HashSet<Address>();
    for(Address server: defaultInfo.getServers()) {
      if(set.size() == serverCount) {
        break;
      }
      set.add(server);
    }
    gAdmin.moveServers(set, groupName);
    RSGroupInfo result = gAdmin.getRSGroupInfo(groupName);
    return result;
  }

  static void removeGroup(RSGroupAdminClient groupAdmin, String groupName) throws IOException {
    RSGroupInfo info = groupAdmin.getRSGroupInfo(groupName);
    groupAdmin.moveTables(info.getTables(), RSGroupInfo.DEFAULT_GROUP);
    groupAdmin.moveServers(info.getServers(), RSGroupInfo.DEFAULT_GROUP);
    groupAdmin.removeRSGroup(groupName);
  }

  protected void deleteTableIfNecessary() throws IOException {
    for (HTableDescriptor desc : TEST_UTIL.getHBaseAdmin().listTables(tablePrefix+".*")) {
      TEST_UTIL.deleteTable(desc.getTableName());
    }
  }

  protected void deleteNamespaceIfNecessary() throws IOException {
    for (NamespaceDescriptor desc : TEST_UTIL.getHBaseAdmin().listNamespaceDescriptors()) {
      if(desc.getName().startsWith(tablePrefix)) {
        admin.deleteNamespace(desc.getName());
      }
    }
  }

  protected void deleteGroups() throws IOException {
    try (RSGroupAdmin groupAdmin = new RSGroupAdminClient(TEST_UTIL.getConnection())) {
      for(RSGroupInfo group: groupAdmin.listRSGroups()) {
        if(!group.getName().equals(RSGroupInfo.DEFAULT_GROUP)) {
          groupAdmin.moveTables(group.getTables(), RSGroupInfo.DEFAULT_GROUP);
          groupAdmin.moveServers(group.getServers(), RSGroupInfo.DEFAULT_GROUP);
          groupAdmin.removeRSGroup(group.getName());
        }
      }
    }
  }

  public Map<TableName, List<String>> getTableRegionMap() throws IOException {
    Map<TableName, List<String>> map = Maps.newTreeMap();
    Map<TableName, Map<ServerName, List<String>>> tableServerRegionMap
        = getTableServerRegionMap();
    for(TableName tableName : tableServerRegionMap.keySet()) {
      if(!map.containsKey(tableName)) {
        map.put(tableName, new LinkedList<String>());
      }
      for(List<String> subset: tableServerRegionMap.get(tableName).values()) {
        map.get(tableName).addAll(subset);
      }
    }
    return map;
  }

  public Map<TableName, Map<ServerName, List<String>>> getTableServerRegionMap()
      throws IOException {
    Map<TableName, Map<ServerName, List<String>>> map = Maps.newTreeMap();
    ClusterStatus status = TEST_UTIL.getHBaseClusterInterface().getClusterStatus();
    for(ServerName serverName : status.getServers()) {
      for(RegionLoad rl : status.getLoad(serverName).getRegionsLoad().values()) {
        TableName tableName = null;
        try {
          tableName = HRegionInfo.getTable(rl.getName());
        } catch (IllegalArgumentException e) {
          LOG.warn("Failed parse a table name from regionname=" +
              Bytes.toStringBinary(rl.getName()));
          continue;
        }
        if(!map.containsKey(tableName)) {
          map.put(tableName, new TreeMap<ServerName, List<String>>());
        }
        if(!map.get(tableName).containsKey(serverName)) {
          map.get(tableName).put(serverName, new LinkedList<String>());
        }
        map.get(tableName).get(serverName).add(rl.getNameAsString());
      }
    }
    return map;
  }

  protected String getGroupName(String baseName) {
    return groupPrefix+"_"+baseName+"_"+rand.nextInt(Integer.MAX_VALUE);
  }

  public static class CPMasterObserver extends BaseMasterObserver {
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
  }
}