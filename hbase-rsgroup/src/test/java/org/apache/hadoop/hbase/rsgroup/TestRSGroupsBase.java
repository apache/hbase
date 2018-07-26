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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.HBaseCluster;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetServerInfoRequest;

public abstract class TestRSGroupsBase {
  protected static final Logger LOG = LoggerFactory.getLogger(TestRSGroupsBase.class);
  @Rule
  public TestName name = new TestName();

  //shared
  protected final static String groupPrefix = "Group";
  protected final static String tablePrefix = "Group";
  protected final static SecureRandom rand = new SecureRandom();

  //shared, cluster type specific
  protected static HBaseTestingUtility TEST_UTIL;
  protected static Admin admin;
  protected static HBaseCluster cluster;
  protected static RSGroupAdmin rsGroupAdmin;
  protected static HMaster master;

  public final static long WAIT_TIMEOUT = 60000*5;
  public final static int NUM_SLAVES_BASE = 4; //number of slaves for the smallest cluster
  public static int NUM_DEAD_SERVERS = 0;

  // Per test variables
  TableName tableName;
  @Before
  public void setup() {
    LOG.info(name.getMethodName());
    tableName = TableName.valueOf(tablePrefix + "_" + name.getMethodName());
  }

  protected RSGroupInfo addGroup(String groupName, int serverCount)
      throws IOException, InterruptedException {
    RSGroupInfo defaultInfo = rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP);
    assertTrue(defaultInfo != null);
    assertTrue(defaultInfo.getServers().size() >= serverCount);
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
    assertTrue(result.getServers().size() >= serverCount);
    return result;
  }

  void removeGroup(String groupName) throws IOException {
    RSGroupInfo RSGroupInfo = rsGroupAdmin.getRSGroupInfo(groupName);
    rsGroupAdmin.moveTables(RSGroupInfo.getTables(), RSGroupInfo.DEFAULT_GROUP);
    rsGroupAdmin.moveServers(RSGroupInfo.getServers(), RSGroupInfo.DEFAULT_GROUP);
    rsGroupAdmin.removeRSGroup(groupName);
  }

  protected void deleteTableIfNecessary() throws IOException {
    for (HTableDescriptor desc : TEST_UTIL.getAdmin().listTables(tablePrefix+".*")) {
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
    RSGroupAdmin groupAdmin =
        new RSGroupAdminClient(TEST_UTIL.getConnection());
    for(RSGroupInfo group: groupAdmin.listRSGroups()) {
      if(!group.getName().equals(RSGroupInfo.DEFAULT_GROUP)) {
        groupAdmin.moveTables(group.getTables(), RSGroupInfo.DEFAULT_GROUP);
        groupAdmin.moveServers(group.getServers(), RSGroupInfo.DEFAULT_GROUP);
        groupAdmin.removeRSGroup(group.getName());
      }
    }
  }

  public Map<TableName, List<String>> getTableRegionMap() throws IOException {
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

  public Map<TableName, Map<ServerName, List<String>>> getTableServerRegionMap()
      throws IOException {
    Map<TableName, Map<ServerName, List<String>>> map = Maps.newTreeMap();
    ClusterMetrics status = TEST_UTIL.getHBaseClusterInterface().getClusterMetrics();
    for (Map.Entry<ServerName, ServerMetrics> entry : status.getLiveServerMetrics().entrySet()) {
      ServerName serverName = entry.getKey();
      for(RegionMetrics rl : entry.getValue().getRegionMetrics().values()) {
        TableName tableName = null;
        try {
          tableName = RegionInfo.getTable(rl.getRegionName());
        } catch (IllegalArgumentException e) {
          LOG.warn("Failed parse a table name from regionname=" +
            Bytes.toStringBinary(rl.getRegionName()));
          continue;
        }
        if(!map.containsKey(tableName)) {
          map.put(tableName, new TreeMap<>());
        }
        if(!map.get(tableName).containsKey(serverName)) {
          map.get(tableName).put(serverName, new LinkedList<>());
        }
        map.get(tableName).get(serverName).add(rl.getNameAsString());
      }
    }
    return map;
  }

  @Test
  public void testBogusArgs() throws Exception {
    assertNull(rsGroupAdmin.getRSGroupInfoOfTable(TableName.valueOf("nonexistent")));
    assertNull(rsGroupAdmin.getRSGroupOfServer(Address.fromParts("bogus",123)));
    assertNull(rsGroupAdmin.getRSGroupInfo("bogus"));

    try {
      rsGroupAdmin.removeRSGroup("bogus");
      fail("Expected removing bogus group to fail");
    } catch(ConstraintException ex) {
      //expected
    }

    try {
      rsGroupAdmin.moveTables(Sets.newHashSet(TableName.valueOf("bogustable")), "bogus");
      fail("Expected move with bogus group to fail");
    } catch(ConstraintException|TableNotFoundException ex) {
      //expected
    }

    try {
      rsGroupAdmin.moveServers(Sets.newHashSet(Address.fromParts("bogus",123)), "bogus");
      fail("Expected move with bogus group to fail");
    } catch(ConstraintException ex) {
      //expected
    }

    try {
      admin.setBalancerRunning(true,true);
      rsGroupAdmin.balanceRSGroup("bogus");
      admin.setBalancerRunning(false,true);
      fail("Expected move with bogus group to fail");
    } catch(ConstraintException ex) {
      //expected
    }
  }

  @Test
  public void testCreateMultiRegion() throws IOException {
    byte[] end = {1,3,5,7,9};
    byte[] start = {0,2,4,6,8};
    byte[][] f = {Bytes.toBytes("f")};
    TEST_UTIL.createTable(tableName, f,1,start,end,10);
  }

  @Test
  public void testCreateAndDrop() throws Exception {
    TEST_UTIL.createTable(tableName, Bytes.toBytes("cf"));
    //wait for created table to be assigned
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return getTableRegionMap().get(tableName) != null;
      }
    });
    TEST_UTIL.deleteTable(tableName);
  }


  @Test
  public void testSimpleRegionServerMove() throws IOException,
      InterruptedException {
    int initNumGroups = rsGroupAdmin.listRSGroups().size();
    RSGroupInfo appInfo = addGroup(getGroupName(name.getMethodName()), 1);
    RSGroupInfo adminInfo = addGroup(getGroupName(name.getMethodName()), 1);
    RSGroupInfo dInfo = rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP);
    Assert.assertEquals(initNumGroups + 2, rsGroupAdmin.listRSGroups().size());
    assertEquals(1, adminInfo.getServers().size());
    assertEquals(1, appInfo.getServers().size());
    assertEquals(getNumServers() - 2, dInfo.getServers().size());
    rsGroupAdmin.moveServers(appInfo.getServers(),
        RSGroupInfo.DEFAULT_GROUP);
    rsGroupAdmin.removeRSGroup(appInfo.getName());
    rsGroupAdmin.moveServers(adminInfo.getServers(),
        RSGroupInfo.DEFAULT_GROUP);
    rsGroupAdmin.removeRSGroup(adminInfo.getName());
    Assert.assertEquals(rsGroupAdmin.listRSGroups().size(), initNumGroups);
  }

  // return the real number of region servers, excluding the master embedded region server in 2.0+
  public int getNumServers() throws IOException {
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

  @Test
  public void testMoveServers() throws Exception {
    //create groups and assign servers
    addGroup("bar", 3);
    rsGroupAdmin.addRSGroup("foo");

    RSGroupInfo barGroup = rsGroupAdmin.getRSGroupInfo("bar");
    RSGroupInfo fooGroup = rsGroupAdmin.getRSGroupInfo("foo");
    assertEquals(3, barGroup.getServers().size());
    assertEquals(0, fooGroup.getServers().size());

    //test fail bogus server move
    try {
      rsGroupAdmin.moveServers(Sets.newHashSet(Address.fromString("foo:9999")),"foo");
      fail("Bogus servers shouldn't have been successfully moved.");
    } catch(IOException ex) {
      String exp = "Source RSGroup for server foo:9999 does not exist.";
      String msg = "Expected '"+exp+"' in exception message: ";
      assertTrue(msg+" "+ex.getMessage(), ex.getMessage().contains(exp));
    }

    //test success case
    LOG.info("moving servers "+barGroup.getServers()+" to group foo");
    rsGroupAdmin.moveServers(barGroup.getServers(), fooGroup.getName());

    barGroup = rsGroupAdmin.getRSGroupInfo("bar");
    fooGroup = rsGroupAdmin.getRSGroupInfo("foo");
    assertEquals(0,barGroup.getServers().size());
    assertEquals(3,fooGroup.getServers().size());

    LOG.info("moving servers "+fooGroup.getServers()+" to group default");
    rsGroupAdmin.moveServers(fooGroup.getServers(), RSGroupInfo.DEFAULT_GROUP);

    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return getNumServers() ==
        rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers().size();
      }
    });

    fooGroup = rsGroupAdmin.getRSGroupInfo("foo");
    assertEquals(0,fooGroup.getServers().size());

    //test group removal
    LOG.info("Remove group "+barGroup.getName());
    rsGroupAdmin.removeRSGroup(barGroup.getName());
    Assert.assertEquals(null, rsGroupAdmin.getRSGroupInfo(barGroup.getName()));
    LOG.info("Remove group "+fooGroup.getName());
    rsGroupAdmin.removeRSGroup(fooGroup.getName());
    Assert.assertEquals(null, rsGroupAdmin.getRSGroupInfo(fooGroup.getName()));
  }

  @Test
  public void testMoveServersFromDefaultGroup() throws Exception {
    //create groups and assign servers
    rsGroupAdmin.addRSGroup("foo");

    RSGroupInfo fooGroup = rsGroupAdmin.getRSGroupInfo("foo");
    assertEquals(0, fooGroup.getServers().size());
    RSGroupInfo defaultGroup = rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP);

    //test remove all servers from default
    try {
      rsGroupAdmin.moveServers(defaultGroup.getServers(), fooGroup.getName());
      fail(RSGroupAdminServer.KEEP_ONE_SERVER_IN_DEFAULT_ERROR_MESSAGE);
    } catch (ConstraintException ex) {
      assertTrue(ex.getMessage().contains(RSGroupAdminServer
              .KEEP_ONE_SERVER_IN_DEFAULT_ERROR_MESSAGE));
    }

    //test success case, remove one server from default ,keep at least one server
    if (defaultGroup.getServers().size() > 1) {
      Address serverInDefaultGroup = defaultGroup.getServers().iterator().next();
      LOG.info("moving server " + serverInDefaultGroup + " from group default to group " +
              fooGroup.getName());
      rsGroupAdmin.moveServers(Sets.newHashSet(serverInDefaultGroup), fooGroup.getName());
    }

    fooGroup = rsGroupAdmin.getRSGroupInfo("foo");
    LOG.info("moving servers " + fooGroup.getServers() + " to group default");
    rsGroupAdmin.moveServers(fooGroup.getServers(), RSGroupInfo.DEFAULT_GROUP);

    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return getNumServers() ==
                rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers().size();
      }
    });

    fooGroup = rsGroupAdmin.getRSGroupInfo("foo");
    assertEquals(0, fooGroup.getServers().size());

    //test group removal
    LOG.info("Remove group " + fooGroup.getName());
    rsGroupAdmin.removeRSGroup(fooGroup.getName());
    Assert.assertEquals(null, rsGroupAdmin.getRSGroupInfo(fooGroup.getName()));
  }

  @Test
  public void testTableMoveTruncateAndDrop() throws Exception {
    final byte[] familyNameBytes = Bytes.toBytes("f");
    String newGroupName = getGroupName(name.getMethodName());
    final RSGroupInfo newGroup = addGroup(newGroupName, 2);

    TEST_UTIL.createMultiRegionTable(tableName, familyNameBytes, 5);
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        List<String> regions = getTableRegionMap().get(tableName);
        if (regions == null) {
          return false;
        }

        return getTableRegionMap().get(tableName).size() >= 5;
      }
    });

    RSGroupInfo tableGrp = rsGroupAdmin.getRSGroupInfoOfTable(tableName);
    assertTrue(tableGrp.getName().equals(RSGroupInfo.DEFAULT_GROUP));

    //change table's group
    LOG.info("Moving table "+tableName+" to "+newGroup.getName());
    rsGroupAdmin.moveTables(Sets.newHashSet(tableName), newGroup.getName());

    //verify group change
    Assert.assertEquals(newGroup.getName(),
        rsGroupAdmin.getRSGroupInfoOfTable(tableName).getName());

    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        Map<ServerName, List<String>> serverMap = getTableServerRegionMap().get(tableName);
        int count = 0;
        if (serverMap != null) {
          for (ServerName rs : serverMap.keySet()) {
            if (newGroup.containsServer(rs.getAddress())) {
              count += serverMap.get(rs).size();
            }
          }
        }
        return count == 5;
      }
    });

    //test truncate
    admin.disableTable(tableName);
    admin.truncateTable(tableName, true);
    Assert.assertEquals(1, rsGroupAdmin.getRSGroupInfo(newGroup.getName()).getTables().size());
    Assert.assertEquals(tableName, rsGroupAdmin.getRSGroupInfo(
        newGroup.getName()).getTables().first());

    //verify removed table is removed from group
    TEST_UTIL.deleteTable(tableName);
    Assert.assertEquals(0, rsGroupAdmin.getRSGroupInfo(newGroup.getName()).getTables().size());
  }

  @Test
  public void testGroupBalance() throws Exception {
    LOG.info(name.getMethodName());
    String newGroupName = getGroupName(name.getMethodName());
    final RSGroupInfo newGroup = addGroup(newGroupName, 3);

    final TableName tableName = TableName.valueOf(tablePrefix+"_ns", name.getMethodName());
    admin.createNamespace(
        NamespaceDescriptor.create(tableName.getNamespaceAsString())
            .addConfiguration(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP, newGroupName).build());
    final byte[] familyNameBytes = Bytes.toBytes("f");
    final HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor("f"));
    byte [] startKey = Bytes.toBytes("aaaaa");
    byte [] endKey = Bytes.toBytes("zzzzz");
    admin.createTable(desc, startKey, endKey, 6);
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        List<String> regions = getTableRegionMap().get(tableName);
        if (regions == null) {
          return false;
        }
        return regions.size() >= 6;
      }
    });

    //make assignment uneven, move all regions to one server
    Map<ServerName,List<String>> assignMap =
        getTableServerRegionMap().get(tableName);
    final ServerName first = assignMap.entrySet().iterator().next().getKey();
    for(RegionInfo region: admin.getTableRegions(tableName)) {
      if(!assignMap.get(first).contains(region.getRegionNameAsString())) {
        admin.move(region.getEncodedNameAsBytes(), Bytes.toBytes(first.getServerName()));
      }
    }
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        Map<ServerName, List<String>> map = getTableServerRegionMap().get(tableName);
        if (map == null) {
          return true;
        }
        List<String> regions = map.get(first);
        if (regions == null) {
          return true;
        }
        return regions.size() >= 6;
      }
    });

    //balance the other group and make sure it doesn't affect the new group
    admin.setBalancerRunning(true,true);
    rsGroupAdmin.balanceRSGroup(RSGroupInfo.DEFAULT_GROUP);
    assertEquals(6, getTableServerRegionMap().get(tableName).get(first).size());

    //disable balance, balancer will not be run and return false
    admin.setBalancerRunning(false,true);
    assertFalse(rsGroupAdmin.balanceRSGroup(newGroupName));
    assertEquals(6, getTableServerRegionMap().get(tableName).get(first).size());

    //enable balance
    admin.setBalancerRunning(true,true);
    rsGroupAdmin.balanceRSGroup(newGroupName);
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        for (List<String> regions : getTableServerRegionMap().get(tableName).values()) {
          if (2 != regions.size()) {
            return false;
          }
        }
        return true;
      }
    });
    admin.setBalancerRunning(false,true);
  }

  @Test
  public void testRegionMove() throws Exception {
    final RSGroupInfo newGroup = addGroup(getGroupName(name.getMethodName()), 1);
    final byte[] familyNameBytes = Bytes.toBytes("f");
    // All the regions created below will be assigned to the default group.
    TEST_UTIL.createMultiRegionTable(tableName, familyNameBytes, 6);
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        List<String> regions = getTableRegionMap().get(tableName);
        if (regions == null) {
          return false;
        }

        return getTableRegionMap().get(tableName).size() >= 6;
      }
    });

    //get target region to move
    Map<ServerName,List<String>> assignMap =
        getTableServerRegionMap().get(tableName);
    String targetRegion = null;
    for(ServerName server : assignMap.keySet()) {
      targetRegion = assignMap.get(server).size() > 0 ? assignMap.get(server).get(0) : null;
      if(targetRegion != null) {
        break;
      }
    }
    //get server which is not a member of new group
    ServerName targetServer = null;
    for (ServerName server : admin.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS))
                                  .getLiveServerMetrics().keySet()) {
      if (!newGroup.containsServer(server.getAddress())) {
        targetServer = server;
        break;
      }
    }

    final AdminProtos.AdminService.BlockingInterface targetRS =
      ((ClusterConnection) admin.getConnection()).getAdmin(targetServer);

    //move target server to group
    rsGroupAdmin.moveServers(Sets.newHashSet(targetServer.getAddress()),
        newGroup.getName());
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return ProtobufUtil.getOnlineRegions(targetRS).size() <= 0;
      }
    });

    // Lets move this region to the new group.
    TEST_UTIL.getAdmin().move(Bytes.toBytes(RegionInfo.encodeRegionName(
            Bytes.toBytes(targetRegion))), Bytes.toBytes(targetServer.getServerName()));
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return
            getTableRegionMap().get(tableName) != null &&
                getTableRegionMap().get(tableName).size() == 6 &&
                admin.getClusterMetrics(EnumSet.of(Option.REGIONS_IN_TRANSITION))
                     .getRegionStatesInTransition().size() < 1;
      }
    });

    //verify that targetServer didn't open it
    for (RegionInfo region: ProtobufUtil.getOnlineRegions(targetRS)) {
      if (targetRegion.equals(region.getRegionNameAsString())) {
        fail("Target server opened region");
      }
    }
  }

  @Test
  public void testFailRemoveGroup() throws IOException, InterruptedException {
    int initNumGroups = rsGroupAdmin.listRSGroups().size();
    addGroup("bar", 3);
    TEST_UTIL.createTable(tableName, Bytes.toBytes("f"));
    rsGroupAdmin.moveTables(Sets.newHashSet(tableName), "bar");
    RSGroupInfo barGroup = rsGroupAdmin.getRSGroupInfo("bar");
    //group is not empty therefore it should fail
    try {
      rsGroupAdmin.removeRSGroup(barGroup.getName());
      fail("Expected remove group to fail");
    } catch(IOException e) {
    }
    //group cannot lose all it's servers therefore it should fail
    try {
      rsGroupAdmin.moveServers(barGroup.getServers(), RSGroupInfo.DEFAULT_GROUP);
      fail("Expected move servers to fail");
    } catch(IOException e) {
    }

    rsGroupAdmin.moveTables(barGroup.getTables(), RSGroupInfo.DEFAULT_GROUP);
    try {
      rsGroupAdmin.removeRSGroup(barGroup.getName());
      fail("Expected move servers to fail");
    } catch(IOException e) {
    }

    rsGroupAdmin.moveServers(barGroup.getServers(), RSGroupInfo.DEFAULT_GROUP);
    rsGroupAdmin.removeRSGroup(barGroup.getName());

    Assert.assertEquals(initNumGroups, rsGroupAdmin.listRSGroups().size());
  }

  @Test
  public void testKillRS() throws Exception {
    RSGroupInfo appInfo = addGroup("appInfo", 1);

    final TableName tableName = TableName.valueOf(tablePrefix+"_ns", name.getMethodName());
    admin.createNamespace(
        NamespaceDescriptor.create(tableName.getNamespaceAsString())
            .addConfiguration(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP, appInfo.getName()).build());
    final HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor("f"));
    admin.createTable(desc);
    //wait for created table to be assigned
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return getTableRegionMap().get(desc.getTableName()) != null;
      }
    });

    ServerName targetServer = ServerName.parseServerName(
        appInfo.getServers().iterator().next().toString());
    AdminProtos.AdminService.BlockingInterface targetRS =
      ((ClusterConnection) admin.getConnection()).getAdmin(targetServer);
    RegionInfo targetRegion = ProtobufUtil.getOnlineRegions(targetRS).get(0);
    Assert.assertEquals(1, ProtobufUtil.getOnlineRegions(targetRS).size());

    try {
      //stopping may cause an exception
      //due to the connection loss
      targetRS.stopServer(null,
          AdminProtos.StopServerRequest.newBuilder().setReason("Die").build());
    } catch(Exception e) {
    }
    assertFalse(cluster.getClusterMetrics().getLiveServerMetrics().containsKey(targetServer));

    //wait for created table to be assigned
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return cluster.getClusterMetrics().getRegionStatesInTransition().isEmpty();
      }
    });
    Set<Address> newServers = Sets.newHashSet();
    newServers.add(
        rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers().iterator().next());
    rsGroupAdmin.moveServers(newServers, appInfo.getName());

    //Make sure all the table's regions get reassigned
    //disabling the table guarantees no conflicting assign/unassign (ie SSH) happens
    admin.disableTable(tableName);
    admin.enableTable(tableName);

    //wait for region to be assigned
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return cluster.getClusterMetrics().getRegionStatesInTransition().isEmpty();
      }
    });

    targetServer = ServerName.parseServerName(
        newServers.iterator().next().toString());
    targetRS =
      ((ClusterConnection) admin.getConnection()).getAdmin(targetServer);
    Assert.assertEquals(1, ProtobufUtil.getOnlineRegions(targetRS).size());
    Assert.assertEquals(tableName,
        ProtobufUtil.getOnlineRegions(targetRS).get(0).getTable());
  }

  @Test
  public void testValidGroupNames() throws IOException {
    String[] badNames = {"foo*","foo@","-"};
    String[] goodNames = {"foo_123"};

    for(String entry: badNames) {
      try {
        rsGroupAdmin.addRSGroup(entry);
        fail("Expected a constraint exception for: "+entry);
      } catch(ConstraintException ex) {
        //expected
      }
    }

    for(String entry: goodNames) {
      rsGroupAdmin.addRSGroup(entry);
    }
  }

  private String getGroupName(String baseName) {
    return groupPrefix+"_"+baseName+"_"+rand.nextInt(Integer.MAX_VALUE);
  }

  @Test
  public void testMultiTableMove() throws Exception {
    final TableName tableNameA = TableName.valueOf(tablePrefix + name.getMethodName() + "A");
    final TableName tableNameB = TableName.valueOf(tablePrefix + name.getMethodName() + "B");
    final byte[] familyNameBytes = Bytes.toBytes("f");
    String newGroupName = getGroupName(name.getMethodName());
    final RSGroupInfo newGroup = addGroup(newGroupName, 1);

    TEST_UTIL.createTable(tableNameA, familyNameBytes);
    TEST_UTIL.createTable(tableNameB, familyNameBytes);
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        List<String> regionsA = getTableRegionMap().get(tableNameA);
        if (regionsA == null) {
          return false;
        }

        List<String> regionsB = getTableRegionMap().get(tableNameB);
        if (regionsB == null) {
          return false;
        }

        return getTableRegionMap().get(tableNameA).size() >= 1
                && getTableRegionMap().get(tableNameB).size() >= 1;
      }
    });

    RSGroupInfo tableGrpA = rsGroupAdmin.getRSGroupInfoOfTable(tableNameA);
    assertTrue(tableGrpA.getName().equals(RSGroupInfo.DEFAULT_GROUP));

    RSGroupInfo tableGrpB = rsGroupAdmin.getRSGroupInfoOfTable(tableNameB);
    assertTrue(tableGrpB.getName().equals(RSGroupInfo.DEFAULT_GROUP));
    //change table's group
    LOG.info("Moving table [" + tableNameA + "," + tableNameB + "] to " + newGroup.getName());
    rsGroupAdmin.moveTables(Sets.newHashSet(tableNameA, tableNameB), newGroup.getName());

    //verify group change
    Assert.assertEquals(newGroup.getName(),
            rsGroupAdmin.getRSGroupInfoOfTable(tableNameA).getName());

    Assert.assertEquals(newGroup.getName(),
            rsGroupAdmin.getRSGroupInfoOfTable(tableNameB).getName());

    //verify tables' not exist in old group
    Set<TableName> DefaultTables = rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP)
            .getTables();
    assertFalse(DefaultTables.contains(tableNameA));
    assertFalse(DefaultTables.contains(tableNameB));

    //verify tables' exist in new group
    Set<TableName> newGroupTables = rsGroupAdmin.getRSGroupInfo(newGroupName).getTables();
    assertTrue(newGroupTables.contains(tableNameA));
    assertTrue(newGroupTables.contains(tableNameB));
  }

  @Test
  public void testDisabledTableMove() throws Exception {
    final byte[] familyNameBytes = Bytes.toBytes("f");
    String newGroupName = getGroupName(name.getMethodName());
    final RSGroupInfo newGroup = addGroup(newGroupName, 2);

    TEST_UTIL.createMultiRegionTable(tableName, familyNameBytes, 5);
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        List<String> regions = getTableRegionMap().get(tableName);
        if (regions == null) {
          return false;
        }
        return getTableRegionMap().get(tableName).size() >= 5;
      }
    });

    RSGroupInfo tableGrp = rsGroupAdmin.getRSGroupInfoOfTable(tableName);
    assertTrue(tableGrp.getName().equals(RSGroupInfo.DEFAULT_GROUP));

    //test disable table
    admin.disableTable(tableName);

    //change table's group
    LOG.info("Moving table "+ tableName + " to " + newGroup.getName());
    rsGroupAdmin.moveTables(Sets.newHashSet(tableName), newGroup.getName());

    //verify group change
    Assert.assertEquals(newGroup.getName(),
        rsGroupAdmin.getRSGroupInfoOfTable(tableName).getName());
  }

  @Test
  public void testNonExistentTableMove() throws Exception {
    TableName tableName = TableName.valueOf(tablePrefix + name.getMethodName());

    RSGroupInfo tableGrp = rsGroupAdmin.getRSGroupInfoOfTable(tableName);
    assertNull(tableGrp);

    //test if table exists already.
    boolean exist = admin.tableExists(tableName);
    assertFalse(exist);

    LOG.info("Moving table "+ tableName + " to " + RSGroupInfo.DEFAULT_GROUP);
    try {
      rsGroupAdmin.moveTables(Sets.newHashSet(tableName), RSGroupInfo.DEFAULT_GROUP);
      fail("Table " + tableName + " shouldn't have been successfully moved.");
    } catch(IOException ex) {
      assertTrue(ex instanceof TableNotFoundException);
    }

    try {
      rsGroupAdmin.moveServersAndTables(
          Sets.newHashSet(Address.fromParts("bogus",123)),
          Sets.newHashSet(tableName), RSGroupInfo.DEFAULT_GROUP);
      fail("Table " + tableName + " shouldn't have been successfully moved.");
    } catch(IOException ex) {
      assertTrue(ex instanceof TableNotFoundException);
    }
    //verify group change
    assertNull(rsGroupAdmin.getRSGroupInfoOfTable(tableName));
  }

  @Test
  public void testMoveServersAndTables() throws Exception {
    LOG.info("testMoveServersAndTables");
    final RSGroupInfo newGroup = addGroup(getGroupName(name.getMethodName()), 1);
    //create table
    final byte[] familyNameBytes = Bytes.toBytes("f");
    TEST_UTIL.createMultiRegionTable(tableName, familyNameBytes, 5);
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        List<String> regions = getTableRegionMap().get(tableName);
        if (regions == null) {
          return false;
        }

        return getTableRegionMap().get(tableName).size() >= 5;
      }
    });

    //get server which is not a member of new group
    ServerName targetServer = null;
    for(ServerName server : admin.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS))
      .getLiveServerMetrics().keySet()) {
      if(!newGroup.containsServer(server.getAddress()) &&
           !rsGroupAdmin.getRSGroupInfo("master").containsServer(server.getAddress())) {
        targetServer = server;
        break;
      }
    }

    LOG.debug("Print group info : " + rsGroupAdmin.listRSGroups());
    int oldDefaultGroupServerSize =
            rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers().size();
    int oldDefaultGroupTableSize =
            rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getTables().size();

    //test fail bogus server move
    try {
      rsGroupAdmin.moveServersAndTables(Sets.newHashSet(Address.fromString("foo:9999")),
              Sets.newHashSet(tableName), newGroup.getName());
      fail("Bogus servers shouldn't have been successfully moved.");
    } catch(IOException ex) {
      String exp = "Source RSGroup for server foo:9999 does not exist.";
      String msg = "Expected '" + exp + "' in exception message: ";
      assertTrue(msg + " " + ex.getMessage(), ex.getMessage().contains(exp));
    }

    //test fail server move
    try {
      rsGroupAdmin.moveServersAndTables(Sets.newHashSet(targetServer.getAddress()),
              Sets.newHashSet(tableName), RSGroupInfo.DEFAULT_GROUP);
      fail("servers shouldn't have been successfully moved.");
    } catch(IOException ex) {
      String exp = "Target RSGroup " + RSGroupInfo.DEFAULT_GROUP +
              " is same as source " + RSGroupInfo.DEFAULT_GROUP + " RSGroup.";
      String msg = "Expected '" + exp + "' in exception message: ";
      assertTrue(msg + " " + ex.getMessage(), ex.getMessage().contains(exp));
    }

    //verify default group info
    Assert.assertEquals(oldDefaultGroupServerSize,
            rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers().size());
    Assert.assertEquals(oldDefaultGroupTableSize,
            rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getTables().size());

    //verify new group info
    Assert.assertEquals(1,
            rsGroupAdmin.getRSGroupInfo(newGroup.getName()).getServers().size());
    Assert.assertEquals(0,
            rsGroupAdmin.getRSGroupInfo(newGroup.getName()).getTables().size());

    //get all region to move targetServer
    List<String> regionList = getTableRegionMap().get(tableName);
    for(String region : regionList) {
      // Lets move this region to the targetServer
      TEST_UTIL.getAdmin().move(Bytes.toBytes(RegionInfo.encodeRegionName(Bytes.toBytes(region))),
              Bytes.toBytes(targetServer.getServerName()));
    }

    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return getTableRegionMap().get(tableName) != null &&
                getTableRegionMap().get(tableName).size() == 5 &&
                getTableServerRegionMap().get(tableName).size() == 1 &&
                admin.getClusterMetrics(EnumSet.of(Option.REGIONS_IN_TRANSITION))
                     .getRegionStatesInTransition().size() < 1;
      }
    });

    //verify that all region move to targetServer
    Assert.assertEquals(5, getTableServerRegionMap().get(tableName).get(targetServer).size());

    //move targetServer and table to newGroup
    LOG.info("moving server and table to newGroup");
    rsGroupAdmin.moveServersAndTables(Sets.newHashSet(targetServer.getAddress()),
            Sets.newHashSet(tableName), newGroup.getName());

    //verify group change
    Assert.assertEquals(newGroup.getName(),
            rsGroupAdmin.getRSGroupInfoOfTable(tableName).getName());

    //verify servers' not exist in old group
    Set<Address> defaultServers = rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP)
            .getServers();
    assertFalse(defaultServers.contains(targetServer.getAddress()));

    //verify servers' exist in new group
    Set<Address> newGroupServers = rsGroupAdmin.getRSGroupInfo(newGroup.getName()).getServers();
    assertTrue(newGroupServers.contains(targetServer.getAddress()));

    //verify tables' not exist in old group
    Set<TableName> defaultTables = rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP)
            .getTables();
    assertFalse(defaultTables.contains(tableName));

    //verify tables' exist in new group
    Set<TableName> newGroupTables = rsGroupAdmin.getRSGroupInfo(newGroup.getName()).getTables();
    assertTrue(newGroupTables.contains(tableName));

    //verify that all region still assgin on targetServer
    Assert.assertEquals(5, getTableServerRegionMap().get(tableName).get(targetServer).size());
  }

  @Test
  public void testClearDeadServers() throws Exception {
    LOG.info("testClearDeadServers");
    final RSGroupInfo newGroup = addGroup(getGroupName(name.getMethodName()), 3);
    NUM_DEAD_SERVERS = cluster.getClusterMetrics().getDeadServerNames().size();

    ServerName targetServer = ServerName.parseServerName(
        newGroup.getServers().iterator().next().toString());
    AdminProtos.AdminService.BlockingInterface targetRS =
        ((ClusterConnection) admin.getConnection()).getAdmin(targetServer);
    try {
      targetServer = ProtobufUtil.toServerName(targetRS.getServerInfo(null,
          GetServerInfoRequest.newBuilder().build()).getServerInfo().getServerName());
      //stopping may cause an exception
      //due to the connection loss
      targetRS.stopServer(null,
          AdminProtos.StopServerRequest.newBuilder().setReason("Die").build());
      NUM_DEAD_SERVERS ++;
    } catch(Exception e) {
    }
    //wait for stopped regionserver to dead server list
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return !master.getServerManager().areDeadServersInProgress()
            && cluster.getClusterMetrics().getDeadServerNames().size() == NUM_DEAD_SERVERS;
      }
    });
    assertFalse(cluster.getClusterMetrics().getLiveServerMetrics().containsKey(targetServer));
    assertTrue(cluster.getClusterMetrics().getDeadServerNames().contains(targetServer));
    assertTrue(newGroup.getServers().contains(targetServer.getAddress()));

    //clear dead servers list
    List<ServerName> notClearedServers = admin.clearDeadServers(Lists.newArrayList(targetServer));
    assertEquals(0, notClearedServers.size());

    Set<Address> newGroupServers = rsGroupAdmin.getRSGroupInfo(newGroup.getName()).getServers();
    assertFalse(newGroupServers.contains(targetServer.getAddress()));
    assertEquals(2, newGroupServers.size());
  }

  @Test
  public void testRemoveServers() throws Exception {
    LOG.info("testRemoveServers");
    final RSGroupInfo newGroup = addGroup(getGroupName(name.getMethodName()), 3);
    Iterator<Address> iterator = newGroup.getServers().iterator();
    ServerName targetServer = ServerName.parseServerName(iterator.next().toString());

    // remove online servers
    try {
      rsGroupAdmin.removeServers(Sets.newHashSet(targetServer.getAddress()));
      fail("Online servers shouldn't have been successfully removed.");
    } catch(IOException ex) {
      String exp = "Server " + targetServer.getAddress()
          + " is an online server, not allowed to remove.";
      String msg = "Expected '" + exp + "' in exception message: ";
      assertTrue(msg + " " + ex.getMessage(), ex.getMessage().contains(exp));
    }
    assertTrue(newGroup.getServers().contains(targetServer.getAddress()));

    // remove dead servers
    NUM_DEAD_SERVERS = cluster.getClusterMetrics().getDeadServerNames().size();
    AdminProtos.AdminService.BlockingInterface targetRS =
        ((ClusterConnection) admin.getConnection()).getAdmin(targetServer);
    try {
      targetServer = ProtobufUtil.toServerName(targetRS.getServerInfo(null,
          GetServerInfoRequest.newBuilder().build()).getServerInfo().getServerName());
      //stopping may cause an exception
      //due to the connection loss
      LOG.info("stopping server " + targetServer.getHostAndPort());
      targetRS.stopServer(null,
          AdminProtos.StopServerRequest.newBuilder().setReason("Die").build());
      NUM_DEAD_SERVERS ++;
    } catch(Exception e) {
    }

    //wait for stopped regionserver to dead server list
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return !master.getServerManager().areDeadServersInProgress()
            && cluster.getClusterMetrics().getDeadServerNames().size() == NUM_DEAD_SERVERS;
      }
    });

    try {
      rsGroupAdmin.removeServers(Sets.newHashSet(targetServer.getAddress()));
      fail("Dead servers shouldn't have been successfully removed.");
    } catch(IOException ex) {
      String exp = "Server " + targetServer.getAddress() + " is on the dead servers list,"
          + " Maybe it will come back again, not allowed to remove.";
      String msg = "Expected '" + exp + "' in exception message: ";
      assertTrue(msg + " " + ex.getMessage(), ex.getMessage().contains(exp));
    }
    assertTrue(newGroup.getServers().contains(targetServer.getAddress()));

    // remove decommissioned servers
    List<ServerName> serversToDecommission = new ArrayList<>();
    targetServer = ServerName.parseServerName(iterator.next().toString());
    targetRS = ((ClusterConnection) admin.getConnection()).getAdmin(targetServer);
    targetServer = ProtobufUtil.toServerName(targetRS.getServerInfo(null,
          GetServerInfoRequest.newBuilder().build()).getServerInfo().getServerName());
    assertTrue(master.getServerManager().getOnlineServers().containsKey(targetServer));
    serversToDecommission.add(targetServer);

    admin.decommissionRegionServers(serversToDecommission, true);
    assertEquals(1, admin.listDecommissionedRegionServers().size());

    assertTrue(newGroup.getServers().contains(targetServer.getAddress()));
    rsGroupAdmin.removeServers(Sets.newHashSet(targetServer.getAddress()));
    Set<Address> newGroupServers = rsGroupAdmin.getRSGroupInfo(newGroup.getName()).getServers();
    assertFalse(newGroupServers.contains(targetServer.getAddress()));
    assertEquals(2, newGroupServers.size());
  }

  @Test
  public void testCreateWhenRsgroupNoOnlineServers() throws Exception {
    LOG.info("testCreateWhenRsgroupNoOnlineServers");

    // set rsgroup has no online servers and test create table
    final RSGroupInfo appInfo = addGroup("appInfo", 1);
    Iterator<Address> iterator = appInfo.getServers().iterator();
    List<ServerName> serversToDecommission = new ArrayList<>();
    ServerName targetServer = ServerName.parseServerName(iterator.next().toString());
    AdminProtos.AdminService.BlockingInterface targetRS =
        ((ClusterConnection) admin.getConnection()).getAdmin(targetServer);
    targetServer = ProtobufUtil.toServerName(
        targetRS.getServerInfo(null, GetServerInfoRequest.newBuilder().build()).getServerInfo()
            .getServerName());
    assertTrue(master.getServerManager().getOnlineServers().containsKey(targetServer));
    serversToDecommission.add(targetServer);
    admin.decommissionRegionServers(serversToDecommission, true);
    assertEquals(1, admin.listDecommissionedRegionServers().size());

    final TableName tableName = TableName.valueOf(tablePrefix + "_ns", name.getMethodName());
    admin.createNamespace(NamespaceDescriptor.create(tableName.getNamespaceAsString())
        .addConfiguration(RSGroupInfo.NAMESPACE_DESC_PROP_GROUP, appInfo.getName()).build());
    final HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor("f"));
    try {
      admin.createTable(desc);
      fail("Shouldn't create table successfully!");
    } catch (Exception e) {
      LOG.debug("create table error", e);
    }

    // recommission and test create table
    admin.recommissionRegionServer(targetServer, null);
    assertEquals(0, admin.listDecommissionedRegionServers().size());
    admin.createTable(desc);
    // wait for created table to be assigned
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override public boolean evaluate() throws Exception {
        return getTableRegionMap().get(desc.getTableName()) != null;
      }
    });
  }
  @Test
  public void testClearNotProcessedDeadServer() throws Exception {
    LOG.info("testClearNotProcessedDeadServer");
    NUM_DEAD_SERVERS = cluster.getClusterMetrics().getDeadServerNames().size();
    RSGroupInfo appInfo = addGroup("deadServerGroup", 1);
    ServerName targetServer =
        ServerName.parseServerName(appInfo.getServers().iterator().next().toString());
    AdminProtos.AdminService.BlockingInterface targetRS =
        ((ClusterConnection) admin.getConnection()).getAdmin(targetServer);
    try {
      targetServer = ProtobufUtil.toServerName(targetRS.getServerInfo(null,
          AdminProtos.GetServerInfoRequest.newBuilder().build()).getServerInfo().getServerName());
      //stopping may cause an exception
      //due to the connection loss
      targetRS.stopServer(null,
          AdminProtos.StopServerRequest.newBuilder().setReason("Die").build());
      NUM_DEAD_SERVERS ++;
    } catch(Exception e) {
    }
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return cluster.getClusterMetrics().getDeadServerNames().size() == NUM_DEAD_SERVERS;
      }
    });
    List<ServerName> notClearedServers = admin.clearDeadServers(Lists.newArrayList(targetServer));
    assertEquals(1, notClearedServers.size());
  }
}
