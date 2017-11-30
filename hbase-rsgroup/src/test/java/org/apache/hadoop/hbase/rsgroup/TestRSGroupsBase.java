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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseCluster;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetServerInfoRequest;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public abstract class TestRSGroupsBase {
  protected static final Log LOG = LogFactory.getLog(TestRSGroupsBase.class);

  //shared
  protected final static String groupPrefix = "Group";
  protected final static String tablePrefix = "Group";
  protected final static SecureRandom rand = new SecureRandom();

  //shared, cluster type specific
  protected static HBaseTestingUtility TEST_UTIL;
  protected static HBaseAdmin admin;
  protected static HBaseCluster cluster;
  protected static RSGroupAdmin rsGroupAdmin;

  public final static long WAIT_TIMEOUT = 60000*5;
  public final static int NUM_SLAVES_BASE = 4; //number of slaves for the smallest cluster



  protected RSGroupInfo addGroup(RSGroupAdmin gAdmin, String groupName,
                                 int serverCount) throws IOException, InterruptedException {
    RSGroupInfo defaultInfo = gAdmin
        .getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP);
    assertTrue(defaultInfo != null);
    assertTrue(defaultInfo.getServers().size() >= serverCount);
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
    assertTrue(result.getServers().size() >= serverCount);
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
    } catch(ConstraintException ex) {
      //expected
    }

    try {
      rsGroupAdmin.moveServers(Sets.newHashSet(Address.fromParts("bogus",123)), "bogus");
      fail("Expected move with bogus group to fail");
    } catch(ConstraintException ex) {
      //expected
    }

    try {
      rsGroupAdmin.balanceRSGroup("bogus");
      fail("Expected move with bogus group to fail");
    } catch(ConstraintException ex) {
      //expected
    }
  }

  @Test
  public void testCreateMultiRegion() throws IOException {
    LOG.info("testCreateMultiRegion");
    TableName tableName = TableName.valueOf(tablePrefix + "_testCreateMultiRegion");
    byte[] end = {1,3,5,7,9};
    byte[] start = {0,2,4,6,8};
    byte[][] f = {Bytes.toBytes("f")};
    TEST_UTIL.createTable(tableName, f,1,start,end,10);
  }

  @Test
  public void testCreateAndDrop() throws Exception {
    LOG.info("testCreateAndDrop");

    final TableName tableName = TableName.valueOf(tablePrefix + "_testCreateAndDrop");
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
    LOG.info("testSimpleRegionServerMove");

    int initNumGroups = rsGroupAdmin.listRSGroups().size();
    RSGroupInfo appInfo = addGroup(rsGroupAdmin, getGroupName("testSimpleRegionServerMove"), 1);
    RSGroupInfo adminInfo = addGroup(rsGroupAdmin, getGroupName("testSimpleRegionServerMove"), 1);
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

  @Test
  public void testMoveServers() throws Exception {
    LOG.info("testMoveServers");

    //create groups and assign servers
    addGroup(rsGroupAdmin, "bar", 3);
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
      String exp = "Server foo:9999 does not have a group.";
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
  public void testTableMoveTruncateAndDrop() throws Exception {
    LOG.info("testTableMove");

    final TableName tableName = TableName.valueOf(tablePrefix + "_testTableMoveAndDrop");
    final byte[] familyNameBytes = Bytes.toBytes("f");
    String newGroupName = getGroupName("testTableMove");
    final RSGroupInfo newGroup = addGroup(rsGroupAdmin, newGroupName, 2);

    TEST_UTIL.createMultiRegionTable(tableName, familyNameBytes, 5);
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        List<String> regions = getTableRegionMap().get(tableName);
        if (regions == null)
          return false;
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
    LOG.info("testGroupBalance");
    String newGroupName = getGroupName("testGroupBalance");
    final RSGroupInfo newGroup = addGroup(rsGroupAdmin, newGroupName, 3);

    final TableName tableName = TableName.valueOf(tablePrefix+"_ns", "testGroupBalance");
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
    for(HRegionInfo region: admin.getTableRegions(tableName)) {
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
    rsGroupAdmin.balanceRSGroup(RSGroupInfo.DEFAULT_GROUP);
    assertEquals(6, getTableServerRegionMap().get(tableName).get(first).size());

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
  }

  @Test
  public void testRegionMove() throws Exception {
    LOG.info("testRegionMove");

    final RSGroupInfo newGroup = addGroup(rsGroupAdmin, getGroupName("testRegionMove"), 1);
    final TableName tableName = TableName.valueOf(tablePrefix + rand.nextInt());
    final byte[] familyNameBytes = Bytes.toBytes("f");
    // All the regions created below will be assigned to the default group.
    TEST_UTIL.createMultiRegionTable(tableName, familyNameBytes, 6);
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        List<String> regions = getTableRegionMap().get(tableName);
        if (regions == null)
          return false;
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
    for(ServerName server : admin.getClusterStatus().getServers()) {
      if(!newGroup.containsServer(server.getAddress())) {
        targetServer = server;
        break;
      }
    }
    assertNotNull(targetServer);

    final AdminProtos.AdminService.BlockingInterface targetRS =
        admin.getConnection().getAdmin(targetServer);

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
    TEST_UTIL.getHBaseAdmin().move(Bytes.toBytes(HRegionInfo.encodeRegionName(Bytes.toBytes(targetRegion))),
        Bytes.toBytes(targetServer.getServerName()));
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        List<String> regions = getTableRegionMap().get(tableName);
        Set<RegionState> regionsInTransition = admin.getClusterStatus().getRegionsInTransition();
        return (regions != null && getTableRegionMap().get(tableName).size() == 6) &&
           ( regionsInTransition == null || regionsInTransition.size() < 1);
      }
    });

    //verify that targetServer didn't open it
    for (HRegionInfo region: ProtobufUtil.getOnlineRegions(targetRS)) {
      if (targetRegion.equals(region.getRegionNameAsString())) {
        fail("Target server opened region");
      }
    }
  }

  @Test
  public void testFailRemoveGroup() throws IOException, InterruptedException {
    LOG.info("testFailRemoveGroup");

    int initNumGroups = rsGroupAdmin.listRSGroups().size();
    addGroup(rsGroupAdmin, "bar", 3);
    TableName tableName = TableName.valueOf(tablePrefix+"_my_table");
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
    LOG.info("testKillRS");
    RSGroupInfo appInfo = addGroup(rsGroupAdmin, "appInfo", 1);

    final TableName tableName = TableName.valueOf(tablePrefix+"_ns", "_testKillRS");
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
        admin.getConnection().getAdmin(targetServer);
    Assert.assertEquals(1, ProtobufUtil.getOnlineRegions(targetRS).size());

    try {
      //stopping may cause an exception
      //due to the connection loss
      targetRS.stopServer(null,
          AdminProtos.StopServerRequest.newBuilder().setReason("Die").build());
    } catch(Exception e) {
    }
    assertFalse(cluster.getClusterStatus().getServers().contains(targetServer));

    //wait for created table to be assigned
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return cluster.getClusterStatus().getRegionsInTransition().size() == 0;
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
        return cluster.getClusterStatus().getRegionsInTransition().size() == 0;
      }
    });

    targetServer = ServerName.parseServerName(
        newServers.iterator().next().toString());
    targetRS =
        admin.getConnection().getAdmin(targetServer);
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
    LOG.info("testMultiTableMove");

    final TableName tableNameA = TableName.valueOf(tablePrefix + "_testMultiTableMoveA");
    final TableName tableNameB = TableName.valueOf(tablePrefix + "_testMultiTableMoveB");
    final byte[] familyNameBytes = Bytes.toBytes("f");
    String newGroupName = getGroupName("testMultiTableMove");
    final RSGroupInfo newGroup = addGroup(rsGroupAdmin, newGroupName, 1);

    TEST_UTIL.createTable(tableNameA, familyNameBytes);
    TEST_UTIL.createTable(tableNameB, familyNameBytes);
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        List<String> regionsA = getTableRegionMap().get(tableNameA);
        if (regionsA == null)
          return false;
        List<String> regionsB = getTableRegionMap().get(tableNameB);
        if (regionsB == null)
          return false;

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
    Set<TableName> DefaultTables = rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getTables();
    assertFalse(DefaultTables.contains(tableNameA));
    assertFalse(DefaultTables.contains(tableNameB));

    //verify tables' exist in new group
    Set<TableName> newGroupTables = rsGroupAdmin.getRSGroupInfo(newGroupName).getTables();
    assertTrue(newGroupTables.contains(tableNameA));
    assertTrue(newGroupTables.contains(tableNameB));
  }

  @Test
  public void testMoveServersAndTables() throws Exception {
    final TableName tableName = TableName.valueOf(tablePrefix + "_testMoveServersAndTables");
    final RSGroupInfo newGroup = addGroup(rsGroupAdmin, getGroupName("testMoveServersAndTables"), 1);

    //create table
    final byte[] familyNameBytes = Bytes.toBytes("f");
    TEST_UTIL.createMultiRegionTable(tableName, familyNameBytes, 5);
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        List<String> regions = getTableRegionMap().get(tableName);
        if (regions == null)
          return false;
        return getTableRegionMap().get(tableName).size() >= 5;
      }
    });

    //get server which is not a member of new group
    ServerName targetServer = null;
    for(ServerName server : admin.getClusterStatus().getServers()) {
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
    }

    //test fail server move
    try {
      rsGroupAdmin.moveServersAndTables(Sets.newHashSet(targetServer.getAddress()),
              Sets.newHashSet(tableName), RSGroupInfo.DEFAULT_GROUP);
      fail("servers shouldn't have been successfully moved.");
    } catch(IOException ex) {
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
      admin.move(Bytes.toBytes(HRegionInfo.encodeRegionName(Bytes.toBytes(region))),
              Bytes.toBytes(targetServer.getServerName()));
    }

    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        List<String> regions = getTableRegionMap().get(tableName);
        Map<ServerName, List<String>> serverMap = getTableServerRegionMap().get(tableName);
        Set<RegionState> regionsInTransition = admin.getClusterStatus().getRegionsInTransition();
        return (regions != null && regions.size() == 5) &&
          (serverMap != null && serverMap.size() == 1) &&
          (regionsInTransition == null || regionsInTransition.size() < 1);
      }
    });

    //verify that all region move to targetServer
    Assert.assertNotNull(getTableServerRegionMap().get(tableName));
    Assert.assertNotNull(getTableServerRegionMap().get(tableName).get(targetServer));
    Assert.assertEquals(5, getTableServerRegionMap().get(tableName).get(targetServer).size());

    //move targetServer and table to newGroup
    LOG.info("moving server and table to newGroup");
    rsGroupAdmin.moveServersAndTables(Sets.newHashSet(targetServer.getAddress()),
            Sets.newHashSet(tableName), newGroup.getName());

    //verify group change
    Assert.assertEquals(newGroup.getName(),
            rsGroupAdmin.getRSGroupInfoOfTable(tableName).getName());

    //verify servers' not exist in old group
    Set<Address> defaultServers = rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers();
    assertFalse(defaultServers.contains(targetServer.getAddress()));

    //verify servers' exist in new group
    Set<Address> newGroupServers = rsGroupAdmin.getRSGroupInfo(newGroup.getName()).getServers();
    assertTrue(newGroupServers.contains(targetServer.getAddress()));

    //verify tables' not exist in old group
    Set<TableName> defaultTables = rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getTables();
    assertFalse(defaultTables.contains(tableName));

    //verify tables' exist in new group
    Set<TableName> newGroupTables = rsGroupAdmin.getRSGroupInfo(newGroup.getName()).getTables();
    assertTrue(newGroupTables.contains(tableName));
  }

  @Test
  public void testDisabledTableMove() throws Exception {
    final TableName tableName = TableName.valueOf(tablePrefix + "_testDisabledTableMove");
    final byte[] familyNameBytes = Bytes.toBytes("f");
    String newGroupName = getGroupName("testDisabledTableMove");
    final RSGroupInfo newGroup = addGroup(rsGroupAdmin, newGroupName, 2);

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
  public void testClearDeadServers() throws Exception {
    final RSGroupInfo newGroup = addGroup(rsGroupAdmin, "testClearDeadServers", 3);

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
    } catch(Exception e) {
    }
    final HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    //wait for stopped regionserver to dead server list
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return !master.getServerManager().areDeadServersInProgress()
            && cluster.getClusterStatus().getDeadServerNames().size() > 0;
      }
    });
    assertFalse(cluster.getClusterStatus().getServers().contains(targetServer));
    assertTrue(cluster.getClusterStatus().getDeadServerNames().contains(targetServer));
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
    final RSGroupInfo newGroup = addGroup(rsGroupAdmin, "testRemoveServers", 3);
    ServerName targetServer = ServerName.parseServerName(
        newGroup.getServers().iterator().next().toString());
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

    AdminProtos.AdminService.BlockingInterface targetRS =
        ((ClusterConnection) admin.getConnection()).getAdmin(targetServer);
    try {
      targetServer = ProtobufUtil.toServerName(targetRS.getServerInfo(null,
          GetServerInfoRequest.newBuilder().build()).getServerInfo().getServerName());
      //stopping may cause an exception
      //due to the connection loss
      targetRS.stopServer(null,
          AdminProtos.StopServerRequest.newBuilder().setReason("Die").build());
    } catch(Exception e) {
    }

    final HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    //wait for stopped regionserver to dead server list
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return !master.getServerManager().areDeadServersInProgress()
            && cluster.getClusterStatus().getDeadServerNames().size() > 0;
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

    ServerName sn = TEST_UTIL.getHBaseClusterInterface().getClusterStatus().getMaster();
    TEST_UTIL.getHBaseClusterInterface().stopMaster(sn);
    TEST_UTIL.getHBaseClusterInterface().waitForMasterToStop(sn, 60000);
    TEST_UTIL.getHBaseClusterInterface().startMaster(sn.getHostname(), 0);
    TEST_UTIL.getHBaseClusterInterface().waitForActiveAndReadyMaster(60000);

    assertEquals(3, cluster.getClusterStatus().getServersSize());
    assertFalse(cluster.getClusterStatus().getServers().contains(targetServer));
    assertFalse(cluster.getClusterStatus().getDeadServerNames().contains(targetServer));
    assertTrue(newGroup.getServers().contains(targetServer.getAddress()));

    rsGroupAdmin.removeServers(Sets.newHashSet(targetServer.getAddress()));
    Set<Address> newGroupServers = rsGroupAdmin.getRSGroupInfo(newGroup.getName()).getServers();
    assertFalse(newGroupServers.contains(targetServer.getAddress()));
    assertEquals(2, newGroupServers.size());
  }
}