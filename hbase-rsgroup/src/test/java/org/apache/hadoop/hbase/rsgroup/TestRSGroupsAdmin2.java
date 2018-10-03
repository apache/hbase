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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Sets;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetServerInfoRequest;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MediumTests.class})
public class TestRSGroupsAdmin2 extends TestRSGroupsBase {
  protected static final Log LOG = LogFactory.getLog(TestRSGroupsAdmin2.class);

  @BeforeClass
  public static void setUp() throws Exception {
    setUpTestBeforeClass();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    tearDownAfterClass();
  }

  @Before
  public void beforeMethod() throws Exception {
    setUpBeforeMethod();
  }

  @After
  public void afterMethod() throws Exception {
    tearDownAfterMethod();
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
  public void testRegionServerMove() throws IOException,
      InterruptedException {
    LOG.info("testRegionServerMove");

    int initNumGroups = rsGroupAdmin.listRSGroups().size();
    RSGroupInfo appInfo = addGroup(rsGroupAdmin, getGroupName("testRegionServerMove"), 1);
    RSGroupInfo adminInfo = addGroup(rsGroupAdmin, getGroupName("testRegionServerMove"), 1);
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

    assertTrue(observer.preRemoveServersCalled);
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

    assertTrue(observer.preMoveServersAndTables);
    assertTrue(observer.postMoveServersAndTables);
  }

}