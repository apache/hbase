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

import static org.apache.hadoop.hbase.util.Threads.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

@Category({ LargeTests.class })
public class TestRSGroupsAdmin2 extends TestRSGroupsBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRSGroupsAdmin2.class);

  protected static final Logger LOG = LoggerFactory.getLogger(TestRSGroupsAdmin2.class);

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

    // get target region to move
    Map<ServerName, List<String>> assignMap = getTableServerRegionMap().get(tableName);
    String targetRegion = null;
    for (ServerName server : assignMap.keySet()) {
      targetRegion = assignMap.get(server).size() > 0 ? assignMap.get(server).get(0) : null;
      if (targetRegion != null) {
        break;
      }
    }
    // get server which is not a member of new group
    ServerName tmpTargetServer = null;
    for (ServerName server : admin.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS))
      .getLiveServerMetrics().keySet()) {
      if (!newGroup.containsServer(server.getAddress())) {
        tmpTargetServer = server;
        break;
      }
    }
    final ServerName targetServer = tmpTargetServer;
    // move target server to group
    rsGroupAdmin.moveServers(Sets.newHashSet(targetServer.getAddress()), newGroup.getName());
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return admin.getRegions(targetServer).size() <= 0;
      }
    });

    // Lets move this region to the new group.
    TEST_UTIL.getAdmin()
      .move(Bytes.toBytes(RegionInfo.encodeRegionName(Bytes.toBytes(targetRegion))), targetServer);
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return getTableRegionMap().get(tableName) != null &&
          getTableRegionMap().get(tableName).size() == 6 &&
          admin.getClusterMetrics(EnumSet.of(Option.REGIONS_IN_TRANSITION))
            .getRegionStatesInTransition().size() < 1;
      }
    });

    // verify that targetServer didn't open it
    for (RegionInfo region : admin.getRegions(targetServer)) {
      if (targetRegion.equals(region.getRegionNameAsString())) {
        fail("Target server opened region");
      }
    }
  }

  @Test
  public void testRegionServerMove() throws IOException, InterruptedException {
    int initNumGroups = rsGroupAdmin.listRSGroups().size();
    RSGroupInfo appInfo = addGroup(getGroupName(name.getMethodName()), 1);
    RSGroupInfo adminInfo = addGroup(getGroupName(name.getMethodName()), 1);
    RSGroupInfo dInfo = rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP);
    Assert.assertEquals(initNumGroups + 2, rsGroupAdmin.listRSGroups().size());
    assertEquals(1, adminInfo.getServers().size());
    assertEquals(1, appInfo.getServers().size());
    assertEquals(getNumServers() - 2, dInfo.getServers().size());
    rsGroupAdmin.moveServers(appInfo.getServers(), RSGroupInfo.DEFAULT_GROUP);
    rsGroupAdmin.removeRSGroup(appInfo.getName());
    rsGroupAdmin.moveServers(adminInfo.getServers(), RSGroupInfo.DEFAULT_GROUP);
    rsGroupAdmin.removeRSGroup(adminInfo.getName());
    Assert.assertEquals(rsGroupAdmin.listRSGroups().size(), initNumGroups);
  }

  @Test
  public void testMoveServers() throws Exception {
    // create groups and assign servers
    addGroup("bar", 3);
    rsGroupAdmin.addRSGroup("foo");

    RSGroupInfo barGroup = rsGroupAdmin.getRSGroupInfo("bar");
    RSGroupInfo fooGroup = rsGroupAdmin.getRSGroupInfo("foo");
    assertEquals(3, barGroup.getServers().size());
    assertEquals(0, fooGroup.getServers().size());

    // test fail bogus server move
    try {
      rsGroupAdmin.moveServers(Sets.newHashSet(Address.fromString("foo:9999")), "foo");
      fail("Bogus servers shouldn't have been successfully moved.");
    } catch (IOException ex) {
      String exp = "Server foo:9999 is either offline or it does not exist.";
      String msg = "Expected '" + exp + "' in exception message: ";
      assertTrue(msg + " " + ex.getMessage(), ex.getMessage().contains(exp));
    }

    // test success case
    LOG.info("moving servers " + barGroup.getServers() + " to group foo");
    rsGroupAdmin.moveServers(barGroup.getServers(), fooGroup.getName());

    barGroup = rsGroupAdmin.getRSGroupInfo("bar");
    fooGroup = rsGroupAdmin.getRSGroupInfo("foo");
    assertEquals(0, barGroup.getServers().size());
    assertEquals(3, fooGroup.getServers().size());

    LOG.info("moving servers " + fooGroup.getServers() + " to group default");
    rsGroupAdmin.moveServers(fooGroup.getServers(), RSGroupInfo.DEFAULT_GROUP);

    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return getNumServers() == rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP)
          .getServers().size();
      }
    });

    fooGroup = rsGroupAdmin.getRSGroupInfo("foo");
    assertEquals(0, fooGroup.getServers().size());

    // test group removal
    LOG.info("Remove group " + barGroup.getName());
    rsGroupAdmin.removeRSGroup(barGroup.getName());
    Assert.assertEquals(null, rsGroupAdmin.getRSGroupInfo(barGroup.getName()));
    LOG.info("Remove group " + fooGroup.getName());
    rsGroupAdmin.removeRSGroup(fooGroup.getName());
    Assert.assertEquals(null, rsGroupAdmin.getRSGroupInfo(fooGroup.getName()));
  }

  @Test
  public void testRemoveServers() throws Exception {
    LOG.info("testRemoveServers");
    final RSGroupInfo newGroup = addGroup(getGroupName(name.getMethodName()), 3);
    Iterator<Address> iterator = newGroup.getServers().iterator();
    ServerName targetServer = getServerName(iterator.next());

    // remove online servers
    try {
      rsGroupAdmin.removeServers(Sets.newHashSet(targetServer.getAddress()));
      fail("Online servers shouldn't have been successfully removed.");
    } catch (IOException ex) {
      String exp =
        "Server " + targetServer.getAddress() + " is an online server, not allowed to remove.";
      String msg = "Expected '" + exp + "' in exception message: ";
      assertTrue(msg + " " + ex.getMessage(), ex.getMessage().contains(exp));
    }
    assertTrue(newGroup.getServers().contains(targetServer.getAddress()));

    // remove dead servers
    NUM_DEAD_SERVERS = cluster.getClusterMetrics().getDeadServerNames().size();
    try {
      // stopping may cause an exception
      // due to the connection loss
      LOG.info("stopping server " + targetServer.getServerName());
      admin.stopRegionServer(targetServer.getAddress().toString());
      NUM_DEAD_SERVERS++;
    } catch (Exception e) {
    }

    // wait for stopped regionserver to dead server list
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return !master.getServerManager().areDeadServersInProgress() &&
          cluster.getClusterMetrics().getDeadServerNames().size() == NUM_DEAD_SERVERS;
      }
    });

    try {
      rsGroupAdmin.removeServers(Sets.newHashSet(targetServer.getAddress()));
      fail("Dead servers shouldn't have been successfully removed.");
    } catch (IOException ex) {
      String exp = "Server " + targetServer.getAddress() + " is on the dead servers list," +
        " Maybe it will come back again, not allowed to remove.";
      String msg = "Expected '" + exp + "' in exception message: ";
      assertTrue(msg + " " + ex.getMessage(), ex.getMessage().contains(exp));
    }
    assertTrue(newGroup.getServers().contains(targetServer.getAddress()));

    // remove decommissioned servers
    List<ServerName> serversToDecommission = new ArrayList<>();
    targetServer = getServerName(iterator.next());
    assertTrue(master.getServerManager().getOnlineServers().containsKey(targetServer));
    serversToDecommission.add(targetServer);

    admin.decommissionRegionServers(serversToDecommission, true);
    assertEquals(1, admin.listDecommissionedRegionServers().size());

    assertTrue(newGroup.getServers().contains(targetServer.getAddress()));
    rsGroupAdmin.removeServers(Sets.newHashSet(targetServer.getAddress()));
    Set<Address> newGroupServers = rsGroupAdmin.getRSGroupInfo(newGroup.getName()).getServers();
    assertFalse(newGroupServers.contains(targetServer.getAddress()));
    assertEquals(2, newGroupServers.size());

    assertTrue(observer.preRemoveServersCalled);
    assertTrue(observer.postRemoveServersCalled);
  }

  @Test
  public void testMoveServersAndTables() throws Exception {
    LOG.info("testMoveServersAndTables");
    final RSGroupInfo newGroup = addGroup(getGroupName(name.getMethodName()), 1);
    // create table
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

    // get server which is not a member of new group
    ServerName targetServer = null;
    for (ServerName server : admin.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS))
      .getLiveServerMetrics().keySet()) {
      if (!newGroup.containsServer(server.getAddress()) &&
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

    // test fail bogus server move
    try {
      rsGroupAdmin.moveServersAndTables(Sets.newHashSet(Address.fromString("foo:9999")),
        Sets.newHashSet(tableName), newGroup.getName());
      fail("Bogus servers shouldn't have been successfully moved.");
    } catch (IOException ex) {
      String exp = "Server foo:9999 is either offline or it does not exist.";
      String msg = "Expected '" + exp + "' in exception message: ";
      assertTrue(msg + " " + ex.getMessage(), ex.getMessage().contains(exp));
    }

    // test move when src = dst
    rsGroupAdmin.moveServersAndTables(Sets.newHashSet(targetServer.getAddress()),
      Sets.newHashSet(tableName), RSGroupInfo.DEFAULT_GROUP);

    // verify default group info
    Assert.assertEquals(oldDefaultGroupServerSize,
      rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers().size());
    Assert.assertEquals(oldDefaultGroupTableSize,
      rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getTables().size());

    // verify new group info
    Assert.assertEquals(1, rsGroupAdmin.getRSGroupInfo(newGroup.getName()).getServers().size());
    Assert.assertEquals(0, rsGroupAdmin.getRSGroupInfo(newGroup.getName()).getTables().size());

    // get all region to move targetServer
    List<String> regionList = getTableRegionMap().get(tableName);
    for (String region : regionList) {
      // Lets move this region to the targetServer
      TEST_UTIL.getAdmin().move(Bytes.toBytes(RegionInfo.encodeRegionName(Bytes.toBytes(region))),
        targetServer);
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

    // verify that all region move to targetServer
    Assert.assertEquals(5, getTableServerRegionMap().get(tableName).get(targetServer).size());

    // move targetServer and table to newGroup
    LOG.info("moving server and table to newGroup");
    rsGroupAdmin.moveServersAndTables(Sets.newHashSet(targetServer.getAddress()),
      Sets.newHashSet(tableName), newGroup.getName());

    // verify group change
    Assert.assertEquals(newGroup.getName(),
      rsGroupAdmin.getRSGroupInfoOfTable(tableName).getName());

    // verify servers' not exist in old group
    Set<Address> defaultServers =
      rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers();
    assertFalse(defaultServers.contains(targetServer.getAddress()));

    // verify servers' exist in new group
    Set<Address> newGroupServers = rsGroupAdmin.getRSGroupInfo(newGroup.getName()).getServers();
    assertTrue(newGroupServers.contains(targetServer.getAddress()));

    // verify tables' not exist in old group
    Set<TableName> defaultTables =
      rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getTables();
    assertFalse(defaultTables.contains(tableName));

    // verify tables' exist in new group
    Set<TableName> newGroupTables = rsGroupAdmin.getRSGroupInfo(newGroup.getName()).getTables();
    assertTrue(newGroupTables.contains(tableName));

    // verify that all region still assgin on targetServer
    Assert.assertEquals(5, getTableServerRegionMap().get(tableName).get(targetServer).size());

    assertTrue(observer.preMoveServersAndTables);
    assertTrue(observer.postMoveServersAndTables);
  }

  @Test
  public void testMoveServersFromDefaultGroup() throws Exception {
    // create groups and assign servers
    rsGroupAdmin.addRSGroup("foo");

    RSGroupInfo fooGroup = rsGroupAdmin.getRSGroupInfo("foo");
    assertEquals(0, fooGroup.getServers().size());
    RSGroupInfo defaultGroup = rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP);

    // test remove all servers from default
    try {
      rsGroupAdmin.moveServers(defaultGroup.getServers(), fooGroup.getName());
      fail(RSGroupAdminServer.KEEP_ONE_SERVER_IN_DEFAULT_ERROR_MESSAGE);
    } catch (ConstraintException ex) {
      assertTrue(
        ex.getMessage().contains(RSGroupAdminServer.KEEP_ONE_SERVER_IN_DEFAULT_ERROR_MESSAGE));
    }

    // test success case, remove one server from default ,keep at least one server
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
        return getNumServers() == rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP)
          .getServers().size();
      }
    });

    fooGroup = rsGroupAdmin.getRSGroupInfo("foo");
    assertEquals(0, fooGroup.getServers().size());

    // test group removal
    LOG.info("Remove group " + fooGroup.getName());
    rsGroupAdmin.removeRSGroup(fooGroup.getName());
    Assert.assertEquals(null, rsGroupAdmin.getRSGroupInfo(fooGroup.getName()));
  }

  @Test
  public void testFailedMoveWhenMoveServer() throws Exception {
    final RSGroupInfo newGroup = addGroup(getGroupName(name.getMethodName()), 1);
    final byte[] familyNameBytes = Bytes.toBytes("f");
    final int tableRegionCount = 10;
    // All the regions created below will be assigned to the default group.
    TEST_UTIL.createMultiRegionTable(tableName, familyNameBytes, tableRegionCount);
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        List<String> regions = getTableRegionMap().get(tableName);
        if (regions == null) {
          return false;
        }
        return getTableRegionMap().get(tableName).size() >= tableRegionCount;
      }
    });

    // get target server to move, which should has more than one regions
    // randomly set a region state to SPLITTING
    Map<ServerName, List<String>> assignMap = getTableServerRegionMap().get(tableName);
    String rregion = null;
    ServerName toMoveServer = null;
    for (ServerName server : assignMap.keySet()) {
      rregion = assignMap.get(server).size() > 1 && !newGroup.containsServer(server.getAddress()) ?
          assignMap.get(server).get(0) :
          null;
      if (rregion != null) {
        toMoveServer = server;
        break;
      }
    }
    assert toMoveServer != null;
    RegionInfo ri = TEST_UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager().
        getRegionInfo(Bytes.toBytesBinary(rregion));
    RegionStateNode rsn =
        TEST_UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager().getRegionStates()
            .getRegionStateNode(ri);
    rsn.setState(RegionState.State.SPLITTING);

    // start thread to recover region state
    final ServerName movedServer = toMoveServer;
    final String sregion = rregion;
    AtomicBoolean changed = new AtomicBoolean(false);
    Thread t1 = new Thread(() -> {
      LOG.debug("thread1 start running, will recover region state");
      long current = System.currentTimeMillis();
      while (System.currentTimeMillis() - current <= 50000) {
        List<RegionInfo> regions = master.getAssignmentManager().getRegionsOnServer(movedServer);
        LOG.debug("server region size is:{}", regions.size());
        assert regions.size() >= 1;
        // when there is exactly one region left, we can determine the move operation encountered
        // exception caused by the strange region state.
        if (regions.size() == 1) {
          assertEquals(regions.get(0).getRegionNameAsString(), sregion);
          rsn.setState(RegionState.State.OPEN);
          LOG.info("set region {} state OPEN", sregion);
          changed.set(true);
          break;
        }
        sleep(5000);
      }
    });
    t1.start();

    // move target server to group
    Thread t2 = new Thread(() -> {
      LOG.info("thread2 start running, to move regions");
      try {
        rsGroupAdmin.moveServers(Sets.newHashSet(movedServer.getAddress()), newGroup.getName());
      } catch (IOException e) {
        LOG.error("move server error", e);
      }
    });
    t2.start();

    t1.join();
    t2.join();

    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() {
        if (changed.get()) {
          return master.getAssignmentManager().getRegionsOnServer(movedServer).size() == 0 && !rsn
              .getRegionLocation().equals(movedServer);
        }
        return false;
      }
    });
  }

  @Test
  public void testFailedMoveWhenMoveTable() throws Exception {
    final RSGroupInfo newGroup = addGroup(getGroupName(name.getMethodName()), 1);
    final byte[] familyNameBytes = Bytes.toBytes("f");
    final int tableRegionCount = 5;
    // All the regions created below will be assigned to the default group.
    TEST_UTIL.createMultiRegionTable(tableName, familyNameBytes, tableRegionCount);
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        List<String> regions = getTableRegionMap().get(tableName);
        if (regions == null) {
          return false;
        }
        return getTableRegionMap().get(tableName).size() >= tableRegionCount;
      }
    });

    // randomly set a region state to SPLITTING
    Map<ServerName, List<String>> assignMap = getTableServerRegionMap().get(tableName);
    String rregion = null;
    ServerName srcServer = null;
    for (ServerName server : assignMap.keySet()) {
      rregion = assignMap.get(server).size() >= 1 && !newGroup.containsServer(server.getAddress()) ?
          assignMap.get(server).get(0) :
          null;
      if (rregion != null) {
        srcServer = server;
        break;
      }
    }
    assert srcServer != null;
    RegionInfo ri = TEST_UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager().
        getRegionInfo(Bytes.toBytesBinary(rregion));
    RegionStateNode rsn =
        TEST_UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager().getRegionStates()
            .getRegionStateNode(ri);
    rsn.setState(RegionState.State.SPLITTING);

    // move table to group
    Thread t2 = new Thread(() -> {
      LOG.info("thread2 start running, to move regions");
      try {
        rsGroupAdmin.moveTables(Sets.newHashSet(tableName), newGroup.getName());
      } catch (IOException e) {
        LOG.error("move server error", e);
      }
    });
    t2.start();

    // start thread to recover region state
    final ServerName ss = srcServer;
    final String sregion = rregion;
    AtomicBoolean changed = new AtomicBoolean(false);
    Thread t1 = new Thread(() -> {
      LOG.info("thread1 start running, will recover region state");
      long current = System.currentTimeMillis();
      while (System.currentTimeMillis() - current <= 50000) {
        List<RegionInfo> regions = master.getAssignmentManager().getRegionsOnServer(ss);
        List<RegionInfo> tableRegions = new ArrayList<>();
        for (RegionInfo regionInfo : regions) {
          if (regionInfo.getTable().equals(tableName)) {
            tableRegions.add(regionInfo);
          }
        }
        LOG.debug("server table region size is:{}", tableRegions.size());
        assert tableRegions.size() >= 1;
        // when there is exactly one region left, we can determine the move operation encountered
        // exception caused by the strange region state.
        if (tableRegions.size() == 1) {
          assertEquals(tableRegions.get(0).getRegionNameAsString(), sregion);
          rsn.setState(RegionState.State.OPEN);
          LOG.info("set region {} state OPEN", sregion);
          changed.set(true);
          break;
        }
        sleep(5000);
      }
    });
    t1.start();

    t1.join();
    t2.join();

    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() {
        if (changed.get()) {
          boolean serverHasTableRegions = false;
          for (RegionInfo regionInfo : master.getAssignmentManager().getRegionsOnServer(ss)) {
            if (regionInfo.getTable().equals(tableName)) {
              serverHasTableRegions = true;
              break;
            }
          }
          return !serverHasTableRegions && !rsn.getRegionLocation().equals(ss);
        }
        return false;
      }
    });
  }

  @Test
  public void testMoveTablePerformance() throws Exception {
    final RSGroupInfo newGroup = addGroup(getGroupName(name.getMethodName()), 1);
    final byte[] familyNameBytes = Bytes.toBytes("f");
    final int tableRegionCount = 100;
    // All the regions created below will be assigned to the default group.
    TEST_UTIL.createMultiRegionTable(tableName, familyNameBytes, tableRegionCount);
    TEST_UTIL.waitFor(WAIT_TIMEOUT, (Waiter.Predicate<Exception>) () -> {
      List<String> regions = getTableRegionMap().get(tableName);
      if (regions == null) {
        return false;
      }
      return getTableRegionMap().get(tableName).size() >= tableRegionCount;
    });
    long startTime = System.currentTimeMillis();
    rsGroupAdmin.moveTables(Sets.newHashSet(tableName), newGroup.getName());
    long timeTaken = System.currentTimeMillis() - startTime;
    String msg =
      "Should not take mote than 15000 ms to move a table with 100 regions. Time taken  ="
        + timeTaken + " ms";
    // This test case is meant to be used for verifying the performance quickly by a developer.
    // Moving 100 regions takes much less than 15000 ms. Given 15000 ms so test cases passes
    // on all environment.
    assertTrue(msg, timeTaken < 15000);
    LOG.info("Time taken to move a table with 100 region is {} ms", timeTaken);
  }
}
