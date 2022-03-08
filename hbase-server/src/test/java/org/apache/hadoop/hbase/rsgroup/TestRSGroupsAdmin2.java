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
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
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
import org.apache.hadoop.hbase.testclassification.RSGroupTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

@Category({ RSGroupTests.class, LargeTests.class })
public class TestRSGroupsAdmin2 extends TestRSGroupsBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRSGroupsAdmin2.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRSGroupsAdmin2.class);

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
    for (ServerName server : ADMIN.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS))
      .getLiveServerMetrics().keySet()) {
      if (!newGroup.containsServer(server.getAddress())) {
        tmpTargetServer = server;
        break;
      }
    }
    final ServerName targetServer = tmpTargetServer;
    // move target server to group
    ADMIN.moveServersToRSGroup(Sets.newHashSet(targetServer.getAddress()), newGroup.getName());
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return ADMIN.getRegions(targetServer).size() <= 0;
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
          ADMIN.getClusterMetrics(EnumSet.of(Option.REGIONS_IN_TRANSITION))
            .getRegionStatesInTransition().size() < 1;
      }
    });

    // verify that targetServer didn't open it
    for (RegionInfo region : ADMIN.getRegions(targetServer)) {
      if (targetRegion.equals(region.getRegionNameAsString())) {
        fail("Target server opened region");
      }
    }
  }

  @Test
  public void testRegionServerMove() throws IOException, InterruptedException {
    int initNumGroups = ADMIN.listRSGroups().size();
    RSGroupInfo appInfo = addGroup(getGroupName(name.getMethodName()), 1);
    RSGroupInfo adminInfo = addGroup(getGroupName(name.getMethodName()), 1);
    RSGroupInfo dInfo = ADMIN.getRSGroup(RSGroupInfo.DEFAULT_GROUP);
    assertEquals(initNumGroups + 2, ADMIN.listRSGroups().size());
    assertEquals(1, adminInfo.getServers().size());
    assertEquals(1, appInfo.getServers().size());
    assertEquals(getNumServers() - 2, dInfo.getServers().size());
    ADMIN.moveServersToRSGroup(appInfo.getServers(), RSGroupInfo.DEFAULT_GROUP);
    ADMIN.removeRSGroup(appInfo.getName());
    ADMIN.moveServersToRSGroup(adminInfo.getServers(), RSGroupInfo.DEFAULT_GROUP);
    ADMIN.removeRSGroup(adminInfo.getName());
    assertEquals(ADMIN.listRSGroups().size(), initNumGroups);
  }

  @Test
  public void testMoveServers() throws Exception {
    // create groups and assign servers
    addGroup("bar", 3);
    ADMIN.addRSGroup("foo");

    RSGroupInfo barGroup = ADMIN.getRSGroup("bar");
    RSGroupInfo fooGroup = ADMIN.getRSGroup("foo");
    assertEquals(3, barGroup.getServers().size());
    assertEquals(0, fooGroup.getServers().size());

    // test fail bogus server move
    try {
      ADMIN.moveServersToRSGroup(Sets.newHashSet(Address.fromString("foo:9999")), "foo");
      fail("Bogus servers shouldn't have been successfully moved.");
    } catch (IOException ex) {
      String exp = "Server foo:9999 is either offline or it does not exist.";
      String msg = "Expected '" + exp + "' in exception message: ";
      assertTrue(msg + " " + ex.getMessage(), ex.getMessage().contains(exp));
    }

    // test success case
    LOG.info("moving servers " + barGroup.getServers() + " to group foo");
    ADMIN.moveServersToRSGroup(barGroup.getServers(), fooGroup.getName());

    barGroup = ADMIN.getRSGroup("bar");
    fooGroup = ADMIN.getRSGroup("foo");
    assertEquals(0, barGroup.getServers().size());
    assertEquals(3, fooGroup.getServers().size());

    LOG.info("moving servers " + fooGroup.getServers() + " to group default");
    ADMIN.moveServersToRSGroup(fooGroup.getServers(), RSGroupInfo.DEFAULT_GROUP);

    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return getNumServers() == ADMIN.getRSGroup(RSGroupInfo.DEFAULT_GROUP).getServers().size();
      }
    });

    fooGroup = ADMIN.getRSGroup("foo");
    assertEquals(0, fooGroup.getServers().size());

    // test group removal
    LOG.info("Remove group " + barGroup.getName());
    ADMIN.removeRSGroup(barGroup.getName());
    assertEquals(null, ADMIN.getRSGroup(barGroup.getName()));
    LOG.info("Remove group " + fooGroup.getName());
    ADMIN.removeRSGroup(fooGroup.getName());
    assertEquals(null, ADMIN.getRSGroup(fooGroup.getName()));
  }

  @Test
  public void testRemoveServers() throws Exception {
    LOG.info("testRemoveServers");
    final RSGroupInfo newGroup = addGroup(getGroupName(name.getMethodName()), 3);
    Iterator<Address> iterator = newGroup.getServers().iterator();
    ServerName targetServer = getServerName(iterator.next());

    // remove online servers
    try {
      ADMIN.removeServersFromRSGroup(Sets.newHashSet(targetServer.getAddress()));
      fail("Online servers shouldn't have been successfully removed.");
    } catch (IOException ex) {
      String exp =
        "Server " + targetServer.getAddress() + " is an online server, not allowed to remove.";
      String msg = "Expected '" + exp + "' in exception message: ";
      assertTrue(msg + " " + ex.getMessage(), ex.getMessage().contains(exp));
    }
    assertTrue(newGroup.getServers().contains(targetServer.getAddress()));

    // remove dead servers
    NUM_DEAD_SERVERS = CLUSTER.getClusterMetrics().getDeadServerNames().size();
    try {
      // stopping may cause an exception
      // due to the connection loss
      LOG.info("stopping server " + targetServer.getServerName());
      ADMIN.stopRegionServer(targetServer.getAddress().toString());
      NUM_DEAD_SERVERS++;
    } catch (Exception e) {
    }

    // wait for stopped regionserver to dead server list
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return !MASTER.getServerManager().areDeadServersInProgress() &&
          CLUSTER.getClusterMetrics().getDeadServerNames().size() == NUM_DEAD_SERVERS;
      }
    });

    try {
      ADMIN.removeServersFromRSGroup(Sets.newHashSet(targetServer.getAddress()));
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
    assertTrue(MASTER.getServerManager().getOnlineServers().containsKey(targetServer));
    serversToDecommission.add(targetServer);

    ADMIN.decommissionRegionServers(serversToDecommission, true);
    assertEquals(1, ADMIN.listDecommissionedRegionServers().size());

    assertTrue(newGroup.getServers().contains(targetServer.getAddress()));
    ADMIN.removeServersFromRSGroup(Sets.newHashSet(targetServer.getAddress()));
    Set<Address> newGroupServers = ADMIN.getRSGroup(newGroup.getName()).getServers();
    assertFalse(newGroupServers.contains(targetServer.getAddress()));
    assertEquals(2, newGroupServers.size());

    assertTrue(OBSERVER.preRemoveServersCalled);
    assertTrue(OBSERVER.postRemoveServersCalled);
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
    for (ServerName server : ADMIN.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS))
      .getLiveServerMetrics().keySet()) {
      if (!newGroup.containsServer(server.getAddress()) &&
        !ADMIN.getRSGroup("master").containsServer(server.getAddress())) {
        targetServer = server;
        break;
      }
    }

    LOG.debug("Print group info : " + ADMIN.listRSGroups());
    int oldDefaultGroupServerSize = ADMIN.getRSGroup(RSGroupInfo.DEFAULT_GROUP).getServers().size();
    int oldDefaultGroupTableSize = ADMIN.listTablesInRSGroup(RSGroupInfo.DEFAULT_GROUP).size();
    assertTrue(OBSERVER.preListTablesInRSGroupCalled);
    assertTrue(OBSERVER.postListTablesInRSGroupCalled);

    // test fail bogus server move
    try {
      ADMIN.moveServersToRSGroup(Sets.newHashSet(Address.fromString("foo:9999")),
        newGroup.getName());
      ADMIN.setRSGroup(Sets.newHashSet(tableName), newGroup.getName());
      fail("Bogus servers shouldn't have been successfully moved.");
    } catch (IOException ex) {
      String exp = "Server foo:9999 is either offline or it does not exist.";
      String msg = "Expected '" + exp + "' in exception message: ";
      assertTrue(msg + " " + ex.getMessage(), ex.getMessage().contains(exp));
    }

    // test move when src = dst
    ADMIN.moveServersToRSGroup(Sets.newHashSet(targetServer.getAddress()),
      RSGroupInfo.DEFAULT_GROUP);
    ADMIN.setRSGroup(Sets.newHashSet(tableName), RSGroupInfo.DEFAULT_GROUP);

    // verify default group info
    assertEquals(oldDefaultGroupServerSize,
      ADMIN.getRSGroup(RSGroupInfo.DEFAULT_GROUP).getServers().size());
    assertEquals(oldDefaultGroupTableSize,
      ADMIN.listTablesInRSGroup(RSGroupInfo.DEFAULT_GROUP).size());

    // verify new group info
    assertEquals(1, ADMIN.getRSGroup(newGroup.getName()).getServers().size());
    assertEquals(0,
      ADMIN.getConfiguredNamespacesAndTablesInRSGroup(newGroup.getName()).getSecond().size());
    assertTrue(OBSERVER.preGetConfiguredNamespacesAndTablesInRSGroupCalled);
    assertTrue(OBSERVER.postGetConfiguredNamespacesAndTablesInRSGroupCalled);

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
          ADMIN.getClusterMetrics(EnumSet.of(Option.REGIONS_IN_TRANSITION))
            .getRegionStatesInTransition().size() < 1;
      }
    });

    // verify that all region move to targetServer
    assertEquals(5, getTableServerRegionMap().get(tableName).get(targetServer).size());

    // move targetServer and table to newGroup
    LOG.info("moving server and table to newGroup");
    ADMIN.moveServersToRSGroup(Sets.newHashSet(targetServer.getAddress()), newGroup.getName());
    ADMIN.setRSGroup(Sets.newHashSet(tableName), newGroup.getName());

    // verify group change
    assertEquals(newGroup.getName(), ADMIN.getRSGroup(tableName).getName());

    // verify servers' not exist in old group
    Set<Address> defaultServers = ADMIN.getRSGroup(RSGroupInfo.DEFAULT_GROUP).getServers();
    assertFalse(defaultServers.contains(targetServer.getAddress()));

    // verify servers' exist in new group
    Set<Address> newGroupServers = ADMIN.getRSGroup(newGroup.getName()).getServers();
    assertTrue(newGroupServers.contains(targetServer.getAddress()));

    // verify tables' not exist in old group
    Set<TableName> defaultTables =
      Sets.newHashSet(ADMIN.listTablesInRSGroup(RSGroupInfo.DEFAULT_GROUP));
    assertFalse(defaultTables.contains(tableName));

    // verify tables' exist in new group
    Set<TableName> newGroupTables = Sets
      .newHashSet(ADMIN.getConfiguredNamespacesAndTablesInRSGroup(newGroup.getName()).getSecond());
    assertTrue(newGroupTables.contains(tableName));

    // verify that all region still assign on targetServer
    // TODO: uncomment after we reimplement moveServersAndTables, now the implementation is
    // moveToRSGroup first and then moveTables, so the region will be moved to other region servers.
    // assertEquals(5, getTableServerRegionMap().get(tableName).get(targetServer).size());

    assertTrue(OBSERVER.preMoveServersCalled);
    assertTrue(OBSERVER.postMoveServersCalled);
  }

  @Test
  public void testMoveServersFromDefaultGroup() throws Exception {
    // create groups and assign servers
    ADMIN.addRSGroup("foo");

    RSGroupInfo fooGroup = ADMIN.getRSGroup("foo");
    assertEquals(0, fooGroup.getServers().size());
    RSGroupInfo defaultGroup = ADMIN.getRSGroup(RSGroupInfo.DEFAULT_GROUP);

    // test remove all servers from default
    try {
      ADMIN.moveServersToRSGroup(defaultGroup.getServers(), fooGroup.getName());
      fail(RSGroupInfoManagerImpl.KEEP_ONE_SERVER_IN_DEFAULT_ERROR_MESSAGE);
    } catch (ConstraintException ex) {
      assertTrue(
        ex.getMessage().contains(RSGroupInfoManagerImpl.KEEP_ONE_SERVER_IN_DEFAULT_ERROR_MESSAGE));
    }

    // test success case, remove one server from default ,keep at least one server
    if (defaultGroup.getServers().size() > 1) {
      Address serverInDefaultGroup = defaultGroup.getServers().iterator().next();
      LOG.info("moving server " + serverInDefaultGroup + " from group default to group " +
        fooGroup.getName());
      ADMIN.moveServersToRSGroup(Sets.newHashSet(serverInDefaultGroup), fooGroup.getName());
    }

    fooGroup = ADMIN.getRSGroup("foo");
    LOG.info("moving servers " + fooGroup.getServers() + " to group default");
    ADMIN.moveServersToRSGroup(fooGroup.getServers(), RSGroupInfo.DEFAULT_GROUP);

    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return getNumServers() == ADMIN.getRSGroup(RSGroupInfo.DEFAULT_GROUP).getServers().size();
      }
    });

    fooGroup = ADMIN.getRSGroup("foo");
    assertEquals(0, fooGroup.getServers().size());

    // test group removal
    LOG.info("Remove group " + fooGroup.getName());
    ADMIN.removeRSGroup(fooGroup.getName());
    assertEquals(null, ADMIN.getRSGroup(fooGroup.getName()));
  }

  @Test
  public void testFailedMoveBeforeRetryExhaustedWhenMoveServer() throws Exception {
    String groupName = getGroupName(name.getMethodName());
    ADMIN.addRSGroup(groupName);
    final RSGroupInfo newGroup = ADMIN.getRSGroup(groupName);
    Pair<ServerName, RegionStateNode> gotPair = createTableWithRegionSplitting(newGroup, 10);

    // start thread to recover region state
    final ServerName movedServer = gotPair.getFirst();
    final RegionStateNode rsn = gotPair.getSecond();
    AtomicBoolean changed = new AtomicBoolean(false);
    Thread t1 = recoverRegionStateThread(movedServer,
      server -> MASTER.getAssignmentManager().getRegionsOnServer(movedServer), rsn, changed);
    t1.start();

    // move target server to group
    Thread t2 = new Thread(() -> {
      LOG.info("thread2 start running, to move regions");
      try {
        ADMIN.moveServersToRSGroup(Sets.newHashSet(movedServer.getAddress()), newGroup.getName());
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
          return MASTER.getAssignmentManager().getRegionsOnServer(movedServer).size() == 0 &&
            !rsn.getRegionLocation().equals(movedServer);
        }
        return false;
      }
    });
  }

  private <T> Thread recoverRegionStateThread(T owner, Function<T, List<RegionInfo>> getRegions,
    RegionStateNode rsn, AtomicBoolean changed) {
    return new Thread(() -> {
      LOG.info("thread1 start running, will recover region state");
      long current = EnvironmentEdgeManager.currentTime();
      // wait until there is only left the region we changed state and recover its state.
      // wait time is set according to the number of max retries, all except failed regions will be
      // moved in one retry, and will sleep 1s until next retry.
      while (EnvironmentEdgeManager.currentTime() -
          current <= RSGroupInfoManagerImpl.DEFAULT_MAX_RETRY_VALUE * 1000) {
        List<RegionInfo> regions = getRegions.apply(owner);
        LOG.debug("server table region size is:{}", regions.size());
        assert regions.size() >= 1;
        // when there is exactly one region left, we can determine the move operation encountered
        // exception caused by the strange region state.
        if (regions.size() == 1) {
          assertEquals(regions.get(0).getRegionNameAsString(),
            rsn.getRegionInfo().getRegionNameAsString());
          rsn.setState(RegionState.State.OPEN);
          LOG.info("set region {} state OPEN", rsn.getRegionInfo().getRegionNameAsString());
          changed.set(true);
          break;
        }
        sleep(5000);
      }
    });
  }

  private Pair<ServerName, RegionStateNode> createTableWithRegionSplitting(RSGroupInfo rsGroupInfo,
    int tableRegionCount) throws Exception {
    final byte[] familyNameBytes = Bytes.toBytes("f");
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

    return randomlySetOneRegionStateToSplitting(rsGroupInfo);
  }

  /**
   * Randomly choose a region to set state.
   * @param newGroup target group
   * @return source server of region, and region state
   * @throws IOException if methods called throw
   */
  private Pair<ServerName, RegionStateNode>
    randomlySetOneRegionStateToSplitting(RSGroupInfo newGroup) throws IOException {
    // get target server to move, which should has more than one regions
    // randomly set a region state to SPLITTING to make move fail
    return randomlySetRegionState(newGroup, RegionState.State.SPLITTING, tableName);
  }

  private Pair<ServerName, RegionStateNode> randomlySetRegionState(RSGroupInfo groupInfo,
    RegionState.State state, TableName... tableNames) throws IOException {
    Preconditions.checkArgument(tableNames.length == 1 || tableNames.length == 2,
      "only support one or two tables");
    Map<TableName, Map<ServerName, List<String>>> tableServerRegionMap = getTableServerRegionMap();
    Map<ServerName, List<String>> assignMap = tableServerRegionMap.get(tableNames[0]);
    if (tableNames.length == 2) {
      Map<ServerName, List<String>> assignMap2 = tableServerRegionMap.get(tableNames[1]);
      assignMap2.forEach((k, v) -> {
        if (!assignMap.containsKey(k)) {
          assignMap.remove(k);
        }
      });
    }
    String toCorrectRegionName = null;
    ServerName srcServer = null;
    for (ServerName server : assignMap.keySet()) {
      toCorrectRegionName =
        assignMap.get(server).size() >= 1 && !groupInfo.containsServer(server.getAddress())
          ? assignMap.get(server).get(0)
          : null;
      if (toCorrectRegionName != null) {
        srcServer = server;
        break;
      }
    }
    assert srcServer != null;
    RegionInfo toCorrectRegionInfo = TEST_UTIL.getMiniHBaseCluster().getMaster()
      .getAssignmentManager().getRegionInfo(Bytes.toBytesBinary(toCorrectRegionName));
    RegionStateNode rsn = TEST_UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
      .getRegionStates().getRegionStateNode(toCorrectRegionInfo);
    rsn.setState(state);
    return new Pair<>(srcServer, rsn);
  }

  @Test
  public void testFailedMoveServersAndRepair() throws Exception {
    // This UT calls moveToRSGroup() twice to test the idempotency of it.
    // The first time, movement fails because a region is made in SPLITTING state
    // which will not be moved.
    // The second time, the region state is OPEN and check if all
    // regions on target group servers after the call.
    final RSGroupInfo newGroup = addGroup(getGroupName(name.getMethodName()), 1);

    // create table
    // randomly set a region state to SPLITTING to make move abort
    Pair<ServerName, RegionStateNode> gotPair =
      createTableWithRegionSplitting(newGroup, ThreadLocalRandom.current().nextInt(8) + 4);
    RegionStateNode rsn = gotPair.getSecond();
    ServerName srcServer = rsn.getRegionLocation();

    // move server to newGroup and check regions
    try {
      ADMIN.moveServersToRSGroup(Sets.newHashSet(srcServer.getAddress()), newGroup.getName());
      fail("should get IOException when retry exhausted but there still exists failed moved " +
        "regions");
    } catch (Exception e) {
      assertTrue(
        e.getMessage().contains(gotPair.getSecond().getRegionInfo().getRegionNameAsString()));
    }
    for (RegionInfo regionInfo : MASTER.getAssignmentManager().getAssignedRegions()) {
      if (regionInfo.getTable().equals(tableName) && regionInfo.equals(rsn.getRegionInfo())) {
        assertEquals(
          MASTER.getAssignmentManager().getRegionStates().getRegionServerOfRegion(regionInfo),
          srcServer);
      }
    }

    // retry move server to newGroup and check if all regions on srcServer was moved
    rsn.setState(RegionState.State.OPEN);
    ADMIN.moveServersToRSGroup(Sets.newHashSet(srcServer.getAddress()), newGroup.getName());
    assertEquals(MASTER.getAssignmentManager().getRegionsOnServer(srcServer).size(), 0);
  }

  @Test
  public void testFailedMoveServersTablesAndRepair() throws Exception {
    // This UT calls moveTablesAndServers() twice to test the idempotency of it.
    // The first time, movement fails because a region is made in SPLITTING state
    // which will not be moved.
    // The second time, the region state is OPEN and check if all
    // regions on target group servers after the call.
    final RSGroupInfo newGroup = addGroup(getGroupName(name.getMethodName()), 1);
    // create table
    final byte[] familyNameBytes = Bytes.toBytes("f");
    TableName table1 = TableName.valueOf(tableName.getNameAsString() + "_1");
    TableName table2 = TableName.valueOf(tableName.getNameAsString() + "_2");
    Random rand = ThreadLocalRandom.current();
    TEST_UTIL.createMultiRegionTable(table1, familyNameBytes, rand.nextInt(12) + 4);
    TEST_UTIL.createMultiRegionTable(table2, familyNameBytes, rand.nextInt(12) + 4);

    // randomly set a region state to SPLITTING to make move abort
    Pair<ServerName, RegionStateNode> gotPair =
      randomlySetRegionState(newGroup, RegionState.State.SPLITTING, table1, table2);
    RegionStateNode rsn = gotPair.getSecond();
    ServerName srcServer = rsn.getRegionLocation();

    // move server and table to newGroup and check regions
    try {
      ADMIN.moveServersToRSGroup(Sets.newHashSet(srcServer.getAddress()), newGroup.getName());
      ADMIN.setRSGroup(Sets.newHashSet(table2), newGroup.getName());
      fail("should get IOException when retry exhausted but there still exists failed moved " +
        "regions");
    } catch (Exception e) {
      assertTrue(
        e.getMessage().contains(gotPair.getSecond().getRegionInfo().getRegionNameAsString()));
    }
    for (RegionInfo regionInfo : MASTER.getAssignmentManager().getAssignedRegions()) {
      if (regionInfo.getTable().equals(table1) && regionInfo.equals(rsn.getRegionInfo())) {
        assertEquals(
          MASTER.getAssignmentManager().getRegionStates().getRegionServerOfRegion(regionInfo),
          srcServer);
      }
    }

    // retry moveServersAndTables to newGroup and check if all regions on srcServer belongs to
    // table2
    rsn.setState(RegionState.State.OPEN);
    ADMIN.moveServersToRSGroup(Sets.newHashSet(srcServer.getAddress()), newGroup.getName());
    ADMIN.setRSGroup(Sets.newHashSet(table2), newGroup.getName());
    for (RegionInfo regionsInfo : MASTER.getAssignmentManager().getRegionsOnServer(srcServer)) {
      assertEquals(regionsInfo.getTable(), table2);
    }
  }

  @Test
  public void testMoveServersToRSGroupPerformance() throws Exception {
    final RSGroupInfo newGroup = addGroup(getGroupName(name.getMethodName()), 2);
    final byte[] familyNameBytes = Bytes.toBytes("f");
    // there will be 100 regions are both the serves
    final int tableRegionCount = 200;
    // All the regions created below will be assigned to the default group.
    TEST_UTIL.createMultiRegionTable(tableName, familyNameBytes, tableRegionCount);
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override public boolean evaluate() throws Exception {
        List<String> regions = getTableRegionMap().get(tableName);
        if (regions == null) {
          return false;
        }
        return getTableRegionMap().get(tableName).size() >= tableRegionCount;
      }
    });
    ADMIN.setRSGroup(Sets.newHashSet(tableName), newGroup.getName());
    TEST_UTIL.waitUntilAllRegionsAssigned(tableName);
    String rsGroup2 = "rsGroup2";
    ADMIN.addRSGroup(rsGroup2);

    long startTime = EnvironmentEdgeManager.currentTime();
    ADMIN.moveServersToRSGroup(Sets.newHashSet(newGroup.getServers().first()), rsGroup2);
    long timeTaken = EnvironmentEdgeManager.currentTime() - startTime;
    String msg =
      "Should not take mote than 15000 ms to move a table with 100 regions. Time taken  ="
        + timeTaken + " ms";
    //This test case is meant to be used for verifying the performance quickly by a developer.
    //Moving 100 regions takes much less than 15000 ms. Given 15000 ms so test cases passes
    // on all environment.
    assertTrue(msg, timeTaken < 15000);
    LOG.info("Time taken to move a table with 100 region is {} ms", timeTaken);
  }
}
