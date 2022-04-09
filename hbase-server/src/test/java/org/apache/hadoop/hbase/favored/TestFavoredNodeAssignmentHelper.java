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
package org.apache.hadoop.hbase.favored;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.master.RackManager;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Triple;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

@Category({MasterTests.class, LargeTests.class})
public class TestFavoredNodeAssignmentHelper {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestFavoredNodeAssignmentHelper.class);

  private static List<ServerName> servers = new ArrayList<>();
  private static Map<String, List<ServerName>> rackToServers = new HashMap<>();
  private static RackManager rackManager = Mockito.mock(RackManager.class);

  // Some tests have randomness, so we run them multiple times
  private static final int MAX_ATTEMPTS = 100;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // Set up some server -> rack mappings
    // Have three racks in the cluster with 10 hosts each.
    for (int i = 0; i < 40; i++) {
      ServerName server = ServerName.valueOf("foo" + i + ":1234", -1);
      if (i < 10) {
        Mockito.when(rackManager.getRack(server)).thenReturn("rack1");
        if (rackToServers.get("rack1") == null) {
          List<ServerName> servers = new ArrayList<>();
          rackToServers.put("rack1", servers);
        }
        rackToServers.get("rack1").add(server);
      }
      if (i >= 10 && i < 20) {
        Mockito.when(rackManager.getRack(server)).thenReturn("rack2");
        if (rackToServers.get("rack2") == null) {
          List<ServerName> servers = new ArrayList<>();
          rackToServers.put("rack2", servers);
        }
        rackToServers.get("rack2").add(server);
      }
      if (i >= 20 && i < 30) {
        Mockito.when(rackManager.getRack(server)).thenReturn("rack3");
        if (rackToServers.get("rack3") == null) {
          List<ServerName> servers = new ArrayList<>();
          rackToServers.put("rack3", servers);
        }
        rackToServers.get("rack3").add(server);
      }
      servers.add(server);
    }
  }

  // The tests decide which racks to work with, and how many machines to
  // work with from any given rack
  // Return a rondom 'count' number of servers from 'rack'
  private static List<ServerName> getServersFromRack(Map<String, Integer> rackToServerCount) {
    List<ServerName> chosenServers = new ArrayList<>();
    for (Map.Entry<String, Integer> entry : rackToServerCount.entrySet()) {
      List<ServerName> servers = rackToServers.get(entry.getKey());
      for (int i = 0; i < entry.getValue(); i++) {
        chosenServers.add(servers.get(i));
      }
    }
    return chosenServers;
  }

  @Test
  public void testSmallCluster() {
    // Test the case where we cannot assign favored nodes (because the number
    // of nodes in the cluster is too less)
    Map<String,Integer> rackToServerCount = new HashMap<>();
    rackToServerCount.put("rack1", 2);
    List<ServerName> servers = getServersFromRack(rackToServerCount);
    FavoredNodeAssignmentHelper helper = new FavoredNodeAssignmentHelper(servers,
        new Configuration());
    helper.initialize();
    assertFalse(helper.canPlaceFavoredNodes());
  }

  @Test
  public void testPlacePrimaryRSAsRoundRobin() {
    // Test the regular case where there are many servers in different racks
    // Test once for few regions and once for many regions
    primaryRSPlacement(6, null, 10, 10, 10);
    // now create lots of regions and try to place them on the limited number of machines
    primaryRSPlacement(600, null, 10, 10, 10);
  }

  @Test
  public void testRoundRobinAssignmentsWithUnevenSizedRacks() {
    //In the case of uneven racks, the regions should be distributed
    //proportionately to the rack sizes
    primaryRSPlacement(6, null, 10, 10, 10);
    primaryRSPlacement(600, null, 10, 10, 5);
    primaryRSPlacement(600, null, 10, 5, 10);
    primaryRSPlacement(600, null, 5, 10, 10);
    primaryRSPlacement(500, null, 10, 10, 5);
    primaryRSPlacement(500, null, 10, 5, 10);
    primaryRSPlacement(500, null, 5, 10, 10);
    primaryRSPlacement(500, null, 9, 7, 8);
    primaryRSPlacement(500, null, 8, 7, 9);
    primaryRSPlacement(500, null, 7, 9, 8);
    primaryRSPlacement(459, null, 7, 9, 8);
  }

  @Test
  public void testSecondaryAndTertiaryPlacementWithSingleRack() {
    // Test the case where there is a single rack and we need to choose
    // Primary/Secondary/Tertiary from a single rack.
    Map<String,Integer> rackToServerCount = new HashMap<>();
    rackToServerCount.put("rack1", 10);
    // have lots of regions to test with
    Triple<Map<RegionInfo, ServerName>, FavoredNodeAssignmentHelper, List<RegionInfo>>
      primaryRSMapAndHelper = secondaryAndTertiaryRSPlacementHelper(60000, rackToServerCount);
    FavoredNodeAssignmentHelper helper = primaryRSMapAndHelper.getSecond();
    Map<RegionInfo, ServerName> primaryRSMap = primaryRSMapAndHelper.getFirst();
    List<RegionInfo> regions = primaryRSMapAndHelper.getThird();
    Map<RegionInfo, ServerName[]> secondaryAndTertiaryMap =
        helper.placeSecondaryAndTertiaryRS(primaryRSMap);
    // although we created lots of regions we should have no overlap on the
    // primary/secondary/tertiary for any given region
    for (RegionInfo region : regions) {
      ServerName[] secondaryAndTertiaryServers = secondaryAndTertiaryMap.get(region);
      assertNotNull(secondaryAndTertiaryServers);
      assertTrue(primaryRSMap.containsKey(region));
      assertTrue(!secondaryAndTertiaryServers[0].equals(primaryRSMap.get(region)));
      assertTrue(!secondaryAndTertiaryServers[1].equals(primaryRSMap.get(region)));
      assertTrue(!secondaryAndTertiaryServers[0].equals(secondaryAndTertiaryServers[1]));
    }
  }

  @Test
  public void testSecondaryAndTertiaryPlacementWithSingleServer() {
    // Test the case where we have a single node in the cluster. In this case
    // the primary can be assigned but the secondary/tertiary would be null
    Map<String,Integer> rackToServerCount = new HashMap<>();
    rackToServerCount.put("rack1", 1);
    Triple<Map<RegionInfo, ServerName>, FavoredNodeAssignmentHelper,
      List<RegionInfo>> primaryRSMapAndHelper =
        secondaryAndTertiaryRSPlacementHelper(1, rackToServerCount);
    FavoredNodeAssignmentHelper helper = primaryRSMapAndHelper.getSecond();
    Map<RegionInfo, ServerName> primaryRSMap = primaryRSMapAndHelper.getFirst();
    List<RegionInfo> regions = primaryRSMapAndHelper.getThird();

    Map<RegionInfo, ServerName[]> secondaryAndTertiaryMap =
        helper.placeSecondaryAndTertiaryRS(primaryRSMap);
    // no secondary/tertiary placement in case of a single RegionServer
    assertTrue(secondaryAndTertiaryMap.get(regions.get(0)) == null);
  }

  @Test
  public void testSecondaryAndTertiaryPlacementWithMultipleRacks() {
    // Test the case where we have multiple racks and the region servers
    // belong to multiple racks
    Map<String,Integer> rackToServerCount = new HashMap<>();
    rackToServerCount.put("rack1", 10);
    rackToServerCount.put("rack2", 10);

    Triple<Map<RegionInfo, ServerName>, FavoredNodeAssignmentHelper, List<RegionInfo>>
      primaryRSMapAndHelper = secondaryAndTertiaryRSPlacementHelper(60000, rackToServerCount);
    FavoredNodeAssignmentHelper helper = primaryRSMapAndHelper.getSecond();
    Map<RegionInfo, ServerName> primaryRSMap = primaryRSMapAndHelper.getFirst();

    assertTrue(primaryRSMap.size() == 60000);
    Map<RegionInfo, ServerName[]> secondaryAndTertiaryMap =
        helper.placeSecondaryAndTertiaryRS(primaryRSMap);
    assertTrue(secondaryAndTertiaryMap.size() == 60000);
    // for every region, the primary should be on one rack and the secondary/tertiary
    // on another (we create a lot of regions just to increase probability of failure)
    for (Map.Entry<RegionInfo, ServerName[]> entry : secondaryAndTertiaryMap.entrySet()) {
      ServerName[] allServersForRegion = entry.getValue();
      String primaryRSRack = rackManager.getRack(primaryRSMap.get(entry.getKey()));
      String secondaryRSRack = rackManager.getRack(allServersForRegion[0]);
      String tertiaryRSRack = rackManager.getRack(allServersForRegion[1]);
      Set<String> racks = Sets.newHashSet(primaryRSRack);
      racks.add(secondaryRSRack);
      racks.add(tertiaryRSRack);
      assertTrue(racks.size() >= 2);
    }
  }

  @Test
  public void testSecondaryAndTertiaryPlacementWithLessThanTwoServersInRacks() {
    // Test the case where we have two racks but with less than two servers in each
    // We will not have enough machines to select secondary/tertiary
    Map<String,Integer> rackToServerCount = new HashMap<>();
    rackToServerCount.put("rack1", 1);
    rackToServerCount.put("rack2", 1);
    Triple<Map<RegionInfo, ServerName>, FavoredNodeAssignmentHelper, List<RegionInfo>>
      primaryRSMapAndHelper = secondaryAndTertiaryRSPlacementHelper(6, rackToServerCount);
    FavoredNodeAssignmentHelper helper = primaryRSMapAndHelper.getSecond();
    Map<RegionInfo, ServerName> primaryRSMap = primaryRSMapAndHelper.getFirst();
    List<RegionInfo> regions = primaryRSMapAndHelper.getThird();
    assertTrue(primaryRSMap.size() == 6);
    Map<RegionInfo, ServerName[]> secondaryAndTertiaryMap =
          helper.placeSecondaryAndTertiaryRS(primaryRSMap);
    for (RegionInfo region : regions) {
      // not enough secondary/tertiary room to place the regions
      assertTrue(secondaryAndTertiaryMap.get(region) == null);
    }
  }

  @Test
  public void testSecondaryAndTertiaryPlacementWithMoreThanOneServerInPrimaryRack() {
    // Test the case where there is only one server in one rack and another rack
    // has more servers. We try to choose secondary/tertiary on different
    // racks than what the primary is on. But if the other rack doesn't have
    // enough nodes to have both secondary/tertiary RSs, the tertiary is placed
    // on the same rack as the primary server is on
    Map<String,Integer> rackToServerCount = new HashMap<>();
    rackToServerCount.put("rack1", 2);
    rackToServerCount.put("rack2", 1);
    Triple<Map<RegionInfo, ServerName>, FavoredNodeAssignmentHelper, List<RegionInfo>>
      primaryRSMapAndHelper = secondaryAndTertiaryRSPlacementHelper(6, rackToServerCount);
    FavoredNodeAssignmentHelper helper = primaryRSMapAndHelper.getSecond();
    Map<RegionInfo, ServerName> primaryRSMap = primaryRSMapAndHelper.getFirst();
    List<RegionInfo> regions = primaryRSMapAndHelper.getThird();
    assertTrue(primaryRSMap.size() == 6);
    Map<RegionInfo, ServerName[]> secondaryAndTertiaryMap =
          helper.placeSecondaryAndTertiaryRS(primaryRSMap);
    assertTrue(secondaryAndTertiaryMap.size() == regions.size());
    for (RegionInfo region : regions) {
      ServerName s = primaryRSMap.get(region);
      ServerName secondaryRS = secondaryAndTertiaryMap.get(region)[0];
      ServerName tertiaryRS = secondaryAndTertiaryMap.get(region)[1];
      Set<String> racks = Sets.newHashSet(rackManager.getRack(s));
      racks.add(rackManager.getRack(secondaryRS));
      racks.add(rackManager.getRack(tertiaryRS));
      assertTrue(racks.size() >= 2);
    }
  }

  private Triple<Map<RegionInfo, ServerName>, FavoredNodeAssignmentHelper, List<RegionInfo>>
  secondaryAndTertiaryRSPlacementHelper(
      int regionCount, Map<String, Integer> rackToServerCount) {
    Map<RegionInfo, ServerName> primaryRSMap = new HashMap<RegionInfo, ServerName>();
    List<ServerName> servers = getServersFromRack(rackToServerCount);
    FavoredNodeAssignmentHelper helper = new FavoredNodeAssignmentHelper(servers, rackManager);
    Map<ServerName, List<RegionInfo>> assignmentMap =
        new HashMap<ServerName, List<RegionInfo>>();
    helper.initialize();
    // create regions
    List<RegionInfo> regions = new ArrayList<>(regionCount);
    for (int i = 0; i < regionCount; i++) {
      regions.add(RegionInfoBuilder.newBuilder(TableName.valueOf(name.getMethodName()))
          .setStartKey(Bytes.toBytes(i))
          .setEndKey(Bytes.toBytes(i + 1))
          .build());
    }
    // place the regions
    helper.placePrimaryRSAsRoundRobin(assignmentMap, primaryRSMap, regions);
    return new Triple<>(primaryRSMap, helper, regions);
  }

  private void primaryRSPlacement(int regionCount, Map<RegionInfo, ServerName> primaryRSMap,
      int firstRackSize, int secondRackSize, int thirdRackSize) {
    Map<String,Integer> rackToServerCount = new HashMap<>();
    rackToServerCount.put("rack1", firstRackSize);
    rackToServerCount.put("rack2", secondRackSize);
    rackToServerCount.put("rack3", thirdRackSize);
    List<ServerName> servers = getServersFromRack(rackToServerCount);
    FavoredNodeAssignmentHelper helper = new FavoredNodeAssignmentHelper(servers,
        rackManager);
    helper.initialize();

    assertTrue(helper.canPlaceFavoredNodes());

    Map<ServerName, List<RegionInfo>> assignmentMap = new HashMap<>();
    if (primaryRSMap == null) primaryRSMap = new HashMap<>();
    // create some regions
    List<RegionInfo> regions = new ArrayList<>(regionCount);
    for (int i = 0; i < regionCount; i++) {
      regions.add(RegionInfoBuilder.newBuilder(TableName.valueOf("foobar"))
          .setStartKey(Bytes.toBytes(i))
          .setEndKey(Bytes.toBytes(i + 1))
          .build());
    }
    // place those regions in primary RSs
    helper.placePrimaryRSAsRoundRobin(assignmentMap, primaryRSMap, regions);

    // we should have all the regions nicely spread across the racks
    int regionsOnRack1 = 0;
    int regionsOnRack2 = 0;
    int regionsOnRack3 = 0;
    for (RegionInfo region : regions) {
      if (rackManager.getRack(primaryRSMap.get(region)).equals("rack1")) {
        regionsOnRack1++;
      } else if (rackManager.getRack(primaryRSMap.get(region)).equals("rack2")) {
        regionsOnRack2++;
      } else if (rackManager.getRack(primaryRSMap.get(region)).equals("rack3")) {
        regionsOnRack3++;
      }
    }
    // Verify that the regions got placed in the way we expect (documented in
    // FavoredNodeAssignmentHelper#placePrimaryRSAsRoundRobin)
    checkNumRegions(regionCount, firstRackSize, secondRackSize, thirdRackSize, regionsOnRack1,
        regionsOnRack2, regionsOnRack3, assignmentMap);
  }

  private void checkNumRegions(int regionCount, int firstRackSize, int secondRackSize,
      int thirdRackSize, int regionsOnRack1, int regionsOnRack2, int regionsOnRack3,
      Map<ServerName, List<RegionInfo>> assignmentMap) {
    //The regions should be distributed proportionately to the racksizes
    //Verify the ordering was as expected by inserting the racks and regions
    //in sorted maps. The keys being the racksize and numregions; values are
    //the relative positions of the racksizes and numregions respectively
    SortedMap<Integer, Integer> rackMap = new TreeMap<>();
    rackMap.put(firstRackSize, 1);
    rackMap.put(secondRackSize, 2);
    rackMap.put(thirdRackSize, 3);
    SortedMap<Integer, Integer> regionMap = new TreeMap<>();
    regionMap.put(regionsOnRack1, 1);
    regionMap.put(regionsOnRack2, 2);
    regionMap.put(regionsOnRack3, 3);
    assertTrue(printProportions(firstRackSize, secondRackSize, thirdRackSize,
        regionsOnRack1, regionsOnRack2, regionsOnRack3),
        rackMap.get(firstRackSize) == regionMap.get(regionsOnRack1));
    assertTrue(printProportions(firstRackSize, secondRackSize, thirdRackSize,
        regionsOnRack1, regionsOnRack2, regionsOnRack3),
        rackMap.get(secondRackSize) == regionMap.get(regionsOnRack2));
    assertTrue(printProportions(firstRackSize, secondRackSize, thirdRackSize,
        regionsOnRack1, regionsOnRack2, regionsOnRack3),
        rackMap.get(thirdRackSize) == regionMap.get(regionsOnRack3));
  }

  private String printProportions(int firstRackSize, int secondRackSize,
      int thirdRackSize, int regionsOnRack1, int regionsOnRack2, int regionsOnRack3) {
    return "The rack sizes " + firstRackSize + " " + secondRackSize
        + " " + thirdRackSize + " " + regionsOnRack1 + " " + regionsOnRack2 +
        " " + regionsOnRack3;
  }

  @Test
  public void testConstrainedPlacement() throws Exception {
    List<ServerName> servers = Lists.newArrayList();
    servers.add(ServerName.valueOf("foo" + 1 + ":1234", -1));
    servers.add(ServerName.valueOf("foo" + 2 + ":1234", -1));
    servers.add(ServerName.valueOf("foo" + 15 + ":1234", -1));
    FavoredNodeAssignmentHelper helper = new FavoredNodeAssignmentHelper(servers, rackManager);
    helper.initialize();
    assertTrue(helper.canPlaceFavoredNodes());

    List<RegionInfo> regions = new ArrayList<>(20);
    for (int i = 0; i < 20; i++) {
      regions.add(RegionInfoBuilder.newBuilder(TableName.valueOf(name.getMethodName()))
          .setStartKey(Bytes.toBytes(i))
          .setEndKey(Bytes.toBytes(i + 1))
          .build());
    }
    Map<ServerName, List<RegionInfo>> assignmentMap =
        new HashMap<ServerName, List<RegionInfo>>();
    Map<RegionInfo, ServerName> primaryRSMap = new HashMap<RegionInfo, ServerName>();
    helper.placePrimaryRSAsRoundRobin(assignmentMap, primaryRSMap, regions);
    assertTrue(primaryRSMap.size() == regions.size());
    Map<RegionInfo, ServerName[]> secondaryAndTertiary =
        helper.placeSecondaryAndTertiaryRS(primaryRSMap);
    assertEquals(regions.size(), secondaryAndTertiary.size());
  }

  @Test
  public void testGetOneRandomRack() throws IOException {

    Map<String,Integer> rackToServerCount = new HashMap<>();
    Set<String> rackList = Sets.newHashSet("rack1", "rack2", "rack3");
    for (String rack : rackList) {
      rackToServerCount.put(rack, 2);
    }
    List<ServerName> servers = getServersFromRack(rackToServerCount);

    FavoredNodeAssignmentHelper helper = new FavoredNodeAssignmentHelper(servers, rackManager);
    helper.initialize();
    assertTrue(helper.canPlaceFavoredNodes());

    // Check we don't get a bad rack on any number of attempts
    for (int attempts = 0 ; attempts < MAX_ATTEMPTS; attempts++) {
      assertTrue(rackList.contains(helper.getOneRandomRack(Sets.newHashSet())));
    }

    // Check skipRack multiple times when an invalid rack is specified
    Set<String> skipRacks = Sets.newHashSet("rack");
    for (int attempts = 0 ; attempts < MAX_ATTEMPTS; attempts++) {
      assertTrue(rackList.contains(helper.getOneRandomRack(skipRacks)));
    }

    // Check skipRack multiple times when an valid rack is specified
    skipRacks = Sets.newHashSet("rack1");
    Set<String> validRacks = Sets.newHashSet("rack2", "rack3");
    for (int attempts = 0 ; attempts < MAX_ATTEMPTS; attempts++) {
      assertTrue(validRacks.contains(helper.getOneRandomRack(skipRacks)));
    }
  }

  @Test
  public void testGetRandomServerSingleRack() throws IOException {

    Map<String,Integer> rackToServerCount = new HashMap<>();
    final String rack = "rack1";
    rackToServerCount.put(rack, 4);
    List<ServerName> servers = getServersFromRack(rackToServerCount);

    FavoredNodeAssignmentHelper helper = new FavoredNodeAssignmentHelper(servers, rackManager);
    helper.initialize();
    assertTrue(helper.canPlaceFavoredNodes());

    // Check we don't get a bad node on any number of attempts
    for (int attempts = 0 ; attempts < MAX_ATTEMPTS; attempts++) {
      ServerName sn = helper.getOneRandomServer(rack, Sets.newHashSet());
      assertTrue("Server:" + sn + " does not belong to list: " + servers, servers.contains(sn));
    }

    // Check skipServers multiple times when an invalid server is specified
    Set<ServerName> skipServers =
        Sets.newHashSet(ServerName.valueOf("invalidnode:1234", ServerName.NON_STARTCODE));
    for (int attempts = 0 ; attempts < MAX_ATTEMPTS; attempts++) {
      ServerName sn = helper.getOneRandomServer(rack, skipServers);
      assertTrue("Server:" + sn + " does not belong to list: " + servers, servers.contains(sn));
    }

    // Check skipRack multiple times when an valid servers are specified
    ServerName skipSN = ServerName.valueOf("foo1:1234", ServerName.NON_STARTCODE);
    skipServers = Sets.newHashSet(skipSN);
    for (int attempts = 0 ; attempts < MAX_ATTEMPTS; attempts++) {
      ServerName sn = helper.getOneRandomServer(rack, skipServers);
      assertNotEquals("Skip server should not be selected ",
          skipSN.getAddress(), sn.getAddress());
      assertTrue("Server:" + sn + " does not belong to list: " + servers, servers.contains(sn));
    }
  }

  @Test
  public void testGetRandomServerMultiRack() throws IOException {
    Map<String,Integer> rackToServerCount = new HashMap<>();
    Set<String> rackList = Sets.newHashSet("rack1", "rack2", "rack3");
    for (String rack : rackList) {
      rackToServerCount.put(rack, 4);
    }
    List<ServerName> servers = getServersFromRack(rackToServerCount);

    FavoredNodeAssignmentHelper helper = new FavoredNodeAssignmentHelper(servers, rackManager);
    helper.initialize();
    assertTrue(helper.canPlaceFavoredNodes());

    // Check we don't get a bad node on any number of attempts
    for (int attempts = 0 ; attempts < MAX_ATTEMPTS; attempts++) {
      for (String rack : rackList) {
        ServerName sn = helper.getOneRandomServer(rack, Sets.newHashSet());
        assertTrue("Server:" + sn + " does not belong to rack servers: " + rackToServers.get(rack),
            rackToServers.get(rack).contains(sn));
      }
    }

    // Check skipServers multiple times when an invalid server is specified
    Set<ServerName> skipServers =
        Sets.newHashSet(ServerName.valueOf("invalidnode:1234", ServerName.NON_STARTCODE));
    for (int attempts = 0 ; attempts < MAX_ATTEMPTS; attempts++) {
      for (String rack : rackList) {
        ServerName sn = helper.getOneRandomServer(rack, skipServers);
        assertTrue("Server:" + sn + " does not belong to rack servers: " + rackToServers.get(rack),
            rackToServers.get(rack).contains(sn));
      }
    }

    // Check skipRack multiple times when an valid servers are specified
    ServerName skipSN1 = ServerName.valueOf("foo1:1234", ServerName.NON_STARTCODE);
    ServerName skipSN2 = ServerName.valueOf("foo10:1234", ServerName.NON_STARTCODE);
    ServerName skipSN3 = ServerName.valueOf("foo20:1234", ServerName.NON_STARTCODE);
    skipServers = Sets.newHashSet(skipSN1, skipSN2, skipSN3);
    for (int attempts = 0 ; attempts < MAX_ATTEMPTS; attempts++) {
      for (String rack : rackList) {
        ServerName sn = helper.getOneRandomServer(rack, skipServers);
        assertFalse("Skip server should not be selected ", skipServers.contains(sn));
        assertTrue("Server:" + sn + " does not belong to rack servers: " + rackToServers.get(rack),
            rackToServers.get(rack).contains(sn));
      }
    }
  }

  @Test
  public void testGetFavoredNodes() throws IOException {
    Map<String,Integer> rackToServerCount = new HashMap<>();
    Set<String> rackList = Sets.newHashSet("rack1", "rack2", "rack3");
    for (String rack : rackList) {
      rackToServerCount.put(rack, 4);
    }
    List<ServerName> servers = getServersFromRack(rackToServerCount);

    FavoredNodeAssignmentHelper helper = new FavoredNodeAssignmentHelper(servers, rackManager);
    helper.initialize();
    assertTrue(helper.canPlaceFavoredNodes());

    RegionInfo region = RegionInfoBuilder.newBuilder(TableName.valueOf(name.getMethodName()))
        .setStartKey(HConstants.EMPTY_START_ROW)
        .setEndKey(HConstants.EMPTY_END_ROW)
        .build();

    for (int maxattempts = 0; maxattempts < MAX_ATTEMPTS; maxattempts++) {
      List<ServerName> fn = helper.generateFavoredNodes(region);
      checkDuplicateFN(fn);
      checkFNRacks(fn);
    }
  }

  @Test
  public void testGenMissingFavoredNodeOneRack() throws IOException {
    Map<String, Integer> rackToServerCount = new HashMap<>();
    final String rack = "rack1";
    rackToServerCount.put(rack, 6);
    List<ServerName> servers = getServersFromRack(rackToServerCount);

    FavoredNodeAssignmentHelper helper = new FavoredNodeAssignmentHelper(servers, rackManager);
    helper.initialize();
    assertTrue(helper.canPlaceFavoredNodes());


    ServerName snRack1SN1 = ServerName.valueOf("foo1:1234", ServerName.NON_STARTCODE);
    ServerName snRack1SN2 = ServerName.valueOf("foo2:1234", ServerName.NON_STARTCODE);
    ServerName snRack1SN3 = ServerName.valueOf("foo3:1234", ServerName.NON_STARTCODE);

    List<ServerName> fn = Lists.newArrayList(snRack1SN1, snRack1SN2);
    for (int attempts = 0; attempts < MAX_ATTEMPTS; attempts++) {
      checkDuplicateFN(fn, helper.generateMissingFavoredNode(fn));
    }

    fn = Lists.newArrayList(snRack1SN1, snRack1SN2);
    List<ServerName> skipServers = Lists.newArrayList(snRack1SN3);
    for (int attempts = 0; attempts < MAX_ATTEMPTS; attempts++) {
      ServerName genSN = helper.generateMissingFavoredNode(fn, skipServers);
      checkDuplicateFN(fn, genSN);
      assertNotEquals("Generated FN should not match excluded one", snRack1SN3, genSN);
    }
  }

  @Test
  public void testGenMissingFavoredNodeMultiRack() throws IOException {

    ServerName snRack1SN1 = ServerName.valueOf("foo1:1234", ServerName.NON_STARTCODE);
    ServerName snRack1SN2 = ServerName.valueOf("foo2:1234", ServerName.NON_STARTCODE);
    ServerName snRack2SN1 = ServerName.valueOf("foo10:1234", ServerName.NON_STARTCODE);
    ServerName snRack2SN2 = ServerName.valueOf("foo11:1234", ServerName.NON_STARTCODE);

    Map<String,Integer> rackToServerCount = new HashMap<>();
    Set<String> rackList = Sets.newHashSet("rack1", "rack2");
    for (String rack : rackList) {
      rackToServerCount.put(rack, 4);
    }
    List<ServerName> servers = getServersFromRack(rackToServerCount);

    FavoredNodeAssignmentHelper helper = new FavoredNodeAssignmentHelper(servers, rackManager);
    helper.initialize();
    assertTrue(helper.canPlaceFavoredNodes());

    List<ServerName> fn = Lists.newArrayList(snRack1SN1, snRack1SN2);
    for (int attempts = 0; attempts < MAX_ATTEMPTS; attempts++) {
      ServerName genSN = helper.generateMissingFavoredNode(fn);
      checkDuplicateFN(fn, genSN);
      checkFNRacks(fn, genSN);
    }

    fn = Lists.newArrayList(snRack1SN1, snRack2SN1);
    for (int attempts = 0; attempts < MAX_ATTEMPTS; attempts++) {
      ServerName genSN = helper.generateMissingFavoredNode(fn);
      checkDuplicateFN(fn, genSN);
      checkFNRacks(fn, genSN);
    }

    fn = Lists.newArrayList(snRack1SN1, snRack2SN1);
    List<ServerName> skipServers = Lists.newArrayList(snRack2SN2);
    for (int attempts = 0; attempts < MAX_ATTEMPTS; attempts++) {
      ServerName genSN = helper.generateMissingFavoredNode(fn, skipServers);
      checkDuplicateFN(fn, genSN);
      checkFNRacks(fn, genSN);
      assertNotEquals("Generated FN should not match excluded one", snRack2SN2, genSN);
    }
  }

  private void checkDuplicateFN(List<ServerName> fnList, ServerName genFN) {
    Set<ServerName> favoredNodes = Sets.newHashSet(fnList);
    assertNotNull("Generated FN can't be null", genFN);
    favoredNodes.add(genFN);
    assertEquals("Did not find expected number of favored nodes",
        FavoredNodeAssignmentHelper.FAVORED_NODES_NUM, favoredNodes.size());
  }

  private void checkDuplicateFN(List<ServerName> fnList) {
    Set<ServerName> favoredNodes = Sets.newHashSet(fnList);
    assertEquals("Did not find expected number of favored nodes",
        FavoredNodeAssignmentHelper.FAVORED_NODES_NUM, favoredNodes.size());
  }

  private void checkFNRacks(List<ServerName> fnList, ServerName genFN) {
    Set<ServerName> favoredNodes = Sets.newHashSet(fnList);
    favoredNodes.add(genFN);
    Set<String> racks = Sets.newHashSet();
    for (ServerName sn : favoredNodes) {
      racks.add(rackManager.getRack(sn));
    }
    assertTrue("FN should be spread atleast across 2 racks", racks.size() >= 2);
  }

  private void checkFNRacks(List<ServerName> fnList) {
    Set<ServerName> favoredNodes = Sets.newHashSet(fnList);
    Set<String> racks = Sets.newHashSet();
    for (ServerName sn : favoredNodes) {
      racks.add(rackManager.getRack(sn));
    }
    assertTrue("FN should be spread atleast across 2 racks", racks.size() >= 2);
  }
}
