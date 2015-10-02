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

package org.apache.hadoop.hbase.master.balancer;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.RackManager;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Triple;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({MasterTests.class, SmallTests.class})
public class TestFavoredNodeAssignmentHelper {

  private static List<ServerName> servers = new ArrayList<ServerName>();
  private static Map<String, List<ServerName>> rackToServers = new HashMap<String,
      List<ServerName>>();
  private static RackManager rackManager = Mockito.mock(RackManager.class);

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // Set up some server -> rack mappings
    // Have three racks in the cluster with 10 hosts each.
    for (int i = 0; i < 40; i++) {
      ServerName server = ServerName.valueOf("foo" + i + ":1234", -1);
      if (i < 10) {
        Mockito.when(rackManager.getRack(server)).thenReturn("rack1");
        if (rackToServers.get("rack1") == null) {
          List<ServerName> servers = new ArrayList<ServerName>();
          rackToServers.put("rack1", servers);
        }
        rackToServers.get("rack1").add(server);
      }
      if (i >= 10 && i < 20) {
        Mockito.when(rackManager.getRack(server)).thenReturn("rack2");
        if (rackToServers.get("rack2") == null) {
          List<ServerName> servers = new ArrayList<ServerName>();
          rackToServers.put("rack2", servers);
        }
        rackToServers.get("rack2").add(server);
      }
      if (i >= 20 && i < 30) {
        Mockito.when(rackManager.getRack(server)).thenReturn("rack3");
        if (rackToServers.get("rack3") == null) {
          List<ServerName> servers = new ArrayList<ServerName>();
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
    List<ServerName> chosenServers = new ArrayList<ServerName>();
    for (Map.Entry<String, Integer> entry : rackToServerCount.entrySet()) {
      List<ServerName> servers = rackToServers.get(entry.getKey());
      for (int i = 0; i < entry.getValue(); i++) {
        chosenServers.add(servers.get(i));
      }
    }
    return chosenServers;
  }

  @Ignore("Disabled for now until FavoredNodes gets finished as a feature") @Test
  public void testSmallCluster() {
    // Test the case where we cannot assign favored nodes (because the number
    // of nodes in the cluster is too less)
    Map<String,Integer> rackToServerCount = new HashMap<String,Integer>();
    rackToServerCount.put("rack1", 2);
    List<ServerName> servers = getServersFromRack(rackToServerCount);
    FavoredNodeAssignmentHelper helper = new FavoredNodeAssignmentHelper(servers,
        new Configuration());
    assertFalse(helper.canPlaceFavoredNodes());
  }

  @Ignore("Disabled for now until FavoredNodes gets finished as a feature") @Test
  public void testPlacePrimaryRSAsRoundRobin() {
    // Test the regular case where there are many servers in different racks
    // Test once for few regions and once for many regions
    primaryRSPlacement(6, null, 10, 10, 10);
    // now create lots of regions and try to place them on the limited number of machines
    primaryRSPlacement(600, null, 10, 10, 10);
  }
  
  @Ignore("Disabled for now until FavoredNodes gets finished as a feature") @Test
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

  @Ignore("Disabled for now until FavoredNodes gets finished as a feature") @Test
  public void testSecondaryAndTertiaryPlacementWithSingleRack() {
    // Test the case where there is a single rack and we need to choose
    // Primary/Secondary/Tertiary from a single rack.
    Map<String,Integer> rackToServerCount = new HashMap<String,Integer>();
    rackToServerCount.put("rack1", 10);
    // have lots of regions to test with
    Triple<Map<HRegionInfo, ServerName>, FavoredNodeAssignmentHelper, List<HRegionInfo>>
      primaryRSMapAndHelper = secondaryAndTertiaryRSPlacementHelper(60000, rackToServerCount);
    FavoredNodeAssignmentHelper helper = primaryRSMapAndHelper.getSecond();
    Map<HRegionInfo, ServerName> primaryRSMap = primaryRSMapAndHelper.getFirst();
    List<HRegionInfo> regions = primaryRSMapAndHelper.getThird();
    Map<HRegionInfo, ServerName[]> secondaryAndTertiaryMap =
        helper.placeSecondaryAndTertiaryRS(primaryRSMap);
    // although we created lots of regions we should have no overlap on the
    // primary/secondary/tertiary for any given region
    for (HRegionInfo region : regions) {
      ServerName[] secondaryAndTertiaryServers = secondaryAndTertiaryMap.get(region);
      assertTrue(!secondaryAndTertiaryServers[0].equals(primaryRSMap.get(region)));
      assertTrue(!secondaryAndTertiaryServers[1].equals(primaryRSMap.get(region)));
      assertTrue(!secondaryAndTertiaryServers[0].equals(secondaryAndTertiaryServers[1]));
    }
  }

  @Ignore("Disabled for now until FavoredNodes gets finished as a feature") @Test
  public void testSecondaryAndTertiaryPlacementWithSingleServer() {
    // Test the case where we have a single node in the cluster. In this case
    // the primary can be assigned but the secondary/tertiary would be null
    Map<String,Integer> rackToServerCount = new HashMap<String,Integer>();
    rackToServerCount.put("rack1", 1);
    Triple<Map<HRegionInfo, ServerName>, FavoredNodeAssignmentHelper, List<HRegionInfo>>
      primaryRSMapAndHelper = secondaryAndTertiaryRSPlacementHelper(1, rackToServerCount);
    FavoredNodeAssignmentHelper helper = primaryRSMapAndHelper.getSecond();
    Map<HRegionInfo, ServerName> primaryRSMap = primaryRSMapAndHelper.getFirst();
    List<HRegionInfo> regions = primaryRSMapAndHelper.getThird();

    Map<HRegionInfo, ServerName[]> secondaryAndTertiaryMap =
        helper.placeSecondaryAndTertiaryRS(primaryRSMap);
    // no secondary/tertiary placement in case of a single RegionServer
    assertTrue(secondaryAndTertiaryMap.get(regions.get(0)) == null);
  }

  @Ignore("Disabled for now until FavoredNodes gets finished as a feature") @Test
  public void testSecondaryAndTertiaryPlacementWithMultipleRacks() {
    // Test the case where we have multiple racks and the region servers
    // belong to multiple racks
    Map<String,Integer> rackToServerCount = new HashMap<String,Integer>();
    rackToServerCount.put("rack1", 10);
    rackToServerCount.put("rack2", 10);

    Triple<Map<HRegionInfo, ServerName>, FavoredNodeAssignmentHelper, List<HRegionInfo>>
      primaryRSMapAndHelper = secondaryAndTertiaryRSPlacementHelper(60000, rackToServerCount);
    FavoredNodeAssignmentHelper helper = primaryRSMapAndHelper.getSecond();
    Map<HRegionInfo, ServerName> primaryRSMap = primaryRSMapAndHelper.getFirst();

    assertTrue(primaryRSMap.size() == 60000);
    Map<HRegionInfo, ServerName[]> secondaryAndTertiaryMap =
        helper.placeSecondaryAndTertiaryRS(primaryRSMap);
    assertTrue(secondaryAndTertiaryMap.size() == 60000);
    // for every region, the primary should be on one rack and the secondary/tertiary
    // on another (we create a lot of regions just to increase probability of failure)
    for (Map.Entry<HRegionInfo, ServerName[]> entry : secondaryAndTertiaryMap.entrySet()) {
      ServerName[] allServersForRegion = entry.getValue();
      String primaryRSRack = rackManager.getRack(primaryRSMap.get(entry.getKey()));
      String secondaryRSRack = rackManager.getRack(allServersForRegion[0]);
      String tertiaryRSRack = rackManager.getRack(allServersForRegion[1]);
      assertTrue(!primaryRSRack.equals(secondaryRSRack));
      assertTrue(secondaryRSRack.equals(tertiaryRSRack));
    }
  }

  @Ignore("Disabled for now until FavoredNodes gets finished as a feature") @Test
  public void testSecondaryAndTertiaryPlacementWithLessThanTwoServersInRacks() {
    // Test the case where we have two racks but with less than two servers in each
    // We will not have enough machines to select secondary/tertiary
    Map<String,Integer> rackToServerCount = new HashMap<String,Integer>();
    rackToServerCount.put("rack1", 1);
    rackToServerCount.put("rack2", 1);
    Triple<Map<HRegionInfo, ServerName>, FavoredNodeAssignmentHelper, List<HRegionInfo>>
      primaryRSMapAndHelper = secondaryAndTertiaryRSPlacementHelper(6, rackToServerCount);
    FavoredNodeAssignmentHelper helper = primaryRSMapAndHelper.getSecond();
    Map<HRegionInfo, ServerName> primaryRSMap = primaryRSMapAndHelper.getFirst();
    List<HRegionInfo> regions = primaryRSMapAndHelper.getThird();
    assertTrue(primaryRSMap.size() == 6);
    Map<HRegionInfo, ServerName[]> secondaryAndTertiaryMap =
          helper.placeSecondaryAndTertiaryRS(primaryRSMap);
    for (HRegionInfo region : regions) {
      // not enough secondary/tertiary room to place the regions
      assertTrue(secondaryAndTertiaryMap.get(region) == null);
    }
  }

  @Ignore("Disabled for now until FavoredNodes gets finished as a feature") @Test
  public void testSecondaryAndTertiaryPlacementWithMoreThanOneServerInPrimaryRack() {
    // Test the case where there is only one server in one rack and another rack
    // has more servers. We try to choose secondary/tertiary on different
    // racks than what the primary is on. But if the other rack doesn't have
    // enough nodes to have both secondary/tertiary RSs, the tertiary is placed
    // on the same rack as the primary server is on
    Map<String,Integer> rackToServerCount = new HashMap<String,Integer>();
    rackToServerCount.put("rack1", 2);
    rackToServerCount.put("rack2", 1);
    Triple<Map<HRegionInfo, ServerName>, FavoredNodeAssignmentHelper, List<HRegionInfo>>
      primaryRSMapAndHelper = secondaryAndTertiaryRSPlacementHelper(6, rackToServerCount);
    FavoredNodeAssignmentHelper helper = primaryRSMapAndHelper.getSecond();
    Map<HRegionInfo, ServerName> primaryRSMap = primaryRSMapAndHelper.getFirst();
    List<HRegionInfo> regions = primaryRSMapAndHelper.getThird();
    assertTrue(primaryRSMap.size() == 6);
    Map<HRegionInfo, ServerName[]> secondaryAndTertiaryMap =
          helper.placeSecondaryAndTertiaryRS(primaryRSMap);
    for (HRegionInfo region : regions) {
      ServerName s = primaryRSMap.get(region);
      ServerName secondaryRS = secondaryAndTertiaryMap.get(region)[0];
      ServerName tertiaryRS = secondaryAndTertiaryMap.get(region)[1];
      if (rackManager.getRack(s).equals("rack1")) {
        assertTrue(rackManager.getRack(secondaryRS).equals("rack2") &&
            rackManager.getRack(tertiaryRS).equals("rack1"));
      }
      if (rackManager.getRack(s).equals("rack2")) {
        assertTrue(rackManager.getRack(secondaryRS).equals("rack1") &&
            rackManager.getRack(tertiaryRS).equals("rack1"));
      }
    }
  }

  private Triple<Map<HRegionInfo, ServerName>, FavoredNodeAssignmentHelper, List<HRegionInfo>>
  secondaryAndTertiaryRSPlacementHelper(
      int regionCount, Map<String, Integer> rackToServerCount) {
    Map<HRegionInfo, ServerName> primaryRSMap = new HashMap<HRegionInfo, ServerName>();
    List<ServerName> servers = getServersFromRack(rackToServerCount);
    FavoredNodeAssignmentHelper helper = new FavoredNodeAssignmentHelper(servers, rackManager);
    Map<ServerName, List<HRegionInfo>> assignmentMap =
        new HashMap<ServerName, List<HRegionInfo>>();
    helper.initialize();
    // create regions
    List<HRegionInfo> regions = new ArrayList<HRegionInfo>(regionCount);
    for (int i = 0; i < regionCount; i++) {
      HRegionInfo region = new HRegionInfo(TableName.valueOf("foobar"),
          Bytes.toBytes(i), Bytes.toBytes(i + 1));
      regions.add(region);
    }
    // place the regions
    helper.placePrimaryRSAsRoundRobin(assignmentMap, primaryRSMap, regions);
    return new Triple<Map<HRegionInfo, ServerName>, FavoredNodeAssignmentHelper, List<HRegionInfo>>
                   (primaryRSMap, helper, regions);
  }

  private void primaryRSPlacement(int regionCount, Map<HRegionInfo, ServerName> primaryRSMap,
      int firstRackSize, int secondRackSize, int thirdRackSize) {
    Map<String,Integer> rackToServerCount = new HashMap<String,Integer>();
    rackToServerCount.put("rack1", firstRackSize);
    rackToServerCount.put("rack2", secondRackSize);
    rackToServerCount.put("rack3", thirdRackSize);
    List<ServerName> servers = getServersFromRack(rackToServerCount);
    FavoredNodeAssignmentHelper helper = new FavoredNodeAssignmentHelper(servers,
        rackManager);
    helper.initialize();

    assertTrue(helper.canPlaceFavoredNodes());

    Map<ServerName, List<HRegionInfo>> assignmentMap =
        new HashMap<ServerName, List<HRegionInfo>>();
    if (primaryRSMap == null) primaryRSMap = new HashMap<HRegionInfo, ServerName>();
    // create some regions
    List<HRegionInfo> regions = new ArrayList<HRegionInfo>(regionCount);
    for (int i = 0; i < regionCount; i++) {
      HRegionInfo region = new HRegionInfo(TableName.valueOf("foobar"),
          Bytes.toBytes(i), Bytes.toBytes(i + 1));
      regions.add(region);
    }
    // place those regions in primary RSs
    helper.placePrimaryRSAsRoundRobin(assignmentMap, primaryRSMap, regions);

    // we should have all the regions nicely spread across the racks
    int regionsOnRack1 = 0;
    int regionsOnRack2 = 0;
    int regionsOnRack3 = 0;
    for (HRegionInfo region : regions) {
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
      Map<ServerName, List<HRegionInfo>> assignmentMap) {
    //The regions should be distributed proportionately to the racksizes
    //Verify the ordering was as expected by inserting the racks and regions
    //in sorted maps. The keys being the racksize and numregions; values are
    //the relative positions of the racksizes and numregions respectively
    SortedMap<Integer, Integer> rackMap = new TreeMap<Integer, Integer>();
    rackMap.put(firstRackSize, 1);
    rackMap.put(secondRackSize, 2);
    rackMap.put(thirdRackSize, 3);
    SortedMap<Integer, Integer> regionMap = new TreeMap<Integer, Integer>();
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
}
