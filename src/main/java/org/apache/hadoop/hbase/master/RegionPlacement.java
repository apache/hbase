/**
 * Copyright 2013 The Apache Software Foundation
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
package org.apache.hadoop.hbase.master;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.master.AssignmentPlan.POSITION;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.MunkresAssignment;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;

public class RegionPlacement implements RegionPlacementPolicy{
  private static final Log LOG = LogFactory.getLog(RegionPlacement.class
      .getName());

  // The cost of a placement that should never be assigned.
  private static final float MAX_COST = Float.POSITIVE_INFINITY;

  // The cost of a placement that is undesirable but acceptable.
  private static final float AVOID_COST = 100000f;

  // The amount by which the cost of a placement is increased if it is the
  // last slot of the server. This is done to more evenly distribute the slop
  // amongst servers.
  private static final float LAST_SLOT_COST_PENALTY = 0.5f;

  // The amount by which the cost of a primary placement is penalized if it is
  // not the host currently serving the region. This is done to minimize moves.
  private static final float NOT_CURRENT_HOST_PENALTY = 0.1f;

  private static boolean USE_MUNKRES_FOR_PLACING_SECONDARY_AND_TERTIARY = false;

  private Configuration conf;
  private final boolean enforceLocality;
  private final boolean enforceMinAssignmentMove;
  private HBaseAdmin admin;
  private Set<String> targetTableSet;

  public RegionPlacement(Configuration conf)
  throws IOException {
    this(conf, true, true);
  }

  public RegionPlacement(Configuration conf, boolean enforceLocality,
      boolean enforceMinAssignmentMove)
  throws IOException {
    this.conf = conf;
    this.enforceLocality = enforceLocality;
    this.enforceMinAssignmentMove = enforceMinAssignmentMove;
    this.targetTableSet = new HashSet<String>();
  }

  @Override
  public AssignmentPlan getNewAssignmentPlan(HRegionInfo[] regions,
      AssignmentDomain domain) throws IOException {
    if (regions == null || regions.length == 0 ||
        domain == null || domain.isEmpty() || !domain.canPlaceFavoredNodes())
      return null;

    try {
      // Place the primary region server based on the regions and servers
      Map<HRegionInfo, HServerAddress> primaryRSMap = this.placePrimaryRSAsRoundRobinBalanced(regions, domain);

      // Place the secondary and tertiary region server
      Map<HRegionInfo, Pair<HServerAddress, HServerAddress>>
        secondaryAndTertiaryRSMap =
        this.placeSecondaryAndTertiaryWithRestrictions(primaryRSMap, domain);

      // Get the assignment plan by initialization with the primaryRSMap and the
      // secondaryAndTertiaryRSMap
      AssignmentPlan plan = new AssignmentPlan();
      plan.initialize(primaryRSMap, secondaryAndTertiaryRSMap);
      return plan;
    } catch (Exception e) {
      LOG.debug("Cannot generate the assignment plan because " + e);
      return null;
    }
  }

  /**
   * Place the primary regions in the round robin way. Algorithm works as
   * following: we merge all regionservers in single map, and sort the map in
   * ascending order by number of regions per regionserver, then start the
   * assignment in round robin fashion.
   *
   * We usually have same number of regionservers per rack, so having #regions
   * per regionserver balanced will imply balance per rack too. If that is not
   * the case, we can expect imbalances per rack.
   *
   * @param regions
   * @param domain
   * @return the map between regions and its primary region server
   * @throws IOException
   */
  private Map<HRegionInfo, HServerAddress> placePrimaryRSAsRoundRobinBalanced(
      HRegionInfo[] regions, AssignmentDomain domain) throws IOException {
    RegionAssignmentSnapshot currentSnapshot = this
        .getRegionAssignmentSnapshot();
    Map<HServerAddress, List<HRegionInfo>> assignmentSnapshotMap = currentSnapshot
        .getRegionServerToRegionMap();
    Set<HServerAddress> allServers = domain.getAllServers();
    // not all servers will be in the assignmentSnapshotMap - so putting the ones
    // which does not have assignment
    for (HServerAddress server : allServers) {
      if (assignmentSnapshotMap.get(server) == null) {
        assignmentSnapshotMap.put(server, new ArrayList<HRegionInfo>());
      }
    }
    // sort the regionserver by #regions assigned in ascending order
    ValueComparator vComp = new ValueComparator(assignmentSnapshotMap);
    Map<HServerAddress, List<HRegionInfo>> sortedMap = new TreeMap<HServerAddress, List<HRegionInfo>>(
        vComp);
    sortedMap.putAll(assignmentSnapshotMap);
    List<HServerAddress> servers = new ArrayList<HServerAddress>(sortedMap.keySet());

    Map<HRegionInfo, HServerAddress> primaryRSMap = new HashMap<HRegionInfo, HServerAddress>();
    int index = 0;
    for (HRegionInfo info : regions) {
      primaryRSMap.put(info, servers.get(index++));
      if (index == servers.size()) {
        index = 0;
      }
    }
    return primaryRSMap;
  }

  /**
   * Comparator will sort HServerAddress based on number of regions assigned to
   * it in ascending order
   *
   */
  private class ValueComparator implements Comparator<HServerAddress> {
    Map<HServerAddress, List<HRegionInfo>> map;

    public ValueComparator(Map<HServerAddress, List<HRegionInfo>> map) {
      this.map = map;
    }

    @Override
    public int compare(HServerAddress server1, HServerAddress server2) {
      if (map.get(server1) == null) {
        return -1;
      }
      if (map.get(server1) != null && map.get(server2) == null) {
        return 1;
      } else {
        if (map.get(server1).size() >= map.get(server2).size()) {
          return 1;
        } else {
          return -1;
        }
      }
    }
  }

  /**
   * Place the primary region server in the round robin way. Algorithm works as
   * following: per rack, per regionserserver -> place region which means the
   * assignment will be balanced per rack *ONLY* and it is possible to be not
   * balanced per regionserver
   *
   * @param regions
   * @param domain
   * @return the map between regions and its primary region server
   * @throws IOException
   */
  @SuppressWarnings("unused")
  private Map<HRegionInfo, HServerAddress> placePrimaryRSAsRoundRobin(
      HRegionInfo[] regions, AssignmentDomain domain) throws IOException {
    // Get the rack to region server map from the assignment domain
    Map<String, List<HServerAddress>> rackToRegionServerMap = domain
        .getRackToRegionServerMap();
    //sort the map by existing #regions/rs

    List<String> rackList = new ArrayList<String>();
    rackList.addAll(rackToRegionServerMap.keySet());
    Map<String, Integer> currentProcessIndexMap = new HashMap<String, Integer>();
    int rackIndex = 0;

    // Place the region with its primary region sever in a round robin way.
    Map<HRegionInfo, HServerAddress> primaryRSMap =
      new HashMap<HRegionInfo, HServerAddress>();
    for (HRegionInfo regionInfo : regions) {
      String rackName = rackList.get(rackIndex);
      // Initialize the current processing host index.
      int serverIndex = 0;

      // Restore the current process index from the currentProcessIndexMap
      Integer currentProcessIndex = currentProcessIndexMap.get(rackName);
      if (currentProcessIndex != null) {
        serverIndex = currentProcessIndex.intValue();
      }
      // Get the server list for the current rack
      List<HServerAddress> currentServerList = rackToRegionServerMap.get(rackName);

      // Get the current process region server
      HServerAddress currentServer = currentServerList.get(serverIndex);

      // Place the current region with the current primary region server
      primaryRSMap.put(regionInfo, currentServer);

      // Set the next processing index
      if ((++serverIndex) >= currentServerList.size()) {
        // Reset the server index for the current rack
        serverIndex = 0;
      }
      // Keep track of the next processing index
      currentProcessIndexMap.put(rackName, serverIndex);
      if ((++rackIndex) >= rackList.size()) {
        rackIndex = 0; // reset the rack index to 0
      }
    }

    return primaryRSMap;
  }

  /**
   * Place the secondary and tertiary region server. Best effort to place the
   * secondary and tertiary into the different rack as the primary region server.
   * Also best effort to place the secondary and tertiary into the same rack.
   *
   * There are more than 3 region server for the placement.
   * @param primaryRSMap
   * @param domain
   * @return
   * @throws IOException
   */
  @SuppressWarnings("unused")
  private Map<HRegionInfo, Pair<HServerAddress,HServerAddress>> placeSecondaryAndTertiaryRS(
      Map<HRegionInfo, HServerAddress> primaryRSMap, AssignmentDomain domain)
      throws IOException {
    Map<HRegionInfo, Pair<HServerAddress,HServerAddress>> secondaryAndTertiaryMap
      = new HashMap<HRegionInfo, Pair<HServerAddress, HServerAddress>>();

    for (Map.Entry<HRegionInfo, HServerAddress> entry : primaryRSMap.entrySet()) {
      // Get the target region and its primary region server rack
      HRegionInfo regionInfo = entry.getKey();
      HServerAddress primaryRS = entry.getValue();

      // Set the random seed in the assignment domain
      domain.setRandomSeed(regionInfo.hashCode());
      try {
        // Create the secondary and tertiary region server pair object.
        Pair<HServerAddress, HServerAddress> pair;
        // Get the rack for the primary region server
        String primaryRack = domain.getRack(primaryRS);

        if (domain.getTotalRackNum() == 1) {
          // Single rack case: have to pick the secondary and tertiary
          // from the same rack
          List<HServerAddress> serverList = domain.getServersFromRack(primaryRack);
          if (serverList.size() <= 2) {
            // Single region server case: cannot not place the favored nodes
            // on any server; !domain.canPlaceFavoredNodes()
            break;
          } else {
            // Randomly select two region servers from the server list and make sure
            // they are not overlap with the primary region server;
           Set<HServerAddress> serverSkipSet = new HashSet<HServerAddress>();
           serverSkipSet.add(primaryRS);

           // Place the secondary RS
           HServerAddress secondaryRS =
             domain.getOneRandomServer(primaryRack, serverSkipSet);
           // Skip the secondary for the tertiary placement
           serverSkipSet.add(secondaryRS);

           // Place the tertiary RS
           HServerAddress tertiaryRS =
             domain.getOneRandomServer(primaryRack, serverSkipSet);

           if (secondaryRS == null || tertiaryRS == null) {
             LOG.error("Cannot place the secondary and terinary" +
                 "region server for region " +
                 regionInfo.getRegionNameAsString());
           }
           // Create the secondary and tertiary pair
           pair = new Pair<HServerAddress, HServerAddress>();
           pair.setFirst(secondaryRS);
           pair.setSecond(tertiaryRS);
          }
        } else {
          // Random to choose the secondary and tertiary region server
          // from another rack to place the secondary and tertiary

          // Random to choose one rack except for the current rack
          Set<String> rackSkipSet = new HashSet<String>();
          rackSkipSet.add(primaryRack);
          String secondaryRack = domain.getOneRandomRack(rackSkipSet);
          List<HServerAddress> serverList = domain.getServersFromRack(secondaryRack);
          if (serverList.size() >= 2) {
            // Randomly pick up two servers from this secondary rack

            // Place the secondary RS
            HServerAddress secondaryRS =
              domain.getOneRandomServer(secondaryRack);

            // Skip the secondary for the tertiary placement
            Set<HServerAddress> skipServerSet = new HashSet<HServerAddress>();
            skipServerSet.add(secondaryRS);
            // Place the tertiary RS
            HServerAddress tertiaryRS =
              domain.getOneRandomServer(secondaryRack, skipServerSet);

            if (secondaryRS == null || tertiaryRS == null) {
              LOG.error("Cannot place the secondary and terinary" +
                  "region server for region " +
                  regionInfo.getRegionNameAsString());
            }
            // Create the secondary and tertiary pair
            pair = new Pair<HServerAddress, HServerAddress>();
            pair.setFirst(secondaryRS);
            pair.setSecond(tertiaryRS);
          } else {
            // Pick the secondary rs from this secondary rack
            // and pick the tertiary from another random rack
            pair = new Pair<HServerAddress, HServerAddress>();
            HServerAddress secondary = domain.getOneRandomServer(secondaryRack);
            pair.setFirst(secondary);

            // Pick the tertiary
            if (domain.getTotalRackNum() == 2) {
              // Pick the tertiary from the same rack of the primary RS
              Set<HServerAddress> serverSkipSet = new HashSet<HServerAddress>();
              serverSkipSet.add(primaryRS);
              HServerAddress tertiary =
                domain.getOneRandomServer(primaryRack, serverSkipSet);
              pair.setSecond(tertiary);
            } else {
              // Pick the tertiary from another rack
              rackSkipSet.add(secondaryRack);
              String tertiaryRandomRack = domain.getOneRandomRack(rackSkipSet);
              HServerAddress tertinary =
                domain.getOneRandomServer(tertiaryRandomRack);
              pair.setSecond(tertinary);
            }
          }
        }
        if (pair != null) {
          secondaryAndTertiaryMap.put(regionInfo, pair);
          LOG.debug("Place the secondary and tertiary region server for region "
              + regionInfo.getRegionNameAsString());
        }
      } catch (Exception e) {
        LOG.warn("Cannot place the favored nodes for region " +
            regionInfo.getRegionNameAsString() + " because " + e);
        continue;
      }
    }
    return secondaryAndTertiaryMap;
  }

  // For regions that share the primary, avoid placing the secondary and tertiary on a same RS
  public Map<HRegionInfo, Pair<HServerAddress, HServerAddress>> placeSecondaryAndTertiaryWithRestrictions(
      Map<HRegionInfo, HServerAddress> primaryRSMap, AssignmentDomain domain)
      throws IOException {
    Map<HServerAddress, String> mapServerToRack = domain
        .getRegionServerToRackMap();
    Map<HServerAddress, Set<HRegionInfo>> serverToPrimaries =
        mapRSToPrimaries(primaryRSMap);
    Map<HRegionInfo, Pair<HServerAddress, HServerAddress>> secondaryAndTertiaryMap =
        new HashMap<HRegionInfo, Pair<HServerAddress, HServerAddress>>();

    for (Entry<HRegionInfo, HServerAddress> entry : primaryRSMap.entrySet()) {
      // Get the target region and its primary region server rack
      HRegionInfo regionInfo = entry.getKey();
      HServerAddress primaryRS = entry.getValue();

      // Set the random seed in the assignment domain
      domain.setRandomSeed(regionInfo.hashCode());
      try {
        // Create the secondary and tertiary region server pair object.
        Pair<HServerAddress, HServerAddress> pair;
        // Get the rack for the primary region server
        String primaryRack = domain.getRack(primaryRS);

        if (domain.getTotalRackNum() == 1) {
          // Single rack case: have to pick the secondary and tertiary
          // from the same rack
          List<HServerAddress> serverList = domain
              .getServersFromRack(primaryRack);
          if (serverList.size() <= 2) {
            // Single region server case: cannot not place the favored nodes
            // on any server; !domain.canPlaceFavoredNodes()
            continue;
          } else {
            // Randomly select two region servers from the server list and make
            // sure
            // they are not overlap with the primary region server;
            Set<HServerAddress> serverSkipSet = new HashSet<HServerAddress>();
            serverSkipSet.add(primaryRS);

            // Place the secondary RS
            HServerAddress secondaryRS = domain.getOneRandomServer(primaryRack,
                serverSkipSet);
            // Skip the secondary for the tertiary placement
            serverSkipSet.add(secondaryRS);

            // Place the tertiary RS
            HServerAddress tertiaryRS = domain.getOneRandomServer(primaryRack,
                serverSkipSet);

            if (secondaryRS == null || tertiaryRS == null) {
              LOG.error("Cannot place the secondary and terinary"
                  + "region server for region "
                  + regionInfo.getRegionNameAsString());
            }
            // Create the secondary and tertiary pair
            pair = new Pair<HServerAddress, HServerAddress>();
            pair.setFirst(secondaryRS);
            pair.setSecond(tertiaryRS);
          }
        } else {
          // Random to choose the secondary and tertiary region server
          // from another rack to place the secondary and tertiary
          // Random to choose one rack except for the current rack
          Set<String> rackSkipSet = new HashSet<String>();
          rackSkipSet.add(primaryRack);
          String secondaryRack = domain.getOneRandomRack(rackSkipSet);
          List<HServerAddress> serverList = domain
              .getServersFromRack(secondaryRack);
          Set<HServerAddress> serverSet = new HashSet<HServerAddress>();
          serverSet.addAll(serverList);

          if (serverList.size() >= 2) {

            // Randomly pick up two servers from this secondary rack
            // Skip the secondary for the tertiary placement
            // skip the servers which share the primary already
            Set<HRegionInfo> primaries = serverToPrimaries.get(primaryRS);
            Set<HServerAddress> skipServerSet = new HashSet<HServerAddress>();
            while (true) {
              Pair<HServerAddress, HServerAddress> secondaryAndTertiary = null;
              if (primaries.size() > 1) {
                // check where his tertiary and secondary are
                for (HRegionInfo primary : primaries) {
                  secondaryAndTertiary = secondaryAndTertiaryMap.get(primary);
                  if (secondaryAndTertiary != null) {
                    if (mapServerToRack.get(secondaryAndTertiary.getFirst())
                        .equals(secondaryRack)) {
                      skipServerSet.add(secondaryAndTertiary.getFirst());
                    }
                    if (mapServerToRack.get(secondaryAndTertiary.getSecond())
                        .equals(secondaryRack)) {
                      skipServerSet.add(secondaryAndTertiary.getSecond());
                    }
                  }
                }
              }
              if (skipServerSet.size() + 2 <= serverSet.size())
                break;
              skipServerSet.clear();
              rackSkipSet.add(secondaryRack);
              // we used all racks
              if (rackSkipSet.size() == domain.getTotalRackNum()) {
                // remove the last two added and break
                skipServerSet.remove(secondaryAndTertiary.getFirst());
                skipServerSet.remove(secondaryAndTertiary.getSecond());
                break;
              }
              secondaryRack = domain.getOneRandomRack(rackSkipSet);
              serverList = domain.getServersFromRack(secondaryRack);
              serverSet = new HashSet<HServerAddress>();
              serverSet.addAll(serverList);
            }

            // Place the secondary RS
            HServerAddress secondaryRS = domain.getOneRandomServer(
                secondaryRack, skipServerSet);
            skipServerSet.add(secondaryRS);
            // Place the tertiary RS
            HServerAddress tertiaryRS = domain.getOneRandomServer(
                secondaryRack, skipServerSet);

            if (secondaryRS == null || tertiaryRS == null) {
              LOG.error("Cannot place the secondary and tertiary"
                  + " region server for region "
                  + regionInfo.getRegionNameAsString());
            }
            // Create the secondary and tertiary pair
            pair = new Pair<HServerAddress, HServerAddress>();
            pair.setFirst(secondaryRS);
            pair.setSecond(tertiaryRS);
          } else {
            // Pick the secondary rs from this secondary rack
            // and pick the tertiary from another random rack
            pair = new Pair<HServerAddress, HServerAddress>();
            HServerAddress secondary = domain.getOneRandomServer(secondaryRack);
            pair.setFirst(secondary);

            // Pick the tertiary
            if (domain.getTotalRackNum() == 2) {
              // Pick the tertiary from the same rack of the primary RS
              Set<HServerAddress> serverSkipSet = new HashSet<HServerAddress>();
              serverSkipSet.add(primaryRS);
              HServerAddress tertiary = domain.getOneRandomServer(primaryRack,
                  serverSkipSet);
              pair.setSecond(tertiary);
            } else {
              // Pick the tertiary from another rack
              rackSkipSet.add(secondaryRack);
              String tertiaryRandomRack = domain.getOneRandomRack(rackSkipSet);
              HServerAddress tertinary = domain
                  .getOneRandomServer(tertiaryRandomRack);
              pair.setSecond(tertinary);
            }
          }
        }
        if (pair != null) {
          secondaryAndTertiaryMap.put(regionInfo, pair);
          LOG.debug("Place the secondary and tertiary region server for region "
              + regionInfo.getRegionNameAsString());
        }
      } catch (Exception e) {
        LOG.warn("Cannot place the favored nodes for region "
            + regionInfo.getRegionNameAsString() + " because " + e);
        continue;
      }
    }
    return secondaryAndTertiaryMap;
  }

  public Map<HServerAddress, Set<HRegionInfo>> mapRSToPrimaries(
      Map<HRegionInfo, HServerAddress> primaryRSMap) {
    Map<HServerAddress, Set<HRegionInfo>> primaryServerMap =
        new HashMap<HServerAddress, Set<HRegionInfo>>();
    for (Entry<HRegionInfo, HServerAddress> e : primaryRSMap.entrySet()) {
      Set<HRegionInfo> currentSet = primaryServerMap.get(e.getValue());
      if (currentSet == null) {
        currentSet = new HashSet<HRegionInfo>();
      }
      currentSet.add(e.getKey());
      primaryServerMap.put(e.getValue(), currentSet);
    }
    return primaryServerMap;
  }

  /**
   * Makes expansion on the new rack by migrating a number of regions from each
   * of the existing racks to a new rack. Assumption: the assignment domain will
   * contain information that the newly added rack is there (but no regions are
   * assigned to it).
   *
   */
  public void expandRegionsToNewRack(String newRack,
      RegionAssignmentSnapshot assignmentSnapshot) throws IOException {
    AssignmentPlan plan = assignmentSnapshot.getExistingAssignmentPlan();
    AssignmentDomain domain = assignmentSnapshot.getGlobalAssignmentDomain();
    Map<HServerAddress, List<HRegionInfo>> mapServerToRegions = assignmentSnapshot
        .getRegionServerToRegionMap();
    Map<String, List<HServerAddress>> mapRackToRegionServers = domain
        .getRackToRegionServerMap();

    int avgRegionsPerRs = calculateAvgRegionsPerRS(mapServerToRegions);
    if (avgRegionsPerRs == 0) {
      System.out
          .println("ERROR: average number of regions per regionserver is ZERO, ABORTING!");
      return;
    }

    // because the domain knows about the newly added rack we subtract 1.
    int totalNumRacks = domain.getTotalRackNum() - 1;
    int numServersNewRack = domain.getRackToRegionServerMap().get(newRack)
        .size();
    int moveRegionsPerRack = (int) Math.floor(avgRegionsPerRs
        * numServersNewRack / totalNumRacks);
    List<HRegionInfo> regionsToMove = new ArrayList<HRegionInfo>();
    for (String rack : mapRackToRegionServers.keySet()) {
      if (!rack.equals(newRack)) {
        List<HRegionInfo> regionsToMoveFromRack = pickRegionsFromRack(plan,
            mapRackToRegionServers.get(rack), mapServerToRegions, rack,
            moveRegionsPerRack, newRack);
        if (regionsToMoveFromRack.isEmpty()) {
          System.out
              .println("WARNING: number of regions to be moved from rack "
                  + rack + " is zero!");
        }
        regionsToMove.addAll(regionsToMoveFromRack);
      }
    }
    moveRegionsToNewRack(plan, domain, regionsToMove, newRack);
  }

  /**
   * This method will pick regions from a given rack, such that these regions
   * are going to be moved to the new rack later. The logic behind is: we move
   * the regions' tertiaries into a new rack
   *
   * @param plan - the current assignment plan
   * @param regionServersFromOneRack - region servers that belong to the given
   * rack
   * @param mapServerToRegions map contains the mapping from a region server to
   * it's regions
   * @param currentRack
   * @param moveRegionsPerRack - how many regions we want to move per rack
   * @param newRack - the new rack where we will move the tertiaries
   * @return a complete list of regions that are going to be moved in the new
   * rack
   */
  private List<HRegionInfo> pickRegionsFromRack(AssignmentPlan plan,
      List<HServerAddress> regionServersFromOneRack,
      Map<HServerAddress, List<HRegionInfo>> mapServerToRegions,
      String currentRack, int moveRegionsPerRack, String newRack) {
    // we want to move equal number of tertiaries per server
    List<HRegionInfo> regionsToMove = new ArrayList<HRegionInfo>();
    System.out.println("------------------------------------------");
    System.out.println("Printing how many regions are planned to be moved per regionserver in rack " + currentRack);
    // use Math.ceil to ensure that we move at least one region from some of the RS
    // if we should move a number of regions smaller than the total number of
    // servers in the rack
    int movePerServer = (int) Math.ceil(moveRegionsPerRack
        / regionServersFromOneRack.size());
    int totalMovedPerRack = 0;
    int serverIndex = 0;
    for (serverIndex = 0; serverIndex < regionServersFromOneRack.size(); serverIndex++) {
      HServerAddress server = regionServersFromOneRack.get(serverIndex);
      int totalMovedPerRs = 0;
      // get all the regions on this server
      List<HRegionInfo> regions = mapServerToRegions.get(server);
      for (HRegionInfo region : regions) {
        List<HServerAddress> favoredNodes = plan.getAssignment(region);
        if (favoredNodes.size() != HConstants.FAVORED_NODES_NUM) {
          System.out.println("WARNING!!! Number of favored nodes for region "
              + region.getEncodedName() + " is not "
              + HConstants.FAVORED_NODES_NUM);
        }
        regionsToMove.add(region);
        totalMovedPerRs++;
        totalMovedPerRack++;
        if (totalMovedPerRs == movePerServer
            || totalMovedPerRack == moveRegionsPerRack)
          break;
      }
      System.out.println(totalMovedPerRs + " tertiary regions from RS "
          + regionServersFromOneRack.get(serverIndex).getHostname()
          + " will be moved to a new rack");
      if (totalMovedPerRack == moveRegionsPerRack)
        break;
    }
    while (serverIndex < regionServersFromOneRack.size()) {
      System.out.println("0 tertiary regions from RS "
          + regionServersFromOneRack.get(serverIndex).getHostname()
          + " will be moved to a new rack");
      serverIndex++;
    }
    System.out.println("Total number of regions: " + totalMovedPerRack
        + " in rack " + currentRack + " will move its tertiary to a new rack: "
        + newRack);
    System.out.println("------------------------------------------");
    return regionsToMove;
  }

  /**
   * Returns the average number of regions per regionserver
   *
   * @param mapServerToRegions
   * @return
   */
  private int calculateAvgRegionsPerRS(
      Map<HServerAddress, List<HRegionInfo>> mapServerToRegions) {
    int totalRegions = 0;
    int totalRS = 0;
    for (Entry<HServerAddress, List<HRegionInfo>> entry : mapServerToRegions
        .entrySet()) {
      if (entry.getKey() != null && entry.getKey() != null) {
        totalRegions += entry.getValue().size();
        totalRS++;
      }
    }
    return (int) Math.floor(totalRegions / totalRS);
  }

  /**
   *
   * Move the regions to the new rack, such that each server will get equal
   * number of regions
   *
   * @param plan - the current assignment plan
   * @param domain - the assignment domain
   * @param regionsToMove - the regions that would be moved to the new rack
   * regionserver per rack are picked to be moved in the new rack
   * @param newRack - the new rack
   * @throws IOException
   */
  private void moveRegionsToNewRack(AssignmentPlan plan,
      AssignmentDomain domain, List<HRegionInfo> regionsToMove, String newRack)
      throws IOException {
    System.out.println("------------------------------------------");
    System.out
        .println("Printing how many regions are planned to be assigned per region server in the new rack (" + newRack + ")");
    List<HServerAddress> serversFromNewRack = domain
        .getServersFromRack(newRack);
    int totalNumRSNewRack = serversFromNewRack.size();
    for (int j = 0; j < totalNumRSNewRack; j++) {
      int regionsPerRs = 0;
      for (int i = j; i < regionsToMove.size(); i += totalNumRSNewRack) {
        HRegionInfo region = regionsToMove.get(i);
        List<HServerAddress> favoredNodes = plan.getAssignment(region);
        if (AssignmentPlan.replaceFavoredNodesServerWithNew(favoredNodes,
            POSITION.TERTIARY, serversFromNewRack.get(j))) {
          plan.updateAssignmentPlan(region, favoredNodes);

        }
        regionsPerRs++;
      }
      System.out.println("RS: " + serversFromNewRack.get(j).getHostname()
          + " got " + regionsPerRs + "tertiary regions");
    }
    System.out
        .println("Do you want to update the assignment plan with this changes (y/n): ");
    Scanner s = new Scanner(System.in);
    String input = s.nextLine().trim();
    s.close();
    if (input.toLowerCase().equals("y")) {
      System.out.println("Updating assignment plan...");
      updateAssignmentPlanToMeta(plan);
      updateAssignmentPlanToRegionServers(plan);
    } else {
      System.out.println("exiting without updating the assignment plan");
    }
  }
  /**
   * Generate the assignment plan for the existing table
   *
   * @param tableName
   * @param assignmentSnapshot
   * @param regionLocalityMap
   * @param plan
   * @param munkresForSecondaryAndTertiary if set on true the assignment plan
   * for the tertiary and secondary will be generated with Munkres algorithm,
   * otherwise will be generated using placeSecondaryAndTertiaryRS
   * @throws IOException
   */
  private void genAssignmentPlan(String tableName,
      RegionAssignmentSnapshot assignmentSnapshot,
      Map<String, Map<String, Float>> regionLocalityMap, AssignmentPlan plan,
      boolean munkresForSecondaryAndTertiary) throws IOException {
      // Get the all the regions for the current table
      List<HRegionInfo> regions =
        assignmentSnapshot.getTableToRegionMap().get(tableName);
      int numRegions = regions.size();

      // Get the current assignment map
      Map<HRegionInfo, HServerAddress> currentAssignmentMap =
        assignmentSnapshot.getRegionToRegionServerMap();

      // Get the assignment domain
      AssignmentDomain domain = assignmentSnapshot.getGlobalAssignmentDomain();

      // Get the all the region servers
      List<HServerAddress> servers = new ArrayList<HServerAddress>();
      servers.addAll(domain.getAllServers());

      LOG.info("Start to generate assignment plan for " + numRegions +
          " regions from table " + tableName + " with " +
          servers.size() + " region servers");

      int slotsPerServer = (int) Math.ceil((float) numRegions /
          servers.size());
      int regionSlots = slotsPerServer * servers.size();

      // Compute the primary, secondary and tertiary costs for each region/server
      // pair. These costs are based only on node locality and rack locality, and
      // will be modified later.
      float[][] primaryCost = new float[numRegions][regionSlots];
      float[][] secondaryCost = new float[numRegions][regionSlots];
      float[][] tertiaryCost = new float[numRegions][regionSlots];

      if (this.enforceLocality && regionLocalityMap != null) {
        // Transform the locality mapping into a 2D array, assuming that any
        // unspecified locality value is 0.
        float[][] localityPerServer = new float[numRegions][regionSlots];
        for (int i = 0; i < numRegions; i++) {
          Map<String, Float> serverLocalityMap =
              regionLocalityMap.get(regions.get(i).getEncodedName());
          if (serverLocalityMap == null) {
            continue;
          }
          for (int j = 0; j < servers.size(); j++) {
            String serverName = servers.get(j).getHostname();
            if (serverName == null) {
              continue;
            }
            Float locality = serverLocalityMap.get(serverName);
            if (locality == null) {
              continue;
            }
            for (int k = 0; k < slotsPerServer; k++) {
              // If we can't find the locality of a region to a server, which occurs
              // because locality is only reported for servers which have some
              // blocks of a region local, then the locality for that pair is 0.
              localityPerServer[i][j * slotsPerServer + k] = locality.floatValue();
            }
          }
        }

        // Compute the total rack locality for each region in each rack. The total
        // rack locality is the sum of the localities of a region on all servers in
        // a rack.
        Map<String, Map<HRegionInfo, Float>> rackRegionLocality =
            new HashMap<String, Map<HRegionInfo, Float>>();
        for (int i = 0; i < numRegions; i++) {
          HRegionInfo region = regions.get(i);
          for (int j = 0; j < regionSlots; j += slotsPerServer) {
            String rack = domain.getRack(servers.get(j / slotsPerServer));
            Map<HRegionInfo, Float> rackLocality = rackRegionLocality.get(rack);
            if (rackLocality == null) {
              rackLocality = new HashMap<HRegionInfo, Float>();
              rackRegionLocality.put(rack, rackLocality);
            }
            Float localityObj = rackLocality.get(region);
            float locality = localityObj == null ? 0 : localityObj.floatValue();
            locality += localityPerServer[i][j];
            rackLocality.put(region, locality);
          }
        }
        for (int i = 0; i < numRegions; i++) {
          for (int j = 0; j < regionSlots; j++) {
            String rack = domain.getRack(servers.get(j / slotsPerServer));
            Float totalRackLocalityObj =
                rackRegionLocality.get(rack).get(regions.get(i));
            float totalRackLocality = totalRackLocalityObj == null ?
                0 : totalRackLocalityObj.floatValue();

            // Primary cost aims to favor servers with high node locality and low
            // rack locality, so that secondaries and tertiaries can be chosen for
            // nodes with high rack locality. This might give primaries with
            // slightly less locality at first compared to a cost which only
            // considers the node locality, but should be better in the long run.
            primaryCost[i][j] = 1 - (2 * localityPerServer[i][j] -
                totalRackLocality);

            // Secondary cost aims to favor servers with high node locality and high
            // rack locality since the tertiary will be chosen from the same rack as
            // the secondary. This could be negative, but that is okay.
            secondaryCost[i][j] = 2 - (localityPerServer[i][j] + totalRackLocality);

            // Tertiary cost is only concerned with the node locality. It will later
            // be restricted to only hosts on the same rack as the secondary.
            tertiaryCost[i][j] = 1 - localityPerServer[i][j];
          }
        }
      }

      if (this.enforceMinAssignmentMove && currentAssignmentMap != null) {
        // We want to minimize the number of regions which move as the result of a
        // new assignment. Therefore, slightly penalize any placement which is for
        // a host that is not currently serving the region.
        for (int i = 0; i < numRegions; i++) {
          for (int j = 0; j < servers.size(); j++) {
            HServerAddress currentAddress = currentAssignmentMap.get(regions.get(i));
            if (currentAddress != null &&
                !currentAddress.equals(servers.get(j))) {
              for (int k = 0; k < slotsPerServer; k++) {
                primaryCost[i][j * slotsPerServer + k] += NOT_CURRENT_HOST_PENALTY;
              }
            }
          }
        }
      }

      // Artificially increase cost of last slot of each server to evenly
      // distribute the slop, otherwise there will be a few servers with too few
      // regions and many servers with the max number of regions.
      for (int i = 0; i < numRegions; i++) {
        for (int j = 0; j < regionSlots; j += slotsPerServer) {
          primaryCost[i][j] += LAST_SLOT_COST_PENALTY;
          secondaryCost[i][j] += LAST_SLOT_COST_PENALTY;
          tertiaryCost[i][j] += LAST_SLOT_COST_PENALTY;
        }
      }

      RandomizedMatrix randomizedMatrix = new RandomizedMatrix(numRegions,
          regionSlots);
      primaryCost = randomizedMatrix.transform(primaryCost);
      int[] primaryAssignment = new MunkresAssignment(primaryCost).solve();
      primaryAssignment = randomizedMatrix.invertIndices(primaryAssignment);

      // Modify the secondary and tertiary costs for each region/server pair to
      // prevent a region from being assigned to the same rack for both primary
      // and either one of secondary or tertiary.
      for (int i = 0; i < numRegions; i++) {
        int slot = primaryAssignment[i];
        String rack = domain.getRack(servers.get(slot / slotsPerServer));
        for (int k = 0; k < servers.size(); k++) {
          if (!domain.getRack(servers.get(k)).equals(rack)) {
            continue;
          }
          if (k == slot / slotsPerServer) {
            // Same node, do not place secondary or tertiary here ever.
            for (int m = 0; m < slotsPerServer; m++) {
              secondaryCost[i][k * slotsPerServer + m] = MAX_COST;
              tertiaryCost[i][k * slotsPerServer + m] = MAX_COST;
            }
          } else {
            // Same rack, do not place secondary or tertiary here if possible.
            for (int m = 0; m < slotsPerServer; m++) {
              secondaryCost[i][k * slotsPerServer + m] = AVOID_COST;
              tertiaryCost[i][k * slotsPerServer + m] = AVOID_COST;
            }
          }
        }
      }
      if (munkresForSecondaryAndTertiary) {
        randomizedMatrix = new RandomizedMatrix(numRegions, regionSlots);
        secondaryCost = randomizedMatrix.transform(secondaryCost);
        int[] secondaryAssignment = new MunkresAssignment(secondaryCost).solve();
        secondaryAssignment = randomizedMatrix.invertIndices(secondaryAssignment);

        // Modify the tertiary costs for each region/server pair to ensure that a
        // region is assigned to a tertiary server on the same rack as its secondary
        // server, but not the same server in that rack.
        for (int i = 0; i < numRegions; i++) {
          int slot = secondaryAssignment[i];
          String rack = domain.getRack(servers.get(slot / slotsPerServer));
          for (int k = 0; k < servers.size(); k++) {
            if (k == slot / slotsPerServer) {
              // Same node, do not place tertiary here ever.
              for (int m = 0; m < slotsPerServer; m++) {
                tertiaryCost[i][k * slotsPerServer + m] = MAX_COST;
              }
            } else {
              if (domain.getRack(servers.get(k)).equals(rack)) {
                continue;
              }
              // Different rack, do not place tertiary here if possible.
              for (int m = 0; m < slotsPerServer; m++) {
                tertiaryCost[i][k * slotsPerServer + m] = AVOID_COST;
              }
            }
          }
        }

        randomizedMatrix = new RandomizedMatrix(numRegions, regionSlots);
        tertiaryCost = randomizedMatrix.transform(tertiaryCost);
        int[] tertiaryAssignment = new MunkresAssignment(tertiaryCost).solve();
        tertiaryAssignment = randomizedMatrix.invertIndices(tertiaryAssignment);

        for (int i = 0; i < numRegions; i++) {
          List<HServerAddress> favoredServers =
            new ArrayList<HServerAddress>(HConstants.FAVORED_NODES_NUM);
          favoredServers.add(servers.get(primaryAssignment[i] / slotsPerServer));
          favoredServers.add(servers.get(secondaryAssignment[i] / slotsPerServer));
          favoredServers.add(servers.get(tertiaryAssignment[i] / slotsPerServer));
          // Update the assignment plan
          plan.updateAssignmentPlan(regions.get(i), favoredServers);
        }
        LOG.info("Generated the assignment plan for " + numRegions +
            " regions from table " + tableName + " with " +
            servers.size() + " region servers");
        LOG.info("Assignment plan for secondary and tertiary generated " +
            "using MunkresAssignment");
      } else {
        Map<HRegionInfo, HServerAddress> primaryRSMap = new HashMap<HRegionInfo, HServerAddress>();
        for (int i = 0; i < numRegions; i++) {
          primaryRSMap.put(regions.get(i), servers.get(primaryAssignment[i] / slotsPerServer));
        }
        Map<HRegionInfo, Pair<HServerAddress, HServerAddress>> secondaryAndTertiaryMap =
          placeSecondaryAndTertiaryWithRestrictions(primaryRSMap, domain);
        for (int i = 0; i < numRegions; i++) {
          List<HServerAddress> favoredServers =
            new ArrayList<HServerAddress>(HConstants.FAVORED_NODES_NUM);
          HRegionInfo currentRegion = regions.get(i);
          favoredServers.add(primaryRSMap.get(currentRegion));
          Pair<HServerAddress, HServerAddress> secondaryAndTertiary =
              secondaryAndTertiaryMap.get(currentRegion);
          favoredServers.add(secondaryAndTertiary.getFirst());
          favoredServers.add(secondaryAndTertiary.getSecond());
          // Update the assignment plan
          plan.updateAssignmentPlan(regions.get(i), favoredServers);
        }
        LOG.info("Generated the assignment plan for " + numRegions +
            " regions from table " + tableName + " with " +
            servers.size() + " region servers");
        LOG.info("Assignment plan for secondary and tertiary generated " +
            "using placeSecondaryAndTertiaryWithRestrictions method");
      }
    }

  @Override
  public AssignmentPlan getNewAssignmentPlan() throws IOException {
    // Get the current region assignment snapshot by scanning from the META
    RegionAssignmentSnapshot assignmentSnapshot =
      this.getRegionAssignmentSnapshot();

    // Get the region locality map
    Map<String, Map<String, Float>> regionLocalityMap = null;
    if (this.enforceLocality) {
      regionLocalityMap = FSUtils.getRegionDegreeLocalityMappingFromFS(conf);
    }
    // Initialize the assignment plan
    AssignmentPlan plan = new AssignmentPlan();

    // Get the table to region mapping
    Map<String, List<HRegionInfo>> tableToRegionMap =
      assignmentSnapshot.getTableToRegionMap();
    LOG.info("Start to generate the new assignment plan for the " +
         + tableToRegionMap.keySet().size() + " tables" );
    for (String table : tableToRegionMap.keySet()) {
      try {
        if (!this.targetTableSet.isEmpty() &&
            !this.targetTableSet.contains(table)) {
          continue;
        }
        // TODO: maybe run the placement in parallel for each table
        genAssignmentPlan(table, assignmentSnapshot, regionLocalityMap, plan,
            USE_MUNKRES_FOR_PLACING_SECONDARY_AND_TERTIARY);
      } catch (Exception e) {
        LOG.error("Get some exceptions for placing primary region server" +
            "for table " + table + " because " + e);
      }
    }
    LOG.info("Finish to generate the new assignment plan for the " +
        + tableToRegionMap.keySet().size() + " tables" );
    return plan;
  }

  @Override
  public void updateAssignmentPlan(AssignmentPlan plan)
  throws IOException {
    LOG.info("Start to update the new assignment plan for the META table and" +
      " the region servers");
    // Update the new assignment plan to META
    updateAssignmentPlanToMeta(plan);
    // Update the new assignment plan to Region Servers
    updateAssignmentPlanToRegionServers(plan);
    LOG.info("Finish to update the new assignment plan for the META table and" +
        " the region servers");
  }

  @Override
  public AssignmentPlan getExistingAssignmentPlan() throws IOException {
    RegionAssignmentSnapshot snapshot = this.getRegionAssignmentSnapshot();
    return snapshot.getExistingAssignmentPlan();
  }

  /**
   * Update the assignment plan into .META.
   * @param plan the assignments plan to be updated into .META.
   * @throws IOException if cannot update assignment plan in .META.
   */
  public void updateAssignmentPlanToMeta(AssignmentPlan plan)
  throws IOException {
    try {
      LOG.info("Start to update the META with the new assignment plan");
      List<Put> puts = new ArrayList<Put>();
      Map<HRegionInfo, List<HServerAddress>> assignmentMap =
        plan.getAssignmentMap();
      for (Map.Entry<HRegionInfo, List<HServerAddress>> entry :
        assignmentMap.entrySet()) {
        String favoredNodes = RegionPlacement.getFavoredNodes(entry.getValue());
        Put put = new Put(entry.getKey().getRegionName());
        put.add(HConstants.CATALOG_FAMILY, HConstants.FAVOREDNODES_QUALIFIER,
            favoredNodes.getBytes());
        puts.add(put);
      }

      // Write the region assignments to the meta table.
      HTable metaTable = new HTable(conf, HConstants.META_TABLE_NAME);
      metaTable.put(puts);
      LOG.info("Updated the META with the new assignment plan");
    } catch (Exception e) {
      LOG.error("Failed to update META with the new assignment" +
          "plan because " + e.getMessage());
    }
  }

  /**
   * Update the assignment plan to all the region servers
   * @param plan
   * @throws IOException
   */
  private void updateAssignmentPlanToRegionServers(AssignmentPlan plan)
  throws IOException{
    LOG.info("Start to update the region servers with the new assignment plan");
    // Get the region to region server map
    Map<HServerAddress, List<HRegionInfo>> currentAssignment =
      this.getRegionAssignmentSnapshot().getRegionServerToRegionMap();
    HConnection connection = this.getHBaseAdmin().getConnection();

    // track of the failed and succeeded updates
    int succeededNum = 0;
    Map<HServerAddress, Exception> failedUpdateMap =
      new HashMap<HServerAddress, Exception>();

    for (Map.Entry<HServerAddress, List<HRegionInfo>> entry :
      currentAssignment.entrySet()) {
      try {
        // Keep track of the favored updates for the current region server
        AssignmentPlan singleServerPlan = null;
        // Find out all the updates for the current region server
        for (HRegionInfo region : entry.getValue()) {
          List<HServerAddress> favoredServerList = plan.getAssignment(region);
          if (favoredServerList != null &&
              favoredServerList.size() == HConstants.FAVORED_NODES_NUM) {
            // Create the single server plan if necessary
            if (singleServerPlan == null) {
              singleServerPlan = new AssignmentPlan();
            }
            // Update the single server update
            singleServerPlan.updateAssignmentPlan(region, favoredServerList);
          }

        }
        if (singleServerPlan != null) {
          // Update the current region server with its updated favored nodes
          HRegionInterface currentRegionServer =
            connection.getHRegionConnection(entry.getKey());
          int updatedRegionNum;
          String hostName =
            currentRegionServer.getHServerInfo().getHostnamePort();
          updatedRegionNum =
            currentRegionServer.updateFavoredNodes(singleServerPlan);
          LOG.info("Region server " + hostName + " has updated " +
              updatedRegionNum + " / " +
              singleServerPlan.getAssignmentMap().size() +
              " regions with the assignment plan");
          succeededNum ++;
        }
      } catch (Exception e) {
        failedUpdateMap.put(entry.getKey(), e);
      }
    }
    // log the succeeded updates
    LOG.info("Updated " + succeededNum + " region servers with " +
            "the new assignment plan");

    // log the failed updates
    int failedNum = failedUpdateMap.size();
    if (failedNum != 0) {
      LOG.error("Failed to update the following + " + failedNum +
          " region servers with its corresponding favored nodes");
      for (Map.Entry<HServerAddress, Exception> entry :
        failedUpdateMap.entrySet() ) {
        LOG.error("Failed to update " + entry.getKey().getHostNameWithPort() +
            " because of " + entry.getValue().getMessage());
      }
    }
  }


  /**
   * Verify the region placement is consistent with the assignment plan;
   * @throws IOException
   */
  public void verifyRegionPlacement(boolean isDetailMode) throws IOException {
    System.out.println("Start to verify the region assignment and " +
        "generate the verification report");
    // Get the region assignment snapshot
    RegionAssignmentSnapshot snapshot = this.getRegionAssignmentSnapshot();

    // Get all the tables
    Set<String> tables = snapshot.getTableSet();

    // Get the region locality map
    Map<String, Map<String, Float>> regionLocalityMap = null;
    if (this.enforceLocality == true) {
      regionLocalityMap = FSUtils.getRegionDegreeLocalityMappingFromFS(conf);
    }
    // Iterate all the tables to fill up the verification report
    for (String table : tables) {
      if (!this.targetTableSet.isEmpty() &&
          !this.targetTableSet.contains(table)) {
        continue;
      }
      AssignmentVerificationReport report = new AssignmentVerificationReport();
      report.fillUp(table, snapshot, regionLocalityMap);
      report.print(isDetailMode);
    }
  }

  public void printDispersionScores(String table,
      RegionAssignmentSnapshot snapshot, int numRegions, AssignmentPlan newPlan,
      boolean simplePrint) {
    if (!this.targetTableSet.isEmpty() && !this.targetTableSet.contains(table)) {
      return;
    }
    AssignmentVerificationReport report = new AssignmentVerificationReport();
    report.fillUpDispersion(table, snapshot, newPlan);
    List<Float> dispersion = report.getDispersionInformation();
    if (simplePrint) {
      DecimalFormat df = new java.text.DecimalFormat("#.##");
      System.out.println("\tAvg dispersion score: "
          + df.format(dispersion.get(0)) + " hosts;\tMax dispersion score: "
          + df.format(dispersion.get(1)) + " hosts;\tMin dispersion score: "
          + df.format(dispersion.get(2)) + " hosts;");
    } else {
      LOG.info("For Table: " + table + " ; #Total Regions: " + numRegions
          + " ; The average dispersion score is " + dispersion.get(0));
    }
  }

  public void setTargetTableName(String[] tableNames) {
    if (tableNames != null) {
      for (String table : tableNames)
        this.targetTableSet.add(table);
    }
  }

  /**
   * @param serverList
   * @return string the favoredNodes generated by the server list.
   */
  public static String getFavoredNodes(List<HServerAddress> serverAddrList) {
    String favoredNodes = "";
    if (serverAddrList != null) {
      for (int i = 0 ; i < serverAddrList.size(); i++) {
        favoredNodes += serverAddrList.get(i).getHostNameWithPort();
        if (i != serverAddrList.size() - 1 ) {
          favoredNodes += ",";
        }
      }
    }
    return favoredNodes;
  }

  /**
   * @param favoredNodes The joint string of the favored nodes.
   * @return The array of the InetSocketAddress
   */
  public static InetSocketAddress[] getFavoredInetSocketAddress(
      String favoredNodes) {
    String[] favoredNodesArray = StringUtils.split(favoredNodes, ",");
    InetSocketAddress[] addresses =
      new InetSocketAddress[favoredNodesArray.length];
    for (int i = 0; i < favoredNodesArray.length; i++) {
      HServerAddress serverAddress = new HServerAddress(favoredNodesArray[i]);
      addresses[i] = serverAddress.getInetSocketAddress();
    }
    return addresses;
  }

  /**
   * @param serverList
   * @return The array of the InetSocketAddress
   */
  public static InetSocketAddress[] getFavoredInetSocketAddress(
      List<HServerAddress> serverList) {
    if (serverList == null || serverList.isEmpty())
      return null;

    InetSocketAddress[] addresses =
      new InetSocketAddress[serverList.size()];
    for (int i = 0; i < serverList.size(); i++) {
      addresses[i] = serverList.get(i).getInetSocketAddress();
    }
    return addresses;
  }

  /**
   * @param favoredNodes The bytes of favored nodes
   * @return the list of HServerAddress for the byte array of favored nodes.
   */
  public static List<HServerAddress> getFavoredNodesList(byte[] favoredNodes) {
    String favoredNodesStr = Bytes.toString(favoredNodes);
    return getFavoredNodeList(favoredNodesStr);
  }

  /**
   * @param favoredNodes The Stromg of favored nodes
   * @return the list of HServerAddress for the byte array of favored nodes.
   */
  public static List<HServerAddress> getFavoredNodeList(String favoredNodesStr) {
    String[] favoredNodesArray = StringUtils.split(favoredNodesStr, ",");
    if (favoredNodesArray == null)
      return null;

    List<HServerAddress> serverList = new ArrayList<HServerAddress>();
    for (String hostNameAndPort : favoredNodesArray) {
      serverList.add(new HServerAddress(hostNameAndPort));
    }
    return serverList;
  }
  /**
   * @param favoredNodes The byte array of the favored nodes
   * @return string the favoredNodes generated by the byte array of favored nodes.
   */
  public static String getFavoredNodes(byte[] favoredNodes) {
    List<HServerAddress> serverList = getFavoredNodesList(favoredNodes);
    String favoredNodesStr = getFavoredNodes(serverList);
    return favoredNodesStr;
  }

  /**
   * Print the assignment plan to the system output stream
   * @param plan
   */
  public static void printAssignmentPlan(AssignmentPlan plan) {
    if (plan == null) return;
    LOG.info("========== Start to print the assignment plan ================");
    // sort the map based on region info
    Map<HRegionInfo, List<HServerAddress>> assignmentMap =
      new TreeMap<HRegionInfo, List<HServerAddress>>(plan.getAssignmentMap());

    for (Map.Entry<HRegionInfo, List<HServerAddress>> entry :
      assignmentMap.entrySet()) {
      String serverList = RegionPlacement.getFavoredNodes(entry.getValue());
      String regionName = entry.getKey().getRegionNameAsString();
      LOG.info("Region: " + regionName );
      LOG.info("Its favored nodes: " + serverList);
    }
    LOG.info("========== Finish to print the assignment plan ================");
  }

  public static void main(String[] args) throws IOException,
      InterruptedException {
    // Set all the options
    Options opt = new Options();
    opt.addOption("w", "write", false, "write the assignments to META only");
    opt.addOption("u", "update", false,
        "update the assignments to META and RegionServers together");
    opt.addOption("n", "dry-run", false, "do not write assignments to META");
    opt.addOption("v", "verify", false, "verify current assignments against META");
    opt.addOption("p", "print", false, "print the current assignment plan in META");
    opt.addOption("h", "help", false, "print usage");
    opt.addOption("d", "verification-details", false,
        "print the details of verification report");

    opt.addOption("zk", true, "to set the zookeeper quorum");
    opt.addOption("fs", true, "to set HDFS");
    opt.addOption("hbase_root", true, "to set hbase_root directory");

    opt.addOption("overwrite", false,
        "overwrite the favored nodes for a single region," +
        "for example: -update -r regionName -f server1:port,server2:port,server3:port");
    opt.addOption("r", true, "The region name that needs to be updated");
    opt.addOption("f", true, "The new favored nodes");

    opt.addOption("upload", true,
        "upload the json file which contains new placement plan");
    opt.addOption("download", true,
        "download existing placement plan into a json file");

    opt.addOption("tables", true,
        "The list of table names splitted by ',' ;" +
        "For example: -tables: t1,t2,...,tn");
    opt.addOption("l", "locality", true, "enforce the maxium locality");
    opt.addOption("m", "min-move", true, "enforce minium assignment move");
    opt.addOption("diff", false, "calculate difference between assignment plans");
    opt.addOption("munkres", false,
        "use munkres to place secondaries and tertiaries");
    opt.addOption("ld", "locality-dispersion", false, "print locality and dispersion information for current plan");
    opt.addOption("exprack", "expand-with-rack", false, "expand the regions to a new rack");
    try {
      // Set the log4j
      Logger.getLogger("org.apache.zookeeper").setLevel(Level.ERROR);
      Logger.getLogger("org.apache.hadoop.hbase").setLevel(Level.ERROR);
      Logger.getLogger("org.apache.hadoop.hbase.master.RegionPlacement")
          .setLevel(Level.INFO);

      CommandLine cmd = new GnuParser().parse(opt, args);
      Configuration conf = HBaseConfiguration.create();

      boolean enforceMinAssignmentMove = true;
      boolean enforceLocality = true;
      boolean verificationDetails = false;

      // Read all the options
      if ((cmd.hasOption("l") &&
          cmd.getOptionValue("l").equalsIgnoreCase("false")) ||
          (cmd.hasOption("locality") &&
          cmd.getOptionValue("locality").equalsIgnoreCase("false"))) {
        enforceLocality = false;
      }

      if ((cmd.hasOption("m") &&
          cmd.getOptionValue("m").equalsIgnoreCase("false")) ||
          (cmd.hasOption("min-move") &&
          cmd.getOptionValue("min-move").equalsIgnoreCase("false"))) {
        enforceMinAssignmentMove = false;
      }

      if (cmd.hasOption("zk")) {
        conf.set(HConstants.ZOOKEEPER_QUORUM, cmd.getOptionValue("zk"));
        LOG.info("Setting the zk quorum: " + conf.get(HConstants.ZOOKEEPER_QUORUM));
      }

      if (cmd.hasOption("fs")) {
        conf.set(FileSystem.FS_DEFAULT_NAME_KEY, cmd.getOptionValue("fs"));
        LOG.info("Setting the HDFS: " + conf.get(FileSystem.FS_DEFAULT_NAME_KEY));
      }

      if (cmd.hasOption("hbase_root")) {
        conf.set(HConstants.HBASE_DIR, cmd.getOptionValue("hbase_root"));
        LOG.info("Setting the hbase root directory: " + conf.get(HConstants.HBASE_DIR));
      }

      // Create the region placement obj
      RegionPlacement rp = new RegionPlacement(conf, enforceLocality,
          enforceMinAssignmentMove);

      if (cmd.hasOption("d") || cmd.hasOption("verification-details")) {
        verificationDetails = true;
      }

      if (cmd.hasOption("tables")) {
        String tableNameListStr = cmd.getOptionValue("tables");
        String[] tableNames = StringUtils.split(tableNameListStr, ",");
        rp.setTargetTableName(tableNames);
      }

      if (cmd.hasOption("munkres")) {
        USE_MUNKRES_FOR_PLACING_SECONDARY_AND_TERTIARY = true;
      }

      // Read all the modes
      if (cmd.hasOption("v") || cmd.hasOption("verify")) {
        // Verify the region placement.
        rp.verifyRegionPlacement(verificationDetails);
      } else if (cmd.hasOption("n") || cmd.hasOption("dry-run")) {
        // Generate the assignment plan only without updating the META and RS
        AssignmentPlan plan = rp.getNewAssignmentPlan();
        RegionPlacement.printAssignmentPlan(plan);
      } else if (cmd.hasOption("w") || cmd.hasOption("write")) {
        // Generate the new assignment plan
        AssignmentPlan plan = rp.getNewAssignmentPlan();
        // Print the new assignment plan
        RegionPlacement.printAssignmentPlan(plan);
        // Write the new assignment plan to META
        rp.updateAssignmentPlanToMeta(plan);
      } else if (cmd.hasOption("u") || cmd.hasOption("update")) {
        // Generate the new assignment plan
        AssignmentPlan plan = rp.getNewAssignmentPlan();
        // Print the new assignment plan
        RegionPlacement.printAssignmentPlan(plan);
        // Update the assignment to META and Region Servers
        rp.updateAssignmentPlan(plan);
      } else if (cmd.hasOption("diff")) {
        AssignmentPlan newPlan = rp.getNewAssignmentPlan();
        Map<String, Map<String, Float>> locality = FSUtils
            .getRegionDegreeLocalityMappingFromFS(conf);
        Map<String, Integer> movesPerTable = rp.getRegionsMovement(newPlan);
        rp.checkDifferencesWithOldPlan(movesPerTable, locality, newPlan);
        System.out.println("Do you want to update the assignment plan? [y/n]");
        Scanner s = new Scanner(System.in);
        String input = s.nextLine().trim();
        if (input.equals("y")) {
          System.out.println("Updating assignment plan...");
          rp.updateAssignmentPlan(newPlan);
        }
        s.close();
      } else if (cmd.hasOption("ld")) {
        Map<String, Map<String, Float>> locality = FSUtils
            .getRegionDegreeLocalityMappingFromFS(conf);
        rp.printLocalityAndDispersionForCurrentPlan(locality);
      } else if (cmd.hasOption("p") || cmd.hasOption("print")) {
        AssignmentPlan plan = rp.getExistingAssignmentPlan();
        RegionPlacement.printAssignmentPlan(plan);
      } else if (cmd.hasOption("overwrite")) {
        if (!cmd.hasOption("f") || !cmd.hasOption("r")) {
          throw new IllegalArgumentException("Please specify: " +
              " -update -r regionName -f server1:port,server2:port,server3:port");
        }

        String regionName = cmd.getOptionValue("r");
        String favoredNodesStr = cmd.getOptionValue("f");
        LOG.info("Going to update the region " + regionName + " with the new favored nodes " +
            favoredNodesStr);
        List<HServerAddress> favoredNodes = null;
        HRegionInfo regionInfo =
            rp.getRegionAssignmentSnapshot().getRegionNameToRegionInfoMap().get(regionName);
        if (regionInfo == null) {
          LOG.error("Cannot find the region " + regionName + " from the META");
        } else {
          try {
            favoredNodes = RegionPlacement.getFavoredNodeList(favoredNodesStr);
          } catch (IllegalArgumentException e) {
            LOG.error("Cannot parse the invalid favored nodes because " + e);
          }
          AssignmentPlan newPlan = new AssignmentPlan();
          newPlan.updateAssignmentPlan(regionInfo, favoredNodes);
          rp.updateAssignmentPlan(newPlan);
        }
      } else if (cmd.hasOption("exprack")) {
        RegionAssignmentSnapshot snapshot = rp.getRegionAssignmentSnapshot();
        AssignmentDomain domain = snapshot.getGlobalAssignmentDomain();
        System.out.println("List of current racks: ");
        Set<String> allRacks = domain.getRackToRegionServerMap().keySet();
        for (String rack : allRacks) {
          System.out.println("\t"
              + rack
              + "\t number of Region Servers: "
              + snapshot.getGlobalAssignmentDomain().getServersFromRack(rack)
                  .size());
        }
        System.out
            .println("Insert the name of the new rack (to which the migration should be done): ");
        Scanner s = new Scanner(System.in);
        String newRack = s.nextLine().trim();
        s.close();
        rp.expandRegionsToNewRack(newRack, snapshot);
      } else if (cmd.hasOption("upload")) {
        String fileName = cmd.getOptionValue("upload");
        try {
          String jsonStr = FileUtils.readFileToString(new File(fileName));
          AssignmentPlan newPlan = rp.loadPlansFromJson(jsonStr);
          Map<String, Map<String, Float>> locality = FSUtils
              .getRegionDegreeLocalityMappingFromFS(conf);
          Map<String, Integer> movesPerTable = rp.getRegionsMovement(newPlan);
          rp.checkDifferencesWithOldPlan(movesPerTable, locality, newPlan);
          System.out.println("Do you want to update the assignment plan? [y/n]");
          Scanner s = new Scanner(System.in);
          String input = s.nextLine().trim();
          if (input.equals("y")) {
            System.out.println("Updating assignment plan...");
            rp.updateAssignmentPlan(newPlan);
          }
          s.close();
        } catch (IOException e) {
          LOG.error("Unable to load plan file: " + e);
        } catch (JsonSyntaxException je) {
          LOG.error("Unable to parse json file: " + je);
        }
      } else if (cmd.hasOption("download")) {
        String path = cmd.getOptionValue("download");
        try {
          RegionAssignmentSnapshot snapshot = rp.getRegionAssignmentSnapshot();
          AssignmentPlan plan = snapshot.getExistingAssignmentPlan();
          AssignmentPlanData data = AssignmentPlanData.constructFromAssignmentPlan(plan);
          Gson prettyGson = new GsonBuilder().setPrettyPrinting().create();
          String jsonOutput = prettyGson.toJson(data);
          FileUtils.write(new File(path), jsonOutput);
        } catch (Exception e) {
          LOG.error("Unable to download current assignment plan" + e);
          e.printStackTrace();
        }
      } else {
        printHelp(opt);
      }
    } catch (ParseException e) {
      printHelp(opt);
    }
  }

  private static void printHelp(Options opt) {
    new HelpFormatter().printHelp(
        "RegionPlacement < -w | -u | -n | -v | -t | -h | -overwrite -r regionName -f favoredNodes " +
        "-diff>" +
        " [-l false] [-m false] [-d] [-tables t1,t2,...tn] [-zk zk1,zk2,zk3]" +
        " [-fs hdfs://a.b.c.d:9000] [-hbase_root /HBASE]", opt);
  }

  /**
   * @return the cached HBaseAdmin
   * @throws IOException
   */
  private HBaseAdmin getHBaseAdmin() throws IOException {
    if (this.admin == null) {
      this.admin = new HBaseAdmin(this.conf);
    }
    return this.admin;
  }

  /**
   * @return the new RegionAssignmentSnapshot
   * @throws IOException
   */
  public RegionAssignmentSnapshot getRegionAssignmentSnapshot()
  throws IOException {
    RegionAssignmentSnapshot currentAssignmentShapshot =
      new RegionAssignmentSnapshot(this.conf);
    currentAssignmentShapshot.initialize();
    return currentAssignmentShapshot;
  }

  /**
   * Some algorithms for solving the assignment problem may traverse workers or
   * jobs in linear order which may result in skewing the assignments of the
   * first jobs in the matrix toward the last workers in the matrix if the
   * costs are uniform. To avoid this kind of clumping, we can randomize the
   * rows and columns of the cost matrix in a reversible way, such that the
   * solution to the assignment problem can be interpreted in terms of the
   * original untransformed cost matrix. Rows and columns are transformed
   * independently such that the elements contained in any row of the input
   * matrix are the same as the elements in the corresponding output matrix,
   * and each row has its elements transformed in the same way. Similarly for
   * columns.
   */
  protected static class RandomizedMatrix {
    private final int rows;
    private final int cols;
    private final int[] rowTransform;
    private final int[] rowInverse;
    private final int[] colTransform;
    private final int[] colInverse;

    /**
     * Create a randomization scheme for a matrix of a given size.
     * @param rows the number of rows in the matrix
     * @param cols the number of columns in the matrix
     */
    public RandomizedMatrix(int rows, int cols) {
      this.rows = rows;
      this.cols = cols;
      Random random = new Random();
      rowTransform = new int[rows];
      rowInverse = new int[rows];
      for (int i = 0; i < rows; i++) {
        rowTransform[i] = i;
      }
      // Shuffle the row indices.
      for (int i = rows - 1; i >= 0; i--) {
        int r = random.nextInt(i + 1);
        int temp = rowTransform[r];
        rowTransform[r] = rowTransform[i];
        rowTransform[i] = temp;
      }
      // Generate the inverse row indices.
      for (int i = 0; i < rows; i++) {
        rowInverse[rowTransform[i]] = i;
      }

      colTransform = new int[cols];
      colInverse = new int[cols];
      for (int i = 0; i < cols; i++) {
        colTransform[i] = i;
      }
      // Shuffle the column indices.
      for (int i = cols - 1; i >= 0; i--) {
        int r = random.nextInt(i + 1);
        int temp = colTransform[r];
        colTransform[r] = colTransform[i];
        colTransform[i] = temp;
      }
      // Generate the inverse column indices.
      for (int i = 0; i < cols; i++) {
        colInverse[colTransform[i]] = i;
      }
    }

    /**
     * Copy a given matrix into a new matrix, transforming each row index and
     * each column index according to the randomization scheme that was created
     * at construction time.
     * @param matrix the cost matrix to transform
     * @return a new matrix with row and column indices transformed
     */
    public float[][] transform(float[][] matrix) {
      float[][] result = new float[rows][cols];
      for (int i = 0; i < rows; i++) {
        for (int j = 0; j < cols; j++) {
          result[rowTransform[i]][colTransform[j]] = matrix[i][j];
        }
      }
      return result;
    }

    /**
     * Copy a given matrix into a new matrix, transforming each row index and
     * each column index according to the inverse of the randomization scheme
     * that was created at construction time.
     * @param matrix the cost matrix to be inverted
     * @return a new matrix with row and column indices inverted
     */
    public float[][] invert(float[][] matrix) {
      float[][] result = new float[rows][cols];
      for (int i = 0; i < rows; i++) {
        for (int j = 0; j < cols; j++) {
          result[rowInverse[i]][colInverse[j]] = matrix[i][j];
        }
      }
      return result;
    }

    /**
     * Given an array where each element {@code indices[i]} represents the
     * randomized column index corresponding to randomized row index {@code i},
     * create a new array with the corresponding inverted indices.
     * @param indices an array of transformed indices to be inverted
     * @return an array of inverted indices
     */
    public int[] invertIndices(int[] indices) {
      int[] result = new int[indices.length];
      for (int i = 0; i < indices.length; i++) {
        result[rowInverse[i]] = colInverse[indices[i]];
      }
      return result;
    }
  }

  /**
   * Return how many regions will move per table since their primary RS will
   * change
   *
   * @param newPlanMap - new AssignmentPlan
   * @return how many primaries will move per table
   */
  public Map<String, Integer> getRegionsMovement(AssignmentPlan newPlan)
      throws IOException {
    Map<String, Integer> movesPerTable = new HashMap<String, Integer>();
    RegionAssignmentSnapshot snapshot = this.getRegionAssignmentSnapshot();
    Map<String, List<HRegionInfo>> tableToRegions = snapshot
        .getTableToRegionMap();
    AssignmentPlan oldPlan = snapshot.getExistingAssignmentPlan();
    Set<String> tables = snapshot.getTableSet();
    for (String table : tables) {
      int movedPrimaries = 0;
      if (!this.targetTableSet.isEmpty()
          && !this.targetTableSet.contains(table)) {
        continue;
      }
      List<HRegionInfo> regions = tableToRegions.get(table);
      for (HRegionInfo region : regions) {
        List<HServerAddress> oldServers = oldPlan.getAssignment(region);
        List<HServerAddress> newServers = newPlan.getAssignment(region);
        if (oldServers != null && newServers != null) {
          HServerAddress oldPrimary = oldServers.get(0);
          HServerAddress newPrimary = newServers.get(0);
          if (oldPrimary.compareTo(newPrimary) != 0) {
            movedPrimaries++;
          }
        }
      }
      movesPerTable.put(table, movedPrimaries);
    }
    return movesPerTable;
  }

  /**
   * Compares two plans and check whether the locality dropped or increased
   * (prints the information as a string) also prints the baseline locality
   *
   * @param movesPerTable - how many primary regions will move per table
   * @param regionLocalityMap - locality map from FS
   * @param newPlan - new assignment plan
   * @param do we want to run verification report
   * @throws IOException
   */
  public void checkDifferencesWithOldPlan(Map<String, Integer> movesPerTable,
      Map<String, Map<String, Float>> regionLocalityMap, AssignmentPlan newPlan)
          throws IOException {
    // localities for primary, secondary and tertiary
    RegionAssignmentSnapshot snapshot = this.getRegionAssignmentSnapshot();
    AssignmentPlan oldPlan = snapshot.getExistingAssignmentPlan();
    Set<String> tables = snapshot.getTableSet();
    Map<String, List<HRegionInfo>> tableToRegionsMap = snapshot.getTableToRegionMap();
    for (String table : tables) {
      float[] deltaLocality = new float[3];
      float[] locality = new float[3];
      if (!this.targetTableSet.isEmpty()
          && !this.targetTableSet.contains(table)) {
        continue;
      }
      List<HRegionInfo> regions = tableToRegionsMap.get(table);
      System.out.println("==================================================");
      System.out.println("Assignment Plan Projection Report For Table: " + table);
      System.out.println("\t Total regions: " + regions.size());
      System.out.println("\t" + movesPerTable.get(table)
          + " primaries will move due to their primary has changed");
      for (HRegionInfo currentRegion : regions) {
        Map<String, Float> regionLocality = regionLocalityMap.get(currentRegion
            .getEncodedName());
        if (regionLocality == null) {
          continue;
        }
        List<HServerAddress> oldServers = oldPlan.getAssignment(currentRegion);
        List<HServerAddress> newServers = newPlan.getAssignment(currentRegion);
        if (newServers != null && oldServers != null) {
          int i=0;
          for (AssignmentPlan.POSITION p : AssignmentPlan.POSITION.values()) {
            HServerAddress newServer = newServers.get(p.ordinal());
            HServerAddress oldServer = oldServers.get(p.ordinal());
            Float oldLocality = 0f;
            if (oldServers != null) {
              oldLocality = regionLocality.get(oldServer.getHostname());
              if (oldLocality == null) {
                oldLocality = 0f;
              }
              locality[i] += oldLocality;
            }
            Float newLocality = regionLocality.get(newServer.getHostname());
            if (newLocality == null) {
              newLocality = 0f;
            }
            deltaLocality[i] += newLocality - oldLocality;
            i++;
          }
        }
      }
      DecimalFormat df = new java.text.DecimalFormat( "#.##");
      for (int i = 0; i < deltaLocality.length; i++) {
        System.out.print("\t\t Baseline locality for ");
        if (i == 0) {
          System.out.print("primary ");
        } else if (i == 1) {
          System.out.print("secondary ");
        } else if (i == 2) {
          System.out.print("tertiary ");
        }
        System.out.println(df.format(100 * locality[i] / regions.size()) + "%");
        System.out.print("\t\t Locality will change with the new plan: ");
        System.out.println(df.format(100 * deltaLocality[i] / regions.size())
            + "%");
      }
      System.out.println("\t Baseline dispersion");
      printDispersionScores(table, snapshot, regions.size(), null, true);
      System.out.println("\t Projected dispersion");
      printDispersionScores(table, snapshot, regions.size(), newPlan, true);
    }
  }

  public void printLocalityAndDispersionForCurrentPlan(
      Map<String, Map<String, Float>> regionLocalityMap) throws IOException {
    RegionAssignmentSnapshot snapshot = this.getRegionAssignmentSnapshot();
    AssignmentPlan assignmentPlan = snapshot.getExistingAssignmentPlan();
    Set<String> tables = snapshot.getTableSet();
    Map<String, List<HRegionInfo>> tableToRegionsMap = snapshot
        .getTableToRegionMap();
    for (String table : tables) {
      float[] locality = new float[3];
      if (!this.targetTableSet.isEmpty()
          && !this.targetTableSet.contains(table)) {
        continue;
      }
      List<HRegionInfo> regions = tableToRegionsMap.get(table);
      for (HRegionInfo currentRegion : regions) {
        Map<String, Float> regionLocality = regionLocalityMap.get(currentRegion
            .getEncodedName());
        if (regionLocality == null) {
          continue;
        }
        List<HServerAddress> servers = assignmentPlan.getAssignment(currentRegion);
        if (servers != null) {
          int i = 0;
          for (AssignmentPlan.POSITION p : AssignmentPlan.POSITION.values()) {
            HServerAddress server = servers.get(p.ordinal());
            Float currentLocality = 0f;
            if (servers != null) {
              currentLocality = regionLocality.get(server.getHostname());
              if (currentLocality == null) {
                currentLocality = 0f;
              }
              locality[i] += currentLocality;
            }
            i++;
          }
        }
      }
      for (int i = 0; i < locality.length; i++) {
        String copy =  null;
        if (i == 0) {
          copy = "primary";
        } else if (i == 1) {
          copy = "secondary";
        } else if (i == 2) {
          copy = "tertiary" ;
        }
        float avgLocality = 100 * locality[i] / regions.size();
        LOG.info("For Table: " + table + " ; #Total Regions: " + regions.size()
            + " ; The average locality for " + copy+ " is " + avgLocality + " %");
      }
      printDispersionScores(table, snapshot, regions.size(), null, false);
    }
  }

  /**
   * Convert json string to assignment plan
   * @param jsonStr
   * @return assignment plan converted from json string
   * @throws JsonSyntaxException
   * @throws IOException
   */
  public AssignmentPlan loadPlansFromJson(String jsonStr)
      throws JsonSyntaxException, IOException {
    AssignmentPlanData data = new Gson().fromJson(jsonStr, AssignmentPlanData.class);
    AssignmentPlan newPlan = new AssignmentPlan();
    RegionAssignmentSnapshot snapshot = this.getRegionAssignmentSnapshot();
    Map<String, HRegionInfo> map = snapshot.getRegionNameToRegionInfoMap();
    for (AssignmentPlanData.Assignment plan : data.getAssignments()) {
      HRegionInfo regionInfo = map.get(plan.getRegionname());
      List<HServerAddress> favoredNodes = null;
      if (regionInfo == null) {
        LOG.error("Cannot find the region " + plan.getRegionname());
      } else {
        List<String> addresses = plan.getFavored();
        String nodesStr = StringUtils.join(addresses, ",");
        try {
          favoredNodes = RegionPlacement.getFavoredNodeList(nodesStr);
        } catch (IllegalArgumentException e) {
          LOG.error("Cannot parse the invalid favored nodes because " + e);
        }
      }
      newPlan.updateAssignmentPlan(regionInfo, favoredNodes);
    }

    return newPlan;
  }
}
