/**
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

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.favored.FavoredNodeAssignmentHelper;
import org.apache.hadoop.hbase.favored.FavoredNodesPlan;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
/**
 * Helper class that is used by {@link RegionPlacementMaintainer} to print
 * information for favored nodes
 *
 */
@InterfaceAudience.Private
public class AssignmentVerificationReport {
  private static final Log LOG = LogFactory.getLog(
      AssignmentVerificationReport.class.getName());

  private TableName tableName = null;
  private boolean enforceLocality = false;
  private boolean isFilledUp = false;

  private int totalRegions = 0;
  private int totalRegionServers = 0;
  // for unassigned regions
  private List<HRegionInfo> unAssignedRegionsList =
    new ArrayList<HRegionInfo>();

  // For regions without valid favored nodes
  private List<HRegionInfo> regionsWithoutValidFavoredNodes =
    new ArrayList<HRegionInfo>();

  // For regions not running on the favored nodes
  private List<HRegionInfo> nonFavoredAssignedRegionList =
    new ArrayList<HRegionInfo>();

  // For regions running on the favored nodes
  private int totalFavoredAssignments = 0;
  private int[] favoredNodes = new int[FavoredNodeAssignmentHelper.FAVORED_NODES_NUM];
  private float[] favoredNodesLocalitySummary =
      new float[FavoredNodeAssignmentHelper.FAVORED_NODES_NUM];
  private float actualLocalitySummary = 0;

  // For region balancing information
  private float avgRegionsOnRS = 0;
  private int maxRegionsOnRS = 0;
  private int minRegionsOnRS = Integer.MAX_VALUE;
  private Set<ServerName> mostLoadedRSSet =
    new HashSet<ServerName>();
  private Set<ServerName> leastLoadedRSSet =
    new HashSet<ServerName>();

  private float avgDispersionScore = 0;
  private float maxDispersionScore = 0;
  private Set<ServerName> maxDispersionScoreServerSet =
    new HashSet<ServerName>();
  private float minDispersionScore = Float.MAX_VALUE;
  private Set<ServerName> minDispersionScoreServerSet =
    new HashSet<ServerName>();

  private float avgDispersionNum = 0;
  private float maxDispersionNum = 0;
  private Set<ServerName> maxDispersionNumServerSet =
    new HashSet<ServerName>();
  private float minDispersionNum = Float.MAX_VALUE;
  private Set<ServerName> minDispersionNumServerSet =
    new HashSet<ServerName>();

  public void fillUp(TableName tableName, SnapshotOfRegionAssignmentFromMeta snapshot,
      Map<String, Map<String, Float>> regionLocalityMap) {
    // Set the table name
    this.tableName = tableName;

    // Get all the regions for this table
    List<HRegionInfo> regionInfoList =
      snapshot.getTableToRegionMap().get(tableName);
    // Get the total region num for the current table
    this.totalRegions = regionInfoList.size();

    // Get the existing assignment plan
    FavoredNodesPlan favoredNodesAssignment = snapshot.getExistingAssignmentPlan();
    // Get the region to region server mapping
    Map<HRegionInfo, ServerName> currentAssignment =
      snapshot.getRegionToRegionServerMap();
    // Initialize the server to its hosing region counter map
    Map<ServerName, Integer> serverToHostingRegionCounterMap =
      new HashMap<ServerName, Integer>();

    Map<ServerName, Integer> primaryRSToRegionCounterMap =
      new HashMap<ServerName, Integer>();
    Map<ServerName, Set<ServerName>> primaryToSecTerRSMap =
      new HashMap<ServerName, Set<ServerName>>();

    // Check the favored nodes and its locality information
    // Also keep tracker of the most loaded and least loaded region servers
    for (HRegionInfo region : regionInfoList) {
      try {
        ServerName currentRS = currentAssignment.get(region);
        // Handle unassigned regions
        if (currentRS == null) {
          unAssignedRegionsList.add(region);
          continue;
        }

        // Keep updating the server to is hosting region counter map
        Integer hostRegionCounter = serverToHostingRegionCounterMap.get(currentRS);
        if (hostRegionCounter == null) {
          hostRegionCounter = Integer.valueOf(0);
        }
        hostRegionCounter = hostRegionCounter.intValue() + 1;
        serverToHostingRegionCounterMap.put(currentRS, hostRegionCounter);

        // Get the favored nodes from the assignment plan and verify it.
        List<ServerName> favoredNodes = favoredNodesAssignment.getFavoredNodes(region);
        if (favoredNodes == null ||
            favoredNodes.size() != FavoredNodeAssignmentHelper.FAVORED_NODES_NUM) {
          regionsWithoutValidFavoredNodes.add(region);
          continue;
        }
        // Get the primary, secondary and tertiary region server
        ServerName primaryRS =
          favoredNodes.get(FavoredNodesPlan.Position.PRIMARY.ordinal());
        ServerName secondaryRS =
          favoredNodes.get(FavoredNodesPlan.Position.SECONDARY.ordinal());
        ServerName tertiaryRS =
          favoredNodes.get(FavoredNodesPlan.Position.TERTIARY.ordinal());

        // Update the primary rs to its region set map
        Integer regionCounter = primaryRSToRegionCounterMap.get(primaryRS);
        if (regionCounter == null) {
          regionCounter = Integer.valueOf(0);
        }
        regionCounter = regionCounter.intValue() + 1;
        primaryRSToRegionCounterMap.put(primaryRS, regionCounter);

        // Update the primary rs to secondary and tertiary rs map
        Set<ServerName> secAndTerSet = primaryToSecTerRSMap.get(primaryRS);
        if (secAndTerSet == null) {
          secAndTerSet = new HashSet<ServerName>();
        }
        secAndTerSet.add(secondaryRS);
        secAndTerSet.add(tertiaryRS);
        primaryToSecTerRSMap.put(primaryRS, secAndTerSet);

        // Get the position of the current region server in the favored nodes list
        FavoredNodesPlan.Position favoredNodePosition =
          FavoredNodesPlan.getFavoredServerPosition(favoredNodes, currentRS);

        // Handle the non favored assignment.
        if (favoredNodePosition == null) {
          nonFavoredAssignedRegionList.add(region);
          continue;
        }
        // Increase the favored nodes assignment.
        this.favoredNodes[favoredNodePosition.ordinal()]++;
        totalFavoredAssignments++;

        // Summary the locality information for each favored nodes
        if (regionLocalityMap != null) {
          // Set the enforce locality as true;
          this.enforceLocality = true;

          // Get the region degree locality map
          Map<String, Float> regionDegreeLocalityMap =
            regionLocalityMap.get(region.getEncodedName());
          if (regionDegreeLocalityMap == null) {
            continue; // ignore the region which doesn't have any store files.
          }

          // Get the locality summary for each favored nodes
          for (FavoredNodesPlan.Position p : FavoredNodesPlan.Position.values()) {
            ServerName favoredNode = favoredNodes.get(p.ordinal());
            // Get the locality for the current favored nodes
            Float locality =
              regionDegreeLocalityMap.get(favoredNode.getHostname());
            if (locality != null) {
              this.favoredNodesLocalitySummary[p.ordinal()] += locality;
            }
          }

          // Get the locality summary for the current region server
          Float actualLocality =
            regionDegreeLocalityMap.get(currentRS.getHostname());
          if (actualLocality != null) {
            this.actualLocalitySummary += actualLocality;
          }
        }
      } catch (Exception e) {
        LOG.error("Cannot verify the region assignment for region " +
            ((region == null) ? " null " : region.getRegionNameAsString()) +
            "because of " + e);
      }
    }

    float dispersionScoreSummary = 0;
    float dispersionNumSummary = 0;
    // Calculate the secondary score for each primary region server
    for (Map.Entry<ServerName, Integer> entry :
      primaryRSToRegionCounterMap.entrySet()) {
      ServerName primaryRS = entry.getKey();
      Integer regionsOnPrimary = entry.getValue();

      // Process the dispersion number and score
      float dispersionScore = 0;
      int dispersionNum = 0;
      if (primaryToSecTerRSMap.get(primaryRS) != null
          && regionsOnPrimary.intValue() != 0) {
        dispersionNum = primaryToSecTerRSMap.get(primaryRS).size();
        dispersionScore = dispersionNum /
          ((float) regionsOnPrimary.intValue() * 2);
      }
      // Update the max dispersion score
      if (dispersionScore > this.maxDispersionScore) {
        this.maxDispersionScoreServerSet.clear();
        this.maxDispersionScoreServerSet.add(primaryRS);
        this.maxDispersionScore = dispersionScore;
      } else if (dispersionScore == this.maxDispersionScore) {
        this.maxDispersionScoreServerSet.add(primaryRS);
      }

      // Update the max dispersion num
      if (dispersionNum > this.maxDispersionNum) {
        this.maxDispersionNumServerSet.clear();
        this.maxDispersionNumServerSet.add(primaryRS);
        this.maxDispersionNum = dispersionNum;
      } else if (dispersionNum == this.maxDispersionNum) {
        this.maxDispersionNumServerSet.add(primaryRS);
      }

      // Update the min dispersion score
      if (dispersionScore < this.minDispersionScore) {
        this.minDispersionScoreServerSet.clear();
        this.minDispersionScoreServerSet.add(primaryRS);
        this.minDispersionScore = dispersionScore;
      } else if (dispersionScore == this.minDispersionScore) {
        this.minDispersionScoreServerSet.add(primaryRS);
      }

      // Update the min dispersion num
      if (dispersionNum < this.minDispersionNum) {
        this.minDispersionNumServerSet.clear();
        this.minDispersionNumServerSet.add(primaryRS);
        this.minDispersionNum = dispersionNum;
      } else if (dispersionNum == this.minDispersionNum) {
        this.minDispersionNumServerSet.add(primaryRS);
      }

      dispersionScoreSummary += dispersionScore;
      dispersionNumSummary += dispersionNum;
    }

    // Update the avg dispersion score
    if (primaryRSToRegionCounterMap.keySet().size() != 0) {
      this.avgDispersionScore = dispersionScoreSummary /
         (float) primaryRSToRegionCounterMap.keySet().size();
      this.avgDispersionNum = dispersionNumSummary /
         (float) primaryRSToRegionCounterMap.keySet().size();
    }

    // Fill up the most loaded and least loaded region server information
    for (Map.Entry<ServerName, Integer> entry :
      serverToHostingRegionCounterMap.entrySet()) {
      ServerName currentRS = entry.getKey();
      int hostRegionCounter = entry.getValue().intValue();

      // Update the most loaded region server list and maxRegionsOnRS
      if (hostRegionCounter > this.maxRegionsOnRS) {
        maxRegionsOnRS = hostRegionCounter;
        this.mostLoadedRSSet.clear();
        this.mostLoadedRSSet.add(currentRS);
      } else if (hostRegionCounter == this.maxRegionsOnRS) {
        this.mostLoadedRSSet.add(currentRS);
      }

      // Update the least loaded region server list and minRegionsOnRS
      if (hostRegionCounter < this.minRegionsOnRS) {
        this.minRegionsOnRS = hostRegionCounter;
        this.leastLoadedRSSet.clear();
        this.leastLoadedRSSet.add(currentRS);
      } else if (hostRegionCounter == this.minRegionsOnRS) {
        this.leastLoadedRSSet.add(currentRS);
      }
    }

    // and total region servers
    this.totalRegionServers = serverToHostingRegionCounterMap.keySet().size();
    this.avgRegionsOnRS = (totalRegionServers == 0) ? 0 :
      (totalRegions / (float) totalRegionServers);
    // Set the isFilledUp as true
    isFilledUp = true;
  }

  /**
   * Use this to project the dispersion scores
   * @param tableName
   * @param snapshot
   * @param newPlan
   */
  public void fillUpDispersion(TableName tableName,
      SnapshotOfRegionAssignmentFromMeta snapshot, FavoredNodesPlan newPlan) {
    // Set the table name
    this.tableName = tableName;
    // Get all the regions for this table
    List<HRegionInfo> regionInfoList = snapshot.getTableToRegionMap().get(
        tableName);
    // Get the total region num for the current table
    this.totalRegions = regionInfoList.size();
    FavoredNodesPlan plan = null;
    if (newPlan == null) {
      plan = snapshot.getExistingAssignmentPlan();
    } else {
      plan = newPlan;
    }
    // Get the region to region server mapping
    Map<ServerName, Integer> primaryRSToRegionCounterMap =
        new HashMap<ServerName, Integer>();
    Map<ServerName, Set<ServerName>> primaryToSecTerRSMap =
        new HashMap<ServerName, Set<ServerName>>();

    // Check the favored nodes and its locality information
    // Also keep tracker of the most loaded and least loaded region servers
    for (HRegionInfo region : regionInfoList) {
      try {
        // Get the favored nodes from the assignment plan and verify it.
        List<ServerName> favoredNodes = plan.getFavoredNodes(region);
        if (favoredNodes == null
            || favoredNodes.size() != FavoredNodeAssignmentHelper.FAVORED_NODES_NUM) {
          regionsWithoutValidFavoredNodes.add(region);
          continue;
        }
        // Get the primary, secondary and tertiary region server
        ServerName primaryRS = favoredNodes
            .get(FavoredNodesPlan.Position.PRIMARY.ordinal());
        ServerName secondaryRS = favoredNodes
            .get(FavoredNodesPlan.Position.SECONDARY.ordinal());
        ServerName tertiaryRS = favoredNodes
            .get(FavoredNodesPlan.Position.TERTIARY.ordinal());

        // Update the primary rs to its region set map
        Integer regionCounter = primaryRSToRegionCounterMap.get(primaryRS);
        if (regionCounter == null) {
          regionCounter = Integer.valueOf(0);
        }
        regionCounter = regionCounter.intValue() + 1;
        primaryRSToRegionCounterMap.put(primaryRS, regionCounter);

        // Update the primary rs to secondary and tertiary rs map
        Set<ServerName> secAndTerSet = primaryToSecTerRSMap.get(primaryRS);
        if (secAndTerSet == null) {
          secAndTerSet = new HashSet<ServerName>();
        }
        secAndTerSet.add(secondaryRS);
        secAndTerSet.add(tertiaryRS);
        primaryToSecTerRSMap.put(primaryRS, secAndTerSet);
      } catch (Exception e) {
        LOG.error("Cannot verify the region assignment for region "
            + ((region == null) ? " null " : region.getRegionNameAsString())
            + "because of " + e);
      }
    }
    float dispersionScoreSummary = 0;
    float dispersionNumSummary = 0;
    // Calculate the secondary score for each primary region server
    for (Map.Entry<ServerName, Integer> entry :
      primaryRSToRegionCounterMap.entrySet()) {
      ServerName primaryRS = entry.getKey();
      Integer regionsOnPrimary = entry.getValue();

      // Process the dispersion number and score
      float dispersionScore = 0;
      int dispersionNum = 0;
      if (primaryToSecTerRSMap.get(primaryRS) != null
          && regionsOnPrimary.intValue() != 0) {
        dispersionNum = primaryToSecTerRSMap.get(primaryRS).size();
        dispersionScore = dispersionNum /
          ((float) regionsOnPrimary.intValue() * 2);
      }

      // Update the max dispersion num
      if (dispersionNum > this.maxDispersionNum) {
        this.maxDispersionNumServerSet.clear();
        this.maxDispersionNumServerSet.add(primaryRS);
        this.maxDispersionNum = dispersionNum;
      } else if (dispersionNum == this.maxDispersionNum) {
        this.maxDispersionNumServerSet.add(primaryRS);
      }

      // Update the min dispersion score
      if (dispersionScore < this.minDispersionScore) {
        this.minDispersionScoreServerSet.clear();
        this.minDispersionScoreServerSet.add(primaryRS);
        this.minDispersionScore = dispersionScore;
      } else if (dispersionScore == this.minDispersionScore) {
        this.minDispersionScoreServerSet.add(primaryRS);
      }

      // Update the min dispersion num
      if (dispersionNum < this.minDispersionNum) {
        this.minDispersionNumServerSet.clear();
        this.minDispersionNumServerSet.add(primaryRS);
        this.minDispersionNum = dispersionNum;
      } else if (dispersionNum == this.minDispersionNum) {
        this.minDispersionNumServerSet.add(primaryRS);
      }

      dispersionScoreSummary += dispersionScore;
      dispersionNumSummary += dispersionNum;
    }

    // Update the avg dispersion score
    if (primaryRSToRegionCounterMap.keySet().size() != 0) {
      this.avgDispersionScore = dispersionScoreSummary /
         (float) primaryRSToRegionCounterMap.keySet().size();
      this.avgDispersionNum = dispersionNumSummary /
         (float) primaryRSToRegionCounterMap.keySet().size();
    }
  }

  /**
   * @return list which contains just 3 elements: average dispersion score, max
   * dispersion score and min dispersion score as first, second and third element
   * respectively.
   *
   */
  public List<Float> getDispersionInformation() {
    List<Float> dispersion = new ArrayList<Float>();
    dispersion.add(avgDispersionScore);
    dispersion.add(maxDispersionScore);
    dispersion.add(minDispersionScore);
    return dispersion;
  }

  public void print(boolean isDetailMode) {
    if (!isFilledUp) {
      System.err.println("[Error] Region assignment verification report" +
          "hasn't been filled up");
    }
    DecimalFormat df = new java.text.DecimalFormat( "#.##");

    // Print some basic information
    System.out.println("Region Assignment Verification for Table: " + tableName +
        "\n\tTotal regions : " + totalRegions);

    // Print the number of regions on each kinds of the favored nodes
    System.out.println("\tTotal regions on favored nodes " +
        totalFavoredAssignments);
    for (FavoredNodesPlan.Position p : FavoredNodesPlan.Position.values()) {
      System.out.println("\t\tTotal regions on "+ p.toString() +
          " region servers: " + favoredNodes[p.ordinal()]);
    }
    // Print the number of regions in each kinds of invalid assignment
    System.out.println("\tTotal unassigned regions: " +
        unAssignedRegionsList.size());
    if (isDetailMode) {
      for (HRegionInfo region : unAssignedRegionsList) {
        System.out.println("\t\t" + region.getRegionNameAsString());
      }
    }

    System.out.println("\tTotal regions NOT on favored nodes: " +
        nonFavoredAssignedRegionList.size());
    if (isDetailMode) {
      for (HRegionInfo region : nonFavoredAssignedRegionList) {
        System.out.println("\t\t" + region.getRegionNameAsString());
      }
    }

    System.out.println("\tTotal regions without favored nodes: " +
        regionsWithoutValidFavoredNodes.size());
    if (isDetailMode) {
      for (HRegionInfo region : regionsWithoutValidFavoredNodes) {
        System.out.println("\t\t" + region.getRegionNameAsString());
      }
    }

    // Print the locality information if enabled
    if (this.enforceLocality && totalRegions != 0) {
      // Print the actual locality for this table
      float actualLocality = 100 *
        this.actualLocalitySummary / (float) totalRegions;
      System.out.println("\n\tThe actual avg locality is " +
          df.format(actualLocality) + " %");

      // Print the expected locality if regions are placed on the each kinds of
      // favored nodes
      for (FavoredNodesPlan.Position p : FavoredNodesPlan.Position.values()) {
        float avgLocality = 100 *
          (favoredNodesLocalitySummary[p.ordinal()] / (float) totalRegions);
        System.out.println("\t\tThe expected avg locality if all regions" +
            " on the " + p.toString() + " region servers: "
            + df.format(avgLocality) + " %");
      }
    }

    // Print the region balancing information
    System.out.println("\n\tTotal hosting region servers: " +
        totalRegionServers);
    // Print the region balance information
    if (totalRegionServers != 0) {
      System.out.println(
          "\tAvg dispersion num: " +df.format(avgDispersionNum) +
          " hosts;\tMax dispersion num: " + df.format(maxDispersionNum) +
          " hosts;\tMin dispersion num: " + df.format(minDispersionNum) +
          " hosts;");

      System.out.println("\t\tThe number of the region servers with the max" +
          " dispersion num: " + this.maxDispersionNumServerSet.size());
      if (isDetailMode) {
        printHServerAddressSet(maxDispersionNumServerSet);
      }

      System.out.println("\t\tThe number of the region servers with the min" +
          " dispersion num: " + this.minDispersionNumServerSet.size());
      if (isDetailMode) {
        printHServerAddressSet(maxDispersionNumServerSet);
      }

      System.out.println(
          "\tAvg dispersion score: " + df.format(avgDispersionScore) +
          ";\tMax dispersion score: " + df.format(maxDispersionScore) +
          ";\tMin dispersion score: " + df.format(minDispersionScore) + ";");

      System.out.println("\t\tThe number of the region servers with the max" +
          " dispersion score: " + this.maxDispersionScoreServerSet.size());
      if (isDetailMode) {
        printHServerAddressSet(maxDispersionScoreServerSet);
      }

      System.out.println("\t\tThe number of the region servers with the min" +
          " dispersion score: " + this.minDispersionScoreServerSet.size());
      if (isDetailMode) {
        printHServerAddressSet(minDispersionScoreServerSet);
      }

      System.out.println(
          "\tAvg regions/region server: " + df.format(avgRegionsOnRS) +
          ";\tMax regions/region server: " + maxRegionsOnRS +
          ";\tMin regions/region server: " + minRegionsOnRS + ";");

      // Print the details about the most loaded region servers
      System.out.println("\t\tThe number of the most loaded region servers: "
          + mostLoadedRSSet.size());
      if (isDetailMode) {
        printHServerAddressSet(mostLoadedRSSet);
      }

      // Print the details about the least loaded region servers
      System.out.println("\t\tThe number of the least loaded region servers: "
          + leastLoadedRSSet.size());
      if (isDetailMode) {
        printHServerAddressSet(leastLoadedRSSet);
      }
    }
    System.out.println("==============================");
  }

  /**
   * Return the unassigned regions
   * @return unassigned regions
   */
  List<HRegionInfo> getUnassignedRegions() {
    return unAssignedRegionsList;
  }

  /**
   * Return the regions without favored nodes
   * @return regions without favored nodes
   */
  List<HRegionInfo> getRegionsWithoutValidFavoredNodes() {
    return regionsWithoutValidFavoredNodes;
  }

  /**
   * Return the regions not assigned to its favored nodes
   * @return regions not assigned to its favored nodes
   */
  List<HRegionInfo> getNonFavoredAssignedRegions() {
    return nonFavoredAssignedRegionList;
  }
  
  /**
   * Return the number of regions assigned to their favored nodes
   * @return number of regions assigned to their favored nodes
   */
  int getTotalFavoredAssignments() {
    return totalFavoredAssignments;
  }

  /**
   * Return the number of regions based on the position (primary/secondary/
   * tertiary) assigned to their favored nodes
   * @param position
   * @return the number of regions
   */
  int getNumRegionsOnFavoredNodeByPosition(FavoredNodesPlan.Position position) {
    return favoredNodes[position.ordinal()];
  }

  private void printHServerAddressSet(Set<ServerName> serverSet) {
    if (serverSet == null) {
      return ;
    }
    int i = 0;
    for (ServerName addr : serverSet){
      if ((i++) % 3 == 0) {
        System.out.print("\n\t\t\t");
      }
      System.out.print(addr.getHostAndPort() + " ; ");
    }
    System.out.println("\n");
  }
}
