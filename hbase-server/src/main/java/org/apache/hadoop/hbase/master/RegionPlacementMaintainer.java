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

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.master.balancer.FavoredNodeAssignmentHelper;
import org.apache.hadoop.hbase.master.balancer.FavoredNodesPlan;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService.BlockingInterface;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.UpdateFavoredNodesRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.UpdateFavoredNodesResponse;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.MunkresAssignment;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * A tool that is used for manipulating and viewing favored nodes information
 * for regions. Run with -h to get a list of the options
 *
 */
@InterfaceAudience.Private
public class RegionPlacementMaintainer {
  private static final Log LOG = LogFactory.getLog(RegionPlacementMaintainer.class
      .getName());
  //The cost of a placement that should never be assigned.
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
  private RackManager rackManager;
  private Set<TableName> targetTableSet;

  public RegionPlacementMaintainer(Configuration conf) {
    this(conf, true, true);
  }

  public RegionPlacementMaintainer(Configuration conf, boolean enforceLocality,
      boolean enforceMinAssignmentMove) {
    this.conf = conf;
    this.enforceLocality = enforceLocality;
    this.enforceMinAssignmentMove = enforceMinAssignmentMove;
    this.targetTableSet = new HashSet<TableName>();
    this.rackManager = new RackManager(conf);
  }
  private static void printHelp(Options opt) {
    new HelpFormatter().printHelp(
        "RegionPlacement < -w | -u | -n | -v | -t | -h | -overwrite -r regionName -f favoredNodes " +
        "-diff>" +
        " [-l false] [-m false] [-d] [-tables t1,t2,...tn] [-zk zk1,zk2,zk3]" +
        " [-fs hdfs://a.b.c.d:9000] [-hbase_root /HBASE]", opt);
  }

  public void setTargetTableName(String[] tableNames) {
    if (tableNames != null) {
      for (String table : tableNames)
        this.targetTableSet.add(TableName.valueOf(table));
    }
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
  public SnapshotOfRegionAssignmentFromMeta getRegionAssignmentSnapshot()
  throws IOException {
    SnapshotOfRegionAssignmentFromMeta currentAssignmentShapshot =
      new SnapshotOfRegionAssignmentFromMeta(HConnectionManager.getConnection(conf));
    currentAssignmentShapshot.initialize();
    return currentAssignmentShapshot;
  }

  /**
   * Verify the region placement is consistent with the assignment plan
   * @param isDetailMode
   * @return reports
   * @throws IOException
   */
  public List<AssignmentVerificationReport> verifyRegionPlacement(boolean isDetailMode)
      throws IOException {
    System.out.println("Start to verify the region assignment and " +
        "generate the verification report");
    // Get the region assignment snapshot
    SnapshotOfRegionAssignmentFromMeta snapshot = this.getRegionAssignmentSnapshot();

    // Get all the tables
    Set<TableName> tables = snapshot.getTableSet();

    // Get the region locality map
    Map<String, Map<String, Float>> regionLocalityMap = null;
    if (this.enforceLocality == true) {
      regionLocalityMap = FSUtils.getRegionDegreeLocalityMappingFromFS(conf);
    }
    List<AssignmentVerificationReport> reports = new ArrayList<AssignmentVerificationReport>();
    // Iterate all the tables to fill up the verification report
    for (TableName table : tables) {
      if (!this.targetTableSet.isEmpty() &&
          !this.targetTableSet.contains(table)) {
        continue;
      }
      AssignmentVerificationReport report = new AssignmentVerificationReport();
      report.fillUp(table, snapshot, regionLocalityMap);
      report.print(isDetailMode);
      reports.add(report);
    }
    return reports;
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
  private void genAssignmentPlan(TableName tableName,
      SnapshotOfRegionAssignmentFromMeta assignmentSnapshot,
      Map<String, Map<String, Float>> regionLocalityMap, FavoredNodesPlan plan,
      boolean munkresForSecondaryAndTertiary) throws IOException {
      // Get the all the regions for the current table
      List<HRegionInfo> regions =
        assignmentSnapshot.getTableToRegionMap().get(tableName);
      int numRegions = regions.size();

      // Get the current assignment map
      Map<HRegionInfo, ServerName> currentAssignmentMap =
        assignmentSnapshot.getRegionToRegionServerMap();

      // Get the all the region servers
      List<ServerName> servers = new ArrayList<ServerName>();
      servers.addAll(getHBaseAdmin().getClusterStatus().getServers());
      
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
            String rack = rackManager.getRack(servers.get(j / slotsPerServer));
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
            String rack = rackManager.getRack(servers.get(j / slotsPerServer));
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
            ServerName currentAddress = currentAssignmentMap.get(regions.get(i));
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
        String rack = rackManager.getRack(servers.get(slot / slotsPerServer));
        for (int k = 0; k < servers.size(); k++) {
          if (!rackManager.getRack(servers.get(k)).equals(rack)) {
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
          String rack = rackManager.getRack(servers.get(slot / slotsPerServer));
          for (int k = 0; k < servers.size(); k++) {
            if (k == slot / slotsPerServer) {
              // Same node, do not place tertiary here ever.
              for (int m = 0; m < slotsPerServer; m++) {
                tertiaryCost[i][k * slotsPerServer + m] = MAX_COST;
              }
            } else {
              if (rackManager.getRack(servers.get(k)).equals(rack)) {
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
          List<ServerName> favoredServers =
            new ArrayList<ServerName>(FavoredNodeAssignmentHelper.FAVORED_NODES_NUM);
          ServerName s = servers.get(primaryAssignment[i] / slotsPerServer);
          favoredServers.add(ServerName.valueOf(s.getHostname(), s.getPort(),
              ServerName.NON_STARTCODE));

          s = servers.get(secondaryAssignment[i] / slotsPerServer);
          favoredServers.add(ServerName.valueOf(s.getHostname(), s.getPort(),
              ServerName.NON_STARTCODE));

          s = servers.get(tertiaryAssignment[i] / slotsPerServer);
          favoredServers.add(ServerName.valueOf(s.getHostname(), s.getPort(),
              ServerName.NON_STARTCODE));
          // Update the assignment plan
          plan.updateAssignmentPlan(regions.get(i), favoredServers);
        }
        LOG.info("Generated the assignment plan for " + numRegions +
            " regions from table " + tableName + " with " +
            servers.size() + " region servers");
        LOG.info("Assignment plan for secondary and tertiary generated " +
            "using MunkresAssignment");
      } else {
        Map<HRegionInfo, ServerName> primaryRSMap = new HashMap<HRegionInfo, ServerName>();
        for (int i = 0; i < numRegions; i++) {
          primaryRSMap.put(regions.get(i), servers.get(primaryAssignment[i] / slotsPerServer));
        }
        FavoredNodeAssignmentHelper favoredNodeHelper =
            new FavoredNodeAssignmentHelper(servers, conf);
        favoredNodeHelper.initialize();
        Map<HRegionInfo, ServerName[]> secondaryAndTertiaryMap =
            favoredNodeHelper.placeSecondaryAndTertiaryWithRestrictions(primaryRSMap);
        for (int i = 0; i < numRegions; i++) {
          List<ServerName> favoredServers =
            new ArrayList<ServerName>(FavoredNodeAssignmentHelper.FAVORED_NODES_NUM);
          HRegionInfo currentRegion = regions.get(i);
          ServerName s = primaryRSMap.get(currentRegion);
          favoredServers.add(ServerName.valueOf(s.getHostname(), s.getPort(),
              ServerName.NON_STARTCODE));

          ServerName[] secondaryAndTertiary =
              secondaryAndTertiaryMap.get(currentRegion);
          s = secondaryAndTertiary[0];
          favoredServers.add(ServerName.valueOf(s.getHostname(), s.getPort(),
              ServerName.NON_STARTCODE));

          s = secondaryAndTertiary[1];
          favoredServers.add(ServerName.valueOf(s.getHostname(), s.getPort(),
              ServerName.NON_STARTCODE));
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

  public FavoredNodesPlan getNewAssignmentPlan() throws IOException {
    // Get the current region assignment snapshot by scanning from the META
    SnapshotOfRegionAssignmentFromMeta assignmentSnapshot =
      this.getRegionAssignmentSnapshot();

    // Get the region locality map
    Map<String, Map<String, Float>> regionLocalityMap = null;
    if (this.enforceLocality) {
      regionLocalityMap = FSUtils.getRegionDegreeLocalityMappingFromFS(conf);
    }
    // Initialize the assignment plan
    FavoredNodesPlan plan = new FavoredNodesPlan();

    // Get the table to region mapping
    Map<TableName, List<HRegionInfo>> tableToRegionMap =
      assignmentSnapshot.getTableToRegionMap();
    LOG.info("Start to generate the new assignment plan for the " +
         + tableToRegionMap.keySet().size() + " tables" );
    for (TableName table : tableToRegionMap.keySet()) {
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
   * Print the assignment plan to the system output stream
   * @param plan
   */
  public static void printAssignmentPlan(FavoredNodesPlan plan) {
    if (plan == null) return;
    LOG.info("========== Start to print the assignment plan ================");
    // sort the map based on region info
    Map<HRegionInfo, List<ServerName>> assignmentMap =
      new TreeMap<HRegionInfo, List<ServerName>>(plan.getAssignmentMap());
    
    for (Map.Entry<HRegionInfo, List<ServerName>> entry : assignmentMap.entrySet()) {
      
      String serverList = FavoredNodeAssignmentHelper.getFavoredNodesAsString(entry.getValue());
      String regionName = entry.getKey().getRegionNameAsString();
      LOG.info("Region: " + regionName );
      LOG.info("Its favored nodes: " + serverList);
    }
    LOG.info("========== Finish to print the assignment plan ================");
  }

  /**
   * Update the assignment plan into hbase:meta
   * @param plan the assignments plan to be updated into hbase:meta
   * @throws IOException if cannot update assignment plan in hbase:meta
   */
  public void updateAssignmentPlanToMeta(FavoredNodesPlan plan)
  throws IOException {
    try {
      LOG.info("Start to update the hbase:meta with the new assignment plan");
      Map<HRegionInfo, List<ServerName>> assignmentMap =
        plan.getAssignmentMap();
      FavoredNodeAssignmentHelper.updateMetaWithFavoredNodesInfo(assignmentMap, conf);
      LOG.info("Updated the hbase:meta with the new assignment plan");
    } catch (Exception e) {
      LOG.error("Failed to update hbase:meta with the new assignment" +
          "plan because " + e.getMessage());
    }
  }

  /**
   * Update the assignment plan to all the region servers
   * @param plan
   * @throws IOException
   */
  private void updateAssignmentPlanToRegionServers(FavoredNodesPlan plan)
  throws IOException{
    LOG.info("Start to update the region servers with the new assignment plan");
    // Get the region to region server map
    Map<ServerName, List<HRegionInfo>> currentAssignment =
      this.getRegionAssignmentSnapshot().getRegionServerToRegionMap();
    HConnection connection = this.getHBaseAdmin().getConnection();

    // track of the failed and succeeded updates
    int succeededNum = 0;
    Map<ServerName, Exception> failedUpdateMap =
      new HashMap<ServerName, Exception>();

    for (Map.Entry<ServerName, List<HRegionInfo>> entry :
      currentAssignment.entrySet()) {
      List<Pair<HRegionInfo, List<ServerName>>> regionUpdateInfos =
          new ArrayList<Pair<HRegionInfo, List<ServerName>>>();
      try {
        // Keep track of the favored updates for the current region server
        FavoredNodesPlan singleServerPlan = null;
        // Find out all the updates for the current region server
        for (HRegionInfo region : entry.getValue()) {
          List<ServerName> favoredServerList = plan.getFavoredNodes(region);
          if (favoredServerList != null &&
              favoredServerList.size() == FavoredNodeAssignmentHelper.FAVORED_NODES_NUM) {
            // Create the single server plan if necessary
            if (singleServerPlan == null) {
              singleServerPlan = new FavoredNodesPlan();
            }
            // Update the single server update
            singleServerPlan.updateAssignmentPlan(region, favoredServerList);
            regionUpdateInfos.add(
              new Pair<HRegionInfo, List<ServerName>>(region, favoredServerList));
          }
        }
        if (singleServerPlan != null) {
          // Update the current region server with its updated favored nodes
          BlockingInterface currentRegionServer = connection.getAdmin(entry.getKey());
          UpdateFavoredNodesRequest request =
              RequestConverter.buildUpdateFavoredNodesRequest(regionUpdateInfos);
          
          UpdateFavoredNodesResponse updateFavoredNodesResponse =
              currentRegionServer.updateFavoredNodes(null, request);
          LOG.info("Region server " +
              ProtobufUtil.getServerInfo(currentRegionServer).getServerName() +
              " has updated " + updateFavoredNodesResponse.getResponse() + " / " +
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
      for (Map.Entry<ServerName, Exception> entry :
        failedUpdateMap.entrySet() ) {
        LOG.error("Failed to update " + entry.getKey().getHostAndPort() +
            " because of " + entry.getValue().getMessage());
      }
    }
  }

  public void updateAssignmentPlan(FavoredNodesPlan plan)
      throws IOException {
    LOG.info("Start to update the new assignment plan for the hbase:meta table and" +
        " the region servers");
    // Update the new assignment plan to META
    updateAssignmentPlanToMeta(plan);
    // Update the new assignment plan to Region Servers
    updateAssignmentPlanToRegionServers(plan);
    LOG.info("Finish to update the new assignment plan for the hbase:meta table and" +
        " the region servers");
  }

  /**
   * Return how many regions will move per table since their primary RS will
   * change
   *
   * @param newPlan - new AssignmentPlan
   * @return how many primaries will move per table
   */
  public Map<TableName, Integer> getRegionsMovement(FavoredNodesPlan newPlan)
      throws IOException {
    Map<TableName, Integer> movesPerTable = new HashMap<TableName, Integer>();
    SnapshotOfRegionAssignmentFromMeta snapshot = this.getRegionAssignmentSnapshot();
    Map<TableName, List<HRegionInfo>> tableToRegions = snapshot
        .getTableToRegionMap();
    FavoredNodesPlan oldPlan = snapshot.getExistingAssignmentPlan();
    Set<TableName> tables = snapshot.getTableSet();
    for (TableName table : tables) {
      int movedPrimaries = 0;
      if (!this.targetTableSet.isEmpty()
          && !this.targetTableSet.contains(table)) {
        continue;
      }
      List<HRegionInfo> regions = tableToRegions.get(table);
      for (HRegionInfo region : regions) {
        List<ServerName> oldServers = oldPlan.getFavoredNodes(region);
        List<ServerName> newServers = newPlan.getFavoredNodes(region);
        if (oldServers != null && newServers != null) {
          ServerName oldPrimary = oldServers.get(0);
          ServerName newPrimary = newServers.get(0);
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
   * @throws IOException
   */
  public void checkDifferencesWithOldPlan(Map<TableName, Integer> movesPerTable,
      Map<String, Map<String, Float>> regionLocalityMap, FavoredNodesPlan newPlan)
          throws IOException {
    // localities for primary, secondary and tertiary
    SnapshotOfRegionAssignmentFromMeta snapshot = this.getRegionAssignmentSnapshot();
    FavoredNodesPlan oldPlan = snapshot.getExistingAssignmentPlan();
    Set<TableName> tables = snapshot.getTableSet();
    Map<TableName, List<HRegionInfo>> tableToRegionsMap = snapshot.getTableToRegionMap();
    for (TableName table : tables) {
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
        List<ServerName> oldServers = oldPlan.getFavoredNodes(currentRegion);
        List<ServerName> newServers = newPlan.getFavoredNodes(currentRegion);
        if (newServers != null && oldServers != null) {
          int i=0;
          for (FavoredNodesPlan.Position p : FavoredNodesPlan.Position.values()) {
            ServerName newServer = newServers.get(p.ordinal());
            ServerName oldServer = oldServers.get(p.ordinal());
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

  public void printDispersionScores(TableName table,
      SnapshotOfRegionAssignmentFromMeta snapshot, int numRegions, FavoredNodesPlan newPlan,
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

  public void printLocalityAndDispersionForCurrentPlan(
      Map<String, Map<String, Float>> regionLocalityMap) throws IOException {
    SnapshotOfRegionAssignmentFromMeta snapshot = this.getRegionAssignmentSnapshot();
    FavoredNodesPlan assignmentPlan = snapshot.getExistingAssignmentPlan();
    Set<TableName> tables = snapshot.getTableSet();
    Map<TableName, List<HRegionInfo>> tableToRegionsMap = snapshot
        .getTableToRegionMap();
    for (TableName table : tables) {
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
        List<ServerName> servers = assignmentPlan.getFavoredNodes(currentRegion);
        if (servers != null) {
          int i = 0;
          for (FavoredNodesPlan.Position p : FavoredNodesPlan.Position.values()) {
            ServerName server = servers.get(p.ordinal());
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
   * @param favoredNodesStr The String of favored nodes
   * @return the list of ServerName for the byte array of favored nodes.
   */
  public static List<ServerName> getFavoredNodeList(String favoredNodesStr) {
    String[] favoredNodesArray = StringUtils.split(favoredNodesStr, ",");
    if (favoredNodesArray == null)
      return null;

    List<ServerName> serverList = new ArrayList<ServerName>();
    for (String hostNameAndPort : favoredNodesArray) {
      serverList.add(ServerName.valueOf(hostNameAndPort, ServerName.NON_STARTCODE));
    }
    return serverList;
  }

  public static void main(String args[]) throws IOException {
    Options opt = new Options();
    opt.addOption("w", "write", false, "write the assignments to hbase:meta only");
    opt.addOption("u", "update", false,
        "update the assignments to hbase:meta and RegionServers together");
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

    opt.addOption("tables", true,
        "The list of table names splitted by ',' ;" +
        "For example: -tables: t1,t2,...,tn");
    opt.addOption("l", "locality", true, "enforce the maxium locality");
    opt.addOption("m", "min-move", true, "enforce minium assignment move");
    opt.addOption("diff", false, "calculate difference between assignment plans");
    opt.addOption("munkres", false,
        "use munkres to place secondaries and tertiaries");
    opt.addOption("ld", "locality-dispersion", false, "print locality and dispersion " +
    		"information for current plan");
    try {
      // Set the log4j
      Logger.getLogger("org.apache.zookeeper").setLevel(Level.ERROR);
      Logger.getLogger("org.apache.hadoop.hbase").setLevel(Level.ERROR);
      Logger.getLogger("org.apache.hadoop.hbase.master.RegionPlacementMaintainer")
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
      RegionPlacementMaintainer rp = new RegionPlacementMaintainer(conf, enforceLocality,
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
        // Generate the assignment plan only without updating the hbase:meta and RS
        FavoredNodesPlan plan = rp.getNewAssignmentPlan();
        printAssignmentPlan(plan);
      } else if (cmd.hasOption("w") || cmd.hasOption("write")) {
        // Generate the new assignment plan
        FavoredNodesPlan plan = rp.getNewAssignmentPlan();
        // Print the new assignment plan
        printAssignmentPlan(plan);
        // Write the new assignment plan to META
        rp.updateAssignmentPlanToMeta(plan);
      } else if (cmd.hasOption("u") || cmd.hasOption("update")) {
        // Generate the new assignment plan
        FavoredNodesPlan plan = rp.getNewAssignmentPlan();
        // Print the new assignment plan
        printAssignmentPlan(plan);
        // Update the assignment to hbase:meta and Region Servers
        rp.updateAssignmentPlan(plan);
      } else if (cmd.hasOption("diff")) {
        FavoredNodesPlan newPlan = rp.getNewAssignmentPlan();
        Map<String, Map<String, Float>> locality = FSUtils
            .getRegionDegreeLocalityMappingFromFS(conf);
        Map<TableName, Integer> movesPerTable = rp.getRegionsMovement(newPlan);
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
        FavoredNodesPlan plan = rp.getRegionAssignmentSnapshot().getExistingAssignmentPlan();
        printAssignmentPlan(plan);
      } else if (cmd.hasOption("overwrite")) {
        if (!cmd.hasOption("f") || !cmd.hasOption("r")) {
          throw new IllegalArgumentException("Please specify: " +
              " -update -r regionName -f server1:port,server2:port,server3:port");
        }

        String regionName = cmd.getOptionValue("r");
        String favoredNodesStr = cmd.getOptionValue("f");
        LOG.info("Going to update the region " + regionName + " with the new favored nodes " +
            favoredNodesStr);
        List<ServerName> favoredNodes = null;
        HRegionInfo regionInfo =
            rp.getRegionAssignmentSnapshot().getRegionNameToRegionInfoMap().get(regionName);
        if (regionInfo == null) {
          LOG.error("Cannot find the region " + regionName + " from the META");
        } else {
          try {
            favoredNodes = getFavoredNodeList(favoredNodesStr);
          } catch (IllegalArgumentException e) {
            LOG.error("Cannot parse the invalid favored nodes because " + e);
          }
          FavoredNodesPlan newPlan = new FavoredNodesPlan();
          newPlan.updateAssignmentPlan(regionInfo, favoredNodes);
          rp.updateAssignmentPlan(newPlan);
        }
      } else {
        printHelp(opt);
      }
    } catch (ParseException e) {
      printHelp(opt);
    }
  }
}
