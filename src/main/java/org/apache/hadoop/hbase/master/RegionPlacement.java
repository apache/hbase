package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.MunkresAssignment;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

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
      Map<HRegionInfo, HServerAddress> primaryRSMap =
        this.placePrimaryRSAsRoundRobin(regions, domain);

      // Place the secondary and tertiary region server
      Map<HRegionInfo, Pair<HServerAddress, HServerAddress>>
        secondaryAndTertiaryRSMap =
        this.placeSecondaryAndTertiaryRS(primaryRSMap, domain);

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
   * Place the primary region server in the round robin way.
   * @param regions
   * @param domain
   * @return the map between regions and its primary region server
   * @throws IOException
   */
  private Map<HRegionInfo, HServerAddress> placePrimaryRSAsRoundRobin(
      HRegionInfo[] regions, AssignmentDomain domain) throws IOException {

    // Get the rack to region server map from the assignment domain
    Map<String, List<HServerAddress>> rackToRegionServerMap=
      domain.getRackToRegionServerMap();

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

  /**
   * Generate the assignment plan for the existing table
   * @param tableName
   * @param assignmentSnapshot
   * @param regionLocalityMap
   * @param plan
   * @throws IOException
   */
  private void genAssignmentPlan(String tableName,
      RegionAssignmentSnapshot assignmentSnapshot,
      Map<String, Map<String, Float>> regionLocalityMap,
      AssignmentPlan plan) throws IOException {
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
        genAssignmentPlan(table, assignmentSnapshot, regionLocalityMap, plan);
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
          int updatedRegionNum =
            currentRegionServer.updateFavoredNodes(singleServerPlan);
          LOG.info("Region server " +
              currentRegionServer.getHServerInfo().getHostnamePort() +
              " has updated " + updatedRegionNum + " / " +
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
    if (serverList == null || serverList.size() == 0)
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

    opt.addOption("tables", true,
        "The list of table names splitted by ',' ;" +
        "For example: -tables: t1,t2,...,tn");
    opt.addOption("l", "locality", true, "enforce the maxium locality");
    opt.addOption("m", "min-move", true, "enforce minium assignment move");
    
    try {
      // Set the log4j
      Logger.getLogger("org.apache.zookeeper").setLevel(Level.ERROR);
      Logger.getLogger("org.apache.hadoop.hbase").setLevel(Level.ERROR);
      Logger.getLogger("org.apache.hadoop.hbase.master.RegionPlacement").
        setLevel(Level.INFO);

      CommandLine cmd = new GnuParser().parse(opt, args);
      Configuration conf = HBaseConfiguration.create();

      boolean enforceMinAssignmentMove = true;
      boolean enforceLocality = true;
      boolean verificationDetails =false;

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
      }  else if (cmd.hasOption("u") || cmd.hasOption("update")) {
        // Generate the new assignment plan
        AssignmentPlan plan = rp.getNewAssignmentPlan();
        // Print the new assignment plan
        RegionPlacement.printAssignmentPlan(plan);
        // Update the assignment to META and Region Servers
        rp.updateAssignmentPlan(plan);
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
      }else {
        printHelp(opt);
      }
    } catch (ParseException e) {
      printHelp(opt);
    }
  }

  private static void printHelp(Options opt) {
    new HelpFormatter().printHelp(
        "RegionPlacement < -w | -u | -n | -v | -t | -h | -overwrite -r regionName -f favoredNodes >" +
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
}
