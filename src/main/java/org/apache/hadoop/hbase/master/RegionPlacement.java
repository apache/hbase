package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.master.AssignmentPlan.POSITION;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.MunkresAssignment;
import org.apache.hadoop.hbase.util.RackManager;
import org.apache.hadoop.hbase.util.Writables;
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
  private RackManager rackManager;
  private final boolean enforceLocality;
  private final boolean enforceMinAssignmentMove;
  private HBaseAdmin admin;

  public RegionPlacement(Configuration conf)
  throws IOException {
    this(conf, true, true);
  }

  public RegionPlacement(Configuration conf, boolean enforceLocality,
      boolean enforceMinAssignmentMove)
  throws IOException {
    this.conf = conf;
    rackManager = new RackManager(conf);
    this.enforceLocality = enforceLocality;
    this.enforceMinAssignmentMove = enforceMinAssignmentMove;
  }

  @Override
  public AssignmentPlan getAssignmentPlan(final HRegionInfo[] regions)
  throws IOException{
    Map<HRegionInfo, HServerAddress> currentAssignment =
      this.getCurrentRegionToRSAssignment();
    for (HRegionInfo newRegion : regions) {
      // There is no currently hosting region server for new created regions.
      currentAssignment.put(newRegion, null);
    }
    // Get the all online region servers
    List<HServerInfo> servers =
      new ArrayList<HServerInfo>(this.getHBaseAdmin().
          getClusterStatus().getServerInfo());

    return getAssignmentPlanForAllRegions(currentAssignment,
        null, servers);
  }

  @Override
  public AssignmentPlan getAssignmentPlan() throws IOException {
    // Get the current assignment
    Map<HRegionInfo, HServerAddress> currentAssignment =
      this.getCurrentRegionToRSAssignment();
    // Get the cached locality map
    Map<String, Map<String, Float>> regionLocalityMap =
      FSUtils.getRegionDegreeLocalityMappingFromFS(conf);
    // Get the all online region servers
    List<HServerInfo> servers =
      new ArrayList<HServerInfo>(this.getHBaseAdmin()
          .getClusterStatus().getServerInfo());

    // Get the assignment plan for the new regions
    return this.getAssignmentPlanForAllRegions(
          currentAssignment, regionLocalityMap, servers);
  }

  @Override
  public void updateAssignmentPlan(AssignmentPlan plan)
  throws IOException {
    // Update the new assignment plan to META
    updateAssignmentPlanToMeta(plan);
    // Update the new assignment plan to Region Servers
    updateAssignmentPlanToRegionServers(plan);
  }

  /**
   * Update the assignment plan into .META.
   * @param plan the assignments plan to be updated into .META.
   * @throws IOException if cannot update assignment plan in .META.
   */
  private void updateAssignmentPlanToMeta(AssignmentPlan plan)
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
    // Get the current assignment
    Map<HServerAddress, List<HRegionInfo>> currentAssignment =
      this.getCurrentRSToRegionAssignment();
    HConnection connection = this.getHBaseAdmin().getConnection();

    // track of the failed and succeeded updates
    int succeededNum = 0;
    Map<HServerAddress, Exception> failedUpdateMap =
      new HashMap<HServerAddress, Exception>();

    for (Map.Entry<HServerAddress, List<HRegionInfo>> entry :
      currentAssignment.entrySet()) {
      try {
        // Keep track of the favored updates for the current region server
        AssignmentPlan singleServerPlan = new AssignmentPlan();

        // Find out all the updates for the current region server
        for (HRegionInfo region : entry.getValue()) {
          List<HServerAddress> favoredServerList = plan.getAssignment(region);
          singleServerPlan.updateAssignmentPlan(region, favoredServerList);
        }

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
      } catch (Exception e) {
        failedUpdateMap.put(entry.getKey(), e);
      }
    }
    // log the succeeded updates
    LOG.info("Updated " + succeededNum + " the region servers with " +
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
   * Check whether regions are assigned to servers consistent with the explicit
   * hints that are persisted in the META table.
   * Also keep track of the number of the regions are assigned to the
   * primary region server.
   * @return the number of regions are assigned to the primary region server
   * @throws IOException
   */
  public int getNumRegionisOnPrimaryRS() throws IOException {
    final AtomicInteger regionOnPrimaryNum = new AtomicInteger(0);
    final AtomicInteger totalRegionNum = new AtomicInteger(0);
    LOG.info("The start of region placement verification");
    MetaScannerVisitor visitor = new MetaScannerVisitor() {
      public boolean processRow(Result result) throws IOException {
        try {
          byte[] regionInfo = result.getValue(HConstants.CATALOG_FAMILY,
              HConstants.REGIONINFO_QUALIFIER);
          byte[] server = result.getValue(HConstants.CATALOG_FAMILY,
              HConstants.SERVER_QUALIFIER);
          byte[] favoredNodes = result.getValue(HConstants.CATALOG_FAMILY,
              "favorednodes".getBytes());
          POSITION[] positions = AssignmentPlan.POSITION.values();
          if (regionInfo != null) {
            HRegionInfo info = Writables.getHRegionInfo(regionInfo);
            totalRegionNum.incrementAndGet();
            if (server != null) {
              String serverString = new String(server);
              if (favoredNodes != null) {
                String[] splits = new String(favoredNodes).split(",");
                String placement = "[NOT FAVORED NODE]";
                for (int i = 0; i < splits.length; i++) {
                  if (splits[i].equals(serverString)) {
                    placement = positions[i].toString();
                    if (i == AssignmentPlan.POSITION.PRIMARY.ordinal()) {
                      regionOnPrimaryNum.incrementAndGet();
                    }
                    break;
                  }
                }
                LOG.info(info.getRegionNameAsString() + " on " +
                    serverString + " " + placement);
              } else {
                LOG.info(info.getRegionNameAsString() + " running on " +
                    serverString + " but there is no favored region server");
              }
            } else {
              LOG.info(info.getRegionNameAsString() +
                  " not assigned to any server");
            }
          }
          return true;
        } catch (RuntimeException e) {
          LOG.error("Result=" + result);
          throw e;
        }
      }
    };
    MetaScanner.metaScan(conf, visitor);
    LOG.info("There are " + regionOnPrimaryNum.intValue() + " out of " +
        totalRegionNum.intValue() + " regions running on the primary" +
			" region servers" );
    return regionOnPrimaryNum.intValue() ;
  }

  public AssignmentPlan getAssignmentPlanFromMeta() throws IOException {
    // Get the assignment Plan from scanning the META
    LOG.info("Start to scan the META for the current assignment plan");
    final AssignmentPlan plan = new AssignmentPlan();
    MetaScannerVisitor visitor = new MetaScannerVisitor() {
      public boolean processRow(Result result) throws IOException {
        try {
          byte[] regionInfo = result.getValue(HConstants.CATALOG_FAMILY,
              HConstants.REGIONINFO_QUALIFIER);
          byte[] favoredNodes = result.getValue(HConstants.CATALOG_FAMILY,
              HConstants.FAVOREDNODES_QUALIFIER);
          HRegionInfo info = Writables.getHRegionInfo(regionInfo);
          if (info == null || favoredNodes == null)
            return true;

          List<HServerAddress> favoredServerList =
            RegionPlacement.getFavoredNodesList(favoredNodes);
          plan.updateAssignmentPlan(info, favoredServerList);
          return true;
        } catch (RuntimeException e) {
          LOG.error("Catche remote exception " + e.getMessage() +
              " when processing" + result);
          throw e;
        }
      }
    };
    // Scan .META. to pick up user regions
    MetaScanner.metaScan(conf, visitor);
    LOG.info("Finished to scan the META for the current assignment plan");
    return plan;
  }

  /**
   * Verify the region placement is consistent with the assignment plan;
   * @throws IOException
   */
  public void verifyRegionPlacement() throws IOException {
    // TODO: need more dimensions to verify the region placement.
    // Currently it checks whether each region is running the primary rs.
    this.getNumRegionisOnPrimaryRS();
  }
  /**
   * @param serverList
   * @return string the favoredNodes generated by the server list.
   */
  public static String getFavoredNodes(List<HServerAddress> serverAddrList) {
    String favoredNodes = "";
    for (int i = 0 ; i < serverAddrList.size(); i++) {
      favoredNodes += serverAddrList.get(i).getHostNameWithPort();
      if (i != serverAddrList.size() - 1 ) {
        favoredNodes += ",";
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
    LOG.info("Start to print the assignment plan:");
    Map<HRegionInfo, List<HServerAddress>> assignmentMap =
      plan.getAssignmentMap();

    for (Map.Entry<HRegionInfo, List<HServerAddress>> entry :
      assignmentMap.entrySet()) {
      String serverList = RegionPlacement.getFavoredNodes(entry.getValue());
      String regionName = entry.getKey().getRegionNameAsString();
      LOG.info("Region: " + regionName + " Favored Nodes: " + serverList);
    }
    LOG.info("Finished to print the assignment plan");
  }

  public static void main(String[] args) throws IOException,
      InterruptedException {
    Options opt = new Options();
    opt.addOption("w", "write", false,
        "write assignments to META and update RegionServers");
    opt.addOption("n", "dry-run", false, "do not write assignments to META");
    opt.addOption("v", "verify", false, "verify current assignments against META");
    opt.addOption("p", "print", false, "print the current assignment plan in META");
    opt.addOption("h", "help", false, "print usage");
    opt.addOption("l", "enforce-locality-assignment", true,
        "enforce locality assignment");
    opt.addOption("m", "enforce-min-assignment-move", true,
        "enforce minium assignment move");

    try {
      Logger.getLogger("org.apache.zookeeper").setLevel(Level.ERROR);
      Logger.getLogger("org.apache.hadoop.hbase").setLevel(Level.ERROR);
      Logger.getLogger("org.apache.hadoop.hbase.master.RegionPlacement").
        setLevel(Level.INFO);

      CommandLine cmd = new GnuParser().parse(opt, args);
      Configuration conf = HBaseConfiguration.create();
      boolean enforceMinAssignmentMove = cmd.hasOption("m") ||
        cmd.hasOption("enforce-min-assignment-move");
      boolean enforceLocality = cmd.hasOption("l") ||
        cmd.hasOption("enforce-locality-assignment");

      RegionPlacement rp = new RegionPlacement(conf, enforceLocality,
          enforceMinAssignmentMove);

      if (cmd.hasOption("v") || cmd.hasOption("verify")) {
        // Verify the region placement.
        rp.verifyRegionPlacement();

      } else if (cmd.hasOption("n") || cmd.hasOption("dry-run")) {
        // Generate the assignment plan only without updating the META and RS
        AssignmentPlan plan = rp.getAssignmentPlan();
        RegionPlacement.printAssignmentPlan(plan);
      } else if (cmd.hasOption("w") || cmd.hasOption("write")) {
        // Generate the new assignment plan
        AssignmentPlan plan = rp.getAssignmentPlan();
        RegionPlacement.printAssignmentPlan(plan);
        // Update the assignment to META and Region Servers
        // TODO: The client may update any customized plan to META/RegionServers
        rp.updateAssignmentPlan(plan);
      } else if (cmd.hasOption("p") || cmd.hasOption("print")) {
        AssignmentPlan plan = rp.getAssignmentPlanFromMeta();
        RegionPlacement.printAssignmentPlan(plan);
      } else {
        printHelp(opt);
      }
    } catch (ParseException e) {
      printHelp(opt);
    }
  }

  private static void printHelp(Options opt) {
    new HelpFormatter().printHelp(
        "RegionPlacement < -w | -n | -v | -t | -h > [-l] | [-m]", opt);
  }

  private HBaseAdmin getHBaseAdmin() throws IOException {
    if (this.admin == null) {
      this.admin = new HBaseAdmin(this.conf);
    }
    return this.admin;
  }
  /**
   * Get the Region to RegionServer assignment mapping from .META.
   * @return map of the regions to region servers from .META.
   * @throws IOException
   */
  private Map<HRegionInfo, HServerAddress> getCurrentRegionToRSAssignment()
  throws IOException {
    LOG.info("Start to scan the META for the current online regions to" +
		"region servers assignment");
    final Map<HRegionInfo, HServerAddress> regionsToRegionServers =
        new HashMap<HRegionInfo, HServerAddress>();
    MetaScannerVisitor visitor = new MetaScannerVisitor() {
      public boolean processRow(Result result) throws IOException {
        try {
          byte[] regionInfo = result.getValue(HConstants.CATALOG_FAMILY,
              HConstants.REGIONINFO_QUALIFIER);
          byte[] server = result.getValue(HConstants.CATALOG_FAMILY,
              HConstants.SERVER_QUALIFIER);
          if (regionInfo != null) {
            if (server != null) {
              regionsToRegionServers.put(Writables.getHRegionInfo(regionInfo),
                  new HServerAddress(Bytes.toString(server)));
            } else {
              regionsToRegionServers.put(Writables.getHRegionInfo(regionInfo),
                  new HServerAddress());
            }
          }
          return true;
        } catch (RuntimeException e) {
          LOG.error("Catche remote exception " + e.getMessage() +
              " when processing" + result);
          throw e;
        }
      }
    };

    // Scan .META. to pick up user regions
    MetaScanner.metaScan(conf, visitor);
    LOG.info("Finsished to scan the META for the current " +
        regionsToRegionServers.size() + " online regions to" +
    "region servers assignment");
    return regionsToRegionServers;
  }

  /**
   * Get the RegionServer to Region assignment mapping from .META.
   * @return map of the region servers to regions from .META.
   * @throws IOException
   */
  private Map<HServerAddress, List<HRegionInfo>> getCurrentRSToRegionAssignment()
  throws IOException {
    LOG.info("Start to scan the META for the current region servers to " +
		"regions assignments");
    final Map<HServerAddress, List<HRegionInfo>> regionServersToRegions =
        new HashMap<HServerAddress, List<HRegionInfo>>();

    MetaScannerVisitor visitor = new MetaScannerVisitor() {
      public boolean processRow(Result result) throws IOException {
        try {
          byte[] regionInfo = result.getValue(HConstants.CATALOG_FAMILY,
              HConstants.REGIONINFO_QUALIFIER);
          byte[] server = result.getValue(HConstants.CATALOG_FAMILY,
              HConstants.SERVER_QUALIFIER);
          if (regionInfo != null && server != null) {
            // Get the server address and region info
            HServerAddress serverAddress =
              new HServerAddress(Bytes.toString(server));
            HRegionInfo info = Writables.getHRegionInfo(regionInfo);

            // Update the regionServersToRegions mapping
            List<HRegionInfo> regionInfoList =
              regionServersToRegions.get(serverAddress);
            if (regionInfoList == null) {
              regionInfoList = new ArrayList<HRegionInfo>();
              regionServersToRegions.put(serverAddress, regionInfoList);
            }
            regionInfoList.add(info);
          }
          return true;
        } catch (RuntimeException e) {
          LOG.error("Catche remote exception " + e.getMessage() +
              " when processing" + result);
          throw e;
        }
      }
    };
    // Scan .META. to pick up user regions
    MetaScanner.metaScan(conf, visitor);
    LOG.info("Finished to scan the META for the current " +
      regionServersToRegions.size() + " region servers to regions assignments");
    return regionServersToRegions;
  }

  private AssignmentPlan getAssignmentPlanForAllRegions(
      Map<HRegionInfo, HServerAddress> currentAssignmentMap,
      Map<String, Map<String, Float>> regionLocalityMap,
      List<HServerInfo> servers)
      throws IOException {
    // Each server may serve multiple regions. Assume that each server has equal
    // capacity in terms of the number of regions that may be served.
    List<HRegionInfo> regions =
      new ArrayList<HRegionInfo>(currentAssignmentMap.keySet());
    int numRegions = regions.size();
    LOG.info("Start to generate assignment plan for the " + numRegions +
        " regions and " + servers.size() + " region servers");
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
          HServerAddress currentAddress = currentAssignmentMap.get(regions.get(i));
          if (currentAddress != null && !currentAddress.equals(
              servers.get(j).getServerAddress())) {
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

    AssignmentPlan plan = new AssignmentPlan();
    for (int i = 0; i < numRegions; i++) {
      List<HServerAddress> favoredServers =
        new ArrayList<HServerAddress>(HConstants.FAVORED_NODES_NUM);
      favoredServers.add(servers.get(primaryAssignment[i] / slotsPerServer).
          getServerAddress());
      favoredServers.add(servers.get(secondaryAssignment[i] / slotsPerServer).
          getServerAddress());
      favoredServers.add(servers.get(tertiaryAssignment[i] / slotsPerServer).
          getServerAddress());

      plan.updateAssignmentPlan(regions.get(i), favoredServers);
    }
    LOG.info("Generated assignment plan for the " + numRegions +
        " regions and " + servers.size() + " region servers");
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
}
