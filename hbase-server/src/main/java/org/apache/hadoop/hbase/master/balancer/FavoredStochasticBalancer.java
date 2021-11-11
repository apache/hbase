/*
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

import static org.apache.hadoop.hbase.ServerName.NON_STARTCODE;
import static org.apache.hadoop.hbase.favored.FavoredNodeAssignmentHelper.FAVORED_NODES_NUM;
import static org.apache.hadoop.hbase.favored.FavoredNodesPlan.Position.PRIMARY;
import static org.apache.hadoop.hbase.favored.FavoredNodesPlan.Position.SECONDARY;
import static org.apache.hadoop.hbase.favored.FavoredNodesPlan.Position.TERTIARY;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.favored.FavoredNodeAssignmentHelper;
import org.apache.hadoop.hbase.favored.FavoredNodesManager;
import org.apache.hadoop.hbase.favored.FavoredNodesPlan;
import org.apache.hadoop.hbase.favored.FavoredNodesPlan.Position;
import org.apache.hadoop.hbase.favored.FavoredNodesPromoter;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

/**
 * An implementation of the {@link org.apache.hadoop.hbase.master.LoadBalancer} that
 * assigns favored nodes for each region. There is a Primary RegionServer that hosts
 * the region, and then there is Secondary and Tertiary RegionServers. Currently, the
 * favored nodes information is used in creating HDFS files - the Primary RegionServer
 * passes the primary, secondary, tertiary node addresses as hints to the
 * DistributedFileSystem API for creating files on the filesystem. These nodes are
 * treated as hints by the HDFS to place the blocks of the file. This alleviates the
 * problem to do with reading from remote nodes (since we can make the Secondary
 * RegionServer as the new Primary RegionServer) after a region is recovered. This
 * should help provide consistent read latencies for the regions even when their
 * primary region servers die. This provides two
 * {@link CandidateGenerator}
 *
 */
@InterfaceAudience.Private
public class FavoredStochasticBalancer extends StochasticLoadBalancer implements
    FavoredNodesPromoter {

  private static final Logger LOG = LoggerFactory.getLogger(FavoredStochasticBalancer.class);
  private FavoredNodesManager fnm;

  @Override
  public void initialize() throws HBaseIOException {
    configureGenerators();
    super.initialize();
  }

  protected void configureGenerators() {
    List<CandidateGenerator> fnPickers = new ArrayList<>(2);
    fnPickers.add(new FavoredNodeLoadPicker());
    fnPickers.add(new FavoredNodeLocalityPicker());
    setCandidateGenerators(fnPickers);
  }

  /**
   * @return any candidate generator in random
   */
  @Override
  protected CandidateGenerator getRandomGenerator() {
    return candidateGenerators.get(RANDOM.nextInt(candidateGenerators.size()));
  }

  @Override
  public synchronized void setMasterServices(MasterServices masterServices) {
    super.setMasterServices(masterServices);
    fnm = masterServices.getFavoredNodesManager();
  }

  /*
   * Round robin assignment: Segregate the regions into two types:
   *
   * 1. The regions that have favored node assignment where at least one of the favored node
   * is still alive. In this case, try to adhere to the current favored nodes assignment as
   * much as possible - i.e., if the current primary is gone, then make the secondary or
   * tertiary as the new host for the region (based on their current load). Note that we don't
   * change the favored node assignments here (even though one or more favored node is
   * currently down). That will be done by the admin operations.
   *
   * 2. The regions that currently don't have favored node assignments. Generate favored nodes
   * for them and then assign. Generate the primary fn in round robin fashion and generate
   * secondary and tertiary as per favored nodes constraints.
   */
  @Override
  @NonNull
  public Map<ServerName, List<RegionInfo>> roundRobinAssignment(List<RegionInfo> regions,
      List<ServerName> servers) throws HBaseIOException {

    metricsBalancer.incrMiscInvocations();

    Set<RegionInfo> regionSet = Sets.newHashSet(regions);
    Map<ServerName, List<RegionInfo>> assignmentMap = assignMasterSystemRegions(regions, servers);
    if (!assignmentMap.isEmpty()) {
      servers = new ArrayList<>(servers);
      // Guarantee not to put other regions on master
      servers.remove(masterServerName);
      List<RegionInfo> masterRegions = assignmentMap.get(masterServerName);
      if (!masterRegions.isEmpty()) {
        for (RegionInfo region: masterRegions) {
          regionSet.remove(region);
        }
      }
    }

    if (regionSet.isEmpty()) {
      return assignmentMap;
    }

    try {
      FavoredNodeAssignmentHelper helper =
          new FavoredNodeAssignmentHelper(servers, fnm.getRackManager());
      helper.initialize();

      Set<RegionInfo> systemRegions = FavoredNodesManager.filterNonFNApplicableRegions(regionSet);
      regionSet.removeAll(systemRegions);

      // Assign all system regions
      Map<ServerName, List<RegionInfo>> systemAssignments =
        super.roundRobinAssignment(Lists.newArrayList(systemRegions), servers);

      // Segregate favored and non-favored nodes regions and assign accordingly.
      Pair<Map<ServerName,List<RegionInfo>>, List<RegionInfo>> segregatedRegions =
        segregateRegionsAndAssignRegionsWithFavoredNodes(regionSet, servers);
      Map<ServerName, List<RegionInfo>> regionsWithFavoredNodesMap = segregatedRegions.getFirst();
      Map<ServerName, List<RegionInfo>> regionsWithoutFN =
        generateFNForRegionsWithoutFN(helper, segregatedRegions.getSecond());

      // merge the assignment maps
      mergeAssignmentMaps(assignmentMap, systemAssignments);
      mergeAssignmentMaps(assignmentMap, regionsWithFavoredNodesMap);
      mergeAssignmentMaps(assignmentMap, regionsWithoutFN);

    } catch (Exception ex) {
      throw new HBaseIOException("Encountered exception while doing favored-nodes assignment "
        + ex + " Falling back to regular assignment", ex);
    }
    return assignmentMap;
  }

  private void mergeAssignmentMaps(Map<ServerName, List<RegionInfo>> assignmentMap,
      Map<ServerName, List<RegionInfo>> otherAssignments) {

    if (otherAssignments == null || otherAssignments.isEmpty()) {
      return;
    }

    for (Entry<ServerName, List<RegionInfo>> entry : otherAssignments.entrySet()) {
      ServerName sn = entry.getKey();
      List<RegionInfo> regionsList = entry.getValue();
      if (assignmentMap.get(sn) == null) {
        assignmentMap.put(sn, Lists.newArrayList(regionsList));
      } else {
        assignmentMap.get(sn).addAll(regionsList);
      }
    }
  }

  private Map<ServerName, List<RegionInfo>> generateFNForRegionsWithoutFN(
      FavoredNodeAssignmentHelper helper, List<RegionInfo> regions) throws IOException {

    Map<ServerName, List<RegionInfo>> assignmentMap = Maps.newHashMap();
    Map<RegionInfo, List<ServerName>> regionsNoFNMap;

    if (regions.size() > 0) {
      regionsNoFNMap = helper.generateFavoredNodesRoundRobin(assignmentMap, regions);
      fnm.updateFavoredNodes(regionsNoFNMap);
    }
    return assignmentMap;
  }

  /*
   * Return a pair - one with assignments when favored nodes are present and another with regions
   * without favored nodes.
   */
  private Pair<Map<ServerName, List<RegionInfo>>, List<RegionInfo>>
  segregateRegionsAndAssignRegionsWithFavoredNodes(Collection<RegionInfo> regions,
      List<ServerName> onlineServers) throws HBaseIOException {

    // Since we expect FN to be present most of the time, lets create map with same size
    Map<ServerName, List<RegionInfo>> assignmentMapForFavoredNodes =
        new HashMap<>(onlineServers.size());
    List<RegionInfo> regionsWithNoFavoredNodes = new ArrayList<>();

    for (RegionInfo region : regions) {
      List<ServerName> favoredNodes = fnm.getFavoredNodes(region);
      ServerName primaryHost = null;
      ServerName secondaryHost = null;
      ServerName tertiaryHost = null;

      if (favoredNodes != null && !favoredNodes.isEmpty()) {
        for (ServerName s : favoredNodes) {
          ServerName serverWithLegitStartCode = getServerFromFavoredNode(onlineServers, s);
          if (serverWithLegitStartCode != null) {
            FavoredNodesPlan.Position position =
                FavoredNodesPlan.getFavoredServerPosition(favoredNodes, s);
            if (Position.PRIMARY.equals(position)) {
              primaryHost = serverWithLegitStartCode;
            } else if (Position.SECONDARY.equals(position)) {
              secondaryHost = serverWithLegitStartCode;
            } else if (Position.TERTIARY.equals(position)) {
              tertiaryHost = serverWithLegitStartCode;
            }
          }
        }
        assignRegionToAvailableFavoredNode(assignmentMapForFavoredNodes, region, primaryHost,
            secondaryHost, tertiaryHost);
      } else {
        regionsWithNoFavoredNodes.add(region);
      }
    }
    return new Pair<>(assignmentMapForFavoredNodes, regionsWithNoFavoredNodes);
  }

  private void addRegionToMap(Map<ServerName, List<RegionInfo>> assignmentMapForFavoredNodes,
      RegionInfo region, ServerName host) {

    List<RegionInfo> regionsOnServer;
    if ((regionsOnServer = assignmentMapForFavoredNodes.get(host)) == null) {
      regionsOnServer = Lists.newArrayList();
      assignmentMapForFavoredNodes.put(host, regionsOnServer);
    }
    regionsOnServer.add(region);
  }

  /*
   * Get the ServerName for the FavoredNode. Since FN's startcode is -1, we could want to get the
   * ServerName with the correct start code from the list of provided servers.
   */
  private ServerName getServerFromFavoredNode(List<ServerName> servers, ServerName fn) {
    for (ServerName server : servers) {
      if (ServerName.isSameAddress(fn, server)) {
        return server;
      }
    }
    return null;
  }

  /*
   * Assign the region to primary if its available. If both secondary and tertiary are available,
   * assign to the host which has less load. Else assign to secondary or tertiary whichever is
   * available (in that order).
   */
  private void assignRegionToAvailableFavoredNode(
      Map<ServerName, List<RegionInfo>> assignmentMapForFavoredNodes, RegionInfo region,
      ServerName primaryHost, ServerName secondaryHost, ServerName tertiaryHost) {

    if (primaryHost != null) {
      addRegionToMap(assignmentMapForFavoredNodes, region, primaryHost);

    } else if (secondaryHost != null && tertiaryHost != null) {

      // Assign the region to the one with a lower load (both have the desired hdfs blocks)
      ServerName s;
      ServerMetrics tertiaryLoad = super.services.getServerManager().getLoad(tertiaryHost);
      ServerMetrics secondaryLoad = super.services.getServerManager().getLoad(secondaryHost);
      if (secondaryLoad != null && tertiaryLoad != null) {
        if (secondaryLoad.getRegionMetrics().size() < tertiaryLoad.getRegionMetrics().size()) {
          s = secondaryHost;
        } else {
          s = tertiaryHost;
        }
      } else {
        // We don't have one/more load, lets just choose a random node
        s = RANDOM.nextBoolean() ? secondaryHost : tertiaryHost;
      }
      addRegionToMap(assignmentMapForFavoredNodes, region, s);
    } else if (secondaryHost != null) {
      addRegionToMap(assignmentMapForFavoredNodes, region, secondaryHost);
    } else if (tertiaryHost != null) {
      addRegionToMap(assignmentMapForFavoredNodes, region, tertiaryHost);
    } else {
      // No favored nodes are online, lets assign to BOGUS server
      addRegionToMap(assignmentMapForFavoredNodes, region, BOGUS_SERVER_NAME);
    }
  }

  /*
   * If we have favored nodes for a region, we will return one of the FN as destination. If
   * favored nodes are not present for a region, we will generate and return one of the FN as
   * destination. If we can't generate anything, lets fallback.
   */
  @Override
  public ServerName randomAssignment(RegionInfo regionInfo, List<ServerName> servers)
      throws HBaseIOException {

    if (servers != null && servers.contains(masterServerName)) {
      if (shouldBeOnMaster(regionInfo)) {
        metricsBalancer.incrMiscInvocations();
        return masterServerName;
      }
      if (!LoadBalancer.isTablesOnMaster(getConf())) {
        // Guarantee we do not put any regions on master
        servers = new ArrayList<>(servers);
        servers.remove(masterServerName);
      }
    }

    ServerName destination = null;
    if (!FavoredNodesManager.isFavoredNodeApplicable(regionInfo)) {
      return super.randomAssignment(regionInfo, servers);
    }

    metricsBalancer.incrMiscInvocations();

    List<ServerName> favoredNodes = fnm.getFavoredNodes(regionInfo);
    if (favoredNodes == null || favoredNodes.isEmpty()) {
      // Generate new favored nodes and return primary
      FavoredNodeAssignmentHelper helper = new FavoredNodeAssignmentHelper(servers, getConf());
      helper.initialize();
      try {
        favoredNodes = helper.generateFavoredNodes(regionInfo);
        updateFavoredNodesForRegion(regionInfo, favoredNodes);

      } catch (IOException e) {
        LOG.warn("Encountered exception while doing favored-nodes (random)assignment " + e);
        throw new HBaseIOException(e);
      }
    }

    List<ServerName> onlineServers = getOnlineFavoredNodes(servers, favoredNodes);
    if (onlineServers.size() > 0) {
      destination = onlineServers.get(RANDOM.nextInt(onlineServers.size()));
    }

    boolean alwaysAssign = getConf().getBoolean(FAVORED_ALWAYS_ASSIGN_REGIONS, true);
    if (destination == null && alwaysAssign) {
      LOG.warn("Can't generate FN for region: " + regionInfo + " falling back");
      destination = super.randomAssignment(regionInfo, servers);
    }
    return destination;
  }

  private void updateFavoredNodesForRegion(RegionInfo regionInfo, List<ServerName> newFavoredNodes)
      throws IOException {
    Map<RegionInfo, List<ServerName>> regionFNMap = Maps.newHashMap();
    regionFNMap.put(regionInfo, newFavoredNodes);
    fnm.updateFavoredNodes(regionFNMap);
  }

  /*
   * Reuse BaseLoadBalancer's retainAssignment, but generate favored nodes when its missing.
   */
  @Override
  @NonNull
  public Map<ServerName, List<RegionInfo>> retainAssignment(Map<RegionInfo, ServerName> regions,
      List<ServerName> servers) throws HBaseIOException {

    Map<ServerName, List<RegionInfo>> assignmentMap = Maps.newHashMap();
    Map<ServerName, List<RegionInfo>> result = super.retainAssignment(regions, servers);
    if (result.isEmpty()) {
      LOG.warn("Nothing to assign to, probably no servers or no regions");
      return result;
    }

    // Guarantee not to put other regions on master
    if (servers != null && servers.contains(masterServerName)) {
      servers = new ArrayList<>(servers);
      servers.remove(masterServerName);
    }

    // Lets check if favored nodes info is in META, if not generate now.
    FavoredNodeAssignmentHelper helper = new FavoredNodeAssignmentHelper(servers, getConf());
    helper.initialize();

    LOG.debug("Generating favored nodes for regions missing them.");
    Map<RegionInfo, List<ServerName>> regionFNMap = Maps.newHashMap();

    try {
      for (Entry<ServerName, List<RegionInfo>> entry : result.entrySet()) {

        ServerName sn = entry.getKey();
        ServerName primary = ServerName.valueOf(sn.getHostname(), sn.getPort(), NON_STARTCODE);

        for (RegionInfo hri : entry.getValue()) {

          if (FavoredNodesManager.isFavoredNodeApplicable(hri)) {
            List<ServerName> favoredNodes = fnm.getFavoredNodes(hri);
            if (favoredNodes == null || favoredNodes.size() < FAVORED_NODES_NUM) {

              LOG.debug("Generating favored nodes for: " + hri + " with primary: " + primary);
              ServerName[] secondaryAndTertiaryNodes = helper.getSecondaryAndTertiary(hri, primary);
              if (secondaryAndTertiaryNodes != null && secondaryAndTertiaryNodes.length == 2) {
                List<ServerName> newFavoredNodes = Lists.newArrayList();
                newFavoredNodes.add(primary);
                newFavoredNodes.add(ServerName.valueOf(secondaryAndTertiaryNodes[0].getHostname(),
                    secondaryAndTertiaryNodes[0].getPort(), NON_STARTCODE));
                newFavoredNodes.add(ServerName.valueOf(secondaryAndTertiaryNodes[1].getHostname(),
                    secondaryAndTertiaryNodes[1].getPort(), NON_STARTCODE));
                regionFNMap.put(hri, newFavoredNodes);
                addRegionToMap(assignmentMap, hri, sn);

              } else {
                throw new HBaseIOException("Cannot generate secondary/tertiary FN for " + hri
                  + " generated "
                  + (secondaryAndTertiaryNodes != null ? secondaryAndTertiaryNodes : " nothing"));
              }
            } else {
              List<ServerName> onlineFN = getOnlineFavoredNodes(servers, favoredNodes);
              if (onlineFN.isEmpty()) {
                // All favored nodes are dead, lets assign it to BOGUS
                addRegionToMap(assignmentMap, hri, BOGUS_SERVER_NAME);
              } else {
                // Is primary not on FN? Less likely, but we can still take care of this.
                if (FavoredNodesPlan.getFavoredServerPosition(favoredNodes, sn) != null) {
                  addRegionToMap(assignmentMap, hri, sn);
                } else {
                  ServerName destination = onlineFN.get(RANDOM.nextInt(onlineFN.size()));
                  LOG.warn("Region: " + hri + " not hosted on favored nodes: " + favoredNodes
                    + " current: " + sn + " moving to: " + destination);
                  addRegionToMap(assignmentMap, hri, destination);
                }
              }
            }
          } else {
            addRegionToMap(assignmentMap, hri, sn);
          }
        }
      }

      if (!regionFNMap.isEmpty()) {
        LOG.debug("Updating FN in meta for missing regions, count: " + regionFNMap.size());
        fnm.updateFavoredNodes(regionFNMap);
      }

    } catch (IOException e) {
      throw new HBaseIOException("Cannot generate/update FN for regions: " + regionFNMap.keySet());
    }

    return assignmentMap;
  }

  /*
   * Return list of favored nodes that are online.
   */
  private List<ServerName> getOnlineFavoredNodes(List<ServerName> onlineServers,
      List<ServerName> serversWithoutStartCodes) {
    if (serversWithoutStartCodes == null) {
      return null;
    } else {
      List<ServerName> result = Lists.newArrayList();
      for (ServerName sn : serversWithoutStartCodes) {
        for (ServerName online : onlineServers) {
          if (ServerName.isSameAddress(sn, online)) {
            result.add(online);
          }
        }
      }
      return result;
    }
  }

  public synchronized List<ServerName> getFavoredNodes(RegionInfo regionInfo) {
    return this.fnm.getFavoredNodes(regionInfo);
  }

  /*
   * Generate Favored Nodes for daughters during region split.
   *
   * If the parent does not have FN, regenerates them for the daughters.
   *
   * If the parent has FN, inherit two FN from parent for each daughter and generate the remaining.
   * The primary FN for both the daughters should be the same as parent. Inherit the secondary
   * FN from the parent but keep it different for each daughter. Choose the remaining FN
   * randomly. This would give us better distribution over a period of time after enough splits.
   */
  @Override
  public void generateFavoredNodesForDaughter(List<ServerName> servers, RegionInfo parent,
      RegionInfo regionA, RegionInfo regionB) throws IOException {

    Map<RegionInfo, List<ServerName>> result = new HashMap<>();
    FavoredNodeAssignmentHelper helper = new FavoredNodeAssignmentHelper(servers, rackManager);
    helper.initialize();

    List<ServerName> parentFavoredNodes = fnm.getFavoredNodes(parent);
    if (parentFavoredNodes == null) {
      LOG.debug("Unable to find favored nodes for parent, " + parent
          + " generating new favored nodes for daughter");
      result.put(regionA, helper.generateFavoredNodes(regionA));
      result.put(regionB, helper.generateFavoredNodes(regionB));

    } else {

      // Lets get the primary and secondary from parent for regionA
      Set<ServerName> regionAFN =
          getInheritedFNForDaughter(helper, parentFavoredNodes, PRIMARY, SECONDARY);
      result.put(regionA, Lists.newArrayList(regionAFN));

      // Lets get the primary and tertiary from parent for regionB
      Set<ServerName> regionBFN =
          getInheritedFNForDaughter(helper, parentFavoredNodes, PRIMARY, TERTIARY);
      result.put(regionB, Lists.newArrayList(regionBFN));
    }

    fnm.updateFavoredNodes(result);
  }

  private Set<ServerName> getInheritedFNForDaughter(FavoredNodeAssignmentHelper helper,
      List<ServerName> parentFavoredNodes, Position primary, Position secondary)
      throws IOException {

    Set<ServerName> daughterFN = Sets.newLinkedHashSet();
    if (parentFavoredNodes.size() >= primary.ordinal()) {
      daughterFN.add(parentFavoredNodes.get(primary.ordinal()));
    }

    if (parentFavoredNodes.size() >= secondary.ordinal()) {
      daughterFN.add(parentFavoredNodes.get(secondary.ordinal()));
    }

    while (daughterFN.size() < FAVORED_NODES_NUM) {
      ServerName newNode = helper.generateMissingFavoredNode(Lists.newArrayList(daughterFN));
      daughterFN.add(newNode);
    }
    return daughterFN;
  }

  /*
   * Generate favored nodes for a region during merge. Choose the FN from one of the sources to
   * keep it simple.
   */
  @Override
  public void generateFavoredNodesForMergedRegion(RegionInfo merged, RegionInfo [] mergeParents)
      throws IOException {
    updateFavoredNodesForRegion(merged, fnm.getFavoredNodes(mergeParents[0]));
  }

  /*
   * Pick favored nodes with the highest locality for a region with lowest locality.
   */
  private class FavoredNodeLocalityPicker extends CandidateGenerator {

    @Override
    protected Cluster.Action generate(Cluster cluster) {

      int thisServer = pickRandomServer(cluster);
      int thisRegion;
      if (thisServer == -1) {
        LOG.trace("Could not pick lowest local region server");
        return Cluster.NullAction;
      } else {
        // Pick lowest local region on this server
        thisRegion = pickLowestLocalRegionOnServer(cluster, thisServer);
      }
      if (thisRegion == -1) {
        if (cluster.regionsPerServer[thisServer].length > 0) {
          LOG.trace("Could not pick lowest local region even when region server held "
            + cluster.regionsPerServer[thisServer].length + " regions");
        }
        return Cluster.NullAction;
      }

      RegionInfo hri = cluster.regions[thisRegion];
      List<ServerName> favoredNodes = fnm.getFavoredNodes(hri);
      int otherServer;
      if (favoredNodes == null) {
        if (!FavoredNodesManager.isFavoredNodeApplicable(hri)) {
          otherServer = pickOtherRandomServer(cluster, thisServer);
        } else {
          // No FN, ignore
          LOG.trace("Ignoring, no favored nodes for region: " + hri);
          return Cluster.NullAction;
        }
      } else {
        // Pick other favored node with the highest locality
        otherServer = getDifferentFavoredNode(cluster, favoredNodes, thisServer);
      }
      return getAction(thisServer, thisRegion, otherServer, -1);
    }

    private int getDifferentFavoredNode(Cluster cluster, List<ServerName> favoredNodes,
        int currentServer) {
      List<Integer> fnIndex = new ArrayList<>();
      for (ServerName sn : favoredNodes) {
        if (cluster.serversToIndex.containsKey(sn.getAddress())) {
          fnIndex.add(cluster.serversToIndex.get(sn.getAddress()));
        }
      }
      float locality = 0;
      int highestLocalRSIndex = -1;
      for (Integer index : fnIndex) {
        if (index != currentServer) {
          float temp = cluster.localityPerServer[index];
          if (temp >= locality) {
            locality = temp;
            highestLocalRSIndex = index;
          }
        }
      }
      return highestLocalRSIndex;
    }

    private int pickLowestLocalRegionOnServer(Cluster cluster, int server) {
      return cluster.getLowestLocalityRegionOnServer(server);
    }
  }

  /*
   * This is like LoadCandidateGenerator, but we choose appropriate FN for the region on the
   * most loaded server.
   */
  class FavoredNodeLoadPicker extends CandidateGenerator {

    @Override
    Cluster.Action generate(Cluster cluster) {
      cluster.sortServersByRegionCount();
      int thisServer = pickMostLoadedServer(cluster);
      int thisRegion = pickRandomRegion(cluster, thisServer, 0);
      RegionInfo hri = cluster.regions[thisRegion];
      int otherServer;
      List<ServerName> favoredNodes = fnm.getFavoredNodes(hri);
      if (favoredNodes == null) {
        if (!FavoredNodesManager.isFavoredNodeApplicable(hri)) {
          otherServer = pickLeastLoadedServer(cluster, thisServer);
        } else {
          return Cluster.NullAction;
        }
      } else {
        otherServer = pickLeastLoadedFNServer(cluster, favoredNodes, thisServer);
      }
      return getAction(thisServer, thisRegion, otherServer, -1);
    }

    private int pickLeastLoadedServer(final Cluster cluster, int thisServer) {
      Integer[] servers = cluster.serverIndicesSortedByRegionCount;
      int index;
      for (index = 0; index < servers.length ; index++) {
        if ((servers[index] != null) && servers[index] != thisServer) {
          break;
        }
      }
      return servers[index];
    }

    private int pickLeastLoadedFNServer(final Cluster cluster, List<ServerName> favoredNodes,
        int currentServerIndex) {
      List<Integer> fnIndex = new ArrayList<>();
      for (ServerName sn : favoredNodes) {
        if (cluster.serversToIndex.containsKey(sn.getAddress())) {
          fnIndex.add(cluster.serversToIndex.get(sn.getAddress()));
        }
      }
      int leastLoadedFN = -1;
      int load = Integer.MAX_VALUE;
      for (Integer index : fnIndex) {
        if (index != currentServerIndex) {
          int temp = cluster.getNumRegions(index);
          if (temp < load) {
            load = temp;
            leastLoadedFN = index;
          }
        }
      }
      return leastLoadedFN;
    }

    private int pickMostLoadedServer(final Cluster cluster) {
      Integer[] servers = cluster.serverIndicesSortedByRegionCount;
      int index;
      for (index = servers.length - 1; index > 0 ; index--) {
        if (servers[index] != null) {
          break;
        }
      }
      return servers[index];
    }
  }

  /*
   * For all regions correctly assigned to favored nodes, we just use the stochastic balancer
   * implementation. For the misplaced regions, we assign a bogus server to it and AM takes care.
   */
  @Override
  public synchronized List<RegionPlan> balanceTable(TableName tableName,
      Map<ServerName, List<RegionInfo>> loadOfOneTable) {

    if (this.services != null) {

      List<RegionPlan> regionPlans = Lists.newArrayList();
      Map<ServerName, List<RegionInfo>> correctAssignments = new HashMap<>();
      int misplacedRegions = 0;

      for (Entry<ServerName, List<RegionInfo>> entry : loadOfOneTable.entrySet()) {
        ServerName current = entry.getKey();
        List<RegionInfo> regions = Lists.newArrayList();
        correctAssignments.put(current, regions);

        for (RegionInfo hri : entry.getValue()) {
          List<ServerName> favoredNodes = fnm.getFavoredNodes(hri);
          if (FavoredNodesPlan.getFavoredServerPosition(favoredNodes, current) != null ||
              !FavoredNodesManager.isFavoredNodeApplicable(hri)) {
            regions.add(hri);

          } else {
            // No favored nodes, lets unassign.
            LOG.warn("Region not on favored nodes, unassign. Region: " + hri
              + " current: " + current + " favored nodes: " + favoredNodes);
            try {
              this.services.getAssignmentManager().unassign(hri);
            } catch (IOException e) {
              LOG.warn("Failed unassign", e);
              continue;
            }
            RegionPlan rp = new RegionPlan(hri, null, null);
            regionPlans.add(rp);
            misplacedRegions++;
          }
        }
      }
      LOG.debug("Found misplaced regions: " + misplacedRegions + ", not on favored nodes.");
      List<RegionPlan> regionPlansFromBalance = super.balanceTable(tableName, correctAssignments);
      if (regionPlansFromBalance != null) {
        regionPlans.addAll(regionPlansFromBalance);
      }
      return regionPlans;
    } else {
      return super.balanceTable(tableName, loadOfOneTable);
    }
  }
}

