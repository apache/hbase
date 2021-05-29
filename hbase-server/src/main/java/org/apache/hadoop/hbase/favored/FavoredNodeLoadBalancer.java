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

import static org.apache.hadoop.hbase.favored.FavoredNodesPlan.Position.PRIMARY;
import static org.apache.hadoop.hbase.favored.FavoredNodesPlan.Position.SECONDARY;
import static org.apache.hadoop.hbase.favored.FavoredNodesPlan.Position.TERTIARY;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.favored.FavoredNodesPlan.Position;
import org.apache.hadoop.hbase.master.RackManager;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer;
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
 * primary region servers die.
 *
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class FavoredNodeLoadBalancer extends BaseLoadBalancer implements FavoredNodesPromoter {
  private static final Logger LOG = LoggerFactory.getLogger(FavoredNodeLoadBalancer.class);

  private RackManager rackManager;
  private Configuration conf;
  private FavoredNodesManager fnm;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public synchronized void initialize() throws HBaseIOException {
    super.initialize();
    super.setConf(conf);
    this.fnm = services.getFavoredNodesManager();
    this.rackManager = new RackManager(conf);
    super.setConf(conf);
  }

  @Override
  public List<RegionPlan> balanceTable(TableName tableName,
      Map<ServerName, List<RegionInfo>> loadOfOneTable) {
    // TODO. Look at is whether Stochastic loadbalancer can be integrated with this
    List<RegionPlan> plans = new ArrayList<>();
    Map<ServerName, ServerName> serverNameWithoutCodeToServerName = new HashMap<>();
    ServerManager serverMgr = super.services.getServerManager();
    for (ServerName sn : serverMgr.getOnlineServersList()) {
      ServerName s = ServerName.valueOf(sn.getHostname(), sn.getPort(), ServerName.NON_STARTCODE);
      // FindBugs complains about useless store! serverNameToServerNameWithoutCode.put(sn, s);
      serverNameWithoutCodeToServerName.put(s, sn);
    }
    for (Map.Entry<ServerName, List<RegionInfo>> entry : loadOfOneTable.entrySet()) {
      ServerName currentServer = entry.getKey();
      // get a server without the startcode for the currentServer
      ServerName currentServerWithoutStartCode = ServerName.valueOf(currentServer.getHostname(),
        currentServer.getPort(), ServerName.NON_STARTCODE);
      List<RegionInfo> list = entry.getValue();
      for (RegionInfo region : list) {
        if (!FavoredNodesManager.isFavoredNodeApplicable(region)) {
          continue;
        }
        List<ServerName> favoredNodes = fnm.getFavoredNodes(region);
        if (favoredNodes == null || favoredNodes.get(0).equals(currentServerWithoutStartCode)) {
          continue; // either favorednodes does not exist or we are already on the primary node
        }
        ServerName destination = null;
        // check whether the primary is available
        destination = serverNameWithoutCodeToServerName.get(favoredNodes.get(0));
        if (destination == null) {
          // check whether the region is on secondary/tertiary
          if (currentServerWithoutStartCode.equals(favoredNodes.get(1))
              || currentServerWithoutStartCode.equals(favoredNodes.get(2))) {
            continue;
          }
          // the region is currently on none of the favored nodes
          // get it on one of them if possible
          ServerMetrics l1 = super.services.getServerManager()
              .getLoad(serverNameWithoutCodeToServerName.get(favoredNodes.get(1)));
          ServerMetrics l2 = super.services.getServerManager()
              .getLoad(serverNameWithoutCodeToServerName.get(favoredNodes.get(2)));
          if (l1 != null && l2 != null) {
            if (l1.getRegionMetrics().size() > l2.getRegionMetrics().size()) {
              destination = serverNameWithoutCodeToServerName.get(favoredNodes.get(2));
            } else {
              destination = serverNameWithoutCodeToServerName.get(favoredNodes.get(1));
            }
          } else if (l1 != null) {
            destination = serverNameWithoutCodeToServerName.get(favoredNodes.get(1));
          } else if (l2 != null) {
            destination = serverNameWithoutCodeToServerName.get(favoredNodes.get(2));
          }
        }

        if (destination != null) {
          RegionPlan plan = new RegionPlan(region, currentServer, destination);
          plans.add(plan);
        }
      }
    }
    return plans;
  }

  @Override
  @NonNull
  public Map<ServerName, List<RegionInfo>> roundRobinAssignment(List<RegionInfo> regions,
      List<ServerName> servers) throws HBaseIOException {
    Map<ServerName, List<RegionInfo>> assignmentMap;
    try {
      FavoredNodeAssignmentHelper assignmentHelper =
          new FavoredNodeAssignmentHelper(servers, rackManager);
      assignmentHelper.initialize();
      if (!assignmentHelper.canPlaceFavoredNodes()) {
        return super.roundRobinAssignment(regions, servers);
      }
      // Segregate the regions into two types:
      // 1. The regions that have favored node assignment, and where at least
      //    one of the favored node is still alive. In this case, try to adhere
      //    to the current favored nodes assignment as much as possible - i.e.,
      //    if the current primary is gone, then make the secondary or tertiary
      //    as the new host for the region (based on their current load).
      //    Note that we don't change the favored
      //    node assignments here (even though one or more favored node is currently
      //    down). It is up to the balanceCluster to do this hard work. The HDFS
      //    can handle the fact that some nodes in the favored nodes hint is down
      //    It'd allocate some other DNs. In combination with stale settings for HDFS,
      //    we should be just fine.
      // 2. The regions that currently don't have favored node assignment. We will
      //    need to come up with favored nodes assignments for them. The corner case
      //    in (1) above is that all the nodes are unavailable and in that case, we
      //    will note that this region doesn't have favored nodes.
      Pair<Map<ServerName,List<RegionInfo>>, List<RegionInfo>> segregatedRegions =
          segregateRegionsAndAssignRegionsWithFavoredNodes(regions, servers);
      Map<ServerName,List<RegionInfo>> regionsWithFavoredNodesMap = segregatedRegions.getFirst();
      List<RegionInfo> regionsWithNoFavoredNodes = segregatedRegions.getSecond();
      assignmentMap = new HashMap<>();
      roundRobinAssignmentImpl(assignmentHelper, assignmentMap, regionsWithNoFavoredNodes,
          servers);
      // merge the assignment maps
      assignmentMap.putAll(regionsWithFavoredNodesMap);
    } catch (Exception ex) {
      LOG.warn("Encountered exception while doing favored-nodes assignment " + ex +
          " Falling back to regular assignment");
      assignmentMap = super.roundRobinAssignment(regions, servers);
    }
    return assignmentMap;
  }

  @Override
  public ServerName randomAssignment(RegionInfo regionInfo, List<ServerName> servers)
      throws HBaseIOException {
    try {
      FavoredNodeAssignmentHelper assignmentHelper =
          new FavoredNodeAssignmentHelper(servers, rackManager);
      assignmentHelper.initialize();
      ServerName primary = super.randomAssignment(regionInfo, servers);
      if (!FavoredNodesManager.isFavoredNodeApplicable(regionInfo)
          || !assignmentHelper.canPlaceFavoredNodes()) {
        return primary;
      }
      List<ServerName> favoredNodes = fnm.getFavoredNodes(regionInfo);
      // check if we have a favored nodes mapping for this region and if so, return
      // a server from the favored nodes list if the passed 'servers' contains this
      // server as well (available servers, that is)
      if (favoredNodes != null) {
        for (ServerName s : favoredNodes) {
          ServerName serverWithLegitStartCode = availableServersContains(servers, s);
          if (serverWithLegitStartCode != null) {
            return serverWithLegitStartCode;
          }
        }
      }
      List<RegionInfo> regions = new ArrayList<>(1);
      regions.add(regionInfo);
      Map<RegionInfo, ServerName> primaryRSMap = new HashMap<>(1);
      primaryRSMap.put(regionInfo, primary);
      assignSecondaryAndTertiaryNodesForRegion(assignmentHelper, regions, primaryRSMap);
      return primary;
    } catch (Exception ex) {
      LOG.warn("Encountered exception while doing favored-nodes (random)assignment " + ex +
          " Falling back to regular assignment");
      return super.randomAssignment(regionInfo, servers);
    }
  }

  private Pair<Map<ServerName, List<RegionInfo>>, List<RegionInfo>>
  segregateRegionsAndAssignRegionsWithFavoredNodes(List<RegionInfo> regions,
      List<ServerName> availableServers) {
    Map<ServerName, List<RegionInfo>> assignmentMapForFavoredNodes = new HashMap<>(regions.size() / 2);
    List<RegionInfo> regionsWithNoFavoredNodes = new ArrayList<>(regions.size()/2);
    for (RegionInfo region : regions) {
      List<ServerName> favoredNodes = fnm.getFavoredNodes(region);
      ServerName primaryHost = null;
      ServerName secondaryHost = null;
      ServerName tertiaryHost = null;
      if (favoredNodes != null) {
        for (ServerName s : favoredNodes) {
          ServerName serverWithLegitStartCode = availableServersContains(availableServers, s);
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
        assignRegionToAvailableFavoredNode(assignmentMapForFavoredNodes, region,
              primaryHost, secondaryHost, tertiaryHost);
      }
      if (primaryHost == null && secondaryHost == null && tertiaryHost == null) {
        //all favored nodes unavailable
        regionsWithNoFavoredNodes.add(region);
      }
    }
    return new Pair<>(assignmentMapForFavoredNodes, regionsWithNoFavoredNodes);
  }

  // Do a check of the hostname and port and return the servername from the servers list
  // that matched (the favoredNode will have a startcode of -1 but we want the real
  // server with the legit startcode
  private ServerName availableServersContains(List<ServerName> servers, ServerName favoredNode) {
    for (ServerName server : servers) {
      if (ServerName.isSameAddress(favoredNode, server)) {
        return server;
      }
    }
    return null;
  }

  private void assignRegionToAvailableFavoredNode(Map<ServerName,
      List<RegionInfo>> assignmentMapForFavoredNodes, RegionInfo region, ServerName primaryHost,
      ServerName secondaryHost, ServerName tertiaryHost) {
    if (primaryHost != null) {
      addRegionToMap(assignmentMapForFavoredNodes, region, primaryHost);
    } else if (secondaryHost != null && tertiaryHost != null) {
      // assign the region to the one with a lower load
      // (both have the desired hdfs blocks)
      ServerName s;
      ServerMetrics tertiaryLoad = super.services.getServerManager().getLoad(tertiaryHost);
      ServerMetrics secondaryLoad = super.services.getServerManager().getLoad(secondaryHost);
      if (secondaryLoad.getRegionMetrics().size() < tertiaryLoad.getRegionMetrics().size()) {
        s = secondaryHost;
      } else {
        s = tertiaryHost;
      }
      addRegionToMap(assignmentMapForFavoredNodes, region, s);
    } else if (secondaryHost != null) {
      addRegionToMap(assignmentMapForFavoredNodes, region, secondaryHost);
    } else if (tertiaryHost != null) {
      addRegionToMap(assignmentMapForFavoredNodes, region, tertiaryHost);
    }
  }

  private void addRegionToMap(Map<ServerName, List<RegionInfo>> assignmentMapForFavoredNodes,
      RegionInfo region, ServerName host) {
    List<RegionInfo> regionsOnServer = null;
    if ((regionsOnServer = assignmentMapForFavoredNodes.get(host)) == null) {
      regionsOnServer = new ArrayList<>();
      assignmentMapForFavoredNodes.put(host, regionsOnServer);
    }
    regionsOnServer.add(region);
  }

  public synchronized List<ServerName> getFavoredNodes(RegionInfo regionInfo) {
    return this.fnm.getFavoredNodes(regionInfo);
  }

  private void roundRobinAssignmentImpl(FavoredNodeAssignmentHelper assignmentHelper,
      Map<ServerName, List<RegionInfo>> assignmentMap,
      List<RegionInfo> regions, List<ServerName> servers) throws IOException {
    Map<RegionInfo, ServerName> primaryRSMap = new HashMap<>();
    // figure the primary RSs
    assignmentHelper.placePrimaryRSAsRoundRobin(assignmentMap, primaryRSMap, regions);
    assignSecondaryAndTertiaryNodesForRegion(assignmentHelper, regions, primaryRSMap);
  }

  private void assignSecondaryAndTertiaryNodesForRegion(
      FavoredNodeAssignmentHelper assignmentHelper,
      List<RegionInfo> regions, Map<RegionInfo, ServerName> primaryRSMap) throws IOException {
    // figure the secondary and tertiary RSs
    Map<RegionInfo, ServerName[]> secondaryAndTertiaryRSMap =
        assignmentHelper.placeSecondaryAndTertiaryRS(primaryRSMap);

    Map<RegionInfo, List<ServerName>> regionFNMap = Maps.newHashMap();
    // now record all the assignments so that we can serve queries later
    for (RegionInfo region : regions) {
      // Store the favored nodes without startCode for the ServerName objects
      // We don't care about the startcode; but only the hostname really
      List<ServerName> favoredNodesForRegion = new ArrayList<>(3);
      ServerName sn = primaryRSMap.get(region);
      favoredNodesForRegion.add(ServerName.valueOf(sn.getHostname(), sn.getPort(),
          ServerName.NON_STARTCODE));
      ServerName[] secondaryAndTertiaryNodes = secondaryAndTertiaryRSMap.get(region);
      if (secondaryAndTertiaryNodes != null) {
        favoredNodesForRegion.add(ServerName.valueOf(secondaryAndTertiaryNodes[0].getHostname(),
            secondaryAndTertiaryNodes[0].getPort(), ServerName.NON_STARTCODE));
        favoredNodesForRegion.add(ServerName.valueOf(secondaryAndTertiaryNodes[1].getHostname(),
            secondaryAndTertiaryNodes[1].getPort(), ServerName.NON_STARTCODE));
      }
      regionFNMap.put(region, favoredNodesForRegion);
    }
    fnm.updateFavoredNodes(regionFNMap);
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

    List<ServerName> parentFavoredNodes = getFavoredNodes(parent);
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

    while (daughterFN.size() < FavoredNodeAssignmentHelper.FAVORED_NODES_NUM) {
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
    Map<RegionInfo, List<ServerName>> regionFNMap = Maps.newHashMap();
    regionFNMap.put(merged, getFavoredNodes(mergeParents[0]));
    fnm.updateFavoredNodes(regionFNMap);
  }

}
