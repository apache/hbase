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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.RackManager;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.SnapshotOfRegionAssignmentFromMeta;
import org.apache.hadoop.hbase.master.balancer.FavoredNodesPlan.Position;
import org.apache.hadoop.hbase.util.Pair;

/**
 * An implementation of the {@link org.apache.hadoop.hbase.master.LoadBalancer} 
 * that assigns favored nodes for each region. There is a Primary RegionServer 
 * that hosts the region, and then there is Secondary and Tertiary RegionServers. 
 * Currently, the favored nodes information is used in creating HDFS files - the Primary 
 * RegionServer passes the primary, secondary, tertiary node addresses as hints to the 
 * DistributedFileSystem API for creating files on the filesystem. These nodes are treated 
 * as hints by the HDFS to place the blocks of the file. This alleviates the problem to 
 * do with reading from remote nodes (since we can make the Secondary RegionServer as the 
 * new Primary RegionServer) after a region is recovered. This should help provide 
 * consistent read latencies for the regions even when their primary region servers die.
 *
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class FavoredNodeLoadBalancer extends BaseLoadBalancer {
  private static final Log LOG = LogFactory.getLog(FavoredNodeLoadBalancer.class);

  private FavoredNodesPlan globalFavoredNodesAssignmentPlan;
  private RackManager rackManager;

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    globalFavoredNodesAssignmentPlan = new FavoredNodesPlan();
    this.rackManager = new RackManager(conf);
    super.setConf(conf);
  }

  @Override
  public List<RegionPlan> balanceCluster(Map<ServerName, List<HRegionInfo>> clusterState)  {
    //TODO. Look at is whether Stochastic loadbalancer can be integrated with this
    List<RegionPlan> plans = new ArrayList<RegionPlan>();
    //perform a scan of the meta to get the latest updates (if any)
    SnapshotOfRegionAssignmentFromMeta snaphotOfRegionAssignment =
        new SnapshotOfRegionAssignmentFromMeta(super.services.getConnection());
    try {
      snaphotOfRegionAssignment.initialize();
    } catch (IOException ie) {
      LOG.warn("Not running balancer since exception was thrown " + ie);
      return plans;
    }
    globalFavoredNodesAssignmentPlan = snaphotOfRegionAssignment.getExistingAssignmentPlan();
    Map<ServerName, ServerName> serverNameToServerNameWithoutCode =
        new HashMap<ServerName, ServerName>();
    Map<ServerName, ServerName> serverNameWithoutCodeToServerName =
        new HashMap<ServerName, ServerName>();
    ServerManager serverMgr = super.services.getServerManager();
    for (ServerName sn: serverMgr.getOnlineServersList()) {
      ServerName s = ServerName.valueOf(sn.getHostname(), sn.getPort(), ServerName.NON_STARTCODE);
      serverNameToServerNameWithoutCode.put(sn, s);
      serverNameWithoutCodeToServerName.put(s, sn);
    }
    for (Map.Entry<ServerName, List<HRegionInfo>> entry : clusterState.entrySet()) {
      ServerName currentServer = entry.getKey();
      //get a server without the startcode for the currentServer
      ServerName currentServerWithoutStartCode = ServerName.valueOf(currentServer.getHostname(),
          currentServer.getPort(), ServerName.NON_STARTCODE);
      List<HRegionInfo> list = entry.getValue();
      for (HRegionInfo region : list) {
        if(region.getTable().getNamespaceAsString()
            .equals(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR)) {
          continue;
        }
        List<ServerName> favoredNodes = globalFavoredNodesAssignmentPlan.getFavoredNodes(region);
        if (favoredNodes == null || favoredNodes.get(0).equals(currentServerWithoutStartCode)) {
          continue; //either favorednodes does not exist or we are already on the primary node
        }
        ServerName destination = null;
        //check whether the primary is available
        destination = serverNameWithoutCodeToServerName.get(favoredNodes.get(0));
        if (destination == null) {
          //check whether the region is on secondary/tertiary
          if (currentServerWithoutStartCode.equals(favoredNodes.get(1)) ||
              currentServerWithoutStartCode.equals(favoredNodes.get(2))) {
            continue;
          }
          //the region is currently on none of the favored nodes
          //get it on one of them if possible
          ServerLoad l1 = super.services.getServerManager().getLoad(
              serverNameWithoutCodeToServerName.get(favoredNodes.get(1)));
          ServerLoad l2 = super.services.getServerManager().getLoad(
              serverNameWithoutCodeToServerName.get(favoredNodes.get(2)));
          if (l1 != null && l2 != null) {
            if (l1.getLoad() > l2.getLoad()) {
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
  public Map<ServerName, List<HRegionInfo>> roundRobinAssignment(List<HRegionInfo> regions,
      List<ServerName> servers) {
    Map<ServerName, List<HRegionInfo>> assignmentMap;
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
      Pair<Map<ServerName,List<HRegionInfo>>, List<HRegionInfo>> segregatedRegions =
          segregateRegionsAndAssignRegionsWithFavoredNodes(regions, servers);
      Map<ServerName,List<HRegionInfo>> regionsWithFavoredNodesMap = segregatedRegions.getFirst();
      List<HRegionInfo> regionsWithNoFavoredNodes = segregatedRegions.getSecond();
      assignmentMap = new HashMap<ServerName, List<HRegionInfo>>();
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
  public ServerName randomAssignment(HRegionInfo regionInfo, List<ServerName> servers) {
    try {
      FavoredNodeAssignmentHelper assignmentHelper =
          new FavoredNodeAssignmentHelper(servers, rackManager);
      assignmentHelper.initialize();
      ServerName primary = super.randomAssignment(regionInfo, servers);
      if (!assignmentHelper.canPlaceFavoredNodes()) {
        return primary;
      }
      List<ServerName> favoredNodes = globalFavoredNodesAssignmentPlan.getFavoredNodes(regionInfo);
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
      List<HRegionInfo> regions = new ArrayList<HRegionInfo>(1);
      regions.add(regionInfo);
      Map<HRegionInfo, ServerName> primaryRSMap = new HashMap<HRegionInfo, ServerName>(1);
      primaryRSMap.put(regionInfo, primary);
      assignSecondaryAndTertiaryNodesForRegion(assignmentHelper, regions, primaryRSMap);
      return primary;
    } catch (Exception ex) {
      LOG.warn("Encountered exception while doing favored-nodes (random)assignment " + ex +
          " Falling back to regular assignment");
      return super.randomAssignment(regionInfo, servers);
    }
  }

  private Pair<Map<ServerName, List<HRegionInfo>>, List<HRegionInfo>>
  segregateRegionsAndAssignRegionsWithFavoredNodes(List<HRegionInfo> regions,
      List<ServerName> availableServers) {
    Map<ServerName, List<HRegionInfo>> assignmentMapForFavoredNodes =
        new HashMap<ServerName, List<HRegionInfo>>(regions.size() / 2);
    List<HRegionInfo> regionsWithNoFavoredNodes = new ArrayList<HRegionInfo>(regions.size()/2);
    for (HRegionInfo region : regions) {
      List<ServerName> favoredNodes = globalFavoredNodesAssignmentPlan.getFavoredNodes(region);
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
    return new Pair<Map<ServerName, List<HRegionInfo>>, List<HRegionInfo>>(
        assignmentMapForFavoredNodes, regionsWithNoFavoredNodes);
  }

  // Do a check of the hostname and port and return the servername from the servers list
  // that matched (the favoredNode will have a startcode of -1 but we want the real
  // server with the legit startcode
  private ServerName availableServersContains(List<ServerName> servers, ServerName favoredNode) {
    for (ServerName server : servers) {
      if (ServerName.isSameHostnameAndPort(favoredNode, server)) {
        return server;
      }
    }
    return null;
  }

  private void assignRegionToAvailableFavoredNode(Map<ServerName,
      List<HRegionInfo>> assignmentMapForFavoredNodes, HRegionInfo region, ServerName primaryHost,
      ServerName secondaryHost, ServerName tertiaryHost) {
    if (primaryHost != null) {
      addRegionToMap(assignmentMapForFavoredNodes, region, primaryHost);
    } else if (secondaryHost != null && tertiaryHost != null) {
      // assign the region to the one with a lower load
      // (both have the desired hdfs blocks)
      ServerName s;
      ServerLoad tertiaryLoad = super.services.getServerManager().getLoad(tertiaryHost);
      ServerLoad secondaryLoad = super.services.getServerManager().getLoad(secondaryHost);
      if (secondaryLoad.getLoad() < tertiaryLoad.getLoad()) {
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

  private void addRegionToMap(Map<ServerName, List<HRegionInfo>> assignmentMapForFavoredNodes,
      HRegionInfo region, ServerName host) {
    List<HRegionInfo> regionsOnServer = null;
    if ((regionsOnServer = assignmentMapForFavoredNodes.get(host)) == null) {
      regionsOnServer = new ArrayList<HRegionInfo>();
      assignmentMapForFavoredNodes.put(host, regionsOnServer);
    }
    regionsOnServer.add(region);
  }

  public List<ServerName> getFavoredNodes(HRegionInfo regionInfo) {
    return this.globalFavoredNodesAssignmentPlan.getFavoredNodes(regionInfo);
  }

  private void roundRobinAssignmentImpl(FavoredNodeAssignmentHelper assignmentHelper,
      Map<ServerName, List<HRegionInfo>> assignmentMap,
      List<HRegionInfo> regions, List<ServerName> servers) {
    Map<HRegionInfo, ServerName> primaryRSMap = new HashMap<HRegionInfo, ServerName>();
    // figure the primary RSs
    assignmentHelper.placePrimaryRSAsRoundRobin(assignmentMap, primaryRSMap, regions);
    assignSecondaryAndTertiaryNodesForRegion(assignmentHelper, regions, primaryRSMap);
  }

  private void assignSecondaryAndTertiaryNodesForRegion(
      FavoredNodeAssignmentHelper assignmentHelper,
      List<HRegionInfo> regions, Map<HRegionInfo, ServerName> primaryRSMap) {
    // figure the secondary and tertiary RSs
    Map<HRegionInfo, ServerName[]> secondaryAndTertiaryRSMap =
        assignmentHelper.placeSecondaryAndTertiaryRS(primaryRSMap);
    // now record all the assignments so that we can serve queries later
    for (HRegionInfo region : regions) {
      // Store the favored nodes without startCode for the ServerName objects
      // We don't care about the startcode; but only the hostname really
      List<ServerName> favoredNodesForRegion = new ArrayList<ServerName>(3);
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
      globalFavoredNodesAssignmentPlan.updateFavoredNodesMap(region, favoredNodesForRegion);
    }
  }
}
