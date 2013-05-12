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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.RegionPlan;

/**
 * An implementation of the {@link LoadBalancer} that assigns favored nodes for
 * each region. There is a Primary RegionServer that hosts the region, and then
 * there is Secondary and Tertiary RegionServers. Currently, the favored nodes
 * information is used in creating HDFS files - the Primary RegionServer passes
 * the primary, secondary, tertiary node addresses as hints to the DistributedFileSystem
 * API for creating files on the filesystem. These nodes are treated as hints by
 * the HDFS to place the blocks of the file. This alleviates the problem to do with
 * reading from remote nodes (since we can make the Secondary RegionServer as the new
 * Primary RegionServer) after a region is recovered. This should help provide consistent
 * read latencies for the regions even when their primary region servers die.
 *
 */
@InterfaceAudience.Private
public class FavoredNodeLoadBalancer extends BaseLoadBalancer {
  private static final Log LOG = LogFactory.getLog(FavoredNodeLoadBalancer.class);

  private FavoredNodes globalFavoredNodesAssignmentPlan;
  private Configuration configuration;

  @Override
  public void setConf(Configuration conf) {
    this.configuration = conf;
    globalFavoredNodesAssignmentPlan = new FavoredNodes();
  }

  @Override
  public List<RegionPlan> balanceCluster(Map<ServerName, List<HRegionInfo>> clusterState) {
    //TODO. At a high level, this should look at the block locality per region, and
    //then reassign regions based on which nodes have the most blocks of the region
    //file(s). There could be different ways like minimize region movement, or, maximum
    //locality, etc. The other dimension to look at is whether Stochastic loadbalancer
    //can be integrated with this
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Map<ServerName, List<HRegionInfo>> roundRobinAssignment(List<HRegionInfo> regions,
      List<ServerName> servers) {
    Map<ServerName, List<HRegionInfo>> assignmentMap;
    try {
      FavoredNodeAssignmentHelper assignmentHelper =
          new FavoredNodeAssignmentHelper(servers, configuration);
      assignmentHelper.initialize();
      if (!assignmentHelper.canPlaceFavoredNodes()) {
        return super.roundRobinAssignment(regions, servers);
      }
      assignmentMap = new HashMap<ServerName, List<HRegionInfo>>();
      roundRobinAssignmentImpl(assignmentHelper, assignmentMap, regions, servers);
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
          new FavoredNodeAssignmentHelper(servers, configuration);
      assignmentHelper.initialize();
      ServerName primary = super.randomAssignment(regionInfo, servers);
      if (!assignmentHelper.canPlaceFavoredNodes()) {
        return primary;
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

  public List<ServerName> getFavoredNodes(HRegionInfo regionInfo) {
    return this.globalFavoredNodesAssignmentPlan.getFavoredNodes(regionInfo);
  }

  private void roundRobinAssignmentImpl(FavoredNodeAssignmentHelper assignmentHelper,
      Map<ServerName, List<HRegionInfo>> assignmentMap,
      List<HRegionInfo> regions, List<ServerName> servers) throws IOException {
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
      List<ServerName> favoredNodesForRegion = new ArrayList<ServerName>(3);
      favoredNodesForRegion.add(primaryRSMap.get(region));
      ServerName[] secondaryAndTertiaryNodes = secondaryAndTertiaryRSMap.get(region);
      if (secondaryAndTertiaryNodes != null) {
        favoredNodesForRegion.add(secondaryAndTertiaryNodes[0]);
        favoredNodesForRegion.add(secondaryAndTertiaryNodes[1]);
      }
      globalFavoredNodesAssignmentPlan.updateFavoredNodesMap(region, favoredNodesForRegion);
    }
  }

  void noteFavoredNodes(final Map<HRegionInfo, ServerName[]> favoredNodesMap) {
    for (Map.Entry<HRegionInfo, ServerName[]> entry : favoredNodesMap.entrySet()) {
      globalFavoredNodesAssignmentPlan.updateFavoredNodesMap(entry.getKey(),
          Arrays.asList(entry.getValue()));
    }
  }
}