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

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.favored.FavoredNodesManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * This is like LoadCandidateGenerator, but we choose appropriate FN for the region on the
 * most loaded server.
 */
@InterfaceAudience.Private
public class FavoredNodeLoadPicker extends CandidateGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(FavoredNodeLoadPicker.class);
  private final FavoredNodesManager fnm;

  public FavoredNodeLoadPicker(FavoredNodesManager fnm) {
    this.fnm = fnm;
  }

  @Override
  BalanceAction generate(BalancerClusterState cluster) {
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
        return BalanceAction.NULL_ACTION;
      }
    } else {
      otherServer = pickLeastLoadedFNServer(cluster, favoredNodes, thisServer);
    }
    return getAction(thisServer, thisRegion, otherServer, -1);
  }

  private int pickLeastLoadedServer(final BalancerClusterState cluster, int thisServer) {
    Integer[] servers = cluster.serverIndicesSortedByRegionCount;
    int index;
    for (index = 0; index < servers.length ; index++) {
      if ((servers[index] != null) && servers[index] != thisServer) {
        break;
      }
    }
    return servers[index];
  }

  private int pickLeastLoadedFNServer(final BalancerClusterState cluster,
    List<ServerName> favoredNodes, int currentServerIndex) {
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

  private int pickMostLoadedServer(final BalancerClusterState cluster) {
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
