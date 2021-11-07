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

/**
   * Pick favored nodes with the highest locality for a region with lowest locality.
*/
@InterfaceAudience.Private
public class FavoredNodeLocalityPicker extends CandidateGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(FavoredNodeLocalityPicker.class);
  private final FavoredNodesManager fnm;

  public FavoredNodeLocalityPicker(FavoredNodesManager fnm) {
    this.fnm = fnm;
  }

  @Override
  protected BalanceAction generate(BalancerClusterState cluster) {

    int thisServer = pickRandomServer(cluster);
    int thisRegion;
    if (thisServer == -1) {
      LOG.trace("Could not pick lowest local region server");
      return BalanceAction.NULL_ACTION;
    } else {
      // Pick lowest local region on this server
      thisRegion = pickLowestLocalRegionOnServer(cluster, thisServer);
    }
    if (thisRegion == -1) {
      if (cluster.regionsPerServer[thisServer].length > 0) {
        LOG.trace("Could not pick lowest local region even when region server held "
          + cluster.regionsPerServer[thisServer].length + " regions");
      }
      return BalanceAction.NULL_ACTION;
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
        return BalanceAction.NULL_ACTION;
      }
    } else {
      // Pick other favored node with the highest locality
      otherServer = getDifferentFavoredNode(cluster, favoredNodes, thisServer);
    }
    return getAction(thisServer, thisRegion, otherServer, -1);
  }

  private int getDifferentFavoredNode(BalancerClusterState cluster, List<ServerName> favoredNodes,
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

  private int pickLowestLocalRegionOnServer(BalancerClusterState cluster, int server) {
    return cluster.getLowestLocalityRegionOnServer(server);
  }
}
