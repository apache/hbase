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
package org.apache.hadoop.hbase.master.balancer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;

/**
 * This class contains the mapping information between each region name and
 * its favored region server list. Used by {@link FavoredNodeLoadBalancer} set
 * of classes and from unit tests (hence the class is public)
 *
 * All the access to this class is thread-safe.
 */
@InterfaceAudience.Private
public class FavoredNodesPlan {

  /** the map between each region name and its favored region server list */
  private Map<String, List<ServerName>> favoredNodesMap;

  public static enum Position {
    PRIMARY,
    SECONDARY,
    TERTIARY
  }

  public FavoredNodesPlan() {
    favoredNodesMap = new ConcurrentHashMap<String, List<ServerName>>();
  }

  /**
   * Update an assignment to the plan
   * @param region
   * @param servers
   */
  public void updateFavoredNodesMap(HRegionInfo region, List<ServerName> servers) {
    if (region == null || servers == null || servers.size() == 0) {
      return;
    }
    this.favoredNodesMap.put(region.getRegionNameAsString(), servers);
  }

  /**
   * @param region
   * @return the list of favored region server for this region based on the plan
   */
  public List<ServerName> getFavoredNodes(HRegionInfo region) {
    return favoredNodesMap.get(region.getRegionNameAsString());
  }

  /**
   * Return the position of the server in the favoredNodes list. Assumes the
   * favoredNodes list is of size 3.
   * @param favoredNodes
   * @param server
   * @return position
   */
  public static Position getFavoredServerPosition(
      List<ServerName> favoredNodes, ServerName server) {
    if (favoredNodes == null || server == null ||
        favoredNodes.size() != FavoredNodeAssignmentHelper.FAVORED_NODES_NUM) {
      return null;
    }
    for (Position p : Position.values()) {
      if (ServerName.isSameHostnameAndPort(favoredNodes.get(p.ordinal()),server)) {
        return p;
      }
    }
    return null;
  }

  /**
   * @return the mapping between each region to its favored region server list
   */
  public Map<String, List<ServerName>> getAssignmentMap() {
    return favoredNodesMap;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null) {
      return false;
    }
    if (getClass() != o.getClass()) {
      return false;
    }
    // To compare the map from objec o is identical to current assignment map.
    Map<String, List<ServerName>> comparedMap = ((FavoredNodesPlan)o).getAssignmentMap();

    // compare the size
    if (comparedMap.size() != this.favoredNodesMap.size())
      return false;

    // compare each element in the assignment map
    for (Map.Entry<String, List<ServerName>> entry :
      comparedMap.entrySet()) {
      List<ServerName> serverList = this.favoredNodesMap.get(entry.getKey());
      if (serverList == null && entry.getValue() != null) {
        return false;
      } else if (serverList != null && !serverList.equals(entry.getValue())) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    return favoredNodesMap.hashCode();
  }
}
