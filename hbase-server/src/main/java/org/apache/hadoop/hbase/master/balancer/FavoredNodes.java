/**
 * Copyright 2012 The Apache Software Foundation
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.jboss.netty.util.internal.ConcurrentHashMap;

/**
 * This class contains the mapping information between each region and
 * its favored region server list. Used by {@link FavoredNodeLoadBalancer} set
 * of classes and from unit tests (hence the class is public)
 *
 * All the access to this class is thread-safe.
 */
@InterfaceAudience.Private
public class FavoredNodes {
  protected static final Log LOG = LogFactory.getLog(
      FavoredNodes.class.getName());

  /** the map between each region and its favored region server list */
  private Map<HRegionInfo, List<ServerName>> favoredNodesMap;

  public static enum Position {
    PRIMARY,
    SECONDARY,
    TERTIARY;
  };

  public FavoredNodes() {
    favoredNodesMap = new ConcurrentHashMap<HRegionInfo, List<ServerName>>();
  }

  /**
   * Add an assignment to the plan
   * @param region
   * @param servers
   */
  public synchronized void updateFavoredNodesMap(HRegionInfo region,
      List<ServerName> servers) {
    if (region == null || servers == null || servers.size() ==0)
      return;
    this.favoredNodesMap.put(region, servers);
  }

  /**
   * @param region
   * @return the list of favored region server for this region based on the plan
   */
  public synchronized List<ServerName> getFavoredNodes(HRegionInfo region) {
    return favoredNodesMap.get(region);
  }

  /**
   * Return the position of the server in the favoredNodes list. Assumes the
   * favoredNodes list is of size 3.
   * @param favoredNodes
   * @param server
   * @return position
   */
  static Position getFavoredServerPosition(
      List<ServerName> favoredNodes, ServerName server) {
    if (favoredNodes == null || server == null ||
        favoredNodes.size() != FavoredNodeAssignmentHelper.FAVORED_NODES_NUM) {
      return null;
    }
    for (Position p : Position.values()) {
      if (favoredNodes.get(p.ordinal()).equals(server)) {
        return p;
      }
    }
    return null;
  }
}
