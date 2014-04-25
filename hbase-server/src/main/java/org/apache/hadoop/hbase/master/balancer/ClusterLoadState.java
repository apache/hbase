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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;

/**
 * Class used to hold the current state of the cluster and how balanced it is.
 */
public class ClusterLoadState {
  private final Map<ServerName, List<HRegionInfo>> clusterState;
  private final NavigableMap<ServerAndLoad, List<HRegionInfo>> serversByLoad;
  private boolean emptyRegionServerPresent = false;
  private int numRegions = 0;
  private int numServers = 0;
  private int numBackupMasters = 0;
  private int backupMasterWeight;

  public ClusterLoadState(ServerName master, Collection<ServerName> backupMasters,
      int backupMasterWeight, Map<ServerName, List<HRegionInfo>> clusterState) {
    this.backupMasterWeight = backupMasterWeight;
    this.numRegions = 0;
    this.numServers = clusterState.size();
    this.clusterState = clusterState;
    serversByLoad = new TreeMap<ServerAndLoad, List<HRegionInfo>>();
    // Iterate so we can count regions as we build the map
    for (Map.Entry<ServerName, List<HRegionInfo>> server : clusterState.entrySet()) {
      if (master != null && numServers > 1 && master.equals(server.getKey())) {
        // Don't count the master since its load is meant to be low.
        numServers--;
        continue;
      }
      List<HRegionInfo> regions = server.getValue();
      int sz = regions.size();
      if (sz == 0) emptyRegionServerPresent = true;
      numRegions += sz;
      if (backupMasters != null && backupMasters.contains(server.getKey())) {
        sz *= backupMasterWeight;
        numBackupMasters++;
      }
      serversByLoad.put(new ServerAndLoad(server.getKey(), sz), regions);
    }
  }

  Map<ServerName, List<HRegionInfo>> getClusterState() {
    return clusterState;
  }

  NavigableMap<ServerAndLoad, List<HRegionInfo>> getServersByLoad() {
    return serversByLoad;
  }

  boolean isEmptyRegionServerPresent() {
    return emptyRegionServerPresent;
  }

  int getNumRegions() {
    return numRegions;
  }

  int getNumServers() {
    return numServers;
  }

  int getNumBackupMasters() {
    return numBackupMasters;
  }

  float getLoadAverage() {
    return numRegions / (numServers - numBackupMasters * (1 - 1.0f/backupMasterWeight));
  }

  int getMaxLoad() {
    return getServersByLoad().lastKey().getLoad();
  }

  int getMinLoad() {
    return getServersByLoad().firstKey().getLoad();
  }

}
