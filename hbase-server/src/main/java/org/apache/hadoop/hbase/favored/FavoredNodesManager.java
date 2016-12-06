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
package org.apache.hadoop.hbase.favored;

import static org.apache.hadoop.hbase.favored.FavoredNodesPlan.Position.PRIMARY;
import static org.apache.hadoop.hbase.favored.FavoredNodesPlan.Position.SECONDARY;
import static org.apache.hadoop.hbase.favored.FavoredNodesPlan.Position.TERTIARY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.SnapshotOfRegionAssignmentFromMeta;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * FavoredNodesManager is responsible for maintaining favored nodes info in internal cache and
 * META table. Its the centralized store for all favored nodes information. All reads and updates
 * should be done through this class. There should only be one instance of
 * {@link FavoredNodesManager} in Master. {@link FavoredNodesPlan} and favored node information
 * from {@link SnapshotOfRegionAssignmentFromMeta} should not be used outside this class (except
 * for may be tools that only read or test cases). All other classes including Favored balancers
 * and {@link FavoredNodeAssignmentHelper} should use {@link FavoredNodesManager} for any
 * read/write/deletes to favored nodes.
 */
@InterfaceAudience.Private
public class FavoredNodesManager {

  private static final Log LOG = LogFactory.getLog(FavoredNodesManager.class);

  private FavoredNodesPlan globalFavoredNodesAssignmentPlan;
  private Map<ServerName, List<HRegionInfo>> primaryRSToRegionMap;
  private Map<ServerName, List<HRegionInfo>> secondaryRSToRegionMap;
  private Map<ServerName, List<HRegionInfo>> teritiaryRSToRegionMap;

  private MasterServices masterServices;

  public FavoredNodesManager(MasterServices masterServices) {
    this.masterServices = masterServices;
    this.globalFavoredNodesAssignmentPlan = new FavoredNodesPlan();
    this.primaryRSToRegionMap = new HashMap<>();
    this.secondaryRSToRegionMap = new HashMap<>();
    this.teritiaryRSToRegionMap = new HashMap<>();
  }

  public void initialize(SnapshotOfRegionAssignmentFromMeta snapshotOfRegionAssignment)
      throws HBaseIOException {
    globalFavoredNodesAssignmentPlan = snapshotOfRegionAssignment.getExistingAssignmentPlan();
    primaryRSToRegionMap = snapshotOfRegionAssignment.getPrimaryToRegionInfoMap();
    secondaryRSToRegionMap = snapshotOfRegionAssignment.getSecondaryToRegionInfoMap();
    teritiaryRSToRegionMap = snapshotOfRegionAssignment.getTertiaryToRegionInfoMap();
  }

  public synchronized List<ServerName> getFavoredNodes(HRegionInfo regionInfo) {
    return this.globalFavoredNodesAssignmentPlan.getFavoredNodes(regionInfo);
  }

  public synchronized void updateFavoredNodes(Map<HRegionInfo, List<ServerName>> regionFNMap)
      throws IOException {

    Map<HRegionInfo, List<ServerName>> regionToFavoredNodes = new HashMap<>();
    for (Map.Entry<HRegionInfo, List<ServerName>> entry : regionFNMap.entrySet()) {
      HRegionInfo regionInfo = entry.getKey();
      List<ServerName> servers = entry.getValue();

      /*
       * None of the following error conditions should happen. If it does, there is an issue with
       * favored nodes generation or the regions its called on.
       */
      if (servers.size() != Sets.newHashSet(servers).size()) {
        throw new IOException("Duplicates found: " + servers);
      }

      if (regionInfo.isSystemTable()) {
        throw new IOException("Can't update FN for system region: "
            + regionInfo.getRegionNameAsString() + " with " + servers);
      }

      if (servers.size() != FavoredNodeAssignmentHelper.FAVORED_NODES_NUM) {
        throw new IOException("At least " + FavoredNodeAssignmentHelper.FAVORED_NODES_NUM
            + " favored nodes should be present for region : " + regionInfo.getEncodedName()
            + " current FN servers:" + servers);
      }

      List<ServerName> serversWithNoStartCodes = Lists.newArrayList();
      for (ServerName sn : servers) {
        if (sn.getStartcode() == ServerName.NON_STARTCODE) {
          serversWithNoStartCodes.add(sn);
        } else {
          serversWithNoStartCodes.add(ServerName.valueOf(sn.getHostname(), sn.getPort(),
              ServerName.NON_STARTCODE));
        }
      }
      regionToFavoredNodes.put(regionInfo, serversWithNoStartCodes);
    }

    // Lets do a bulk update to meta since that reduces the RPC's
    FavoredNodeAssignmentHelper.updateMetaWithFavoredNodesInfo(
        regionToFavoredNodes,
        masterServices.getConnection());
    deleteFavoredNodesForRegions(regionToFavoredNodes.keySet());

    for (Map.Entry<HRegionInfo, List<ServerName>> entry : regionToFavoredNodes.entrySet()) {
      HRegionInfo regionInfo = entry.getKey();
      List<ServerName> serversWithNoStartCodes = entry.getValue();
      globalFavoredNodesAssignmentPlan.updateFavoredNodesMap(regionInfo, serversWithNoStartCodes);
      addToReplicaLoad(regionInfo, serversWithNoStartCodes);
    }
  }

  private synchronized void addToReplicaLoad(HRegionInfo hri, List<ServerName> servers) {
    ServerName serverToUse = ServerName.valueOf(servers.get(PRIMARY.ordinal()).getHostAndPort(),
        ServerName.NON_STARTCODE);
    List<HRegionInfo> regionList = primaryRSToRegionMap.get(serverToUse);
    if (regionList == null) {
      regionList = new ArrayList<>();
    }
    regionList.add(hri);
    primaryRSToRegionMap.put(serverToUse, regionList);

    serverToUse = ServerName
        .valueOf(servers.get(SECONDARY.ordinal()).getHostAndPort(), ServerName.NON_STARTCODE);
    regionList = secondaryRSToRegionMap.get(serverToUse);
    if (regionList == null) {
      regionList = new ArrayList<>();
    }
    regionList.add(hri);
    secondaryRSToRegionMap.put(serverToUse, regionList);

    serverToUse = ServerName.valueOf(servers.get(TERTIARY.ordinal()).getHostAndPort(),
      ServerName.NON_STARTCODE);
    regionList = teritiaryRSToRegionMap.get(serverToUse);
    if (regionList == null) {
      regionList = new ArrayList<>();
    }
    regionList.add(hri);
    teritiaryRSToRegionMap.put(serverToUse, regionList);
  }

  private synchronized void deleteFavoredNodesForRegions(Collection<HRegionInfo> regionInfoList) {
    for (HRegionInfo hri : regionInfoList) {
      List<ServerName> favNodes = getFavoredNodes(hri);
      if (favNodes != null) {
        if (primaryRSToRegionMap.containsKey(favNodes.get(PRIMARY.ordinal()))) {
          primaryRSToRegionMap.get(favNodes.get(PRIMARY.ordinal())).remove(hri);
        }
        if (secondaryRSToRegionMap.containsKey(favNodes.get(SECONDARY.ordinal()))) {
          secondaryRSToRegionMap.get(favNodes.get(SECONDARY.ordinal())).remove(hri);
        }
        if (teritiaryRSToRegionMap.containsKey(favNodes.get(TERTIARY.ordinal()))) {
          teritiaryRSToRegionMap.get(favNodes.get(TERTIARY.ordinal())).remove(hri);
        }
        globalFavoredNodesAssignmentPlan.removeFavoredNodes(hri);
      }
    }
  }
}
