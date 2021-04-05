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
package org.apache.hadoop.hbase.favored;

import static org.apache.hadoop.hbase.ServerName.NON_STARTCODE;
import static org.apache.hadoop.hbase.favored.FavoredNodeAssignmentHelper.FAVORED_NODES_NUM;
import static org.apache.hadoop.hbase.favored.FavoredNodesPlan.Position.PRIMARY;
import static org.apache.hadoop.hbase.favored.FavoredNodesPlan.Position.SECONDARY;
import static org.apache.hadoop.hbase.favored.FavoredNodesPlan.Position.TERTIARY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RackManager;
import org.apache.hadoop.hbase.master.SnapshotOfRegionAssignmentFromMeta;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.net.NetUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

/**
 * FavoredNodesManager is responsible for maintaining favored nodes info in internal cache and
 * META table. Its the centralized store for all favored nodes information. All reads and updates
 * should be done through this class. There should only be one instance of
 * {@link FavoredNodesManager} in Master. {@link FavoredNodesPlan} and favored node information
 * from {@link SnapshotOfRegionAssignmentFromMeta} should not be used outside this class (except
 * for tools that only read or fortest cases). All other classes including Favored balancers
 * and {@link FavoredNodeAssignmentHelper} should use {@link FavoredNodesManager} for any
 * read/write/deletes to favored nodes.
 */
@InterfaceAudience.Private
public class FavoredNodesManager {
  private static final Logger LOG = LoggerFactory.getLogger(FavoredNodesManager.class);

  private final FavoredNodesPlan globalFavoredNodesAssignmentPlan;
  private final Map<ServerName, List<RegionInfo>> primaryRSToRegionMap;
  private final Map<ServerName, List<RegionInfo>> secondaryRSToRegionMap;
  private final Map<ServerName, List<RegionInfo>> teritiaryRSToRegionMap;

  private final MasterServices masterServices;
  private final RackManager rackManager;

  /**
   * Datanode port to be used for Favored Nodes.
   */
  private int datanodeDataTransferPort;

  public FavoredNodesManager(MasterServices masterServices) {
    this.masterServices = masterServices;
    this.globalFavoredNodesAssignmentPlan = new FavoredNodesPlan();
    this.primaryRSToRegionMap = new HashMap<>();
    this.secondaryRSToRegionMap = new HashMap<>();
    this.teritiaryRSToRegionMap = new HashMap<>();
    this.rackManager = new RackManager(masterServices.getConfiguration());
  }

  public synchronized void initialize(SnapshotOfRegionAssignmentFromMeta snapshot) {
    // Add snapshot to structures made on creation. Current structures may have picked
    // up data between construction and the scan of meta needed before this method
    // is called.  See HBASE-23737 "[Flakey Tests] TestFavoredNodeTableImport fails 30% of the time"
    this.globalFavoredNodesAssignmentPlan.
      updateFavoredNodesMap(snapshot.getExistingAssignmentPlan());
    primaryRSToRegionMap.putAll(snapshot.getPrimaryToRegionInfoMap());
    secondaryRSToRegionMap.putAll(snapshot.getSecondaryToRegionInfoMap());
    teritiaryRSToRegionMap.putAll(snapshot.getTertiaryToRegionInfoMap());
    datanodeDataTransferPort= getDataNodePort();
  }

  public int getDataNodePort() {
    HdfsConfiguration.init();

    Configuration dnConf = new HdfsConfiguration(masterServices.getConfiguration());

    int dnPort = NetUtils.createSocketAddr(
        dnConf.get(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY,
            DFSConfigKeys.DFS_DATANODE_ADDRESS_DEFAULT)).getPort();
    LOG.debug("Loaded default datanode port for FN: " + datanodeDataTransferPort);
    return dnPort;
  }

  public synchronized List<ServerName> getFavoredNodes(RegionInfo regionInfo) {
    return this.globalFavoredNodesAssignmentPlan.getFavoredNodes(regionInfo);
  }

  /*
   * Favored nodes are not applicable for system tables. We will use this to check before
   * we apply any favored nodes logic on a region.
   */
  public static boolean isFavoredNodeApplicable(RegionInfo regionInfo) {
    return !regionInfo.getTable().isSystemTable();
  }

  /**
   * Filter and return regions for which favored nodes is not applicable.
   * @return set of regions for which favored nodes is not applicable
   */
  public static Set<RegionInfo> filterNonFNApplicableRegions(Collection<RegionInfo> regions) {
    return regions.stream().filter(r -> !isFavoredNodeApplicable(r)).collect(Collectors.toSet());
  }

  /*
   * This should only be used when sending FN information to the region servers. Instead of
   * sending the region server port, we use the datanode port. This helps in centralizing the DN
   * port logic in Master. The RS uses the port from the favored node list as hints.
   */
  public synchronized List<ServerName> getFavoredNodesWithDNPort(RegionInfo regionInfo) {
    if (getFavoredNodes(regionInfo) == null) {
      return null;
    }

    List<ServerName> fnWithDNPort = Lists.newArrayList();
    for (ServerName sn : getFavoredNodes(regionInfo)) {
      fnWithDNPort.add(ServerName.valueOf(sn.getHostname(), datanodeDataTransferPort,
        NON_STARTCODE));
    }
    return fnWithDNPort;
  }

  public synchronized void updateFavoredNodes(Map<RegionInfo, List<ServerName>> regionFNMap)
      throws IOException {

    Map<RegionInfo, List<ServerName>> regionToFavoredNodes = new HashMap<>();
    for (Map.Entry<RegionInfo, List<ServerName>> entry : regionFNMap.entrySet()) {
      RegionInfo regionInfo = entry.getKey();
      List<ServerName> servers = entry.getValue();

      /*
       * None of the following error conditions should happen. If it does, there is an issue with
       * favored nodes generation or the regions its called on.
       */
      if (servers.size() != Sets.newHashSet(servers).size()) {
        throw new IOException("Duplicates found: " + servers);
      }

      if (!isFavoredNodeApplicable(regionInfo)) {
        throw new IOException("Can't update FN for a un-applicable region: "
            + regionInfo.getRegionNameAsString() + " with " + servers);
      }

      if (servers.size() != FAVORED_NODES_NUM) {
        throw new IOException("At least " + FAVORED_NODES_NUM
            + " favored nodes should be present for region : " + regionInfo.getEncodedName()
            + " current FN servers:" + servers);
      }

      List<ServerName> serversWithNoStartCodes = Lists.newArrayList();
      for (ServerName sn : servers) {
        if (sn.getStartcode() == NON_STARTCODE) {
          serversWithNoStartCodes.add(sn);
        } else {
          serversWithNoStartCodes.add(ServerName.valueOf(sn.getHostname(), sn.getPort(),
              NON_STARTCODE));
        }
      }
      regionToFavoredNodes.put(regionInfo, serversWithNoStartCodes);
    }

    // Lets do a bulk update to meta since that reduces the RPC's
    FavoredNodeAssignmentHelper.updateMetaWithFavoredNodesInfo(regionToFavoredNodes,
        masterServices.getConnection());
    deleteFavoredNodesForRegions(regionToFavoredNodes.keySet());

    for (Map.Entry<RegionInfo, List<ServerName>> entry : regionToFavoredNodes.entrySet()) {
      RegionInfo regionInfo = entry.getKey();
      List<ServerName> serversWithNoStartCodes = entry.getValue();
      globalFavoredNodesAssignmentPlan.updateFavoredNodesMap(regionInfo, serversWithNoStartCodes);
      addToReplicaLoad(regionInfo, serversWithNoStartCodes);
    }
  }

  private synchronized void addToReplicaLoad(RegionInfo hri, List<ServerName> servers) {
    ServerName serverToUse =
      ServerName.valueOf(servers.get(PRIMARY.ordinal()).getAddress().toString(), NON_STARTCODE);
    List<RegionInfo> regionList = primaryRSToRegionMap.get(serverToUse);
    if (regionList == null) {
      regionList = new ArrayList<>();
    }
    regionList.add(hri);
    primaryRSToRegionMap.put(serverToUse, regionList);

    serverToUse = ServerName
        .valueOf(servers.get(SECONDARY.ordinal()).getAddress(), NON_STARTCODE);
    regionList = secondaryRSToRegionMap.get(serverToUse);
    if (regionList == null) {
      regionList = new ArrayList<>();
    }
    regionList.add(hri);
    secondaryRSToRegionMap.put(serverToUse, regionList);

    serverToUse = ServerName.valueOf(servers.get(TERTIARY.ordinal()).getAddress(),
      NON_STARTCODE);
    regionList = teritiaryRSToRegionMap.get(serverToUse);
    if (regionList == null) {
      regionList = new ArrayList<>();
    }
    regionList.add(hri);
    teritiaryRSToRegionMap.put(serverToUse, regionList);
  }

  /*
   * Get the replica count for the servers provided.
   *
   * For each server, replica count includes three counts for primary, secondary and tertiary.
   * If a server is the primary favored node for 10 regions, secondary for 5 and tertiary
   * for 1, then the list would be [10, 5, 1]. If the server is newly added to the cluster is
   * not a favored node for any region, the replica count would be [0, 0, 0].
   */
  public synchronized Map<ServerName, List<Integer>> getReplicaLoad(List<ServerName> servers) {
    Map<ServerName, List<Integer>> result = Maps.newHashMap();
    for (ServerName sn : servers) {
      ServerName serverWithNoStartCode = ServerName.valueOf(sn.getAddress(), NON_STARTCODE);
      List<Integer> countList = Lists.newArrayList();
      if (primaryRSToRegionMap.containsKey(serverWithNoStartCode)) {
        countList.add(primaryRSToRegionMap.get(serverWithNoStartCode).size());
      } else {
        countList.add(0);
      }
      if (secondaryRSToRegionMap.containsKey(serverWithNoStartCode)) {
        countList.add(secondaryRSToRegionMap.get(serverWithNoStartCode).size());
      } else {
        countList.add(0);
      }
      if (teritiaryRSToRegionMap.containsKey(serverWithNoStartCode)) {
        countList.add(teritiaryRSToRegionMap.get(serverWithNoStartCode).size());
      } else {
        countList.add(0);
      }
      result.put(sn, countList);
    }
    return result;
  }

  public synchronized void deleteFavoredNodesForRegion(RegionInfo regionInfo) {
    List<ServerName> favNodes = getFavoredNodes(regionInfo);
    if (favNodes != null) {
      if (primaryRSToRegionMap.containsKey(favNodes.get(PRIMARY.ordinal()))) {
        primaryRSToRegionMap.get(favNodes.get(PRIMARY.ordinal())).remove(regionInfo);
      }
      if (secondaryRSToRegionMap.containsKey(favNodes.get(SECONDARY.ordinal()))) {
        secondaryRSToRegionMap.get(favNodes.get(SECONDARY.ordinal())).remove(regionInfo);
      }
      if (teritiaryRSToRegionMap.containsKey(favNodes.get(TERTIARY.ordinal()))) {
        teritiaryRSToRegionMap.get(favNodes.get(TERTIARY.ordinal())).remove(regionInfo);
      }
      globalFavoredNodesAssignmentPlan.removeFavoredNodes(regionInfo);
    }
  }

  public synchronized void deleteFavoredNodesForRegions(Collection<RegionInfo> regionInfoList) {
    for (RegionInfo regionInfo : regionInfoList) {
      deleteFavoredNodesForRegion(regionInfo);
    }
  }

  public synchronized Set<RegionInfo> getRegionsOfFavoredNode(ServerName serverName) {
    Set<RegionInfo> regionInfos = Sets.newHashSet();

    ServerName serverToUse = ServerName.valueOf(serverName.getAddress(), NON_STARTCODE);
    if (primaryRSToRegionMap.containsKey(serverToUse)) {
      regionInfos.addAll(primaryRSToRegionMap.get(serverToUse));
    }
    if (secondaryRSToRegionMap.containsKey(serverToUse)) {
      regionInfos.addAll(secondaryRSToRegionMap.get(serverToUse));
    }
    if (teritiaryRSToRegionMap.containsKey(serverToUse)) {
      regionInfos.addAll(teritiaryRSToRegionMap.get(serverToUse));
    }
    return regionInfos;
  }

  public RackManager getRackManager() {
    return rackManager;
  }
}
