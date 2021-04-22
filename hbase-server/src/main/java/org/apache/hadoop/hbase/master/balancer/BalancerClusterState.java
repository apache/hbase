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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.master.RackManager;
import org.apache.hadoop.hbase.net.Address;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An efficient array based implementation similar to ClusterState for keeping the status of the
 * cluster in terms of region assignment and distribution. LoadBalancers, such as
 * StochasticLoadBalancer uses this Cluster object because of hundreds of thousands of hashmap
 * manipulations are very costly, which is why this class uses mostly indexes and arrays.
 * <p/>
 * BalancerClusterState tracks a list of unassigned regions, region assignments, and the server
 * topology in terms of server names, hostnames and racks.
 */
@InterfaceAudience.Private
class BalancerClusterState {

  private static final Logger LOG = LoggerFactory.getLogger(BalancerClusterState.class);

  ServerName[] servers;
  // ServerName uniquely identifies a region server. multiple RS can run on the same host
  String[] hosts;
  String[] racks;
  boolean multiServersPerHost = false; // whether or not any host has more than one server

  ArrayList<String> tables;
  RegionInfo[] regions;
  Deque<BalancerRegionLoad>[] regionLoads;
  private RegionLocationFinder regionFinder;

  int[][] regionLocations; // regionIndex -> list of serverIndex sorted by locality

  int[] serverIndexToHostIndex; // serverIndex -> host index
  int[] serverIndexToRackIndex; // serverIndex -> rack index

  int[][] regionsPerServer; // serverIndex -> region list
  int[] serverIndexToRegionsOffset; // serverIndex -> offset of region list
  int[][] regionsPerHost; // hostIndex -> list of regions
  int[][] regionsPerRack; // rackIndex -> region list
  int[][] primariesOfRegionsPerServer; // serverIndex -> sorted list of regions by primary region
                                       // index
  int[][] primariesOfRegionsPerHost; // hostIndex -> sorted list of regions by primary region index
  int[][] primariesOfRegionsPerRack; // rackIndex -> sorted list of regions by primary region index

  int[][] serversPerHost; // hostIndex -> list of server indexes
  int[][] serversPerRack; // rackIndex -> list of server indexes
  int[] regionIndexToServerIndex; // regionIndex -> serverIndex
  int[] initialRegionIndexToServerIndex; // regionIndex -> serverIndex (initial cluster state)
  int[] regionIndexToTableIndex; // regionIndex -> tableIndex
  int[][] numRegionsPerServerPerTable; // serverIndex -> tableIndex -> # regions
  int[] numMaxRegionsPerTable; // tableIndex -> max number of regions in a single RS
  int[] regionIndexToPrimaryIndex; // regionIndex -> regionIndex of the primary
  boolean hasRegionReplicas = false; // whether there is regions with replicas

  Integer[] serverIndicesSortedByRegionCount;
  Integer[] serverIndicesSortedByLocality;

  Map<Address, Integer> serversToIndex;
  Map<String, Integer> hostsToIndex;
  Map<String, Integer> racksToIndex;
  Map<String, Integer> tablesToIndex;
  Map<RegionInfo, Integer> regionsToIndex;
  float[] localityPerServer;

  int numServers;
  int numHosts;
  int numRacks;
  int numTables;
  int numRegions;

  int numMovedRegions = 0; // num moved regions from the initial configuration
  Map<ServerName, List<RegionInfo>> clusterState;

  private final RackManager rackManager;
  // Maps region -> rackIndex -> locality of region on rack
  private float[][] rackLocalities;
  // Maps localityType -> region -> [server|rack]Index with highest locality
  private int[][] regionsToMostLocalEntities;

  static class DefaultRackManager extends RackManager {
    @Override
    public String getRack(ServerName server) {
      return UNKNOWN_RACK;
    }
  }

  BalancerClusterState(Map<ServerName, List<RegionInfo>> clusterState,
    Map<String, Deque<BalancerRegionLoad>> loads, RegionLocationFinder regionFinder,
    RackManager rackManager) {
    this(null, clusterState, loads, regionFinder, rackManager);
  }

  @SuppressWarnings("unchecked")
  BalancerClusterState(Collection<RegionInfo> unassignedRegions,
    Map<ServerName, List<RegionInfo>> clusterState, Map<String, Deque<BalancerRegionLoad>> loads,
    RegionLocationFinder regionFinder, RackManager rackManager) {
    if (unassignedRegions == null) {
      unassignedRegions = Collections.emptyList();
    }

    serversToIndex = new HashMap<>();
    hostsToIndex = new HashMap<>();
    racksToIndex = new HashMap<>();
    tablesToIndex = new HashMap<>();

    // TODO: We should get the list of tables from master
    tables = new ArrayList<>();
    this.rackManager = rackManager != null ? rackManager : new DefaultRackManager();

    numRegions = 0;

    List<List<Integer>> serversPerHostList = new ArrayList<>();
    List<List<Integer>> serversPerRackList = new ArrayList<>();
    this.clusterState = clusterState;
    this.regionFinder = regionFinder;

    // Use servername and port as there can be dead servers in this list. We want everything with
    // a matching hostname and port to have the same index.
    for (ServerName sn : clusterState.keySet()) {
      if (sn == null) {
        LOG.warn("TODO: Enable TRACE on BaseLoadBalancer. Empty servername); " +
          "skipping; unassigned regions?");
        if (LOG.isTraceEnabled()) {
          LOG.trace("EMPTY SERVERNAME " + clusterState.toString());
        }
        continue;
      }
      if (serversToIndex.get(sn.getAddress()) == null) {
        serversToIndex.put(sn.getAddress(), numServers++);
      }
      if (!hostsToIndex.containsKey(sn.getHostname())) {
        hostsToIndex.put(sn.getHostname(), numHosts++);
        serversPerHostList.add(new ArrayList<>(1));
      }

      int serverIndex = serversToIndex.get(sn.getAddress());
      int hostIndex = hostsToIndex.get(sn.getHostname());
      serversPerHostList.get(hostIndex).add(serverIndex);

      String rack = this.rackManager.getRack(sn);
      if (!racksToIndex.containsKey(rack)) {
        racksToIndex.put(rack, numRacks++);
        serversPerRackList.add(new ArrayList<>());
      }
      int rackIndex = racksToIndex.get(rack);
      serversPerRackList.get(rackIndex).add(serverIndex);
    }

    // Count how many regions there are.
    for (Map.Entry<ServerName, List<RegionInfo>> entry : clusterState.entrySet()) {
      numRegions += entry.getValue().size();
    }
    numRegions += unassignedRegions.size();

    regionsToIndex = new HashMap<>(numRegions);
    servers = new ServerName[numServers];
    serversPerHost = new int[numHosts][];
    serversPerRack = new int[numRacks][];
    regions = new RegionInfo[numRegions];
    regionIndexToServerIndex = new int[numRegions];
    initialRegionIndexToServerIndex = new int[numRegions];
    regionIndexToTableIndex = new int[numRegions];
    regionIndexToPrimaryIndex = new int[numRegions];
    regionLoads = new Deque[numRegions];

    regionLocations = new int[numRegions][];
    serverIndicesSortedByRegionCount = new Integer[numServers];
    serverIndicesSortedByLocality = new Integer[numServers];
    localityPerServer = new float[numServers];

    serverIndexToHostIndex = new int[numServers];
    serverIndexToRackIndex = new int[numServers];
    regionsPerServer = new int[numServers][];
    serverIndexToRegionsOffset = new int[numServers];
    regionsPerHost = new int[numHosts][];
    regionsPerRack = new int[numRacks][];
    primariesOfRegionsPerServer = new int[numServers][];
    primariesOfRegionsPerHost = new int[numHosts][];
    primariesOfRegionsPerRack = new int[numRacks][];

    int tableIndex = 0, regionIndex = 0, regionPerServerIndex = 0;

    for (Map.Entry<ServerName, List<RegionInfo>> entry : clusterState.entrySet()) {
      if (entry.getKey() == null) {
        LOG.warn("SERVERNAME IS NULL, skipping " + entry.getValue());
        continue;
      }
      int serverIndex = serversToIndex.get(entry.getKey().getAddress());

      // keep the servername if this is the first server name for this hostname
      // or this servername has the newest startcode.
      if (servers[serverIndex] == null ||
        servers[serverIndex].getStartcode() < entry.getKey().getStartcode()) {
        servers[serverIndex] = entry.getKey();
      }

      if (regionsPerServer[serverIndex] != null) {
        // there is another server with the same hostAndPort in ClusterState.
        // allocate the array for the total size
        regionsPerServer[serverIndex] =
          new int[entry.getValue().size() + regionsPerServer[serverIndex].length];
      } else {
        regionsPerServer[serverIndex] = new int[entry.getValue().size()];
      }
      primariesOfRegionsPerServer[serverIndex] = new int[regionsPerServer[serverIndex].length];
      serverIndicesSortedByRegionCount[serverIndex] = serverIndex;
      serverIndicesSortedByLocality[serverIndex] = serverIndex;
    }

    hosts = new String[numHosts];
    for (Map.Entry<String, Integer> entry : hostsToIndex.entrySet()) {
      hosts[entry.getValue()] = entry.getKey();
    }
    racks = new String[numRacks];
    for (Map.Entry<String, Integer> entry : racksToIndex.entrySet()) {
      racks[entry.getValue()] = entry.getKey();
    }

    for (Map.Entry<ServerName, List<RegionInfo>> entry : clusterState.entrySet()) {
      int serverIndex = serversToIndex.get(entry.getKey().getAddress());
      regionPerServerIndex = serverIndexToRegionsOffset[serverIndex];

      int hostIndex = hostsToIndex.get(entry.getKey().getHostname());
      serverIndexToHostIndex[serverIndex] = hostIndex;

      int rackIndex = racksToIndex.get(this.rackManager.getRack(entry.getKey()));
      serverIndexToRackIndex[serverIndex] = rackIndex;

      for (RegionInfo region : entry.getValue()) {
        registerRegion(region, regionIndex, serverIndex, loads, regionFinder);
        regionsPerServer[serverIndex][regionPerServerIndex++] = regionIndex;
        regionIndex++;
      }
      serverIndexToRegionsOffset[serverIndex] = regionPerServerIndex;
    }

    for (RegionInfo region : unassignedRegions) {
      registerRegion(region, regionIndex, -1, loads, regionFinder);
      regionIndex++;
    }

    for (int i = 0; i < serversPerHostList.size(); i++) {
      serversPerHost[i] = new int[serversPerHostList.get(i).size()];
      for (int j = 0; j < serversPerHost[i].length; j++) {
        serversPerHost[i][j] = serversPerHostList.get(i).get(j);
      }
      if (serversPerHost[i].length > 1) {
        multiServersPerHost = true;
      }
    }

    for (int i = 0; i < serversPerRackList.size(); i++) {
      serversPerRack[i] = new int[serversPerRackList.get(i).size()];
      for (int j = 0; j < serversPerRack[i].length; j++) {
        serversPerRack[i][j] = serversPerRackList.get(i).get(j);
      }
    }

    numTables = tables.size();
    numRegionsPerServerPerTable = new int[numServers][numTables];

    for (int i = 0; i < numServers; i++) {
      for (int j = 0; j < numTables; j++) {
        numRegionsPerServerPerTable[i][j] = 0;
      }
    }

    for (int i = 0; i < regionIndexToServerIndex.length; i++) {
      if (regionIndexToServerIndex[i] >= 0) {
        numRegionsPerServerPerTable[regionIndexToServerIndex[i]][regionIndexToTableIndex[i]]++;
      }
    }

    numMaxRegionsPerTable = new int[numTables];
    for (int[] aNumRegionsPerServerPerTable : numRegionsPerServerPerTable) {
      for (tableIndex = 0; tableIndex < aNumRegionsPerServerPerTable.length; tableIndex++) {
        if (aNumRegionsPerServerPerTable[tableIndex] > numMaxRegionsPerTable[tableIndex]) {
          numMaxRegionsPerTable[tableIndex] = aNumRegionsPerServerPerTable[tableIndex];
        }
      }
    }

    for (int i = 0; i < regions.length; i++) {
      RegionInfo info = regions[i];
      if (RegionReplicaUtil.isDefaultReplica(info)) {
        regionIndexToPrimaryIndex[i] = i;
      } else {
        hasRegionReplicas = true;
        RegionInfo primaryInfo = RegionReplicaUtil.getRegionInfoForDefaultReplica(info);
        regionIndexToPrimaryIndex[i] = regionsToIndex.getOrDefault(primaryInfo, -1);
      }
    }

    for (int i = 0; i < regionsPerServer.length; i++) {
      primariesOfRegionsPerServer[i] = new int[regionsPerServer[i].length];
      for (int j = 0; j < regionsPerServer[i].length; j++) {
        int primaryIndex = regionIndexToPrimaryIndex[regionsPerServer[i][j]];
        primariesOfRegionsPerServer[i][j] = primaryIndex;
      }
      // sort the regions by primaries.
      Arrays.sort(primariesOfRegionsPerServer[i]);
    }

    // compute regionsPerHost
    if (multiServersPerHost) {
      for (int i = 0; i < serversPerHost.length; i++) {
        int numRegionsPerHost = 0;
        for (int j = 0; j < serversPerHost[i].length; j++) {
          numRegionsPerHost += regionsPerServer[serversPerHost[i][j]].length;
        }
        regionsPerHost[i] = new int[numRegionsPerHost];
        primariesOfRegionsPerHost[i] = new int[numRegionsPerHost];
      }
      for (int i = 0; i < serversPerHost.length; i++) {
        int numRegionPerHostIndex = 0;
        for (int j = 0; j < serversPerHost[i].length; j++) {
          for (int k = 0; k < regionsPerServer[serversPerHost[i][j]].length; k++) {
            int region = regionsPerServer[serversPerHost[i][j]][k];
            regionsPerHost[i][numRegionPerHostIndex] = region;
            int primaryIndex = regionIndexToPrimaryIndex[region];
            primariesOfRegionsPerHost[i][numRegionPerHostIndex] = primaryIndex;
            numRegionPerHostIndex++;
          }
        }
        // sort the regions by primaries.
        Arrays.sort(primariesOfRegionsPerHost[i]);
      }
    }

    // compute regionsPerRack
    if (numRacks > 1) {
      for (int i = 0; i < serversPerRack.length; i++) {
        int numRegionsPerRack = 0;
        for (int j = 0; j < serversPerRack[i].length; j++) {
          numRegionsPerRack += regionsPerServer[serversPerRack[i][j]].length;
        }
        regionsPerRack[i] = new int[numRegionsPerRack];
        primariesOfRegionsPerRack[i] = new int[numRegionsPerRack];
      }

      for (int i = 0; i < serversPerRack.length; i++) {
        int numRegionPerRackIndex = 0;
        for (int j = 0; j < serversPerRack[i].length; j++) {
          for (int k = 0; k < regionsPerServer[serversPerRack[i][j]].length; k++) {
            int region = regionsPerServer[serversPerRack[i][j]][k];
            regionsPerRack[i][numRegionPerRackIndex] = region;
            int primaryIndex = regionIndexToPrimaryIndex[region];
            primariesOfRegionsPerRack[i][numRegionPerRackIndex] = primaryIndex;
            numRegionPerRackIndex++;
          }
        }
        // sort the regions by primaries.
        Arrays.sort(primariesOfRegionsPerRack[i]);
      }
    }
  }

  /** Helper for Cluster constructor to handle a region */
  private void registerRegion(RegionInfo region, int regionIndex, int serverIndex,
    Map<String, Deque<BalancerRegionLoad>> loads, RegionLocationFinder regionFinder) {
    String tableName = region.getTable().getNameAsString();
    if (!tablesToIndex.containsKey(tableName)) {
      tables.add(tableName);
      tablesToIndex.put(tableName, tablesToIndex.size());
    }
    int tableIndex = tablesToIndex.get(tableName);

    regionsToIndex.put(region, regionIndex);
    regions[regionIndex] = region;
    regionIndexToServerIndex[regionIndex] = serverIndex;
    initialRegionIndexToServerIndex[regionIndex] = serverIndex;
    regionIndexToTableIndex[regionIndex] = tableIndex;

    // region load
    if (loads != null) {
      Deque<BalancerRegionLoad> rl = loads.get(region.getRegionNameAsString());
      // That could have failed if the RegionLoad is using the other regionName
      if (rl == null) {
        // Try getting the region load using encoded name.
        rl = loads.get(region.getEncodedName());
      }
      regionLoads[regionIndex] = rl;
    }

    if (regionFinder != null) {
      // region location
      List<ServerName> loc = regionFinder.getTopBlockLocations(region);
      regionLocations[regionIndex] = new int[loc.size()];
      for (int i = 0; i < loc.size(); i++) {
        regionLocations[regionIndex][i] = loc.get(i) == null ? -1 :
          (serversToIndex.get(loc.get(i).getAddress()) == null ? -1 :
            serversToIndex.get(loc.get(i).getAddress()));
      }
    }
  }

  /**
   * Returns true iff a given server has less regions than the balanced amount
   */
  public boolean serverHasTooFewRegions(int server) {
    int minLoad = this.numRegions / numServers;
    int numRegions = getNumRegions(server);
    return numRegions < minLoad;
  }

  /**
   * Retrieves and lazily initializes a field storing the locality of every region/server
   * combination
   */
  public float[][] getOrComputeRackLocalities() {
    if (rackLocalities == null || regionsToMostLocalEntities == null) {
      computeCachedLocalities();
    }
    return rackLocalities;
  }

  /**
   * Lazily initializes and retrieves a mapping of region -> server for which region has the highest
   * the locality
   */
  public int[] getOrComputeRegionsToMostLocalEntities(BalancerClusterState.LocalityType type) {
    if (rackLocalities == null || regionsToMostLocalEntities == null) {
      computeCachedLocalities();
    }
    return regionsToMostLocalEntities[type.ordinal()];
  }

  /**
   * Looks up locality from cache of localities. Will create cache if it does not already exist.
   */
  public float getOrComputeLocality(int region, int entity,
    BalancerClusterState.LocalityType type) {
    switch (type) {
      case SERVER:
        return getLocalityOfRegion(region, entity);
      case RACK:
        return getOrComputeRackLocalities()[region][entity];
      default:
        throw new IllegalArgumentException("Unsupported LocalityType: " + type);
    }
  }

  /**
   * Returns locality weighted by region size in MB. Will create locality cache if it does not
   * already exist.
   */
  public double getOrComputeWeightedLocality(int region, int server,
    BalancerClusterState.LocalityType type) {
    return getRegionSizeMB(region) * getOrComputeLocality(region, server, type);
  }

  /**
   * Returns the size in MB from the most recent RegionLoad for region
   */
  public int getRegionSizeMB(int region) {
    Deque<BalancerRegionLoad> load = regionLoads[region];
    // This means regions have no actual data on disk
    if (load == null) {
      return 0;
    }
    return regionLoads[region].getLast().getStorefileSizeMB();
  }

  /**
   * Computes and caches the locality for each region/rack combinations, as well as storing a
   * mapping of region -> server and region -> rack such that server and rack have the highest
   * locality for region
   */
  private void computeCachedLocalities() {
    rackLocalities = new float[numRegions][numRacks];
    regionsToMostLocalEntities = new int[LocalityType.values().length][numRegions];

    // Compute localities and find most local server per region
    for (int region = 0; region < numRegions; region++) {
      int serverWithBestLocality = 0;
      float bestLocalityForRegion = 0;
      for (int server = 0; server < numServers; server++) {
        // Aggregate per-rack locality
        float locality = getLocalityOfRegion(region, server);
        int rack = serverIndexToRackIndex[server];
        int numServersInRack = serversPerRack[rack].length;
        rackLocalities[region][rack] += locality / numServersInRack;

        if (locality > bestLocalityForRegion) {
          serverWithBestLocality = server;
          bestLocalityForRegion = locality;
        }
      }
      regionsToMostLocalEntities[LocalityType.SERVER.ordinal()][region] = serverWithBestLocality;

      // Find most local rack per region
      int rackWithBestLocality = 0;
      float bestRackLocalityForRegion = 0.0f;
      for (int rack = 0; rack < numRacks; rack++) {
        float rackLocality = rackLocalities[region][rack];
        if (rackLocality > bestRackLocalityForRegion) {
          bestRackLocalityForRegion = rackLocality;
          rackWithBestLocality = rack;
        }
      }
      regionsToMostLocalEntities[LocalityType.RACK.ordinal()][region] = rackWithBestLocality;
    }

  }

  /**
   * Maps region index to rack index
   */
  public int getRackForRegion(int region) {
    return serverIndexToRackIndex[regionIndexToServerIndex[region]];
  }

  enum LocalityType {
    SERVER, RACK
  }

  public void doAction(BalanceAction action) {
    switch (action.getType()) {
      case NULL:
        break;
      case ASSIGN_REGION:
        // FindBugs: Having the assert quietens FB BC_UNCONFIRMED_CAST warnings
        assert action instanceof AssignRegionAction : action.getClass();
        AssignRegionAction ar = (AssignRegionAction) action;
        regionsPerServer[ar.getServer()] =
          addRegion(regionsPerServer[ar.getServer()], ar.getRegion());
        regionMoved(ar.getRegion(), -1, ar.getServer());
        break;
      case MOVE_REGION:
        assert action instanceof MoveRegionAction : action.getClass();
        MoveRegionAction mra = (MoveRegionAction) action;
        regionsPerServer[mra.getFromServer()] =
          removeRegion(regionsPerServer[mra.getFromServer()], mra.getRegion());
        regionsPerServer[mra.getToServer()] =
          addRegion(regionsPerServer[mra.getToServer()], mra.getRegion());
        regionMoved(mra.getRegion(), mra.getFromServer(), mra.getToServer());
        break;
      case SWAP_REGIONS:
        assert action instanceof SwapRegionsAction : action.getClass();
        SwapRegionsAction a = (SwapRegionsAction) action;
        regionsPerServer[a.getFromServer()] =
          replaceRegion(regionsPerServer[a.getFromServer()], a.getFromRegion(), a.getToRegion());
        regionsPerServer[a.getToServer()] =
          replaceRegion(regionsPerServer[a.getToServer()], a.getToRegion(), a.getFromRegion());
        regionMoved(a.getFromRegion(), a.getFromServer(), a.getToServer());
        regionMoved(a.getToRegion(), a.getToServer(), a.getFromServer());
        break;
      default:
        throw new RuntimeException("Uknown action:" + action.getType());
    }
  }

  /**
   * Return true if the placement of region on server would lower the availability of the region in
   * question
   * @return true or false
   */
  boolean wouldLowerAvailability(RegionInfo regionInfo, ServerName serverName) {
    if (!serversToIndex.containsKey(serverName.getAddress())) {
      return false; // safeguard against race between cluster.servers and servers from LB method
                    // args
    }
    int server = serversToIndex.get(serverName.getAddress());
    int region = regionsToIndex.get(regionInfo);

    // Region replicas for same region should better assign to different servers
    for (int i : regionsPerServer[server]) {
      RegionInfo otherRegionInfo = regions[i];
      if (RegionReplicaUtil.isReplicasForSameRegion(regionInfo, otherRegionInfo)) {
        return true;
      }
    }

    int primary = regionIndexToPrimaryIndex[region];
    if (primary == -1) {
      return false;
    }
    // there is a subset relation for server < host < rack
    // check server first
    if (contains(primariesOfRegionsPerServer[server], primary)) {
      // check for whether there are other servers that we can place this region
      for (int i = 0; i < primariesOfRegionsPerServer.length; i++) {
        if (i != server && !contains(primariesOfRegionsPerServer[i], primary)) {
          return true; // meaning there is a better server
        }
      }
      return false; // there is not a better server to place this
    }

    // check host
    if (multiServersPerHost) {
      // these arrays would only be allocated if we have more than one server per host
      int host = serverIndexToHostIndex[server];
      if (contains(primariesOfRegionsPerHost[host], primary)) {
        // check for whether there are other hosts that we can place this region
        for (int i = 0; i < primariesOfRegionsPerHost.length; i++) {
          if (i != host && !contains(primariesOfRegionsPerHost[i], primary)) {
            return true; // meaning there is a better host
          }
        }
        return false; // there is not a better host to place this
      }
    }

    // check rack
    if (numRacks > 1) {
      int rack = serverIndexToRackIndex[server];
      if (contains(primariesOfRegionsPerRack[rack], primary)) {
        // check for whether there are other racks that we can place this region
        for (int i = 0; i < primariesOfRegionsPerRack.length; i++) {
          if (i != rack && !contains(primariesOfRegionsPerRack[i], primary)) {
            return true; // meaning there is a better rack
          }
        }
        return false; // there is not a better rack to place this
      }
    }

    return false;
  }

  void doAssignRegion(RegionInfo regionInfo, ServerName serverName) {
    if (!serversToIndex.containsKey(serverName.getAddress())) {
      return;
    }
    int server = serversToIndex.get(serverName.getAddress());
    int region = regionsToIndex.get(regionInfo);
    doAction(new AssignRegionAction(region, server));
  }

  void regionMoved(int region, int oldServer, int newServer) {
    regionIndexToServerIndex[region] = newServer;
    if (initialRegionIndexToServerIndex[region] == newServer) {
      numMovedRegions--; // region moved back to original location
    } else if (oldServer >= 0 && initialRegionIndexToServerIndex[region] == oldServer) {
      numMovedRegions++; // region moved from original location
    }
    int tableIndex = regionIndexToTableIndex[region];
    if (oldServer >= 0) {
      numRegionsPerServerPerTable[oldServer][tableIndex]--;
    }
    numRegionsPerServerPerTable[newServer][tableIndex]++;

    // check whether this caused maxRegionsPerTable in the new Server to be updated
    if (numRegionsPerServerPerTable[newServer][tableIndex] > numMaxRegionsPerTable[tableIndex]) {
      numMaxRegionsPerTable[tableIndex] = numRegionsPerServerPerTable[newServer][tableIndex];
    } else if (oldServer >= 0 && (numRegionsPerServerPerTable[oldServer][tableIndex]
        + 1) == numMaxRegionsPerTable[tableIndex]) {
      // recompute maxRegionsPerTable since the previous value was coming from the old server
      numMaxRegionsPerTable[tableIndex] = 0;
      for (int[] aNumRegionsPerServerPerTable : numRegionsPerServerPerTable) {
        if (aNumRegionsPerServerPerTable[tableIndex] > numMaxRegionsPerTable[tableIndex]) {
          numMaxRegionsPerTable[tableIndex] = aNumRegionsPerServerPerTable[tableIndex];
        }
      }
    }

    // update for servers
    int primary = regionIndexToPrimaryIndex[region];
    if (oldServer >= 0) {
      primariesOfRegionsPerServer[oldServer] =
        removeRegion(primariesOfRegionsPerServer[oldServer], primary);
    }
    primariesOfRegionsPerServer[newServer] =
      addRegionSorted(primariesOfRegionsPerServer[newServer], primary);

    // update for hosts
    if (multiServersPerHost) {
      int oldHost = oldServer >= 0 ? serverIndexToHostIndex[oldServer] : -1;
      int newHost = serverIndexToHostIndex[newServer];
      if (newHost != oldHost) {
        regionsPerHost[newHost] = addRegion(regionsPerHost[newHost], region);
        primariesOfRegionsPerHost[newHost] =
          addRegionSorted(primariesOfRegionsPerHost[newHost], primary);
        if (oldHost >= 0) {
          regionsPerHost[oldHost] = removeRegion(regionsPerHost[oldHost], region);
          primariesOfRegionsPerHost[oldHost] =
            removeRegion(primariesOfRegionsPerHost[oldHost], primary); // will still be sorted
        }
      }
    }

    // update for racks
    if (numRacks > 1) {
      int oldRack = oldServer >= 0 ? serverIndexToRackIndex[oldServer] : -1;
      int newRack = serverIndexToRackIndex[newServer];
      if (newRack != oldRack) {
        regionsPerRack[newRack] = addRegion(regionsPerRack[newRack], region);
        primariesOfRegionsPerRack[newRack] =
          addRegionSorted(primariesOfRegionsPerRack[newRack], primary);
        if (oldRack >= 0) {
          regionsPerRack[oldRack] = removeRegion(regionsPerRack[oldRack], region);
          primariesOfRegionsPerRack[oldRack] =
            removeRegion(primariesOfRegionsPerRack[oldRack], primary); // will still be sorted
        }
      }
    }
  }

  int[] removeRegion(int[] regions, int regionIndex) {
    // TODO: this maybe costly. Consider using linked lists
    int[] newRegions = new int[regions.length - 1];
    int i = 0;
    for (i = 0; i < regions.length; i++) {
      if (regions[i] == regionIndex) {
        break;
      }
      newRegions[i] = regions[i];
    }
    System.arraycopy(regions, i + 1, newRegions, i, newRegions.length - i);
    return newRegions;
  }

  int[] addRegion(int[] regions, int regionIndex) {
    int[] newRegions = new int[regions.length + 1];
    System.arraycopy(regions, 0, newRegions, 0, regions.length);
    newRegions[newRegions.length - 1] = regionIndex;
    return newRegions;
  }

  int[] addRegionSorted(int[] regions, int regionIndex) {
    int[] newRegions = new int[regions.length + 1];
    int i = 0;
    for (i = 0; i < regions.length; i++) { // find the index to insert
      if (regions[i] > regionIndex) {
        break;
      }
    }
    System.arraycopy(regions, 0, newRegions, 0, i); // copy first half
    System.arraycopy(regions, i, newRegions, i + 1, regions.length - i); // copy second half
    newRegions[i] = regionIndex;

    return newRegions;
  }

  int[] replaceRegion(int[] regions, int regionIndex, int newRegionIndex) {
    int i = 0;
    for (i = 0; i < regions.length; i++) {
      if (regions[i] == regionIndex) {
        regions[i] = newRegionIndex;
        break;
      }
    }
    return regions;
  }

  void sortServersByRegionCount() {
    Arrays.sort(serverIndicesSortedByRegionCount, numRegionsComparator);
  }

  int getNumRegions(int server) {
    return regionsPerServer[server].length;
  }

  boolean contains(int[] arr, int val) {
    return Arrays.binarySearch(arr, val) >= 0;
  }

  private Comparator<Integer> numRegionsComparator = Comparator.comparingInt(this::getNumRegions);

  int getLowestLocalityRegionOnServer(int serverIndex) {
    if (regionFinder != null) {
      float lowestLocality = 1.0f;
      int lowestLocalityRegionIndex = -1;
      if (regionsPerServer[serverIndex].length == 0) {
        // No regions on that region server
        return -1;
      }
      for (int j = 0; j < regionsPerServer[serverIndex].length; j++) {
        int regionIndex = regionsPerServer[serverIndex][j];
        HDFSBlocksDistribution distribution =
          regionFinder.getBlockDistribution(regions[regionIndex]);
        float locality = distribution.getBlockLocalityIndex(servers[serverIndex].getHostname());
        // skip empty region
        if (distribution.getUniqueBlocksTotalWeight() == 0) {
          continue;
        }
        if (locality < lowestLocality) {
          lowestLocality = locality;
          lowestLocalityRegionIndex = j;
        }
      }
      if (lowestLocalityRegionIndex == -1) {
        return -1;
      }
      if (LOG.isTraceEnabled()) {
        LOG.trace("Lowest locality region is " +
          regions[regionsPerServer[serverIndex][lowestLocalityRegionIndex]]
            .getRegionNameAsString() +
          " with locality " + lowestLocality + " and its region server contains " +
          regionsPerServer[serverIndex].length + " regions");
      }
      return regionsPerServer[serverIndex][lowestLocalityRegionIndex];
    } else {
      return -1;
    }
  }

  float getLocalityOfRegion(int region, int server) {
    if (regionFinder != null) {
      HDFSBlocksDistribution distribution = regionFinder.getBlockDistribution(regions[region]);
      return distribution.getBlockLocalityIndex(servers[server].getHostname());
    } else {
      return 0f;
    }
  }

  void setNumRegions(int numRegions) {
    this.numRegions = numRegions;
  }

  void setNumMovedRegions(int numMovedRegions) {
    this.numMovedRegions = numMovedRegions;
  }

  @Override
  public String toString() {
    StringBuilder desc = new StringBuilder("Cluster={servers=[");
    for (ServerName sn : servers) {
      desc.append(sn.getAddress().toString()).append(", ");
    }
    desc.append("], serverIndicesSortedByRegionCount=")
      .append(Arrays.toString(serverIndicesSortedByRegionCount)).append(", regionsPerServer=")
      .append(Arrays.deepToString(regionsPerServer));

    desc.append(", numMaxRegionsPerTable=").append(Arrays.toString(numMaxRegionsPerTable))
      .append(", numRegions=").append(numRegions).append(", numServers=").append(numServers)
      .append(", numTables=").append(numTables).append(", numMovedRegions=").append(numMovedRegions)
      .append('}');
    return desc.toString();
  }
}