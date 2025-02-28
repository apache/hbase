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
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.agrona.collections.Hashing;
import org.agrona.collections.Int2IntCounterMap;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.master.RackManager;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Suppliers;

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
  Int2IntCounterMap[] colocatedReplicaCountsPerServer; // serverIndex -> counts of colocated
  // replicas by primary region index
  Int2IntCounterMap[] colocatedReplicaCountsPerHost; // hostIndex -> counts of colocated replicas by
  // primary region index
  Int2IntCounterMap[] colocatedReplicaCountsPerRack; // rackIndex -> counts of colocated replicas by
  // primary region index

  int[][] serversPerHost; // hostIndex -> list of server indexes
  int[][] serversPerRack; // rackIndex -> list of server indexes
  int[] regionIndexToServerIndex; // regionIndex -> serverIndex
  int[] initialRegionIndexToServerIndex; // regionIndex -> serverIndex (initial cluster state)
  int[] regionIndexToTableIndex; // regionIndex -> tableIndex
  int[][] numRegionsPerServerPerTable; // tableIndex -> serverIndex -> # regions
  int[] numRegionsPerTable; // tableIndex -> region count
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
  int maxReplicas = 1;

  int numMovedRegions = 0; // num moved regions from the initial configuration
  Map<ServerName, List<RegionInfo>> clusterState;

  private final RackManager rackManager;
  // Maps region -> rackIndex -> locality of region on rack
  private float[][] rackLocalities;
  // Maps localityType -> region -> [server|rack]Index with highest locality
  private int[][] regionsToMostLocalEntities;
  // Maps region -> serverIndex -> regionCacheRatio of a region on a server
  private Map<Pair<Integer, Integer>, Float> regionIndexServerIndexRegionCachedRatio;
  // Maps regionIndex -> serverIndex with best region cache ratio
  private int[] regionServerIndexWithBestRegionCachedRatio;
  // Maps regionName -> oldServerName -> cache ratio of the region on the old server
  Map<String, Pair<ServerName, Float>> regionCacheRatioOnOldServerMap;

  private final Supplier<List<Integer>> shuffledServerIndicesSupplier =
    Suppliers.memoizeWithExpiration(() -> {
      Collection<Integer> serverIndices = serversToIndex.values();
      List<Integer> shuffledServerIndices = new ArrayList<>(serverIndices);
      Collections.shuffle(shuffledServerIndices);
      return shuffledServerIndices;
    }, 5, TimeUnit.SECONDS);
  private long stopRequestedAt = Long.MAX_VALUE;

  static class DefaultRackManager extends RackManager {
    @Override
    public String getRack(ServerName server) {
      return UNKNOWN_RACK;
    }
  }

  BalancerClusterState(Map<ServerName, List<RegionInfo>> clusterState,
    Map<String, Deque<BalancerRegionLoad>> loads, RegionLocationFinder regionFinder,
    RackManager rackManager) {
    this(null, clusterState, loads, regionFinder, rackManager, null);
  }

  protected BalancerClusterState(Map<ServerName, List<RegionInfo>> clusterState,
    Map<String, Deque<BalancerRegionLoad>> loads, RegionLocationFinder regionFinder,
    RackManager rackManager, Map<String, Pair<ServerName, Float>> oldRegionServerRegionCacheRatio) {
    this(null, clusterState, loads, regionFinder, rackManager, oldRegionServerRegionCacheRatio);
  }

  @SuppressWarnings("unchecked")
  BalancerClusterState(Collection<RegionInfo> unassignedRegions,
    Map<ServerName, List<RegionInfo>> clusterState, Map<String, Deque<BalancerRegionLoad>> loads,
    RegionLocationFinder regionFinder, RackManager rackManager,
    Map<String, Pair<ServerName, Float>> oldRegionServerRegionCacheRatio) {
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

    this.regionCacheRatioOnOldServerMap = oldRegionServerRegionCacheRatio;

    numRegions = 0;

    List<List<Integer>> serversPerHostList = new ArrayList<>();
    List<List<Integer>> serversPerRackList = new ArrayList<>();
    this.clusterState = clusterState;
    this.regionFinder = regionFinder;

    // Use servername and port as there can be dead servers in this list. We want everything with
    // a matching hostname and port to have the same index.
    for (ServerName sn : clusterState.keySet()) {
      if (sn == null) {
        LOG.warn("TODO: Enable TRACE on BaseLoadBalancer. Empty servername); "
          + "skipping; unassigned regions?");
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
    LOG.debug("Hosts are {} racks are {}", hostsToIndex, racksToIndex);
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
    colocatedReplicaCountsPerServer = new Int2IntCounterMap[numServers];
    colocatedReplicaCountsPerHost = new Int2IntCounterMap[numHosts];
    colocatedReplicaCountsPerRack = new Int2IntCounterMap[numRacks];

    int regionIndex = 0, regionPerServerIndex = 0;

    for (Map.Entry<ServerName, List<RegionInfo>> entry : clusterState.entrySet()) {
      if (entry.getKey() == null) {
        LOG.warn("SERVERNAME IS NULL, skipping " + entry.getValue());
        continue;
      }
      int serverIndex = serversToIndex.get(entry.getKey().getAddress());

      // keep the servername if this is the first server name for this hostname
      // or this servername has the newest startcode.
      if (
        servers[serverIndex] == null
          || servers[serverIndex].getStartcode() < entry.getKey().getStartcode()
      ) {
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
      colocatedReplicaCountsPerServer[serverIndex] =
        new Int2IntCounterMap(regionsPerServer[serverIndex].length, Hashing.DEFAULT_LOAD_FACTOR, 0);
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
        LOG.debug("server {} is on host {}", serversPerHostList.get(i).get(j), i);
      }
      if (serversPerHost[i].length > 1) {
        multiServersPerHost = true;
      }
    }

    for (int i = 0; i < serversPerRackList.size(); i++) {
      serversPerRack[i] = new int[serversPerRackList.get(i).size()];
      for (int j = 0; j < serversPerRack[i].length; j++) {
        serversPerRack[i][j] = serversPerRackList.get(i).get(j);
        LOG.info("server {} is on rack {}", serversPerRackList.get(i).get(j), i);
      }
    }

    numTables = tables.size();
    LOG.debug("Number of tables={}, number of hosts={}, number of racks={}", numTables, numHosts,
      numRacks);
    numRegionsPerServerPerTable = new int[numTables][numServers];
    numRegionsPerTable = new int[numTables];

    for (int i = 0; i < numTables; i++) {
      for (int j = 0; j < numServers; j++) {
        numRegionsPerServerPerTable[i][j] = 0;
      }
    }

    for (int i = 0; i < regionIndexToServerIndex.length; i++) {
      if (regionIndexToServerIndex[i] >= 0) {
        numRegionsPerServerPerTable[regionIndexToTableIndex[i]][regionIndexToServerIndex[i]]++;
        numRegionsPerTable[regionIndexToTableIndex[i]]++;
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
      colocatedReplicaCountsPerServer[i] =
        new Int2IntCounterMap(regionsPerServer[i].length, Hashing.DEFAULT_LOAD_FACTOR, 0);
      for (int j = 0; j < regionsPerServer[i].length; j++) {
        int primaryIndex = regionIndexToPrimaryIndex[regionsPerServer[i][j]];
        colocatedReplicaCountsPerServer[i].getAndIncrement(primaryIndex);
      }
    }
    // compute regionsPerHost
    if (multiServersPerHost) {
      populateRegionPerLocationFromServer(regionsPerHost, colocatedReplicaCountsPerHost,
        serversPerHost);
    }

    // compute regionsPerRack
    if (numRacks > 1) {
      populateRegionPerLocationFromServer(regionsPerRack, colocatedReplicaCountsPerRack,
        serversPerRack);
    }
  }

  private void populateRegionPerLocationFromServer(int[][] regionsPerLocation,
    Int2IntCounterMap[] colocatedReplicaCountsPerLocation, int[][] serversPerLocation) {
    for (int i = 0; i < serversPerLocation.length; i++) {
      int numRegionsPerLocation = 0;
      for (int j = 0; j < serversPerLocation[i].length; j++) {
        numRegionsPerLocation += regionsPerServer[serversPerLocation[i][j]].length;
      }
      regionsPerLocation[i] = new int[numRegionsPerLocation];
      colocatedReplicaCountsPerLocation[i] =
        new Int2IntCounterMap(numRegionsPerLocation, Hashing.DEFAULT_LOAD_FACTOR, 0);
    }

    for (int i = 0; i < serversPerLocation.length; i++) {
      int numRegionPerLocationIndex = 0;
      for (int j = 0; j < serversPerLocation[i].length; j++) {
        for (int k = 0; k < regionsPerServer[serversPerLocation[i][j]].length; k++) {
          int region = regionsPerServer[serversPerLocation[i][j]][k];
          regionsPerLocation[i][numRegionPerLocationIndex] = region;
          int primaryIndex = regionIndexToPrimaryIndex[region];
          colocatedReplicaCountsPerLocation[i].getAndIncrement(primaryIndex);
          numRegionPerLocationIndex++;
        }
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
        regionLocations[regionIndex][i] = loc.get(i) == null
          ? -1
          : (serversToIndex.get(loc.get(i).getAddress()) == null
            ? -1
            : serversToIndex.get(loc.get(i).getAddress()));
      }
    }

    int numReplicas = region.getReplicaId() + 1;
    if (numReplicas > maxReplicas) {
      maxReplicas = numReplicas;
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
   * Returns the size of hFiles from the most recent RegionLoad for region
   */
  public int getTotalRegionHFileSizeMB(int region) {
    Deque<BalancerRegionLoad> load = regionLoads[region];
    if (load == null) {
      // This means, that the region has no actual data on disk
      return 0;
    }
    return regionLoads[region].getLast().getRegionSizeMB();
  }

  /**
   * Returns the weighted cache ratio of a region on the given region server
   */
  public float getOrComputeWeightedRegionCacheRatio(int region, int server) {
    return getTotalRegionHFileSizeMB(region) * getOrComputeRegionCacheRatio(region, server);
  }

  /**
   * Returns the amount by which a region is cached on a given region server. If the region is not
   * currently hosted on the given region server, then find out if it was previously hosted there
   * and return the old cache ratio.
   */
  protected float getRegionCacheRatioOnRegionServer(int region, int regionServerIndex) {
    float regionCacheRatio = 0.0f;

    // Get the current region cache ratio if the region is hosted on the server regionServerIndex
    for (int regionIndex : regionsPerServer[regionServerIndex]) {
      if (region != regionIndex) {
        continue;
      }

      Deque<BalancerRegionLoad> regionLoadList = regionLoads[regionIndex];

      // The region is currently hosted on this region server. Get the region cache ratio for this
      // region on this server
      regionCacheRatio =
        regionLoadList == null ? 0.0f : regionLoadList.getLast().getCurrentRegionCacheRatio();

      return regionCacheRatio;
    }

    // Region is not currently hosted on this server. Check if the region was cached on this
    // server earlier. This can happen when the server was shutdown and the cache was persisted.
    // Search using the region name and server name and not the index id and server id as these ids
    // may change when a server is marked as dead or a new server is added.
    String regionEncodedName = regions[region].getEncodedName();
    ServerName serverName = servers[regionServerIndex];
    if (
      regionCacheRatioOnOldServerMap != null
        && regionCacheRatioOnOldServerMap.containsKey(regionEncodedName)
    ) {
      Pair<ServerName, Float> cacheRatioOfRegionOnServer =
        regionCacheRatioOnOldServerMap.get(regionEncodedName);
      if (ServerName.isSameAddress(cacheRatioOfRegionOnServer.getFirst(), serverName)) {
        regionCacheRatio = cacheRatioOfRegionOnServer.getSecond();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Old cache ratio found for region {} on server {}: {}", regionEncodedName,
            serverName, regionCacheRatio);
        }
      }
    }
    return regionCacheRatio;
  }

  /**
   * Populate the maps containing information about how much a region is cached on a region server.
   */
  private void computeRegionServerRegionCacheRatio() {
    regionIndexServerIndexRegionCachedRatio = new HashMap<>();
    regionServerIndexWithBestRegionCachedRatio = new int[numRegions];

    for (int region = 0; region < numRegions; region++) {
      float bestRegionCacheRatio = 0.0f;
      int serverWithBestRegionCacheRatio = 0;
      for (int server = 0; server < numServers; server++) {
        float regionCacheRatio = getRegionCacheRatioOnRegionServer(region, server);
        if (regionCacheRatio > 0.0f || server == regionIndexToServerIndex[region]) {
          // A region with cache ratio 0 on a server means nothing. Hence, just make a note of
          // cache ratio only if the cache ratio is greater than 0.
          Pair<Integer, Integer> regionServerPair = new Pair<>(region, server);
          regionIndexServerIndexRegionCachedRatio.put(regionServerPair, regionCacheRatio);
        }
        if (regionCacheRatio > bestRegionCacheRatio) {
          serverWithBestRegionCacheRatio = server;
          // If the server currently hosting the region has equal cache ratio to a historical
          // server, consider the current server to keep hosting the region
          bestRegionCacheRatio = regionCacheRatio;
        } else if (
          regionCacheRatio == bestRegionCacheRatio && server == regionIndexToServerIndex[region]
        ) {
          // If two servers have same region cache ratio, then the server currently hosting the
          // region
          // should retain the region
          serverWithBestRegionCacheRatio = server;
        }
      }
      regionServerIndexWithBestRegionCachedRatio[region] = serverWithBestRegionCacheRatio;
      Pair<Integer, Integer> regionServerPair =
        new Pair<>(region, regionIndexToServerIndex[region]);
      float tempRegionCacheRatio = regionIndexServerIndexRegionCachedRatio.get(regionServerPair);
      if (tempRegionCacheRatio > bestRegionCacheRatio) {
        LOG.warn(
          "INVALID CONDITION: region {} on server {} cache ratio {} is greater than the "
            + "best region cache ratio {} on server {}",
          regions[region].getEncodedName(), servers[regionIndexToServerIndex[region]],
          tempRegionCacheRatio, bestRegionCacheRatio, servers[serverWithBestRegionCacheRatio]);
      }
    }
  }

  protected float getOrComputeRegionCacheRatio(int region, int server) {
    if (
      regionServerIndexWithBestRegionCachedRatio == null
        || regionIndexServerIndexRegionCachedRatio.isEmpty()
    ) {
      computeRegionServerRegionCacheRatio();
    }

    Pair<Integer, Integer> regionServerPair = new Pair<>(region, server);
    return regionIndexServerIndexRegionCachedRatio.containsKey(regionServerPair)
      ? regionIndexServerIndexRegionCachedRatio.get(regionServerPair)
      : 0.0f;
  }

  public int[] getOrComputeServerWithBestRegionCachedRatio() {
    if (
      regionServerIndexWithBestRegionCachedRatio == null
        || regionIndexServerIndexRegionCachedRatio.isEmpty()
    ) {
      computeRegionServerRegionCacheRatio();
    }
    return regionServerIndexWithBestRegionCachedRatio;
  }

  /**
   * Maps region index to rack index
   */
  public int getRackForRegion(int region) {
    return serverIndexToRackIndex[regionIndexToServerIndex[region]];
  }

  enum LocalityType {
    SERVER,
    RACK
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
      case MOVE_BATCH:
        assert action instanceof MoveBatchAction : action.getClass();
        MoveBatchAction mba = (MoveBatchAction) action;
        for (int serverIndex : mba.getServerToRegionsToRemove().keySet()) {
          Set<Integer> regionsToRemove = mba.getServerToRegionsToRemove().get(serverIndex);
          regionsPerServer[serverIndex] =
            removeRegions(regionsPerServer[serverIndex], regionsToRemove);
        }
        for (int serverIndex : mba.getServerToRegionsToAdd().keySet()) {
          Set<Integer> regionsToAdd = mba.getServerToRegionsToAdd().get(serverIndex);
          regionsPerServer[serverIndex] = addRegions(regionsPerServer[serverIndex], regionsToAdd);
        }
        for (MoveRegionAction moveRegionAction : mba.getMoveActions()) {
          regionMoved(moveRegionAction.getRegion(), moveRegionAction.getFromServer(),
            moveRegionAction.getToServer());
        }
        break;
      default:
        throw new RuntimeException("Unknown action:" + action.getType());
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
    int result = checkLocationForPrimary(server, colocatedReplicaCountsPerServer, primary);
    if (result != 0) {
      return result > 0;
    }

    // check host
    if (multiServersPerHost) {
      result = checkLocationForPrimary(serverIndexToHostIndex[server],
        colocatedReplicaCountsPerHost, primary);
      if (result != 0) {
        return result > 0;
      }
    }

    // check rack
    if (numRacks > 1) {
      result = checkLocationForPrimary(serverIndexToRackIndex[server],
        colocatedReplicaCountsPerRack, primary);
      if (result != 0) {
        return result > 0;
      }
    }
    return false;
  }

  /**
   * Common method for better solution check.
   * @param colocatedReplicaCountsPerLocation colocatedReplicaCountsPerHost or
   *                                          colocatedReplicaCountsPerRack
   * @return 1 for better, -1 for no better, 0 for unknown
   */
  private int checkLocationForPrimary(int location,
    Int2IntCounterMap[] colocatedReplicaCountsPerLocation, int primary) {
    if (colocatedReplicaCountsPerLocation[location].containsKey(primary)) {
      // check for whether there are other Locations that we can place this region
      for (int i = 0; i < colocatedReplicaCountsPerLocation.length; i++) {
        if (i != location && !colocatedReplicaCountsPerLocation[i].containsKey(primary)) {
          return 1; // meaning there is a better Location
        }
      }
      return -1; // there is not a better Location to place this
    }
    return 0;
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
      numRegionsPerServerPerTable[tableIndex][oldServer]--;
    }
    numRegionsPerServerPerTable[tableIndex][newServer]++;

    // update for servers
    int primary = regionIndexToPrimaryIndex[region];
    if (oldServer >= 0) {
      colocatedReplicaCountsPerServer[oldServer].getAndDecrement(primary);
    }
    colocatedReplicaCountsPerServer[newServer].getAndIncrement(primary);

    // update for hosts
    if (multiServersPerHost) {
      updateForLocation(serverIndexToHostIndex, regionsPerHost, colocatedReplicaCountsPerHost,
        oldServer, newServer, primary, region);
    }

    // update for racks
    if (numRacks > 1) {
      updateForLocation(serverIndexToRackIndex, regionsPerRack, colocatedReplicaCountsPerRack,
        oldServer, newServer, primary, region);
    }
  }

  /**
   * Common method for per host and per Location region index updates when a region is moved.
   * @param serverIndexToLocation             serverIndexToHostIndex or serverIndexToLocationIndex
   * @param regionsPerLocation                regionsPerHost or regionsPerLocation
   * @param colocatedReplicaCountsPerLocation colocatedReplicaCountsPerHost or
   *                                          colocatedReplicaCountsPerRack
   */
  private void updateForLocation(int[] serverIndexToLocation, int[][] regionsPerLocation,
    Int2IntCounterMap[] colocatedReplicaCountsPerLocation, int oldServer, int newServer,
    int primary, int region) {
    int oldLocation = oldServer >= 0 ? serverIndexToLocation[oldServer] : -1;
    int newLocation = serverIndexToLocation[newServer];
    if (newLocation != oldLocation) {
      regionsPerLocation[newLocation] = addRegion(regionsPerLocation[newLocation], region);
      colocatedReplicaCountsPerLocation[newLocation].getAndIncrement(primary);
      if (oldLocation >= 0) {
        regionsPerLocation[oldLocation] = removeRegion(regionsPerLocation[oldLocation], region);
        colocatedReplicaCountsPerLocation[oldLocation].getAndDecrement(primary);
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

  int[] removeRegions(int[] regions, Set<Integer> regionIndicesToRemove) {
    // Calculate the size of the new regions array
    int newSize = regions.length - regionIndicesToRemove.size();
    if (newSize < 0) {
      throw new IllegalStateException(
        "Region indices mismatch: more regions to remove than in the regions array");
    }

    int[] newRegions = new int[newSize];
    int newIndex = 0;

    // Copy only the regions not in the removal set
    for (int region : regions) {
      if (!regionIndicesToRemove.contains(region)) {
        newRegions[newIndex++] = region;
      }
    }

    // If the newIndex is smaller than newSize, some regions were missing from the input array
    if (newIndex != newSize) {
      throw new IllegalStateException("Region indices mismatch: some regions in the removal "
        + "set were not found in the regions array");
    }

    return newRegions;
  }

  int[] addRegions(int[] regions, Set<Integer> regionIndicesToAdd) {
    int[] newRegions = new int[regions.length + regionIndicesToAdd.size()];

    // Copy the existing regions to the new array
    System.arraycopy(regions, 0, newRegions, 0, regions.length);

    // Add the new regions at the end of the array
    int newIndex = regions.length;
    for (int regionIndex : regionIndicesToAdd) {
      newRegions[newIndex++] = regionIndex;
    }

    return newRegions;
  }

  List<Integer> getShuffledServerIndices() {
    return shuffledServerIndicesSupplier.get();
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

  public Comparator<Integer> getNumRegionsComparator() {
    return numRegionsComparator;
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
        LOG.trace("Lowest locality region is "
          + regions[regionsPerServer[serverIndex][lowestLocalityRegionIndex]]
            .getRegionNameAsString()
          + " with locality " + lowestLocality + " and its region server contains "
          + regionsPerServer[serverIndex].length + " regions");
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

  public int getMaxReplicas() {
    return maxReplicas;
  }

  void setStopRequestedAt(long stopRequestedAt) {
    this.stopRequestedAt = stopRequestedAt;
  }

  long getStopRequestedAt() {
    return stopRequestedAt;
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

    desc.append(", numRegions=").append(numRegions).append(", numServers=").append(numServers)
      .append(", numTables=").append(numTables).append(", numMovedRegions=").append(numMovedRegions)
      .append('}');
    return desc.toString();
  }
}
