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

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RackManager;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer.Cluster.Action.Type;
import org.apache.hadoop.hbase.net.Address;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Joiner;
import org.apache.hbase.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

/**
 * The base class for load balancers. It provides the the functions used to by
 * {@link org.apache.hadoop.hbase.master.assignment.AssignmentManager} to assign regions
 * in the edge cases. It doesn't provide an implementation of the
 * actual balancing algorithm.
 *
 */
@InterfaceAudience.Private
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="IS2_INCONSISTENT_SYNC",
  justification="Complaint is about isByTable not being synchronized; we don't modify often")
public abstract class BaseLoadBalancer implements LoadBalancer {

  public static final String BALANCER_DECISION_BUFFER_ENABLED =
    "hbase.master.balancer.decision.buffer.enabled";
  public static final boolean DEFAULT_BALANCER_DECISION_BUFFER_ENABLED = false;

  public static final String BALANCER_REJECTION_BUFFER_ENABLED =
    "hbase.master.balancer.rejection.buffer.enabled";
  public static final boolean DEFAULT_BALANCER_REJECTION_BUFFER_ENABLED = false;

  protected static final int MIN_SERVER_BALANCE = 2;
  private volatile boolean stopped = false;

  private static final List<RegionInfo> EMPTY_REGION_LIST = Collections.emptyList();

  static final Predicate<ServerMetrics> IDLE_SERVER_PREDICATOR
    = load -> load.getRegionMetrics().isEmpty();

  protected RegionLocationFinder regionFinder;
  protected boolean useRegionFinder;
  protected boolean isByTable = false;

  private static class DefaultRackManager extends RackManager {
    @Override
    public String getRack(ServerName server) {
      return UNKNOWN_RACK;
    }
  }

  /**
   * The constructor that uses the basic MetricsBalancer
   */
  protected BaseLoadBalancer() {
    this(null);
  }

  /**
   * This Constructor accepts an instance of MetricsBalancer,
   * which will be used instead of creating a new one
   */
  protected BaseLoadBalancer(MetricsBalancer metricsBalancer) {
    this.metricsBalancer = (metricsBalancer != null) ? metricsBalancer : new MetricsBalancer();
    createRegionFinder();
  }

  private void createRegionFinder() {
    useRegionFinder = config.getBoolean("hbase.master.balancer.uselocality", true);
    if (useRegionFinder) {
      regionFinder = new RegionLocationFinder();
    }
  }

  /**
   * An efficient array based implementation similar to ClusterState for keeping
   * the status of the cluster in terms of region assignment and distribution.
   * LoadBalancers, such as StochasticLoadBalancer uses this Cluster object because of
   * hundreds of thousands of hashmap manipulations are very costly, which is why this
   * class uses mostly indexes and arrays.
   *
   * Cluster tracks a list of unassigned regions, region assignments, and the server
   * topology in terms of server names, hostnames and racks.
   */
  protected static class Cluster {
    ServerName[] servers;
    String[] hosts; // ServerName uniquely identifies a region server. multiple RS can run on the same host
    String[] racks;
    boolean multiServersPerHost = false; // whether or not any host has more than one server

    ArrayList<String> tables;
    RegionInfo[] regions;
    Deque<BalancerRegionLoad>[] regionLoads;
    private RegionLocationFinder regionFinder;

    int[][] regionLocations; //regionIndex -> list of serverIndex sorted by locality

    int[]   serverIndexToHostIndex;      //serverIndex -> host index
    int[]   serverIndexToRackIndex;      //serverIndex -> rack index

    int[][] regionsPerServer;            //serverIndex -> region list
    int[]   serverIndexToRegionsOffset;  //serverIndex -> offset of region list
    int[][] regionsPerHost;              //hostIndex -> list of regions
    int[][] regionsPerRack;              //rackIndex -> region list
    int[][] primariesOfRegionsPerServer; //serverIndex -> sorted list of regions by primary region index
    int[][] primariesOfRegionsPerHost;   //hostIndex -> sorted list of regions by primary region index
    int[][] primariesOfRegionsPerRack;   //rackIndex -> sorted list of regions by primary region index

    int[][] serversPerHost;              //hostIndex -> list of server indexes
    int[][] serversPerRack;              //rackIndex -> list of server indexes
    int[]   regionIndexToServerIndex;    //regionIndex -> serverIndex
    int[]   initialRegionIndexToServerIndex;    //regionIndex -> serverIndex (initial cluster state)
    int[]   regionIndexToTableIndex;     //regionIndex -> tableIndex
    int[][] numRegionsPerServerPerTable; // tableIndex -> serverIndex -> # regions
    int[] numRegionsPerTable; // tableIndex -> region count
    int[]   regionIndexToPrimaryIndex;   //regionIndex -> regionIndex of the primary
    boolean hasRegionReplicas = false;   //whether there is regions with replicas

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

    int numMovedRegions = 0; //num moved regions from the initial configuration
    Map<ServerName, List<RegionInfo>> clusterState;

    protected final RackManager rackManager;
    // Maps region -> rackIndex -> locality of region on rack
    private float[][] rackLocalities;
    // Maps localityType -> region -> [server|rack]Index with highest locality
    private int[][] regionsToMostLocalEntities;

    protected Cluster(
        Map<ServerName, List<RegionInfo>> clusterState,
        Map<String, Deque<BalancerRegionLoad>> loads,
        RegionLocationFinder regionFinder,
        RackManager rackManager) {
      this(null, clusterState, loads, regionFinder, rackManager);
    }

    @SuppressWarnings("unchecked")
    protected Cluster(
        Collection<RegionInfo> unassignedRegions,
        Map<ServerName, List<RegionInfo>> clusterState,
        Map<String, Deque<BalancerRegionLoad>> loads,
        RegionLocationFinder regionFinder,
        RackManager rackManager) {

      if (unassignedRegions == null) {
        unassignedRegions = EMPTY_REGION_LIST;
      }

      serversToIndex = new HashMap<>();
      hostsToIndex = new HashMap<>();
      racksToIndex = new HashMap<>();
      tablesToIndex = new HashMap<>();

      //TODO: We should get the list of tables from master
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
      for (Entry<ServerName, List<RegionInfo>> entry : clusterState.entrySet()) {
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

      for (Entry<ServerName, List<RegionInfo>> entry : clusterState.entrySet()) {
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
          regionsPerServer[serverIndex] = new int[entry.getValue().size() + regionsPerServer[serverIndex].length];
        } else {
          regionsPerServer[serverIndex] = new int[entry.getValue().size()];
        }
        primariesOfRegionsPerServer[serverIndex] = new int[regionsPerServer[serverIndex].length];
        serverIndicesSortedByRegionCount[serverIndex] = serverIndex;
        serverIndicesSortedByLocality[serverIndex] = serverIndex;
      }

      hosts = new String[numHosts];
      for (Entry<String, Integer> entry : hostsToIndex.entrySet()) {
        hosts[entry.getValue()] = entry.getKey();
      }
      racks = new String[numRacks];
      for (Entry<String, Integer> entry : racksToIndex.entrySet()) {
        racks[entry.getValue()] = entry.getKey();
      }

      LOG.debug("Hosts are {} racks are {}", hostsToIndex, racksToIndex);
      for (Entry<ServerName, List<RegionInfo>> entry : clusterState.entrySet()) {
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
          LOG.debug("server {} is on host {}",serversPerHostList.get(i).get(j), i);
        }
        if (serversPerHost[i].length > 1) {
          multiServersPerHost = true;
        }
      }

      for (int i = 0; i < serversPerRackList.size(); i++) {
        serversPerRack[i] = new int[serversPerRackList.get(i).size()];
        for (int j = 0; j < serversPerRack[i].length; j++) {
          serversPerRack[i][j] = serversPerRackList.get(i).get(j);
          LOG.trace("server {} is on rack {}",serversPerRackList.get(i).get(j), i);
        }
      }

      numTables = tables.size();
      LOG.debug("Number of tables={}, number of hosts={}, number of racks={}", numTables,
        numHosts, numRacks);
      numRegionsPerServerPerTable = new int[numTables][numServers];
      numRegionsPerTable = new int[numTables];

      for (int i = 0; i < numTables; i++) {
        for (int j = 0; j < numServers; j++) {
          numRegionsPerServerPerTable[i][j] = 0;
        }
      }

      for (int i=0; i < regionIndexToServerIndex.length; i++) {
        if (regionIndexToServerIndex[i] >= 0) {
          numRegionsPerServerPerTable[regionIndexToTableIndex[i]][regionIndexToServerIndex[i]]++;
          numRegionsPerTable[regionIndexToTableIndex[i]]++;
        }
      }

      for (int i = 0; i < regions.length; i ++) {
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
        for (int i = 0 ; i < serversPerHost.length; i++) {
          int numRegionsPerHost = 0;
          for (int j = 0; j < serversPerHost[i].length; j++) {
            numRegionsPerHost += regionsPerServer[serversPerHost[i][j]].length;
          }
          regionsPerHost[i] = new int[numRegionsPerHost];
          primariesOfRegionsPerHost[i] = new int[numRegionsPerHost];
        }
        for (int i = 0 ; i < serversPerHost.length; i++) {
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
        for (int i = 0 ; i < serversPerRack.length; i++) {
          int numRegionsPerRack = 0;
          for (int j = 0; j < serversPerRack[i].length; j++) {
            numRegionsPerRack += regionsPerServer[serversPerRack[i][j]].length;
          }
          regionsPerRack[i] = new int[numRegionsPerRack];
          primariesOfRegionsPerRack[i] = new int[numRegionsPerRack];
        }

        for (int i = 0 ; i < serversPerRack.length; i++) {
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
    private void registerRegion(RegionInfo region, int regionIndex,
        int serverIndex, Map<String, Deque<BalancerRegionLoad>> loads,
        RegionLocationFinder regionFinder) {
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
          regionLocations[regionIndex][i] = loc.get(i) == null ? -1
              : (serversToIndex.get(loc.get(i).getAddress()) == null ? -1
                  : serversToIndex.get(loc.get(i).getAddress()));
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
     * Retrieves and lazily initializes a field storing the locality of
     * every region/server combination
     */
    public float[][] getOrComputeRackLocalities() {
      if (rackLocalities == null || regionsToMostLocalEntities == null) {
        computeCachedLocalities();
      }
      return rackLocalities;
    }

    /**
     * Lazily initializes and retrieves a mapping of region -> server for which region has
     * the highest the locality
     */
    public int[] getOrComputeRegionsToMostLocalEntities(LocalityType type) {
      if (rackLocalities == null || regionsToMostLocalEntities == null) {
        computeCachedLocalities();
      }
      return regionsToMostLocalEntities[type.ordinal()];
    }

    /**
     * Looks up locality from cache of localities. Will create cache if it does
     * not already exist.
     */
    public float getOrComputeLocality(int region, int entity, LocalityType type) {
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
     * Returns locality weighted by region size in MB. Will create locality cache
     * if it does not already exist.
     */
    public double getOrComputeWeightedLocality(int region, int server, LocalityType type) {
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
     * Computes and caches the locality for each region/rack combinations,
     * as well as storing a mapping of region -> server and region -> rack such that server
     * and rack have the highest locality for region
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
      SERVER,
      RACK
    }

    /** An action to move or swap a region */
    public static class Action {
      public enum Type {
        ASSIGN_REGION,
        MOVE_REGION,
        SWAP_REGIONS,
        NULL,
      }

      public Type type;
      public Action (Type type) {this.type = type;}
      /** Returns an Action which would undo this action */
      public Action undoAction() { return this; }
      @Override
      public String toString() { return type + ":";}
    }

    public static class AssignRegionAction extends Action {
      public int region;
      public int server;
      public AssignRegionAction(int region, int server) {
        super(Type.ASSIGN_REGION);
        this.region = region;
        this.server = server;
      }
      @Override
      public Action undoAction() {
        // TODO implement this. This action is not being used by the StochasticLB for now
        // in case it uses it, we should implement this function.
        throw new NotImplementedException(HConstants.NOT_IMPLEMENTED);
      }
      @Override
      public String toString() {
        return type + ": " + region + ":" + server;
      }
    }

    public static class MoveRegionAction extends Action {
      public int region;
      public int fromServer;
      public int toServer;

      public MoveRegionAction(int region, int fromServer, int toServer) {
        super(Type.MOVE_REGION);
        this.fromServer = fromServer;
        this.region = region;
        this.toServer = toServer;
      }
      @Override
      public Action undoAction() {
        return new MoveRegionAction (region, toServer, fromServer);
      }
      @Override
      public String toString() {
        return type + ": " + region + ":" + fromServer + " -> " + toServer;
      }
    }

    public static class SwapRegionsAction extends Action {
      public int fromServer;
      public int fromRegion;
      public int toServer;
      public int toRegion;
      public SwapRegionsAction(int fromServer, int fromRegion, int toServer, int toRegion) {
        super(Type.SWAP_REGIONS);
        this.fromServer = fromServer;
        this.fromRegion = fromRegion;
        this.toServer = toServer;
        this.toRegion = toRegion;
      }
      @Override
      public Action undoAction() {
        return new SwapRegionsAction (fromServer, toRegion, toServer, fromRegion);
      }
      @Override
      public String toString() {
        return type + ": " + fromRegion + ":" + fromServer + " <-> " + toRegion + ":" + toServer;
      }
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NM_FIELD_NAMING_CONVENTION",
        justification="Mistake. Too disruptive to change now")
    public static final Action NullAction = new Action(Type.NULL);

    public void doAction(Action action) {
      switch (action.type) {
      case NULL: break;
      case ASSIGN_REGION:
        // FindBugs: Having the assert quietens FB BC_UNCONFIRMED_CAST warnings
        assert action instanceof AssignRegionAction: action.getClass();
        AssignRegionAction ar = (AssignRegionAction) action;
        regionsPerServer[ar.server] = addRegion(regionsPerServer[ar.server], ar.region);
        regionMoved(ar.region, -1, ar.server);
        break;
      case MOVE_REGION:
        assert action instanceof MoveRegionAction: action.getClass();
        MoveRegionAction mra = (MoveRegionAction) action;
        regionsPerServer[mra.fromServer] = removeRegion(regionsPerServer[mra.fromServer], mra.region);
        regionsPerServer[mra.toServer] = addRegion(regionsPerServer[mra.toServer], mra.region);
        regionMoved(mra.region, mra.fromServer, mra.toServer);
        break;
      case SWAP_REGIONS:
        assert action instanceof SwapRegionsAction: action.getClass();
        SwapRegionsAction a = (SwapRegionsAction) action;
        regionsPerServer[a.fromServer] = replaceRegion(regionsPerServer[a.fromServer], a.fromRegion, a.toRegion);
        regionsPerServer[a.toServer] = replaceRegion(regionsPerServer[a.toServer], a.toRegion, a.fromRegion);
        regionMoved(a.fromRegion, a.fromServer, a.toServer);
        regionMoved(a.toRegion, a.toServer, a.fromServer);
        break;
      default:
        throw new RuntimeException("Uknown action:" + action.type);
      }
    }

    /**
     * Return true if the placement of region on server would lower the availability
     * of the region in question
     * @return true or false
     */
    boolean wouldLowerAvailability(RegionInfo regionInfo, ServerName serverName) {
      if (!serversToIndex.containsKey(serverName.getAddress())) {
        return false; // safeguard against race between cluster.servers and servers from LB method args
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
        numMovedRegions--; //region moved back to original location
      } else if (oldServer >= 0 && initialRegionIndexToServerIndex[region] == oldServer) {
        numMovedRegions++; //region moved from original location
      }
      int tableIndex = regionIndexToTableIndex[region];
      if (oldServer >= 0) {
        numRegionsPerServerPerTable[tableIndex][oldServer]--;
      }
      numRegionsPerServerPerTable[tableIndex][newServer]++;
      // update for servers
      int primary = regionIndexToPrimaryIndex[region];
      if (oldServer >= 0) {
        primariesOfRegionsPerServer[oldServer] = removeRegion(
          primariesOfRegionsPerServer[oldServer], primary);
      }
      primariesOfRegionsPerServer[newServer] = addRegionSorted(
        primariesOfRegionsPerServer[newServer], primary);

      // update for hosts
      if (multiServersPerHost) {
        int oldHost = oldServer >= 0 ? serverIndexToHostIndex[oldServer] : -1;
        int newHost = serverIndexToHostIndex[newServer];
        if (newHost != oldHost) {
          regionsPerHost[newHost] = addRegion(regionsPerHost[newHost], region);
          primariesOfRegionsPerHost[newHost] = addRegionSorted(primariesOfRegionsPerHost[newHost], primary);
          if (oldHost >= 0) {
            regionsPerHost[oldHost] = removeRegion(regionsPerHost[oldHost], region);
            primariesOfRegionsPerHost[oldHost] = removeRegion(
              primariesOfRegionsPerHost[oldHost], primary); // will still be sorted
          }
        }
      }

      // update for racks
      if (numRacks > 1) {
        int oldRack = oldServer >= 0 ? serverIndexToRackIndex[oldServer] : -1;
        int newRack = serverIndexToRackIndex[newServer];
        if (newRack != oldRack) {
          regionsPerRack[newRack] = addRegion(regionsPerRack[newRack], region);
          primariesOfRegionsPerRack[newRack] = addRegionSorted(primariesOfRegionsPerRack[newRack], primary);
          if (oldRack >= 0) {
            regionsPerRack[oldRack] = removeRegion(regionsPerRack[oldRack], region);
            primariesOfRegionsPerRack[oldRack] = removeRegion(
              primariesOfRegionsPerRack[oldRack], primary); // will still be sorted
          }
        }
      }
    }

    int[] removeRegion(int[] regions, int regionIndex) {
      //TODO: this maybe costly. Consider using linked lists
      int[] newRegions = new int[regions.length - 1];
      int i = 0;
      for (i = 0; i < regions.length; i++) {
        if (regions[i] == regionIndex) {
          break;
        }
        newRegions[i] = regions[i];
      }
      System.arraycopy(regions, i+1, newRegions, i, newRegions.length - i);
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
      System.arraycopy(regions, i, newRegions, i+1, regions.length - i); // copy second half
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

    public Comparator<Integer> getNumRegionsComparator() {
      return numRegionsComparator;
    }

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
          HDFSBlocksDistribution distribution = regionFinder
              .getBlockDistribution(regions[regionIndex]);
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
                  .getRegionNameAsString() + " with locality " + lowestLocality
              + " and its region server contains " + regionsPerServer[serverIndex].length
              + " regions");
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

    protected void setNumRegions(int numRegions) {
      this.numRegions = numRegions;
    }

    protected void setNumMovedRegions(int numMovedRegions) {
      this.numMovedRegions = numMovedRegions;
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="SBSC_USE_STRINGBUFFER_CONCATENATION",
        justification="Not important but should be fixed")
    @Override
    public String toString() {
      StringBuilder desc = new StringBuilder("Cluster={servers=[");
      for(ServerName sn:servers) {
        desc.append(sn.getAddress().toString()).append(", ");
      }
      desc.append("], serverIndicesSortedByRegionCount=")
          .append(Arrays.toString(serverIndicesSortedByRegionCount))
          .append(", regionsPerServer=").append(Arrays.deepToString(regionsPerServer));

      desc.append(", numRegions=").append(numRegions).append(", numServers=").append(numServers)
        .append(", numTables=").append(numTables).append(", numMovedRegions=")
          .append(numMovedRegions).append('}');
      return desc.toString();
    }
  }

  // slop for regions
  protected float slop;
  // overallSlop to control simpleLoadBalancer's cluster level threshold
  protected float overallSlop;
  protected Configuration config = HBaseConfiguration.create();
  protected RackManager rackManager;
  private static final Logger LOG = LoggerFactory.getLogger(BaseLoadBalancer.class);
  protected MetricsBalancer metricsBalancer = null;
  protected ClusterMetrics clusterStatus = null;
  protected ServerName masterServerName;
  protected MasterServices services;

  /**
   * @deprecated since 2.4.0, will be removed in 3.0.0.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-15549">HBASE-15549</a>
   */
  @Deprecated
  protected boolean onlySystemTablesOnMaster;

  @Override
  public void setConf(Configuration conf) {
    this.config = conf;
    setSlop(conf);
    if (slop < 0) {
      slop = 0;
    } else if (slop > 1) {
      slop = 1;
    }

    if (overallSlop < 0) {
      overallSlop = 0;
    } else if (overallSlop > 1) {
      overallSlop = 1;
    }

    this.onlySystemTablesOnMaster = LoadBalancer.isSystemTablesOnlyOnMaster(this.config);

    this.rackManager = new RackManager(getConf());
    useRegionFinder = config.getBoolean("hbase.master.balancer.uselocality", true);
    if (useRegionFinder) {
      regionFinder = new RegionLocationFinder();
      regionFinder.setConf(conf);
    }
    this.isByTable = conf.getBoolean(HConstants.HBASE_MASTER_LOADBALANCE_BYTABLE, isByTable);
    // Print out base configs. Don't print overallSlop since it for simple balancer exclusively.
    LOG.info("slop={}, systemTablesOnMaster={}", this.slop, this.onlySystemTablesOnMaster);
  }

  protected void setSlop(Configuration conf) {
    this.slop = conf.getFloat("hbase.regions.slop", (float) 0.2);
    this.overallSlop = conf.getFloat("hbase.regions.overallSlop", slop);
  }

  /**
   * Check if a region belongs to some system table.
   * If so, the primary replica may be expected to be put on the master regionserver.
   *
   * @deprecated since 2.4.0, will be removed in 3.0.0.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-15549">HBASE-15549</a>
   */
  @Deprecated
  public boolean shouldBeOnMaster(RegionInfo region) {
    return this.onlySystemTablesOnMaster && region.getTable().isSystemTable();
  }

  /**
   * Balance the regions that should be on master regionserver.
   *
   * @deprecated since 2.4.0, will be removed in 3.0.0.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-15549">HBASE-15549</a>
   */
  @Deprecated
  protected List<RegionPlan> balanceMasterRegions(Map<ServerName, List<RegionInfo>> clusterMap) {
    if (masterServerName == null || clusterMap == null || clusterMap.size() <= 1) return null;
    List<RegionPlan> plans = null;
    List<RegionInfo> regions = clusterMap.get(masterServerName);
    if (regions != null) {
      Iterator<ServerName> keyIt = null;
      for (RegionInfo region: regions) {
        if (shouldBeOnMaster(region)) continue;

        // Find a non-master regionserver to host the region
        if (keyIt == null || !keyIt.hasNext()) {
          keyIt = clusterMap.keySet().iterator();
        }
        ServerName dest = keyIt.next();
        if (masterServerName.equals(dest)) {
          if (!keyIt.hasNext()) {
            keyIt = clusterMap.keySet().iterator();
          }
          dest = keyIt.next();
        }

        // Move this region away from the master regionserver
        RegionPlan plan = new RegionPlan(region, masterServerName, dest);
        if (plans == null) {
          plans = new ArrayList<>();
        }
        plans.add(plan);
      }
    }
    for (Map.Entry<ServerName, List<RegionInfo>> server: clusterMap.entrySet()) {
      if (masterServerName.equals(server.getKey())) continue;
      for (RegionInfo region: server.getValue()) {
        if (!shouldBeOnMaster(region)) continue;

        // Move this region to the master regionserver
        RegionPlan plan = new RegionPlan(region, server.getKey(), masterServerName);
        if (plans == null) {
          plans = new ArrayList<>();
        }
        plans.add(plan);
      }
    }
    return plans;
  }

  /**
   * If master is configured to carry system tables only, in here is
   * where we figure what to assign it.
   *
   * @deprecated since 2.4.0, will be removed in 3.0.0.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-15549">HBASE-15549</a>
   */
  @Deprecated
  @NonNull
  protected Map<ServerName, List<RegionInfo>> assignMasterSystemRegions(
      Collection<RegionInfo> regions, List<ServerName> servers) {
    Map<ServerName, List<RegionInfo>> assignments = new TreeMap<>();
    if (this.onlySystemTablesOnMaster) {
      if (masterServerName != null && servers.contains(masterServerName)) {
        assignments.put(masterServerName, new ArrayList<>());
        for (RegionInfo region : regions) {
          if (shouldBeOnMaster(region)) {
            assignments.get(masterServerName).add(region);
          }
        }
      }
    }
    return assignments;
  }

  @Override
  public Configuration getConf() {
    return this.config;
  }

  @Override
  public synchronized void setClusterMetrics(ClusterMetrics st) {
    this.clusterStatus = st;
    if (useRegionFinder) {
      regionFinder.setClusterMetrics(st);
    }
  }


  @Override
  public void setMasterServices(MasterServices masterServices) {
    masterServerName = masterServices.getServerName();
    this.services = masterServices;
    if (useRegionFinder) {
      this.regionFinder.setServices(masterServices);
    }
  }

  @Override
  public void postMasterStartupInitialize() {
    if (services != null && regionFinder != null) {
      try {
        Set<RegionInfo> regions =
            services.getAssignmentManager().getRegionStates().getRegionAssignments().keySet();
        regionFinder.refreshAndWait(regions);
      } catch (Exception e) {
        LOG.warn("Refreshing region HDFS Block dist failed with exception, ignoring", e);
      }
    }
  }

  public void setRackManager(RackManager rackManager) {
    this.rackManager = rackManager;
  }

  protected boolean needsBalance(TableName tableName, Cluster c) {
    ClusterLoadState cs = new ClusterLoadState(c.clusterState);
    if (cs.getNumServers() < MIN_SERVER_BALANCE) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Not running balancer because only " + cs.getNumServers()
            + " active regionserver(s)");
      }
      return false;
    }
    if(areSomeRegionReplicasColocated(c)) return true;
    if(idleRegionServerExist(c)) {
      return true;
    }

    // Check if we even need to do any load balancing
    // HBASE-3681 check sloppiness first
    float average = cs.getLoadAverage(); // for logging
    int floor = (int) Math.floor(average * (1 - slop));
    int ceiling = (int) Math.ceil(average * (1 + slop));
    if (!(cs.getMaxLoad() > ceiling || cs.getMinLoad() < floor)) {
      NavigableMap<ServerAndLoad, List<RegionInfo>> serversByLoad = cs.getServersByLoad();
      if (LOG.isTraceEnabled()) {
        // If nothing to balance, then don't say anything unless trace-level logging.
        LOG.trace("Skipping load balancing because balanced cluster; " +
          "servers=" + cs.getNumServers() +
          " regions=" + cs.getNumRegions() + " average=" + average +
          " mostloaded=" + serversByLoad.lastKey().getLoad() +
          " leastloaded=" + serversByLoad.firstKey().getLoad());
      }
      return false;
    }
    return true;
  }

  /**
   * Subclasses should implement this to return true if the cluster has nodes that hosts
   * multiple replicas for the same region, or, if there are multiple racks and the same
   * rack hosts replicas of the same region
   * @param c Cluster information
   * @return whether region replicas are currently co-located
   */
  protected boolean areSomeRegionReplicasColocated(Cluster c) {
    return false;
  }

  protected final boolean idleRegionServerExist(Cluster c){
    boolean isServerExistsWithMoreRegions = false;
    boolean isServerExistsWithZeroRegions = false;
    for (int[] serverList: c.regionsPerServer){
      if (serverList.length > 1) {
        isServerExistsWithMoreRegions = true;
      }
      if (serverList.length == 0) {
        isServerExistsWithZeroRegions = true;
      }
    }
    return isServerExistsWithMoreRegions && isServerExistsWithZeroRegions;
  }

  /**
   * Generates a bulk assignment plan to be used on cluster startup using a
   * simple round-robin assignment.
   * <p>
   * Takes a list of all the regions and all the servers in the cluster and
   * returns a map of each server to the regions that it should be assigned.
   * <p>
   * Currently implemented as a round-robin assignment. Same invariant as load
   * balancing, all servers holding floor(avg) or ceiling(avg).
   *
   * TODO: Use block locations from HDFS to place regions with their blocks
   *
   * @param regions all regions
   * @param servers all servers
   * @return map of server to the regions it should take, or emptyMap if no
   *         assignment is possible (ie. no servers)
   */
  @Override
  @NonNull
  public Map<ServerName, List<RegionInfo>> roundRobinAssignment(List<RegionInfo> regions,
      List<ServerName> servers) throws HBaseIOException {
    metricsBalancer.incrMiscInvocations();
    Map<ServerName, List<RegionInfo>> assignments = assignMasterSystemRegions(regions, servers);
    if (!assignments.isEmpty()) {
      servers = new ArrayList<>(servers);
      // Guarantee not to put other regions on master
      servers.remove(masterServerName);
      List<RegionInfo> masterRegions = assignments.get(masterServerName);
      if (!masterRegions.isEmpty()) {
        regions = new ArrayList<>(regions);
        regions.removeAll(masterRegions);
      }
    }
    /**
     * only need assign system table
     */
    if (regions.isEmpty()) {
      return assignments;
    }

    int numServers = servers == null ? 0 : servers.size();
    if (numServers == 0) {
      LOG.warn("Wanted to do round robin assignment but no servers to assign to");
      return Collections.singletonMap(BOGUS_SERVER_NAME, new ArrayList<>(regions));
    }

    // TODO: instead of retainAssignment() and roundRobinAssignment(), we should just run the
    // normal LB.balancerCluster() with unassignedRegions. We only need to have a candidate
    // generator for AssignRegionAction. The LB will ensure the regions are mostly local
    // and balanced. This should also run fast with fewer number of iterations.

    if (numServers == 1) { // Only one server, nothing fancy we can do here
      ServerName server = servers.get(0);
      assignments.put(server, new ArrayList<>(regions));
      return assignments;
    }

    Cluster cluster = createCluster(servers, regions);
    roundRobinAssignment(cluster, regions, servers, assignments);
    return assignments;
  }

  protected Cluster createCluster(List<ServerName> servers, Collection<RegionInfo> regions)
      throws HBaseIOException {
    boolean hasRegionReplica = false;
    try {
      if (services != null && services.getTableDescriptors() != null) {
        Map<String, TableDescriptor> tds = services.getTableDescriptors().getAll();
        for (RegionInfo regionInfo : regions) {
          TableDescriptor td = tds.get(regionInfo.getTable().getNameWithNamespaceInclAsString());
          if (td != null && td.getRegionReplication() > 1) {
            hasRegionReplica = true;
            break;
          }
        }
      }
    } catch (IOException ioe) {
      throw new HBaseIOException(ioe);
    }

    // Get the snapshot of the current assignments for the regions in question, and then create
    // a cluster out of it. Note that we might have replicas already assigned to some servers
    // earlier. So we want to get the snapshot to see those assignments, but this will only contain
    // replicas of the regions that are passed (for performance).
    Map<ServerName, List<RegionInfo>> clusterState = null;
    if (!hasRegionReplica) {
      clusterState = getRegionAssignmentsByServer(regions);
    } else {
      // for the case where we have region replica it is better we get the entire cluster's snapshot
      clusterState = getRegionAssignmentsByServer(null);
    }

    for (ServerName server : servers) {
      if (!clusterState.containsKey(server)) {
        clusterState.put(server, EMPTY_REGION_LIST);
      }
    }
    return new Cluster(regions, clusterState, null, this.regionFinder,
        rackManager);
  }

  private List<ServerName> findIdleServers(List<ServerName> servers) {
    return this.services.getServerManager()
            .getOnlineServersListWithPredicator(servers, IDLE_SERVER_PREDICATOR);
  }

  /**
   * Used to assign a single region to a random server.
   */
  @Override
  public ServerName randomAssignment(RegionInfo regionInfo, List<ServerName> servers)
      throws HBaseIOException {
    metricsBalancer.incrMiscInvocations();
    if (servers != null && servers.contains(masterServerName)) {
      if (shouldBeOnMaster(regionInfo)) {
        return masterServerName;
      }
      if (!LoadBalancer.isTablesOnMaster(getConf())) {
        // Guarantee we do not put any regions on master
        servers = new ArrayList<>(servers);
        servers.remove(masterServerName);
      }
    }

    int numServers = servers == null ? 0 : servers.size();
    if (numServers == 0) {
      LOG.warn("Wanted to retain assignment but no servers to assign to");
      return null;
    }
    if (numServers == 1) { // Only one server, nothing fancy we can do here
      return servers.get(0);
    }
    List<ServerName> idleServers = findIdleServers(servers);
    if (idleServers.size() == 1) {
      return idleServers.get(0);
    }
    final List<ServerName> finalServers = idleServers.isEmpty() ?
            servers : idleServers;
    List<RegionInfo> regions = Lists.newArrayList(regionInfo);
    Cluster cluster = createCluster(finalServers, regions);
    return randomAssignment(cluster, regionInfo, finalServers);
  }

  /**
   * Generates a bulk assignment startup plan, attempting to reuse the existing
   * assignment information from META, but adjusting for the specified list of
   * available/online servers available for assignment.
   * <p>
   * Takes a map of all regions to their existing assignment from META. Also
   * takes a list of online servers for regions to be assigned to. Attempts to
   * retain all assignment, so in some instances initial assignment will not be
   * completely balanced.
   * <p>
   * Any leftover regions without an existing server to be assigned to will be
   * assigned randomly to available servers.
   *
   * @param regions regions and existing assignment from meta
   * @param servers available servers
   * @return map of servers and regions to be assigned to them, or emptyMap if no
   *           assignment is possible (ie. no servers)
   */
  @Override
  @NonNull
  public Map<ServerName, List<RegionInfo>> retainAssignment(Map<RegionInfo, ServerName> regions,
      List<ServerName> servers) throws HBaseIOException {
    // Update metrics
    metricsBalancer.incrMiscInvocations();
    Map<ServerName, List<RegionInfo>> assignments = assignMasterSystemRegions(regions.keySet(), servers);
    if (!assignments.isEmpty()) {
      servers = new ArrayList<>(servers);
      // Guarantee not to put other regions on master
      servers.remove(masterServerName);
      List<RegionInfo> masterRegions = assignments.get(masterServerName);
      regions = regions.entrySet().stream().filter(e -> !masterRegions.contains(e.getKey()))
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
    if (regions.isEmpty()) {
      return assignments;
    }

    int numServers = servers == null ? 0 : servers.size();
    if (numServers == 0) {
      LOG.warn("Wanted to do retain assignment but no servers to assign to");
      return Collections.singletonMap(BOGUS_SERVER_NAME, new ArrayList<>(regions.keySet()));
    }
    if (numServers == 1) { // Only one server, nothing fancy we can do here
      ServerName server = servers.get(0);
      assignments.put(server, new ArrayList<>(regions.keySet()));
      return assignments;
    }

    // Group all of the old assignments by their hostname.
    // We can't group directly by ServerName since the servers all have
    // new start-codes.

    // Group the servers by their hostname. It's possible we have multiple
    // servers on the same host on different ports.
    ArrayListMultimap<String, ServerName> serversByHostname = ArrayListMultimap.create();
    for (ServerName server : servers) {
      assignments.put(server, new ArrayList<>());
      serversByHostname.put(server.getHostnameLowerCase(), server);
    }

    // Collection of the hostnames that used to have regions
    // assigned, but for which we no longer have any RS running
    // after the cluster restart.
    Set<String> oldHostsNoLongerPresent = Sets.newTreeSet();

    // If the old servers aren't present, lets assign those regions later.
    List<RegionInfo> randomAssignRegions = Lists.newArrayList();

    int numRandomAssignments = 0;
    int numRetainedAssigments = 0;
    for (Map.Entry<RegionInfo, ServerName> entry : regions.entrySet()) {
      RegionInfo region = entry.getKey();
      ServerName oldServerName = entry.getValue();
      List<ServerName> localServers = new ArrayList<>();
      if (oldServerName != null) {
        localServers = serversByHostname.get(oldServerName.getHostnameLowerCase());
      }
      if (localServers.isEmpty()) {
        // No servers on the new cluster match up with this hostname, assign randomly, later.
        randomAssignRegions.add(region);
        if (oldServerName != null) {
          oldHostsNoLongerPresent.add(oldServerName.getHostnameLowerCase());
        }
      } else if (localServers.size() == 1) {
        // the usual case - one new server on same host
        ServerName target = localServers.get(0);
        assignments.get(target).add(region);
        numRetainedAssigments++;
      } else {
        // multiple new servers in the cluster on this same host
        if (localServers.contains(oldServerName)) {
          assignments.get(oldServerName).add(region);
          numRetainedAssigments++;
        } else {
          ServerName target = null;
          for (ServerName tmp : localServers) {
            if (tmp.getPort() == oldServerName.getPort()) {
              target = tmp;
              assignments.get(tmp).add(region);
              numRetainedAssigments++;
              break;
            }
          }
          if (target == null) {
            randomAssignRegions.add(region);
          }
        }
      }
    }

    // If servers from prior assignment aren't present, then lets do randomAssignment on regions.
    if (randomAssignRegions.size() > 0) {
      Cluster cluster = createCluster(servers, regions.keySet());
      for (Map.Entry<ServerName, List<RegionInfo>> entry : assignments.entrySet()) {
        ServerName sn = entry.getKey();
        for (RegionInfo region : entry.getValue()) {
          cluster.doAssignRegion(region, sn);
        }
      }
      for (RegionInfo region : randomAssignRegions) {
        ServerName target = randomAssignment(cluster, region, servers);
        assignments.get(target).add(region);
        numRandomAssignments++;
      }
    }

    String randomAssignMsg = "";
    if (numRandomAssignments > 0) {
      randomAssignMsg =
          numRandomAssignments + " regions were assigned "
              + "to random hosts, since the old hosts for these regions are no "
              + "longer present in the cluster. These hosts were:\n  "
              + Joiner.on("\n  ").join(oldHostsNoLongerPresent);
    }

    LOG.info("Reassigned " + regions.size() + " regions. " + numRetainedAssigments
        + " retained the pre-restart assignment. " + randomAssignMsg);
    return assignments;
  }

  @Override
  public void initialize() throws HBaseIOException{
  }

  @Override
  public void regionOnline(RegionInfo regionInfo, ServerName sn) {
  }

  @Override
  public void regionOffline(RegionInfo regionInfo) {
  }

  @Override
  public boolean isStopped() {
    return stopped;
  }

  @Override
  public void stop(String why) {
    LOG.info("Load Balancer stop requested: "+why);
    stopped = true;
  }

  /**
  * Updates the balancer status tag reported to JMX
  */
  public void updateBalancerStatus(boolean status) {
    metricsBalancer.balancerStatus(status);
  }

  /**
   * Used to assign a single region to a random server.
   */
  private ServerName randomAssignment(Cluster cluster, RegionInfo regionInfo,
      List<ServerName> servers) {
    int numServers = servers.size(); // servers is not null, numServers > 1
    ServerName sn = null;
    final int maxIterations = numServers * 4;
    int iterations = 0;
    List<ServerName> usedSNs = new ArrayList<>(servers.size());
    Random rand = ThreadLocalRandom.current();
    do {
      int i = rand.nextInt(numServers);
      sn = servers.get(i);
      if (!usedSNs.contains(sn)) {
        usedSNs.add(sn);
      }
    } while (cluster.wouldLowerAvailability(regionInfo, sn)
        && iterations++ < maxIterations);
    if (iterations >= maxIterations) {
      // We have reached the max. Means the servers that we collected is still lowering the
      // availability
      for (ServerName unusedServer : servers) {
        if (!usedSNs.contains(unusedServer)) {
          // check if any other unused server is there for us to use.
          // If so use it. Else we have not other go but to go with one of them
          if (!cluster.wouldLowerAvailability(regionInfo, unusedServer)) {
            sn = unusedServer;
            break;
          }
        }
      }
    }
    cluster.doAssignRegion(regionInfo, sn);
    return sn;
  }

  /**
   * Round robin a list of regions to a list of servers
   */
  private void roundRobinAssignment(Cluster cluster, List<RegionInfo> regions,
    List<ServerName> servers, Map<ServerName, List<RegionInfo>> assignments) {
    Random rand = ThreadLocalRandom.current();
    List<RegionInfo> unassignedRegions = new ArrayList<>();
    int numServers = servers.size();
    int numRegions = regions.size();
    int max = (int) Math.ceil((float) numRegions / numServers);
    int serverIdx = 0;
    if (numServers > 1) {
      serverIdx = rand.nextInt(numServers);
    }
    int regionIdx = 0;
    for (int j = 0; j < numServers; j++) {
      ServerName server = servers.get((j + serverIdx) % numServers);
      List<RegionInfo> serverRegions = new ArrayList<>(max);
      for (int i = regionIdx; i < numRegions; i += numServers) {
        RegionInfo region = regions.get(i % numRegions);
        if (cluster.wouldLowerAvailability(region, server)) {
          unassignedRegions.add(region);
        } else {
          serverRegions.add(region);
          cluster.doAssignRegion(region, server);
        }
      }
      assignments.put(server, serverRegions);
      regionIdx++;
    }


    List<RegionInfo> lastFewRegions = new ArrayList<>();
    // assign the remaining by going through the list and try to assign to servers one-by-one
    serverIdx = rand.nextInt(numServers);
    for (RegionInfo region : unassignedRegions) {
      boolean assigned = false;
      for (int j = 0; j < numServers; j++) { // try all servers one by one
        ServerName server = servers.get((j + serverIdx) % numServers);
        if (cluster.wouldLowerAvailability(region, server)) {
          continue;
        } else {
          assignments.computeIfAbsent(server, k -> new ArrayList<>()).add(region);
          cluster.doAssignRegion(region, server);
          serverIdx = (j + serverIdx + 1) % numServers; // remain from next server
          assigned = true;
          break;
        }
      }
      if (!assigned) {
        lastFewRegions.add(region);
      }
    }
    // just sprinkle the rest of the regions on random regionservers. The balanceCluster will
    // make it optimal later. we can end up with this if numReplicas > numServers.
    for (RegionInfo region : lastFewRegions) {
      int i = rand.nextInt(numServers);
      ServerName server = servers.get(i);
      assignments.computeIfAbsent(server, k -> new ArrayList<>()).add(region);
      cluster.doAssignRegion(region, server);
    }
  }

  protected Map<ServerName, List<RegionInfo>> getRegionAssignmentsByServer(
    Collection<RegionInfo> regions) {
    if (this.services != null && this.services.getAssignmentManager() != null) {
      return this.services.getAssignmentManager().getSnapShotOfAssignment(regions);
    } else {
      return new HashMap<>();
    }
  }

  private Map<ServerName, List<RegionInfo>> toEnsumbleTableLoad(
      Map<TableName, Map<ServerName, List<RegionInfo>>> LoadOfAllTable) {
    Map<ServerName, List<RegionInfo>> returnMap = new TreeMap<>();
    for (Map<ServerName, List<RegionInfo>> serverNameListMap : LoadOfAllTable.values()) {
      serverNameListMap.forEach((serverName, regionInfoList) -> {
        List<RegionInfo> regionInfos =
            returnMap.computeIfAbsent(serverName, k -> new ArrayList<>());
        regionInfos.addAll(regionInfoList);
      });
    }
    return returnMap;
  }

  @Override
  public abstract List<RegionPlan> balanceTable(TableName tableName,
      Map<ServerName, List<RegionInfo>> loadOfOneTable);

  @Override
  public List<RegionPlan>
      balanceCluster(Map<TableName, Map<ServerName, List<RegionInfo>>> loadOfAllTable) {
    if (isByTable) {
      List<RegionPlan> result = new ArrayList<>();
      loadOfAllTable.forEach((tableName, loadOfOneTable) -> {
        LOG.info("Start Generate Balance plan for table: " + tableName);
        List<RegionPlan> partialPlans = balanceTable(tableName, loadOfOneTable);
        if (partialPlans != null) {
          result.addAll(partialPlans);
        }
      });
      return result;
    } else {
      LOG.debug("Start Generate Balance plan for cluster.");
      return balanceTable(HConstants.ENSEMBLE_TABLE_NAME, toEnsumbleTableLoad(loadOfAllTable));
    }
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
  }
}
