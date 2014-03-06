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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.security.access.AccessControlLists;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.master.RackManager;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer.Cluster.Action.Type;

import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * The base class for load balancers. It provides the the functions used to by
 * {@link AssignmentManager} to assign regions in the edge cases. It doesn't
 * provide an implementation of the actual balancing algorithm.
 *
 */
public abstract class BaseLoadBalancer implements LoadBalancer {
  private static final int MIN_SERVER_BALANCE = 2;
  private volatile boolean stopped = false;

  private static final List<HRegionInfo> EMPTY_REGION_LIST = new ArrayList<HRegionInfo>(0);

  protected final RegionLocationFinder regionFinder = new RegionLocationFinder();

  private static class DefaultRackManager extends RackManager {
    @Override
    public String getRack(ServerName server) {
      return UNKNOWN_RACK;
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
    ServerName masterServerName;
    Set<String> tablesOnMaster;
    ServerName[] servers;
    String[] hosts; // ServerName uniquely identifies a region server. multiple RS can run on the same host
    String[] racks;
    boolean multiServersPerHost = false; // whether or not any host has more than one server

    ArrayList<String> tables;
    HRegionInfo[] regions;
    Deque<RegionLoad>[] regionLoads;
    boolean[] backupMasterFlags;
    int activeMasterIndex = -1;

    int[][] regionLocations; //regionIndex -> list of serverIndex sorted by locality

    int[]   serverIndexToHostIndex;      //serverIndex -> host index
    int[]   serverIndexToRackIndex;      //serverIndex -> rack index

    int[][] regionsPerServer;            //serverIndex -> region list
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
    int[][] numRegionsPerServerPerTable; //serverIndex -> tableIndex -> # regions
    int[]   numMaxRegionsPerTable;       //tableIndex -> max number of regions in a single RS
    int     numUserRegionsOnMaster;      //number of user regions on the active master
    int[]   regionIndexToPrimaryIndex;   //regionIndex -> regionIndex of the primary
    boolean hasRegionReplicas = false;   //whether there is regions with replicas

    Integer[] serverIndicesSortedByRegionCount;

    Map<String, Integer> serversToIndex;
    Map<String, Integer> hostsToIndex;
    Map<String, Integer> racksToIndex;
    Map<String, Integer> tablesToIndex;
    Map<HRegionInfo, Integer> regionsToIndex;

    int numServers;
    int numHosts;
    int numRacks;
    int numTables;
    int numRegions;

    int numMovedRegions = 0; //num moved regions from the initial configuration
    // num of moved regions away from master that should be on the master
    int numMovedMasterHostedRegions = 0;
    int numMovedMetaRegions = 0;       //num of moved regions that are META
    Map<ServerName, List<HRegionInfo>> clusterState;

    protected final RackManager rackManager;

    protected Cluster(
        ServerName masterServerName,
        Map<ServerName, List<HRegionInfo>> clusterState,
        Map<String, Deque<RegionLoad>> loads,
        RegionLocationFinder regionFinder,
        Collection<ServerName> backupMasters,
        Set<String> tablesOnMaster,
        RackManager rackManager) {
      this(masterServerName, null, clusterState, loads, regionFinder, backupMasters,
        tablesOnMaster, rackManager);
    }

    protected Cluster(
        ServerName masterServerName,
        Collection<HRegionInfo> unassignedRegions,
        Map<ServerName, List<HRegionInfo>> clusterState,
        Map<String, Deque<RegionLoad>> loads,
        RegionLocationFinder regionFinder,
        Collection<ServerName> backupMasters,
        Set<String> tablesOnMaster,
        RackManager rackManager) {

      if (unassignedRegions == null) {
        unassignedRegions = EMPTY_REGION_LIST;
      }

      this.masterServerName = masterServerName;
      this.tablesOnMaster = tablesOnMaster;

      serversToIndex = new HashMap<String, Integer>();
      hostsToIndex = new HashMap<String, Integer>();
      racksToIndex = new HashMap<String, Integer>();
      tablesToIndex = new HashMap<String, Integer>();

      //TODO: We should get the list of tables from master
      tables = new ArrayList<String>();
      this.rackManager = rackManager != null ? rackManager : new DefaultRackManager();

      numRegions = 0;

      List<List<Integer>> serversPerHostList = new ArrayList<List<Integer>>();
      List<List<Integer>> serversPerRackList = new ArrayList<List<Integer>>();
      this.clusterState = clusterState;

      // Use servername and port as there can be dead servers in this list. We want everything with
      // a matching hostname and port to have the same index.
      for (ServerName sn : clusterState.keySet()) {
        if (serversToIndex.get(sn.getHostAndPort()) == null) {
          serversToIndex.put(sn.getHostAndPort(), numServers++);
        }
        if (!hostsToIndex.containsKey(sn.getHostname())) {
          hostsToIndex.put(sn.getHostname(), numHosts++);
          serversPerHostList.add(new ArrayList<Integer>(1));
        }

        int serverIndex = serversToIndex.get(sn.getHostAndPort());
        int hostIndex = hostsToIndex.get(sn.getHostname());
        serversPerHostList.get(hostIndex).add(serverIndex);

        String rack = this.rackManager.getRack(sn);
        if (!racksToIndex.containsKey(rack)) {
          racksToIndex.put(rack, numRacks++);
          serversPerRackList.add(new ArrayList<Integer>());
        }
        int rackIndex = racksToIndex.get(rack);
        serversPerRackList.get(rackIndex).add(serverIndex);
      }

      // Count how many regions there are.
      for (Entry<ServerName, List<HRegionInfo>> entry : clusterState.entrySet()) {
        numRegions += entry.getValue().size();
      }
      numRegions += unassignedRegions.size();

      regionsToIndex = new HashMap<HRegionInfo, Integer>(numRegions);
      servers = new ServerName[numServers];
      serversPerHost = new int[numHosts][];
      serversPerRack = new int[numRacks][];
      regions = new HRegionInfo[numRegions];
      regionIndexToServerIndex = new int[numRegions];
      initialRegionIndexToServerIndex = new int[numRegions];
      regionIndexToTableIndex = new int[numRegions];
      regionIndexToPrimaryIndex = new int[numRegions];
      regionLoads = new Deque[numRegions];
      regionLocations = new int[numRegions][];
      serverIndicesSortedByRegionCount = new Integer[numServers];
      backupMasterFlags = new boolean[numServers];

      serverIndexToHostIndex = new int[numServers];
      serverIndexToRackIndex = new int[numServers];
      regionsPerServer = new int[numServers][];
      regionsPerHost = new int[numHosts][];
      regionsPerRack = new int[numRacks][];
      primariesOfRegionsPerServer = new int[numServers][];
      primariesOfRegionsPerHost = new int[numHosts][];
      primariesOfRegionsPerRack = new int[numRacks][];

      int tableIndex = 0, regionIndex = 0, regionPerServerIndex = 0;

      for (Entry<ServerName, List<HRegionInfo>> entry : clusterState.entrySet()) {
        int serverIndex = serversToIndex.get(entry.getKey().getHostAndPort());

        // keep the servername if this is the first server name for this hostname
        // or this servername has the newest startcode.
        if (servers[serverIndex] == null ||
            servers[serverIndex].getStartcode() < entry.getKey().getStartcode()) {
          servers[serverIndex] = entry.getKey();
          backupMasterFlags[serverIndex] = backupMasters != null
            && backupMasters.contains(servers[serverIndex]);
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

        if (servers[serverIndex].equals(masterServerName)) {
          activeMasterIndex = serverIndex;
          for (HRegionInfo hri: entry.getValue()) {
            if (!shouldBeOnMaster(hri)) {
              numUserRegionsOnMaster++;
            }
          }
        }
      }

      hosts = new String[numHosts];
      for (Entry<String, Integer> entry : hostsToIndex.entrySet()) {
        hosts[entry.getValue()] = entry.getKey();
      }
      racks = new String[numRacks];
      for (Entry<String, Integer> entry : racksToIndex.entrySet()) {
        racks[entry.getValue()] = entry.getKey();
      }

      for (Entry<ServerName, List<HRegionInfo>> entry : clusterState.entrySet()) {
        int serverIndex = serversToIndex.get(entry.getKey().getHostAndPort());
        regionPerServerIndex = 0;

        int hostIndex = hostsToIndex.get(entry.getKey().getHostname());
        serverIndexToHostIndex[serverIndex] = hostIndex;

        int rackIndex = racksToIndex.get(this.rackManager.getRack(entry.getKey()));
        serverIndexToRackIndex[serverIndex] = rackIndex;

        for (HRegionInfo region : entry.getValue()) {
          registerRegion(region, regionIndex, serverIndex, loads, regionFinder);

          regionsPerServer[serverIndex][regionPerServerIndex++] = regionIndex;
          regionIndex++;
        }
      }
      for (HRegionInfo region : unassignedRegions) {
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

      for (int i=0; i < regionIndexToServerIndex.length; i++) {
        if (regionIndexToServerIndex[i] >= 0) {
          numRegionsPerServerPerTable[regionIndexToServerIndex[i]][regionIndexToTableIndex[i]]++;
        }
      }

      numMaxRegionsPerTable = new int[numTables];
      for (int serverIndex = 0 ; serverIndex < numRegionsPerServerPerTable.length; serverIndex++) {
        for (tableIndex = 0 ; tableIndex < numRegionsPerServerPerTable[serverIndex].length; tableIndex++) {
          if (numRegionsPerServerPerTable[serverIndex][tableIndex] > numMaxRegionsPerTable[tableIndex]) {
            numMaxRegionsPerTable[tableIndex] = numRegionsPerServerPerTable[serverIndex][tableIndex];
          }
        }
      }

      for (int i = 0; i < regions.length; i ++) {
        HRegionInfo info = regions[i];
        if (RegionReplicaUtil.isDefaultReplica(info)) {
          regionIndexToPrimaryIndex[i] = i;
        } else {
          hasRegionReplicas = true;
          HRegionInfo primaryInfo = RegionReplicaUtil.getRegionInfoForDefaultReplica(info);
          regionIndexToPrimaryIndex[i] =
              regionsToIndex.containsKey(primaryInfo) ?
              regionsToIndex.get(primaryInfo):
              -1;
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
    private void registerRegion(HRegionInfo region, int regionIndex, int serverIndex,
        Map<String, Deque<RegionLoad>> loads, RegionLocationFinder regionFinder) {
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
        Deque<RegionLoad> rl = loads.get(region.getRegionNameAsString());
        // That could have failed if the RegionLoad is using the other regionName
        if (rl == null) {
          // Try getting the region load using encoded name.
          rl = loads.get(region.getEncodedName());
        }
        regionLoads[regionIndex] = rl;
      }

      if (regionFinder != null) {
        //region location
        List<ServerName> loc = regionFinder.getTopBlockLocations(region);
        regionLocations[regionIndex] = new int[loc.size()];
        for (int i=0; i < loc.size(); i++) {
          regionLocations[regionIndex][i] =
              loc.get(i) == null ? -1 :
                (serversToIndex.get(loc.get(i).getHostAndPort()) == null ? -1
                    : serversToIndex.get(loc.get(i).getHostAndPort()));
        }
      }
    }

    /** An action to move or swap a region */
    public static class Action {
      public static enum Type {
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
        throw new NotImplementedException();
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

    public static Action NullAction = new Action(Type.NULL);

    public void doAction(Action action) {
      switch (action.type) {
      case NULL: break;
      case ASSIGN_REGION:
        AssignRegionAction ar = (AssignRegionAction) action;
        regionsPerServer[ar.server] = addRegion(regionsPerServer[ar.server], ar.region);
        regionMoved(ar.region, -1, ar.server);
        break;
      case MOVE_REGION:
        MoveRegionAction mra = (MoveRegionAction) action;
        regionsPerServer[mra.fromServer] = removeRegion(regionsPerServer[mra.fromServer], mra.region);
        regionsPerServer[mra.toServer] = addRegion(regionsPerServer[mra.toServer], mra.region);
        regionMoved(mra.region, mra.fromServer, mra.toServer);
        break;
      case SWAP_REGIONS:
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
     * @param server
     * @param region
     * @return true or false
     */
    boolean wouldLowerAvailability(HRegionInfo regionInfo, ServerName serverName) {
      if (!serversToIndex.containsKey(serverName.getHostAndPort())) {
        return false; // safeguard against race between cluster.servers and servers from LB method args
      }
      int server = serversToIndex.get(serverName.getHostAndPort());
      int region = regionsToIndex.get(regionInfo);

      int primary = regionIndexToPrimaryIndex[region];

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
      if (multiServersPerHost) { // these arrays would only be allocated if we have more than one server per host
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

    void doAssignRegion(HRegionInfo regionInfo, ServerName serverName) {
      if (!serversToIndex.containsKey(serverName.getHostAndPort())) {
        return;
      }
      int server = serversToIndex.get(serverName.getHostAndPort());
      int region = regionsToIndex.get(regionInfo);
      doAction(new AssignRegionAction(region, server));
    }

    void regionMoved(int region, int oldServer, int newServer) {
      regionIndexToServerIndex[region] = newServer;
      if (initialRegionIndexToServerIndex[region] == newServer) {
        numMovedRegions--; //region moved back to original location
        if (shouldBeOnMaster(regions[region]) && isActiveMaster(newServer)) {
          //Master hosted region moved back to the active master
          numMovedMasterHostedRegions--;
        }
      } else if (oldServer >= 0 && initialRegionIndexToServerIndex[region] == oldServer) {
        numMovedRegions++; //region moved from original location
        if (shouldBeOnMaster(regions[region]) && isActiveMaster(oldServer)) {
          // Master hosted region moved away from active the master
          numMovedMasterHostedRegions++;
        }
      }
      int tableIndex = regionIndexToTableIndex[region];
      if (oldServer >= 0) {
        numRegionsPerServerPerTable[oldServer][tableIndex]--;
      }
      numRegionsPerServerPerTable[newServer][tableIndex]++;

      //check whether this caused maxRegionsPerTable in the new Server to be updated
      if (numRegionsPerServerPerTable[newServer][tableIndex] > numMaxRegionsPerTable[tableIndex]) {
        numRegionsPerServerPerTable[newServer][tableIndex] = numMaxRegionsPerTable[tableIndex];
      } else if (oldServer >= 0 && (numRegionsPerServerPerTable[oldServer][tableIndex] + 1)
          == numMaxRegionsPerTable[tableIndex]) {
        //recompute maxRegionsPerTable since the previous value was coming from the old server
        for (int serverIndex = 0 ; serverIndex < numRegionsPerServerPerTable.length; serverIndex++) {
          if (numRegionsPerServerPerTable[serverIndex][tableIndex] > numMaxRegionsPerTable[tableIndex]) {
            numMaxRegionsPerTable[tableIndex] = numRegionsPerServerPerTable[serverIndex][tableIndex];
          }
        }
      }

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
      if (oldServer >= 0 && isActiveMaster(oldServer)) {
        if (!shouldBeOnMaster(regions[region])) {
          numUserRegionsOnMaster--;
        }
      } else if (isActiveMaster(newServer)) {
        if (!shouldBeOnMaster(regions[region])) {
          numUserRegionsOnMaster++;
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

    boolean isBackupMaster(int server) {
      return backupMasterFlags[server];
    }

    boolean isActiveMaster(int server) {
      return activeMasterIndex == server;
    }

    boolean shouldBeOnMaster(HRegionInfo region) {
      return tablesOnMaster != null && tablesOnMaster.contains(
        region.getTable().getNameAsString());
    }

    boolean contains(int[] arr, int val) {
      return Arrays.binarySearch(arr, val) >= 0;
    }

    private Comparator<Integer> numRegionsComparator = new Comparator<Integer>() {
      @Override
      public int compare(Integer integer, Integer integer2) {
        return Integer.valueOf(getNumRegions(integer)).compareTo(getNumRegions(integer2));
      }
    };

    @Override
    public String toString() {
      String desc = "Cluster{" +
          "servers=[";
          for(ServerName sn:servers) {
             desc += sn.getHostAndPort() + ", ";
          }
          desc +=
          ", serverIndicesSortedByRegionCount="+
          Arrays.toString(serverIndicesSortedByRegionCount) +
          ", regionsPerServer=[";

          for (int[]r:regionsPerServer) {
            desc += Arrays.toString(r);
          }
          desc += "]" +
          ", numMaxRegionsPerTable=" +
          Arrays.toString(numMaxRegionsPerTable) +
          ", numRegions=" +
          numRegions +
          ", numServers=" +
          numServers +
          ", numTables=" +
          numTables +
          ", numMovedRegions=" +
          numMovedRegions +
          ", numMovedMasterHostedRegions=" +
          numMovedMasterHostedRegions +
          '}';
      return desc;
    }
  }

  // slop for regions
  protected float slop;
  protected Configuration config;
  protected RackManager rackManager;
  private static final Random RANDOM = new Random(System.currentTimeMillis());
  private static final Log LOG = LogFactory.getLog(BaseLoadBalancer.class);

  // The weight means that each region on the active/backup master is
  // equal to that many regions on a normal regionserver, in calculating
  // the region load by the load balancer. So that the active/backup master
  // can host less (or equal if weight = 1) regions than normal regionservers.
  //
  // The weight can be used to control the number of regions on backup
  // masters, which shouldn't host as many regions as normal regionservers.
  // So that we don't need to move around too many regions when a
  // backup master becomes the active one.
  //
  // Currently, the active master weight is used only by StockasticLoadBalancer.
  // Generally, we don't put any user regions on the active master, which
  // only hosts regions of tables defined in TABLES_ON_MASTER.
  // That's why the default activeMasterWeight is high.
  public static final String BACKUP_MASTER_WEIGHT_KEY =
    "hbase.balancer.backupMasterWeight";
  public static final int DEFAULT_BACKUP_MASTER_WEIGHT = 1;

  private static final String ACTIVE_MASTER_WEIGHT_KEY =
    "hbase.balancer.activeMasterWeight";
  private static final int DEFAULT_ACTIVE_MASTER_WEIGHT = 200;

  // Regions of these tables are put on the master by default.
  private static final String[] DEFAULT_TABLES_ON_MASTER =
    new String[] {AccessControlLists.ACL_TABLE_NAME.getNameAsString(),
      TableName.NAMESPACE_TABLE_NAME.getNameAsString(),
      TableName.META_TABLE_NAME.getNameAsString()};

  protected int activeMasterWeight;
  protected int backupMasterWeight;

  // a flag to indicate if assigning regions to backup masters
  protected boolean usingBackupMasters = true;
  protected final Set<ServerName> excludedServers =
    Collections.synchronizedSet(new HashSet<ServerName>());

  protected final Set<String> tablesOnMaster = new HashSet<String>();
  protected final MetricsBalancer metricsBalancer = new MetricsBalancer();
  protected ClusterStatus clusterStatus = null;
  protected ServerName masterServerName;
  protected MasterServices services;

  @Override
  public void setConf(Configuration conf) {
    setSlop(conf);
    if (slop < 0) slop = 0;
    else if (slop > 1) slop = 1;

    this.config = conf;
    activeMasterWeight = conf.getInt(
      ACTIVE_MASTER_WEIGHT_KEY, DEFAULT_ACTIVE_MASTER_WEIGHT);
    backupMasterWeight = conf.getInt(
      BACKUP_MASTER_WEIGHT_KEY, DEFAULT_BACKUP_MASTER_WEIGHT);
    if (backupMasterWeight < 1) {
      usingBackupMasters = false;
      LOG.info("Backup master won't host any region since "
        + BACKUP_MASTER_WEIGHT_KEY + " is " + backupMasterWeight
        + "(<1)");
    }
    String[] tables = conf.getStrings(
      "hbase.balancer.tablesOnMaster", DEFAULT_TABLES_ON_MASTER);
    if (tables != null) {
      for (String table: tables) {
        tablesOnMaster.add(table);
      }
    }
    this.rackManager = new RackManager(getConf());
    regionFinder.setConf(conf);
  }

  protected void setSlop(Configuration conf) {
    this.slop = conf.getFloat("hbase.regions.slop", (float) 0.2);
  }

  /**
   * If there is any server excluded, filter it out from the cluster map so
   * we won't assign any region to it, assuming none's already assigned there.
   */
  protected void filterExcludedServers(Map<ServerName, List<HRegionInfo>> clusterMap) {
    if (excludedServers.isEmpty()) { // No server to filter out
      return;
    }
    Iterator<Map.Entry<ServerName, List<HRegionInfo>>> it = clusterMap.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<ServerName, List<HRegionInfo>> en = it.next();
      if (excludedServers.contains(en.getKey()) && en.getValue().isEmpty()) {
        it.remove();
      }
    }
  }

  /**
   * Check if a region belongs to some small system table.
   * If so, it may be expected to be put on the master regionserver.
   */
  protected boolean shouldBeOnMaster(HRegionInfo region) {
    return tablesOnMaster.contains(region.getTable().getNameAsString());
  }

  /**
   * Balance the regions that should be on master regionserver.
   */
  protected List<RegionPlan> balanceMasterRegions(
      Map<ServerName, List<HRegionInfo>> clusterMap) {
    if (services == null || clusterMap.size() <= 1) return null;
    List<RegionPlan> plans = null;
    List<HRegionInfo> regions = clusterMap.get(masterServerName);
    if (regions != null) {
      Iterator<ServerName> keyIt = null;
      for (HRegionInfo region: regions) {
        if (shouldBeOnMaster(region)) continue;

        // Find a non-master regionserver to host the region
        if (keyIt == null || !keyIt.hasNext()) {
          keyIt = clusterMap.keySet().iterator();
        }
        ServerName dest = keyIt.next();
        if (masterServerName.equals(dest)) {
          dest = keyIt.next();
        }

        // Move this region away from the master regionserver
        RegionPlan plan = new RegionPlan(region, masterServerName, dest);
        if (plans == null) {
          plans = new ArrayList<RegionPlan>();
        }
        plans.add(plan);
      }
    }
    for (Map.Entry<ServerName, List<HRegionInfo>> server: clusterMap.entrySet()) {
      if (masterServerName.equals(server.getKey())) continue;
      for (HRegionInfo region: server.getValue()) {
        if (!shouldBeOnMaster(region)) continue;

        // Move this region to the master regionserver
        RegionPlan plan = new RegionPlan(region, server.getKey(), masterServerName);
        if (plans == null) {
          plans = new ArrayList<RegionPlan>();
        }
        plans.add(plan);
      }
    }
    return plans;
  }

  public void excludeServer(ServerName serverName) {
    if (!usingBackupMasters) excludedServers.add(serverName);
  }

  public Set<ServerName> getExcludedServers() {
    return excludedServers;
  }

  @Override
  public Configuration getConf() {
    return this.config;
  }

  @Override
  public void setClusterStatus(ClusterStatus st) {
    this.clusterStatus = st;
    if (st == null || usingBackupMasters) return;

    // Not assign any region to backup masters.
    // Put them on the excluded server list.
    // Assume there won't be too much backup masters
    // re/starting, so this won't leak much memory.
    excludedServers.addAll(st.getBackupMasters());
    regionFinder.setClusterStatus(st);
  }

  @Override
  public void setMasterServices(MasterServices masterServices) {
    masterServerName = masterServices.getServerName();
    excludedServers.remove(masterServerName);
    this.services = masterServices;
    this.regionFinder.setServices(masterServices);
  }

  public void setRackManager(RackManager rackManager) {
    this.rackManager = rackManager;
  }

  protected Collection<ServerName> getBackupMasters() {
    return clusterStatus == null ? null : clusterStatus.getBackupMasters();
  }

  protected boolean needsBalance(Cluster c) {
    ClusterLoadState cs = new ClusterLoadState(
      masterServerName, getBackupMasters(), backupMasterWeight, c.clusterState);
    if (cs.getNumServers() < MIN_SERVER_BALANCE) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Not running balancer because only " + cs.getNumServers()
            + " active regionserver(s)");
      }
      return false;
    }
    if(areSomeRegionReplicasColocated(c)) return true;
    // Check if we even need to do any load balancing
    // HBASE-3681 check sloppiness first
    float average = cs.getLoadAverage(); // for logging
    int floor = (int) Math.floor(average * (1 - slop));
    int ceiling = (int) Math.ceil(average * (1 + slop));
    if (!(cs.getMaxLoad() > ceiling || cs.getMinLoad() < floor)) {
      NavigableMap<ServerAndLoad, List<HRegionInfo>> serversByLoad = cs.getServersByLoad();
      if (LOG.isTraceEnabled()) {
        // If nothing to balance, then don't say anything unless trace-level logging.
        LOG.trace("Skipping load balancing because balanced cluster; " +
          "servers=" + cs.getNumServers() + "(backupMasters=" + cs.getNumBackupMasters() +
          ") regions=" + cs.getNumRegions() + " average=" + average + " " +
          "mostloaded=" + serversByLoad.lastKey().getLoad() +
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
   * @param c
   * @return
   */
  protected boolean areSomeRegionReplicasColocated(Cluster c) {
    return false;
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
   * @return map of server to the regions it should take, or null if no
   *         assignment is possible (ie. no regions or no servers)
   */
  @Override
  public Map<ServerName, List<HRegionInfo>> roundRobinAssignment(List<HRegionInfo> regions,
      List<ServerName> servers) {
    metricsBalancer.incrMiscInvocations();
    if (regions == null || regions.isEmpty()) {
      return null;
    }

    List<ServerName> backupMasters = normalizeServers(servers);
    int numServers = servers == null ? 0 : servers.size();
    int numBackupMasters = backupMasters == null ? 0 : backupMasters.size();
    if (numServers == 0 && numBackupMasters == 0) {
      LOG.warn("Wanted to do round robin assignment but no servers to assign to");
      return null;
    }

    // TODO: instead of retainAssignment() and roundRobinAssignment(), we should just run the
    // normal LB.balancerCluster() with unassignedRegions. We only need to have a candidate
    // generator for AssignRegionAction. The LB will ensure the regions are mostly local
    // and balanced. This should also run fast with fewer number of iterations.

    Map<ServerName, List<HRegionInfo>> assignments = new TreeMap<ServerName, List<HRegionInfo>>();
    if (numServers + numBackupMasters == 1) { // Only one server, nothing fancy we can do here
      ServerName server = numServers > 0 ? servers.get(0) : backupMasters.get(0);
      assignments.put(server, new ArrayList<HRegionInfo>(regions));
      return assignments;
    }
    List<HRegionInfo> masterRegions = null;
    if (numServers > 0 && servers.contains(masterServerName)) {
      masterRegions = new ArrayList<HRegionInfo>();
      if (numServers == 1) {
        // The only server in servers is the master,
        // Assign all regions to backup masters
        numServers = 0;
      }
    }

    Cluster cluster = createCluster(servers, regions, backupMasters, tablesOnMaster);
    List<HRegionInfo> unassignedRegions = new ArrayList<HRegionInfo>();

    int total = regions.size();
    // Get the number of regions to be assigned
    // to backup masters based on the weight
    int numRegions = total * numBackupMasters
      / (numServers * backupMasterWeight + numBackupMasters);
    if (numRegions > 0) {
      // backupMasters can't be null, according to the formula, numBackupMasters != 0
      roundRobinAssignment(cluster, regions, unassignedRegions, 0,
        numRegions, backupMasters, masterRegions, assignments);
    }
    int remainder = total - numRegions;
    if (remainder > 0) {
      // servers can't be null, or contains the master only since numServers != 0
      roundRobinAssignment(cluster, regions, unassignedRegions, numRegions, remainder,
        servers, masterRegions, assignments);
    }
    if (masterRegions != null && !masterRegions.isEmpty()) {
      assignments.put(masterServerName, masterRegions);
      for (HRegionInfo r : masterRegions) {
        cluster.doAssignRegion(r, masterServerName);
      }
    }
    List<HRegionInfo> lastFewRegions = new ArrayList<HRegionInfo>();
    // assign the remaining by going through the list and try to assign to servers one-by-one
    int serverIdx = RANDOM.nextInt(numServers);
    for (HRegionInfo region : unassignedRegions) {
      for (int j = 0; j < numServers; j++) { // try all servers one by one
        ServerName serverName = servers.get((j + serverIdx) % numServers);
        if (serverName.equals(masterServerName)) {
          continue;
        }
        if (!cluster.wouldLowerAvailability(region, serverName)) {
          List<HRegionInfo> serverRegions = assignments.get(serverName);
          if (serverRegions == null) {
            serverRegions = new ArrayList<HRegionInfo>();
            assignments.put(serverName, serverRegions);
          }
          serverRegions.add(region);
          cluster.doAssignRegion(region, serverName);
          serverIdx = (j + serverIdx + 1) % numServers; //remain from next server
          break;
        } else {
          lastFewRegions.add(region);
        }
      }
    }
    // just sprinkle the rest of the regions on random regionservers. The balanceCluster will
    // make it optimal later. we can end up with this if numReplicas > numServers.
    for (HRegionInfo region : lastFewRegions) {
      ServerName server = null;
      if (numServers == 0) {
        // select from backup masters
        int i = RANDOM.nextInt(backupMasters.size());
        server = backupMasters.get(i);
      } else {
        do {
          int i = RANDOM.nextInt(numServers);
          server = servers.get(i);
        } while (numServers > 1 && server.equals(masterServerName));
      }
      List<HRegionInfo> serverRegions = assignments.get(server);
      if (serverRegions == null) {
        serverRegions = new ArrayList<HRegionInfo>();
        assignments.put(server, serverRegions);
      }
      serverRegions.add(region);
      cluster.doAssignRegion(region, server);
    }
    return assignments;
  }

  protected Cluster createCluster(List<ServerName> servers,
      Collection<HRegionInfo> regions, List<ServerName> backupMasters, Set<String> tablesOnMaster) {
    // Get the snapshot of the current assignments for the regions in question, and then create
    // a cluster out of it. Note that we might have replicas already assigned to some servers
    // earlier. So we want to get the snapshot to see those assignments, but this will only contain
    // replicas of the regions that are passed (for performance).
    Map<ServerName, List<HRegionInfo>> clusterState = getRegionAssignmentsByServer(regions);

    for (ServerName server : servers) {
      if (!clusterState.containsKey(server)) {
        clusterState.put(server, EMPTY_REGION_LIST);
      }
    }
    return new Cluster(masterServerName, regions, clusterState, null, this.regionFinder, backupMasters,
      tablesOnMaster, rackManager);
  }

  /**
   * Generates an immediate assignment plan to be used by a new master for
   * regions in transition that do not have an already known destination.
   *
   * Takes a list of regions that need immediate assignment and a list of all
   * available servers. Returns a map of regions to the server they should be
   * assigned to.
   *
   * This method will return quickly and does not do any intelligent balancing.
   * The goal is to make a fast decision not the best decision possible.
   *
   * Currently this is random.
   *
   * @param regions
   * @param servers
   * @return map of regions to the server it should be assigned to
   */
  @Override
  public Map<HRegionInfo, ServerName> immediateAssignment(List<HRegionInfo> regions,
      List<ServerName> servers) {
    metricsBalancer.incrMiscInvocations();
    if (servers == null || servers.isEmpty()) {
      LOG.warn("Wanted to do random assignment but no servers to assign to");
      return null;
    }

    Map<HRegionInfo, ServerName> assignments = new TreeMap<HRegionInfo, ServerName>();
    for (HRegionInfo region : regions) {
      assignments.put(region, randomAssignment(region, servers));
    }
    return assignments;
  }

  /**
   * Used to assign a single region to a random server.
   */
  @Override
  public ServerName randomAssignment(HRegionInfo regionInfo, List<ServerName> servers) {
    metricsBalancer.incrMiscInvocations();
    if (servers == null || servers.isEmpty()) {
      LOG.warn("Wanted to do random assignment but no servers to assign to");
      return null;
    }
    List<ServerName> backupMasters = normalizeServers(servers);
    List<HRegionInfo> regions = Lists.newArrayList(regionInfo);
    Cluster cluster = createCluster(servers, regions, backupMasters, tablesOnMaster);

    return randomAssignment(cluster, regionInfo, servers, backupMasters);
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
   * @return map of servers and regions to be assigned to them
   */
  @Override
  public Map<ServerName, List<HRegionInfo>> retainAssignment(Map<HRegionInfo, ServerName> regions,
      List<ServerName> servers) {
    // Update metrics
    metricsBalancer.incrMiscInvocations();
    if (regions == null || regions.isEmpty()) {
      return null;
    }

    List<ServerName> backupMasters = normalizeServers(servers);
    int numServers = servers == null ? 0 : servers.size();
    int numBackupMasters = backupMasters == null ? 0 : backupMasters.size();
    if (numServers == 0 && numBackupMasters == 0) {
      LOG.warn("Wanted to do retain assignment but no servers to assign to");
      return null;
    }
    Map<ServerName, List<HRegionInfo>> assignments = new TreeMap<ServerName, List<HRegionInfo>>();
    if (numServers + numBackupMasters == 1) { // Only one server, nothing fancy we can do here
      ServerName server = numServers > 0 ? servers.get(0) : backupMasters.get(0);
      assignments.put(server, new ArrayList<HRegionInfo>(regions.keySet()));
      return assignments;
    }

    // Group all of the old assignments by their hostname.
    // We can't group directly by ServerName since the servers all have
    // new start-codes.

    // Group the servers by their hostname. It's possible we have multiple
    // servers on the same host on different ports.
    ArrayListMultimap<String, ServerName> serversByHostname = ArrayListMultimap.create();
    for (ServerName server : servers) {
      assignments.put(server, new ArrayList<HRegionInfo>());
      if (!server.equals(masterServerName)) {
        serversByHostname.put(server.getHostname(), server);
      }
    }
    if (numBackupMasters > 0) {
      for (ServerName server : backupMasters) {
        assignments.put(server, new ArrayList<HRegionInfo>());
      }
    }

    // Collection of the hostnames that used to have regions
    // assigned, but for which we no longer have any RS running
    // after the cluster restart.
    Set<String> oldHostsNoLongerPresent = Sets.newTreeSet();

    // Master regionserver is in the server list.
    boolean masterIncluded = servers.contains(masterServerName);

    int numRandomAssignments = 0;
    int numRetainedAssigments = 0;

    Cluster cluster = createCluster(servers, regions.keySet(), backupMasters, tablesOnMaster);

    for (Map.Entry<HRegionInfo, ServerName> entry : regions.entrySet()) {
      HRegionInfo region = entry.getKey();
      ServerName oldServerName = entry.getValue();
      List<ServerName> localServers = new ArrayList<ServerName>();
      if (oldServerName != null) {
        localServers = serversByHostname.get(oldServerName.getHostname());
      }
      if (masterIncluded && shouldBeOnMaster(region)) {
        assignments.get(masterServerName).add(region);
        if (localServers.contains(masterServerName)) {
          numRetainedAssigments++;
        } else {
          numRandomAssignments++;
        }
      } else if (localServers.isEmpty()) {
        // No servers on the new cluster match up with this hostname,
        // assign randomly.
        ServerName randomServer = randomAssignment(cluster, region, servers, backupMasters);
        assignments.get(randomServer).add(region);
        numRandomAssignments++;
        if (oldServerName != null) oldHostsNoLongerPresent.add(oldServerName.getHostname());
      } else if (localServers.size() == 1) {
        // the usual case - one new server on same host
        ServerName target = localServers.get(0);
        assignments.get(target).add(region);
        cluster.doAssignRegion(region, target);
        numRetainedAssigments++;
      } else {
        // multiple new servers in the cluster on this same host
        if (localServers.contains(oldServerName)) {
          assignments.get(oldServerName).add(region);
          cluster.doAssignRegion(region, oldServerName);
        } else {
          ServerName target = null;
          for (ServerName tmp: localServers) {
            if (tmp.getPort() == oldServerName.getPort()) {
              target = tmp;
              break;
            }
          }
          if (target == null) {
            target = randomAssignment(cluster, region, localServers, backupMasters);
          }
          assignments.get(target).add(region);
        }
        numRetainedAssigments++;
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
  public void regionOnline(HRegionInfo regionInfo, ServerName sn) {
  }

  @Override
  public void regionOffline(HRegionInfo regionInfo) {
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
   * Prepare the list of target regionservers so that it doesn't
   * contain any excluded server, or backup master. Those backup masters
   * used to be in the original list are returned.
   */
  private List<ServerName> normalizeServers(List<ServerName> servers) {
    if (servers == null) {
      return null;
    }
    if (!excludedServers.isEmpty()) {
      servers.removeAll(excludedServers);
    }
    Collection<ServerName> allBackupMasters = getBackupMasters();
    List<ServerName> backupMasters = null;
    if (allBackupMasters != null && !allBackupMasters.isEmpty()) {
      for (ServerName server: allBackupMasters) {
        if (!servers.contains(server)) {
          // Ignore backup masters not included
          continue;
        }
        servers.remove(server);
        if (backupMasters == null) {
          backupMasters = new ArrayList<ServerName>();
        }
        backupMasters.add(server);
      }
    }
    return backupMasters;
  }

  /**
   * Used to assign a single region to a random server. The input should
   * have been already normalized: 1) servers doesn't include any exclude sever,
   * 2) servers doesn't include any backup master, 3) backupMasters contains
   * only backup masters that are intended to host this region, i.e, it
   * may not have all the backup masters.
   */
  private ServerName randomAssignment(Cluster cluster, HRegionInfo regionInfo,
      List<ServerName> servers, List<ServerName> backupMasters) {
    int numServers = servers == null ? 0 : servers.size();
    int numBackupMasters = backupMasters == null ? 0 : backupMasters.size();
    if (numServers == 0 && numBackupMasters == 0) {
      LOG.warn("Wanted to do random assignment but no servers to assign to");
      return null;
    }
    if (servers != null && shouldBeOnMaster(regionInfo)
        && servers.contains(masterServerName)) {
      return masterServerName;
    }
    ServerName sn = null;
    final int maxIterations = servers.size() * 4;
    int iterations = 0;

    do {
      // Generate a random number weighted more towards
      // regular regionservers instead of backup masters.
      // This formula is chosen for simplicity.
      int i = RANDOM.nextInt(
        numBackupMasters + numServers * backupMasterWeight);
      if (i < numBackupMasters) {
        sn = backupMasters.get(i);
        continue;
      }
      i = (i - numBackupMasters)/backupMasterWeight;
      sn = servers.get(i);
      if (sn.equals(masterServerName)) {
        // Try to avoid master for a user region
        if (numServers > 1) {
          i = (i == 0 ? 1 : i - 1);
          sn = servers.get(i);
        } else if (numBackupMasters > 0) {
          sn = backupMasters.get(0);
        }
      }
    } while (cluster.wouldLowerAvailability(regionInfo, sn)
        && iterations++ < maxIterations);
    cluster.doAssignRegion(regionInfo, sn);
    return sn;
  }

  /**
   * Round robin a chunk of a list of regions to a list of servers
   */
  private void roundRobinAssignment(Cluster cluster, List<HRegionInfo> regions,
      List<HRegionInfo> unassignedRegions, int offset,
      int numRegions, List<ServerName> servers, List<HRegionInfo> masterRegions,
      Map<ServerName, List<HRegionInfo>> assignments) {

    boolean masterIncluded = servers.contains(masterServerName);
    int numServers = servers.size();
    int skipServers = numServers;
    if (masterIncluded) {
      skipServers--;
    }
    int max = (int) Math.ceil((float) numRegions / skipServers);
    int serverIdx = 0;
    if (numServers > 1) {
      serverIdx = RANDOM.nextInt(numServers);
    }
    int regionIdx = 0;

    for (int j = 0; j < numServers; j++) {
      ServerName server = servers.get((j + serverIdx) % numServers);
      if (masterIncluded && server.equals(masterServerName)) {
        // Don't put non-special region on the master regionserver,
        // So that it is not overloaded.
        continue;
      }
      List<HRegionInfo> serverRegions = new ArrayList<HRegionInfo>(max);
      for (int i = regionIdx; i < numRegions; i += skipServers) {
        HRegionInfo region = regions.get(offset + i % numRegions);
        if (masterRegions == null || !shouldBeOnMaster(region)) {
          if (cluster.wouldLowerAvailability(region, server)) {
            unassignedRegions.add(region);
          } else {
            serverRegions.add(region);
            cluster.doAssignRegion(region, server);
          }
          continue;
        }
        // Master is in the list and this is a special region
        masterRegions.add(region);
      }
      assignments.put(server, serverRegions);
      regionIdx++;
    }
  }

  protected Map<ServerName, List<HRegionInfo>> getRegionAssignmentsByServer(
    Collection<HRegionInfo> regions) {
    if (this.services != null && this.services.getAssignmentManager() != null) {
      return this.services.getAssignmentManager().getSnapShotOfAssignment(regions);
    } else {
      return new HashMap<ServerName, List<HRegionInfo>>();
    }
  }
}
