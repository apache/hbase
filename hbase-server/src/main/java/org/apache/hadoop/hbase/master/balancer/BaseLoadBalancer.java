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

import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
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

  /**
   * An efficient array based implementation similar to ClusterState for keeping
   * the status of the cluster in terms of region assignment and distribution.
   * To be used by LoadBalancers.
   */
  protected static class Cluster {
    ServerName masterServerName;
    Set<String> tablesOnMaster;
    ServerName[] servers;
    ArrayList<String> tables;
    HRegionInfo[] regions;
    Deque<RegionLoad>[] regionLoads;
    boolean[] backupMasterFlags;
    int activeMasterIndex = -1;
    int[][] regionLocations; //regionIndex -> list of serverIndex sorted by locality

    int[][] regionsPerServer;            //serverIndex -> region list
    int[]   regionIndexToServerIndex;    //regionIndex -> serverIndex
    int[]   initialRegionIndexToServerIndex;    //regionIndex -> serverIndex (initial cluster state)
    int[]   regionIndexToTableIndex;     //regionIndex -> tableIndex
    int[][] numRegionsPerServerPerTable; //serverIndex -> tableIndex -> # regions
    int[]   numMaxRegionsPerTable;       //tableIndex -> max number of regions in a single RS
    int     numUserRegionsOnMaster;      //number of user regions on the active master

    Integer[] serverIndicesSortedByRegionCount;

    Map<String, Integer> serversToIndex;
    Map<String, Integer> tablesToIndex;

    int numRegions;
    int numServers;
    int numTables;

    int numMovedRegions = 0; //num moved regions from the initial configuration
    // num of moved regions away from master that should be on the master
    int numMovedMasterHostedRegions = 0;

    @SuppressWarnings("unchecked")
    protected Cluster(ServerName masterServerName,
        Map<ServerName, List<HRegionInfo>> clusterState,
        Map<String, Deque<RegionLoad>> loads,
        RegionLocationFinder regionFinder,
        Collection<ServerName> backupMasters,
        Set<String> tablesOnMaster) {

      this.tablesOnMaster = tablesOnMaster;
      this.masterServerName = masterServerName;
      serversToIndex = new HashMap<String, Integer>();
      tablesToIndex = new HashMap<String, Integer>();
      //regionsToIndex = new HashMap<HRegionInfo, Integer>();

      //TODO: We should get the list of tables from master
      tables = new ArrayList<String>();

      numRegions = 0;

      int serverIndex = 0;

      // Use servername and port as there can be dead servers in this list. We want everything with
      // a matching hostname and port to have the same index.
      for (ServerName sn:clusterState.keySet()) {
        if (serversToIndex.get(sn.getHostAndPort()) == null) {
          serversToIndex.put(sn.getHostAndPort(), serverIndex++);
        }
      }

      // Count how many regions there are.
      for (Entry<ServerName, List<HRegionInfo>> entry : clusterState.entrySet()) {
        numRegions += entry.getValue().size();
      }

      numServers = serversToIndex.size();
      regionsPerServer = new int[serversToIndex.size()][];

      servers = new ServerName[numServers];
      regions = new HRegionInfo[numRegions];
      regionIndexToServerIndex = new int[numRegions];
      initialRegionIndexToServerIndex = new int[numRegions];
      regionIndexToTableIndex = new int[numRegions];
      regionLoads = new Deque[numRegions];
      regionLocations = new int[numRegions][];
      serverIndicesSortedByRegionCount = new Integer[numServers];
      backupMasterFlags = new boolean[numServers];

      int tableIndex = 0, regionIndex = 0, regionPerServerIndex = 0;

      for (Entry<ServerName, List<HRegionInfo>> entry : clusterState.entrySet()) {
        serverIndex = serversToIndex.get(entry.getKey().getHostAndPort());

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

      for (Entry<ServerName, List<HRegionInfo>> entry : clusterState.entrySet()) {
        serverIndex = serversToIndex.get(entry.getKey().getHostAndPort());
        regionPerServerIndex = 0;

        for (HRegionInfo region : entry.getValue()) {
          String tableName = region.getTable().getNameAsString();
          Integer idx = tablesToIndex.get(tableName);
          if (idx == null) {
            tables.add(tableName);
            idx = tableIndex;
            tablesToIndex.put(tableName, tableIndex++);
          }

          regions[regionIndex] = region;
          regionIndexToServerIndex[regionIndex] = serverIndex;
          initialRegionIndexToServerIndex[regionIndex] = serverIndex;
          regionIndexToTableIndex[regionIndex] = idx;
          regionsPerServer[serverIndex][regionPerServerIndex++] = regionIndex;

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
                    (serversToIndex.get(loc.get(i).getHostAndPort()) == null ? -1 : serversToIndex.get(loc.get(i).getHostAndPort()));
            }
          }

          regionIndex++;
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
        numRegionsPerServerPerTable[regionIndexToServerIndex[i]][regionIndexToTableIndex[i]]++;
      }

      numMaxRegionsPerTable = new int[numTables];
      for (serverIndex = 0 ; serverIndex < numRegionsPerServerPerTable.length; serverIndex++) {
        for (tableIndex = 0 ; tableIndex < numRegionsPerServerPerTable[serverIndex].length; tableIndex++) {
          if (numRegionsPerServerPerTable[serverIndex][tableIndex] > numMaxRegionsPerTable[tableIndex]) {
            numMaxRegionsPerTable[tableIndex] = numRegionsPerServerPerTable[serverIndex][tableIndex];
          }
        }
      }
    }

    public void moveOrSwapRegion(int lServer, int rServer, int lRegion, int rRegion) {
      if (servers[lServer].equals(masterServerName)) {
        if (lRegion >= 0 && !shouldBeOnMaster(regions[lRegion])) {
          numUserRegionsOnMaster--;
        }
        if (rRegion >= 0 && !shouldBeOnMaster(regions[rRegion])) {
          numUserRegionsOnMaster++;
        }
      } else if (servers[rServer].equals(masterServerName)) {
        if (lRegion >= 0 && !shouldBeOnMaster(regions[lRegion])) {
          numUserRegionsOnMaster++;
        }
        if (rRegion >= 0 && !shouldBeOnMaster(regions[rRegion])) {
          numUserRegionsOnMaster--;
        }
      }
      //swap
      if (rRegion >= 0 && lRegion >= 0) {
        regionMoved(rRegion, rServer, lServer);
        regionsPerServer[rServer] = replaceRegion(regionsPerServer[rServer], rRegion, lRegion);
        regionMoved(lRegion, lServer, rServer);
        regionsPerServer[lServer] = replaceRegion(regionsPerServer[lServer], lRegion, rRegion);
      } else if (rRegion >= 0) { //move rRegion
        regionMoved(rRegion, rServer, lServer);
        regionsPerServer[rServer] = removeRegion(regionsPerServer[rServer], rRegion);
        regionsPerServer[lServer] = addRegion(regionsPerServer[lServer], rRegion);
      } else if (lRegion >= 0) { //move lRegion
        regionMoved(lRegion, lServer, rServer);
        regionsPerServer[lServer] = removeRegion(regionsPerServer[lServer], lRegion);
        regionsPerServer[rServer] = addRegion(regionsPerServer[rServer], lRegion);
      }
    }

    /** Region moved out of the server */
    void regionMoved(int regionIndex, int oldServerIndex, int newServerIndex) {
      regionIndexToServerIndex[regionIndex] = newServerIndex;
      if (initialRegionIndexToServerIndex[regionIndex] == newServerIndex) {
        numMovedRegions--; //region moved back to original location
        if (shouldBeOnMaster(regions[regionIndex]) && isActiveMaster(newServerIndex)) {
          // Master hosted region moved back to the active master
          numMovedMasterHostedRegions--;
        }
      } else if (initialRegionIndexToServerIndex[regionIndex] == oldServerIndex) {
        numMovedRegions++; //region moved from original location
        if (shouldBeOnMaster(regions[regionIndex]) && isActiveMaster(oldServerIndex)) {
          // Master hosted region moved away from active the master
          numMovedMasterHostedRegions++;
        }
      }
      int tableIndex = regionIndexToTableIndex[regionIndex];
      numRegionsPerServerPerTable[oldServerIndex][tableIndex]--;
      numRegionsPerServerPerTable[newServerIndex][tableIndex]++;

      //check whether this caused maxRegionsPerTable in the new Server to be updated
      if (numRegionsPerServerPerTable[newServerIndex][tableIndex] > numMaxRegionsPerTable[tableIndex]) {
        numRegionsPerServerPerTable[newServerIndex][tableIndex] = numMaxRegionsPerTable[tableIndex];
      } else if ((numRegionsPerServerPerTable[oldServerIndex][tableIndex] + 1)
          == numMaxRegionsPerTable[tableIndex]) {
        //recompute maxRegionsPerTable since the previous value was coming from the old server
        for (int serverIndex = 0 ; serverIndex < numRegionsPerServerPerTable.length; serverIndex++) {
          if (numRegionsPerServerPerTable[serverIndex][tableIndex] > numMaxRegionsPerTable[tableIndex]) {
            numMaxRegionsPerTable[tableIndex] = numRegionsPerServerPerTable[serverIndex][tableIndex];
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
  }

  @Override
  public void setMasterServices(MasterServices masterServices) {
    masterServerName = masterServices.getServerName();
    excludedServers.remove(masterServerName);
    this.services = masterServices;
  }

  protected Collection<ServerName> getBackupMasters() {
    return clusterStatus == null ? null : clusterStatus.getBackupMasters();
  }

  protected boolean needsBalance(ClusterLoadState cs) {
    if (cs.getNumServers() < MIN_SERVER_BALANCE) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Not running balancer because only " + cs.getNumServers()
            + " active regionserver(s)");
      }
      return false;
    }
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
    int total = regions.size();
    // Get the number of regions to be assigned
    // to backup masters based on the weight
    int numRegions = total * numBackupMasters
      / (numServers * backupMasterWeight + numBackupMasters);
    if (numRegions > 0) {
      // backupMasters can't be null, according to the formula, numBackupMasters != 0
      roundRobinAssignment(regions, 0,
        numRegions, backupMasters, masterRegions, assignments);
    }
    int remainder = total - numRegions;
    if (remainder > 0) {
      // servers can't be null, or contains the master only since numServers != 0
      roundRobinAssignment(regions, numRegions, remainder,
        servers, masterRegions, assignments);
    }
    if (masterRegions != null && !masterRegions.isEmpty()) {
      assignments.put(masterServerName, masterRegions);
    }
    return assignments;
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
    List<ServerName> backupMasters = normalizeServers(servers);
    for (HRegionInfo region : regions) {
      assignments.put(region, randomAssignment(region, servers, backupMasters));
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
    return randomAssignment(regionInfo, servers,
      normalizeServers(servers));
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
        ServerName randomServer = randomAssignment(region, servers, backupMasters);
        assignments.get(randomServer).add(region);
        numRandomAssignments++;
        if (oldServerName != null) oldHostsNoLongerPresent.add(oldServerName.getHostname());
      } else if (localServers.size() == 1) {
        // the usual case - one new server on same host
        assignments.get(localServers.get(0)).add(region);
        numRetainedAssigments++;
      } else {
        // multiple new servers in the cluster on this same host
        int size = localServers.size();
        ServerName target =
            localServers.contains(oldServerName) ? oldServerName : localServers.get(RANDOM
                .nextInt(size));
        assignments.get(target).add(region);
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
  private ServerName randomAssignment(HRegionInfo regionInfo,
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
    // Generate a random number weighted more towards
    // regular regionservers instead of backup masters.
    // This formula is chosen for simplicity.
    int i = RANDOM.nextInt(
      numBackupMasters + numServers * backupMasterWeight);
    if (i < numBackupMasters) {
      return backupMasters.get(i);
    }
    i = (i - numBackupMasters)/backupMasterWeight;
    ServerName sn = servers.get(i);
    if (sn.equals(masterServerName)) {
      // Try to avoid master for a user region
      if (numServers > 1) {
        i = (i == 0 ? 1 : i - 1);
        sn = servers.get(i);
      } else if (numBackupMasters > 0) {
        sn = backupMasters.get(0);
      }
    }
    return sn;
  }

  /**
   * Round robin a chunk of a list of regions to a list of servers
   */
  private void roundRobinAssignment(List<HRegionInfo> regions, int offset,
      int numRegions, List<ServerName> servers, List<HRegionInfo> masterRegions,
      Map<ServerName, List<HRegionInfo>> assignments) {
    boolean masterIncluded = servers.contains(masterServerName);
    int numServers = servers.size();
    int skipServers = numServers;
    if (masterIncluded) {
      skipServers--;
    }
    int max = (int) Math.ceil((float) numRegions / skipServers);
    int serverIdx = RANDOM.nextInt(numServers);
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
          serverRegions.add(region);
          continue;
        }
        // Master is in the list and this is a special region
        masterRegions.add(region);
      }
      assignments.put(server, serverRegions);
      regionIdx++;
    }
  }
}
