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

package org.apache.hadoop.hbase.rsgroup;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.balancer.LoadBalancerFactory;
import org.apache.hadoop.hbase.master.balancer.StochasticLoadBalancer;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;

/**
 * GroupBasedLoadBalancer, used when Region Server Grouping is configured (HBase-6721)
 * It does region balance based on a table's group membership.
 *
 * Most assignment methods contain two exclusive code paths: Online - when the group
 * table is online and Offline - when it is unavailable.
 *
 * During Offline, assignments are assigned based on cached information in zookeeper.
 * If unavailable (ie bootstrap) then regions are assigned randomly.
 *
 * Once the GROUP table has been assigned, the balancer switches to Online and will then
 * start providing appropriate assignments for user tables.
 *
 */
@InterfaceAudience.Private
public class RSGroupBasedLoadBalancer implements RSGroupableBalancer {
  private static final Logger LOG = LoggerFactory.getLogger(RSGroupBasedLoadBalancer.class);

  private Configuration config;
  private ClusterMetrics clusterStatus;
  private MasterServices masterServices;
  private volatile RSGroupInfoManager rsGroupInfoManager;
  private LoadBalancer internalBalancer;

  /**
   * Set this key to {@code true} to allow region fallback.
   * Fallback to the default rsgroup first, then fallback to any group if no online servers in
   * default rsgroup.
   * Please keep balancer switch on at the same time, which is relied on to correct misplaced
   * regions
   */
  public static final String FALLBACK_GROUP_ENABLE_KEY = "hbase.rsgroup.fallback.enable";

  private boolean fallbackEnabled = false;

  /**
   * Used by reflection in {@link org.apache.hadoop.hbase.master.balancer.LoadBalancerFactory}.
   */
  @InterfaceAudience.Private
  public RSGroupBasedLoadBalancer() {}

  @Override
  public Configuration getConf() {
    return config;
  }

  @Override
  public void setConf(Configuration conf) {
    this.config = conf;
    if (internalBalancer != null) {
      internalBalancer.setConf(conf);
    }
  }

  @Override
  public void setClusterMetrics(ClusterMetrics sm) {
    this.clusterStatus = sm;
    if (internalBalancer != null) {
      internalBalancer.setClusterMetrics(sm);
    }
  }

  @Override
  public void setMasterServices(MasterServices masterServices) {
    this.masterServices = masterServices;
  }

  /**
   * Override to balance by RSGroup
   * not invoke {@link #balanceTable(TableName, Map)}
   */
  @Override
  public List<RegionPlan> balanceCluster(
      Map<TableName, Map<ServerName, List<RegionInfo>>> loadOfAllTable) throws IOException {
    if (!isOnline()) {
      throw new ConstraintException(RSGroupInfoManager.RSGROUP_TABLE_NAME +
          " is not online, unable to perform balance");
    }
    // Calculate correct assignments and a list of RegionPlan for mis-placed regions
    Pair<Map<TableName, Map<ServerName, List<RegionInfo>>>, List<RegionPlan>>
      correctedStateAndRegionPlans = correctAssignments(loadOfAllTable);
    Map<TableName, Map<ServerName, List<RegionInfo>>> correctedLoadOfAllTable =
        correctedStateAndRegionPlans.getFirst();
    List<RegionPlan> regionPlans = correctedStateAndRegionPlans.getSecond();
    // Add RegionPlan for the regions which have been placed according to the region server group
    // assignment into the movement list
    try {
      // For each rsgroup
      for (RSGroupInfo rsgroup : rsGroupInfoManager.listRSGroups()) {
        Map<TableName, Map<ServerName, List<RegionInfo>>> loadOfTablesInGroup = new HashMap<>();
        for (Map.Entry<TableName, Map<ServerName, List<RegionInfo>>> entry : correctedLoadOfAllTable
            .entrySet()) {
          TableName tableName = entry.getKey();
          String targetRSGroupName = rsGroupInfoManager.getRSGroupOfTable(tableName);
          if (targetRSGroupName == null) {
            targetRSGroupName = RSGroupInfo.DEFAULT_GROUP;
          }
          if (targetRSGroupName.equals(rsgroup.getName())) {
            loadOfTablesInGroup.put(tableName, entry.getValue());
          }
        }
        List<RegionPlan> groupPlans = null;
        if (!loadOfTablesInGroup.isEmpty()) {
          LOG.info("Start Generate Balance plan for group: " + rsgroup.getName());
          groupPlans = this.internalBalancer.balanceCluster(loadOfTablesInGroup);
        }
        if (groupPlans != null) {
          regionPlans.addAll(groupPlans);
        }
      }
    } catch (IOException exp) {
      LOG.warn("Exception while balancing cluster.", exp);
      regionPlans.clear();
    }
    return regionPlans;
  }

  @Override
  @NonNull
  public Map<ServerName, List<RegionInfo>> roundRobinAssignment(List<RegionInfo> regions,
      List<ServerName> servers) throws HBaseIOException {
    Map<ServerName, List<RegionInfo>> assignments = Maps.newHashMap();
    List<Pair<List<RegionInfo>, List<ServerName>>> pairs =
      generateGroupAssignments(regions, servers);
    for (Pair<List<RegionInfo>, List<ServerName>> pair : pairs) {
      Map<ServerName, List<RegionInfo>> result =
          this.internalBalancer.roundRobinAssignment(pair.getFirst(), pair.getSecond());
      result.forEach((server, regionInfos) -> assignments
          .computeIfAbsent(server, s -> Lists.newArrayList()).addAll(regionInfos));
    }
    return assignments;
  }

  @Override
  @NonNull
  public Map<ServerName, List<RegionInfo>> retainAssignment(Map<RegionInfo, ServerName> regions,
      List<ServerName> servers) throws HBaseIOException {
    try {
      Map<ServerName, List<RegionInfo>> assignments = new TreeMap<>();
      List<Pair<List<RegionInfo>, List<ServerName>>> pairs =
          generateGroupAssignments(Lists.newArrayList(regions.keySet()), servers);
      for (Pair<List<RegionInfo>, List<ServerName>> pair : pairs) {
        List<RegionInfo> regionList = pair.getFirst();
        Map<RegionInfo, ServerName> currentAssignmentMap = Maps.newTreeMap();
        regionList.forEach(r -> currentAssignmentMap.put(r, regions.get(r)));
        Map<ServerName, List<RegionInfo>> pairResult =
            this.internalBalancer.retainAssignment(currentAssignmentMap, pair.getSecond());
        pairResult.forEach((server, rs) -> assignments
            .computeIfAbsent(server, s -> Lists.newArrayList()).addAll(rs));
      }
      return assignments;
    } catch (IOException e) {
      throw new HBaseIOException("Failed to do online retain assignment", e);
    }
  }

  @Override
  public ServerName randomAssignment(RegionInfo region,
      List<ServerName> servers) throws HBaseIOException {
    List<Pair<List<RegionInfo>, List<ServerName>>> pairs =
      generateGroupAssignments(Lists.newArrayList(region), servers);
    List<ServerName> filteredServers = pairs.iterator().next().getSecond();
    return this.internalBalancer.randomAssignment(region, filteredServers);
  }

  private List<Pair<List<RegionInfo>, List<ServerName>>> generateGroupAssignments(
    List<RegionInfo> regions, List<ServerName> servers) throws HBaseIOException {
    try {
      ListMultimap<String, RegionInfo> regionMap = ArrayListMultimap.create();
      ListMultimap<String, ServerName> serverMap = ArrayListMultimap.create();
      RSGroupInfo defaultInfo = rsGroupInfoManager.getRSGroup(RSGroupInfo.DEFAULT_GROUP);
      for (RegionInfo region : regions) {
        String groupName =
          Optional.ofNullable(rsGroupInfoManager.getRSGroupOfTable(region.getTable()))
            .orElse(defaultInfo.getName());
        regionMap.put(groupName, region);
      }
      for (String groupKey : regionMap.keySet()) {
        RSGroupInfo info = rsGroupInfoManager.getRSGroup(groupKey);
        serverMap.putAll(groupKey, filterOfflineServers(info, servers));
      }

      List<Pair<List<RegionInfo>, List<ServerName>>> result = Lists.newArrayList();
      List<RegionInfo> fallbackRegions = Lists.newArrayList();
      for (String groupKey : regionMap.keySet()) {
        if (serverMap.get(groupKey).isEmpty()) {
          fallbackRegions.addAll(regionMap.get(groupKey));
        } else {
          result.add(Pair.newPair(regionMap.get(groupKey), serverMap.get(groupKey)));
        }
      }
      if (!fallbackRegions.isEmpty()) {
        List<ServerName> candidates = null;
        if (isFallbackEnabled()) {
          candidates = getFallBackCandidates(servers);
        }
        candidates = (candidates == null || candidates.isEmpty()) ?
          Lists.newArrayList(BOGUS_SERVER_NAME) : candidates;
        result.add(Pair.newPair(fallbackRegions, candidates));
      }
      return result;
    } catch(IOException e) {
      throw new HBaseIOException("Failed to generate group assignments", e);
    }
  }

  private List<ServerName> filterOfflineServers(RSGroupInfo RSGroupInfo,
                                                List<ServerName> onlineServers) {
    if (RSGroupInfo != null) {
      return filterServers(RSGroupInfo.getServers(), onlineServers);
    } else {
      LOG.warn("RSGroup Information found to be null. Some regions might be unassigned.");
      return Collections.emptyList();
    }
  }

  /**
   * Filter servers based on the online servers.
   * <p/>
   * servers is actually a TreeSet (see {@link org.apache.hadoop.hbase.rsgroup.RSGroupInfo}), having
   * its contains()'s time complexity as O(logn), which is good enough.
   * <p/>
   * TODO: consider using HashSet to pursue O(1) for contains() throughout the calling chain if
   * needed.
   * @param servers the servers
   * @param onlineServers List of servers which are online.
   * @return the list
   */
  private List<ServerName> filterServers(Set<Address> servers, List<ServerName> onlineServers) {
    ArrayList<ServerName> finalList = new ArrayList<>();
    for (ServerName onlineServer : onlineServers) {
      if (servers.contains(onlineServer.getAddress())) {
        finalList.add(onlineServer);
      }
    }
    return finalList;
  }

  private Pair<Map<TableName, Map<ServerName, List<RegionInfo>>>, List<RegionPlan>>
      correctAssignments(Map<TableName, Map<ServerName, List<RegionInfo>>> existingAssignments)
          throws IOException {
    // To return
    Map<TableName, Map<ServerName, List<RegionInfo>>> correctAssignments = new HashMap<>();
    List<RegionPlan> regionPlansForMisplacedRegions = new ArrayList<>();
    for (Map.Entry<TableName, Map<ServerName, List<RegionInfo>>> assignments : existingAssignments
        .entrySet()) {
      TableName tableName = assignments.getKey();
      Map<ServerName, List<RegionInfo>> clusterLoad = assignments.getValue();
      Map<ServerName, List<RegionInfo>> correctServerRegion = new TreeMap<>();
      RSGroupInfo targetRSGInfo = null;
      try {
        String groupName = rsGroupInfoManager.getRSGroupOfTable(tableName);
        if (groupName == null) {
          LOG.debug("Group not found for table " + tableName + ", using default");
          groupName = RSGroupInfo.DEFAULT_GROUP;
        }
        targetRSGInfo = rsGroupInfoManager.getRSGroup(groupName);
      } catch (IOException exp) {
        LOG.debug("RSGroup information null for region of table " + tableName, exp);
      }
      for (Map.Entry<ServerName, List<RegionInfo>> serverRegionMap : clusterLoad.entrySet()) {
        ServerName currentHostServer = serverRegionMap.getKey();
        List<RegionInfo> regionInfoList = serverRegionMap.getValue();
        if (targetRSGInfo == null
            || !targetRSGInfo.containsServer(currentHostServer.getAddress())) {
          regionInfoList.forEach(regionInfo -> {
            regionPlansForMisplacedRegions.add(new RegionPlan(regionInfo, currentHostServer, null));
          });
        } else {
          correctServerRegion.put(currentHostServer, regionInfoList);
        }
      }
      correctAssignments.put(tableName, correctServerRegion);
    }
    // Return correct assignments and region movement plan for mis-placed regions together
    return new Pair<Map<TableName, Map<ServerName, List<RegionInfo>>>, List<RegionPlan>>(
        correctAssignments, regionPlansForMisplacedRegions);
  }

  @Override
  public void initialize() throws HBaseIOException {
    try {
      if (rsGroupInfoManager == null) {
        List<RSGroupAdminEndpoint> cps =
          masterServices.getMasterCoprocessorHost().findCoprocessors(RSGroupAdminEndpoint.class);
        if (cps.size() != 1) {
          String msg = "Expected one implementation of GroupAdminEndpoint but found " + cps.size();
          LOG.error(msg);
          throw new HBaseIOException(msg);
        }
        rsGroupInfoManager = cps.get(0).getGroupInfoManager();
        if(rsGroupInfoManager == null){
          String msg = "RSGroupInfoManager hasn't been initialized";
          LOG.error(msg);
          throw new HBaseIOException(msg);
        }
        rsGroupInfoManager.start();
      }
    } catch (IOException e) {
      throw new HBaseIOException("Failed to initialize GroupInfoManagerImpl", e);
    }

    // Create the balancer
    Class<? extends LoadBalancer> balancerClass = config.getClass(HBASE_RSGROUP_LOADBALANCER_CLASS,
        StochasticLoadBalancer.class, LoadBalancer.class);
    if (this.getClass().isAssignableFrom(balancerClass)) {
      LOG.warn("The internal balancer of RSGroupBasedLoadBalancer cannot be itself, " +
              "falling back to the default LoadBalancer class");
      balancerClass = LoadBalancerFactory.getDefaultLoadBalancerClass();
    }
    internalBalancer = ReflectionUtils.newInstance(balancerClass, config);
    internalBalancer.setMasterServices(masterServices);
    if (clusterStatus != null) {
      internalBalancer.setClusterMetrics(clusterStatus);
    }
    internalBalancer.setConf(config);
    internalBalancer.initialize();
    // init fallback groups
    this.fallbackEnabled = config.getBoolean(FALLBACK_GROUP_ENABLE_KEY, false);
  }

  public boolean isOnline() {
    if (this.rsGroupInfoManager == null) {
      return false;
    }

    return this.rsGroupInfoManager.isOnline();
  }

  public boolean isFallbackEnabled() {
    return fallbackEnabled;
  }

  @Override
  public void regionOnline(RegionInfo regionInfo, ServerName sn) {
  }

  @Override
  public void regionOffline(RegionInfo regionInfo) {
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    this.config = conf;
    boolean newFallbackEnabled = conf.getBoolean(FALLBACK_GROUP_ENABLE_KEY, false);
    if (fallbackEnabled != newFallbackEnabled) {
      LOG.info("Changing the value of {} from {} to {}", FALLBACK_GROUP_ENABLE_KEY,
        fallbackEnabled, newFallbackEnabled);
      fallbackEnabled = newFallbackEnabled;
    }
    internalBalancer.onConfigurationChange(conf);
  }

  @Override
  public void stop(String why) {
  }

  @Override
  public boolean isStopped() {
    return false;
  }

  public void setRsGroupInfoManager(RSGroupInfoManager rsGroupInfoManager) {
    this.rsGroupInfoManager = rsGroupInfoManager;
  }

  @Override
  public void postMasterStartupInitialize() {
    this.internalBalancer.postMasterStartupInitialize();
  }

  public void updateBalancerStatus(boolean status) {
    internalBalancer.updateBalancerStatus(status);
  }

  /**
   * can achieve table balanced rather than overall balanced
   */
  @Override
  public List<RegionPlan> balanceTable(TableName tableName,
      Map<ServerName, List<RegionInfo>> loadOfOneTable) {
    if (!isOnline()) {
      LOG.error(RSGroupInfoManager.class.getSimpleName()
          + " is not online, unable to perform balanceTable");
      return null;
    }
    Map<TableName, Map<ServerName, List<RegionInfo>>> loadOfThisTable = new HashMap<>();
    loadOfThisTable.put(tableName, loadOfOneTable);
    Pair<Map<TableName, Map<ServerName, List<RegionInfo>>>, List<RegionPlan>>
      correctedStateAndRegionPlans;
    // Calculate correct assignments and a list of RegionPlan for mis-placed regions
    try {
      correctedStateAndRegionPlans = correctAssignments(loadOfThisTable);
    } catch (IOException e) {
      LOG.error("get correct assignments and mis-placed regions error ", e);
      return null;
    }
    Map<TableName, Map<ServerName, List<RegionInfo>>> correctedLoadOfThisTable =
        correctedStateAndRegionPlans.getFirst();
    List<RegionPlan> regionPlans = correctedStateAndRegionPlans.getSecond();
    List<RegionPlan> tablePlans =
        this.internalBalancer.balanceTable(tableName, correctedLoadOfThisTable.get(tableName));

    if (tablePlans != null) {
      regionPlans.addAll(tablePlans);
    }
    return regionPlans;
  }

  private List<ServerName> getFallBackCandidates(List<ServerName> servers) {
    List<ServerName> serverNames = null;
    try {
      RSGroupInfo info = rsGroupInfoManager.getRSGroup(RSGroupInfo.DEFAULT_GROUP);
      serverNames = filterOfflineServers(info, servers);
    } catch (IOException e) {
      LOG.error("Failed to get default rsgroup info to fallback", e);
    }
    return serverNames == null || serverNames.isEmpty() ? servers : serverNames;
  }
}
