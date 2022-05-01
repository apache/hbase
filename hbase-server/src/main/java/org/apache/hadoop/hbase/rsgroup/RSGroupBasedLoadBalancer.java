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
package org.apache.hadoop.hbase.rsgroup;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.favored.FavoredNodesManager;
import org.apache.hadoop.hbase.favored.FavoredNodesPromoter;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.balancer.ClusterInfoProvider;
import org.apache.hadoop.hbase.master.balancer.LoadBalancerFactory;
import org.apache.hadoop.hbase.master.balancer.MasterClusterInfoProvider;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;

/**
 * GroupBasedLoadBalancer, used when Region Server Grouping is configured (HBase-6721) It does
 * region balance based on a table's group membership.
 * <p/>
 * Most assignment methods contain two exclusive code paths: Online - when the group table is online
 * and Offline - when it is unavailable.
 * <p/>
 * During Offline, assignments are assigned based on cached information in zookeeper. If unavailable
 * (ie bootstrap) then regions are assigned randomly.
 * <p/>
 * Once the GROUP table has been assigned, the balancer switches to Online and will then start
 * providing appropriate assignments for user tables.
 */
@InterfaceAudience.Private
public class RSGroupBasedLoadBalancer implements LoadBalancer {
  private static final Logger LOG = LoggerFactory.getLogger(RSGroupBasedLoadBalancer.class);

  private MasterServices masterServices;
  private ClusterInfoProvider provider;
  private FavoredNodesManager favoredNodesManager;
  private volatile RSGroupInfoManager rsGroupInfoManager;
  private volatile LoadBalancer internalBalancer;

  /**
   * Set this key to {@code true} to allow region fallback. Fallback to the default rsgroup first,
   * then fallback to any group if no online servers in default rsgroup. Please keep balancer switch
   * on at the same time, which is relied on to correct misplaced regions
   */
  public static final String FALLBACK_GROUP_ENABLE_KEY = "hbase.rsgroup.fallback.enable";

  private volatile boolean fallbackEnabled = false;

  /**
   * Used by reflection in {@link org.apache.hadoop.hbase.master.balancer.LoadBalancerFactory}.
   */
  @InterfaceAudience.Private
  public RSGroupBasedLoadBalancer() {
  }

  // must be called after calling initialize
  @Override
  public synchronized void updateClusterMetrics(ClusterMetrics sm) {
    assert internalBalancer != null;
    internalBalancer.updateClusterMetrics(sm);
  }

  @Override
  public synchronized void
    updateBalancerLoadInfo(Map<TableName, Map<ServerName, List<RegionInfo>>> loadOfAllTable) {
    internalBalancer.updateBalancerLoadInfo(loadOfAllTable);
  }

  public void setMasterServices(MasterServices masterServices) {
    this.masterServices = masterServices;
  }

  /**
   * Balance by RSGroup.
   */
  @Override
  public synchronized List<RegionPlan> balanceCluster(
    Map<TableName, Map<ServerName, List<RegionInfo>>> loadOfAllTable) throws IOException {
    if (!isOnline()) {
      throw new ConstraintException(
        RSGroupInfoManager.class.getSimpleName() + " is not online, unable to perform balance");
    }

    // Calculate correct assignments and a list of RegionPlan for mis-placed regions
    Pair<Map<TableName, Map<ServerName, List<RegionInfo>>>,
      List<RegionPlan>> correctedStateAndRegionPlans = correctAssignments(loadOfAllTable);
    Map<TableName, Map<ServerName, List<RegionInfo>>> correctedLoadOfAllTable =
      correctedStateAndRegionPlans.getFirst();
    List<RegionPlan> regionPlans = correctedStateAndRegionPlans.getSecond();
    RSGroupInfo defaultInfo = rsGroupInfoManager.getRSGroup(RSGroupInfo.DEFAULT_GROUP);
    // Add RegionPlan
    // for the regions which have been placed according to the region server group assignment
    // into the movement list
    try {
      // For each rsgroup
      for (RSGroupInfo rsgroup : rsGroupInfoManager.listRSGroups()) {
        LOG.debug("Balancing RSGroup={}", rsgroup.getName());
        Map<TableName, Map<ServerName, List<RegionInfo>>> loadOfTablesInGroup = new HashMap<>();
        for (Map.Entry<TableName, Map<ServerName, List<RegionInfo>>> entry : correctedLoadOfAllTable
          .entrySet()) {
          TableName tableName = entry.getKey();
          RSGroupInfo targetRSGInfo = RSGroupUtil
            .getRSGroupInfo(masterServices, rsGroupInfoManager, tableName).orElse(defaultInfo);
          if (targetRSGInfo.getName().equals(rsgroup.getName())) {
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

    // Return the whole movement list
    return regionPlans;
  }

  @Override
  @NonNull
  public Map<ServerName, List<RegionInfo>> roundRobinAssignment(List<RegionInfo> regions,
    List<ServerName> servers) throws IOException {
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
  public ServerName randomAssignment(RegionInfo region, List<ServerName> servers)
    throws IOException {
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
          RSGroupUtil.getRSGroupInfo(masterServices, rsGroupInfoManager, region.getTable())
            .orElse(defaultInfo).getName();
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
          if (LOG.isDebugEnabled()) {
            LOG.debug("Falling back {} regions to servers outside their RSGroup. Regions: {}",
              fallbackRegions.size(), fallbackRegions.stream()
                .map(RegionInfo::getRegionNameAsString).collect(Collectors.toSet()));
          }
          candidates = getFallBackCandidates(servers);
        }
        candidates = (candidates == null || candidates.isEmpty())
          ? Lists.newArrayList(BOGUS_SERVER_NAME)
          : candidates;
        result.add(Pair.newPair(fallbackRegions, candidates));
      }
      return result;
    } catch (IOException e) {
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
   * @param servers       the servers
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
    RSGroupInfo defaultInfo = rsGroupInfoManager.getRSGroup(RSGroupInfo.DEFAULT_GROUP);
    for (Map.Entry<TableName, Map<ServerName, List<RegionInfo>>> assignments : existingAssignments
      .entrySet()) {
      TableName tableName = assignments.getKey();
      Map<ServerName, List<RegionInfo>> clusterLoad = assignments.getValue();
      RSGroupInfo targetRSGInfo = null;
      Map<ServerName, List<RegionInfo>> correctServerRegion = new TreeMap<>();
      try {
        targetRSGInfo = RSGroupUtil.getRSGroupInfo(masterServices, rsGroupInfoManager, tableName)
          .orElse(defaultInfo);
      } catch (IOException exp) {
        LOG.debug("RSGroup information null for region of table " + tableName, exp);
      }
      for (Map.Entry<ServerName, List<RegionInfo>> serverRegionMap : clusterLoad.entrySet()) {
        ServerName currentHostServer = serverRegionMap.getKey();
        List<RegionInfo> regionInfoList = serverRegionMap.getValue();
        if (
          targetRSGInfo == null || !targetRSGInfo.containsServer(currentHostServer.getAddress())
        ) {
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
  public void initialize() throws IOException {
    if (rsGroupInfoManager == null) {
      rsGroupInfoManager = masterServices.getRSGroupInfoManager();
      if (rsGroupInfoManager == null) {
        String msg = "RSGroupInfoManager hasn't been initialized";
        LOG.error(msg);
        throw new HBaseIOException(msg);
      }
      rsGroupInfoManager.start();
    }

    // Create the balancer
    Configuration conf = masterServices.getConfiguration();
    Class<? extends LoadBalancer> balancerClass;
    @SuppressWarnings("deprecation")
    String balancerClassName = conf.get(HBASE_RSGROUP_LOADBALANCER_CLASS);
    if (balancerClassName == null) {
      balancerClass = conf.getClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
        LoadBalancerFactory.getDefaultLoadBalancerClass(), LoadBalancer.class);
    } else {
      try {
        balancerClass = Class.forName(balancerClassName).asSubclass(LoadBalancer.class);
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }
    }
    this.provider = new MasterClusterInfoProvider(masterServices);
    // avoid infinite nesting
    if (getClass().isAssignableFrom(balancerClass)) {
      balancerClass = LoadBalancerFactory.getDefaultLoadBalancerClass();
    }
    internalBalancer = ReflectionUtils.newInstance(balancerClass);
    internalBalancer.setClusterInfoProvider(provider);
    // special handling for favor node balancers
    if (internalBalancer instanceof FavoredNodesPromoter) {
      favoredNodesManager = new FavoredNodesManager(provider);
      ((FavoredNodesPromoter) internalBalancer).setFavoredNodesManager(favoredNodesManager);
    }
    internalBalancer.initialize();
    // init fallback groups
    this.fallbackEnabled = conf.getBoolean(FALLBACK_GROUP_ENABLE_KEY, false);
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
  public synchronized void onConfigurationChange(Configuration conf) {
    boolean newFallbackEnabled = conf.getBoolean(FALLBACK_GROUP_ENABLE_KEY, false);
    if (fallbackEnabled != newFallbackEnabled) {
      LOG.info("Changing the value of {} from {} to {}", FALLBACK_GROUP_ENABLE_KEY, fallbackEnabled,
        newFallbackEnabled);
      fallbackEnabled = newFallbackEnabled;
    }
    provider.onConfigurationChange(conf);
    internalBalancer.onConfigurationChange(conf);
  }

  @Override
  public void stop(String why) {
    internalBalancer.stop(why);
  }

  @Override
  public boolean isStopped() {
    return internalBalancer.isStopped();
  }

  public LoadBalancer getInternalBalancer() {
    return internalBalancer;
  }

  public FavoredNodesManager getFavoredNodesManager() {
    return favoredNodesManager;
  }

  @Override
  public synchronized void postMasterStartupInitialize() {
    this.internalBalancer.postMasterStartupInitialize();
  }

  public void updateBalancerStatus(boolean status) {
    internalBalancer.updateBalancerStatus(status);
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

  @Override
  public void setClusterInfoProvider(ClusterInfoProvider provider) {
    throw new UnsupportedOperationException("Just call set master service instead");
  }
}
