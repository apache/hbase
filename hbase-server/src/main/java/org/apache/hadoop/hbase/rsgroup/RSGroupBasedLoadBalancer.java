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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
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
import org.apache.hadoop.hbase.master.balancer.LoadBalancerFactory;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.LinkedListMultimap;
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

  private Configuration config;
  private ClusterMetrics clusterStatus;
  private MasterServices masterServices;
  private FavoredNodesManager favoredNodesManager;
  private volatile RSGroupInfoManager rsGroupInfoManager;
  private LoadBalancer internalBalancer;

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
    if(internalBalancer != null) {
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
      throw new ConstraintException(
          RSGroupInfoManager.class.getSimpleName() + " is not online, unable to perform balance");
    }

    // Calculate correct assignments and a list of RegionPlan for mis-placed regions
    Pair<Map<TableName, Map<ServerName, List<RegionInfo>>>, List<RegionPlan>>
      correctedStateAndRegionPlans = correctAssignments(loadOfAllTable);
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
  public Map<ServerName, List<RegionInfo>> roundRobinAssignment(
      List<RegionInfo> regions, List<ServerName> servers) throws IOException {
    Map<ServerName, List<RegionInfo>> assignments = Maps.newHashMap();
    ListMultimap<String, RegionInfo> regionMap = ArrayListMultimap.create();
    ListMultimap<String, ServerName> serverMap = ArrayListMultimap.create();
    generateGroupMaps(regions, servers, regionMap, serverMap);
    for (String groupKey : regionMap.keySet()) {
      if (regionMap.get(groupKey).size() > 0) {
        Map<ServerName, List<RegionInfo>> result = this.internalBalancer
          .roundRobinAssignment(regionMap.get(groupKey), serverMap.get(groupKey));
        if (result != null) {
          if (result.containsKey(LoadBalancer.BOGUS_SERVER_NAME) &&
            assignments.containsKey(LoadBalancer.BOGUS_SERVER_NAME)) {
            assignments.get(LoadBalancer.BOGUS_SERVER_NAME)
              .addAll(result.get(LoadBalancer.BOGUS_SERVER_NAME));
          } else {
            assignments.putAll(result);
          }
        }
      }
    }
    return assignments;
  }

  @Override
  public Map<ServerName, List<RegionInfo>> retainAssignment(Map<RegionInfo, ServerName> regions,
    List<ServerName> servers) throws HBaseIOException {
    try {
      Map<ServerName, List<RegionInfo>> assignments = new TreeMap<>();
      ListMultimap<String, RegionInfo> groupToRegion = ArrayListMultimap.create();
      RSGroupInfo defaultInfo = rsGroupInfoManager.getRSGroup(RSGroupInfo.DEFAULT_GROUP);
      for (RegionInfo region : regions.keySet()) {
        String groupName =
          RSGroupUtil.getRSGroupInfo(masterServices, rsGroupInfoManager, region.getTable())
              .orElse(defaultInfo).getName();
        groupToRegion.put(groupName, region);
      }
      for (String key : groupToRegion.keySet()) {
        Map<RegionInfo, ServerName> currentAssignmentMap = new TreeMap<RegionInfo, ServerName>();
        List<RegionInfo> regionList = groupToRegion.get(key);
        RSGroupInfo info = rsGroupInfoManager.getRSGroup(key);
        List<ServerName> candidateList = filterOfflineServers(info, servers);
        for (RegionInfo region : regionList) {
          currentAssignmentMap.put(region, regions.get(region));
        }
        if (candidateList.size() > 0) {
          assignments
            .putAll(this.internalBalancer.retainAssignment(currentAssignmentMap, candidateList));
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("No available servers to assign regions: {}",
              RegionInfo.getShortNameToLog(regionList));
          }
          assignments.computeIfAbsent(LoadBalancer.BOGUS_SERVER_NAME, s -> new ArrayList<>())
            .addAll(regionList);
        }
      }
      return assignments;
    } catch (IOException e) {
      throw new HBaseIOException("Failed to do online retain assignment", e);
    }
  }

  @Override
  public ServerName randomAssignment(RegionInfo region,
      List<ServerName> servers) throws IOException {
    ListMultimap<String,RegionInfo> regionMap = LinkedListMultimap.create();
    ListMultimap<String,ServerName> serverMap = LinkedListMultimap.create();
    generateGroupMaps(Lists.newArrayList(region), servers, regionMap, serverMap);
    List<ServerName> filteredServers = serverMap.get(regionMap.keySet().iterator().next());
    return this.internalBalancer.randomAssignment(region, filteredServers);
  }

  private void generateGroupMaps(List<RegionInfo> regions, List<ServerName> servers,
    ListMultimap<String, RegionInfo> regionMap, ListMultimap<String, ServerName> serverMap)
    throws HBaseIOException {
    try {
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
        if(serverMap.get(groupKey).size() < 1) {
          serverMap.put(groupKey, LoadBalancer.BOGUS_SERVER_NAME);
        }
      }
    } catch(IOException e) {
      throw new HBaseIOException("Failed to generate group maps", e);
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
    Class<? extends LoadBalancer> balancerClass;
    String balancerClassName = config.get(HBASE_RSGROUP_LOADBALANCER_CLASS);
    if (balancerClassName == null) {
      balancerClass = config.getClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
        LoadBalancerFactory.getDefaultLoadBalancerClass(), LoadBalancer.class);
    } else {
      try {
        balancerClass = Class.forName(balancerClassName).asSubclass(LoadBalancer.class);
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }
    }
    // avoid infinite nesting
    if (getClass().isAssignableFrom(balancerClass)) {
      balancerClass = LoadBalancerFactory.getDefaultLoadBalancerClass();
    }
    internalBalancer = ReflectionUtils.newInstance(balancerClass);
    if (internalBalancer instanceof FavoredNodesPromoter) {
      favoredNodesManager = new FavoredNodesManager(masterServices);
    }
    internalBalancer.setConf(config);
    internalBalancer.setMasterServices(masterServices);
    if(clusterStatus != null) {
      internalBalancer.setClusterMetrics(clusterStatus);
    }
    internalBalancer.initialize();
  }

  public boolean isOnline() {
    if (this.rsGroupInfoManager == null) {
      return false;
    }

    return this.rsGroupInfoManager.isOnline();
  }


  @Override
  public void regionOnline(RegionInfo regionInfo, ServerName sn) {
  }

  @Override
  public void regionOffline(RegionInfo regionInfo) {
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    //DO nothing for now
  }

  @Override
  public void stop(String why) {
  }

  @Override
  public boolean isStopped() {
    return false;
  }

  @VisibleForTesting
  public void setRsGroupInfoManager(RSGroupInfoManager rsGroupInfoManager) {
    this.rsGroupInfoManager = rsGroupInfoManager;
  }

  public LoadBalancer getInternalBalancer() {
    return internalBalancer;
  }

  public FavoredNodesManager getFavoredNodesManager() {
    return favoredNodesManager;
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
}
