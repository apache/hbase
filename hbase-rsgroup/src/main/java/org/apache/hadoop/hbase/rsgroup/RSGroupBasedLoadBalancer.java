/**
 * Copyright The Apache Software Foundation
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

package org.apache.hadoop.hbase.rsgroup;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.balancer.StochasticLoadBalancer;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.ReflectionUtils;

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
public class RSGroupBasedLoadBalancer implements RSGroupableBalancer, LoadBalancer {
  /** Config for pluggable load balancers */
  public static final String HBASE_GROUP_LOADBALANCER_CLASS = "hbase.group.grouploadbalancer.class";

  private static final Log LOG = LogFactory.getLog(RSGroupBasedLoadBalancer.class);

  private Configuration config;
  private ClusterStatus clusterStatus;
  private MasterServices masterServices;
  private RSGroupInfoManager infoManager;
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

  //used during reflection by LoadBalancerFactory
  @InterfaceAudience.Private
  public RSGroupBasedLoadBalancer() {
  }

  //This constructor should only be used for unit testing
  @InterfaceAudience.Private
  public RSGroupBasedLoadBalancer(RSGroupInfoManager RSGroupInfoManager) {
    this.infoManager = RSGroupInfoManager;
  }

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
  public void setClusterStatus(ClusterStatus st) {
    this.clusterStatus = st;
    if (internalBalancer != null) {
      internalBalancer.setClusterStatus(st);
    }
  }

  @Override
  public void setMasterServices(MasterServices masterServices) {
    this.masterServices = masterServices;
  }

  @Override
  public List<RegionPlan> balanceCluster(TableName tableName, Map<ServerName, List<HRegionInfo>>
      clusterState) throws HBaseIOException {
    return balanceCluster(clusterState);
  }

  @Override
  public List<RegionPlan> balanceCluster(Map<ServerName, List<HRegionInfo>> clusterState)
      throws HBaseIOException {
    if (!isOnline()) {
      throw new ConstraintException(RSGroupInfoManager.RSGROUP_TABLE_NAME +
          " is not online, unable to perform balance");
    }

    Map<ServerName,List<HRegionInfo>> correctedState = correctAssignments(clusterState);
    List<RegionPlan> regionPlans = new ArrayList<>();

    List<HRegionInfo> misplacedRegions = correctedState.get(LoadBalancer.BOGUS_SERVER_NAME);
    for (HRegionInfo regionInfo : misplacedRegions) {
      if (fallbackEnabled) {
        regionPlans.add(new RegionPlan(regionInfo, findServerForRegion(clusterState, regionInfo),
          null));
      } else {
        regionPlans.add(new RegionPlan(regionInfo, null, null));
      }
    }
    try {
      // Record which region servers have been processed，so as to skip them after processed
      HashSet<ServerName> processedServers = new HashSet<>();

      // For each rsgroup
      for (RSGroupInfo rsgroup : infoManager.listRSGroups()) {
        Map<ServerName, List<HRegionInfo>> groupClusterState = new HashMap<>();
        for (ServerName server : clusterState.keySet()) { // for each region server
          if (!processedServers.contains(server) // server is not processed yet
              && rsgroup.containsServer(server.getAddress())) { // server belongs to this rsgroup
            List<HRegionInfo> regionsOnServer = correctedState.get(server);
            groupClusterState.put(server, regionsOnServer);
            processedServers.add(server);
          }
        }

        List<RegionPlan> groupPlans = this.internalBalancer
            .balanceCluster(groupClusterState);
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
  public Map<ServerName, List<HRegionInfo>> roundRobinAssignment(
      List<HRegionInfo> regions, List<ServerName> servers) throws HBaseIOException {
    Map<ServerName, List<HRegionInfo>> assignments = Maps.newHashMap();
    List<Pair<List<HRegionInfo>, List<ServerName>>> pairs =
      generateGroupAssignments(regions, servers);
    for (Pair<List<HRegionInfo>, List<ServerName>> pair : pairs) {
      Map<ServerName, List<HRegionInfo>> result = this.internalBalancer
        .roundRobinAssignment(pair.getFirst(), pair.getSecond());
      if (result != null) {
        for (Map.Entry<ServerName, List<HRegionInfo>> entry : result.entrySet()) {
          ServerName serverName = entry.getKey();
          List<HRegionInfo> regionInfos = entry.getValue();
          if (!assignments.containsKey(serverName)) {
            assignments.put(serverName, Lists.<HRegionInfo>newArrayList());
          }
          assignments.get(serverName).addAll(regionInfos);
        }
      }
    }
    return assignments;
  }

  @Override
  public Map<ServerName, List<HRegionInfo>> retainAssignment(
      Map<HRegionInfo, ServerName> regions, List<ServerName> servers) throws HBaseIOException {
    try {
      Map<ServerName, List<HRegionInfo>> assignments = new TreeMap<>();
      List<Pair<List<HRegionInfo>, List<ServerName>>> pairs =
        generateGroupAssignments(Lists.newArrayList(regions.keySet()), servers);
      for (Pair<List<HRegionInfo>, List<ServerName>> pair : pairs) {
        List<HRegionInfo> regionList = pair.getFirst();
        Map<HRegionInfo, ServerName> currentAssignmentMap = Maps.newTreeMap();
        for (HRegionInfo regionInfo: regionList) {
          currentAssignmentMap.put(regionInfo, regions.get(regionInfo));
        }
        Map<ServerName, List<HRegionInfo>> pairResult =
          this.internalBalancer.retainAssignment(currentAssignmentMap, pair.getSecond());
        for (Map.Entry<ServerName, List<HRegionInfo>> entry : pairResult.entrySet()) {
          ServerName serverName = entry.getKey();
          List<HRegionInfo> regionInfos = entry.getValue();
          if (!assignments.containsKey(serverName)) {
            assignments.put(serverName, Lists.<HRegionInfo>newArrayList());
          }
          assignments.get(serverName).addAll(regionInfos);
        }
      }
      return assignments;
    } catch (IOException e) {
      throw new HBaseIOException("Failed to do online retain assignment", e);
    }
  }

  @Override
  public Map<HRegionInfo, ServerName> immediateAssignment(List<HRegionInfo> regions,
      List<ServerName> servers) throws HBaseIOException {
    throw new UnsupportedOperationException("immediateAssignment is not supported");
  }

  @Override
  public ServerName randomAssignment(HRegionInfo region,
      List<ServerName> servers) throws HBaseIOException {
    List<Pair<List<HRegionInfo>, List<ServerName>>> pairs =
      generateGroupAssignments(Lists.newArrayList(region), servers);
    List<ServerName> filteredServers = pairs.iterator().next().getSecond();
    return this.internalBalancer.randomAssignment(region, filteredServers);
  }

  private List<Pair<List<HRegionInfo>, List<ServerName>>> generateGroupAssignments(
    List<HRegionInfo> regions, List<ServerName> servers) throws HBaseIOException {
    try {
      ListMultimap<String, HRegionInfo> regionMap = ArrayListMultimap.create();
      ListMultimap<String, ServerName> serverMap = ArrayListMultimap.create();
      for (HRegionInfo region : regions) {
        String groupName = infoManager.getRSGroupOfTable(region.getTable());
        if (groupName == null) {
          LOG.debug("Group not found for table " + region.getTable() + ", using default");
          groupName = RSGroupInfo.DEFAULT_GROUP;
        }
        regionMap.put(groupName, region);
      }
      for (String groupKey : regionMap.keySet()) {
        RSGroupInfo info = infoManager.getRSGroup(groupKey);
        serverMap.putAll(groupKey, filterOfflineServers(info, servers));
      }

      List<Pair<List<HRegionInfo>, List<ServerName>>> result = Lists.newArrayList();
      List<HRegionInfo> fallbackRegions = Lists.newArrayList();
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
      LOG.debug("Group Information found to be null. Some regions might be unassigned.");
      return Collections.emptyList();
    }
  }

  /**
   * Filter servers based on the online servers.
   *
   * @param servers
   *          the servers
   * @param onlineServers
   *          List of servers which are online.
   * @return the list
   */
  private List<ServerName> filterServers(Set<Address> servers,
      List<ServerName> onlineServers) {
    /**
     * servers is actually a TreeSet (see {@link org.apache.hadoop.hbase.rsgroup.RSGroupInfo}),
     * having its contains()'s time complexity as O(logn), which is good enough.
     * TODO: consider using HashSet to pursue O(1) for contains() throughout the calling chain
     * if needed.
     */
    ArrayList<ServerName> finalList = new ArrayList<>();
    for (ServerName onlineServer : onlineServers) {
      if (servers.contains(onlineServer.getAddress())) {
        finalList.add(onlineServer);
      }
    }

    return finalList;
  }

  public Set<HRegionInfo> getMisplacedRegions(
      Map<HRegionInfo, ServerName> regions) throws IOException {
    Set<HRegionInfo> misplacedRegions = new HashSet<HRegionInfo>();
    for(Map.Entry<HRegionInfo, ServerName> region : regions.entrySet()) {
      HRegionInfo regionInfo = region.getKey();
      ServerName assignedServer = region.getValue();
      String groupName = infoManager.getRSGroupOfTable(regionInfo.getTable());
      if (groupName == null) {
        LOG.debug("Group not found for table " + regionInfo.getTable() + ", using default");
        groupName = RSGroupInfo.DEFAULT_GROUP;
      }
      RSGroupInfo info = infoManager.getRSGroup(groupName);
      if (assignedServer == null) {
        LOG.debug("There is no assigned server for " + region);
        continue;
      }
      RSGroupInfo otherInfo = infoManager.getRSGroupOfServer(assignedServer.getAddress());
      if (info == null && otherInfo == null) {
        LOG.warn("Couldn't obtain rs group information for " + region + " on " + assignedServer);
        continue;
      }
      if ((info == null || !info.containsServer(assignedServer.getAddress()))) {
        LOG.debug("Found misplaced region: " + regionInfo.getRegionNameAsString() +
            " on server: " + assignedServer +
            " found in group: " +  otherInfo +
            " outside of group: " + (info == null ? "UNKNOWN" : info.getName()));
        misplacedRegions.add(regionInfo);
      }
    }
    return misplacedRegions;
  }

  private ServerName findServerForRegion(
    Map<ServerName, List<HRegionInfo>> existingAssignments, HRegionInfo region) {
    for (Map.Entry<ServerName, List<HRegionInfo>> entry : existingAssignments.entrySet()) {
      if (entry.getValue().contains(region)) {
        return entry.getKey();
      }
    }

    throw new IllegalStateException("Could not find server for region "
      + region.getShortNameToLog());
  }

  private Map<ServerName, List<HRegionInfo>> correctAssignments(
      Map<ServerName, List<HRegionInfo>> existingAssignments) {
    Map<ServerName, List<HRegionInfo>> correctAssignments =
        new TreeMap<ServerName, List<HRegionInfo>>();
    correctAssignments.put(LoadBalancer.BOGUS_SERVER_NAME, new LinkedList<HRegionInfo>());
    for (Map.Entry<ServerName, List<HRegionInfo>> assignments : existingAssignments.entrySet()){
      ServerName sName = assignments.getKey();
      correctAssignments.put(sName, new LinkedList<HRegionInfo>());
      List<HRegionInfo> regions = assignments.getValue();
      for (HRegionInfo region : regions) {
        RSGroupInfo info = null;
        try {
          String groupName = infoManager.getRSGroupOfTable(region.getTable());
          if (groupName == null) {
            LOG.debug("Group not found for table " + region.getTable() + ", using default");
            groupName = RSGroupInfo.DEFAULT_GROUP;
          }
          info = infoManager.getRSGroup(groupName);
        } catch (IOException exp) {
          LOG.debug("Group information null for region of table " + region.getTable(),
              exp);
        }
        if ((info == null) || (!info.containsServer(sName.getAddress()))) {
          correctAssignments.get(LoadBalancer.BOGUS_SERVER_NAME).add(region);
        } else {
          correctAssignments.get(sName).add(region);
        }
      }
    }
    return correctAssignments;
  }

  @Override
  public void initialize() throws HBaseIOException {
    try {
      if (infoManager == null) {
        List<RSGroupAdminEndpoint> cps =
          masterServices.getMasterCoprocessorHost().findCoprocessors(RSGroupAdminEndpoint.class);
        if (cps.size() != 1) {
          String msg = "Expected one implementation of GroupAdminEndpoint but found " + cps.size();
          LOG.error(msg);
          throw new HBaseIOException(msg);
        }
        infoManager = cps.get(0).getGroupInfoManager();
        if(infoManager == null){
          String msg = "RSGroupInfoManager hasn't been initialized";
          LOG.error(msg);
          throw new HBaseIOException(msg);
        }
        infoManager.start();
      }
    } catch (IOException e) {
      throw new HBaseIOException("Failed to initialize GroupInfoManagerImpl", e);
    }

    // Create the balancer
    Class<? extends LoadBalancer> balancerKlass = config.getClass(
        HBASE_GROUP_LOADBALANCER_CLASS,
        StochasticLoadBalancer.class, LoadBalancer.class);
    internalBalancer = ReflectionUtils.newInstance(balancerKlass, config);
    if (clusterStatus != null) {
      internalBalancer.setClusterStatus(clusterStatus);
    }
    internalBalancer.setMasterServices(masterServices);
    internalBalancer.setConf(config);
    internalBalancer.initialize();
    // init fallback groups
    this.fallbackEnabled = config.getBoolean(FALLBACK_GROUP_ENABLE_KEY, false);
  }

  public boolean isOnline() {
    return infoManager != null && infoManager.isOnline();
  }

  public boolean isFallbackEnabled() {
    return fallbackEnabled;
  }

  @Override
  public void regionOnline(HRegionInfo regionInfo, ServerName sn) {
  }

  @Override
  public void regionOffline(HRegionInfo regionInfo) {
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    boolean newFallbackEnabled = conf.getBoolean(FALLBACK_GROUP_ENABLE_KEY, false);
    if (fallbackEnabled != newFallbackEnabled) {
      LOG.info("Changing the value of " + FALLBACK_GROUP_ENABLE_KEY + " from " + fallbackEnabled
        + " to " + newFallbackEnabled);
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

  @Override
  public void postMasterStartupInitialize() {
    this.internalBalancer.postMasterStartupInitialize();
  }

  public void updateBalancerStatus(boolean status) {
    internalBalancer.updateBalancerStatus(status);
  }

  private List<ServerName> getFallBackCandidates(List<ServerName> servers) {
    List<ServerName> serverNames = null;
    try {
      RSGroupInfo info = infoManager.getRSGroup(RSGroupInfo.DEFAULT_GROUP);
      serverNames = filterOfflineServers(info, servers);
    } catch (IOException e) {
      LOG.error("Failed to get default rsgroup info to fallback", e);
    }
    return serverNames == null || serverNames.isEmpty() ? servers : serverNames;
  }
}
