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
import java.util.HashSet;
import java.util.LinkedList;
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
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.balancer.StochasticLoadBalancer;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.ReflectionUtils;
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
public class RSGroupBasedLoadBalancer implements RSGroupableBalancer {
  private static final Logger LOG = LoggerFactory.getLogger(RSGroupBasedLoadBalancer.class);

  private Configuration config;
  private ClusterMetrics clusterStatus;
  private MasterServices masterServices;
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

  @Override
  public List<RegionPlan> balanceCluster(TableName tableName, Map<ServerName, List<RegionInfo>>
      clusterState) throws HBaseIOException {
    return balanceCluster(clusterState);
  }

  @Override
  public List<RegionPlan> balanceCluster(Map<ServerName, List<RegionInfo>> clusterState)
      throws HBaseIOException {
    if (!isOnline()) {
      throw new ConstraintException(
          RSGroupInfoManager.class.getSimpleName() + " is not online, unable to perform balance");
    }

    // Calculate correct assignments and a list of RegionPlan for mis-placed regions
    Pair<Map<ServerName,List<RegionInfo>>, List<RegionPlan>> correctedStateAndRegionPlans =
        correctAssignments(clusterState);
    Map<ServerName,List<RegionInfo>> correctedState = correctedStateAndRegionPlans.getFirst();
    List<RegionPlan> regionPlans = correctedStateAndRegionPlans.getSecond();

    // Add RegionPlan
    // for the regions which have been placed according to the region server group assignment
    // into the movement list
    try {
      // Record which region servers have been processed，so as to skip them after processed
      HashSet<ServerName> processedServers = new HashSet<>();

      // For each rsgroup
      for (RSGroupInfo rsgroup : rsGroupInfoManager.listRSGroups()) {
        Map<ServerName, List<RegionInfo>> groupClusterState = new HashMap<>();
        Map<TableName, Map<ServerName, List<RegionInfo>>> groupClusterLoad = new HashMap<>();
        for (ServerName server : clusterState.keySet()) { // for each region server
          if (!processedServers.contains(server) // server is not processed yet
              && rsgroup.containsServer(server.getAddress())) { // server belongs to this rsgroup
            List<RegionInfo> regionsOnServer = correctedState.get(server);
            groupClusterState.put(server, regionsOnServer);

            processedServers.add(server);
          }
        }

        groupClusterLoad.put(HConstants.ENSEMBLE_TABLE_NAME, groupClusterState);
        this.internalBalancer.setClusterLoad(groupClusterLoad);
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

    // Return the whole movement list
    return regionPlans;
  }

  @Override
  public Map<ServerName, List<RegionInfo>> roundRobinAssignment(
      List<RegionInfo> regions, List<ServerName> servers) throws HBaseIOException {
    Map<ServerName, List<RegionInfo>> assignments = Maps.newHashMap();
    ListMultimap<String,RegionInfo> regionMap = ArrayListMultimap.create();
    ListMultimap<String,ServerName> serverMap = ArrayListMultimap.create();
    generateGroupMaps(regions, servers, regionMap, serverMap);
    for(String groupKey : regionMap.keySet()) {
      if (regionMap.get(groupKey).size() > 0) {
        Map<ServerName, List<RegionInfo>> result =
            this.internalBalancer.roundRobinAssignment(
                regionMap.get(groupKey),
                serverMap.get(groupKey));
        if(result != null) {
          if(result.containsKey(LoadBalancer.BOGUS_SERVER_NAME) &&
              assignments.containsKey(LoadBalancer.BOGUS_SERVER_NAME)){
            assignments.get(LoadBalancer.BOGUS_SERVER_NAME).addAll(
              result.get(LoadBalancer.BOGUS_SERVER_NAME));
          } else {
            assignments.putAll(result);
          }
        }
      }
    }
    return assignments;
  }

  @Override
  public Map<ServerName, List<RegionInfo>> retainAssignment(
      Map<RegionInfo, ServerName> regions, List<ServerName> servers) throws HBaseIOException {
    try {
      Map<ServerName, List<RegionInfo>> assignments = new TreeMap<>();
      ListMultimap<String, RegionInfo> groupToRegion = ArrayListMultimap.create();
      Set<RegionInfo> misplacedRegions = getMisplacedRegions(regions);
      for (RegionInfo region : regions.keySet()) {
        if (!misplacedRegions.contains(region)) {
          String groupName = rsGroupInfoManager.getRSGroupOfTable(region.getTable());
          if (groupName == null) {
            LOG.debug("Group not found for table " + region.getTable() + ", using default");
            groupName = RSGroupInfo.DEFAULT_GROUP;
          }
          groupToRegion.put(groupName, region);
        }
      }
      // Now the "groupToRegion" map has only the regions which have correct
      // assignments.
      for (String key : groupToRegion.keySet()) {
        Map<RegionInfo, ServerName> currentAssignmentMap = new TreeMap<RegionInfo, ServerName>();
        List<RegionInfo> regionList = groupToRegion.get(key);
        RSGroupInfo info = rsGroupInfoManager.getRSGroup(key);
        List<ServerName> candidateList = filterOfflineServers(info, servers);
        for (RegionInfo region : regionList) {
          currentAssignmentMap.put(region, regions.get(region));
        }
        if(candidateList.size() > 0) {
          assignments.putAll(this.internalBalancer.retainAssignment(
              currentAssignmentMap, candidateList));
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("No available servers to assign regions: {}",
                RegionInfo.getShortNameToLog(regionList));
          }
          assignments.computeIfAbsent(LoadBalancer.BOGUS_SERVER_NAME, s -> new ArrayList<>())
              .addAll(regionList);
        }
      }

      for (RegionInfo region : misplacedRegions) {
        String groupName = rsGroupInfoManager.getRSGroupOfTable(region.getTable());
        if (groupName == null) {
          LOG.debug("Group not found for table " + region.getTable() + ", using default");
          groupName = RSGroupInfo.DEFAULT_GROUP;
        }
        RSGroupInfo info = rsGroupInfoManager.getRSGroup(groupName);
        List<ServerName> candidateList = filterOfflineServers(info, servers);
        ServerName server = this.internalBalancer.randomAssignment(region,
            candidateList);
        if (server != null) {
          assignments.computeIfAbsent(server, s -> new ArrayList<>()).add(region);
        } else {
          assignments.computeIfAbsent(LoadBalancer.BOGUS_SERVER_NAME, s -> new ArrayList<>())
              .add(region);
        }
      }
      return assignments;
    } catch (IOException e) {
      throw new HBaseIOException("Failed to do online retain assignment", e);
    }
  }

  @Override
  public ServerName randomAssignment(RegionInfo region,
      List<ServerName> servers) throws HBaseIOException {
    ListMultimap<String,RegionInfo> regionMap = LinkedListMultimap.create();
    ListMultimap<String,ServerName> serverMap = LinkedListMultimap.create();
    generateGroupMaps(Lists.newArrayList(region), servers, regionMap, serverMap);
    List<ServerName> filteredServers = serverMap.get(regionMap.keySet().iterator().next());
    return this.internalBalancer.randomAssignment(region, filteredServers);
  }

  private void generateGroupMaps(
    List<RegionInfo> regions,
    List<ServerName> servers,
    ListMultimap<String, RegionInfo> regionMap,
    ListMultimap<String, ServerName> serverMap) throws HBaseIOException {
    try {
      for (RegionInfo region : regions) {
        String groupName = rsGroupInfoManager.getRSGroupOfTable(region.getTable());
        if (groupName == null) {
          LOG.debug("Group not found for table " + region.getTable() + ", using default");
          groupName = RSGroupInfo.DEFAULT_GROUP;
        }
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
     * if needed. */
    ArrayList<ServerName> finalList = new ArrayList<>();
    for (ServerName onlineServer : onlineServers) {
      if (servers.contains(onlineServer.getAddress())) {
        finalList.add(onlineServer);
      }
    }

    return finalList;
  }

  @VisibleForTesting
  public Set<RegionInfo> getMisplacedRegions(
      Map<RegionInfo, ServerName> regions) throws IOException {
    Set<RegionInfo> misplacedRegions = new HashSet<>();
    for(Map.Entry<RegionInfo, ServerName> region : regions.entrySet()) {
      RegionInfo regionInfo = region.getKey();
      ServerName assignedServer = region.getValue();
      String groupName = rsGroupInfoManager.getRSGroupOfTable(regionInfo.getTable());
      if (groupName == null) {
        LOG.debug("Group not found for table " + regionInfo.getTable() + ", using default");
        groupName = RSGroupInfo.DEFAULT_GROUP;
      }
      RSGroupInfo info = rsGroupInfoManager.getRSGroup(groupName);
      if (assignedServer == null) {
        LOG.debug("There is no assigned server for {}", region);
        continue;
      }
      RSGroupInfo otherInfo = rsGroupInfoManager.getRSGroupOfServer(assignedServer.getAddress());
      if (info == null && otherInfo == null) {
        LOG.warn("Couldn't obtain rs group information for {} on {}", region, assignedServer);
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

  private Pair<Map<ServerName, List<RegionInfo>>, List<RegionPlan>> correctAssignments(
      Map<ServerName, List<RegionInfo>> existingAssignments) throws HBaseIOException{
    // To return
    Map<ServerName, List<RegionInfo>> correctAssignments = new TreeMap<>();
    List<RegionPlan> regionPlansForMisplacedRegions = new ArrayList<>();

    for (Map.Entry<ServerName, List<RegionInfo>> assignments : existingAssignments.entrySet()){
      ServerName currentHostServer = assignments.getKey();
      correctAssignments.put(currentHostServer, new LinkedList<>());
      List<RegionInfo> regions = assignments.getValue();
      for (RegionInfo region : regions) {
        RSGroupInfo targetRSGInfo = null;
        try {
          String groupName = rsGroupInfoManager.getRSGroupOfTable(region.getTable());
          if (groupName == null) {
            LOG.debug("Group not found for table " + region.getTable() + ", using default");
            groupName = RSGroupInfo.DEFAULT_GROUP;
          }
          targetRSGInfo = rsGroupInfoManager.getRSGroup(groupName);
        } catch (IOException exp) {
          LOG.debug("RSGroup information null for region of table " + region.getTable(),
              exp);
        }
        if (targetRSGInfo == null ||
            !targetRSGInfo.containsServer(currentHostServer.getAddress())) { // region is mis-placed
          regionPlansForMisplacedRegions.add(new RegionPlan(region, currentHostServer, null));
        } else { // region is placed as expected
          correctAssignments.get(currentHostServer).add(region);
        }
      }
    }

    // Return correct assignments and region movement plan for mis-placed regions together
    return new Pair<Map<ServerName, List<RegionInfo>>, List<RegionPlan>>(
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
    Class<? extends LoadBalancer> balancerKlass = config.getClass(HBASE_RSGROUP_LOADBALANCER_CLASS,
        StochasticLoadBalancer.class, LoadBalancer.class);
    internalBalancer = ReflectionUtils.newInstance(balancerKlass, config);
    internalBalancer.setMasterServices(masterServices);
    if(clusterStatus != null) {
      internalBalancer.setClusterMetrics(clusterStatus);
    }
    internalBalancer.setConf(config);
    internalBalancer.initialize();
  }

  public boolean isOnline() {
    if (this.rsGroupInfoManager == null) {
      return false;
    }

    return this.rsGroupInfoManager.isOnline();
  }

  @Override
  public void setClusterLoad(Map<TableName, Map<ServerName, List<RegionInfo>>> clusterLoad) {
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

  @Override
  public void postMasterStartupInitialize() {
    this.internalBalancer.postMasterStartupInitialize();
  }

  public void updateBalancerStatus(boolean status) {
    internalBalancer.updateBalancerStatus(status);
  }
}
