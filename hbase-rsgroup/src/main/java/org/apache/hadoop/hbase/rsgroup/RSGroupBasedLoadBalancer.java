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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
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
import org.apache.hadoop.hbase.HConstants;
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
public class RSGroupBasedLoadBalancer implements RSGroupableBalancer {
  private static final Log LOG = LogFactory.getLog(RSGroupBasedLoadBalancer.class);

  private Configuration config;
  private ClusterStatus clusterStatus;
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
  }

  @Override
  public void setClusterStatus(ClusterStatus st) {
    this.clusterStatus = st;
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
      regionPlans.add(new RegionPlan(regionInfo, null, null));
    }
    try {
      List<RSGroupInfo> rsgi = rsGroupInfoManager.listRSGroups();
      for (RSGroupInfo info: rsgi) {
        Map<ServerName, List<HRegionInfo>> groupClusterState = new HashMap<>();
        Map<TableName, Map<ServerName, List<HRegionInfo>>> groupClusterLoad = new HashMap<>();
        for (Address sName : info.getServers()) {
          for(ServerName curr: clusterState.keySet()) {
            if(curr.getAddress().equals(sName)) {
              groupClusterState.put(curr, correctedState.get(curr));
            }
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
    return regionPlans;
  }

  @Override
  public Map<ServerName, List<HRegionInfo>> roundRobinAssignment(
      List<HRegionInfo> regions, List<ServerName> servers) throws HBaseIOException {
    Map<ServerName, List<HRegionInfo>> assignments = Maps.newHashMap();
    ListMultimap<String,HRegionInfo> regionMap = ArrayListMultimap.create();
    ListMultimap<String,ServerName> serverMap = ArrayListMultimap.create();
    generateGroupMaps(regions, servers, regionMap, serverMap);
    for(String groupKey : regionMap.keySet()) {
      if (regionMap.get(groupKey).size() > 0) {
        Map<ServerName, List<HRegionInfo>> result =
            this.internalBalancer.roundRobinAssignment(
                regionMap.get(groupKey),
                serverMap.get(groupKey));
        if(result != null) {
          assignments.putAll(result);
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
      ListMultimap<String, HRegionInfo> groupToRegion = ArrayListMultimap.create();
      Set<HRegionInfo> misplacedRegions = getMisplacedRegions(regions);
      for (HRegionInfo region : regions.keySet()) {
        if (!misplacedRegions.contains(region)) {
          String groupName = rsGroupInfoManager.getRSGroupOfTable(region.getTable());
          groupToRegion.put(groupName, region);
        }
      }
      // Now the "groupToRegion" map has only the regions which have correct
      // assignments.
      for (String key : groupToRegion.keySet()) {
        Map<HRegionInfo, ServerName> currentAssignmentMap = new TreeMap<HRegionInfo, ServerName>();
        List<HRegionInfo> regionList = groupToRegion.get(key);
        RSGroupInfo info = rsGroupInfoManager.getRSGroup(key);
        List<ServerName> candidateList = filterOfflineServers(info, servers);
        for (HRegionInfo region : regionList) {
          currentAssignmentMap.put(region, regions.get(region));
        }
        if(candidateList.size() > 0) {
          assignments.putAll(this.internalBalancer.retainAssignment(
              currentAssignmentMap, candidateList));
        }
      }

      for (HRegionInfo region : misplacedRegions) {
        String groupName = rsGroupInfoManager.getRSGroupOfTable(region.getTable());;
        RSGroupInfo info = rsGroupInfoManager.getRSGroup(groupName);
        List<ServerName> candidateList = filterOfflineServers(info, servers);
        ServerName server = this.internalBalancer.randomAssignment(region,
            candidateList);
        if (server != null) {
          if (!assignments.containsKey(server)) {
            assignments.put(server, new ArrayList<>());
          }
          assignments.get(server).add(region);
        } else {
          //if not server is available assign to bogus so it ends up in RIT
          if(!assignments.containsKey(LoadBalancer.BOGUS_SERVER_NAME)) {
            assignments.put(LoadBalancer.BOGUS_SERVER_NAME, new ArrayList<>());
          }
          assignments.get(LoadBalancer.BOGUS_SERVER_NAME).add(region);
        }
      }
      return assignments;
    } catch (IOException e) {
      throw new HBaseIOException("Failed to do online retain assignment", e);
    }
  }

  @Override
  public ServerName randomAssignment(HRegionInfo region,
      List<ServerName> servers) throws HBaseIOException {
    ListMultimap<String,HRegionInfo> regionMap = LinkedListMultimap.create();
    ListMultimap<String,ServerName> serverMap = LinkedListMultimap.create();
    generateGroupMaps(Lists.newArrayList(region), servers, regionMap, serverMap);
    List<ServerName> filteredServers = serverMap.get(regionMap.keySet().iterator().next());
    return this.internalBalancer.randomAssignment(region, filteredServers);
  }

  private void generateGroupMaps(
    List<HRegionInfo> regions,
    List<ServerName> servers,
    ListMultimap<String, HRegionInfo> regionMap,
    ListMultimap<String, ServerName> serverMap) throws HBaseIOException {
    try {
      for (HRegionInfo region : regions) {
        String groupName = rsGroupInfoManager.getRSGroupOfTable(region.getTable());
        if (groupName == null) {
          LOG.warn("Group for table "+region.getTable()+" is null");
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
      return Collections.EMPTY_LIST;
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
  private List<ServerName> filterServers(Collection<Address> servers,
      Collection<ServerName> onlineServers) {
    ArrayList<ServerName> finalList = new ArrayList<ServerName>();
    for (Address server : servers) {
      for(ServerName curr: onlineServers) {
        if(curr.getAddress().equals(server)) {
          finalList.add(curr);
        }
      }
    }
    return finalList;
  }

  private Set<HRegionInfo> getMisplacedRegions(
      Map<HRegionInfo, ServerName> regions) throws IOException {
    Set<HRegionInfo> misplacedRegions = new HashSet<>();
    for(Map.Entry<HRegionInfo, ServerName> region : regions.entrySet()) {
      HRegionInfo regionInfo = region.getKey();
      ServerName assignedServer = region.getValue();
      RSGroupInfo info = rsGroupInfoManager.getRSGroup(rsGroupInfoManager.
              getRSGroupOfTable(regionInfo.getTable()));
      if (assignedServer != null &&
          (info == null || !info.containsServer(assignedServer.getAddress()))) {
        RSGroupInfo otherInfo = null;
        otherInfo = rsGroupInfoManager.getRSGroupOfServer(assignedServer.getAddress());
        LOG.debug("Found misplaced region: " + regionInfo.getRegionNameAsString() +
            " on server: " + assignedServer +
            " found in group: " +  otherInfo +
            " outside of group: " + (info == null ? "UNKNOWN" : info.getName()));
        misplacedRegions.add(regionInfo);
      }
    }
    return misplacedRegions;
  }

  private Map<ServerName, List<HRegionInfo>> correctAssignments(
       Map<ServerName, List<HRegionInfo>> existingAssignments){
    Map<ServerName, List<HRegionInfo>> correctAssignments = new TreeMap<>();
    List<HRegionInfo> misplacedRegions = new LinkedList<>();
    correctAssignments.put(LoadBalancer.BOGUS_SERVER_NAME, new LinkedList<>());
    for (Map.Entry<ServerName, List<HRegionInfo>> assignments : existingAssignments.entrySet()){
      ServerName sName = assignments.getKey();
      correctAssignments.put(sName, new LinkedList<>());
      List<HRegionInfo> regions = assignments.getValue();
      for (HRegionInfo region : regions) {
        RSGroupInfo info = null;
        try {
          info = rsGroupInfoManager.getRSGroup(
              rsGroupInfoManager.getRSGroupOfTable(region.getTable()));
        } catch (IOException exp) {
          LOG.debug("RSGroup information null for region of table " + region.getTable(),
              exp);
        }
        if ((info == null) || (!info.containsServer(sName.getAddress()))) {
          correctAssignments.get(LoadBalancer.BOGUS_SERVER_NAME).add(region);
        } else {
          correctAssignments.get(sName).add(region);
        }
      }
    }

    //TODO bulk unassign?
    //unassign misplaced regions, so that they are assigned to correct groups.
    for(HRegionInfo info: misplacedRegions) {
      this.masterServices.getAssignmentManager().unassign(info);
    }
    return correctAssignments;
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
      }
    } catch (IOException e) {
      throw new HBaseIOException("Failed to initialize GroupInfoManagerImpl", e);
    }

    // Create the balancer
    Class<? extends LoadBalancer> balancerKlass = config.getClass(HBASE_RSGROUP_LOADBALANCER_CLASS,
        StochasticLoadBalancer.class, LoadBalancer.class);
    internalBalancer = ReflectionUtils.newInstance(balancerKlass, config);
    internalBalancer.setMasterServices(masterServices);
    internalBalancer.setClusterStatus(clusterStatus);
    internalBalancer.setConf(config);
    internalBalancer.initialize();
  }

  public boolean isOnline() {
    if (this.rsGroupInfoManager == null) return false;
    return this.rsGroupInfoManager.isOnline();
  }

  @Override
  public void setClusterLoad(Map<TableName, Map<ServerName, List<HRegionInfo>>> clusterLoad) {
  }

  @Override
  public void regionOnline(HRegionInfo regionInfo, ServerName sn) {
  }

  @Override
  public void regionOffline(HRegionInfo regionInfo) {
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
}
