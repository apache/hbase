/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.master.balancer.grouploadbalancer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer;
import org.apache.hadoop.hbase.master.balancer.SimpleLoadBalancer;


/**
 * This load balancer partitions servers and tables into groups. Then, within each group, it uses
 * another load balancer to balance within each group.
 *
 * The configuration for the groups is set within the hbase-site.xml file. Within hbase-site.xml:
 *
 * "hbase.master.loadbalancer.class" needs to be set to
 * "org.apache.hadoop.hbase.master.balancer.grouploadbalancer.GroupLoadBalancer" for this load
 * balancer to work.
 *
 * "hbase.master.balancer.grouploadbalancer.groups" configures the names of the groups, separated
 * with a ";" eg. "group1;group2" creates two groups, named group1 and group2.
 *
 * "hbase.master.balancer.grouploadbalancer.defaultgroup" configures the name of the default group.
 * Note that the defaultgroup must be a pre-existing group defined in
 * "hbase.master.balancer.grouploadbalancer.groups". eg. "group1" sets group1 to be the default
 * group.
 *
 * To put servers in groups, you need to create a property named
 * "hbase.master.balancer.grouploadbalancer.servergroups." + groupName, and set it's value to the
 * IP address and port of the server in a comma separated list.
 * Note that the IP address and the port over the server is separated by a ",", not a ":".
 * eg. "hbase.master.balancer.grouploadbalancer.servergroups.group1" with a value
 * "10.255.196.145,60020;10.255.196.145,60021" will put two servers in group1.
 *
 * To put tables in groups, you need to create a property named
 * "hbase.master.balancer.grouploadbalancer.tablegroups." + groupName, and set it's value to the
 * name of the table in a comma separated list. You must specify it's namespace in here also.
 * eg. "hbase.master.balancer.grouploadbalancer.tablegroups.group1" with a value
 * "my_ns:namespace_table;test" will put two servers in group1. Note that "namespace_table" is under
 * the namespace "my_ns".
*/
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class GroupLoadBalancer extends BaseLoadBalancer {

  private static final Log LOG = LogFactory.getLog(GroupLoadBalancer.class);

  private Configuration configuration;

  @Override
  public void onConfigurationChange(Configuration conf) {
    setConf(conf);
  }

  @Override
  public synchronized void setConf(Configuration conf) {
    super.setConf(conf);
    this.configuration = conf;
  }

  @Override
  public List<RegionPlan> balanceCluster(Map<ServerName, List<HRegionInfo>> clusterMap) {

    LOG.info("**************** USING GROUP LOAD BALANCER *******************");

    // don't balance master
    if (masterServerName != null && clusterMap.containsKey(masterServerName)) {
      clusterMap = new HashMap<>(clusterMap);
      clusterMap.remove(masterServerName);
    }

    // see if master regions need to be balanced
    List<RegionPlan> regionsToReturn = balanceMasterRegions(clusterMap);
    if (regionsToReturn != null) {
      return regionsToReturn;
    }

    GroupLoadBalancerConfiguration groupLoadBalancerConfiguration =
        new GroupLoadBalancerConfiguration(this.configuration, clusterMap);

    GroupLoadBalancerGroupedClusterFactory groupLoadBalancerGroupedClusters =
        new GroupLoadBalancerGroupedClusterFactory(groupLoadBalancerConfiguration, clusterMap);
    Map<String, Map<ServerName, List<HRegionInfo>>> groupedClusterMap =
        groupLoadBalancerGroupedClusters.getGroupedClusters();

    regionsToReturn = new ArrayList<>();

    List<RegionPlan> regionsToReconcile =
        reconcileToGroupConfiguration(groupedClusterMap, clusterMap);

    if (regionsToReconcile != null) {
      regionsToReturn.addAll(regionsToReconcile);
    }

    // Use another load balancer to do the actual balancing
    for (Map<ServerName, List<HRegionInfo>> cluster : groupedClusterMap.values()) {
      SimpleLoadBalancer simpleLoadBalancer = new SimpleLoadBalancer();
      List<RegionPlan> regionsToReturnForGroup = simpleLoadBalancer.balanceCluster(cluster);
      if (regionsToReturnForGroup != null) {
        regionsToReturn.addAll(simpleLoadBalancer.balanceCluster(cluster));
      }
    }

    return regionsToReturn;
  }

  /**
   * If there is a mismatch between the groupedClusterMap and clusterMap regarding which tables
   * belongs on which servers, then create a list of region plans so that the tables are moved to
   * the servers which are specified by groupedClusterMap.
   *
   * @param groupedClusterMap a clusterMap which as been partitioned by groups
   * @param clusterMap a mapping of servers and a list of regions which they hold
   * @return a list of region plans which move regions around so that the regions which are placed
   * on the servers in clusterMap are the same as what is specified in groupedClusterMap
   */
  public List<RegionPlan> reconcileToGroupConfiguration(
      Map<String, Map<ServerName, List<HRegionInfo>>> groupedClusterMap,
      Map<ServerName, List<HRegionInfo>> clusterMap) {

    Map<HRegionInfo, ServerName> hriToServerNameMap = new HashMap<>();
    for (Map<ServerName, List<HRegionInfo>> clusterMapGroup : groupedClusterMap.values()) {
      for (ServerName serverName : clusterMapGroup.keySet()) {
        for (HRegionInfo hri : clusterMapGroup.get(serverName)) {
          hriToServerNameMap.put(hri, serverName);
        }
      }
    }

    List<RegionPlan> regionsToReconcile = new ArrayList<>();
    for (ServerName serverName : clusterMap.keySet()) {
      for (HRegionInfo hri : clusterMap.get(serverName)) {
        if (serverName != hriToServerNameMap.get(hri)) {
          RegionPlan regionPlan = new RegionPlan(hri, serverName, hriToServerNameMap.get(hri));
          regionsToReconcile.add(regionPlan);
        }
      }
    }

    return regionsToReconcile;

  }
}
