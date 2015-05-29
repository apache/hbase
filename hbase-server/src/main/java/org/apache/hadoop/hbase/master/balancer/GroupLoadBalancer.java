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
package org.apache.hadoop.hbase.master.balancer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.balancer.GroupLoadBalancerGroupedClusterFactory;
import org.apache.hadoop.hbase.master.RegionPlan;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG) public class GroupLoadBalancer
    extends BaseLoadBalancer {

  private static final Log LOG = LogFactory.getLog(GroupLoadBalancer.class);

  @Override public List<RegionPlan> balanceCluster(Map<ServerName, List<HRegionInfo>> clusterMap) {

    LOG.info("**************** USING GROUP LOAD BALANCER *******************");

    LOG.info("**************** masterServerName " + masterServerName);

    LOG.info("**************** clusterMap " + clusterMap);


    // don't balance master
    if (masterServerName != null && clusterMap.containsKey(masterServerName)) {
      clusterMap = new HashMap<ServerName, List<HRegionInfo>>(clusterMap);
      clusterMap.remove(masterServerName);
    }

    // see if master regions need to be balanced
    List<RegionPlan> regionsToReturn = balanceMasterRegions(clusterMap);
    if (regionsToReturn != null) {
      return regionsToReturn;
    }

    // Move all tables to either first or last server
    ServerName destinationServer1 = masterServerName;
    ServerName destinationServer2 = masterServerName;

    for (Map.Entry<ServerName, List<HRegionInfo>> entry : clusterMap.entrySet()) {
      ServerName serverName = entry.getKey();
      if (serverName.toString().contains("10.255.196.145,60020")) {
        destinationServer1 = serverName;
      } else {
        destinationServer2 = serverName;
      }
    }

    regionsToReturn = new ArrayList<RegionPlan>();

    for (Map.Entry<ServerName, List<HRegionInfo>> entry : clusterMap.entrySet()) {
      ServerName serverName = entry.getKey();
      List<HRegionInfo> hriList = entry.getValue();
      for (HRegionInfo  hri : hriList) {
        ServerName destinationServer = hri.toString().contains("test")?destinationServer1:destinationServer2;
        RegionPlan rp = new RegionPlan(hri, serverName, destinationServer);
        regionsToReturn.add(rp);
      }
    }


    LOG.info("**************** regionsToReturn " + regionsToReturn);

    Cluster cluster = new Cluster(clusterMap, null, this.regionFinder, this.rackManager);
    Configuration configuration = HBaseConfiguration.create();
    GroupLoadBalancerConfiguration groupLoadBalancerConfiguration = new GroupLoadBalancerConfiguration(configuration);
    GroupLoadBalancerGroupedClusterFactory groupLoadBalancerGroupedClusters = new GroupLoadBalancerGroupedClusterFactory(groupLoadBalancerConfiguration, clusterMap);

    LOG.info("**************** groupLoadBalancerGroupedClusters " + groupLoadBalancerGroupedClusters);
    groupLoadBalancerGroupedClusters.getGroupedClusters();

    return regionsToReturn;
  }
}
