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
package org.apache.hadoop.hbase.master.balancer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ArrayListMultimap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.rsgroup.RSGroupBasedLoadBalancer;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfo;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test RSGroupBasedLoadBalancer with SimpleLoadBalancer as internal balancer
 */
@Category(SmallTests.class)
public class TestRSGroupBasedLoadBalancer extends RSGroupableBalancerTestBase {
  private static final Log LOG = LogFactory.getLog(TestRSGroupBasedLoadBalancer.class);
  private static RSGroupBasedLoadBalancer loadBalancer;

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    servers = generateServers(7);
    groupMap = constructGroupInfo(servers, groups);
    tableDescs = constructTableDesc(true);
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.regions.slop", "0");
    conf.set("hbase.group.grouploadbalancer.class", SimpleLoadBalancer.class.getCanonicalName());
    loadBalancer = new RSGroupBasedLoadBalancer(getMockedGroupInfoManager());
    loadBalancer.setMasterServices(getMockedMaster());
    loadBalancer.setConf(conf);
    loadBalancer.initialize();
  }

  /**
   * Test the load balancing algorithm.
   * Invariant is that all servers of the group should be hosting either floor(average) or
   * ceiling(average)
   */
  @Test
  public void testBalanceCluster() throws Exception {
    Map<ServerName, List<HRegionInfo>> servers = mockClusterServers();
    ArrayListMultimap<String, ServerAndLoad> list = convertToGroupBasedMap(servers);
    LOG.info("Mock Cluster :  " + printStats(list));
    List<RegionPlan> plans = loadBalancer.balanceCluster(servers);
    ArrayListMultimap<String, ServerAndLoad> balancedCluster = reconcile(
        list, plans);
    LOG.info("Mock Balance : " + printStats(balancedCluster));
    assertClusterAsBalanced(balancedCluster);
  }

  /**
   * Tests the bulk assignment used during cluster startup.
   * Round-robin. Should yield a balanced cluster so same invariant as the
   * load balancer holds, all servers holding either floor(avg) or
   * ceiling(avg).
   */
  @Test
  public void testBulkAssignment() throws Exception {
    List<HRegionInfo> regions = randomRegions(25);
    Map<ServerName, List<HRegionInfo>> assignments = loadBalancer
        .roundRobinAssignment(regions, servers);
    //test empty region/servers scenario
    //this should not throw an NPE
    loadBalancer.roundRobinAssignment(regions, Collections.<ServerName>emptyList());
    //test regular scenario
    assertTrue(assignments.keySet().size() == servers.size());
    for (ServerName sn : assignments.keySet()) {
      List<HRegionInfo> regionAssigned = assignments.get(sn);
      for (HRegionInfo region : regionAssigned) {
        TableName tableName = region.getTable();
        String groupName =
            getMockedGroupInfoManager().getRSGroupOfTable(tableName);
        assertTrue(StringUtils.isNotEmpty(groupName));
        RSGroupInfo gInfo = getMockedGroupInfoManager().getRSGroup(
            groupName);
        assertTrue(
            "Region is not correctly assigned to group servers.",
            gInfo.containsServer(sn.getAddress()));
      }
    }
    ArrayListMultimap<String, ServerAndLoad> loadMap = convertToGroupBasedMap(assignments);
    assertClusterAsBalanced(loadMap);
  }

  /**
   * Test the cluster startup bulk assignment which attempts to retain
   * assignment info.
   */
  @Test
  public void testRetainAssignment() throws Exception {
    // Test simple case where all same servers are there
    Map<ServerName, List<HRegionInfo>> currentAssignments = mockClusterServers();
    Map<HRegionInfo, ServerName> inputForTest = new HashMap<HRegionInfo, ServerName>();
    for (ServerName sn : currentAssignments.keySet()) {
      for (HRegionInfo region : currentAssignments.get(sn)) {
        inputForTest.put(region, sn);
      }
    }
    //verify region->null server assignment is handled
    inputForTest.put(randomRegions(1).get(0), null);
    Map<ServerName, List<HRegionInfo>> newAssignment = loadBalancer
        .retainAssignment(inputForTest, servers);
    assertRetainedAssignment(inputForTest, servers, newAssignment);
  }

  @Test
  public void testGetMisplacedRegions() throws Exception {
    // Test case where region is not considered misplaced if RSGroupInfo cannot be determined
    Map<HRegionInfo, ServerName> inputForTest = new HashMap<>();
    HRegionInfo ri = new HRegionInfo(
        table0, new byte[16], new byte[16], false, regionId++);
    inputForTest.put(ri, servers.iterator().next());
    Set<HRegionInfo> misplacedRegions = loadBalancer.getMisplacedRegions(inputForTest);
    assertFalse(misplacedRegions.contains(ri));
  }

  /**
   * Test BOGUS_SERVER_NAME among groups do not overwrite each other
   */
  @Test
  public void testRoundRobinAssignment() throws Exception {
    List<ServerName> onlineServers = new ArrayList<ServerName>(servers.size());
    onlineServers.addAll(servers);
    List<HRegionInfo> regions = randomRegions(25);
    int bogusRegion = 0;
    for(HRegionInfo region : regions){
      String group = tableMap.get(region.getTable());
      if("dg3".equals(group) || "dg4".equals(group)){
        bogusRegion++;
      }
    }
    Set<Address> offlineServers = new HashSet<Address>();
    offlineServers.addAll(groupMap.get("dg3").getServers());
    offlineServers.addAll(groupMap.get("dg4").getServers());
    for(Iterator<ServerName> it =  onlineServers.iterator(); it.hasNext();){
      ServerName server = it.next();
      Address address = server.getAddress();
      if(offlineServers.contains(address)){
        it.remove();
      }
    }
    Map<ServerName, List<HRegionInfo>> assignments = loadBalancer
        .roundRobinAssignment(regions, onlineServers);
    assertEquals(bogusRegion, assignments.get(LoadBalancer.BOGUS_SERVER_NAME).size());
  }
}
