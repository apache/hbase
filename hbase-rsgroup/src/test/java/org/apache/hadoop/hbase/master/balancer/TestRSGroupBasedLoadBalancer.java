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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.rsgroup.RSGroupBasedLoadBalancer;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfo;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ArrayListMultimap;
/**
 * Test RSGroupBasedLoadBalancer with SimpleLoadBalancer as internal balancer
 */
@Category(LargeTests.class)
public class TestRSGroupBasedLoadBalancer extends RSGroupableBalancerTestBase {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRSGroupBasedLoadBalancer.class);
  private static final Logger LOG = LoggerFactory.getLogger(TestRSGroupBasedLoadBalancer.class);
  private static RSGroupBasedLoadBalancer loadBalancer;

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    servers = generateServers(7);
    groupMap = constructGroupInfo(servers, groups);
    tableDescs = constructTableDesc(true);
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.regions.slop", "0");
    conf.set("hbase.rsgroup.grouploadbalancer.class", SimpleLoadBalancer.class.getCanonicalName());
    loadBalancer = new RSGroupBasedLoadBalancer();
    loadBalancer.setRsGroupInfoManager(getMockedGroupInfoManager());
    loadBalancer.setMasterServices(getMockedMaster());
    loadBalancer.setConf(conf);
    loadBalancer.initialize();
  }

  /**
   * Test the load balancing algorithm.
   *
   * Invariant is that all servers of the group should be hosting either floor(average) or
   * ceiling(average)
   */
  @Test
  public void testBalanceCluster() throws Exception {
    // Test with/without per table balancer.
    boolean[] perTableBalancerConfigs = { true, false };
    for (boolean isByTable : perTableBalancerConfigs) {
      Configuration conf = loadBalancer.getConf();
      conf.setBoolean(HConstants.HBASE_MASTER_LOADBALANCE_BYTABLE, isByTable);
      loadBalancer.setConf(conf);
      Map<ServerName, List<RegionInfo>> servers = mockClusterServers();
      ArrayListMultimap<String, ServerAndLoad> list = convertToGroupBasedMap(servers);
      LOG.info("Mock Cluster :  " + printStats(list));
      Map<TableName, Map<ServerName, List<RegionInfo>>> LoadOfAllTable =
          (Map) mockClusterServersWithTables(servers);
      List<RegionPlan> plans = loadBalancer.balanceCluster(LoadOfAllTable);
      ArrayListMultimap<String, ServerAndLoad> balancedCluster = reconcile(list, plans);
      LOG.info("Mock Balance : " + printStats(balancedCluster));
      assertClusterAsBalanced(balancedCluster);
    }
  }

  /**
   * Tests the bulk assignment used during cluster startup.
   *
   * Round-robin. Should yield a balanced cluster so same invariant as the
   * load balancer holds, all servers holding either floor(avg) or
   * ceiling(avg).
   */
  @Test
  public void testBulkAssignment() throws Exception {
    List<RegionInfo> regions = randomRegions(25);
    Map<ServerName, List<RegionInfo>> assignments = loadBalancer
        .roundRobinAssignment(regions, servers);
    //test empty region/servers scenario
    //this should not throw an NPE
    loadBalancer.roundRobinAssignment(regions, Collections.emptyList());
    //test regular scenario
    assertTrue(assignments.keySet().size() == servers.size());
    for (ServerName sn : assignments.keySet()) {
      List<RegionInfo> regionAssigned = assignments.get(sn);
      for (RegionInfo region : regionAssigned) {
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
   * Test the cluster startup bulk assignment which attempts to retain assignment info.
   */
  @Test
  public void testRetainAssignment() throws Exception {
    // Test simple case where all same servers are there
    Map<ServerName, List<RegionInfo>> currentAssignments = mockClusterServers();
    Map<RegionInfo, ServerName> inputForTest = new HashMap<>();
    for (ServerName sn : currentAssignments.keySet()) {
      for (RegionInfo region : currentAssignments.get(sn)) {
        inputForTest.put(region, sn);
      }
    }
    //verify region->null server assignment is handled
    inputForTest.put(randomRegions(1).get(0), null);
    Map<ServerName, List<RegionInfo>> newAssignment = loadBalancer
        .retainAssignment(inputForTest, servers);
    assertRetainedAssignment(inputForTest, servers, newAssignment);
  }

  /**
   * Test BOGUS_SERVER_NAME among groups do not overwrite each other.
   */
  @Test
  public void testRoundRobinAssignment() throws Exception {
    List<ServerName> onlineServers = new ArrayList<ServerName>(servers.size());
    onlineServers.addAll(servers);
    List<RegionInfo> regions = randomRegions(25);
    int bogusRegion = 0;
    for(RegionInfo region : regions){
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
    Map<ServerName, List<RegionInfo>> assignments = loadBalancer
        .roundRobinAssignment(regions, onlineServers);
    assertEquals(bogusRegion, assignments.get(LoadBalancer.BOGUS_SERVER_NAME).size());
  }

  @Test
  public void testOnConfigurationChange() {
    // fallbackEnabled default is false
    assertFalse(loadBalancer.isFallbackEnabled());

    // change FALLBACK_GROUP_ENABLE_KEY from false to true
    Configuration conf = loadBalancer.getConf();
    conf.setBoolean(RSGroupBasedLoadBalancer.FALLBACK_GROUP_ENABLE_KEY, true);
    loadBalancer.onConfigurationChange(conf);
    assertTrue(loadBalancer.isFallbackEnabled());

    // restore
    conf.setBoolean(RSGroupBasedLoadBalancer.FALLBACK_GROUP_ENABLE_KEY, false);
    loadBalancer.onConfigurationChange(conf);
    assertFalse(loadBalancer.isFallbackEnabled());
  }
}
