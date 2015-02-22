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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.balancer.FavoredNodeLoadBalancer;
import org.apache.hadoop.hbase.master.balancer.LoadBalancerFactory;
import org.apache.hadoop.hbase.master.balancer.FavoredNodesPlan.Position;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MasterTests.class, MediumTests.class})
public class TestRegionPlacement2 {
  final static Log LOG = LogFactory.getLog(TestRegionPlacement2.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static int SLAVES = 7;
  private final static int PRIMARY = Position.PRIMARY.ordinal();
  private final static int SECONDARY = Position.SECONDARY.ordinal();
  private final static int TERTIARY = Position.TERTIARY.ordinal();

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    // Enable the favored nodes based load balancer
    conf.setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
        FavoredNodeLoadBalancer.class, LoadBalancer.class);
    conf.setBoolean("hbase.tests.use.shortcircuit.reads", false);
    TEST_UTIL.startMiniCluster(SLAVES);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testFavoredNodesPresentForRoundRobinAssignment() throws HBaseIOException {
    LoadBalancer balancer = LoadBalancerFactory.getLoadBalancer(TEST_UTIL.getConfiguration());
    balancer.setMasterServices(TEST_UTIL.getMiniHBaseCluster().getMaster());
    List<ServerName> servers = new ArrayList<ServerName>();
    for (int i = 0; i < SLAVES; i++) {
      ServerName server = TEST_UTIL.getMiniHBaseCluster().getRegionServer(i).getServerName();
      servers.add(server);
    }
    List<HRegionInfo> regions = new ArrayList<HRegionInfo>(1);
    HRegionInfo region = new HRegionInfo(TableName.valueOf("foobar"));
    regions.add(region);
    Map<ServerName,List<HRegionInfo>> assignmentMap = balancer.roundRobinAssignment(regions,
        servers);
    Set<ServerName> serverBefore = assignmentMap.keySet();
    List<ServerName> favoredNodesBefore =
        ((FavoredNodeLoadBalancer)balancer).getFavoredNodes(region);
    assertTrue(favoredNodesBefore.size() == 3);
    // the primary RS should be the one that the balancer's assignment returns
    assertTrue(ServerName.isSameHostnameAndPort(serverBefore.iterator().next(),
        favoredNodesBefore.get(PRIMARY)));
    // now remove the primary from the list of available servers
    List<ServerName> removedServers = removeMatchingServers(serverBefore, servers);
    // call roundRobinAssignment with the modified servers list
    assignmentMap = balancer.roundRobinAssignment(regions, servers);
    List<ServerName> favoredNodesAfter =
        ((FavoredNodeLoadBalancer)balancer).getFavoredNodes(region);
    assertTrue(favoredNodesAfter.size() == 3);
    // We don't expect the favored nodes assignments to change in multiple calls
    // to the roundRobinAssignment method in the balancer (relevant for AssignmentManager.assign
    // failures)
    assertTrue(favoredNodesAfter.containsAll(favoredNodesBefore));
    Set<ServerName> serverAfter = assignmentMap.keySet();
    // We expect the new RegionServer assignee to be one of the favored nodes
    // chosen earlier.
    assertTrue(ServerName.isSameHostnameAndPort(serverAfter.iterator().next(),
                 favoredNodesBefore.get(SECONDARY)) ||
               ServerName.isSameHostnameAndPort(serverAfter.iterator().next(),
                 favoredNodesBefore.get(TERTIARY)));

    // put back the primary in the list of available servers
    servers.addAll(removedServers);
    // now roundRobinAssignment with the modified servers list should return the primary
    // as the regionserver assignee
    assignmentMap = balancer.roundRobinAssignment(regions, servers);
    Set<ServerName> serverWithPrimary = assignmentMap.keySet();
    assertTrue(serverBefore.containsAll(serverWithPrimary));

    // Make all the favored nodes unavailable for assignment
    removeMatchingServers(favoredNodesAfter, servers);
    // call roundRobinAssignment with the modified servers list
    assignmentMap = balancer.roundRobinAssignment(regions, servers);
    List<ServerName> favoredNodesNow =
        ((FavoredNodeLoadBalancer)balancer).getFavoredNodes(region);
    assertTrue(favoredNodesNow.size() == 3);
    assertTrue(!favoredNodesNow.contains(favoredNodesAfter.get(PRIMARY)) &&
        !favoredNodesNow.contains(favoredNodesAfter.get(SECONDARY)) &&
        !favoredNodesNow.contains(favoredNodesAfter.get(TERTIARY)));
  }

  @Test
  public void testFavoredNodesPresentForRandomAssignment() throws HBaseIOException {
    LoadBalancer balancer = LoadBalancerFactory.getLoadBalancer(TEST_UTIL.getConfiguration());
    balancer.setMasterServices(TEST_UTIL.getMiniHBaseCluster().getMaster());
    List<ServerName> servers = new ArrayList<ServerName>();
    for (int i = 0; i < SLAVES; i++) {
      ServerName server = TEST_UTIL.getMiniHBaseCluster().getRegionServer(i).getServerName();
      servers.add(server);
    }
    List<HRegionInfo> regions = new ArrayList<HRegionInfo>(1);
    HRegionInfo region = new HRegionInfo(TableName.valueOf("foobar"));
    regions.add(region);
    ServerName serverBefore = balancer.randomAssignment(region, servers);
    List<ServerName> favoredNodesBefore =
        ((FavoredNodeLoadBalancer)balancer).getFavoredNodes(region);
    assertTrue(favoredNodesBefore.size() == 3);
    // the primary RS should be the one that the balancer's assignment returns
    assertTrue(ServerName.isSameHostnameAndPort(serverBefore,favoredNodesBefore.get(PRIMARY)));
    // now remove the primary from the list of servers
    removeMatchingServers(serverBefore, servers);
    // call randomAssignment with the modified servers list
    ServerName serverAfter = balancer.randomAssignment(region, servers);
    List<ServerName> favoredNodesAfter =
        ((FavoredNodeLoadBalancer)balancer).getFavoredNodes(region);
    assertTrue(favoredNodesAfter.size() == 3);
    // We don't expect the favored nodes assignments to change in multiple calls
    // to the randomAssignment method in the balancer (relevant for AssignmentManager.assign
    // failures)
    assertTrue(favoredNodesAfter.containsAll(favoredNodesBefore));
    // We expect the new RegionServer assignee to be one of the favored nodes
    // chosen earlier.
    assertTrue(ServerName.isSameHostnameAndPort(serverAfter, favoredNodesBefore.get(SECONDARY)) ||
               ServerName.isSameHostnameAndPort(serverAfter, favoredNodesBefore.get(TERTIARY)));
    // Make all the favored nodes unavailable for assignment
    removeMatchingServers(favoredNodesAfter, servers);
    // call randomAssignment with the modified servers list
    balancer.randomAssignment(region, servers);
    List<ServerName> favoredNodesNow =
        ((FavoredNodeLoadBalancer)balancer).getFavoredNodes(region);
    assertTrue(favoredNodesNow.size() == 3);
    assertTrue(!favoredNodesNow.contains(favoredNodesAfter.get(PRIMARY)) &&
        !favoredNodesNow.contains(favoredNodesAfter.get(SECONDARY)) &&
        !favoredNodesNow.contains(favoredNodesAfter.get(TERTIARY)));
  }

  private List<ServerName> removeMatchingServers(Collection<ServerName> serversWithoutStartCode,
      List<ServerName> servers) {
    List<ServerName> serversToRemove = new ArrayList<ServerName>();
    for (ServerName s : serversWithoutStartCode) {
      serversToRemove.addAll(removeMatchingServers(s, servers));
    }
    return serversToRemove;
  }

  private List<ServerName> removeMatchingServers(ServerName serverWithoutStartCode,
      List<ServerName> servers) {
    List<ServerName> serversToRemove = new ArrayList<ServerName>();
    for (ServerName s : servers) {
      if (ServerName.isSameHostnameAndPort(s, serverWithoutStartCode)) {
        serversToRemove.add(s);
      }
    }
    servers.removeAll(serversToRemove);
    return serversToRemove;
  }
}
