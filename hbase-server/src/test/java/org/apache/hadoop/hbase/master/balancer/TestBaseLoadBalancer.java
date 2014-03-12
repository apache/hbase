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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer.Cluster;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

@Category(MediumTests.class)
public class TestBaseLoadBalancer extends BalancerTestBase {

  private static LoadBalancer loadBalancer;
  private static final Log LOG = LogFactory.getLog(TestStochasticLoadBalancer.class);

  int[][] regionsAndServersMocks = new int[][] {
      // { num regions, num servers }
      new int[] { 0, 0 }, new int[] { 0, 1 }, new int[] { 1, 1 }, new int[] { 2, 1 },
      new int[] { 10, 1 }, new int[] { 1, 2 }, new int[] { 2, 2 }, new int[] { 3, 2 },
      new int[] { 1, 3 }, new int[] { 2, 3 }, new int[] { 3, 3 }, new int[] { 25, 3 },
      new int[] { 2, 10 }, new int[] { 2, 100 }, new int[] { 12, 10 }, new int[] { 12, 100 }, };

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    loadBalancer = new MockBalancer();
    loadBalancer.setConf(conf);
  }

  public static class MockBalancer extends BaseLoadBalancer {

    @Override
    public List<RegionPlan> balanceCluster(Map<ServerName, List<HRegionInfo>> clusterState) {
      return null;
    }

  }

  /**
   * Tests immediate assignment.
   *
   * Invariant is that all regions have an assignment.
   *
   * @throws Exception
   */
  @Test
  public void testImmediateAssignment() throws Exception {
    for (int[] mock : regionsAndServersMocks) {
      LOG.debug("testImmediateAssignment with " + mock[0] + " regions and " + mock[1] + " servers");
      List<HRegionInfo> regions = randomRegions(mock[0]);
      List<ServerAndLoad> servers = randomServers(mock[1], 0);
      List<ServerName> list = getListOfServerNames(servers);
      Map<HRegionInfo, ServerName> assignments = loadBalancer.immediateAssignment(regions, list);
      assertImmediateAssignment(regions, list, assignments);
      returnRegions(regions);
      returnServers(list);
    }
  }

  /**
   * All regions have an assignment.
   * @param regions
   * @param servers
   * @param assignments
   */
  private void assertImmediateAssignment(List<HRegionInfo> regions, List<ServerName> servers,
      Map<HRegionInfo, ServerName> assignments) {
    for (HRegionInfo region : regions) {
      assertTrue(assignments.containsKey(region));
    }
  }

  /**
   * Tests the bulk assignment used during cluster startup.
   *
   * Round-robin. Should yield a balanced cluster so same invariant as the load
   * balancer holds, all servers holding either floor(avg) or ceiling(avg).
   *
   * @throws Exception
   */
  @Test
  public void testBulkAssignment() throws Exception {
    for (int[] mock : regionsAndServersMocks) {
      LOG.debug("testBulkAssignment with " + mock[0] + " regions and " + mock[1] + " servers");
      List<HRegionInfo> regions = randomRegions(mock[0]);
      List<ServerAndLoad> servers = randomServers(mock[1], 0);
      List<ServerName> list = getListOfServerNames(servers);
      Map<ServerName, List<HRegionInfo>> assignments =
          loadBalancer.roundRobinAssignment(regions, list);
      float average = (float) regions.size() / servers.size();
      int min = (int) Math.floor(average);
      int max = (int) Math.ceil(average);
      if (assignments != null && !assignments.isEmpty()) {
        for (List<HRegionInfo> regionList : assignments.values()) {
          assertTrue(regionList.size() == min || regionList.size() == max);
        }
      }
      returnRegions(regions);
      returnServers(list);
    }
  }

  /**
   * Test the cluster startup bulk assignment which attempts to retain
   * assignment info.
   * @throws Exception
   */
  @Test
  public void testRetainAssignment() throws Exception {
    // Test simple case where all same servers are there
    List<ServerAndLoad> servers = randomServers(10, 10);
    List<HRegionInfo> regions = randomRegions(100);
    Map<HRegionInfo, ServerName> existing = new TreeMap<HRegionInfo, ServerName>();
    for (int i = 0; i < regions.size(); i++) {
      ServerName sn = servers.get(i % servers.size()).getServerName();
      // The old server would have had same host and port, but different
      // start code!
      ServerName snWithOldStartCode =
          ServerName.valueOf(sn.getHostname(), sn.getPort(), sn.getStartcode() - 10);
      existing.put(regions.get(i), snWithOldStartCode);
    }
    List<ServerName> listOfServerNames = getListOfServerNames(servers);
    Map<ServerName, List<HRegionInfo>> assignment =
        loadBalancer.retainAssignment(existing, listOfServerNames);
    assertRetainedAssignment(existing, listOfServerNames, assignment);

    // Include two new servers that were not there before
    List<ServerAndLoad> servers2 = new ArrayList<ServerAndLoad>(servers);
    servers2.add(randomServer(10));
    servers2.add(randomServer(10));
    listOfServerNames = getListOfServerNames(servers2);
    assignment = loadBalancer.retainAssignment(existing, listOfServerNames);
    assertRetainedAssignment(existing, listOfServerNames, assignment);

    // Remove two of the servers that were previously there
    List<ServerAndLoad> servers3 = new ArrayList<ServerAndLoad>(servers);
    servers3.remove(0);
    servers3.remove(0);
    listOfServerNames = getListOfServerNames(servers3);
    assignment = loadBalancer.retainAssignment(existing, listOfServerNames);
    assertRetainedAssignment(existing, listOfServerNames, assignment);
  }

  private List<ServerName> getListOfServerNames(final List<ServerAndLoad> sals) {
    List<ServerName> list = new ArrayList<ServerName>();
    for (ServerAndLoad e : sals) {
      list.add(e.getServerName());
    }
    return list;
  }

  /**
   * Asserts a valid retained assignment plan.
   * <p>
   * Must meet the following conditions:
   * <ul>
   * <li>Every input region has an assignment, and to an online server
   * <li>If a region had an existing assignment to a server with the same
   * address a a currently online server, it will be assigned to it
   * </ul>
   * @param existing
   * @param servers
   * @param assignment
   */
  private void assertRetainedAssignment(Map<HRegionInfo, ServerName> existing,
      List<ServerName> servers, Map<ServerName, List<HRegionInfo>> assignment) {
    // Verify condition 1, every region assigned, and to online server
    Set<ServerName> onlineServerSet = new TreeSet<ServerName>(servers);
    Set<HRegionInfo> assignedRegions = new TreeSet<HRegionInfo>();
    for (Map.Entry<ServerName, List<HRegionInfo>> a : assignment.entrySet()) {
      assertTrue("Region assigned to server that was not listed as online",
        onlineServerSet.contains(a.getKey()));
      for (HRegionInfo r : a.getValue())
        assignedRegions.add(r);
    }
    assertEquals(existing.size(), assignedRegions.size());

    // Verify condition 2, if server had existing assignment, must have same
    Set<String> onlineHostNames = new TreeSet<String>();
    for (ServerName s : servers) {
      onlineHostNames.add(s.getHostname());
    }

    for (Map.Entry<ServerName, List<HRegionInfo>> a : assignment.entrySet()) {
      ServerName assignedTo = a.getKey();
      for (HRegionInfo r : a.getValue()) {
        ServerName address = existing.get(r);
        if (address != null && onlineHostNames.contains(address.getHostname())) {
          // this region was prevously assigned somewhere, and that
          // host is still around, then it should be re-assigned on the
          // same host
          assertEquals(address.getHostname(), assignedTo.getHostname());
        }
      }
    }
  }

  @Test
  public void testClusterServersWithSameHostPort() {
    // tests whether the BaseLoadBalancer.Cluster can be constructed with servers
    // sharing same host and port
    List<ServerName> servers = getListOfServerNames(randomServers(10, 10));
    List<HRegionInfo> regions = randomRegions(101);
    Map<ServerName, List<HRegionInfo>> clusterState = new HashMap<ServerName, List<HRegionInfo>>();

    assignRegions(regions, servers, clusterState);

    // construct another list of servers, but sharing same hosts and ports
    List<ServerName> oldServers = new ArrayList<ServerName>(servers.size());
    for (ServerName sn : servers) {
      // The old server would have had same host and port, but different start code!
      oldServers.add(ServerName.valueOf(sn.getHostname(), sn.getPort(), sn.getStartcode() - 10));
    }

    regions = randomRegions(9); // some more regions
    assignRegions(regions, oldServers, clusterState);

    // should not throw exception:
    BaseLoadBalancer.Cluster cluster = new Cluster(clusterState, null, null);
    assertEquals(101 + 9, cluster.numRegions);
    assertEquals(10, cluster.numServers); // only 10 servers because they share the same host + port
  }

  private void assignRegions(List<HRegionInfo> regions, List<ServerName> servers,
      Map<ServerName, List<HRegionInfo>> clusterState) {
    for (int i = 0; i < regions.size(); i++) {
      ServerName sn = servers.get(i % servers.size());
      List<HRegionInfo> regionsOfServer = clusterState.get(sn);
      if (regionsOfServer == null) {
        regionsOfServer = new ArrayList<HRegionInfo>(10);
        clusterState.put(sn, regionsOfServer);
      }

      regionsOfServer.add(regions.get(i));
    }
  }

  @Test
  public void testClusterRegionLocations() {
    // tests whether region locations are handled correctly in Cluster
    List<ServerName> servers = getListOfServerNames(randomServers(10, 10));
    List<HRegionInfo> regions = randomRegions(101);
    Map<ServerName, List<HRegionInfo>> clusterState = new HashMap<ServerName, List<HRegionInfo>>();

    assignRegions(regions, servers, clusterState);

    // mock block locality for some regions
    RegionLocationFinder locationFinder = mock(RegionLocationFinder.class);
    // block locality: region:0   => {server:0}
    //                 region:1   => {server:0, server:1}
    //                 region:42 => {server:4, server:9, server:5}
    when(locationFinder.getTopBlockLocations(regions.get(0))).thenReturn(
      Lists.newArrayList(servers.get(0)));
    when(locationFinder.getTopBlockLocations(regions.get(1))).thenReturn(
      Lists.newArrayList(servers.get(0), servers.get(1)));
    when(locationFinder.getTopBlockLocations(regions.get(42))).thenReturn(
      Lists.newArrayList(servers.get(4), servers.get(9), servers.get(5)));
    when(locationFinder.getTopBlockLocations(regions.get(43))).thenReturn(
      Lists.newArrayList(ServerName.valueOf("foo", 0, 0))); // this server does not exists in clusterStatus

    BaseLoadBalancer.Cluster cluster = new Cluster(clusterState, null, locationFinder);

    int r0 = ArrayUtils.indexOf(cluster.regions, regions.get(0)); // this is ok, it is just a test
    int r1 = ArrayUtils.indexOf(cluster.regions, regions.get(1));
    int r10 = ArrayUtils.indexOf(cluster.regions, regions.get(10));
    int r42 = ArrayUtils.indexOf(cluster.regions, regions.get(42));
    int r43 = ArrayUtils.indexOf(cluster.regions, regions.get(43));

    int s0 = cluster.serversToIndex.get(servers.get(0).getHostAndPort());
    int s1 = cluster.serversToIndex.get(servers.get(1).getHostAndPort());
    int s4 = cluster.serversToIndex.get(servers.get(4).getHostAndPort());
    int s5 = cluster.serversToIndex.get(servers.get(5).getHostAndPort());
    int s9 = cluster.serversToIndex.get(servers.get(9).getHostAndPort());

    // region 0 locations
    assertEquals(1, cluster.regionLocations[r0].length);
    assertEquals(s0, cluster.regionLocations[r0][0]);

    // region 1 locations
    assertEquals(2, cluster.regionLocations[r1].length);
    assertEquals(s0, cluster.regionLocations[r1][0]);
    assertEquals(s1, cluster.regionLocations[r1][1]);

    // region 10 locations
    assertEquals(0, cluster.regionLocations[r10].length);

    // region 42 locations
    assertEquals(3, cluster.regionLocations[r42].length);
    assertEquals(s4, cluster.regionLocations[r42][0]);
    assertEquals(s9, cluster.regionLocations[r42][1]);
    assertEquals(s5, cluster.regionLocations[r42][2]);

    // region 43 locations
    assertEquals(1, cluster.regionLocations[r43].length);
    assertEquals(-1, cluster.regionLocations[r43][0]);
  }

}
