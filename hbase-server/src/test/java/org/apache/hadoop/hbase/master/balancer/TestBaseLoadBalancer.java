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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RackManager;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer.Cluster;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer.Cluster.MoveRegionAction;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

@Category({MasterTests.class, MediumTests.class})
public class TestBaseLoadBalancer extends BalancerTestBase {

  private static LoadBalancer loadBalancer;
  private static final Log LOG = LogFactory.getLog(TestBaseLoadBalancer.class);
  private static final ServerName master = ServerName.valueOf("fake-master", 0, 1L);
  private static RackManager rackManager;
  private static final int NUM_SERVERS = 15;
  private static ServerName[] servers = new ServerName[NUM_SERVERS];

  int[][] regionsAndServersMocks = new int[][] {
      // { num regions, num servers }
      new int[] { 0, 0 }, new int[] { 0, 1 }, new int[] { 1, 1 }, new int[] { 2, 1 },
      new int[] { 10, 1 }, new int[] { 1, 2 }, new int[] { 2, 2 }, new int[] { 3, 2 },
      new int[] { 1, 3 }, new int[] { 2, 3 }, new int[] { 3, 3 }, new int[] { 25, 3 },
      new int[] { 2, 10 }, new int[] { 2, 100 }, new int[] { 12, 10 }, new int[] { 12, 100 }, };

  // This class is introduced because IP to rack resolution can be lengthy.
  public static class MockMapping implements DNSToSwitchMapping {
    public MockMapping(Configuration conf) {
    }

    private static String RACK = "rack";

    public List<String> resolve(List<String> names) {
      List<String> ret = new ArrayList<String>(names.size());
      for (String name : names) {
        ret.add(RACK);
      }
      return ret;
    }

    // do not add @Override annotations here. It mighty break compilation with earlier Hadoops
    public void reloadCachedMappings() {
    }

    // do not add @Override annotations here. It mighty break compilation with earlier Hadoops
    public void reloadCachedMappings(List<String> arg0) {
    }
  }

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setClass("hbase.util.ip.to.rack.determiner", MockMapping.class, DNSToSwitchMapping.class);
    loadBalancer = new MockBalancer();
    loadBalancer.setConf(conf);
    MasterServices st = Mockito.mock(MasterServices.class);
    Mockito.when(st.getServerName()).thenReturn(master);
    loadBalancer.setMasterServices(st);

    // Set up the rack topologies (5 machines per rack)
    rackManager = Mockito.mock(RackManager.class);
    for (int i = 0; i < NUM_SERVERS; i++) {
      servers[i] = ServerName.valueOf("foo"+i+":1234",-1);
      if (i < 5) {
        Mockito.when(rackManager.getRack(servers[i])).thenReturn("rack1");
      }
      if (i >= 5 && i < 10) {
        Mockito.when(rackManager.getRack(servers[i])).thenReturn("rack2");
      }
      if (i >= 10) {
        Mockito.when(rackManager.getRack(servers[i])).thenReturn("rack3");
      }
    }
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
  @Test (timeout=30000)
  public void testImmediateAssignment() throws Exception {
    List<ServerName> tmp = getListOfServerNames(randomServers(1, 0));
    tmp.add(master);
    ServerName sn = loadBalancer.randomAssignment(HRegionInfo.FIRST_META_REGIONINFO, tmp);
    assertEquals(master, sn);
    HRegionInfo hri = randomRegions(1, -1).get(0);
    sn = loadBalancer.randomAssignment(hri, tmp);
    assertNotEquals(master, sn);
    tmp = new ArrayList<ServerName>();
    tmp.add(master);
    sn = loadBalancer.randomAssignment(hri, tmp);
    assertNull("Should not assign user regions on master", sn);
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
  @Test (timeout=180000)
  public void testBulkAssignment() throws Exception {
    List<ServerName> tmp = getListOfServerNames(randomServers(5, 0));
    List<HRegionInfo> hris = randomRegions(20);
    hris.add(HRegionInfo.FIRST_META_REGIONINFO);
    tmp.add(master);
    Map<ServerName, List<HRegionInfo>> plans = loadBalancer.roundRobinAssignment(hris, tmp);
    assertTrue(plans.get(master).contains(HRegionInfo.FIRST_META_REGIONINFO));
    assertEquals(1, plans.get(master).size());
    int totalRegion = 0;
    for (List<HRegionInfo> regions: plans.values()) {
      totalRegion += regions.size();
    }
    assertEquals(hris.size(), totalRegion);
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
  @Test (timeout=180000)
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

  @Test (timeout=180000)
  public void testRegionAvailability() throws Exception {
    // Create a cluster with a few servers, assign them to specific racks
    // then assign some regions. The tests should check whether moving a
    // replica from one node to a specific other node or rack lowers the
    // availability of the region or not

    List<HRegionInfo> list0 = new ArrayList<HRegionInfo>();
    List<HRegionInfo> list1 = new ArrayList<HRegionInfo>();
    List<HRegionInfo> list2 = new ArrayList<HRegionInfo>();
    // create a region (region1)
    HRegionInfo hri1 = new HRegionInfo(
        TableName.valueOf("table"), "key1".getBytes(), "key2".getBytes(),
        false, 100);
    // create a replica of the region (replica_of_region1)
    HRegionInfo hri2 = RegionReplicaUtil.getRegionInfoForReplica(hri1, 1);
    // create a second region (region2)
    HRegionInfo hri3 = new HRegionInfo(
        TableName.valueOf("table"), "key2".getBytes(), "key3".getBytes(),
        false, 101);
    list0.add(hri1); //only region1
    list1.add(hri2); //only replica_of_region1
    list2.add(hri3); //only region2
    Map<ServerName, List<HRegionInfo>> clusterState =
        new LinkedHashMap<ServerName, List<HRegionInfo>>();
    clusterState.put(servers[0], list0); //servers[0] hosts region1
    clusterState.put(servers[1], list1); //servers[1] hosts replica_of_region1
    clusterState.put(servers[2], list2); //servers[2] hosts region2
    // create a cluster with the above clusterState. The way in which the
    // cluster is created (constructor code) would make sure the indices of
    // the servers are in the order in which it is inserted in the clusterState
    // map (linkedhashmap is important). A similar thing applies to the region lists
    Cluster cluster = new Cluster(clusterState, null, null, rackManager);
    // check whether a move of region1 from servers[0] to servers[1] would lower
    // the availability of region1
    assertTrue(cluster.wouldLowerAvailability(hri1, servers[1]));
    // check whether a move of region1 from servers[0] to servers[2] would lower
    // the availability of region1
    assertTrue(!cluster.wouldLowerAvailability(hri1, servers[2]));
    // check whether a move of replica_of_region1 from servers[0] to servers[2] would lower
    // the availability of replica_of_region1
    assertTrue(!cluster.wouldLowerAvailability(hri2, servers[2]));
    // check whether a move of region2 from servers[0] to servers[1] would lower
    // the availability of region2
    assertTrue(!cluster.wouldLowerAvailability(hri3, servers[1]));

    // now lets have servers[1] host replica_of_region2
    list1.add(RegionReplicaUtil.getRegionInfoForReplica(hri3, 1));
    // create a new clusterState with the above change
    cluster = new Cluster(clusterState, null, null, rackManager);
    // now check whether a move of a replica from servers[0] to servers[1] would lower
    // the availability of region2
    assertTrue(cluster.wouldLowerAvailability(hri3, servers[1]));

    // start over again
    clusterState.clear();
    clusterState.put(servers[0], list0); //servers[0], rack1 hosts region1
    clusterState.put(servers[5], list1); //servers[5], rack2 hosts replica_of_region1 and replica_of_region2
    clusterState.put(servers[6], list2); //servers[6], rack2 hosts region2
    clusterState.put(servers[10], new ArrayList<HRegionInfo>()); //servers[10], rack3 hosts no region
    // create a cluster with the above clusterState
    cluster = new Cluster(clusterState, null, null, rackManager);
    // check whether a move of region1 from servers[0],rack1 to servers[6],rack2 would
    // lower the availability

    assertTrue(cluster.wouldLowerAvailability(hri1, servers[0]));

    // now create a cluster without the rack manager
    cluster = new Cluster(clusterState, null, null, null);
    // now repeat check whether a move of region1 from servers[0] to servers[6] would
    // lower the availability
    assertTrue(!cluster.wouldLowerAvailability(hri1, servers[6]));
  }

  @Test (timeout=180000)
  public void testRegionAvailabilityWithRegionMoves() throws Exception {
    List<HRegionInfo> list0 = new ArrayList<HRegionInfo>();
    List<HRegionInfo> list1 = new ArrayList<HRegionInfo>();
    List<HRegionInfo> list2 = new ArrayList<HRegionInfo>();
    // create a region (region1)
    HRegionInfo hri1 = new HRegionInfo(
        TableName.valueOf("table"), "key1".getBytes(), "key2".getBytes(),
        false, 100);
    // create a replica of the region (replica_of_region1)
    HRegionInfo hri2 = RegionReplicaUtil.getRegionInfoForReplica(hri1, 1);
    // create a second region (region2)
    HRegionInfo hri3 = new HRegionInfo(
        TableName.valueOf("table"), "key2".getBytes(), "key3".getBytes(),
        false, 101);
    list0.add(hri1); //only region1
    list1.add(hri2); //only replica_of_region1
    list2.add(hri3); //only region2
    Map<ServerName, List<HRegionInfo>> clusterState =
        new LinkedHashMap<ServerName, List<HRegionInfo>>();
    clusterState.put(servers[0], list0); //servers[0] hosts region1
    clusterState.put(servers[1], list1); //servers[1] hosts replica_of_region1
    clusterState.put(servers[2], list2); //servers[2] hosts region2
    // create a cluster with the above clusterState. The way in which the
    // cluster is created (constructor code) would make sure the indices of
    // the servers are in the order in which it is inserted in the clusterState
    // map (linkedhashmap is important).
    Cluster cluster = new Cluster(clusterState, null, null, rackManager);
    // check whether moving region1 from servers[1] to servers[2] would lower availability
    assertTrue(!cluster.wouldLowerAvailability(hri1, servers[2]));

    // now move region1 from servers[0] to servers[2]
    cluster.doAction(new MoveRegionAction(0, 0, 2));
    // now repeat check whether moving region1 from servers[1] to servers[2]
    // would lower availability
    assertTrue(cluster.wouldLowerAvailability(hri1, servers[2]));

    // start over again
    clusterState.clear();
    List<HRegionInfo> list3 = new ArrayList<HRegionInfo>();
    HRegionInfo hri4 = RegionReplicaUtil.getRegionInfoForReplica(hri3, 1);
    list3.add(hri4);
    clusterState.put(servers[0], list0); //servers[0], rack1 hosts region1
    clusterState.put(servers[5], list1); //servers[5], rack2 hosts replica_of_region1
    clusterState.put(servers[6], list2); //servers[6], rack2 hosts region2
    clusterState.put(servers[12], list3); //servers[12], rack3 hosts replica_of_region2
    // create a cluster with the above clusterState
    cluster = new Cluster(clusterState, null, null, rackManager);
    // check whether a move of replica_of_region2 from servers[12],rack3 to servers[0],rack1 would
    // lower the availability
    assertTrue(!cluster.wouldLowerAvailability(hri4, servers[0]));
    // now move region2 from servers[6],rack2 to servers[0],rack1
    cluster.doAction(new MoveRegionAction(2, 2, 0));
    // now repeat check if replica_of_region2 from servers[12],rack3 to servers[0],rack1 would
    // lower the availability
    assertTrue(cluster.wouldLowerAvailability(hri3, servers[0]));
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

  @Test (timeout=180000)
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
    BaseLoadBalancer.Cluster cluster = new Cluster(clusterState, null, null, null);
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

  @Test (timeout=180000)
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

    BaseLoadBalancer.Cluster cluster = new Cluster(clusterState, null, locationFinder, null);

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