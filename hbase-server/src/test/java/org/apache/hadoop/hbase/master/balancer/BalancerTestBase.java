/*
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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.master.RackManager;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class used to be the base of unit tests on load balancers. It gives helper
 * methods to create maps of {@link ServerName} to lists of {@link RegionInfo}
 * and to check list of region plans.
 *
 */
public class BalancerTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(BalancerTestBase.class);
  static int regionId = 0;
  protected static Configuration conf;
  protected static StochasticLoadBalancer loadBalancer;

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    conf = HBaseConfiguration.create();
    conf.setClass("hbase.util.ip.to.rack.determiner", MockMapping.class, DNSToSwitchMapping.class);
    conf.setFloat("hbase.regions.slop", 0.0f);
    conf.setFloat("hbase.master.balancer.stochastic.localityCost", 0);
    loadBalancer = new StochasticLoadBalancer();
    loadBalancer.setConf(conf);
  }

  protected int[] largeCluster = new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 56 };

  // int[testnum][servernumber] -> numregions
  protected int[][] clusterStateMocks = new int[][]{
      // 1 node
      new int[]{0},
      new int[]{1},
      new int[]{10},
      // 2 node
      new int[]{0, 0},
      new int[]{2, 0},
      new int[]{2, 1},
      new int[]{2, 2},
      new int[]{2, 3},
      new int[]{2, 4},
      new int[]{1, 1},
      new int[]{0, 1},
      new int[]{10, 1},
      new int[]{514, 1432},
      new int[]{48, 53},
      // 3 node
      new int[]{0, 1, 2},
      new int[]{1, 2, 3},
      new int[]{0, 2, 2},
      new int[]{0, 3, 0},
      new int[]{0, 4, 0},
      new int[]{20, 20, 0},
      // 4 node
      new int[]{0, 1, 2, 3},
      new int[]{4, 0, 0, 0},
      new int[]{5, 0, 0, 0},
      new int[]{6, 6, 0, 0},
      new int[]{6, 2, 0, 0},
      new int[]{6, 1, 0, 0},
      new int[]{6, 0, 0, 0},
      new int[]{4, 4, 4, 7},
      new int[]{4, 4, 4, 8},
      new int[]{0, 0, 0, 7},
      // 5 node
      new int[]{1, 1, 1, 1, 4},
      // 6 nodes
      new int[]{1500, 500, 500, 500, 10, 0},
      new int[]{1500, 500, 500, 500, 500, 0},
      // more nodes
      new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
      new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 10},
      new int[]{6, 6, 5, 6, 6, 6, 6, 6, 6, 1},
      new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 54},
      new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 55},
      new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 56},
      new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 16},
      new int[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 8},
      new int[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 9},
      new int[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 10},
      new int[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 123},
      new int[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 155},
      new int[]{10, 7, 12, 8, 11, 10, 9, 14},
      new int[]{13, 14, 6, 10, 10, 10, 8, 10},
      new int[]{130, 14, 60, 10, 100, 10, 80, 10},
      new int[]{130, 140, 60, 100, 100, 100, 80, 100},
      new int[]{0, 5 , 5, 5, 5},
      largeCluster,

  };


  // This class is introduced because IP to rack resolution can be lengthy.
  public static class MockMapping implements DNSToSwitchMapping {
    public MockMapping(Configuration conf) {
    }

    @Override
    public List<String> resolve(List<String> names) {
      return Stream.generate(() -> "rack").limit(names.size()).collect(Collectors.toList());
    }

    // do not add @Override annotations here. It mighty break compilation with earlier Hadoops
    public void reloadCachedMappings() {
    }

    // do not add @Override annotations here. It mighty break compilation with earlier Hadoops
    public void reloadCachedMappings(List<String> arg0) {
    }
  }

  /**
   * Invariant is that all servers have between floor(avg) and ceiling(avg)
   * number of regions.
   */
  public void assertClusterAsBalanced(List<ServerAndLoad> servers) {
    int numServers = servers.size();
    int numRegions = 0;
    int maxRegions = 0;
    int minRegions = Integer.MAX_VALUE;
    for (ServerAndLoad server : servers) {
      int nr = server.getLoad();
      if (nr > maxRegions) {
        maxRegions = nr;
      }
      if (nr < minRegions) {
        minRegions = nr;
      }
      numRegions += nr;
    }
    if (maxRegions - minRegions < 2) {
      // less than 2 between max and min, can't balance
      return;
    }
    int min = numRegions / numServers;
    int max = numRegions % numServers == 0 ? min : min + 1;

    for (ServerAndLoad server : servers) {
      assertTrue("All servers should have a positive load. " + server, server.getLoad() >= 0);
      assertTrue("All servers should have load no more than " + max + ". " + server,
          server.getLoad() <= max);
      assertTrue("All servers should have load no less than " + min + ". " + server,
          server.getLoad() >= min);
    }
  }

  /**
   * Invariant is that all servers have between acceptable range
   * number of regions.
   */
  public boolean assertClusterOverallAsBalanced(List<ServerAndLoad> servers, int tablenum) {
    int numServers = servers.size();
    int numRegions = 0;
    int maxRegions = 0;
    int minRegions = Integer.MAX_VALUE;
    for (ServerAndLoad server : servers) {
      int nr = server.getLoad();
      if (nr > maxRegions) {
        maxRegions = nr;
      }
      if (nr < minRegions) {
        minRegions = nr;
      }
      numRegions += nr;
    }
    if (maxRegions - minRegions < 2) {
      // less than 2 between max and min, can't balance
      return true;
    }
    int min = numRegions / numServers;
    int max = numRegions % numServers == 0 ? min : min + 1;

    for (ServerAndLoad server : servers) {
      // The '5' in below is arbitrary.
      if (server.getLoad() < 0 || server.getLoad() > max + (tablenum/2 + 5)  ||
          server.getLoad() < (min - tablenum/2 - 5)) {
        LOG.warn("server={}, load={}, max={}, tablenum={}, min={}",
          server.getServerName(), server.getLoad(), max, tablenum, min);
        return false;
      }
    }
    return true;
  }

  /**
   * Checks whether region replicas are not hosted on the same host.
   */
  public void assertRegionReplicaPlacement(Map<ServerName, List<RegionInfo>> serverMap, RackManager rackManager) {
    TreeMap<String, Set<RegionInfo>> regionsPerHost = new TreeMap<>();
    TreeMap<String, Set<RegionInfo>> regionsPerRack = new TreeMap<>();

    for (Entry<ServerName, List<RegionInfo>> entry : serverMap.entrySet()) {
      String hostname = entry.getKey().getHostname();
      Set<RegionInfo> infos = regionsPerHost.get(hostname);
      if (infos == null) {
        infos = new HashSet<>();
        regionsPerHost.put(hostname, infos);
      }

      for (RegionInfo info : entry.getValue()) {
        RegionInfo primaryInfo = RegionReplicaUtil.getRegionInfoForDefaultReplica(info);
        if (!infos.add(primaryInfo)) {
          Assert.fail("Two or more region replicas are hosted on the same host after balance");
        }
      }
    }

    if (rackManager == null) {
      return;
    }

    for (Entry<ServerName, List<RegionInfo>> entry : serverMap.entrySet()) {
      String rack = rackManager.getRack(entry.getKey());
      Set<RegionInfo> infos = regionsPerRack.get(rack);
      if (infos == null) {
        infos = new HashSet<>();
        regionsPerRack.put(rack, infos);
      }

      for (RegionInfo info : entry.getValue()) {
        RegionInfo primaryInfo = RegionReplicaUtil.getRegionInfoForDefaultReplica(info);
        if (!infos.add(primaryInfo)) {
          Assert.fail("Two or more region replicas are hosted on the same rack after balance");
        }
      }
    }
  }

  protected String printStats(List<ServerAndLoad> servers) {
    int numServers = servers.size();
    int totalRegions = 0;
    for (ServerAndLoad server : servers) {
      totalRegions += server.getLoad();
    }
    float average = (float) totalRegions / numServers;
    int max = (int) Math.ceil(average);
    int min = (int) Math.floor(average);
    return "[srvr=" + numServers + " rgns=" + totalRegions + " avg=" + average + " max=" + max
        + " min=" + min + "]";
  }

  protected List<ServerAndLoad> convertToList(final Map<ServerName, List<RegionInfo>> servers) {
    List<ServerAndLoad> list = new ArrayList<>(servers.size());
    for (Map.Entry<ServerName, List<RegionInfo>> e : servers.entrySet()) {
      list.add(new ServerAndLoad(e.getKey(), e.getValue().size()));
    }
    return list;
  }

  protected String printMock(List<ServerAndLoad> balancedCluster) {
    SortedSet<ServerAndLoad> sorted = new TreeSet<>(balancedCluster);
    ServerAndLoad[] arr = sorted.toArray(new ServerAndLoad[sorted.size()]);
    StringBuilder sb = new StringBuilder(sorted.size() * 4 + 4);
    sb.append("{ ");
    for (int i = 0; i < arr.length; i++) {
      if (i != 0) {
        sb.append(" , ");
      }
      sb.append(arr[i].getServerName().getHostname());
      sb.append(":");
      sb.append(arr[i].getLoad());
    }
    sb.append(" }");
    return sb.toString();
  }

  /**
   * This assumes the RegionPlan HSI instances are the same ones in the map, so
   * actually no need to even pass in the map, but I think it's clearer.
   *
   * @param list
   * @param plans
   * @return a list of all added {@link ServerAndLoad} values.
   */
  protected List<ServerAndLoad> reconcile(List<ServerAndLoad> list,
                                          List<RegionPlan> plans,
                                          Map<ServerName, List<RegionInfo>> servers) {
    List<ServerAndLoad> result = new ArrayList<>(list.size());

    Map<ServerName, ServerAndLoad> map = new HashMap<>(list.size());
    for (ServerAndLoad sl : list) {
      map.put(sl.getServerName(), sl);
    }
    if (plans != null) {
      for (RegionPlan plan : plans) {
        ServerName source = plan.getSource();

        updateLoad(map, source, -1);
        ServerName destination = plan.getDestination();
        updateLoad(map, destination, +1);

        servers.get(source).remove(plan.getRegionInfo());
        servers.get(destination).add(plan.getRegionInfo());
      }
    }
    result.clear();
    result.addAll(map.values());
    return result;
  }

  protected void updateLoad(final Map<ServerName, ServerAndLoad> map,
                            final ServerName sn,
                            final int diff) {
    ServerAndLoad sal = map.get(sn);
    if (sal == null) sal = new ServerAndLoad(sn, 0);
    sal = new ServerAndLoad(sn, sal.getLoad() + diff);
    map.put(sn, sal);
  }

  protected TreeMap<ServerName, List<RegionInfo>> mockClusterServers(int[] mockCluster) {
    return mockClusterServers(mockCluster, -1);
  }

  protected BaseLoadBalancer.Cluster mockCluster(int[] mockCluster) {
    return new BaseLoadBalancer.Cluster(
      mockClusterServers(mockCluster, -1), null, null, null);
  }

  protected TreeMap<ServerName, List<RegionInfo>> mockClusterServers(int[] mockCluster, int numTables) {
    int numServers = mockCluster.length;
    TreeMap<ServerName, List<RegionInfo>> servers = new TreeMap<>();
    for (int i = 0; i < numServers; i++) {
      int numRegions = mockCluster[i];
      ServerAndLoad sal = randomServer(0);
      List<RegionInfo> regions = randomRegions(numRegions, numTables);
      servers.put(sal.getServerName(), regions);
    }
    return servers;
  }

  protected TreeMap<ServerName, List<RegionInfo>> mockUniformClusterServers(int[] mockCluster) {
    int numServers = mockCluster.length;
    TreeMap<ServerName, List<RegionInfo>> servers = new TreeMap<>();
    for (int i = 0; i < numServers; i++) {
      int numRegions = mockCluster[i];
      ServerAndLoad sal = randomServer(0);
      List<RegionInfo> regions = uniformRegions(numRegions);
      servers.put(sal.getServerName(), regions);
    }
    return servers;
  }

  protected HashMap<TableName, TreeMap<ServerName, List<RegionInfo>>> mockClusterServersWithTables(Map<ServerName, List<RegionInfo>> clusterServers) {
    HashMap<TableName, TreeMap<ServerName, List<RegionInfo>>> result = new HashMap<>();
    for (Map.Entry<ServerName, List<RegionInfo>> entry : clusterServers.entrySet()) {
      ServerName sal = entry.getKey();
      List<RegionInfo> regions = entry.getValue();
      for (RegionInfo hri : regions){
        TreeMap<ServerName, List<RegionInfo>> servers = result.get(hri.getTable());
        if (servers == null) {
          servers = new TreeMap<>();
          result.put(hri.getTable(), servers);
        }
        List<RegionInfo> hrilist = servers.get(sal);
        if (hrilist == null) {
          hrilist = new ArrayList<>();
          servers.put(sal, hrilist);
        }
        hrilist.add(hri);
      }
    }
    for(Map.Entry<TableName, TreeMap<ServerName, List<RegionInfo>>> entry : result.entrySet()){
      for(ServerName srn : clusterServers.keySet()){
        if (!entry.getValue().containsKey(srn)) entry.getValue().put(srn, new ArrayList<>());
      }
    }
    return result;
  }

  private Queue<RegionInfo> regionQueue = new LinkedList<>();

  protected List<RegionInfo> randomRegions(int numRegions) {
    return randomRegions(numRegions, -1);
  }

  protected List<RegionInfo> createRegions(int numRegions, TableName tableName) {
    List<RegionInfo> regions = new ArrayList<>(numRegions);
    byte[] start = new byte[16];
    Bytes.random(start);
    byte[] end = new byte[16];
    Bytes.random(end);
    for (int i = 0; i < numRegions; i++) {
      Bytes.putInt(start, 0, numRegions << 1);
      Bytes.putInt(end, 0, (numRegions << 1) + 1);
      RegionInfo hri = RegionInfoBuilder.newBuilder(tableName)
        .setStartKey(start)
        .setEndKey(end)
        .setSplit(false)
        .build();
      regions.add(hri);
    }
    return regions;
  }

  protected List<RegionInfo> randomRegions(int numRegions, int numTables) {
    List<RegionInfo> regions = new ArrayList<>(numRegions);
    byte[] start = new byte[16];
    Bytes.random(start);
    byte[] end = new byte[16];
    Bytes.random(end);
    for (int i = 0; i < numRegions; i++) {
      if (!regionQueue.isEmpty()) {
        regions.add(regionQueue.poll());
        continue;
      }
      Bytes.putInt(start, 0, numRegions << 1);
      Bytes.putInt(end, 0, (numRegions << 1) + 1);
      TableName tableName = TableName.valueOf("table" +
        (numTables > 0 ? ThreadLocalRandom.current().nextInt(numTables) : i));
      RegionInfo hri = RegionInfoBuilder.newBuilder(tableName)
          .setStartKey(start)
          .setEndKey(end)
          .setSplit(false)
          .setRegionId(regionId++)
          .build();
      regions.add(hri);
    }
    return regions;
  }

  protected List<RegionInfo> uniformRegions(int numRegions) {
    List<RegionInfo> regions = new ArrayList<>(numRegions);
    byte[] start = new byte[16];
    Bytes.random(start);
    byte[] end = new byte[16];
    Bytes.random(end);
    for (int i = 0; i < numRegions; i++) {
      Bytes.putInt(start, 0, numRegions << 1);
      Bytes.putInt(end, 0, (numRegions << 1) + 1);
      TableName tableName = TableName.valueOf("table" + i);
      RegionInfo hri = RegionInfoBuilder.newBuilder(tableName)
          .setStartKey(start)
          .setEndKey(end)
          .setSplit(false)
          .build();
      regions.add(hri);
    }
    return regions;
  }

  protected void returnRegions(List<RegionInfo> regions) {
    regionQueue.addAll(regions);
  }

  private Queue<ServerName> serverQueue = new LinkedList<>();

  protected ServerAndLoad randomServer(final int numRegionsPerServer) {
    if (!this.serverQueue.isEmpty()) {
      ServerName sn = this.serverQueue.poll();
      return new ServerAndLoad(sn, numRegionsPerServer);
    }
    Random rand = ThreadLocalRandom.current();
    String host = "srv" + rand.nextInt(Integer.MAX_VALUE);
    int port = rand.nextInt(60000);
    long startCode = rand.nextLong();
    ServerName sn = ServerName.valueOf(host, port, startCode);
    return new ServerAndLoad(sn, numRegionsPerServer);
  }

  protected List<ServerAndLoad> randomServers(int numServers, int numRegionsPerServer) {
    List<ServerAndLoad> servers = new ArrayList<>(numServers);
    for (int i = 0; i < numServers; i++) {
      servers.add(randomServer(numRegionsPerServer));
    }
    return servers;
  }

  protected void returnServer(ServerName server) {
    serverQueue.add(server);
  }

  protected void returnServers(List<ServerName> servers) {
    this.serverQueue.addAll(servers);
  }

  protected void testWithCluster(int numNodes,
      int numRegions,
      int numRegionsPerServer,
      int replication,
      int numTables,
      boolean assertFullyBalanced, boolean assertFullyBalancedForReplicas) {
    Map<ServerName, List<RegionInfo>> serverMap =
        createServerMap(numNodes, numRegions, numRegionsPerServer, replication, numTables);
    testWithCluster(serverMap, null, assertFullyBalanced, assertFullyBalancedForReplicas);
  }

  protected void testWithClusterWithIteration(int numNodes, int numRegions, int numRegionsPerServer,
    int replication, int numTables, boolean assertFullyBalanced,
    boolean assertFullyBalancedForReplicas) {
    Map<ServerName, List<RegionInfo>> serverMap =
      createServerMap(numNodes, numRegions, numRegionsPerServer, replication, numTables);
    testWithClusterWithIteration(serverMap, null, assertFullyBalanced,
      assertFullyBalancedForReplicas);
  }

  protected void testWithCluster(Map<ServerName, List<RegionInfo>> serverMap,
      RackManager rackManager, boolean assertFullyBalanced, boolean assertFullyBalancedForReplicas) {
    List<ServerAndLoad> list = convertToList(serverMap);
    LOG.info("Mock Cluster : " + printMock(list) + " " + printStats(list));

    loadBalancer.setRackManager(rackManager);
    // Run the balancer.
    Map<TableName, Map<ServerName, List<RegionInfo>>> LoadOfAllTable =
        (Map) mockClusterServersWithTables(serverMap);
    List<RegionPlan> plans = loadBalancer.balanceCluster(LoadOfAllTable);
    assertNotNull("Initial cluster balance should produce plans.", plans);

    // Check to see that this actually got to a stable place.
    if (assertFullyBalanced || assertFullyBalancedForReplicas) {
      // Apply the plan to the mock cluster.
      List<ServerAndLoad> balancedCluster = reconcile(list, plans, serverMap);

      // Print out the cluster loads to make debugging easier.
      LOG.info("Mock after Balance : " + printMock(balancedCluster));

      if (assertFullyBalanced) {
        assertClusterAsBalanced(balancedCluster);
        LoadOfAllTable = (Map) mockClusterServersWithTables(serverMap);
        List<RegionPlan> secondPlans = loadBalancer.balanceCluster(LoadOfAllTable);
        assertNull("Given a requirement to be fully balanced, second attempt at plans should " +
            "produce none.", secondPlans);
      }

      if (assertFullyBalancedForReplicas) {
        assertRegionReplicaPlacement(serverMap, rackManager);
      }
    }
  }

  protected void testWithClusterWithIteration(Map<ServerName, List<RegionInfo>> serverMap,
    RackManager rackManager, boolean assertFullyBalanced, boolean assertFullyBalancedForReplicas) {
    List<ServerAndLoad> list = convertToList(serverMap);
    LOG.info("Mock Cluster : " + printMock(list) + " " + printStats(list));

    loadBalancer.setRackManager(rackManager);
    // Run the balancer.
    Map<TableName, Map<ServerName, List<RegionInfo>>> LoadOfAllTable =
      (Map) mockClusterServersWithTables(serverMap);
    List<RegionPlan> plans = loadBalancer.balanceCluster(LoadOfAllTable);
    assertNotNull("Initial cluster balance should produce plans.", plans);

    List<ServerAndLoad> balancedCluster = null;
    // Run through iteration until done. Otherwise will be killed as test time out
    while (plans != null && (assertFullyBalanced || assertFullyBalancedForReplicas)) {
      // Apply the plan to the mock cluster.
      balancedCluster = reconcile(list, plans, serverMap);

      // Print out the cluster loads to make debugging easier.
      LOG.info("Mock after balance: " + printMock(balancedCluster));

      LoadOfAllTable = (Map) mockClusterServersWithTables(serverMap);
      plans = loadBalancer.balanceCluster(LoadOfAllTable);
    }

    // Print out the cluster loads to make debugging easier.
    LOG.info("Mock Final balance: " + printMock(balancedCluster));

    if (assertFullyBalanced) {
      assertNull("Given a requirement to be fully balanced, second attempt at plans should " +
        "produce none.", plans);
    }
    if (assertFullyBalancedForReplicas) {
      assertRegionReplicaPlacement(serverMap, rackManager);
    }
  }

  protected Map<ServerName, List<RegionInfo>> createServerMap(int numNodes,
                                                             int numRegions,
                                                             int numRegionsPerServer,
                                                             int replication,
                                                             int numTables) {
    //construct a cluster of numNodes, having  a total of numRegions. Each RS will hold
    //numRegionsPerServer many regions except for the last one, which will host all the
    //remaining regions
    int[] cluster = new int[numNodes];
    for (int i =0; i < numNodes; i++) {
      cluster[i] = numRegionsPerServer;
    }
    cluster[cluster.length - 1] = numRegions - ((cluster.length - 1) * numRegionsPerServer);
    Map<ServerName, List<RegionInfo>> clusterState = mockClusterServers(cluster, numTables);
    if (replication > 0) {
      // replicate the regions to the same servers
      for (List<RegionInfo> regions : clusterState.values()) {
        int length = regions.size();
        for (int i = 0; i < length; i++) {
          for (int r = 1; r < replication ; r++) {
            regions.add(RegionReplicaUtil.getRegionInfoForReplica(regions.get(i), r));
          }
        }
      }
    }

    return clusterState;
  }

}
