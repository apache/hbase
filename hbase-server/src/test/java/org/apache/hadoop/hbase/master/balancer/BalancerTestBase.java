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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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

import com.google.protobuf.Service;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.master.RackManager;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.master.*;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.quotas.MasterQuotaManager;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.junit.Assert;
import org.junit.BeforeClass;

/**
 * Class used to be the base of unit tests on load balancers. It gives helper
 * methods to create maps of {@link ServerName} to lists of {@link HRegionInfo}
 * and to check list of region plans.
 *
 */
public class BalancerTestBase {
  private static final Log LOG = LogFactory.getLog(BalancerTestBase.class);
  protected static Random rand = new Random();
  static int regionId = 0;
  protected static Configuration conf;
  protected static StochasticLoadBalancer loadBalancer;

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    conf = HBaseConfiguration.create();
    conf.setClass("hbase.util.ip.to.rack.determiner", MockMapping.class, DNSToSwitchMapping.class);
    conf.setFloat("hbase.master.balancer.stochastic.maxMovePercent", 0.75f);
    conf.setFloat("hbase.regions.slop", 0.0f);
    conf.setFloat("hbase.master.balancer.stochastic.localityCost", 0);
    conf.setFloat("hbase.master.balancer.stochastic.minCostNeedBalance", 0.0f);
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

  /**
   * Data set for testLocalityCost:
   *
   * [test][regions][0] = [serverIndex] -> number of regions
   * [test][regions][regionIndex+1] = {server hosting region, locality percentage, datanodes}
   *
   * For each [test], there is a list of cluster config information grouped by [regions].
   * - [0] - the first element of the [regions] list is a list of servers with the value
   *         indicating the number of regions it hosts.
   * - [regionIndex+1] - the remaining elements of the array are regions, where the index value
   *         is 1 greater than the regionIndex.  This element holds an array that identifies:
   *     [0] - the serverIndex of the server hosting this region
   *     [1] - the locality percentage returned by getLocalityOfRegion(region, server) when the
   *           server is hosting both region and the hdfs blocks.
   *     [.] - the serverIndex of servers hosting the hdfs blocks, where a value of -1 indicates
   *         a dfs server not in the list of region servers.
   */
  protected int[][][] clusterRegionLocationMocks = new int[][][]{
      // Test 1: Basic region placement with 1 region server not hosting dfs block
      //     Locality Calculation:
      //        region[0] = 1 - 80/100 = (.2)  - server[2] hosts both the region and dfs blocks
      //        region[1] = 1.0                - server[0] only hosts the region, not dfs blocks
      //        region[2] = 1 - 70/100 = (.3)  - server[1] hosts both the region and dfs blocks
      //
      //      RESULT = 0.2 + 1.0 + 0.3 / 3.0 (3.0 is max value)
      //             = 1.5 / 3.0
      //             = 0.5
      new int[][]{
          new int[]{1, 1, 1},         // 3 region servers with 1 region each
          new int[]{2, 80, 1, 2, 0},  // region[0] on server[2] w/ 80% locality
          new int[]{0, 50, 1, 2},     // region[1] on server[0] w/ 50% , but no local dfs blocks
          new int[]{1, 70, 2, 0, 1},  // region[2] on server[1] w/ 70% locality
      },

      // Test 2: Sames as Test 1, but the last region has a datanode that isn't a region server
      new int[][]{
          new int[]{1, 1, 1},
          new int[]{2, 80, 1, 2, 0},
          new int[]{0, 50, 1, 2},
          new int[]{1, 70, -1, 2, 0, 1},  // the first region location is not on a region server
      },
  };

  // This mock allows us to test the LocalityCostFunction
  protected class MockCluster extends BaseLoadBalancer.Cluster {

    protected int[][] localityValue = null;   // [region][server] = percent of blocks

    protected MockCluster(int[][] regions) {

      // regions[0] is an array where index = serverIndex an value = number of regions
      super(mockClusterServers(regions[0], 1), null, null, null);

      localityValue = new int[regions.length-1][];
      // the remaining elements in the regions array contain values for:
      //   [0] - the serverIndex of the server hosting this region
      //   [1] - the locality percentage (in whole numbers) for the hosting region server
      //   [.] - a list of servers hosting dfs blocks for the region (-1 means its not one
      //         of our region servers.
      for (int i = 1; i < regions.length; i++){
        int regionIndex = i - 1;
        int serverIndex = regions[i][0];
        int locality = regions[i][1];
        int[] locations = Arrays.copyOfRange(regions[i], 2, regions[i].length);

        regionIndexToServerIndex[regionIndex] = serverIndex;
        localityValue[regionIndex] = new int[servers.length];
        localityValue[regionIndex][serverIndex] = (locality > 100)? locality % 100 : locality;
        regionLocations[regionIndex] = locations;
      }
    }

    @Override
    float getLocalityOfRegion(int region, int server) {
      // convert the locality percentage to a fraction
      return localityValue[region][server] / 100.0f;
    }
  }

  // This class is introduced because IP to rack resolution can be lengthy.
  public static class MockMapping implements DNSToSwitchMapping {
    public MockMapping(Configuration conf) {
    }

    public List<String> resolve(List<String> names) {
      List<String> ret = new ArrayList<String>(names.size());
      for (String name : names) {
        ret.add("rack");
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
      assertTrue(server.getLoad() >= 0);
      assertTrue(server.getLoad() <= max);
      assertTrue(server.getLoad() >= min);
    }
  }

  /**
   * Checks whether region replicas are not hosted on the same host.
   */
  public void assertRegionReplicaPlacement(Map<ServerName, List<HRegionInfo>> serverMap, RackManager rackManager) {
    TreeMap<String, Set<HRegionInfo>> regionsPerHost = new TreeMap<String, Set<HRegionInfo>>();
    TreeMap<String, Set<HRegionInfo>> regionsPerRack = new TreeMap<String, Set<HRegionInfo>>();

    for (Entry<ServerName, List<HRegionInfo>> entry : serverMap.entrySet()) {
      String hostname = entry.getKey().getHostname();
      Set<HRegionInfo> infos = regionsPerHost.get(hostname);
      if (infos == null) {
        infos = new HashSet<HRegionInfo>();
        regionsPerHost.put(hostname, infos);
      }

      for (HRegionInfo info : entry.getValue()) {
        HRegionInfo primaryInfo = RegionReplicaUtil.getRegionInfoForDefaultReplica(info);
        if (!infos.add(primaryInfo)) {
          Assert.fail("Two or more region replicas are hosted on the same host after balance");
        }
      }
    }

    if (rackManager == null) {
      return;
    }

    for (Entry<ServerName, List<HRegionInfo>> entry : serverMap.entrySet()) {
      String rack = rackManager.getRack(entry.getKey());
      Set<HRegionInfo> infos = regionsPerRack.get(rack);
      if (infos == null) {
        infos = new HashSet<HRegionInfo>();
        regionsPerRack.put(rack, infos);
      }

      for (HRegionInfo info : entry.getValue()) {
        HRegionInfo primaryInfo = RegionReplicaUtil.getRegionInfoForDefaultReplica(info);
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

  protected List<ServerAndLoad> convertToList(final Map<ServerName, List<HRegionInfo>> servers) {
    List<ServerAndLoad> list = new ArrayList<ServerAndLoad>(servers.size());
    for (Map.Entry<ServerName, List<HRegionInfo>> e : servers.entrySet()) {
      list.add(new ServerAndLoad(e.getKey(), e.getValue().size()));
    }
    return list;
  }

  protected String printMock(List<ServerAndLoad> balancedCluster) {
    SortedSet<ServerAndLoad> sorted = new TreeSet<ServerAndLoad>(balancedCluster);
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
   * @return
   */
  protected List<ServerAndLoad> reconcile(List<ServerAndLoad> list,
                                          List<RegionPlan> plans,
                                          Map<ServerName, List<HRegionInfo>> servers) {
    List<ServerAndLoad> result = new ArrayList<ServerAndLoad>(list.size());

    Map<ServerName, ServerAndLoad> map = new HashMap<ServerName, ServerAndLoad>(list.size());
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

  protected TreeMap<ServerName, List<HRegionInfo>> mockClusterServers(int[] mockCluster) {
    return mockClusterServers(mockCluster, -1);
  }

  protected BaseLoadBalancer.Cluster mockCluster(int[] mockCluster) {
    return new BaseLoadBalancer.Cluster(
      mockClusterServers(mockCluster, -1), null, null, null);
  }

  protected TreeMap<ServerName, List<HRegionInfo>> mockClusterServers(int[] mockCluster, int numTables) {
    int numServers = mockCluster.length;
    TreeMap<ServerName, List<HRegionInfo>> servers = new TreeMap<ServerName, List<HRegionInfo>>();
    for (int i = 0; i < numServers; i++) {
      int numRegions = mockCluster[i];
      ServerAndLoad sal = randomServer(0);
      List<HRegionInfo> regions = randomRegions(numRegions, numTables);
      servers.put(sal.getServerName(), regions);
    }
    return servers;
  }

  private Queue<HRegionInfo> regionQueue = new LinkedList<HRegionInfo>();

  protected List<HRegionInfo> randomRegions(int numRegions) {
    return randomRegions(numRegions, -1);
  }

  protected List<HRegionInfo> randomRegions(int numRegions, int numTables) {
    List<HRegionInfo> regions = new ArrayList<HRegionInfo>(numRegions);
    byte[] start = new byte[16];
    byte[] end = new byte[16];
    rand.nextBytes(start);
    rand.nextBytes(end);
    for (int i = 0; i < numRegions; i++) {
      if (!regionQueue.isEmpty()) {
        regions.add(regionQueue.poll());
        continue;
      }
      Bytes.putInt(start, 0, numRegions << 1);
      Bytes.putInt(end, 0, (numRegions << 1) + 1);
      TableName tableName =
          TableName.valueOf("table" + (numTables > 0 ? rand.nextInt(numTables) : i));
      HRegionInfo hri = new HRegionInfo(tableName, start, end, false, regionId++);
      regions.add(hri);
    }
    return regions;
  }

  protected void returnRegions(List<HRegionInfo> regions) {
    regionQueue.addAll(regions);
  }

  private Queue<ServerName> serverQueue = new LinkedList<ServerName>();

  protected ServerAndLoad randomServer(final int numRegionsPerServer) {
    if (!this.serverQueue.isEmpty()) {
      ServerName sn = this.serverQueue.poll();
      return new ServerAndLoad(sn, numRegionsPerServer);
    }
    String host = "srv" + rand.nextInt(Integer.MAX_VALUE);
    int port = rand.nextInt(60000);
    long startCode = rand.nextLong();
    ServerName sn = ServerName.valueOf(host, port, startCode);
    return new ServerAndLoad(sn, numRegionsPerServer);
  }

  protected List<ServerAndLoad> randomServers(int numServers, int numRegionsPerServer) {
    List<ServerAndLoad> servers = new ArrayList<ServerAndLoad>(numServers);
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
    Map<ServerName, List<HRegionInfo>> serverMap =
        createServerMap(numNodes, numRegions, numRegionsPerServer, replication, numTables);
    testWithCluster(serverMap, null, assertFullyBalanced, assertFullyBalancedForReplicas);
  }

  protected void testWithCluster(Map<ServerName, List<HRegionInfo>> serverMap,
      RackManager rackManager, boolean assertFullyBalanced, boolean assertFullyBalancedForReplicas) {
    List<ServerAndLoad> list = convertToList(serverMap);
    LOG.info("Mock Cluster : " + printMock(list) + " " + printStats(list));

    loadBalancer.setRackManager(rackManager);
    // Run the balancer.
    List<RegionPlan> plans = loadBalancer.balanceCluster(serverMap);
    assertNotNull(plans);

    // Check to see that this actually got to a stable place.
    if (assertFullyBalanced || assertFullyBalancedForReplicas) {
      // Apply the plan to the mock cluster.
      List<ServerAndLoad> balancedCluster = reconcile(list, plans, serverMap);

      // Print out the cluster loads to make debugging easier.
      LOG.info("Mock Balance : " + printMock(balancedCluster));

      if (assertFullyBalanced) {
        assertClusterAsBalanced(balancedCluster);
        List<RegionPlan> secondPlans =  loadBalancer.balanceCluster(serverMap);
        assertNull(secondPlans);
      }

      if (assertFullyBalancedForReplicas) {
        assertRegionReplicaPlacement(serverMap, rackManager);
      }
    }
  }

  protected Map<ServerName, List<HRegionInfo>> createServerMap(int numNodes,
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
    Map<ServerName, List<HRegionInfo>> clusterState = mockClusterServers(cluster, numTables);
    if (replication > 0) {
      // replicate the regions to the same servers
      for (List<HRegionInfo> regions : clusterState.values()) {
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
