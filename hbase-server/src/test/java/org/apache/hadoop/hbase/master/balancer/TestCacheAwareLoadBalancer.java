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

import static org.apache.hadoop.hbase.io.hfile.BlockCacheFactory.BUCKET_CACHE_PERSISTENT_PATH_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Size;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ LargeTests.class })
public class TestCacheAwareLoadBalancer extends BalancerTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCacheAwareLoadBalancer.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestCacheAwareLoadBalancer.class);

  private static CacheAwareLoadBalancer loadBalancer;

  static List<ServerName> servers;

  static List<TableDescriptor> tableDescs;

  static Map<TableName, String> tableMap = new HashMap<>();

  static TableName[] tables = new TableName[] { TableName.valueOf("dt1"), TableName.valueOf("dt2"),
    TableName.valueOf("dt3"), TableName.valueOf("dt4") };

  private static List<ServerName> generateServers(int numServers) {
    List<ServerName> servers = new ArrayList<>(numServers);
    Random rand = ThreadLocalRandom.current();
    for (int i = 0; i < numServers; i++) {
      String host = "server" + rand.nextInt(100000);
      int port = rand.nextInt(60000);
      servers.add(ServerName.valueOf(host, port, -1));
    }
    return servers;
  }

  private static List<TableDescriptor> constructTableDesc(boolean hasBogusTable) {
    List<TableDescriptor> tds = Lists.newArrayList();
    for (int i = 0; i < tables.length; i++) {
      TableDescriptor htd = TableDescriptorBuilder.newBuilder(tables[i]).build();
      tds.add(htd);
    }
    return tds;
  }

  protected static MasterServices getMockedMaster() throws IOException {
    TableDescriptors tds = Mockito.mock(TableDescriptors.class);
    Mockito.when(tds.get(tables[0])).thenReturn(tableDescs.get(0));
    Mockito.when(tds.get(tables[1])).thenReturn(tableDescs.get(1));
    Mockito.when(tds.get(tables[2])).thenReturn(tableDescs.get(2));
    Mockito.when(tds.get(tables[3])).thenReturn(tableDescs.get(3));
    MasterServices services = Mockito.mock(HMaster.class);
    Mockito.when(services.getTableDescriptors()).thenReturn(tds);
    AssignmentManager am = Mockito.mock(AssignmentManager.class);
    Mockito.when(services.getAssignmentManager()).thenReturn(am);
    return services;
  }

  private ServerMetrics mockServerMetricsWithRegionPrefetchInfo(ServerName server,
    List<RegionInfo> regionsOnServer, float currentPrefetchRatio,
    List<RegionInfo> oldPrefechedRegions, int oldPrefetchSize, int regionSize) {
    ServerMetrics serverMetrics = mock(ServerMetrics.class);
    Map<byte[], RegionMetrics> regionLoadMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (RegionInfo info : regionsOnServer) {
      RegionMetrics rl = mock(RegionMetrics.class);
      when(rl.getReadRequestCount()).thenReturn(0L);
      when(rl.getWriteRequestCount()).thenReturn(0L);
      when(rl.getMemStoreSize()).thenReturn(Size.ZERO);
      when(rl.getStoreFileSize()).thenReturn(Size.ZERO);
      when(rl.getCurrentRegionPrefetchRatio()).thenReturn(currentPrefetchRatio);
      when(rl.getRegionSizeMB()).thenReturn(new Size(regionSize, Size.Unit.MEGABYTE));
      regionLoadMap.put(info.getRegionName(), rl);
    }
    when(serverMetrics.getRegionMetrics()).thenReturn(regionLoadMap);
    Map<String, Integer> oldPrefetchInfoMap = new HashMap<>();
    for (RegionInfo info : oldPrefechedRegions) {
      oldPrefetchInfoMap.put(info.getEncodedName(), oldPrefetchSize);
    }
    when(serverMetrics.getRegionPrefetchInfo()).thenReturn(oldPrefetchInfoMap);
    return serverMetrics;
  }

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    servers = generateServers(3);
    tableDescs = constructTableDesc(false);
    Configuration conf = HBaseConfiguration.create();
    conf.set(BUCKET_CACHE_PERSISTENT_PATH_KEY, "prefetch_list");
    loadBalancer = new CacheAwareLoadBalancer();
    loadBalancer.setMasterServices(getMockedMaster());
    loadBalancer.setConf(conf);
    loadBalancer.initialize();
  }

  @Test
  public void testRegionsNotPrefetchedOnOldServerAndCurrentServer() throws Exception {
    // The regions are not prefetched on old server as well as the current server. This causes
    // skewness in the region allocation which should be fixed by the balancer

    Map<ServerName, List<RegionInfo>> clusterState = new HashMap<>();
    ServerName server0 = servers.get(0);
    ServerName server1 = servers.get(1);
    ServerName server2 = servers.get(2);

    // Simulate that the regions previously hosted by server1 are now hosted on server0
    List<RegionInfo> regionsOnServer0 = randomRegions(10);
    List<RegionInfo> regionsOnServer1 = randomRegions(0);
    List<RegionInfo> regionsOnServer2 = randomRegions(5);

    clusterState.put(server0, regionsOnServer0);
    clusterState.put(server1, regionsOnServer1);
    clusterState.put(server2, regionsOnServer2);

    // Mock cluster metrics
    Map<ServerName, ServerMetrics> serverMetricsMap = new TreeMap<>();
    serverMetricsMap.put(server0, mockServerMetricsWithRegionPrefetchInfo(server0, regionsOnServer0,
      0.0f, new ArrayList<>(), 0, 10));
    serverMetricsMap.put(server1, mockServerMetricsWithRegionPrefetchInfo(server1, regionsOnServer1,
      0.0f, new ArrayList<>(), 0, 10));
    serverMetricsMap.put(server2, mockServerMetricsWithRegionPrefetchInfo(server2, regionsOnServer2,
      0.0f, new ArrayList<>(), 0, 10));
    ClusterMetrics clusterMetrics = mock(ClusterMetrics.class);
    when(clusterMetrics.getLiveServerMetrics()).thenReturn(serverMetricsMap);
    loadBalancer.setClusterMetrics(clusterMetrics);

    Map<TableName, Map<ServerName, List<RegionInfo>>> LoadOfAllTable =
      (Map) mockClusterServersWithTables(clusterState);
    List<RegionPlan> plans = loadBalancer.balanceCluster(LoadOfAllTable);
    Set<RegionInfo> regionsMovedFromServer0 = new HashSet<>();
    Map<ServerName, List<RegionInfo>> targetServers = new HashMap<>();
    for (RegionPlan plan : plans) {
      if (plan.getSource().equals(server0)) {
        regionsMovedFromServer0.add(plan.getRegionInfo());
        if (!targetServers.containsKey(plan.getDestination())) {
          targetServers.put(plan.getDestination(), new ArrayList<>());
        }
        targetServers.get(plan.getDestination()).add(plan.getRegionInfo());
      }
    }
    // should move 5 regions from server0 to server 1
    assertEquals(5, regionsMovedFromServer0.size());
    assertEquals(5, targetServers.get(server1).size());
  }

  @Test
  public void testRegionsPartiallyPrefetchedOnOldServerAndNotPrefetchedOnCurrentServer() throws Exception {
    //The regions are partially prefetched on old server but not prefetched on the current server

    Map<ServerName, List<RegionInfo>> clusterState = new HashMap<>();
    ServerName server0 = servers.get(0);
    ServerName server1 = servers.get(1);
    ServerName server2 = servers.get(2);

    // Simulate that the regions previously hosted by server1 are now hosted on server0
    List<RegionInfo> regionsOnServer0 = randomRegions(10);
    List<RegionInfo> regionsOnServer1 = randomRegions(0);
    List<RegionInfo> regionsOnServer2 = randomRegions(5);

    clusterState.put(server0, regionsOnServer0);
    clusterState.put(server1, regionsOnServer1);
    clusterState.put(server2, regionsOnServer2);

    // Mock cluster metrics

    // Mock 5 regions from server0 were previously hosted on server1
    List<RegionInfo> oldPrefetchedRegions = regionsOnServer0.subList(5, regionsOnServer0.size() - 1);

    Map<ServerName, ServerMetrics> serverMetricsMap = new TreeMap<>();
    serverMetricsMap.put(server0, mockServerMetricsWithRegionPrefetchInfo(server0, regionsOnServer0,
      0.0f, new ArrayList<>(), 0, 10));
    serverMetricsMap.put(server1, mockServerMetricsWithRegionPrefetchInfo(server1, regionsOnServer1,
      0.0f, oldPrefetchedRegions, 6, 10));
    serverMetricsMap.put(server2, mockServerMetricsWithRegionPrefetchInfo(server2, regionsOnServer2,
      0.0f, new ArrayList<>(), 0, 10));
    ClusterMetrics clusterMetrics = mock(ClusterMetrics.class);
    when(clusterMetrics.getLiveServerMetrics()).thenReturn(serverMetricsMap);
    loadBalancer.setClusterMetrics(clusterMetrics);

    Map<TableName, Map<ServerName, List<RegionInfo>>> LoadOfAllTable =
      (Map) mockClusterServersWithTables(clusterState);
    List<RegionPlan> plans = loadBalancer.balanceCluster(LoadOfAllTable);
    Set<RegionInfo> regionsMovedFromServer0 = new HashSet<>();
    Map<ServerName, List<RegionInfo>> targetServers = new HashMap<>();
    for (RegionPlan plan : plans) {
      if (plan.getSource().equals(server0)) {
        regionsMovedFromServer0.add(plan.getRegionInfo());
        if (!targetServers.containsKey(plan.getDestination())) {
          targetServers.put(plan.getDestination(), new ArrayList<>());
        }
        targetServers.get(plan.getDestination()).add(plan.getRegionInfo());
      }
    }
    // should move 5 regions from server0 to server1
    assertEquals(5, regionsMovedFromServer0.size());
    assertEquals(5, targetServers.get(server1).size());
    assertTrue(targetServers.get(server1).containsAll(oldPrefetchedRegions));
  }

  @Test
  public void testRegionsFullyPrefetchedOnOldServerAndNoPrefetchedOnCurrentServers() throws Exception {
    //The regions are fully prefetched on old server

    Map<ServerName, List<RegionInfo>> clusterState = new HashMap<>();
    ServerName server0 = servers.get(0);
    ServerName server1 = servers.get(1);
    ServerName server2 = servers.get(2);

    // Simulate that the regions previously hosted by server1 are now hosted on server0
    List<RegionInfo> regionsOnServer0 = randomRegions(10);
    List<RegionInfo> regionsOnServer1 = randomRegions(0);
    List<RegionInfo> regionsOnServer2 = randomRegions(5);

    clusterState.put(server0, regionsOnServer0);
    clusterState.put(server1, regionsOnServer1);
    clusterState.put(server2, regionsOnServer2);

    // Mock cluster metrics

    // Mock 5 regions from server0 were previously hosted on server1
    List<RegionInfo> oldPrefetchedRegions = regionsOnServer0.subList(5, regionsOnServer0.size() - 1);

    Map<ServerName, ServerMetrics> serverMetricsMap = new TreeMap<>();
    serverMetricsMap.put(server0, mockServerMetricsWithRegionPrefetchInfo(server0, regionsOnServer0,
      0.0f, new ArrayList<>(), 0, 10));
    serverMetricsMap.put(server1, mockServerMetricsWithRegionPrefetchInfo(server1, regionsOnServer1,
      0.0f, oldPrefetchedRegions, 10, 10));
    serverMetricsMap.put(server2, mockServerMetricsWithRegionPrefetchInfo(server2, regionsOnServer2,
      0.0f, new ArrayList<>(), 0, 10));
    ClusterMetrics clusterMetrics = mock(ClusterMetrics.class);
    when(clusterMetrics.getLiveServerMetrics()).thenReturn(serverMetricsMap);
    loadBalancer.setClusterMetrics(clusterMetrics);

    Map<TableName, Map<ServerName, List<RegionInfo>>> LoadOfAllTable =
      (Map) mockClusterServersWithTables(clusterState);
    List<RegionPlan> plans = loadBalancer.balanceCluster(LoadOfAllTable);
    Set<RegionInfo> regionsMovedFromServer0 = new HashSet<>();
    Map<ServerName, List<RegionInfo>> targetServers = new HashMap<>();
    for (RegionPlan plan : plans) {
      if (plan.getSource().equals(server0)) {
        regionsMovedFromServer0.add(plan.getRegionInfo());
        if (!targetServers.containsKey(plan.getDestination())) {
          targetServers.put(plan.getDestination(), new ArrayList<>());
        }
        targetServers.get(plan.getDestination()).add(plan.getRegionInfo());
      }
    }
    // should move 5 regions from server0 to server1
    assertEquals(5, regionsMovedFromServer0.size());
    assertEquals(5, targetServers.get(server1).size());
    assertTrue(targetServers.get(server1).containsAll(oldPrefetchedRegions));
  }

  @Test
  public void testRegionsFullyPrefetchedOnOldAndCurrentServers() throws Exception {
    //The regions are fully prefetched on old server

    Map<ServerName, List<RegionInfo>> clusterState = new HashMap<>();
    ServerName server0 = servers.get(0);
    ServerName server1 = servers.get(1);
    ServerName server2 = servers.get(2);

    // Simulate that the regions previously hosted by server1 are now hosted on server0
    List<RegionInfo> regionsOnServer0 = randomRegions(10);
    List<RegionInfo> regionsOnServer1 = randomRegions(0);
    List<RegionInfo> regionsOnServer2 = randomRegions(5);

    clusterState.put(server0, regionsOnServer0);
    clusterState.put(server1, regionsOnServer1);
    clusterState.put(server2, regionsOnServer2);

    // Mock cluster metrics

    // Mock 5 regions from server0 were previously hosted on server1
    List<RegionInfo> oldPrefetchedRegions = regionsOnServer0.subList(5, regionsOnServer0.size() - 1);

    Map<ServerName, ServerMetrics> serverMetricsMap = new TreeMap<>();
    serverMetricsMap.put(server0, mockServerMetricsWithRegionPrefetchInfo(server0, regionsOnServer0,
      1.0f, new ArrayList<>(), 0, 10));
    serverMetricsMap.put(server1, mockServerMetricsWithRegionPrefetchInfo(server1, regionsOnServer1,
      1.0f, oldPrefetchedRegions, 10, 10));
    serverMetricsMap.put(server2, mockServerMetricsWithRegionPrefetchInfo(server2, regionsOnServer2,
      1.0f, new ArrayList<>(), 0, 10));
    ClusterMetrics clusterMetrics = mock(ClusterMetrics.class);
    when(clusterMetrics.getLiveServerMetrics()).thenReturn(serverMetricsMap);
    loadBalancer.setClusterMetrics(clusterMetrics);

    Map<TableName, Map<ServerName, List<RegionInfo>>> LoadOfAllTable =
      (Map) mockClusterServersWithTables(clusterState);
    List<RegionPlan> plans = loadBalancer.balanceCluster(LoadOfAllTable);
    Set<RegionInfo> regionsMovedFromServer0 = new HashSet<>();
    Map<ServerName, List<RegionInfo>> targetServers = new HashMap<>();
    for (RegionPlan plan : plans) {
      if (plan.getSource().equals(server0)) {
        regionsMovedFromServer0.add(plan.getRegionInfo());
        if (!targetServers.containsKey(plan.getDestination())) {
          targetServers.put(plan.getDestination(), new ArrayList<>());
        }
        targetServers.get(plan.getDestination()).add(plan.getRegionInfo());
      }
    }
    // should move 5 regions from server0 to server1
    assertEquals(5, regionsMovedFromServer0.size());
    assertEquals(5, targetServers.get(server1).size());
    assertTrue(targetServers.get(server1).containsAll(oldPrefetchedRegions));
  }

  @Test
  public void testRegionsPartiallyPrefetchedOnOldServerAndCurrentServer() throws Exception {
    //The regions are partially prefetched on old server

    Map<ServerName, List<RegionInfo>> clusterState = new HashMap<>();
    ServerName server0 = servers.get(0);
    ServerName server1 = servers.get(1);
    ServerName server2 = servers.get(2);

    // Simulate that the regions previously hosted by server1 are now hosted on server0
    List<RegionInfo> regionsOnServer0 = randomRegions(10);
    List<RegionInfo> regionsOnServer1 = randomRegions(0);
    List<RegionInfo> regionsOnServer2 = randomRegions(5);

    clusterState.put(server0, regionsOnServer0);
    clusterState.put(server1, regionsOnServer1);
    clusterState.put(server2, regionsOnServer2);

    // Mock cluster metrics

    // Mock 5 regions from server0 were previously hosted on server1
    List<RegionInfo> oldPrefetchedRegions = regionsOnServer0.subList(5, regionsOnServer0.size() - 1);

    Map<ServerName, ServerMetrics> serverMetricsMap = new TreeMap<>();
    serverMetricsMap.put(server0,
      mockServerMetricsWithRegionPrefetchInfo(server0, regionsOnServer0, 0.2f, new ArrayList<>(), 0,
        10));
    serverMetricsMap.put(server1,
      mockServerMetricsWithRegionPrefetchInfo(server1, regionsOnServer1, 0.0f, oldPrefetchedRegions,
        6, 10));
    serverMetricsMap.put(server2,
      mockServerMetricsWithRegionPrefetchInfo(server2, regionsOnServer2, 1.0f, new ArrayList<>(), 0,
        10));
    ClusterMetrics clusterMetrics = mock(ClusterMetrics.class);
    when(clusterMetrics.getLiveServerMetrics()).thenReturn(serverMetricsMap);
    loadBalancer.setClusterMetrics(clusterMetrics);

    Map<TableName, Map<ServerName, List<RegionInfo>>> LoadOfAllTable =
      (Map) mockClusterServersWithTables(clusterState);
    List<RegionPlan> plans = loadBalancer.balanceCluster(LoadOfAllTable);
    Set<RegionInfo> regionsMovedFromServer0 = new HashSet<>();
    Map<ServerName, List<RegionInfo>> targetServers = new HashMap<>();
    for (RegionPlan plan : plans) {
      if (plan.getSource().equals(server0)) {
        regionsMovedFromServer0.add(plan.getRegionInfo());
        if (!targetServers.containsKey(plan.getDestination())) {
          targetServers.put(plan.getDestination(), new ArrayList<>());
        }
        targetServers.get(plan.getDestination()).add(plan.getRegionInfo());
      }
    }
    assertEquals(5, regionsMovedFromServer0.size());
    assertEquals(5, targetServers.get(server1).size());
    assertTrue(targetServers.get(server1).containsAll(oldPrefetchedRegions));
  }
}
