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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Size;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@Category({ LargeTests.class })
public class TestCacheAwareLoadBalancer extends BalancerTestBase {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCacheAwareLoadBalancer.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestCacheAwareLoadBalancer.class);

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

  private ServerMetrics mockServerMetricsWithRegionCacheInfo(ServerName server,
    List<RegionInfo> regionsOnServer, float currentCacheRatio, List<RegionInfo> oldRegionCacheInfo,
    int oldRegionCachedSize, int regionSize) {
    ServerMetrics serverMetrics = mock(ServerMetrics.class);
    Map<byte[], RegionMetrics> regionLoadMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (RegionInfo info : regionsOnServer) {
      RegionMetrics rl = mock(RegionMetrics.class);
      when(rl.getReadRequestCount()).thenReturn(0L);
      when(rl.getWriteRequestCount()).thenReturn(0L);
      when(rl.getMemStoreSize()).thenReturn(Size.ZERO);
      when(rl.getStoreFileSize()).thenReturn(Size.ZERO);
      when(rl.getCurrentRegionCachedRatio()).thenReturn(currentCacheRatio);
      when(rl.getRegionSizeMB()).thenReturn(new Size(regionSize, Size.Unit.MEGABYTE));
      regionLoadMap.put(info.getRegionName(), rl);
    }
    when(serverMetrics.getRegionMetrics()).thenReturn(regionLoadMap);
    Map<String, Integer> oldCacheRatioMap = new HashMap<>();
    for (RegionInfo info : oldRegionCacheInfo) {
      oldCacheRatioMap.put(info.getEncodedName(), oldRegionCachedSize);
    }
    when(serverMetrics.getRegionCachedInfo()).thenReturn(oldCacheRatioMap);
    return serverMetrics;
  }

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    servers = generateServers(3);
    tableDescs = constructTableDesc(false);
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.BUCKET_CACHE_PERSISTENT_PATH_KEY, "prefetch_file_list");
    loadBalancer = new CacheAwareLoadBalancer();
    loadBalancer.setClusterInfoProvider(new DummyClusterInfoProvider(conf));
    loadBalancer.loadConf(conf);
  }

  @Test
  public void testRegionsNotCachedOnOldServerAndCurrentServer() throws Exception {
    // The regions are not cached on old server as well as the current server. This causes
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
    serverMetricsMap.put(server0, mockServerMetricsWithRegionCacheInfo(server0, regionsOnServer0,
      0.0f, new ArrayList<>(), 0, 10));
    serverMetricsMap.put(server1, mockServerMetricsWithRegionCacheInfo(server1, regionsOnServer1,
      0.0f, new ArrayList<>(), 0, 10));
    serverMetricsMap.put(server2, mockServerMetricsWithRegionCacheInfo(server2, regionsOnServer2,
      0.0f, new ArrayList<>(), 0, 10));
    ClusterMetrics clusterMetrics = mock(ClusterMetrics.class);
    when(clusterMetrics.getLiveServerMetrics()).thenReturn(serverMetricsMap);
    loadBalancer.updateClusterMetrics(clusterMetrics);

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
  public void testRegionsPartiallyCachedOnOldServerAndNotCachedOnCurrentServer() throws Exception {
    // The regions are partially cached on old server but not cached on the current server

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
    List<RegionInfo> oldCachedRegions = regionsOnServer0.subList(5, regionsOnServer0.size() - 1);

    Map<ServerName, ServerMetrics> serverMetricsMap = new TreeMap<>();
    serverMetricsMap.put(server0, mockServerMetricsWithRegionCacheInfo(server0, regionsOnServer0,
      0.0f, new ArrayList<>(), 0, 10));
    serverMetricsMap.put(server1, mockServerMetricsWithRegionCacheInfo(server1, regionsOnServer1,
      0.0f, oldCachedRegions, 6, 10));
    serverMetricsMap.put(server2, mockServerMetricsWithRegionCacheInfo(server2, regionsOnServer2,
      0.0f, new ArrayList<>(), 0, 10));
    ClusterMetrics clusterMetrics = mock(ClusterMetrics.class);
    when(clusterMetrics.getLiveServerMetrics()).thenReturn(serverMetricsMap);
    loadBalancer.updateClusterMetrics(clusterMetrics);

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
    assertTrue(targetServers.get(server1).containsAll(oldCachedRegions));
  }

  @Test
  public void testThrottlingRegionBeyondThreshold() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    CacheAwareLoadBalancer balancer = new CacheAwareLoadBalancer();
    balancer.setClusterInfoProvider(new DummyClusterInfoProvider(conf));
    balancer.loadConf(conf);
    balancer.initialize();
    ServerName server0 = servers.get(0);
    ServerName server1 = servers.get(1);
    Pair<ServerName, Float> regionRatio = new Pair<>();
    regionRatio.setFirst(server0);
    regionRatio.setSecond(1.0f);
    balancer.regionCacheRatioOnOldServerMap.put("region1", regionRatio);
    RegionInfo mockedInfo = mock(RegionInfo.class);
    when(mockedInfo.getEncodedName()).thenReturn("region1");
    RegionPlan plan = new RegionPlan(mockedInfo, server1, server0);
    long startTime = EnvironmentEdgeManager.currentTime();
    balancer.throttle(plan);
    long endTime = EnvironmentEdgeManager.currentTime();
    assertTrue((endTime - startTime) < 10);
  }

  @Test
  public void testThrottlingRegionBelowThreshold() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setLong(CacheAwareLoadBalancer.MOVE_THROTTLING, 100);
    CacheAwareLoadBalancer balancer = new CacheAwareLoadBalancer();
    balancer.setClusterInfoProvider(new DummyClusterInfoProvider(conf));
    balancer.loadConf(conf);
    balancer.initialize();
    ServerName server0 = servers.get(0);
    ServerName server1 = servers.get(1);
    Pair<ServerName, Float> regionRatio = new Pair<>();
    regionRatio.setFirst(server0);
    regionRatio.setSecond(0.1f);
    balancer.regionCacheRatioOnOldServerMap.put("region1", regionRatio);
    RegionInfo mockedInfo = mock(RegionInfo.class);
    when(mockedInfo.getEncodedName()).thenReturn("region1");
    RegionPlan plan = new RegionPlan(mockedInfo, server1, server0);
    long startTime = EnvironmentEdgeManager.currentTime();
    balancer.throttle(plan);
    long endTime = EnvironmentEdgeManager.currentTime();
    assertTrue((endTime - startTime) >= 100);
  }

  @Test
  public void testThrottlingCacheRatioUnknownOnTarget() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setLong(CacheAwareLoadBalancer.MOVE_THROTTLING, 100);
    CacheAwareLoadBalancer balancer = new CacheAwareLoadBalancer();
    balancer.setClusterInfoProvider(new DummyClusterInfoProvider(conf));
    balancer.loadConf(conf);
    balancer.initialize();
    ServerName server0 = servers.get(0);
    ServerName server1 = servers.get(1);
    ServerName server3 = servers.get(2);
    // setting region cache ratio 100% on server 3, though this is not the target in the region plan
    Pair<ServerName, Float> regionRatio = new Pair<>();
    regionRatio.setFirst(server3);
    regionRatio.setSecond(1.0f);
    balancer.regionCacheRatioOnOldServerMap.put("region1", regionRatio);
    RegionInfo mockedInfo = mock(RegionInfo.class);
    when(mockedInfo.getEncodedName()).thenReturn("region1");
    RegionPlan plan = new RegionPlan(mockedInfo, server1, server0);
    long startTime = EnvironmentEdgeManager.currentTime();
    balancer.throttle(plan);
    long endTime = EnvironmentEdgeManager.currentTime();
    assertTrue((endTime - startTime) >= 100);
  }

  @Test
  public void testThrottlingCacheRatioUnknownForRegion() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setLong(CacheAwareLoadBalancer.MOVE_THROTTLING, 100);
    CacheAwareLoadBalancer balancer = new CacheAwareLoadBalancer();
    balancer.setClusterInfoProvider(new DummyClusterInfoProvider(conf));
    balancer.loadConf(conf);
    balancer.initialize();
    ServerName server0 = servers.get(0);
    ServerName server1 = servers.get(1);
    ServerName server3 = servers.get(2);
    // No cache ratio available for region1
    RegionInfo mockedInfo = mock(RegionInfo.class);
    when(mockedInfo.getEncodedName()).thenReturn("region1");
    RegionPlan plan = new RegionPlan(mockedInfo, server1, server0);
    long startTime = EnvironmentEdgeManager.currentTime();
    balancer.throttle(plan);
    long endTime = EnvironmentEdgeManager.currentTime();
    assertTrue((endTime - startTime) >= 100);
  }

  @Test
  public void testRegionPlansSortedByCacheRatioOnTarget() throws Exception {
    // The regions are fully cached on old server

    Map<ServerName, List<RegionInfo>> clusterState = new HashMap<>();
    ServerName server0 = servers.get(0);
    ServerName server1 = servers.get(1);
    ServerName server2 = servers.get(2);

    // Simulate on RS with all regions, and two RSes with no regions
    List<RegionInfo> regionsOnServer0 = randomRegions(15);
    List<RegionInfo> regionsOnServer1 = randomRegions(0);
    List<RegionInfo> regionsOnServer2 = randomRegions(0);

    clusterState.put(server0, regionsOnServer0);
    clusterState.put(server1, regionsOnServer1);
    clusterState.put(server2, regionsOnServer2);

    // Mock cluster metrics
    // Mock 5 regions from server0 were previously hosted on server1
    List<RegionInfo> oldCachedRegions1 = regionsOnServer0.subList(5, 10);
    List<RegionInfo> oldCachedRegions2 = regionsOnServer0.subList(10, regionsOnServer0.size());
    Map<ServerName, ServerMetrics> serverMetricsMap = new TreeMap<>();
    // mock server metrics to set cache ratio as 0 in the RS 0
    serverMetricsMap.put(server0, mockServerMetricsWithRegionCacheInfo(server0, regionsOnServer0,
      0.0f, new ArrayList<>(), 0, 10));
    // mock server metrics to set cache ratio as 1 in the RS 1
    serverMetricsMap.put(server1, mockServerMetricsWithRegionCacheInfo(server1, regionsOnServer1,
      0.0f, oldCachedRegions1, 10, 10));
    // mock server metrics to set cache ratio as .8 in the RS 2
    serverMetricsMap.put(server2, mockServerMetricsWithRegionCacheInfo(server2, regionsOnServer2,
      0.0f, oldCachedRegions2, 8, 10));
    ClusterMetrics clusterMetrics = mock(ClusterMetrics.class);
    when(clusterMetrics.getLiveServerMetrics()).thenReturn(serverMetricsMap);
    loadBalancer.updateClusterMetrics(clusterMetrics);

    Map<TableName, Map<ServerName, List<RegionInfo>>> LoadOfAllTable =
      (Map) mockClusterServersWithTables(clusterState);
    List<RegionPlan> plans = loadBalancer.balanceCluster(LoadOfAllTable);
    LOG.debug("plans size: {}", plans.size());
    LOG.debug("plans: {}", plans);
    LOG.debug("server1 name: {}", server1.getServerName());
    // assert the plans are in descending order from the most cached to the least cached
    int highCacheCount = 0;
    for (RegionPlan plan : plans) {
      LOG.debug("plan region: {}, target server: {}", plan.getRegionInfo().getEncodedName(),
        plan.getDestination().getServerName());
      if (highCacheCount < 5) {
        LOG.debug("Count: {}", highCacheCount);
        assertTrue(oldCachedRegions1.contains(plan.getRegionInfo()));
        assertFalse(oldCachedRegions2.contains(plan.getRegionInfo()));
        highCacheCount++;
      } else {
        assertTrue(oldCachedRegions2.contains(plan.getRegionInfo()));
        assertFalse(oldCachedRegions1.contains(plan.getRegionInfo()));
      }
    }

  }

  @Test
  public void testRegionsFullyCachedOnOldServerAndNotCachedOnCurrentServers() throws Exception {
    // The regions are fully cached on old server

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
    List<RegionInfo> oldCachedRegions = regionsOnServer0.subList(5, regionsOnServer0.size() - 1);

    Map<ServerName, ServerMetrics> serverMetricsMap = new TreeMap<>();
    serverMetricsMap.put(server0, mockServerMetricsWithRegionCacheInfo(server0, regionsOnServer0,
      0.0f, new ArrayList<>(), 0, 10));
    serverMetricsMap.put(server1, mockServerMetricsWithRegionCacheInfo(server1, regionsOnServer1,
      0.0f, oldCachedRegions, 10, 10));
    serverMetricsMap.put(server2, mockServerMetricsWithRegionCacheInfo(server2, regionsOnServer2,
      0.0f, new ArrayList<>(), 0, 10));
    ClusterMetrics clusterMetrics = mock(ClusterMetrics.class);
    when(clusterMetrics.getLiveServerMetrics()).thenReturn(serverMetricsMap);
    loadBalancer.updateClusterMetrics(clusterMetrics);

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
    assertTrue(targetServers.get(server1).containsAll(oldCachedRegions));
  }

  @Test
  public void testRegionsFullyCachedOnOldAndCurrentServers() throws Exception {
    // The regions are fully cached on old server

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
    List<RegionInfo> oldCachedRegions = regionsOnServer0.subList(5, regionsOnServer0.size() - 1);

    Map<ServerName, ServerMetrics> serverMetricsMap = new TreeMap<>();
    serverMetricsMap.put(server0, mockServerMetricsWithRegionCacheInfo(server0, regionsOnServer0,
      1.0f, new ArrayList<>(), 0, 10));
    serverMetricsMap.put(server1, mockServerMetricsWithRegionCacheInfo(server1, regionsOnServer1,
      1.0f, oldCachedRegions, 10, 10));
    serverMetricsMap.put(server2, mockServerMetricsWithRegionCacheInfo(server2, regionsOnServer2,
      1.0f, new ArrayList<>(), 0, 10));
    ClusterMetrics clusterMetrics = mock(ClusterMetrics.class);
    when(clusterMetrics.getLiveServerMetrics()).thenReturn(serverMetricsMap);
    loadBalancer.updateClusterMetrics(clusterMetrics);

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
    assertTrue(targetServers.get(server1).containsAll(oldCachedRegions));
  }

  @Test
  public void testRegionsPartiallyCachedOnOldServerAndCurrentServer() throws Exception {
    // The regions are partially cached on old server

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
    List<RegionInfo> oldCachedRegions = regionsOnServer0.subList(5, regionsOnServer0.size() - 1);

    Map<ServerName, ServerMetrics> serverMetricsMap = new TreeMap<>();
    serverMetricsMap.put(server0, mockServerMetricsWithRegionCacheInfo(server0, regionsOnServer0,
      0.2f, new ArrayList<>(), 0, 10));
    serverMetricsMap.put(server1, mockServerMetricsWithRegionCacheInfo(server1, regionsOnServer1,
      0.0f, oldCachedRegions, 6, 10));
    serverMetricsMap.put(server2, mockServerMetricsWithRegionCacheInfo(server2, regionsOnServer2,
      1.0f, new ArrayList<>(), 0, 10));
    ClusterMetrics clusterMetrics = mock(ClusterMetrics.class);
    when(clusterMetrics.getLiveServerMetrics()).thenReturn(serverMetricsMap);
    loadBalancer.updateClusterMetrics(clusterMetrics);

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
    assertTrue(targetServers.get(server1).containsAll(oldCachedRegions));
  }

  @Test
  public void testBalancerNotThrowNPEWhenBalancerPlansIsNull() throws Exception {
    Map<ServerName, List<RegionInfo>> clusterState = new HashMap<>();
    ServerName server0 = servers.get(0);
    ServerName server1 = servers.get(1);
    ServerName server2 = servers.get(2);

    List<RegionInfo> regionsOnServer0 = randomRegions(5);
    List<RegionInfo> regionsOnServer1 = randomRegions(5);
    List<RegionInfo> regionsOnServer2 = randomRegions(5);

    clusterState.put(server0, regionsOnServer0);
    clusterState.put(server1, regionsOnServer1);
    clusterState.put(server2, regionsOnServer2);

    // Mock cluster metrics
    Map<ServerName, ServerMetrics> serverMetricsMap = new TreeMap<>();
    serverMetricsMap.put(server0, mockServerMetricsWithRegionCacheInfo(server0, regionsOnServer0,
      0.0f, new ArrayList<>(), 0, 10));
    serverMetricsMap.put(server1, mockServerMetricsWithRegionCacheInfo(server1, regionsOnServer1,
      0.0f, new ArrayList<>(), 0, 10));
    serverMetricsMap.put(server2, mockServerMetricsWithRegionCacheInfo(server2, regionsOnServer2,
      0.0f, new ArrayList<>(), 0, 10));

    ClusterMetrics clusterMetrics = mock(ClusterMetrics.class);
    when(clusterMetrics.getLiveServerMetrics()).thenReturn(serverMetricsMap);
    loadBalancer.updateClusterMetrics(clusterMetrics);

    Map<TableName, Map<ServerName, List<RegionInfo>>> LoadOfAllTable =
      (Map) mockClusterServersWithTables(clusterState);
    try {
      List<RegionPlan> plans = loadBalancer.balanceCluster(LoadOfAllTable);
      assertNull(plans);
    } catch (NullPointerException npe) {
      fail("NPE should not be thrown");
    }
  }
}
