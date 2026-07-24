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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
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
import org.apache.hadoop.hbase.util.Pair;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@Tag(LargeTests.TAG)
public class TestCacheAwareLoadBalancer extends BalancerTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestCacheAwareLoadBalancer.class);

  private static CacheAwareLoadBalancer loadBalancer;

  static List<ServerName> servers;

  static List<TableDescriptor> tableDescs;

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
    when(serverMetrics.getCacheFreeSize()).thenReturn(100L * 1024 * 1024 * 1024);
    return serverMetrics;
  }

  @BeforeAll
  public static void beforeAllTests() throws Exception {
    servers = generateServers(3);
    tableDescs = constructTableDesc(false);
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.BUCKET_CACHE_PERSISTENT_PATH_KEY, "prefetch_file_list");
    conf.setFloat(HConstants.BUCKET_CACHE_SIZE_KEY, 10);
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

    // Mock cluster metrics — give only server1 free cache so moves are directed there
    Map<ServerName, ServerMetrics> serverMetricsMap = new TreeMap<>();
    ServerMetrics sm0 = mockServerMetricsWithRegionCacheInfo(server0, regionsOnServer0, 0.0f,
      new ArrayList<>(), 0, 10);
    when(sm0.getCacheFreeSize()).thenReturn(0L);
    ServerMetrics sm1 = mockServerMetricsWithRegionCacheInfo(server1, regionsOnServer1, 0.0f,
      new ArrayList<>(), 0, 10);
    ServerMetrics sm2 = mockServerMetricsWithRegionCacheInfo(server2, regionsOnServer2, 0.0f,
      new ArrayList<>(), 0, 10);
    when(sm2.getCacheFreeSize()).thenReturn(0L);
    serverMetricsMap.put(server0, sm0);
    serverMetricsMap.put(server1, sm1);
    serverMetricsMap.put(server2, sm2);
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
    // should move at least 5 regions from server0 to balance cluster (10/0/5 -> ~5/5/5)
    assertTrue(regionsMovedFromServer0.size() >= 5,
      "Expected at least 5 moves from server0, got " + regionsMovedFromServer0.size());
  }

  /**
   * Regions on the overloaded RS report low block-cache ratio; no RS reports prefetch/historical
   * cache for those regions (so {@link CacheAwareLoadBalancer.CacheAwareCandidateGenerator} has no
   * "old server" to prefer). Another RS has ample free block cache. The balancer should still emit
   * plans that shed load from the hot RS onto the idle RS with spare cache capacity.
   */
  @Test
  public void testLowCacheRatioNoHistoricalCacheRelocatesWhenTargetHasFreeBlockCache()
    throws Exception {
    Map<ServerName, List<RegionInfo>> clusterState = new HashMap<>();
    ServerName server0 = servers.get(0);
    ServerName server1 = servers.get(1);
    ServerName server2 = servers.get(2);

    List<RegionInfo> regionsOnServer0 = randomRegions(10);
    List<RegionInfo> regionsOnServer1 = randomRegions(0);
    List<RegionInfo> regionsOnServer2 = randomRegions(5);

    clusterState.put(server0, regionsOnServer0);
    clusterState.put(server1, regionsOnServer1);
    clusterState.put(server2, regionsOnServer2);

    // Below LOW_CACHE_RATIO_FOR_RELOCATION_DEFAULT (0.35);
    ServerMetrics sm0 = mockServerMetricsWithRegionCacheInfo(server0, regionsOnServer0, 0.1f,
      new ArrayList<>(), 0, 10);
    when(sm0.getCacheFreeSize()).thenReturn(0L);
    ServerMetrics sm1 = mockServerMetricsWithRegionCacheInfo(server1, regionsOnServer1, 0.0f,
      new ArrayList<>(), 0, 10);
    // Simulates 1GB free cache space on server1
    when(sm1.getCacheFreeSize()).thenReturn(1024L * 1024 * 1024);
    ServerMetrics sm2 = mockServerMetricsWithRegionCacheInfo(server2, regionsOnServer2, 1.0f,
      new ArrayList<>(), 0, 10);
    when(sm2.getCacheFreeSize()).thenReturn(0L);

    Map<ServerName, ServerMetrics> serverMetricsMap = new TreeMap<>();
    serverMetricsMap.put(server0, sm0);
    serverMetricsMap.put(server1, sm1);
    serverMetricsMap.put(server2, sm2);
    ClusterMetrics clusterMetrics = mock(ClusterMetrics.class);
    when(clusterMetrics.getLiveServerMetrics()).thenReturn(serverMetricsMap);
    loadBalancer.updateClusterMetrics(clusterMetrics);

    assertTrue(loadBalancer.regionCacheRatioOnOldServerMap.isEmpty());

    Map<TableName, Map<ServerName, List<RegionInfo>>> loadOfAllTable =
      (Map) mockClusterServersWithTables(clusterState);
    List<RegionPlan> plans = loadBalancer.balanceCluster(loadOfAllTable);
    assertNotNull(plans);

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
    assertNotNull(targetServers.get(server1));
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
    // should move regions from server0 to server1 (old-cached regions should be among them)
    assertTrue(regionsMovedFromServer0.size() >= 4);
    assertNotNull(targetServers.get(server1));
    int oldCachedOnServer1 = 0;
    for (RegionInfo ri : oldCachedRegions) {
      if (targetServers.get(server1).contains(ri)) {
        oldCachedOnServer1++;
      }
    }
    assertTrue(oldCachedOnServer1 > 0,
      "Expected old-cached regions to move to server1, got " + oldCachedOnServer1);
  }

  @Test
  public void testThrottlingRegionBeyondThreshold() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    CacheAwareLoadBalancer balancer = new CacheAwareLoadBalancer();
    balancer.loadConf(conf);
    balancer.setClusterInfoProvider(new DummyClusterInfoProvider(conf));
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
    assertEquals(0L, balancer.getThrottleDurationMs(plan));
  }

  @Test
  public void testThrottlingRegionBelowThreshold() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setLong(CacheAwareLoadBalancer.MOVE_THROTTLING, 100);
    CacheAwareLoadBalancer balancer = new CacheAwareLoadBalancer();
    balancer.loadConf(conf);
    balancer.setClusterInfoProvider(new DummyClusterInfoProvider(conf));
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
    assertEquals(100L, balancer.getThrottleDurationMs(plan));
  }

  @Test
  public void testThrottlingCacheRatioUnknownOnTarget() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setLong(CacheAwareLoadBalancer.MOVE_THROTTLING, 100);
    CacheAwareLoadBalancer balancer = new CacheAwareLoadBalancer();
    balancer.loadConf(conf);
    balancer.setClusterInfoProvider(new DummyClusterInfoProvider(conf));
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
    assertEquals(100L, balancer.getThrottleDurationMs(plan));
  }

  @Test
  public void testThrottlingCacheRatioUnknownForRegion() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setLong(CacheAwareLoadBalancer.MOVE_THROTTLING, 100);
    CacheAwareLoadBalancer balancer = new CacheAwareLoadBalancer();
    balancer.loadConf(conf);
    balancer.setClusterInfoProvider(new DummyClusterInfoProvider(conf));
    balancer.initialize();
    ServerName server0 = servers.get(0);
    ServerName server1 = servers.get(1);
    ServerName server3 = servers.get(2);
    // No cache ratio available for region1
    RegionInfo mockedInfo = mock(RegionInfo.class);
    when(mockedInfo.getEncodedName()).thenReturn("region1");
    RegionPlan plan = new RegionPlan(mockedInfo, server1, server0);
    assertEquals(100L, balancer.getThrottleDurationMs(plan));
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
    // Plans are sorted by cache ratio on destination (descending). Verify that plans
    // for old-cached regions going to their correct servers appear before plans with no
    // cache data on destination.
    float prevRatio = Float.MAX_VALUE;
    int oldCached1Count = 0;
    int oldCached2Count = 0;
    for (RegionPlan plan : plans) {
      LOG.debug("plan region: {}, target server: {}", plan.getRegionInfo().getEncodedName(),
        plan.getDestination().getServerName());
      float ratio = 0f;
      if (
        oldCachedRegions1.contains(plan.getRegionInfo()) && server1.equals(plan.getDestination())
      ) {
        ratio = 1.0f;
        oldCached1Count++;
      } else if (
        oldCachedRegions2.contains(plan.getRegionInfo()) && server2.equals(plan.getDestination())
      ) {
        ratio = 0.8f;
        oldCached2Count++;
      }
      assertTrue(ratio <= prevRatio,
        "Plans should be sorted by cache ratio on destination (descending)");
      prevRatio = ratio;
    }
    // The cache-aware generator should move at least some old-cached regions to their
    // cached servers. Exact count depends on stochastic walk order.
    assertTrue(oldCached1Count > 0, "Some old-cached regions should move to server1");
    assertTrue(oldCached2Count > 0, "Some old-cached regions should move to server2");

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
    // should move regions from server0 to server1 (old-cached regions should be among them)
    assertTrue(regionsMovedFromServer0.size() >= 4);
    assertNotNull(targetServers.get(server1));
    assertTrue(targetServers.get(server1).size() >= 4);
    int oldCachedOnServer1 = 0;
    for (RegionInfo ri : oldCachedRegions) {
      if (targetServers.get(server1).contains(ri)) {
        oldCachedOnServer1++;
      }
    }
    assertTrue(oldCachedOnServer1 > 0,
      "Expected most old-cached regions to move to server1, got " + oldCachedOnServer1);
  }

  @Test
  public void testRegionsFullyCachedOnOldAndCurrentServers() throws Exception {
    // When regions are fully cached on BOTH the old and current server, the balancer should
    // NOT disrupt them by moving them to the old server based on potentially stale historical
    // cache data. Instead, it should still rebalance for skew.

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

    // Mock 4 regions from server0 were previously hosted on server1
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
    // Skew rebalancing should still move 5 regions from server0 to server1 to balance the
    // cluster (10 on server0, 0 on server1, 5 on server2 → target ~5 on each). But the
    // specific regions moved are not dictated by old cache data since all regions are already
    // well-cached on their current server.
    assertEquals(5, regionsMovedFromServer0.size());
    assertEquals(5, targetServers.get(server1).size());
  }

  @Test
  public void testRegionsPartiallyCachedOnOldServerAndCurrentServer() throws Exception {
    // The regions are partially cached on old server (0.6) and have lower cache on current (0.2).
    // The balancer should move regions to server1 to fix skew, and the cache-aware generator
    // guides some of those moves to be the old-cached regions.

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

    // Mock 4 regions from server0 were previously hosted on server1
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
    // Balanced state for 15 total regions on 3 servers = 5 each.
    // server0(10) → server1(0): should move 5
    assertEquals(5, regionsMovedFromServer0.size());
    assertEquals(5, targetServers.get(server1).size());
    // The cache-aware generator should move at least some old-cached regions to server1
    // (where they have better cache). Due to stochastic walk non-determinism, not all 4
    // are guaranteed to be picked over equally-viable alternatives.
    long oldCachedOnServer1 =
      targetServers.get(server1).stream().filter(oldCachedRegions::contains).count();
    assertTrue(oldCachedOnServer1 > 0, "At least some old-cached regions should move to server1");
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
