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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.rsgroup.RSGroupBasedLoadBalancer;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfo;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(LargeTests.TAG)
public class TestRSGroupBasedLoadBalancerWithCacheAwareLoadBalancerAsInternal
  extends RSGroupableBalancerTestBase {

  private static final Logger LOG =
    LoggerFactory.getLogger(TestRSGroupBasedLoadBalancerWithCacheAwareLoadBalancerAsInternal.class);

  private static RSGroupBasedLoadBalancer loadBalancer;

  @BeforeAll
  public static void beforeAllTests() throws Exception {
    groups = new String[] { RSGroupInfo.DEFAULT_GROUP };
    servers = generateServers(3);
    groupMap = constructGroupInfo(servers, groups);
    tableDescs = constructTableDesc(false);
    Configuration cong = HBaseConfiguration.create();
    conf.set(HConstants.BUCKET_CACHE_PERSISTENT_PATH_KEY, "prefetch_file_list");
    conf.set("hbase.rsgroup.grouploadbalancer.class",
      CacheAwareLoadBalancer.class.getCanonicalName());
    loadBalancer = new RSGroupBasedLoadBalancer();
    loadBalancer.setMasterServices(getMockedMaster());
    loadBalancer.initialize();
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
    conf.set(HConstants.BUCKET_CACHE_PERSISTENT_PATH_KEY, "prefetch_file_list");
    conf.set("hbase.rsgroup.grouploadbalancer.class",
      CacheAwareLoadBalancer.class.getCanonicalName());
    RSGroupBasedLoadBalancer balancer = new RSGroupBasedLoadBalancer();
    balancer.setMasterServices(getMockedMaster());
    balancer.initialize();

    ServerName server0 = servers.get(0);
    ServerName server1 = servers.get(1);
    Pair<ServerName, Float> regionRatio = new Pair<>();
    regionRatio.setFirst(server0);
    regionRatio.setSecond(1.0f);
    CacheAwareLoadBalancer internalBalancer =
      (CacheAwareLoadBalancer) balancer.getInternalBalancer();
    internalBalancer.regionCacheRatioOnOldServerMap.put("region1", regionRatio);
    RegionInfo mockedInfo = mock(RegionInfo.class);
    when(mockedInfo.getEncodedName()).thenReturn("region1");
    RegionPlan plan = new RegionPlan(mockedInfo, server1, server0);
    long startTime = EnvironmentEdgeManager.currentTime();
    synchronized (balancer) {
      balancer.throttle(plan);
    }
    long endTime = EnvironmentEdgeManager.currentTime();
    assertTrue((endTime - startTime) < 10);
  }

  @Test
  public void testThrottlingRegionBelowThreshold() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.BUCKET_CACHE_PERSISTENT_PATH_KEY, "prefetch_file_list");
    conf.setLong(CacheAwareLoadBalancer.MOVE_THROTTLING, 100);
    conf.set("hbase.rsgroup.grouploadbalancer.class",
      CacheAwareLoadBalancer.class.getCanonicalName());
    RSGroupBasedLoadBalancer loadBalancer = new RSGroupBasedLoadBalancer();
    loadBalancer.setMasterServices(getMockedMaster());
    loadBalancer.initialize();
    CacheAwareLoadBalancer internalBalancer =
      (CacheAwareLoadBalancer) loadBalancer.getInternalBalancer();
    internalBalancer.loadConf(conf);

    ServerName server0 = servers.get(0);
    ServerName server1 = servers.get(1);
    Pair<ServerName, Float> regionRatio = new Pair<>();
    regionRatio.setFirst(server0);
    regionRatio.setSecond(0.1f);
    internalBalancer = (CacheAwareLoadBalancer) loadBalancer.getInternalBalancer();
    internalBalancer.regionCacheRatioOnOldServerMap.put("region1", regionRatio);
    RegionInfo mockedInfo = mock(RegionInfo.class);
    when(mockedInfo.getEncodedName()).thenReturn("region1");
    RegionPlan plan = new RegionPlan(mockedInfo, server1, server0);
    long startTime = EnvironmentEdgeManager.currentTime();
    synchronized (loadBalancer) {
      loadBalancer.throttle(plan);
    }
    long endTime = EnvironmentEdgeManager.currentTime();
    assertTrue((endTime - startTime) >= 100);
  }

  @Test
  public void testThrottlingCacheRatioUnknownOnTarget() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.BUCKET_CACHE_PERSISTENT_PATH_KEY, "prefetch_file_list");
    conf.setLong(CacheAwareLoadBalancer.MOVE_THROTTLING, 100);
    conf.set("hbase.rsgroup.grouploadbalancer.class",
      CacheAwareLoadBalancer.class.getCanonicalName());
    RSGroupBasedLoadBalancer loadBalancer = new RSGroupBasedLoadBalancer();
    loadBalancer.setMasterServices(getMockedMaster());
    loadBalancer.initialize();
    CacheAwareLoadBalancer internalBalancer =
      (CacheAwareLoadBalancer) loadBalancer.getInternalBalancer();
    internalBalancer.loadConf(conf);

    ServerName server0 = servers.get(0);
    ServerName server1 = servers.get(1);
    ServerName server3 = servers.get(2);
    // setting region cache ratio 100% on server 3, though this is not the target in the region plan
    Pair<ServerName, Float> regionRatio = new Pair<>();
    regionRatio.setFirst(server3);
    regionRatio.setSecond(1.0f);
    internalBalancer = (CacheAwareLoadBalancer) loadBalancer.getInternalBalancer();
    internalBalancer.regionCacheRatioOnOldServerMap.put("region1", regionRatio);
    RegionInfo mockedInfo = mock(RegionInfo.class);
    when(mockedInfo.getEncodedName()).thenReturn("region1");
    RegionPlan plan = new RegionPlan(mockedInfo, server1, server0);
    long startTime = EnvironmentEdgeManager.currentTime();
    synchronized (loadBalancer) {
      loadBalancer.throttle(plan);
    }
    long endTime = EnvironmentEdgeManager.currentTime();
    assertTrue((endTime - startTime) >= 100);
  }

  @Test
  public void testThrottlingCacheRatioUnknownForRegion() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.BUCKET_CACHE_PERSISTENT_PATH_KEY, "prefetch_file_list");
    conf.setLong(CacheAwareLoadBalancer.MOVE_THROTTLING, 100);
    conf.set("hbase.rsgroup.grouploadbalancer.class",
      CacheAwareLoadBalancer.class.getCanonicalName());
    RSGroupBasedLoadBalancer loadBalancer = new RSGroupBasedLoadBalancer();
    loadBalancer.setMasterServices(getMockedMaster());
    loadBalancer.initialize();
    CacheAwareLoadBalancer internalBalancer =
      (CacheAwareLoadBalancer) loadBalancer.getInternalBalancer();
    internalBalancer.loadConf(conf);

    ServerName server0 = servers.get(0);
    ServerName server1 = servers.get(1);
    ServerName server3 = servers.get(2);
    // No cache ratio available for region1
    RegionInfo mockedInfo = mock(RegionInfo.class);
    when(mockedInfo.getEncodedName()).thenReturn("region1");
    RegionPlan plan = new RegionPlan(mockedInfo, server1, server0);
    long startTime = EnvironmentEdgeManager.currentTime();
    synchronized (loadBalancer) {
      loadBalancer.throttle(plan);
    }
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

  @Timeout(60)
  @Test
  public void testConfigUpdateDuringBalance() throws Exception {
    float expectedOldRatioThreshold = 0.8f;
    float expectedNewRatioThreshold = 0.95f;
    long throttlingTimeMillis = 10000;

    conf = HBaseConfiguration.create();
    conf.setLong(CacheAwareLoadBalancer.MOVE_THROTTLING, throttlingTimeMillis);
    conf.setFloat(CacheAwareLoadBalancer.CACHE_RATIO_THRESHOLD, expectedOldRatioThreshold);
    conf.set(HConstants.BUCKET_CACHE_PERSISTENT_PATH_KEY, "prefetch_file_list");
    conf.set("hbase.rsgroup.grouploadbalancer.class",
      CacheAwareLoadBalancer.class.getCanonicalName());

    RSGroupBasedLoadBalancer balancer = new RSGroupBasedLoadBalancer();
    balancer.setMasterServices(getMockedMaster());
    balancer.initialize();

    Map<ServerName, List<RegionInfo>> clusterState = new HashMap<>();
    ServerName server0 = servers.get(0);
    ServerName server1 = servers.get(1);
    ServerName server2 = servers.get(2);

    // Setup cluster: all 3 regions on server0 (unbalanced)
    List<RegionInfo> regionsOnServer0 = randomRegions(3);
    List<RegionInfo> regionsOnServer1 = randomRegions(0);
    List<RegionInfo> regionsOnServer2 = randomRegions(0);

    clusterState.put(server0, regionsOnServer0);
    clusterState.put(server1, regionsOnServer1);
    clusterState.put(server2, regionsOnServer2);

    // Mock metrics: NO cache info for any region = all will be throttled
    Map<ServerName, ServerMetrics> serverMetricsMap = new TreeMap<>();
    serverMetricsMap.put(server0, mockServerMetricsWithRegionCacheInfo(server0, regionsOnServer0,
      0.0f, new ArrayList<>(), 0, 10));
    serverMetricsMap.put(server1, mockServerMetricsWithRegionCacheInfo(server1, regionsOnServer1,
      0.0f, new ArrayList<>(), 0, 10));
    serverMetricsMap.put(server2, mockServerMetricsWithRegionCacheInfo(server2, regionsOnServer2,
      0.0f, new ArrayList<>(), 0, 10));

    ClusterMetrics clusterMetrics = mock(ClusterMetrics.class);
    when(clusterMetrics.getLiveServerMetrics()).thenReturn(serverMetricsMap);
    balancer.updateClusterMetrics(clusterMetrics);

    final Map<TableName, Map<ServerName, List<RegionInfo>>> loadOfAllTable =
      (Map) mockClusterServersWithTables(clusterState);

    // Verify initial configuration is set correctly
    CacheAwareLoadBalancer internalBalancer =
      (CacheAwareLoadBalancer) balancer.getInternalBalancer();
    assertEquals(expectedOldRatioThreshold, internalBalancer.ratioThreshold, 0.001f);

    CountDownLatch balanceStarted = new CountDownLatch(1);
    CountDownLatch configUpdateInitiated = new CountDownLatch(1);
    long[] configUpdateDuration = new long[1];
    long[] balanceDuration = new long[1];

    // Actual old ratio threshold used during balance
    float[] actualOldRatioThresholdDuringBalance = new float[1];

    ExecutorService executor = Executors.newFixedThreadPool(2);

    try {
      // Thread 1 Simulate similar flow to HMaster.balance() which holds synchronized(balancer) for
      // the duration of balance
      Future<Long> balanceFuture = executor.submit(() -> {
        try {
          long start = EnvironmentEdgeManager.currentTime();
          synchronized (balancer) {
            try {
              // Simulate beginning of HMaster.balance() mark balancing window open
              balancer.onBalancingStart();
              balanceStarted.countDown();

              List<RegionPlan> plans = balancer.balanceCluster(loadOfAllTable);
              if (plans != null) {
                for (RegionPlan plan : plans) {
                  balancer.throttle(plan);
                }
              }
              // Wait until config update is initiated while balance is still in progress
              configUpdateInitiated.await();

              // Old config should still be visible during current balance run
              CacheAwareLoadBalancer currentInternal =
                (CacheAwareLoadBalancer) balancer.getInternalBalancer();
              actualOldRatioThresholdDuringBalance[0] = currentInternal.ratioThreshold;

            } finally {
              balancer.onBalancingComplete();
            }
          }
          return EnvironmentEdgeManager.currentTime() - start;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      // Thread 2: Simulate update_all_config / onConfigurationChange
      Future<Long> configUpdateFuture = executor.submit(() -> {
        try {
          long startTime = System.currentTimeMillis();
          // Wait for balance to start
          balanceStarted.await();

          // Call onConfigurationChange - should NOT hang
          Configuration newConf = new Configuration(conf);
          newConf.set(HConstants.BUCKET_CACHE_PERSISTENT_PATH_KEY, "prefetch_file_list");
          newConf.setLong(CacheAwareLoadBalancer.MOVE_THROTTLING, 10000);
          newConf.setFloat(CacheAwareLoadBalancer.CACHE_RATIO_THRESHOLD, expectedNewRatioThreshold);
          balancer.onConfigurationChange(newConf);
          configUpdateInitiated.countDown();

          return System.currentTimeMillis() - startTime;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      // Wait for both threads to complete
      configUpdateDuration[0] = configUpdateFuture.get();
      balanceDuration[0] = balanceFuture.get();

      // Verify that config update didn't hang/timeout waiting for balance
      assertTrue(configUpdateDuration[0] < balanceDuration[0]);

      // Verify that ratio threshold used during balance is stll the old
      assertEquals(expectedOldRatioThreshold, actualOldRatioThresholdDuringBalance[0], 0.001f);

      // Verify that config updated successfully after balance completed
      internalBalancer = (CacheAwareLoadBalancer) balancer.getInternalBalancer();
      assertEquals(expectedNewRatioThreshold, internalBalancer.ratioThreshold, 0.001f);

    } finally {
      executor.shutdownNow();
    }
  }
}
