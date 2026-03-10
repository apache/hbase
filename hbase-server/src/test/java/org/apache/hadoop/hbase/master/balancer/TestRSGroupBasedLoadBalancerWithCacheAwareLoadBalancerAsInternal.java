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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Size;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.rsgroup.RSGroupBasedLoadBalancer;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfo;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestRSGroupBasedLoadBalancerWithCacheAwareLoadBalancerAsInternal
  extends RSGroupableBalancerTestBase {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule
    .forClass(TestRSGroupBasedLoadBalancerWithCacheAwareLoadBalancerAsInternal.class);
  private static RSGroupBasedLoadBalancer loadBalancer;

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    groups = new String[] { RSGroupInfo.DEFAULT_GROUP };
    servers = generateServers(3);
    groupMap = constructGroupInfo(servers, groups);
    tableDescs = constructTableDesc(false);
  }

  @Test(timeout = 60000)
  public void testConfigUpdateDuringBalance() throws Exception {
    float expectedOldRatioThreshold = 0.8f;
    float expectedNewRatioThreshold = 0.95f;
    long throttlingTimeMillis = 10000L;

    conf.setLong(CacheAwareLoadBalancer.MOVE_THROTTLING, throttlingTimeMillis);
    conf.setFloat(CacheAwareLoadBalancer.CACHE_RATIO_THRESHOLD, expectedOldRatioThreshold);
    conf.set(HConstants.BUCKET_CACHE_PERSISTENT_PATH_KEY, "prefetch_file_list");
    conf.set("hbase.rsgroup.grouploadbalancer.class",
      CacheAwareLoadBalancer.class.getCanonicalName());
    loadBalancer = new RSGroupBasedLoadBalancer();
    loadBalancer.setMasterServices(getMockedMaster());
    loadBalancer.initialize();

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
    loadBalancer.updateClusterMetrics(clusterMetrics);

    final Map<TableName, Map<ServerName, List<RegionInfo>>> loadOfAllTable =
      (Map) mockClusterServersWithTables(clusterState);

    // Verify initial configuration is set correctly
    CacheAwareLoadBalancer internalBalancer =
      (CacheAwareLoadBalancer) loadBalancer.getInternalBalancer();
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
          simulateBalanceOperation(loadOfAllTable, balanceStarted, configUpdateInitiated,
            actualOldRatioThresholdDuringBalance);
          return EnvironmentEdgeManager.currentTime() - start;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      // Thread 2: Simulate update_all_config / onConfigurationChange
      Future<Long> configUpdateFuture = executor.submit(() -> {
        try {
          long startTime = System.currentTimeMillis();
          simulateConfigurationUpdate(balanceStarted, configUpdateInitiated,
            expectedNewRatioThreshold);
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
      internalBalancer = (CacheAwareLoadBalancer) loadBalancer.getInternalBalancer();
      assertEquals(expectedNewRatioThreshold, internalBalancer.ratioThreshold, 0.001f);

    } finally {
      executor.shutdownNow();
    }
  }

  /**
   * Simulates HMaster.balance() operation that holds the balancer lock, performs balancing with
   * throttling, and captures the configuration state during balance.
   */
  private void simulateBalanceOperation(
    Map<TableName, Map<ServerName, List<RegionInfo>>> loadOfAllTable, CountDownLatch balanceStarted,
    CountDownLatch configUpdateInitiated, float[] actualOldRatioThresholdDuringBalance)
    throws Exception {

    synchronized (loadBalancer) {
      try {
        // Simulate beginning of HMaster.balance() mark balancing window open
        loadBalancer.onBalancingStart();
        balanceStarted.countDown();

        List<RegionPlan> plans = loadBalancer.balanceCluster(loadOfAllTable);
        if (plans != null) {
          for (RegionPlan plan : plans) {
            loadBalancer.throttle(plan, loadBalancer);
          }
        }
        // Wait until config update is initiated while balance is still in progress
        configUpdateInitiated.await();

        // Old config should still be visible during current balance run
        CacheAwareLoadBalancer currentInternal =
          (CacheAwareLoadBalancer) loadBalancer.getInternalBalancer();
        actualOldRatioThresholdDuringBalance[0] = currentInternal.ratioThreshold;

      } finally {
        loadBalancer.onBalancingComplete();
      }
    }
  }

  /**
   * Simulates configuration update via onConfigurationChange while balance is in progress. This
   * should not block waiting for balance to complete.
   */
  private void simulateConfigurationUpdate(CountDownLatch balanceStarted,
    CountDownLatch configUpdateInitiated, float expectedNewRatioThreshold) throws Exception {

    // Wait for balance to start
    balanceStarted.await();

    // Call onConfigurationChange - should NOT hang
    Configuration newConf = new Configuration(conf);
    newConf.set(HConstants.BUCKET_CACHE_PERSISTENT_PATH_KEY, "prefetch_file_list");
    newConf.setLong(CacheAwareLoadBalancer.MOVE_THROTTLING, 10000);
    newConf.setFloat(CacheAwareLoadBalancer.CACHE_RATIO_THRESHOLD, expectedNewRatioThreshold);
    loadBalancer.onConfigurationChange(newConf);
    configUpdateInitiated.countDown();
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
}
