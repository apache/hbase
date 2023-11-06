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
  import static org.junit.Assert.assertFalse;
  import static org.junit.Assert.assertTrue;
  import java.util.Arrays;
  import java.util.HashMap;
  import java.util.Map;
  import org.apache.hadoop.hbase.HBaseClassTestRule;
  import org.apache.hadoop.hbase.ServerName;
  import org.apache.hadoop.hbase.testclassification.MasterTests;
  import org.apache.hadoop.hbase.testclassification.MediumTests;

  import org.apache.hadoop.conf.Configuration;
  import org.apache.hadoop.hbase.util.Pair;
  import org.junit.Before;
  import org.junit.BeforeClass;
  import org.junit.ClassRule;
  import org.junit.Test;
  import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, MediumTests.class })
public class TestCacheAwareLoadBalancerCostFunctions extends BalancerTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCacheAwareLoadBalancerCostFunctions.class);

  // Mapping of prefetch test -> expected prefetch
  private final float[] expectedPrefetch = { 0.0f, 0.0f, 0.5f, 1.0f, 0.0f, 0.572f, 0.0f, 0.075f };

  /**
   * Data set to testPrefetchCost: [test][0][0] = mapping of server to number of regions it hosts
   * [test][region + 1][0] = server that region is hosted on [test][region + 1][server + 1] =
   * prefetch of that region on server
   */
  private final int[][][] clusterRegionPrefetchMocks = new int[][][] {
    // Test 1: each region is entirely on server that hosts it
    // Cost of moving the regions in this case should be high as the regions are fully prefetched
    // on the server they are currently hosted on
    new int[][] { new int[] { 2, 1, 1 }, // Server 0 has 2, server 1 has 1 and server 2 has 1
      // region(s) hosted respectively
      new int[] { 0, 100, 0, 0 }, // region 0 is hosted and prefetched only on server 0
      new int[] { 0, 100, 0, 0 }, // region 1 is hosted and prefetched only on server 0
      new int[] { 1, 0, 100, 0 }, // region 2 is hosted and prefetched only on server 1
      new int[] { 2, 0, 0, 100 }, // region 3 is hosted and prefetched only on server 2
    },

    // Test 2: each region is prefetched completely on the server it is currently hosted on,
    // but it was also prefetched on some other server historically
    // Cost of moving the regions in this case should be high as the regions are fully prefetched
    // on the server they are currently hosted on. Although, the regions were previously hosted and
    // prefetched on some other server, since they are completely prefetched on the new server,
    // there is no need to move the regions back to the previously hosting cluster
    new int[][] { new int[] { 1, 2, 1 }, // Server 0 has 1, server 1 has 2 and server 2 has 1
      // region(s) hosted respectively
      new int[] { 0, 100, 0, 100 }, // region 0 is hosted and currently prefetched on server 0,
      // but previously prefetched completely on server 2
      new int[] { 1, 100, 100, 0 }, // region 1 is hosted and currently prefetched on server 1,
      // but previously prefetched completely on server 0
      new int[] { 1, 0, 100, 100 }, // region 2 is hosted and currently prefetched on server 1,
      // but previously prefetched on server 2
      new int[] { 2, 0, 100, 100 }, // region 3 is hosted and currently prefetched on server 2,
      // but previously prefetched on server 1
    },

    // Test 3: The regions were hosted and fully prefetched on a server but later moved to other
    // because of server crash procedure. The regions are partially prefetched on the server they
    // are currently hosted on
    new int[][] { new int[] { 1, 2, 1 }, new int[] { 0, 50, 0, 100 }, // Region 0 is currently
      // hosted and partially
      // prefetched on
      // server 0, but was fully
      // prefetched on server 2
      // previously
      new int[] { 1, 100, 50, 0 }, // Region 1 is currently hosted and partially prefetched on
      // server 1, but was fully prefetched on server 0 previously
      new int[] { 1, 0, 50, 100 }, // Region 2 is currently hosted and partially prefetched on
      // server 1, but was fully prefetched on server 2 previously
      new int[] { 2, 0, 100, 50 }, // Region 3 is currently hosted and partially prefetched on
      // server 2, but was fully prefetched on server 1 previously
    },

    // Test 4: The regions were hosted and fully prefetched on a server, but later moved to other
    // server because of server crash procedure. The regions are not at all prefetched on the server
    // they are currently hosted on
    new int[][] { new int[] { 1, 1, 2 }, new int[] { 0, 0, 0, 100 }, // Region 0 is currently hosted
      // but not prefetched on server
      // 0,
      // but was fully prefetched on
      // server 2 previously
      new int[] { 1, 100, 0, 0 }, // Region 1 is currently hosted but not prefetched on server 1,
      // but was fully prefetched on server 0 previously
      new int[] { 2, 0, 100, 0 }, // Region 2 is currently hosted but not prefetched on server 2,
      // but was fully prefetched on server 1 previously
      new int[] { 2, 100, 0, 0 }, // Region 3 is currently hosted but not prefetched on server 2,
      // but was fully prefetched on server 1 previously
    },

    // Test 5: The regions were partially prefetched on old servers, before moving to the new server
    // where also, they are partially prefetched
    new int[][] { new int[] { 2, 1, 1 }, new int[] { 0, 50, 50, 0 }, // Region 0 is hosted and
      // partially prefetched on
      // server 0, but
      // was previously hosted and
      // partially prefetched on
      // server 1
      new int[] { 0, 50, 0, 50 }, // Region 1 is hosted and partially prefetched on server 0, but
      // was previously hosted and partially prefetched on server 2
      new int[] { 1, 0, 50, 50 }, // Region 2 is hosted and partially prefetched on server 1, but
      // was previously hosted and partially prefetched on server 2
      new int[] { 2, 0, 50, 50 }, // Region 3 is hosted and partially prefetched on server 2, but
      // was previously hosted and partially prefetched on server 1
    },

    // Test 6: The regions are less prefetched on the new servers as compared to what they were
    // prefetched on the server before they were moved to the new servers
    new int[][] { new int[] { 1, 2, 1 }, new int[] { 0, 30, 70, 0 }, // Region 0 is hosted and
      // prefetched 30% on server 0,
      // but was
      // previously hosted and
      // prefetched 70% on server 1
      new int[] { 1, 70, 30, 0 }, // Region 1 is hosted and prefetched 30% on server 1, but was
      // previously hosted and prefetched 70% on server 0
      new int[] { 1, 0, 30, 70 }, // Region 2 is hosted and prefetched 30% on server 1, but was
      // previously hosted and prefetched 70% on server 2
      new int[] { 2, 0, 70, 30 }, // Region 3 is hosted and prefetched 30% on server 2, but was
      // previously hosted and prefetched 70% on server 1
    },

    // Test 7: The regions are more prefetched on the new servers as compared to what they were
    // prefetched on the server before they were moved to the new servers
    new int[][] { new int[] { 2, 1, 1 }, new int[] { 0, 80, 20, 0 }, // Region 0 is hosted and 80%
      // prefetched on server 0, but
      // was
      // previously hosted and 20%
      // prefetched on server 1
      new int[] { 0, 80, 0, 20 }, // Region 1 is hosted and 80% prefetched on server 0, but was
      // previously hosted and 20% prefetched on server 2
      new int[] { 1, 20, 80, 0 }, // Region 2 is hosted and 80% prefetched on server 1, but was
      // previously hosted and 20% prefetched on server 0
      new int[] { 2, 0, 20, 80 }, // Region 3 is hosted and 80% prefetched on server 2, but was
      // previously hosted and 20% prefetched on server 1
    },

    // Test 8: The regions are randomly assigned to the server with some regions historically
    // hosted on other region servers
    new int[][] { new int[] { 1, 2, 1 }, new int[] { 0, 34, 0, 58 }, // Region 0 is hosted and
      // partially prefetched on
      // server 0,
      // but was previously hosted
      // and partially prefetched on
      // server 2
      // current prefetch <
      // historical prefetch
      new int[] { 1, 78, 100, 0 }, // Region 1 is hosted and fully prefetched on server 1,
      // but was previously hosted and partially prefetched on server 0
      // current prefetch > historical prefetch
      new int[] { 1, 66, 66, 0 }, // Region 2 is hosted and partially prefetched on server 1,
      // but was previously hosted and partially prefetched on server 0
      // current prefetch == historical prefetch
      new int[] { 2, 0, 0, 96 }, // Region 3 is hosted and partially prefetched on server 0
      // No historical prefetch
    }, };

  private static Configuration storedConfiguration;

  @BeforeClass
  public static void saveInitialConfiguration() {
    storedConfiguration = new Configuration(conf);
  }

  @Before
  public void beforeEachTest() {
    conf = new Configuration(storedConfiguration);
    loadBalancer.setConf(conf);
  }

  @Test
  public void testVerifyPrefetchAwareSkewnessCostFunctionEnabled() {
    CacheAwareLoadBalancer lb = new CacheAwareLoadBalancer();
    lb.setConf(conf);
    assertTrue(Arrays.asList(lb.getCostFunctionNames())
      .contains(CacheAwareLoadBalancer.PrefetchAwareRegionSkewnessCostFunction.class.getSimpleName()));
  }

  @Test
  public void testVerifyPrefetchAwareSkewnessCostFunctionDisabled() {
    conf.setFloat(
      StochasticLoadBalancer.RegionCountSkewCostFunction.REGION_COUNT_SKEW_COST_KEY,
      0.0f);

    CacheAwareLoadBalancer lb = new CacheAwareLoadBalancer();
    lb.setConf(conf);

    assertFalse(Arrays.asList(lb.getCostFunctionNames())
      .contains(StochasticLoadBalancer.RegionCountSkewCostFunction.class.getSimpleName()));
  }

  @Test
  public void testVerifyPrefetchCostFunctionEnabled() {
    conf.set(BUCKET_CACHE_PERSISTENT_PATH_KEY, " /tmp/prefetch.persistence");
    CacheAwareLoadBalancer lb = new CacheAwareLoadBalancer();
    lb.setConf(conf);

    assertTrue(Arrays.asList(lb.getCostFunctionNames())
      .contains(CacheAwareLoadBalancer.PrefetchCacheCostFunction.class.getSimpleName()));
  }

  @Test
  public void testVerifyPrefetchCostFunctionDisabledByNoPersistencePathKey() {
    assertFalse(Arrays.asList(loadBalancer.getCostFunctionNames())
      .contains(CacheAwareLoadBalancer.PrefetchCacheCostFunction.class.getSimpleName()));
  }

  @Test
  public void testVerifyPrefetchCostFunctionDisabledByNoMultiplier() {
    conf.set(BUCKET_CACHE_PERSISTENT_PATH_KEY, " /tmp/prefetch.persistence");
    conf.setFloat("hbase.master.balancer.stochastic.prefetchCacheCost", 0.0f);
    assertFalse(Arrays.asList(loadBalancer.getCostFunctionNames())
      .contains(CacheAwareLoadBalancer.PrefetchCacheCostFunction.class.getSimpleName()));
  }

  @Test
  public void testPrefetchCost() {
    conf.set(BUCKET_CACHE_PERSISTENT_PATH_KEY, " /tmp/prefetch.persistence");
    CacheAwareLoadBalancer.CostFunction costFunction =
      new CacheAwareLoadBalancer.PrefetchCacheCostFunction(conf);

    for (int test = 0; test < clusterRegionPrefetchMocks.length; test++) {
      int[][] clusterRegionLocations = clusterRegionPrefetchMocks[test];
      TestCacheAwareLoadBalancerCostFunctions.MockClusterForPrefetch cluster =
        new TestCacheAwareLoadBalancerCostFunctions.MockClusterForPrefetch(
          clusterRegionLocations);
      costFunction.init(cluster);
      double cost = costFunction.cost();
      assertEquals(expectedPrefetch[test], cost, 0.01);
    }
  }

  private class MockClusterForPrefetch extends BaseLoadBalancer.Cluster {
    //private final int[][] regionServerPrefetch; // [region][server] = prefetch percent
    private final Map<Pair<Integer, Integer>, Float> regionServerPrefetch = new HashMap<>();

    public MockClusterForPrefetch(int[][] regionsArray) {
      // regions[0] is an array where index = serverIndex and value = number of regions
      super(mockClusterServersUnsorted(regionsArray[0], 1), null, null, null, null);
      Map<String, Pair<ServerName, Float>> oldPrefetchRatio = new HashMap<>();
      for (int i = 1; i < regionsArray.length; i++) {
        int regionIndex = i - 1;
        for (int j = 1; j < regionsArray[i].length; j++) {
          int serverIndex = j - 1;
          float prefetch = (float) regionsArray[i][j]/100;
          regionServerPrefetch.put(new Pair<>(regionIndex, serverIndex), prefetch);
          if (prefetch > 0.0f && serverIndex != regionsArray[i][0]) {
            // This is the historical prefetch value
            oldPrefetchRatio.put(regions[regionIndex].getEncodedName(),
              new Pair<>(servers[serverIndex], prefetch));
          }
        }
      }
      oldRegionServerPrefetchRatio = oldPrefetchRatio;
    }

    @Override
    public int getTotalRegionHFileSizeMB(int region) {
      return 1;
    }

    @Override
    protected float getRegionServerPrefetchRatio(int region, int regionServerIndex) {
      float prefetchRatio = 0.0f;

      // Get the prefetch cache ratio if the region is currently hosted on this server
      if (regionServerIndex == regionIndexToServerIndex[region]) {
        return regionServerPrefetch.get(new Pair<>(region, regionServerIndex));
      }

      // Region is not currently hosted on this server. Check if the region was prefetched on this
      // server earlier. This can happen when the server was shutdown and the cache was persisted.
      // Search using the index name and server name and not the index id and server id as these
      // ids may change when a server is marked as dead or a new server is added.
      String regionEncodedName = regions[region].getEncodedName();
      ServerName serverName = servers[regionServerIndex];
      if (
        oldRegionServerPrefetchRatio != null
          && oldRegionServerPrefetchRatio.containsKey(regionEncodedName)
      ) {
        Pair<ServerName, Float> serverPrefetchRatio =
          oldRegionServerPrefetchRatio.get(regionEncodedName);
        if (ServerName.isSameAddress(serverName, serverPrefetchRatio.getFirst())) {
          prefetchRatio = serverPrefetchRatio.getSecond();
          oldRegionServerPrefetchRatio.remove(regionEncodedName);
        }
      }
      return prefetchRatio;
    }
  }
}
