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
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, MediumTests.class })
public class TestPrefetchCacheCostLoadBalancerFunction extends StochasticBalancerTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestPrefetchCacheCostLoadBalancerFunction.class);

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
      new int[] { 2, 0, 0, 100 }, // region 0 is hosted and prefetched only on server 2
      new int[] { 0, 100, 0, 0 }, // region 1 is hosted and prefetched only on server 0
      new int[] { 0, 100, 0, 0 }, // region 2 is hosted and prefetched only on server 0
      new int[] { 1, 0, 100, 0 }, // region 3 is hosted and prefetched only on server 1
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
      new int[] { 2, 100, 0, 0 }, // Region 1 is currently hosted but not prefetched on server 2,
                                  // but was fully prefetched on server 0 previously
      new int[] { 1, 0, 0, 100 }, // Region 2 is currently hosted but not prefetched on server 1,
                                  // but was fully prefetched on server 2 previously
      new int[] { 2, 0, 100, 0 }, // Region 3 is currently hosted but not prefetched on server 2,
                                  // but was fully prefetched on server 1 previously
    },

    // Test 5: The regions were partially prefetched on old servers, before moving to the new server
    // where also, they are partially prefetched
    new int[][] { new int[] { 2, 1, 1 }, new int[] { 1, 50, 50, 0 }, // Region 0 is hosted and
                                                                     // partially prefetched on
                                                                     // server 1, but
                                                                     // was previously hosted and
                                                                     // partially prefetched on
                                                                     // server 0
      new int[] { 2, 0, 50, 50 }, // Region 1 is hosted and partially prefetched on server 2, but
                                  // was previously hosted and partially prefetched on server 1
      new int[] { 0, 50, 0, 50 }, // Region 2 is hosted and partially prefetched on server 0, but
                                  // was previously hosted and partially prefetched on server 2
      new int[] { 0, 50, 50, 0 }, // Region 3 is hosted and partially prefetched on server 0, but
                                  // was previously hosted and partially prefetched on server 1
    },

    // Test 6: The regions are less prefetched on the new servers as compared to what they were
    // prefetched on the server before they were moved to the new servers
    new int[][] { new int[] { 1, 2, 1 }, new int[] { 0, 30, 70, 0 }, // Region 0 is hosted and
                                                                     // prefetched 30% on server 0,
                                                                     // but was
                                                                     // previously hosted and
                                                                     // prefetched 70% on server 1
      new int[] { 2, 70, 0, 30 }, // Region 1 is hosted and prefetched 30% on server 2, but was
                                  // previously hosted and prefetched 70% on server 0
      new int[] { 1, 70, 30, 0 }, // Region 2 is hosted and prefetched 30% on server 1, but was
                                  // previously hosted and prefetched 70% on server 0
      new int[] { 1, 0, 30, 70 }, // Region 3 is hosted and prefetched 30% on server 1, but was
                                  // previously hosted and prefetched 70% on server 3
    },

    // Test 7: The regions are more prefetched on the new servers as compared to what they were
    // prefetched on the server before they were moved to the new servers
    new int[][] { new int[] { 2, 1, 1 }, new int[] { 2, 0, 20, 80 }, // Region 0 is hosted and 80%
                                                                     // prefetched on server 2, but
                                                                     // was
                                                                     // previously hosted and 20%
                                                                     // prefetched on server 1
      new int[] { 2, 20, 0, 80 }, // Region 1 is hosted and 80% prefetched on server 2, but was
                                  // previously hosted and 20% prefetched on server 0
      new int[] { 1, 20, 80, 0 }, // Region 2 is hosted and 80% prefetched on server 1, but was
                                  // previously hosted and 20% prefetched on server 0
      new int[] { 0, 80, 20, 0 }, // Region 3 is hosted and 80% prefetched on server 0, but was
                                  // previously hosted and 20% prefetched on server 1
    },

    // Test 8: The regions are randomly assigned to the server with some regions historically
    // hosted on other region servers
    new int[][] { new int[] { 1, 2, 1 }, new int[] { 1, 0, 34, 58 }, // Region 0 is hosted and
                                                                     // partially prefetched on
                                                                     // server 1,
                                                                     // but was previously hosted
                                                                     // and partially prefetched on
                                                                     // server 2
                                                                     // current prefetch <
                                                                     // historical prefetch
      new int[] { 2, 78, 0, 100 }, // Region 1 is hosted and fully prefetched on server 2,
                                   // but was previously hosted and partially prefetched on server 0
                                   // current prefetch > historical prefetch
      new int[] { 1, 66, 66, 0 }, // Region 2 is hosted and partially prefetched on server 1,
                                  // but was previously hosted and partially prefetched on server 0
                                  // current prefetch == historical prefetch
      new int[] { 0, 96, 0, 0 }, // Region 3 is hosted and partially prefetched on server 0
                                 // No historical prefetch
    }, };

  @Test
  public void testVerifyPrefetchCostFunctionEnabled() {
    conf.set(HConstants.PREFETCH_PERSISTENCE_PATH_KEY, "/tmp/prefetch.persistence");

    StochasticLoadBalancer lb = new StochasticLoadBalancer();
    lb.loadConf(conf);

    assertTrue(Arrays.asList(lb.getCostFunctionNames())
      .contains(PrefetchCacheCostFunction.class.getSimpleName()));
  }

  @Test
  public void testVerifyPrefetchCostFunctionDisabled() {
    assertFalse(Arrays.asList(loadBalancer.getCostFunctionNames())
      .contains(PrefetchCacheCostFunction.class.getSimpleName()));
  }

  @Test
  public void testPrefetchCost() throws Exception {
    conf.set(HConstants.PREFETCH_PERSISTENCE_PATH_KEY, "/tmp/prefetch.persistence");
    CostFunction costFunction = new PrefetchCacheCostFunction(conf);

    for (int test = 0; test < clusterRegionPrefetchMocks.length; test++) {
      int[][] clusterRegionLocations = clusterRegionPrefetchMocks[test];
      TestPrefetchCacheCostLoadBalancerFunction.MockClusterForPrefetch cluster =
        new TestPrefetchCacheCostLoadBalancerFunction.MockClusterForPrefetch(
          clusterRegionLocations);
      costFunction.prepare(cluster);
      double cost = costFunction.cost();
      assertEquals(expectedPrefetch[test], cost, 0.01);
    }
  }

  private class MockClusterForPrefetch extends BalancerClusterState {
    private int[][] regionServerPrefetch = null; // [region][server] = prefetch percent

    public MockClusterForPrefetch(int[][] regionsArray) {
      // regions[0] is an array where index = serverIndex and value = number of regions
      super(mockClusterServers(regionsArray[0], 1), null, null, null, null);
      regionServerPrefetch = new int[regionsArray.length - 1][];
      Map<String, Map<Address, Float>> historicalPrefetchRatio =
        new HashMap<String, Map<Address, Float>>();
      for (int i = 1; i < regionsArray.length; i++) {
        int regionIndex = i - 1;
        regionServerPrefetch[regionIndex] = new int[regionsArray[i].length - 1];
        regionIndexToServerIndex[regionIndex] = regionsArray[i][0];
        for (int j = 1; j < regionsArray[i].length; j++) {
          int serverIndex = j - 1;
          regionServerPrefetch[regionIndex][serverIndex] = regionsArray[i][j];
          if (regionsArray[i][j] > 0 && serverIndex != regionsArray[i][0]) {
            // This is the historical prefetch value
            Map<Address, Float> historicalPrefetch = new HashMap<>();
            historicalPrefetch.put(servers[serverIndex].getAddress(), (float) regionsArray[i][j]);
            historicalPrefetchRatio.put(regions[regionIndex].getRegionNameAsString(),
              historicalPrefetch);
          }
        }
      }
      historicalRegionServerPrefetchRatio = historicalPrefetchRatio;
    }

    @Override
    public int getRegionSizeMB(int region) {
      return 1;
    }

    @Override
    protected float getRegionServerPrefetchRatio(int region, int regionServerIndex) {
      float prefetchRatio = 0.0f;

      // Get the prefetch cache ratio if the region is currently hosted on this server
      if (regionServerIndex == regionIndexToServerIndex[region]) {
        return regionServerPrefetch[region][regionServerIndex];
      }

      // Region is not currently hosted on this server. Check if the region was prefetched on this
      // server earlier. This can happen when the server was shutdown and the cache was persisted.
      // Search using the index name and server name and not the index id and server id as these
      // ids may change when a server is marked as dead or a new server is added.
      String regionNameAsString = regions[region].getRegionNameAsString();
      Address serverAddress = servers[regionServerIndex].getAddress();
      if (
        historicalRegionServerPrefetchRatio != null
          && historicalRegionServerPrefetchRatio.containsKey(regionNameAsString)
      ) {
        Map<Address, Float> serverPrefetchRatio =
          historicalRegionServerPrefetchRatio.get(regionNameAsString);
        if (serverPrefetchRatio.containsKey(serverAddress)) {
          prefetchRatio = serverPrefetchRatio.get(serverAddress);
          // The old prefetch cache ratio has been accounted for and hence, clear up this
          // information
          // as it is not needed anymore
          historicalRegionServerPrefetchRatio.remove(regionNameAsString, serverPrefetchRatio);
        }
      }
      return prefetchRatio;
    }
  }
}
