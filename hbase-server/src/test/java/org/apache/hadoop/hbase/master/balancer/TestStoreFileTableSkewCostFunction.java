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

import static org.apache.hadoop.hbase.master.balancer.CandidateGeneratorTestUtil.createMockBalancerClusterState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Size;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({ MasterTests.class, SmallTests.class })
public class TestStoreFileTableSkewCostFunction {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestStoreFileTableSkewCostFunction.class);

  private static final TableName DEFAULT_TABLE = TableName.valueOf("testTable");
  private static final Map<Long, Integer> REGION_TO_STORE_FILE_SIZE_MB = new HashMap<>();

  /**
   * Tests that a uniform store file distribution (single table) across servers results in zero
   * cost.
   */
  @Test
  public void testUniformDistribution() {
    ServerName server1 = ServerName.valueOf("server1.example.org", 1234, 1L);
    ServerName server2 = ServerName.valueOf("server2.example.org", 1234, 1L);
    ServerName server3 = ServerName.valueOf("server3.example.org", 1234, 1L);
    ServerName server4 = ServerName.valueOf("server4.example.org", 1234, 1L);

    Map<ServerName, List<RegionInfo>> serverToRegions = new HashMap<>();
    serverToRegions.put(server1, Arrays.asList(createMockRegionInfo(10), createMockRegionInfo(10)));
    serverToRegions.put(server2, Arrays.asList(createMockRegionInfo(10), createMockRegionInfo(10)));
    serverToRegions.put(server3, Arrays.asList(createMockRegionInfo(10), createMockRegionInfo(10)));
    serverToRegions.put(server4, Arrays.asList(createMockRegionInfo(10), createMockRegionInfo(10)));

    BalancerClusterState clusterState = createMockBalancerClusterState(serverToRegions);
    DummyBalancerClusterState state = new DummyBalancerClusterState(clusterState);

    StoreFileTableSkewCostFunction costFunction =
      new StoreFileTableSkewCostFunction(new Configuration());
    costFunction.prepare(state);
    double cost = costFunction.cost();

    // Expect zero cost since all regions (from the same table) are balanced.
    assertEquals("Uniform distribution should yield zero cost", 0.0, cost, 1e-6);
  }

  /**
   * Tests that a skewed store file distribution (single table) results in a positive cost.
   */
  @Test
  public void testSkewedDistribution() {
    ServerName server1 = ServerName.valueOf("server1.example.org", 1234, 1L);
    ServerName server2 = ServerName.valueOf("server2.example.org", 1234, 1L);
    ServerName server3 = ServerName.valueOf("server3.example.org", 1234, 1L);
    ServerName server4 = ServerName.valueOf("server4.example.org", 1234, 1L);

    Map<ServerName, List<RegionInfo>> serverToRegions = new HashMap<>();
    // Three servers get regions with 10 store files each,
    // while one server gets regions with 30 store files each.
    serverToRegions.put(server1, Arrays.asList(createMockRegionInfo(10), createMockRegionInfo(10)));
    serverToRegions.put(server2, Arrays.asList(createMockRegionInfo(10), createMockRegionInfo(10)));
    serverToRegions.put(server3, Arrays.asList(createMockRegionInfo(10), createMockRegionInfo(10)));
    serverToRegions.put(server4, Arrays.asList(createMockRegionInfo(30), createMockRegionInfo(30)));

    BalancerClusterState clusterState = createMockBalancerClusterState(serverToRegions);
    DummyBalancerClusterState state = new DummyBalancerClusterState(clusterState);

    StoreFileTableSkewCostFunction costFunction =
      new StoreFileTableSkewCostFunction(new Configuration());
    costFunction.prepare(state);
    double cost = costFunction.cost();

    // Expect a positive cost because the distribution is skewed.
    assertTrue("Skewed distribution should yield a positive cost", cost > 0.0);
  }

  /**
   * Tests that an empty cluster (no servers/regions) is handled gracefully.
   */
  @Test
  public void testEmptyDistribution() {
    Map<ServerName, List<RegionInfo>> serverToRegions = new HashMap<>();

    BalancerClusterState clusterState = createMockBalancerClusterState(serverToRegions);
    DummyBalancerClusterState state = new DummyBalancerClusterState(clusterState);

    StoreFileTableSkewCostFunction costFunction =
      new StoreFileTableSkewCostFunction(new Configuration());
    costFunction.prepare(state);
    double cost = costFunction.cost();

    // Expect zero cost when there is no load.
    assertEquals("Empty distribution should yield zero cost", 0.0, cost, 1e-6);
  }

  /**
   * Tests that having multiple tables results in a positive cost when each table's regions are not
   * balanced across servers – even if the overall load per server is balanced.
   */
  @Test
  public void testMultipleTablesDistribution() {
    // Two servers.
    ServerName server1 = ServerName.valueOf("server1.example.org", 1234, 1L);
    ServerName server2 = ServerName.valueOf("server2.example.org", 1234, 1L);

    // Define two tables.
    TableName table1 = TableName.valueOf("testTable1");
    TableName table2 = TableName.valueOf("testTable2");

    // For table1, all regions are on server1.
    // For table2, all regions are on server2.
    Map<ServerName, List<RegionInfo>> serverToRegions = new HashMap<>();
    serverToRegions.put(server1,
      Arrays.asList(createMockRegionInfo(table1, 10), createMockRegionInfo(table1, 10)));
    serverToRegions.put(server2,
      Arrays.asList(createMockRegionInfo(table2, 10), createMockRegionInfo(table2, 10)));

    // Although each server gets 20 MB overall, table1 and table2 are not balanced across servers.
    BalancerClusterState clusterState = createMockBalancerClusterState(serverToRegions);
    DummyBalancerClusterState state = new DummyBalancerClusterState(clusterState);

    StoreFileTableSkewCostFunction costFunction =
      new StoreFileTableSkewCostFunction(new Configuration());
    costFunction.prepare(state);
    double cost = costFunction.cost();

    // Expect a positive cost because the skew is computed per table.
    assertTrue("Multiple table distribution should yield a positive cost", cost > 0.0);
  }

  /**
   * Helper method to create a RegionInfo for the default table with the given store file size.
   */
  private static RegionInfo createMockRegionInfo(int storeFileSizeMb) {
    return createMockRegionInfo(DEFAULT_TABLE, storeFileSizeMb);
  }

  /**
   * Helper method to create a RegionInfo for a specified table with the given store file size.
   */
  private static RegionInfo createMockRegionInfo(TableName table, int storeFileSizeMb) {
    long regionId = new Random().nextLong();
    REGION_TO_STORE_FILE_SIZE_MB.put(regionId, storeFileSizeMb);
    return RegionInfoBuilder.newBuilder(table).setStartKey(generateRandomByteArray(4))
      .setEndKey(generateRandomByteArray(4)).setReplicaId(0).setRegionId(regionId).build();
  }

  private static byte[] generateRandomByteArray(int n) {
    byte[] byteArray = new byte[n];
    new Random().nextBytes(byteArray);
    return byteArray;
  }

  /**
   * A simplified BalancerClusterState which ensures we provide the intended test RegionMetrics data
   * when balancing this cluster
   */
  private static class DummyBalancerClusterState extends BalancerClusterState {
    private final RegionInfo[] testRegions;

    DummyBalancerClusterState(BalancerClusterState bcs) {
      super(bcs.clusterState, null, null, null, null);
      this.testRegions = bcs.regions;
    }

    @Override
    Deque<BalancerRegionLoad>[] getRegionLoads() {
      @SuppressWarnings("unchecked")
      Deque<BalancerRegionLoad>[] loads = new Deque[testRegions.length];
      for (int i = 0; i < testRegions.length; i++) {
        Deque<BalancerRegionLoad> dq = new ArrayDeque<>();
        dq.add(new BalancerRegionLoad(createMockRegionMetrics(testRegions[i])) {
        });
        loads[i] = dq;
      }
      return loads;
    }
  }

  /**
   * Creates a mocked RegionMetrics for the given region.
   */
  private static RegionMetrics createMockRegionMetrics(RegionInfo regionInfo) {
    RegionMetrics regionMetrics = Mockito.mock(RegionMetrics.class);

    // Important
    int storeFileSizeMb = REGION_TO_STORE_FILE_SIZE_MB.get(regionInfo.getRegionId());
    when(regionMetrics.getRegionSizeMB()).thenReturn(new Size(storeFileSizeMb, Size.Unit.MEGABYTE));
    when(regionMetrics.getStoreFileSize())
      .thenReturn(new Size(storeFileSizeMb, Size.Unit.MEGABYTE));

    // Not important
    when(regionMetrics.getReadRequestCount()).thenReturn(0L);
    when(regionMetrics.getWriteRequestCount()).thenReturn(0L);
    when(regionMetrics.getMemStoreSize()).thenReturn(new Size(0, Size.Unit.MEGABYTE));
    when(regionMetrics.getCurrentRegionCachedRatio()).thenReturn(0.0f);
    return regionMetrics;
  }
}
