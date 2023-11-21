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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, SmallTests.class })
public class TestOverallSkewChecker {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestOverallSkewChecker.class);

  private final ServerName sn1 = ServerName.valueOf("host1,5000,1111");
  private final ServerName sn2 = ServerName.valueOf("host2,5000,2222");
  private final ServerName sn3 = ServerName.valueOf("host3,5000,3333");
  private final TableName t1 = TableName.valueOf("table1");
  private final TableName t2 = TableName.valueOf("table2");
  private final TableName t3 = TableName.valueOf("table3");

  @Test
  public void testCheckSkew() {
    OverallSkewChecker overallSkewChecker = new OverallSkewChecker(HBaseConfiguration.create());
    Map<String, Deque<BalancerRegionLoad>> loads = new HashMap<>();
    Map<TableName, Map<ServerName, List<RegionInfo>>> tableLoads =
      mockTwoTableSkewClusterLoad(loads);
    assertTrue(overallSkewChecker.needsCoarseBalance(tableLoads));
  }

  @Test
  public void testCheckNotSkew() {
    OverallSkewChecker overallSkewChecker = new OverallSkewChecker(HBaseConfiguration.create());
    Map<String, Deque<BalancerRegionLoad>> loads = new HashMap<>();
    Map<TableName, Map<ServerName, List<RegionInfo>>> tableLoads =
      mockOneTableSkewClusterLoad(loads);
    assertFalse(overallSkewChecker.needsCoarseBalance(tableLoads));
  }

  private Map<TableName, Map<ServerName, List<RegionInfo>>>
    mockOneTableSkewClusterLoad(Map<String, Deque<BalancerRegionLoad>> loads) {
    Map<TableName, Map<ServerName, List<RegionInfo>>> allTableLoads = new HashMap<>();
    allTableLoads.put(t1, mockServerRegions(t1, 10, 5, 3, loads, 80000L));
    allTableLoads.put(t2, mockServerRegions(t2, 5, 5, 5, loads, 500L));
    allTableLoads.put(t3, mockServerRegions(t3, 3, 2, 1, loads, 100L));
    return allTableLoads;
  }

  private Map<TableName, Map<ServerName, List<RegionInfo>>>
    mockTwoTableSkewClusterLoad(Map<String, Deque<BalancerRegionLoad>> loads) {
    Map<TableName, Map<ServerName, List<RegionInfo>>> allTableLoads = new HashMap<>();
    allTableLoads.put(t1, mockServerRegions(t1, 10, 5, 3, loads, 80000L));
    allTableLoads.put(t2, mockServerRegions(t2, 5, 1, 2, loads, 500L));
    allTableLoads.put(t3, mockServerRegions(t3, 3, 2, 1, loads, 100L));
    return allTableLoads;
  }

  protected List<RegionInfo> mockTableRegions(TableName tableName, int count,
    Map<String, Deque<BalancerRegionLoad>> loads, long baseLoad) {
    return mockTableRegions(tableName, count, loads, baseLoad, null);
  }

  protected List<RegionInfo> mockTableRegions(TableName tableName, int count,
    Map<String, Deque<BalancerRegionLoad>> loads, long baseLoad, String baseRegionName) {
    List<RegionInfo> regions = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      RegionInfo mockRegion = mockRegionInfo(
        tableName.getNameAsString() + (baseRegionName == null ? "_region_" : baseRegionName) + i,
        tableName);
      regions.add(mockRegion);
      Deque<BalancerRegionLoad> BalancerRegionLoads =
        loads.computeIfAbsent(mockRegion.getEncodedName(), k -> new ArrayDeque<>());
      BalancerRegionLoads.add(mockBalancerRegionLoad(0));
      BalancerRegionLoads.add(mockBalancerRegionLoad(baseLoad + i));
    }
    return regions;
  }

  protected RegionInfo mockRegionInfo(String encodeName, TableName tableName) {
    RegionInfo regionInfo = mock(RegionInfo.class);
    when(regionInfo.getEncodedName()).thenReturn(encodeName);
    when(regionInfo.getTable()).thenReturn(tableName);
    return regionInfo;
  }

  protected Map<ServerName, List<RegionInfo>> mockServerRegions(TableName tableName, int s1Count,
    int s2Count, int s3Count, Map<String, Deque<BalancerRegionLoad>> loads, long baseLoad) {
    Map<ServerName, List<RegionInfo>> serverRegions = new HashMap<>(3);
    serverRegions.put(sn1, mockTableRegions(tableName, s1Count, loads, baseLoad));
    serverRegions.put(sn2, mockTableRegions(tableName, s2Count, loads, baseLoad));
    serverRegions.put(sn3, mockTableRegions(tableName, s3Count, loads, baseLoad));
    return serverRegions;
  }

  protected BalancerRegionLoad mockBalancerRegionLoad(long load) {
    BalancerRegionLoad rl = mock(BalancerRegionLoad.class);
    when(rl.getReadRequestsCount()).thenReturn(load);
    when(rl.getWriteRequestsCount()).thenReturn(load);
    return rl;
  }
}
