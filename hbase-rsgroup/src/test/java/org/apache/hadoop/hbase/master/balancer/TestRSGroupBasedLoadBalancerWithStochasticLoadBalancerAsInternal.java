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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.rsgroup.RSGroupBasedLoadBalancer;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfo;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Test RSGroupBasedLoadBalancer with StochasticLoadBalancer as internal balancer
 */
@Category(SmallTests.class)
public class TestRSGroupBasedLoadBalancerWithStochasticLoadBalancerAsInternal
    extends RSGroupableBalancerTestBase {
  private static RSGroupBasedLoadBalancer loadBalancer;

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    groups = new String[] { RSGroupInfo.DEFAULT_GROUP };
    servers = generateServers(3);
    groupMap = constructGroupInfo(servers, groups);
    tableDescs = constructTableDesc(false);
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.regions.slop", "0");
    conf.setFloat("hbase.master.balancer.stochastic.readRequestCost", 10000f);
    conf.set("hbase.rsgroup.grouploadbalancer.class",
        StochasticLoadBalancer.class.getCanonicalName());
    loadBalancer = new RSGroupBasedLoadBalancer(getMockedGroupInfoManager());
    loadBalancer.setMasterServices(getMockedMaster());
    loadBalancer.setConf(conf);
    loadBalancer.initialize();
  }

  private ServerLoad mockServerLoadWithReadRequests(ServerName server,
      List<HRegionInfo> regionsOnServer, long readRequestCount) {
    ServerLoad serverMetrics = mock(ServerLoad.class);
    Map<byte[], RegionLoad> regionLoadMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for(HRegionInfo info : regionsOnServer){
      RegionLoad rl = mock(RegionLoad.class);
      when(rl.getReadRequestsCount()).thenReturn(readRequestCount);
      when(rl.getWriteRequestsCount()).thenReturn(0L);
      when(rl.getMemStoreSizeMB()).thenReturn(0);
      when(rl.getStorefileSizeMB()).thenReturn(0);
      regionLoadMap.put(info.getEncodedNameAsBytes(), rl);
    }
    when(serverMetrics.getRegionsLoad()).thenReturn(regionLoadMap);
    return serverMetrics;
  }

  /**
   * Test HBASE-20791
   */
  @Test
  public void testBalanceCluster() throws HBaseIOException {
    // mock cluster State
    Map<ServerName, List<HRegionInfo>> clusterState = new HashMap<ServerName, List<HRegionInfo>>();
    ServerName serverA = servers.get(0);
    ServerName serverB = servers.get(1);
    ServerName serverC = servers.get(2);
    List<HRegionInfo> regionsOnServerA = randomRegions(3);
    List<HRegionInfo> regionsOnServerB = randomRegions(3);
    List<HRegionInfo> regionsOnServerC = randomRegions(3);
    clusterState.put(serverA, regionsOnServerA);
    clusterState.put(serverB, regionsOnServerB);
    clusterState.put(serverC, regionsOnServerC);
    // mock ClusterMetrics
    final Map<ServerName, ServerLoad> serverMetricsMap = new TreeMap<>();
    serverMetricsMap.put(serverA, mockServerLoadWithReadRequests(serverA, regionsOnServerA, 0));
    serverMetricsMap.put(serverB, mockServerLoadWithReadRequests(serverB, regionsOnServerB, 0));
    serverMetricsMap.put(serverC, mockServerLoadWithReadRequests(serverC, regionsOnServerC, 0));
    ClusterStatus clusterStatus = mock(ClusterStatus.class);
    when(clusterStatus.getServers()).thenReturn(serverMetricsMap.keySet());
    when(clusterStatus.getLoad(Mockito.any(ServerName.class)))
        .thenAnswer(new Answer<ServerLoad>() {
          @Override
          public ServerLoad answer(InvocationOnMock invocation) throws Throwable {
            return serverMetricsMap.get(invocation.getArguments()[0]);
          }
        });
    loadBalancer.setClusterStatus(clusterStatus);

    // ReadRequestCostFunction are Rate based, So doing setClusterMetrics again
    // this time, regions on serverA with more readRequestCount load
    // serverA : 1000,1000,1000
    // serverB : 0,0,0
    // serverC : 0,0,0
    // so should move two regions from serverA to serverB & serverC
    final Map<ServerName, ServerLoad> serverMetricsMap2 = new TreeMap<>();
    serverMetricsMap2.put(serverA, mockServerLoadWithReadRequests(serverA,
        regionsOnServerA, 1000));
    serverMetricsMap2.put(serverB, mockServerLoadWithReadRequests(serverB, regionsOnServerB, 0));
    serverMetricsMap2.put(serverC, mockServerLoadWithReadRequests(serverC, regionsOnServerC, 0));
    clusterStatus = mock(ClusterStatus.class);
    when(clusterStatus.getServers()).thenReturn(serverMetricsMap2.keySet());
    when(clusterStatus.getLoad(Mockito.any(ServerName.class)))
        .thenAnswer(new Answer<ServerLoad>() {
          @Override
          public ServerLoad answer(InvocationOnMock invocation) throws Throwable {
            return serverMetricsMap2.get(invocation.getArguments()[0]);
          }
        });
    loadBalancer.setClusterStatus(clusterStatus);

    List<RegionPlan> plans = loadBalancer.balanceCluster(clusterState);
    Set<HRegionInfo> regionsMoveFromServerA = new HashSet<>();
    Set<ServerName> targetServers = new HashSet<>();
    for(RegionPlan plan : plans) {
      if(plan.getSource().equals(serverA)) {
        regionsMoveFromServerA.add(plan.getRegionInfo());
        targetServers.add(plan.getDestination());
      }
    }
    // should move 2 regions from serverA, one moves to serverB, the other moves to serverC
    assertEquals(2, regionsMoveFromServerA.size());
    assertEquals(2, targetServers.size());
    assertTrue(regionsOnServerA.containsAll(regionsMoveFromServerA));
  }
}
