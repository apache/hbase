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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.LogEntry;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.namequeues.BalancerRejectionDetails;
import org.apache.hadoop.hbase.namequeues.request.NamedQueueGetRequest;
import org.apache.hadoop.hbase.namequeues.response.NamedQueueGetResponse;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RecentLogs;

/**
 * Test BalancerRejection ring buffer using namedQueue interface
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestBalancerRejection extends StochasticBalancerTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBalancerRejection.class);

  static class MockCostFunction extends CostFunction {
    public static double mockCost;

    public MockCostFunction(Configuration c) {
    }

    @Override
    protected double cost() {
      return mockCost;
    }

    @Override
    float getMultiplier() {
      return 1;
    }
  }

  @Test
  public void testBalancerRejections() throws Exception {
    try {
      // enabled balancer rejection recording
      conf.setBoolean(BaseLoadBalancer.BALANCER_REJECTION_BUFFER_ENABLED, true);
      conf.set(StochasticLoadBalancer.COST_FUNCTIONS_COST_FUNCTIONS_KEY,
        MockCostFunction.class.getName());
      MasterServices services = mock(MasterServices.class);
      when(services.getConfiguration()).thenReturn(conf);
      MasterClusterInfoProvider provider = new MasterClusterInfoProvider(services);
      loadBalancer.setClusterInfoProvider(provider);
      loadBalancer.onConfigurationChange(conf);
      // Simulate 2 servers with 5 regions.
      Map<ServerName, List<RegionInfo>> servers = mockClusterServers(new int[] { 5, 5 });
      Map<TableName, Map<ServerName, List<RegionInfo>>> LoadOfAllTable =
        (Map) mockClusterServersWithTables(servers);

      // Reject case 1: Total cost < 0
      MockCostFunction.mockCost = -Double.MAX_VALUE;
      // Since the Balancer was rejected, there should not be any plans
      Assert.assertNull(loadBalancer.balanceCluster(LoadOfAllTable));

      // Reject case 2: Cost < minCostNeedBalance
      MockCostFunction.mockCost = 1;
      conf.setFloat("hbase.master.balancer.stochastic.minCostNeedBalance", Float.MAX_VALUE);
      loadBalancer.onConfigurationChange(conf);
      Assert.assertNull(loadBalancer.balanceCluster(LoadOfAllTable));

      // NamedQueue is an async Producer-consumer Pattern, waiting here until it completed
      int maxWaitingCount = 10;
      while (maxWaitingCount-- > 0 && getBalancerRejectionLogEntries(provider).size() != 2) {
        Thread.sleep(1000);
      }
      // There are two cases, should be 2 logEntries
      List<LogEntry> logEntries = getBalancerRejectionLogEntries(provider);
      Assert.assertEquals(2, logEntries.size());
      Assert.assertTrue(logEntries.get(0).toJsonPrettyPrint().contains("minCostNeedBalance"));
      Assert.assertTrue(logEntries.get(1).toJsonPrettyPrint().contains("cost1*multiplier1"));
    } finally {
      conf.unset(StochasticLoadBalancer.COST_FUNCTIONS_COST_FUNCTIONS_KEY);
      conf.unset(BaseLoadBalancer.BALANCER_REJECTION_BUFFER_ENABLED);
      loadBalancer.onConfigurationChange(conf);
    }
  }

  private List<LogEntry> getBalancerRejectionLogEntries(MasterClusterInfoProvider provider) {
    NamedQueueGetRequest namedQueueGetRequest = new NamedQueueGetRequest();
    namedQueueGetRequest.setNamedQueueEvent(BalancerRejectionDetails.BALANCER_REJECTION_EVENT);
    namedQueueGetRequest
      .setBalancerRejectionsRequest(MasterProtos.BalancerRejectionsRequest.getDefaultInstance());
    NamedQueueGetResponse namedQueueGetResponse =
      provider.getNamedQueueRecorder().getNamedQueueRecords(namedQueueGetRequest);
    List<RecentLogs.BalancerRejection> balancerRejections =
      namedQueueGetResponse.getBalancerRejections();
    MasterProtos.BalancerRejectionsResponse response = MasterProtos.BalancerRejectionsResponse
      .newBuilder().addAllBalancerRejection(balancerRejections).build();
    List<LogEntry> balancerRejectionRecords = ProtobufUtil.getBalancerRejectionEntries(response);
    return balancerRejectionRecords;
  }
}
