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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.LogEntry;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.namequeues.BalancerDecisionDetails;
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
 * Test BalancerDecision ring buffer using namedQueue interface
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestBalancerDecision extends BalancerTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBalancerDecision.class);

  @Test
  public void testBalancerDecisions() {
    conf.setBoolean("hbase.master.balancer.decision.buffer.enabled", true);
    loadBalancer.setConf(conf);
    float minCost = conf.getFloat("hbase.master.balancer.stochastic.minCostNeedBalance", 0.05f);
    conf.setFloat("hbase.master.balancer.stochastic.minCostNeedBalance", 1.0f);
    try {
      // Test with/without per table balancer.
      boolean[] perTableBalancerConfigs = {true, false};
      for (boolean isByTable : perTableBalancerConfigs) {
        conf.setBoolean(HConstants.HBASE_MASTER_LOADBALANCE_BYTABLE, isByTable);
        loadBalancer.setConf(conf);
        for (int[] mockCluster : clusterStateMocks) {
          Map<ServerName, List<RegionInfo>> servers = mockClusterServers(mockCluster);
          Map<TableName, Map<ServerName, List<RegionInfo>>> LoadOfAllTable =
            (Map) mockClusterServersWithTables(servers);
          List<RegionPlan> plans = loadBalancer.balanceCluster(LoadOfAllTable);
          boolean emptyPlans = plans == null || plans.isEmpty();
          Assert.assertTrue(emptyPlans || needsBalanceIdleRegion(mockCluster));
        }
      }
      final NamedQueueGetRequest namedQueueGetRequest = new NamedQueueGetRequest();
      namedQueueGetRequest.setNamedQueueEvent(BalancerDecisionDetails.BALANCER_DECISION_EVENT);
      namedQueueGetRequest
        .setBalancerDecisionsRequest(MasterProtos.BalancerDecisionsRequest.getDefaultInstance());
      NamedQueueGetResponse namedQueueGetResponse =
        loadBalancer.namedQueueRecorder.getNamedQueueRecords(namedQueueGetRequest);
      List<RecentLogs.BalancerDecision> balancerDecisions =
        namedQueueGetResponse.getBalancerDecisions();
      MasterProtos.BalancerDecisionsResponse response =
        MasterProtos.BalancerDecisionsResponse.newBuilder()
          .addAllBalancerDecision(balancerDecisions)
          .build();
      List<LogEntry> balancerDecisionRecords =
        ProtobufUtil.getBalancerDecisionEntries(response);
      Assert.assertTrue(balancerDecisionRecords.size() > 160);
    } finally {
      // reset config
      conf.unset(HConstants.HBASE_MASTER_LOADBALANCE_BYTABLE);
      conf.setFloat("hbase.master.balancer.stochastic.minCostNeedBalance", minCost);
      loadBalancer.setConf(conf);
    }
  }

  private static boolean needsBalanceIdleRegion(int[] cluster) {
    return (Arrays.stream(cluster).anyMatch(x -> x > 1)) && (Arrays.stream(cluster)
      .anyMatch(x -> x < 1));
  }

}
