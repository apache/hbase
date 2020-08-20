/*
 *
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

package org.apache.hadoop.hbase.namequeues.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.BalancerDecision;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer;
import org.apache.hadoop.hbase.namequeues.BalancerDecisionDetails;
import org.apache.hadoop.hbase.namequeues.NamedQueuePayload;
import org.apache.hadoop.hbase.namequeues.NamedQueueService;
import org.apache.hadoop.hbase.namequeues.request.NamedQueueGetRequest;
import org.apache.hadoop.hbase.namequeues.response.NamedQueueGetResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RecentLogs;
import org.apache.hbase.thirdparty.com.google.common.collect.EvictingQueue;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Queues;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

/**
 * In-memory Queue service provider for Balancer Decision events
 */
@InterfaceAudience.Private
public class BalancerDecisionQueueService implements NamedQueueService {

  private static final Logger LOG = LoggerFactory.getLogger(BalancerDecisionQueueService.class);

  private final boolean isBalancerDecisionRecording;

  private static final String BALANCER_DECISION_QUEUE_SIZE =
    "hbase.master.balancer.decision.queue.size";
  private static final int DEFAULT_BALANCER_DECISION_QUEUE_SIZE = 250;

  private static final int REGION_PLANS_THRESHOLD_PER_BALANCER = 15;

  private final Queue<RecentLogs.BalancerDecision> balancerDecisionQueue;

  public BalancerDecisionQueueService(Configuration conf) {
    isBalancerDecisionRecording = conf.getBoolean(BaseLoadBalancer.BALANCER_DECISION_BUFFER_ENABLED,
      BaseLoadBalancer.DEFAULT_BALANCER_DECISION_BUFFER_ENABLED);
    if (!isBalancerDecisionRecording) {
      balancerDecisionQueue = null;
      return;
    }
    final int queueSize =
      conf.getInt(BALANCER_DECISION_QUEUE_SIZE, DEFAULT_BALANCER_DECISION_QUEUE_SIZE);
    final EvictingQueue<RecentLogs.BalancerDecision> evictingQueue =
      EvictingQueue.create(queueSize);
    balancerDecisionQueue = Queues.synchronizedQueue(evictingQueue);
  }

  @Override
  public NamedQueuePayload.NamedQueueEvent getEvent() {
    return NamedQueuePayload.NamedQueueEvent.BALANCE_DECISION;
  }

  @Override
  public void consumeEventFromDisruptor(NamedQueuePayload namedQueuePayload) {
    if (!isBalancerDecisionRecording) {
      return;
    }
    if (!(namedQueuePayload instanceof BalancerDecisionDetails)) {
      LOG.warn(
        "BalancerDecisionQueueService: NamedQueuePayload is not of type BalancerDecisionDetails.");
      return;
    }
    BalancerDecisionDetails balancerDecisionDetails = (BalancerDecisionDetails) namedQueuePayload;
    BalancerDecision balancerDecisionRecords =
      balancerDecisionDetails.getBalancerDecision();
    List<String> regionPlans = balancerDecisionRecords.getRegionPlans();
    List<List<String>> regionPlansList;
    if (regionPlans.size() > REGION_PLANS_THRESHOLD_PER_BALANCER) {
      regionPlansList = Lists.partition(regionPlans, REGION_PLANS_THRESHOLD_PER_BALANCER);
    } else {
      regionPlansList = Collections.singletonList(regionPlans);
    }
    for (List<String> regionPlansPerBalancer : regionPlansList) {
      RecentLogs.BalancerDecision balancerDecision = RecentLogs.BalancerDecision.newBuilder()
        .setInitTotalCost(balancerDecisionRecords.getInitTotalCost())
        .setInitialFunctionCosts(balancerDecisionRecords.getInitialFunctionCosts())
        .setComputedTotalCost(balancerDecisionRecords.getComputedTotalCost())
        .setFinalFunctionCosts(balancerDecisionRecords.getFinalFunctionCosts())
        .setComputedSteps(balancerDecisionRecords.getComputedSteps())
        .addAllRegionPlans(regionPlansPerBalancer)
        .build();
      balancerDecisionQueue.add(balancerDecision);
    }
  }

  @Override
  public boolean clearNamedQueue() {
    if (!isBalancerDecisionRecording) {
      return false;
    }
    LOG.debug("Received request to clean up balancer decision queue.");
    balancerDecisionQueue.clear();
    return true;
  }

  @Override
  public NamedQueueGetResponse getNamedQueueRecords(NamedQueueGetRequest request) {
    if (!isBalancerDecisionRecording) {
      return null;
    }
    List<RecentLogs.BalancerDecision> balancerDecisions =
      Arrays.stream(balancerDecisionQueue.toArray(new RecentLogs.BalancerDecision[0]))
        .collect(Collectors.toList());
    // latest records should be displayed first, hence reverse order sorting
    Collections.reverse(balancerDecisions);
    int limit = balancerDecisions.size();
    if (request.getBalancerDecisionsRequest().hasLimit()) {
      limit = Math.min(request.getBalancerDecisionsRequest().getLimit(), balancerDecisions.size());
    }
    // filter limit if provided
    balancerDecisions = balancerDecisions.subList(0, limit);
    final NamedQueueGetResponse namedQueueGetResponse = new NamedQueueGetResponse();
    namedQueueGetResponse.setNamedQueueEvent(BalancerDecisionDetails.BALANCER_DECISION_EVENT);
    namedQueueGetResponse.setBalancerDecisions(balancerDecisions);
    return namedQueueGetResponse;
  }

  @Override
  public void persistAll() {
    // no-op for now
  }

}
