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
import org.apache.hadoop.hbase.client.BalancerRejection;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer;
import org.apache.hadoop.hbase.namequeues.BalancerRejectionDetails;
import org.apache.hadoop.hbase.namequeues.NamedQueuePayload;
import org.apache.hadoop.hbase.namequeues.NamedQueueService;
import org.apache.hadoop.hbase.namequeues.request.NamedQueueGetRequest;
import org.apache.hadoop.hbase.namequeues.response.NamedQueueGetResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RecentLogs;
import org.apache.hbase.thirdparty.com.google.common.collect.EvictingQueue;
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
 * In-memory Queue service provider for Balancer Rejection events
 */
@InterfaceAudience.Private
public class BalancerRejectionQueueService implements NamedQueueService {

  private static final Logger LOG = LoggerFactory.getLogger(BalancerRejectionQueueService.class);

  private final boolean isBalancerRejectionRecording;
  private static final String BALANCER_REJECTION_QUEUE_SIZE =
    "hbase.master.balancer.rejection.queue.size";
  private static final int DEFAULT_BALANCER_REJECTION_QUEUE_SIZE = 250;

  private final Queue<RecentLogs.BalancerRejection> balancerRejectionQueue;

  public BalancerRejectionQueueService(Configuration conf) {
    isBalancerRejectionRecording = conf.getBoolean(BaseLoadBalancer.BALANCER_REJECTION_BUFFER_ENABLED,
      BaseLoadBalancer.DEFAULT_BALANCER_REJECTION_BUFFER_ENABLED);
    if (!isBalancerRejectionRecording) {
      balancerRejectionQueue = null;
      return;
    }
    final int queueSize =
      conf.getInt(BALANCER_REJECTION_QUEUE_SIZE, DEFAULT_BALANCER_REJECTION_QUEUE_SIZE);
    final EvictingQueue<RecentLogs.BalancerRejection> evictingQueue =
      EvictingQueue.create(queueSize);
    balancerRejectionQueue = Queues.synchronizedQueue(evictingQueue);
  }

  @Override
  public NamedQueuePayload.NamedQueueEvent getEvent() {
    return NamedQueuePayload.NamedQueueEvent.BALANCE_REJECTION;
  }

  @Override
  public void consumeEventFromDisruptor(NamedQueuePayload namedQueuePayload) {
    if (!isBalancerRejectionRecording) {
      return;
    }
    if (!(namedQueuePayload instanceof BalancerRejectionDetails)) {
      LOG.warn("BalancerRejectionQueueService: NamedQueuePayload is not of type"
        + " BalancerRejectionDetails.");
      return;
    }
    BalancerRejectionDetails balancerRejectionDetails = (BalancerRejectionDetails) namedQueuePayload;
    BalancerRejection balancerRejectionRecord =
      balancerRejectionDetails.getBalancerRejection();
      RecentLogs.BalancerRejection BalancerRejection = RecentLogs.BalancerRejection.newBuilder()
        .setReason(balancerRejectionRecord.getReason())
        .addAllCostFuncInfo(balancerRejectionRecord.getCostFuncInfoList())
        .build();
      balancerRejectionQueue.add(BalancerRejection);
  }

  @Override
  public boolean clearNamedQueue() {
    if (!isBalancerRejectionRecording) {
      return false;
    }
    LOG.debug("Received request to clean up balancer rejection queue.");
    balancerRejectionQueue.clear();
    return true;
  }

  @Override
  public NamedQueueGetResponse getNamedQueueRecords(NamedQueueGetRequest request) {
    if (!isBalancerRejectionRecording) {
      return null;
    }
    List<RecentLogs.BalancerRejection> balancerRejections =
      Arrays.stream(balancerRejectionQueue.toArray(new RecentLogs.BalancerRejection[0]))
        .collect(Collectors.toList());
    // latest records should be displayed first, hence reverse order sorting
    Collections.reverse(balancerRejections);
    int limit = balancerRejections.size();
    if (request.getBalancerRejectionsRequest().hasLimit()) {
      limit = Math.min(request.getBalancerRejectionsRequest().getLimit(), balancerRejections.size());
    }
    // filter limit if provided
    balancerRejections = balancerRejections.subList(0, limit);
    final NamedQueueGetResponse namedQueueGetResponse = new NamedQueueGetResponse();
    namedQueueGetResponse.setNamedQueueEvent(BalancerRejectionDetails.BALANCER_REJECTION_EVENT);
    namedQueueGetResponse.setBalancerRejections(balancerRejections);
    return namedQueueGetResponse;
  }

  @Override
  public void persistAll() {
    // no-op for now
  }

}
