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

package org.apache.hadoop.hbase.regionserver.slowlog;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.SlowLogParams;
import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.EvictingQueue;
import org.apache.hbase.thirdparty.com.google.common.collect.Queues;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.TooSlowLog.SlowLogPayload;

/**
 * Event Handler run by disruptor ringbuffer consumer
 */
@InterfaceAudience.Private
class SlowLogEventHandler implements EventHandler<RingBufferEnvelope> {

  private static final Logger LOG = LoggerFactory.getLogger(SlowLogEventHandler.class);

  private final Queue<SlowLogPayload> queue;

  SlowLogEventHandler(int eventCount) {
    EvictingQueue<SlowLogPayload> evictingQueue = EvictingQueue.create(eventCount);
    queue = Queues.synchronizedQueue(evictingQueue);
  }

  /**
   * Called when a publisher has published an event to the {@link RingBuffer}
   *
   * @param event published to the {@link RingBuffer}
   * @param sequence of the event being processed
   * @param endOfBatch flag to indicate if this is the last event in a batch from
   *   the {@link RingBuffer}
   * @throws Exception if the EventHandler would like the exception handled further up the chain
   */
  @Override
  public void onEvent(RingBufferEnvelope event, long sequence, boolean endOfBatch)
      throws Exception {
    final RpcLogDetails rpcCallDetails = event.getPayload();
    final RpcCall rpcCall = rpcCallDetails.getRpcCall();
    final String clientAddress = rpcCallDetails.getClientAddress();
    final long responseSize = rpcCallDetails.getResponseSize();
    final String className = rpcCallDetails.getClassName();
    Descriptors.MethodDescriptor methodDescriptor = rpcCall.getMethod();
    Message param = rpcCall.getParam();
    long receiveTime = rpcCall.getReceiveTime();
    long startTime = rpcCall.getStartTime();
    long endTime = System.currentTimeMillis();
    int processingTime = (int) (endTime - startTime);
    int qTime = (int) (startTime - receiveTime);
    final SlowLogParams slowLogParams = ProtobufUtil.getSlowLogParams(param);
    int numGets = 0;
    int numMutations = 0;
    int numServiceCalls = 0;
    if (param instanceof ClientProtos.MultiRequest) {
      ClientProtos.MultiRequest multi = (ClientProtos.MultiRequest) param;
      for (ClientProtos.RegionAction regionAction : multi.getRegionActionList()) {
        for (ClientProtos.Action action : regionAction.getActionList()) {
          if (action.hasMutation()) {
            numMutations++;
          }
          if (action.hasGet()) {
            numGets++;
          }
          if (action.hasServiceCall()) {
            numServiceCalls++;
          }
        }
      }
    }
    final String userName = rpcCall.getRequestUserName().orElse(StringUtils.EMPTY);
    final String methodDescriptorName =
      methodDescriptor != null ? methodDescriptor.getName() : StringUtils.EMPTY;
    SlowLogPayload slowLogPayload = SlowLogPayload.newBuilder()
      .setCallDetails(methodDescriptorName + "(" + param.getClass().getName() + ")")
      .setClientAddress(clientAddress)
      .setMethodName(methodDescriptorName)
      .setMultiGets(numGets)
      .setMultiMutations(numMutations)
      .setMultiServiceCalls(numServiceCalls)
      .setParam(slowLogParams != null ? slowLogParams.getParams() : StringUtils.EMPTY)
      .setProcessingTime(processingTime)
      .setQueueTime(qTime)
      .setRegionName(slowLogParams != null ? slowLogParams.getRegionName() : StringUtils.EMPTY)
      .setResponseSize(responseSize)
      .setServerClass(className)
      .setStartTime(startTime)
      .setUserName(userName)
      .build();
    queue.add(slowLogPayload);
  }

  /**
   * Cleans up slow log payloads
   *
   * @return true if slow log payloads are cleaned up, false otherwise
   */
  boolean clearSlowLogs() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Received request to clean up online slowlog buffer..");
    }
    queue.clear();
    return true;
  }

  /**
   * Retrieve list of slow log payloads
   *
   * @param request slow log request parameters
   * @return list of slow log payloads
   */
  List<SlowLogPayload> getSlowLogPayloads(final AdminProtos.SlowLogResponseRequest request) {
    List<SlowLogPayload> slowLogPayloadList =
      Arrays.stream(queue.toArray(new SlowLogPayload[0])).collect(Collectors.toList());

    // latest slow logs first, operator is interested in latest records from in-memory buffer
    Collections.reverse(slowLogPayloadList);

    if (isFilterProvided(request)) {
      slowLogPayloadList = filterSlowLogs(request, slowLogPayloadList);
    }
    int limit = request.getLimit() >= slowLogPayloadList.size() ? slowLogPayloadList.size()
      : request.getLimit();
    return slowLogPayloadList.subList(0, limit);
  }

  private boolean isFilterProvided(AdminProtos.SlowLogResponseRequest request) {
    if (StringUtils.isNotEmpty(request.getUserName())) {
      return true;
    }
    if (StringUtils.isNotEmpty(request.getTableName())) {
      return true;
    }
    if (StringUtils.isNotEmpty(request.getClientAddress())) {
      return true;
    }
    return StringUtils.isNotEmpty(request.getRegionName());
  }

  private List<SlowLogPayload> filterSlowLogs(AdminProtos.SlowLogResponseRequest request,
      List<SlowLogPayload> slowLogPayloadList) {
    List<SlowLogPayload> filteredSlowLogPayloads = new ArrayList<>();
    for (SlowLogPayload slowLogPayload : slowLogPayloadList) {
      if (StringUtils.isNotEmpty(request.getRegionName())) {
        if (slowLogPayload.getRegionName().equals(request.getRegionName())) {
          filteredSlowLogPayloads.add(slowLogPayload);
          continue;
        }
      }
      if (StringUtils.isNotEmpty(request.getTableName())) {
        if (slowLogPayload.getRegionName().startsWith(request.getTableName())) {
          filteredSlowLogPayloads.add(slowLogPayload);
          continue;
        }
      }
      if (StringUtils.isNotEmpty(request.getClientAddress())) {
        if (slowLogPayload.getClientAddress().equals(request.getClientAddress())) {
          filteredSlowLogPayloads.add(slowLogPayload);
          continue;
        }
      }
      if (StringUtils.isNotEmpty(request.getUserName())) {
        if (slowLogPayload.getUserName().equals(request.getUserName())) {
          filteredSlowLogPayloads.add(slowLogPayload);
        }
      }
    }
    return filteredSlowLogPayloads;
  }

}
