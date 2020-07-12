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

package org.apache.hadoop.hbase.namequeues;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
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
 * Event Handler run by disruptor ringbuffer consumer.
 * Although this is generic implementation for namedQueue, it can have individual queue specific
 * logic.
 */
@InterfaceAudience.Private
class LogEventHandler implements EventHandler<RingBufferEnvelope> {

  private static final Logger LOG = LoggerFactory.getLogger(LogEventHandler.class);

  private static final String SLOW_LOG_RING_BUFFER_SIZE =
    "hbase.regionserver.slowlog.ringbuffer.size";

  // Map that binds namedQueues.
  // If NamedQueue of specific type is enabled, corresponding Queue will be used to
  // insert and retrieve records.
  // Individual queue sizes should be determined based on their individual configs.
  private final Map<NamedQueuePayload.NamedQueueEvent, Queue> namedQueues = new HashMap<>();

  private final boolean isSlowLogTableEnabled;
  private final SlowLogPersistentService slowLogPersistentService;

  LogEventHandler(final Configuration conf) {
    // Initialize SlowLog Queue
    int slowLogQueueSize = conf.getInt(SLOW_LOG_RING_BUFFER_SIZE,
      HConstants.DEFAULT_SLOW_LOG_RING_BUFFER_SIZE);
    EvictingQueue<SlowLogPayload> evictingQueue = EvictingQueue.create(slowLogQueueSize);

    // Add slowLog queue in the namedQueue map
    Queue<SlowLogPayload> slowLogQueue = Queues.synchronizedQueue(evictingQueue);
    namedQueues.put(NamedQueuePayload.NamedQueueEvent.SLOW_LOG, slowLogQueue);

    this.isSlowLogTableEnabled = conf.getBoolean(HConstants.SLOW_LOG_SYS_TABLE_ENABLED_KEY,
      HConstants.DEFAULT_SLOW_LOG_SYS_TABLE_ENABLED_KEY);
    if (isSlowLogTableEnabled) {
      slowLogPersistentService = new SlowLogPersistentService(conf);
    } else {
      slowLogPersistentService = null;
    }
  }

  /**
   * Called when a publisher has published an event to the {@link RingBuffer}.
   * This is generic consumer of disruptor ringbuffer and for each new namedQueue that we
   * add, we should also provide specific consumer logic here.
   *
   * @param event published to the {@link RingBuffer}
   * @param sequence of the event being processed
   * @param endOfBatch flag to indicate if this is the last event in a batch from
   *   the {@link RingBuffer}
   */
  @Override
  public void onEvent(RingBufferEnvelope event, long sequence, boolean endOfBatch) {
    final NamedQueuePayload namedQueuePayload = event.getPayload();
    // consume ringbuffer payload based on event type
    if (NamedQueuePayload.NamedQueueEvent.SLOW_LOG
        .equals(namedQueuePayload.getNamedQueueEvent())) {
      consumeSlowLogEvent((RpcLogDetails) namedQueuePayload);
    }
  }

  /**
   * This implementation is specific to slowLog event. This consumes slowLog event from
   * disruptor and inserts records to EvictingQueue.
   *
   * @param rpcLogDetails Input for slow/largeLog events
   */
  private void consumeSlowLogEvent(RpcLogDetails rpcLogDetails) {
    final RpcCall rpcCall = rpcLogDetails.getRpcCall();
    final String clientAddress = rpcLogDetails.getClientAddress();
    final long responseSize = rpcLogDetails.getResponseSize();
    final String className = rpcLogDetails.getClassName();
    final SlowLogPayload.Type type = getLogType(rpcLogDetails);
    if (type == null) {
      return;
    }
    Descriptors.MethodDescriptor methodDescriptor = rpcCall.getMethod();
    Message param = rpcLogDetails.getParam();
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
      .setType(type)
      .setUserName(userName)
      .build();
    namedQueues.get(NamedQueuePayload.NamedQueueEvent.SLOW_LOG).add(slowLogPayload);
    if (isSlowLogTableEnabled) {
      if (!slowLogPayload.getRegionName().startsWith("hbase:slowlog")) {
        slowLogPersistentService.addToQueueForSysTable(slowLogPayload);
      }
    }
  }

  private SlowLogPayload.Type getLogType(RpcLogDetails rpcCallDetails) {
    final boolean isSlowLog = rpcCallDetails.isSlowLog();
    final boolean isLargeLog = rpcCallDetails.isLargeLog();
    final SlowLogPayload.Type type;
    if (!isSlowLog && !isLargeLog) {
      LOG.error("slowLog and largeLog both are false. Ignoring the event. rpcCallDetails: {}",
        rpcCallDetails);
      return null;
    }
    if (isSlowLog && isLargeLog) {
      type = SlowLogPayload.Type.ALL;
    } else if (isSlowLog) {
      type = SlowLogPayload.Type.SLOW_LOG;
    } else {
      type = SlowLogPayload.Type.LARGE_LOG;
    }
    return type;
  }

  /**
   * Cleans up slow log payloads
   *
   * @param namedQueueEvent type of queue to clear
   * @return true if slow log payloads are cleaned up, false otherwise
   */
  boolean clearNamedQueue(NamedQueuePayload.NamedQueueEvent namedQueueEvent) {
    LOG.debug("Received request to clean up online slowlog buffer..");
    namedQueues.get(namedQueueEvent).clear();
    return true;
  }

  /**
   * Retrieve list of slow log payloads
   *
   * @param request slow log request parameters
   * @return list of slow log payloads
   */
  List<SlowLogPayload> getSlowLogPayloads(final AdminProtos.SlowLogResponseRequest request) {
    Queue<SlowLogPayload> slowLogQueue =
      namedQueues.get(NamedQueuePayload.NamedQueueEvent.SLOW_LOG);
    List<SlowLogPayload> slowLogPayloadList =
      Arrays.stream(slowLogQueue.toArray(new SlowLogPayload[0]))
        .filter(e -> e.getType() == SlowLogPayload.Type.ALL
          || e.getType() == SlowLogPayload.Type.SLOW_LOG)
        .collect(Collectors.toList());

    // latest slow logs first, operator is interested in latest records from in-memory buffer
    Collections.reverse(slowLogPayloadList);

    return LogHandlerUtils.getFilteredLogs(request, slowLogPayloadList);
  }

  /**
   * Retrieve list of large log payloads
   *
   * @param request large log request parameters
   * @return list of large log payloads
   */
  List<SlowLogPayload> getLargeLogPayloads(final AdminProtos.SlowLogResponseRequest request) {
    Queue<SlowLogPayload> largeLogQueue =
      namedQueues.get(NamedQueuePayload.NamedQueueEvent.SLOW_LOG);
    List<SlowLogPayload> slowLogPayloadList =
      Arrays.stream(largeLogQueue.toArray(new SlowLogPayload[0]))
        .filter(e -> e.getType() == SlowLogPayload.Type.ALL
          || e.getType() == SlowLogPayload.Type.LARGE_LOG)
        .collect(Collectors.toList());

    // latest large logs first, operator is interested in latest records from in-memory buffer
    Collections.reverse(slowLogPayloadList);

    return LogHandlerUtils.getFilteredLogs(request, slowLogPayloadList);
  }

  /**
   * Add all slowLog events to system table. This is only for slowLog event's persistence on
   * system table.
   */
  void addAllSlowLogsToSysTable() {
    if (slowLogPersistentService != null) {
      slowLogPersistentService.addAllLogsToSysTable();
    }
  }

}
