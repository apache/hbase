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
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.SlowLogParams;
import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.hadoop.hbase.slowlog.SlowLogTableAccessor;
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
class LogEventHandler implements EventHandler<RingBufferEnvelope> {

  private static final Logger LOG = LoggerFactory.getLogger(LogEventHandler.class);

  private static final String SYS_TABLE_QUEUE_SIZE =
    "hbase.regionserver.slowlog.systable.queue.size";
  private static final int DEFAULT_SYS_TABLE_QUEUE_SIZE = 1000;
  private static final int SYSTABLE_PUT_BATCH_SIZE = 100;

  private final Queue<SlowLogPayload> queueForRingBuffer;
  private final Queue<SlowLogPayload> queueForSysTable;
  private final boolean isSlowLogTableEnabled;

  private Configuration configuration;

  private static final ReentrantLock LOCK = new ReentrantLock();

  LogEventHandler(int eventCount, boolean isSlowLogTableEnabled, Configuration conf) {
    this.configuration = conf;
    EvictingQueue<SlowLogPayload> evictingQueue = EvictingQueue.create(eventCount);
    queueForRingBuffer = Queues.synchronizedQueue(evictingQueue);
    this.isSlowLogTableEnabled = isSlowLogTableEnabled;
    if (isSlowLogTableEnabled) {
      int sysTableQueueSize = conf.getInt(SYS_TABLE_QUEUE_SIZE, DEFAULT_SYS_TABLE_QUEUE_SIZE);
      EvictingQueue<SlowLogPayload> evictingQueueForTable =
        EvictingQueue.create(sysTableQueueSize);
      queueForSysTable = Queues.synchronizedQueue(evictingQueueForTable);
    } else {
      queueForSysTable = null;
    }
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
    final SlowLogPayload.Type type = getLogType(rpcCallDetails);
    if (type == null) {
      return;
    }
    Descriptors.MethodDescriptor methodDescriptor = rpcCall.getMethod();
    Message param = rpcCallDetails.getParam();
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
    queueForRingBuffer.add(slowLogPayload);
    if (isSlowLogTableEnabled) {
      if (!slowLogPayload.getRegionName().startsWith("hbase:slowlog")) {
        queueForSysTable.add(slowLogPayload);
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
   * @return true if slow log payloads are cleaned up, false otherwise
   */
  boolean clearSlowLogs() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Received request to clean up online slowlog buffer..");
    }
    queueForRingBuffer.clear();
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
      Arrays.stream(queueForRingBuffer.toArray(new SlowLogPayload[0]))
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
    List<SlowLogPayload> slowLogPayloadList =
      Arrays.stream(queueForRingBuffer.toArray(new SlowLogPayload[0]))
        .filter(e -> e.getType() == SlowLogPayload.Type.ALL
          || e.getType() == SlowLogPayload.Type.LARGE_LOG)
        .collect(Collectors.toList());

    // latest large logs first, operator is interested in latest records from in-memory buffer
    Collections.reverse(slowLogPayloadList);

    return LogHandlerUtils.getFilteredLogs(request, slowLogPayloadList);
  }

  /**
   * Poll from queueForSysTable and insert 100 records in hbase:slowlog table in single batch
   */
  void addAllLogsToSysTable() {
    if (queueForSysTable == null) {
      // hbase.regionserver.slowlog.systable.enabled is turned off. Exiting.
      return;
    }
    if (LOCK.isLocked()) {
      return;
    }
    LOCK.lock();
    try {
      List<SlowLogPayload> slowLogPayloads = new ArrayList<>();
      int i = 0;
      while (!queueForSysTable.isEmpty()) {
        slowLogPayloads.add(queueForSysTable.poll());
        i++;
        if (i == SYSTABLE_PUT_BATCH_SIZE) {
          SlowLogTableAccessor.addSlowLogRecords(slowLogPayloads, this.configuration);
          slowLogPayloads.clear();
          i = 0;
        }
      }
      if (slowLogPayloads.size() > 0) {
        SlowLogTableAccessor.addSlowLogRecords(slowLogPayloads, this.configuration);
      }
    } finally {
      LOCK.unlock();
    }
  }

}
