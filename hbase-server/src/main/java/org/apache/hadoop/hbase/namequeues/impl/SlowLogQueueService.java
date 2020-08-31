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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.SlowLogParams;
import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.hadoop.hbase.namequeues.LogHandlerUtils;
import org.apache.hadoop.hbase.namequeues.NamedQueuePayload;
import org.apache.hadoop.hbase.namequeues.NamedQueueService;
import org.apache.hadoop.hbase.namequeues.RpcLogDetails;
import org.apache.hadoop.hbase.namequeues.SlowLogPersistentService;
import org.apache.hadoop.hbase.namequeues.request.NamedQueueGetRequest;
import org.apache.hadoop.hbase.namequeues.response.NamedQueueGetResponse;
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
import org.apache.hadoop.hbase.shaded.protobuf.generated.TooSlowLog;

/**
 * In-memory Queue service provider for Slow/LargeLog events
 */
@InterfaceAudience.Private
public class SlowLogQueueService implements NamedQueueService {

  private static final Logger LOG = LoggerFactory.getLogger(SlowLogQueueService.class);

  private static final String SLOW_LOG_RING_BUFFER_SIZE =
    "hbase.regionserver.slowlog.ringbuffer.size";

  private final boolean isOnlineLogProviderEnabled;
  private final boolean isSlowLogTableEnabled;
  private final SlowLogPersistentService slowLogPersistentService;
  private final Queue<TooSlowLog.SlowLogPayload> slowLogQueue;

  public SlowLogQueueService(Configuration conf) {
    this.isOnlineLogProviderEnabled = conf.getBoolean(HConstants.SLOW_LOG_BUFFER_ENABLED_KEY,
      HConstants.DEFAULT_ONLINE_LOG_PROVIDER_ENABLED);

    if (!isOnlineLogProviderEnabled) {
      this.isSlowLogTableEnabled = false;
      this.slowLogPersistentService = null;
      this.slowLogQueue = null;
      return;
    }

    // Initialize SlowLog Queue
    int slowLogQueueSize =
      conf.getInt(SLOW_LOG_RING_BUFFER_SIZE, HConstants.DEFAULT_SLOW_LOG_RING_BUFFER_SIZE);

    EvictingQueue<TooSlowLog.SlowLogPayload> evictingQueue =
      EvictingQueue.create(slowLogQueueSize);
    slowLogQueue = Queues.synchronizedQueue(evictingQueue);

    this.isSlowLogTableEnabled = conf.getBoolean(HConstants.SLOW_LOG_SYS_TABLE_ENABLED_KEY,
      HConstants.DEFAULT_SLOW_LOG_SYS_TABLE_ENABLED_KEY);
    if (isSlowLogTableEnabled) {
      slowLogPersistentService = new SlowLogPersistentService(conf);
    } else {
      slowLogPersistentService = null;
    }
  }

  @Override
  public NamedQueuePayload.NamedQueueEvent getEvent() {
    return NamedQueuePayload.NamedQueueEvent.SLOW_LOG;
  }

  /**
   * This implementation is specific to slowLog event. This consumes slowLog event from
   * disruptor and inserts records to EvictingQueue.
   *
   * @param namedQueuePayload namedQueue payload from disruptor ring buffer
   */
  @Override
  public void consumeEventFromDisruptor(NamedQueuePayload namedQueuePayload) {
    if (!isOnlineLogProviderEnabled) {
      return;
    }
    if (!(namedQueuePayload instanceof RpcLogDetails)) {
      LOG.warn("SlowLogQueueService: NamedQueuePayload is not of type RpcLogDetails.");
      return;
    }
    final RpcLogDetails rpcLogDetails = (RpcLogDetails) namedQueuePayload;
    final RpcCall rpcCall = rpcLogDetails.getRpcCall();
    final String clientAddress = rpcLogDetails.getClientAddress();
    final long responseSize = rpcLogDetails.getResponseSize();
    final String className = rpcLogDetails.getClassName();
    final TooSlowLog.SlowLogPayload.Type type = getLogType(rpcLogDetails);
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
    TooSlowLog.SlowLogPayload slowLogPayload = TooSlowLog.SlowLogPayload.newBuilder()
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
    slowLogQueue.add(slowLogPayload);
    if (isSlowLogTableEnabled) {
      if (!slowLogPayload.getRegionName().startsWith("hbase:slowlog")) {
        slowLogPersistentService.addToQueueForSysTable(slowLogPayload);
      }
    }
  }

  @Override
  public boolean clearNamedQueue() {
    if (!isOnlineLogProviderEnabled) {
      return false;
    }
    LOG.debug("Received request to clean up online slowlog buffer.");
    slowLogQueue.clear();
    return true;
  }

  @Override
  public NamedQueueGetResponse getNamedQueueRecords(NamedQueueGetRequest request) {
    if (!isOnlineLogProviderEnabled) {
      return null;
    }
    final AdminProtos.SlowLogResponseRequest slowLogResponseRequest =
      request.getSlowLogResponseRequest();
    final List<TooSlowLog.SlowLogPayload> slowLogPayloads;
    if (AdminProtos.SlowLogResponseRequest.LogType.LARGE_LOG
        .equals(slowLogResponseRequest.getLogType())) {
      slowLogPayloads = getLargeLogPayloads(slowLogResponseRequest);
    } else {
      slowLogPayloads = getSlowLogPayloads(slowLogResponseRequest);
    }
    NamedQueueGetResponse response = new NamedQueueGetResponse();
    response.setNamedQueueEvent(RpcLogDetails.SLOW_LOG_EVENT);
    response.setSlowLogPayloads(slowLogPayloads);
    return response;
  }

  private TooSlowLog.SlowLogPayload.Type getLogType(RpcLogDetails rpcCallDetails) {
    final boolean isSlowLog = rpcCallDetails.isSlowLog();
    final boolean isLargeLog = rpcCallDetails.isLargeLog();
    final TooSlowLog.SlowLogPayload.Type type;
    if (!isSlowLog && !isLargeLog) {
      LOG.error("slowLog and largeLog both are false. Ignoring the event. rpcCallDetails: {}",
        rpcCallDetails);
      return null;
    }
    if (isSlowLog && isLargeLog) {
      type = TooSlowLog.SlowLogPayload.Type.ALL;
    } else if (isSlowLog) {
      type = TooSlowLog.SlowLogPayload.Type.SLOW_LOG;
    } else {
      type = TooSlowLog.SlowLogPayload.Type.LARGE_LOG;
    }
    return type;
  }

  /**
   * Add all slowLog events to system table. This is only for slowLog event's persistence on
   * system table.
   */
  @Override
  public void persistAll() {
    if (!isOnlineLogProviderEnabled) {
      return;
    }
    if (slowLogPersistentService != null) {
      slowLogPersistentService.addAllLogsToSysTable();
    }
  }

  private List<TooSlowLog.SlowLogPayload> getSlowLogPayloads(
      final AdminProtos.SlowLogResponseRequest request) {
    List<TooSlowLog.SlowLogPayload> slowLogPayloadList =
      Arrays.stream(slowLogQueue.toArray(new TooSlowLog.SlowLogPayload[0])).filter(
        e -> e.getType() == TooSlowLog.SlowLogPayload.Type.ALL
          || e.getType() == TooSlowLog.SlowLogPayload.Type.SLOW_LOG).collect(Collectors.toList());
    // latest slow logs first, operator is interested in latest records from in-memory buffer
    Collections.reverse(slowLogPayloadList);
    return LogHandlerUtils.getFilteredLogs(request, slowLogPayloadList);
  }

  private List<TooSlowLog.SlowLogPayload> getLargeLogPayloads(
      final AdminProtos.SlowLogResponseRequest request) {
    List<TooSlowLog.SlowLogPayload> slowLogPayloadList =
      Arrays.stream(slowLogQueue.toArray(new TooSlowLog.SlowLogPayload[0])).filter(
        e -> e.getType() == TooSlowLog.SlowLogPayload.Type.ALL
          || e.getType() == TooSlowLog.SlowLogPayload.Type.LARGE_LOG).collect(Collectors.toList());
    // latest large logs first, operator is interested in latest records from in-memory buffer
    Collections.reverse(slowLogPayloadList);
    return LogHandlerUtils.getFilteredLogs(request, slowLogPayloadList);
  }

}
