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
package org.apache.hadoop.hbase.namequeues.impl;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.namequeues.LogHandlerUtils;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionHist;
import org.apache.hbase.thirdparty.com.google.common.collect.EvictingQueue;
import org.apache.hbase.thirdparty.com.google.common.collect.Queues;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.namequeues.NamedQueuePayload;
import org.apache.hadoop.hbase.namequeues.NamedQueueService;
import org.apache.hadoop.hbase.namequeues.RegionHistorianPayload;
import org.apache.hadoop.hbase.namequeues.request.NamedQueueGetRequest;
import org.apache.hadoop.hbase.namequeues.response.NamedQueueGetResponse;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-memory Queue service provider for providing region states and event details
 */

@InterfaceAudience.Private
public class RegionHistorianQueueService implements NamedQueueService{
  private static final Logger LOG= LoggerFactory.getLogger(RegionHistorianQueueService.class);
  private static final String REGION_HISTORIAN_RING_BUFFER_SIZE = "hbase.master.regionHistorian.ringbuffer.size";
  private final boolean isRegionHistorianEnabled;
  private int queueSize;
  private final Queue<RegionHist.RegionHistorianPayload> regionHistorianQueue;

  public RegionHistorianQueueService(Configuration conf) {
    this.isRegionHistorianEnabled = conf.getBoolean(HConstants.REGION_HISTORIAN_BUFFER_ENABLED_KEY,
      HConstants.DEFAULT_REGION_HISTORIAN_ENABLED_KEY);
    this.queueSize = conf.getInt(REGION_HISTORIAN_RING_BUFFER_SIZE,256);
    EvictingQueue<RegionHist.RegionHistorianPayload> evictingQueue = EvictingQueue.create(queueSize);
    regionHistorianQueue = Queues.synchronizedQueue(evictingQueue);
  }

  @Override
  public NamedQueuePayload.NamedQueueEvent getEvent() {
    return NamedQueuePayload.NamedQueueEvent.REGION_HISTORIAN;
  }

  @Override
  public boolean clearNamedQueue() {
    if (!isRegionHistorianEnabled) {
      return false;
    }
    LOG.debug("Received request to clean up region historian buffer.");
    regionHistorianQueue.clear();
    return true;
  }

  @Override
  public NamedQueueGetResponse getNamedQueueRecords(NamedQueueGetRequest request) {
    if (!isRegionHistorianEnabled) {
      return null;
    }
    final AdminProtos.RegionHistorianResponseRequest regionHistorianResponseRequest = request.getRegionHistorianResponseRequest();
    final List<RegionHist.RegionHistorianPayload> regionHistorianPayloadList;
    regionHistorianPayloadList = getRegionHistorianPayloads(regionHistorianResponseRequest);
    NamedQueueGetResponse response = new NamedQueueGetResponse();
    response.setNamedQueueEvent(RegionHistorianPayload.REGION_HISTORIAN_EVENT);
    response.setRegionHistorianPayloads(regionHistorianPayloadList);
    return response;
  }

  @Override
  public void consumeEventFromDisruptor(NamedQueuePayload namedQueuePayload) {
    if (!isRegionHistorianEnabled) {
      return;
    }
    final RegionHistorianPayload regionHistorianPayloadDetail = (RegionHistorianPayload) namedQueuePayload;
    final String regionName = regionHistorianPayloadDetail.getRegionName();
    final String hostName = regionHistorianPayloadDetail.getHostname();
    final String tableName = regionHistorianPayloadDetail.getTableName();
    final String eventType = regionHistorianPayloadDetail.getEventType();
    final long eventTimestamp = regionHistorianPayloadDetail.getEventTimestamp();
    final long pid = regionHistorianPayloadDetail.getPid();
    final long ppid = regionHistorianPayloadDetail.getPpid();
    RegionHist.RegionHistorianPayload regionHistorianPayload = RegionHist.RegionHistorianPayload.newBuilder()
      .setHostName(hostName)
      .setRegionName(regionName)
      .setTableName(tableName)
      .setEventType(eventType)
      .setEventTimestamp(eventTimestamp)
      .setPid(pid)
      .setPpid(ppid).build();
    addToQueue(regionHistorianPayload);
  }

  synchronized void addToQueue(RegionHist.RegionHistorianPayload payload) {
    try {
      regionHistorianQueue.add(payload);
    } catch (Exception e) {
      LOG.error("Error adding payload to region historian queue: ",e);
    }
  }

  @Override
  public void persistAll(Connection connection) {
    if (!isRegionHistorianEnabled) {
      return;
    }
    //will use this to persist records in sys table
    return;
  }

  private List<RegionHist.RegionHistorianPayload> getRegionHistorianPayloads(
    final AdminProtos.RegionHistorianResponseRequest request) {
    List<RegionHist.RegionHistorianPayload> regionHistorianPayloadList =
      Arrays.stream(regionHistorianQueue.toArray(new RegionHist.RegionHistorianPayload[0]))
        .collect(Collectors.toList());
    Collections.reverse(regionHistorianPayloadList);
    return LogHandlerUtils.getFilteredLogsRegionHist(request, regionHistorianPayloadList);
  }
}
