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

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.namequeues.request.NamedQueueGetRequest;
import org.apache.hadoop.hbase.namequeues.response.NamedQueueGetResponse;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Event Handler run by disruptor ringbuffer consumer.
 * Although this is generic implementation for namedQueue, it can have individual queue specific
 * logic.
 */
@InterfaceAudience.Private
class LogEventHandler implements EventHandler<RingBufferEnvelope> {

  private static final Logger LOG = LoggerFactory.getLogger(LogEventHandler.class);

  // Map that binds namedQueues to corresponding queue service implementation.
  // If NamedQueue of specific type is enabled, corresponding service will be used to
  // insert and retrieve records.
  // Individual queue sizes should be determined based on their individual configs within
  // each service.
  private final Map<NamedQueuePayload.NamedQueueEvent, NamedQueueService> namedQueueServices =
    new HashMap<>();

  private static final String NAMED_QUEUE_PROVIDER_CLASSES = "hbase.namedqueue.provider.classes";

  LogEventHandler(final Configuration conf) {
    for (String implName : conf.getStringCollection(NAMED_QUEUE_PROVIDER_CLASSES)) {
      Class<?> clz;
      try {
        clz = Class.forName(implName);
      } catch (ClassNotFoundException e) {
        LOG.warn("Failed to find NamedQueueService implementor class {}", implName, e);
        continue;
      }

      if (!NamedQueueService.class.isAssignableFrom(clz)) {
        LOG.warn("Class {} is not implementor of NamedQueueService.", clz);
        continue;
      }

      // add all service mappings here
      try {
        NamedQueueService namedQueueService =
          (NamedQueueService) clz.getConstructor(Configuration.class).newInstance(conf);
        namedQueueServices.put(namedQueueService.getEvent(), namedQueueService);
      } catch (InstantiationException | IllegalAccessException | NoSuchMethodException
          | InvocationTargetException e) {
        LOG.warn("Unable to instantiate/add NamedQueueService implementor {} to service map.",
          clz);
      }
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
    namedQueueServices.get(namedQueuePayload.getNamedQueueEvent())
      .consumeEventFromDisruptor(namedQueuePayload);
  }

  /**
   * Cleans up queues maintained by services.
   *
   * @param namedQueueEvent type of queue to clear
   * @return true if queue is cleaned up, false otherwise
   */
  boolean clearNamedQueue(NamedQueuePayload.NamedQueueEvent namedQueueEvent) {
    return namedQueueServices.get(namedQueueEvent).clearNamedQueue();
  }

  /**
   * Add all in memory queue records to system table. The implementors can use system table
   * or direct HDFS file or ZK as persistence system.
   */
  void persistAll(NamedQueuePayload.NamedQueueEvent namedQueueEvent) {
    namedQueueServices.get(namedQueueEvent).persistAll();
  }

  /**
   * Retrieve in memory queue records from ringbuffer
   *
   * @param request namedQueue request with event type
   * @return queue records from ringbuffer after filter (if applied)
   */
  NamedQueueGetResponse getNamedQueueRecords(NamedQueueGetRequest request) {
    return namedQueueServices.get(request.getNamedQueueEvent()).getNamedQueueRecords(request);
  }

}
