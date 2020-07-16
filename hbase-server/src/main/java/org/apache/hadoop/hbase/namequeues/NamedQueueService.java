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

import org.apache.hadoop.hbase.namequeues.request.NamedQueueGetRequest;
import org.apache.hadoop.hbase.namequeues.response.NamedQueueGetResponse;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * In-memory Queue service provider for multiple use-cases. Implementers should be
 * registered in LogEventHandler
 */
@InterfaceAudience.Private
public interface NamedQueueService {

  /**
   * Retrieve event type for NamedQueueService implementation.
   *
   * @return {@link NamedQueuePayload.NamedQueueEvent}
   */
  NamedQueuePayload.NamedQueueEvent getEvent();

  /**
   * This implementation is generic for consuming records from LMAX
   * disruptor and inserts records to EvictingQueue which is maintained by each
   * ringbuffer provider.
   *
   * @param namedQueuePayload namedQueue payload from disruptor ring buffer
   */
  void consumeEventFromDisruptor(NamedQueuePayload namedQueuePayload);

  /**
   * Cleans up queues maintained by services.
   *
   * @return true if slow log payloads are cleaned up, false otherwise
   */
  boolean clearNamedQueue();

  /**
   * Retrieve in memory queue records from ringbuffer
   *
   * @param request namedQueue request with event type
   * @return queue records from ringbuffer after filter (if applied)
   */
  NamedQueueGetResponse getNamedQueueRecords(NamedQueueGetRequest request);

  /**
   * Add all in memory queue records to system table. The implementors can use system table
   * or direct HDFS file or ZK as persistence system.
   */
  void persistAll();
}
