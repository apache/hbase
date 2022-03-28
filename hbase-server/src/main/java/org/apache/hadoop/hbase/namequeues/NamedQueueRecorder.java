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

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.namequeues.request.NamedQueueGetRequest;
import org.apache.hadoop.hbase.namequeues.response.NamedQueueGetResponse;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * NamedQueue recorder that maintains various named queues.
 * The service uses LMAX Disruptor to save queue records which are then consumed by
 * a queue and based on the ring buffer size, the available records are then fetched
 * from the queue in thread-safe manner.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class NamedQueueRecorder {

  private final Disruptor<RingBufferEnvelope> disruptor;
  private final LogEventHandler logEventHandler;

  private static NamedQueueRecorder namedQueueRecorder;
  private static boolean isInit = false;
  private static final Object LOCK = new Object();

  /**
   * Initialize disruptor with configurable ringbuffer size
   */
  private NamedQueueRecorder(Configuration conf) {

    // This is the 'writer' -- a single threaded executor. This single thread consumes what is
    // put on the ringbuffer.
    final String hostingThreadName = Thread.currentThread().getName();

    int eventCount = conf.getInt("hbase.namedqueue.ringbuffer.size", 1024);

    // disruptor initialization with BlockingWaitStrategy
    this.disruptor = new Disruptor<>(RingBufferEnvelope::new, getEventCount(eventCount),
      new ThreadFactoryBuilder().setNameFormat(hostingThreadName + ".slowlog.append-pool-%d")
        .setDaemon(true).setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build(),
      ProducerType.MULTI, new BlockingWaitStrategy());
    this.disruptor.setDefaultExceptionHandler(new DisruptorExceptionHandler());

    // initialize ringbuffer event handler
    this.logEventHandler = new LogEventHandler(conf);
    this.disruptor.handleEventsWith(new LogEventHandler[]{this.logEventHandler});
    this.disruptor.start();
  }

  public static NamedQueueRecorder getInstance(Configuration conf) {
    if (namedQueueRecorder != null) {
      return namedQueueRecorder;
    }
    synchronized (LOCK) {
      if (!isInit) {
        namedQueueRecorder = new NamedQueueRecorder(conf);
        isInit = true;
      }
    }
    return namedQueueRecorder;
  }

  // must be power of 2 for disruptor ringbuffer
  private int getEventCount(int eventCount) {
    Preconditions.checkArgument(eventCount >= 0, "hbase.namedqueue.ringbuffer.size must be > 0");
    int floor = Integer.highestOneBit(eventCount);
    if (floor == eventCount) {
      return floor;
    }
    // max capacity is 1 << 30
    if (floor >= 1 << 29) {
      return 1 << 30;
    }
    return floor << 1;
  }

  /**
   * Retrieve in memory queue records from ringbuffer
   *
   * @param request namedQueue request with event type
   * @return queue records from ringbuffer after filter (if applied)
   */
  public NamedQueueGetResponse getNamedQueueRecords(NamedQueueGetRequest request) {
    return this.logEventHandler.getNamedQueueRecords(request);
  }

  /**
   * clears queue records from ringbuffer
   *
   * @param namedQueueEvent type of queue to clear
   * @return true if slow log payloads are cleaned up or
   *   hbase.regionserver.slowlog.buffer.enabled is not set to true, false if failed to
   *   clean up slow logs
   */
  public boolean clearNamedQueue(NamedQueuePayload.NamedQueueEvent namedQueueEvent) {
    return this.logEventHandler.clearNamedQueue(namedQueueEvent);
  }

  /**
   * Add various NamedQueue records to ringbuffer. Based on the type of the event (e.g slowLog),
   * consumer of disruptor ringbuffer will have specific logic.
   * This method is producer of disruptor ringbuffer which is initialized in NamedQueueRecorder
   * constructor.
   *
   * @param namedQueuePayload namedQueue payload sent by client of ring buffer
   *   service
   */
  public void addRecord(NamedQueuePayload namedQueuePayload) {
    RingBuffer<RingBufferEnvelope> ringBuffer = this.disruptor.getRingBuffer();
    long seqId = ringBuffer.next();
    try {
      ringBuffer.get(seqId).load(namedQueuePayload);
    } finally {
      ringBuffer.publish(seqId);
    }
  }

  /**
   * Add all in memory queue records to system table. The implementors can use system table
   * or direct HDFS file or ZK as persistence system.
   */
  public void persistAll(NamedQueuePayload.NamedQueueEvent namedQueueEvent) {
    if (this.logEventHandler != null) {
      this.logEventHandler.persistAll(namedQueueEvent);
    }
  }

}
