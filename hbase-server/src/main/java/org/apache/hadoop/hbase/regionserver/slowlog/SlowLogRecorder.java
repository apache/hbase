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

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.TooSlowLog.SlowLogPayload;

/**
 * Online SlowLog Provider Service that keeps slow RPC logs in the ring buffer.
 * The service uses LMAX Disruptor to save slow records which are then consumed by
 * a queue and based on the ring buffer size, the available records are then fetched
 * from the queue in thread-safe manner.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SlowLogRecorder {

  private final Disruptor<RingBufferEnvelope> disruptor;
  private final SlowLogEventHandler slowLogEventHandler;
  private final int eventCount;
  private final boolean isOnlineSlowLogProviderEnabled;

  private static final String SLOW_LOG_RING_BUFFER_SIZE =
    "hbase.regionserver.slowlog.ringbuffer.size";

  /**
   * Initialize disruptor with configurable ringbuffer size
   */
  public SlowLogRecorder(Configuration conf) {
    isOnlineSlowLogProviderEnabled = conf.getBoolean(HConstants.SLOW_LOG_BUFFER_ENABLED_KEY,
      HConstants.DEFAULT_ONLINE_LOG_PROVIDER_ENABLED);

    if (!isOnlineSlowLogProviderEnabled) {
      this.disruptor = null;
      this.slowLogEventHandler = null;
      this.eventCount = 0;
      return;
    }

    this.eventCount = conf.getInt(SLOW_LOG_RING_BUFFER_SIZE,
      HConstants.DEFAULT_SLOW_LOG_RING_BUFFER_SIZE);

    // This is the 'writer' -- a single threaded executor. This single thread consumes what is
    // put on the ringbuffer.
    final String hostingThreadName = Thread.currentThread().getName();

    // disruptor initialization with BlockingWaitStrategy
    this.disruptor = new Disruptor<>(RingBufferEnvelope::new,
      getEventCount(),
      Threads.newDaemonThreadFactory(hostingThreadName + ".slowlog.append"),
      ProducerType.MULTI,
      new BlockingWaitStrategy());
    this.disruptor.setDefaultExceptionHandler(new DisruptorExceptionHandler());

    // initialize ringbuffer event handler
    this.slowLogEventHandler = new SlowLogEventHandler(this.eventCount);
    this.disruptor.handleEventsWith(new SlowLogEventHandler[]{this.slowLogEventHandler});
    this.disruptor.start();
  }

  // must be power of 2 for disruptor ringbuffer
  private int getEventCount() {
    Preconditions.checkArgument(eventCount >= 0,
      SLOW_LOG_RING_BUFFER_SIZE + " must be > 0");
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
   * Retrieve online slow logs from ringbuffer
   *
   * @param request slow log request parameters
   * @return online slow logs from ringbuffer
   */
  public List<SlowLogPayload> getSlowLogPayloads(AdminProtos.SlowLogResponseRequest request) {
    return isOnlineSlowLogProviderEnabled ? this.slowLogEventHandler.getSlowLogPayloads(request)
      : Collections.emptyList();
  }

  /**
   * clears slow log payloads from ringbuffer
   *
   * @return true if slow log payloads are cleaned up or
   *   hbase.regionserver.slowlog.buffer.enabled is not set to true, false if failed to
   *   clean up slow logs
   */
  public boolean clearSlowLogPayloads() {
    if (!isOnlineSlowLogProviderEnabled) {
      return true;
    }
    return this.slowLogEventHandler.clearSlowLogs();
  }

  /**
   * Add slow log rpcCall details to ringbuffer
   *
   * @param rpcLogDetails all details of rpc call that would be useful for ring buffer
   *   consumers
   */
  public void addSlowLogPayload(RpcLogDetails rpcLogDetails) {
    if (!isOnlineSlowLogProviderEnabled) {
      return;
    }
    RingBuffer<RingBufferEnvelope> ringBuffer = this.disruptor.getRingBuffer();
    long seqId = ringBuffer.next();
    try {
      ringBuffer.get(seqId).load(rpcLogDetails);
    } finally {
      ringBuffer.publish(seqId);
    }
  }

}
