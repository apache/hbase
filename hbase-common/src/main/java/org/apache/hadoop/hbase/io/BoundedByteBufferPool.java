/**
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
package org.apache.hadoop.hbase.io;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.BoundedArrayQueue;

import com.google.common.annotations.VisibleForTesting;

/**
 * Like Hadoops' ByteBufferPool only you do not specify desired size when getting a ByteBuffer.
 * This pool keeps an upper bound on the count of ByteBuffers in the pool and on the maximum size
 * of ByteBuffer that it will retain (Hence the pool is 'bounded' as opposed to, say,
 * Hadoop's ElasticByteBuffferPool).
 * If a ByteBuffer is bigger than the configured threshold, we will just let the ByteBuffer go
 * rather than add it to the pool. If more ByteBuffers than the configured maximum instances,
 * we will not add the passed ByteBuffer to the pool; we will just drop it
 * (we will log a WARN in this case that we are at capacity).
 *
 * <p>The intended use case is a reservoir of bytebuffers that an RPC can reuse; buffers tend to
 * achieve a particular 'run' size over time give or take a few extremes. Set TRACE level on this
 * class for a couple of seconds to get reporting on how it is running when deployed.
 *
 * <p>This pool returns off heap ByteBuffers.
 *
 * <p>This class is thread safe.
 */
@InterfaceAudience.Private
public class BoundedByteBufferPool {
  private static final Log LOG = LogFactory.getLog(BoundedByteBufferPool.class);

  @VisibleForTesting
  final Queue<ByteBuffer> buffers;

  // Maximum size of a ByteBuffer to retain in pool
  private final int maxByteBufferSizeToCache;

  // A running average only it only rises, it never recedes
  @VisibleForTesting
  volatile int runningAverage;

  // Scratch that keeps rough total size of pooled bytebuffers
  private volatile int totalReservoirCapacity;

  // For reporting
  private AtomicLong allocations = new AtomicLong(0);

  private ReentrantLock lock =  new ReentrantLock();

  /**
   * @param maxByteBufferSizeToCache
   * @param initialByteBufferSize
   * @param maxToCache
   */
  public BoundedByteBufferPool(final int maxByteBufferSizeToCache, final int initialByteBufferSize,
      final int maxToCache) {
    this.maxByteBufferSizeToCache = maxByteBufferSizeToCache;
    this.runningAverage = initialByteBufferSize;
    this.buffers = new BoundedArrayQueue<ByteBuffer>(maxToCache);
  }

  public ByteBuffer getBuffer() {
    ByteBuffer bb = null;
    lock.lock();
    try {
      bb = this.buffers.poll();
      if (bb != null) {
        this.totalReservoirCapacity -= bb.capacity();
      }
    } finally {
      lock.unlock();
    }
    if (bb != null) {
      // Clear sets limit == capacity. Postion == 0.
      bb.clear();
    } else {
      bb = ByteBuffer.allocateDirect(this.runningAverage);
      this.allocations.incrementAndGet();
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("runningAverage=" + this.runningAverage +
        ", totalCapacity=" + this.totalReservoirCapacity + ", count=" + this.buffers.size() +
        ", alloctions=" + this.allocations.get());
    }
    return bb;
  }

  public void putBuffer(ByteBuffer bb) {
    // If buffer is larger than we want to keep around, just let it go.
    if (bb.capacity() > this.maxByteBufferSizeToCache) return;
    boolean success = false;
    int average = 0;
    lock.lock();
    try {
      success = this.buffers.offer(bb);
      if (success) {
        this.totalReservoirCapacity += bb.capacity();
        average = this.totalReservoirCapacity / this.buffers.size(); // size will never be 0.
      }
    } finally {
      lock.unlock();
    }
    if (!success) {
      LOG.warn("At capacity: " + this.buffers.size());
    } else {
      if (average > this.runningAverage && average < this.maxByteBufferSizeToCache) {
        this.runningAverage = average;
      }
    }
  }
}
