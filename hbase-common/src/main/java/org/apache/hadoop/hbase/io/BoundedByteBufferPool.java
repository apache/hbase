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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

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

  private final Queue<ByteBuffer> buffers = new ConcurrentLinkedQueue<ByteBuffer>();

  @VisibleForTesting
  int getQueueSize() {
    return buffers.size();
  }

  private final int maxToCache;

  // Maximum size of a ByteBuffer to retain in pool
  private final int maxByteBufferSizeToCache;

  // A running average only it only rises, it never recedes
  private final AtomicInteger runningAverageRef;

  @VisibleForTesting
  int getRunningAverage() {
    return runningAverageRef.get();
  }

  // Count (lower 32bit) and total capacity (upper 32bit) of pooled bytebuffers.
  // Both are non-negative. They are equal to or larger than those of the actual
  // queued buffers in any transition.
  private final AtomicLong stateRef = new AtomicLong();

  @VisibleForTesting
  static int toCountOfBuffers(long state) {
    return (int)state;
  }

  @VisibleForTesting
  static int toTotalCapacity(long state) {
    return (int)(state >>> 32);
  }

  @VisibleForTesting
  static long toState(int countOfBuffers, int totalCapacity) {
    return ((long)totalCapacity << 32) | countOfBuffers;
  }

  @VisibleForTesting
  static long subtractOneBufferFromState(long state, int capacity) {
    return state - ((long)capacity << 32) - 1;
  }

  // For reporting, only used in the log
  private final AtomicLong allocationsRef = new AtomicLong();

  /**
   * @param maxByteBufferSizeToCache
   * @param initialByteBufferSize
   * @param maxToCache
   */
  public BoundedByteBufferPool(final int maxByteBufferSizeToCache, final int initialByteBufferSize,
      final int maxToCache) {
    this.maxByteBufferSizeToCache = maxByteBufferSizeToCache;
    this.runningAverageRef = new AtomicInteger(initialByteBufferSize);
    this.maxToCache = maxToCache;
  }

  public ByteBuffer getBuffer() {
    ByteBuffer bb = buffers.poll();
    if (bb != null) {
      long state;
      while (true) {
        long prevState = stateRef.get();
        state = subtractOneBufferFromState(prevState, bb.capacity());
        if (stateRef.compareAndSet(prevState, state)) {
          break;
        }
      }
      // Clear sets limit == capacity. Postion == 0.
      bb.clear();

      if (LOG.isTraceEnabled()) {
        int countOfBuffers = toCountOfBuffers(state);
        int totalCapacity = toTotalCapacity(state);
        LOG.trace("totalCapacity=" + totalCapacity + ", count=" + countOfBuffers);
      }
      return bb;
    }

    int runningAverage = runningAverageRef.get();
    bb = ByteBuffer.allocateDirect(runningAverage);

    if (LOG.isTraceEnabled()) {
      long allocations = allocationsRef.incrementAndGet();
      LOG.trace("runningAverage=" + runningAverage + ", alloctions=" + allocations);
    }
    return bb;
  }

  public void putBuffer(ByteBuffer bb) {
    // If buffer is larger than we want to keep around, just let it go.
    if (bb.capacity() > maxByteBufferSizeToCache) {
      return;
    }

    int countOfBuffers;
    int totalCapacity;
    while (true) {
      long prevState = stateRef.get();
      countOfBuffers = toCountOfBuffers(prevState);
      if (countOfBuffers >= maxToCache) {
        if (LOG.isWarnEnabled()) {
          LOG.warn("At capacity: " + countOfBuffers);
        }
        return;
      }
      countOfBuffers++;
      assert 0 < countOfBuffers && countOfBuffers <= maxToCache;

      totalCapacity = toTotalCapacity(prevState) + bb.capacity();
      if (totalCapacity < 0) {
        if (LOG.isWarnEnabled()) {
          LOG.warn("Overflowed total capacity.");
        }
        return;
      }

      long state = toState(countOfBuffers, totalCapacity);
      if (stateRef.compareAndSet(prevState, state)) {
        break;
      }
    }

    // ConcurrentLinkQueue#offer says "this method will never return false"
    buffers.offer(bb);

    int runningAverageUpdate = Math.min(
        totalCapacity / countOfBuffers, // size will never be 0.
        maxByteBufferSizeToCache);
    while (true) {
      int prev = runningAverageRef.get();
      if (prev >= runningAverageUpdate || // only rises, never recedes
          runningAverageRef.compareAndSet(prev, runningAverageUpdate)) {
        break;
      }
    }
  }
}
