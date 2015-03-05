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
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

import com.google.common.annotations.VisibleForTesting;

/**
 * Like Hadoops' ByteBufferPool only you do not specify desired size when getting a ByteBuffer. We
 * also keep upper bounds on ByteBuffer size and amount of ByteBuffers we keep int the pool hence
 * it is 'bounded' as opposed to 'elastic' as in ElasticByteBuffferPool If a ByteBuffer is bigger
 * than a threshold, we will just let the ByteBuffer go rather than keep it around. If more
 * ByteBuffers than configured maximum instances, then we do not cache either (we will log a
 * WARN in this case).
 * 
 * <p>The intended use case is a reservoir of bytebuffers that an RPC can reuse; buffers tend to
 * achieve a particular 'run' size over time give or take a few extremes.
 * 
 * <p>Thread safe.
 */
@InterfaceAudience.Private
public class BoundedByteBufferPool {
  private final Log LOG = LogFactory.getLog(this.getClass());

  private final class Key implements Comparable<Key> {
    private final int capacity;

    Key(final int capacity) {
      this.capacity = capacity;
    }

    @Override
    public int compareTo(Key that) {
      if (this.capacity < that.capacity) return -1;
      if (this.capacity > that.capacity) return 1;
      return this.hashCode() - that.hashCode();
    }
  }

  @VisibleForTesting
  final NavigableMap<Key, ByteBuffer> buffers = new TreeMap<Key, ByteBuffer>();

  private final int maxByteBufferSizeToCache;
  private final int maxToCache;
  // A running average only it just rises, never recedes
  private int runningAverage;
  private int totalReservoirCapacity;

  /**
   * @param maxByteBufferSizeToCache
   * @param initialByteBufferSize
   * @param maxToCache
   */
  public BoundedByteBufferPool(final int maxByteBufferSizeToCache, final int initialByteBufferSize,
      final int maxToCache) {
    this.maxByteBufferSizeToCache = maxByteBufferSizeToCache;
    this.runningAverage = initialByteBufferSize;
    this.maxToCache = maxToCache;
  }

  public synchronized ByteBuffer getBuffer() {
    Key key = this.buffers.isEmpty()? null: this.buffers.firstKey();
    ByteBuffer bb = null;
    if (key == null) {
      bb = ByteBuffer.allocate(this.runningAverage);
    } else {
      bb = this.buffers.remove(key);
      if (bb ==  null) throw new IllegalStateException();
      bb.clear();
      this.totalReservoirCapacity -= bb.capacity();
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("runningAverage=" + this.runningAverage +
        ", totalCapacity=" + this.totalReservoirCapacity + ", count=" + this.buffers.size());
    }
    return bb;
  }

  public synchronized void putBuffer(ByteBuffer buffer) {
    // If buffer is larger than we want to keep around, just let it go.
    if (buffer.capacity() > this.maxByteBufferSizeToCache) return;
    // futureSize is how many byte buffers the reservoir will have if this method succeeds.
    int futureSize = this.buffers.size() + 1;
    if (futureSize > this.maxToCache) {
      // If at max size, something is wrong. WARN.
      if (LOG.isWarnEnabled()) LOG.warn("At capacity: " + futureSize);
      return;
    }
    this.totalReservoirCapacity += buffer.capacity();
    int average = this.totalReservoirCapacity / futureSize;
    if (average > this.runningAverage && average < this.maxByteBufferSizeToCache) {
      this.runningAverage = average;
    }
    this.buffers.put(new Key(buffer.capacity()), buffer);
  }
}