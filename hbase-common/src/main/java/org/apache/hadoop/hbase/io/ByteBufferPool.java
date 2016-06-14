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

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Like Hadoops' ByteBufferPool only you do not specify desired size when getting a ByteBuffer. This
 * pool keeps an upper bound on the count of ByteBuffers in the pool and a fixed size of ByteBuffer
 * that it will create. When requested, if a free ByteBuffer is already present, it will return
 * that. And when no free ByteBuffer available and we are below the max count, it will create a new
 * one and return that.
 *
 * <p>
 * Note: This pool returns off heap ByteBuffers by default. If on heap ByteBuffers to be pooled,
 * pass 'directByteBuffer' as false while construction of the pool.
 * <p>
 * This class is thread safe.
 *
 * @see ByteBufferListOutputStream
 */
@InterfaceAudience.Private
public class ByteBufferPool {
  private static final Log LOG = LogFactory.getLog(ByteBufferPool.class);
  // TODO better config names?
  // hbase.ipc.server.reservoir.initial.max -> hbase.ipc.server.reservoir.max.buffer.count
  // hbase.ipc.server.reservoir.initial.buffer.size -> hbase.ipc.server.reservoir.buffer.size
  public static final String MAX_POOL_SIZE_KEY = "hbase.ipc.server.reservoir.initial.max";
  public static final String BUFFER_SIZE_KEY = "hbase.ipc.server.reservoir.initial.buffer.size";
  public static final int DEFAULT_BUFFER_SIZE = 64 * 1024;// 64 KB. Making it same as the chunk size
                                                          // what we will write/read to/from the
                                                          // socket channel.
  private final Queue<ByteBuffer> buffers = new ConcurrentLinkedQueue<ByteBuffer>();

  private final int bufferSize;
  private final int maxPoolSize;
  private AtomicInteger count; // Count of the BBs created already for this pool.
  private final boolean directByteBuffer; //Whether this pool should return DirectByteBuffers
  private boolean maxPoolSizeInfoLevelLogged = false;

  /**
   * @param bufferSize Size of each buffer created by this pool.
   * @param maxPoolSize Max number of buffers to keep in this pool.
   */
  public ByteBufferPool(int bufferSize, int maxPoolSize) {
    this(bufferSize, maxPoolSize, true);
  }

  /**
   * @param bufferSize Size of each buffer created by this pool.
   * @param maxPoolSize Max number of buffers to keep in this pool.
   * @param directByteBuffer Whether to create direct ByteBuffer or on heap ByteBuffer.
   */
  public ByteBufferPool(int bufferSize, int maxPoolSize, boolean directByteBuffer) {
    this.bufferSize = bufferSize;
    this.maxPoolSize = maxPoolSize;
    this.directByteBuffer = directByteBuffer;
    // TODO can add initialPoolSize config also and make those many BBs ready for use.
    LOG.info("Created ByteBufferPool with bufferSize : " + bufferSize + " and maxPoolSize : "
        + maxPoolSize);
    this.count = new AtomicInteger(0);
  }

  /**
   * @return One free ByteBuffer from the pool. If no free ByteBuffer and we have not reached the
   *         maximum pool size, it will create a new one and return. In case of max pool size also
   *         reached, will return null. When pool returned a ByteBuffer, make sure to return it back
   *         to pool after use.
   * @see #putbackBuffer(ByteBuffer)
   */
  public ByteBuffer getBuffer() {
    ByteBuffer bb = buffers.poll();
    if (bb != null) {
      // Clear sets limit == capacity. Position == 0.
      bb.clear();
      return bb;
    }
    while (true) {
      int c = this.count.intValue();
      if (c >= this.maxPoolSize) {
        if (maxPoolSizeInfoLevelLogged) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Pool already reached its max capacity : " + this.maxPoolSize
                + " and no free buffers now. Consider increasing the value for '"
                + MAX_POOL_SIZE_KEY + "' ?");
          }
        } else {
          LOG.info("Pool already reached its max capacity : " + this.maxPoolSize
              + " and no free buffers now. Consider increasing the value for '" + MAX_POOL_SIZE_KEY
              + "' ?");
          maxPoolSizeInfoLevelLogged = true;
        }
        return null;
      }
      if (!this.count.compareAndSet(c, c + 1)) {
        continue;
      }
      if (LOG.isTraceEnabled()) {
        LOG.trace("Creating a new offheap ByteBuffer of size: " + this.bufferSize);
      }
      return this.directByteBuffer ? ByteBuffer.allocateDirect(this.bufferSize)
          : ByteBuffer.allocate(this.bufferSize);
    }
  }

  /**
   * Return back a ByteBuffer after its use. Do not try to return put back a ByteBuffer, not
   * obtained from this pool.
   * @param buf ByteBuffer to return.
   */
  public void putbackBuffer(ByteBuffer buf) {
    if (buf.capacity() != this.bufferSize || (this.directByteBuffer ^ buf.isDirect())) {
      LOG.warn("Trying to put a buffer, not created by this pool! Will be just ignored");
      return;
    }
    buffers.offer(buf);
  }

  int getBufferSize() {
    return this.bufferSize;
  }

  /**
   * @return Number of free buffers
   */
  @VisibleForTesting
  int getQueueSize() {
    return buffers.size();
  }
}
