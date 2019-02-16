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
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.MultiByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * ByteBuffAllocator is used for allocating/freeing the ByteBuffers from/to NIO ByteBuffer pool, and
 * it provide high-level interfaces for upstream. when allocating desired memory size, it will
 * return {@link ByteBuff}, if we are sure that those ByteBuffers have reached the end of life
 * cycle, we must do the {@link ByteBuff#release()} to return back the buffers to the pool,
 * otherwise ByteBuffers leak will happen, and the NIO ByteBuffer pool may be exhausted. there's
 * possible that the desired memory size is large than ByteBufferPool has, we'll downgrade to
 * allocate ByteBuffers from heap which meaning the GC pressure may increase again. Of course, an
 * better way is increasing the ByteBufferPool size if we detected this case. <br/>
 * <br/>
 * On the other hand, for better memory utilization, we have set an lower bound named
 * minSizeForReservoirUse in this allocator, and if the desired size is less than
 * minSizeForReservoirUse, the allocator will just allocate the ByteBuffer from heap and let the JVM
 * free its memory, because it's too wasting to allocate a single fixed-size ByteBuffer for some
 * small objects. <br/>
 * <br/>
 * We recommend to use this class to allocate/free {@link ByteBuff} in the RPC layer or the entire
 * read/write path, because it hide the details of memory management and its APIs are more friendly
 * to the upper layer.
 */
@InterfaceAudience.Private
public class ByteBuffAllocator {

  private static final Logger LOG = LoggerFactory.getLogger(ByteBuffAllocator.class);

  public static final String MAX_BUFFER_COUNT_KEY = "hbase.ipc.server.allocator.max.buffer.count";

  public static final String BUFFER_SIZE_KEY = "hbase.ipc.server.allocator.buffer.size";
  // 64 KB. Making it same as the chunk size what we will write/read to/from the socket channel.
  public static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

  public static final String MIN_ALLOCATE_SIZE_KEY =
      "hbase.ipc.server.reservoir.minimal.allocating.size";

  public static final Recycler NONE = () -> {
  };

  public interface Recycler {
    void free();
  }

  private final boolean reservoirEnabled;
  private final int bufSize;
  private final int maxBufCount;
  private final AtomicInteger usedBufCount = new AtomicInteger(0);

  private boolean maxPoolSizeInfoLevelLogged = false;

  // If the desired size is at least this size, it'll allocated from ByteBufferPool, otherwise it'll
  // allocated from heap for better utilization. We make this to be 1/6th of the pool buffer size.
  private final int minSizeForReservoirUse;

  private final Queue<ByteBuffer> buffers = new ConcurrentLinkedQueue<>();

  /**
   * Initialize an {@link ByteBuffAllocator} which will try to allocate ByteBuffers from off-heap if
   * reservoir is enabled and the reservoir has enough buffers, otherwise the allocator will just
   * allocate the insufficient buffers from on-heap to meet the requirement.
   * @param conf which get the arguments to initialize the allocator.
   * @param reservoirEnabled indicate whether the reservoir is enabled or disabled.
   * @return ByteBuffAllocator to manage the byte buffers.
   */
  public static ByteBuffAllocator create(Configuration conf, boolean reservoirEnabled) {
    int poolBufSize = conf.getInt(BUFFER_SIZE_KEY, DEFAULT_BUFFER_SIZE);
    if (reservoirEnabled) {
      // The max number of buffers to be pooled in the ByteBufferPool. The default value been
      // selected based on the #handlers configured. When it is read request, 2 MB is the max size
      // at which we will send back one RPC request. Means max we need 2 MB for creating the
      // response cell block. (Well it might be much lesser than this because in 2 MB size calc, we
      // include the heap size overhead of each cells also.) Considering 2 MB, we will need
      // (2 * 1024 * 1024) / poolBufSize buffers to make the response cell block. Pool buffer size
      // is by default 64 KB.
      // In case of read request, at the end of the handler process, we will make the response
      // cellblock and add the Call to connection's response Q and a single Responder thread takes
      // connections and responses from that one by one and do the socket write. So there is chances
      // that by the time a handler originated response is actually done writing to socket and so
      // released the BBs it used, the handler might have processed one more read req. On an avg 2x
      // we consider and consider that also for the max buffers to pool
      int bufsForTwoMB = (2 * 1024 * 1024) / poolBufSize;
      int maxBuffCount =
          conf.getInt(MAX_BUFFER_COUNT_KEY, conf.getInt(HConstants.REGION_SERVER_HANDLER_COUNT,
            HConstants.DEFAULT_REGION_SERVER_HANDLER_COUNT) * bufsForTwoMB * 2);
      int minSizeForReservoirUse = conf.getInt(MIN_ALLOCATE_SIZE_KEY, poolBufSize / 6);
      return new ByteBuffAllocator(true, maxBuffCount, poolBufSize, minSizeForReservoirUse);
    } else {
      return new ByteBuffAllocator(false, 0, poolBufSize, Integer.MAX_VALUE);
    }
  }

  /**
   * Initialize an {@link ByteBuffAllocator} which only allocate ByteBuffer from on-heap, it's
   * designed for testing purpose or disabled reservoir case.
   * @return allocator to allocate on-heap ByteBuffer.
   */
  public static ByteBuffAllocator createOnHeap() {
    return new ByteBuffAllocator(false, 0, DEFAULT_BUFFER_SIZE, Integer.MAX_VALUE);
  }

  @VisibleForTesting
  ByteBuffAllocator(boolean reservoirEnabled, int maxBufCount, int bufSize,
      int minSizeForReservoirUse) {
    this.reservoirEnabled = reservoirEnabled;
    this.maxBufCount = maxBufCount;
    this.bufSize = bufSize;
    this.minSizeForReservoirUse = minSizeForReservoirUse;
  }

  public boolean isReservoirEnabled() {
    return reservoirEnabled;
  }

  @VisibleForTesting
  public int getQueueSize() {
    return this.buffers.size();
  }

  /**
   * Allocate an buffer with buffer size from ByteBuffAllocator, Note to call the
   * {@link ByteBuff#release()} if no need any more, otherwise the memory leak happen in NIO
   * ByteBuffer pool.
   * @return an ByteBuff with the buffer size.
   */
  public SingleByteBuff allocateOneBuffer() {
    if (isReservoirEnabled()) {
      ByteBuffer bb = getBuffer();
      if (bb != null) {
        return new SingleByteBuff(() -> putbackBuffer(bb), bb);
      }
    }
    // Allocated from heap, let the JVM free its memory.
    return new SingleByteBuff(NONE, ByteBuffer.allocate(this.bufSize));
  }

  /**
   * Allocate size bytes from the ByteBufAllocator, Note to call the {@link ByteBuff#release()} if
   * no need any more, otherwise the memory leak happen in NIO ByteBuffer pool.
   * @param size to allocate
   * @return an ByteBuff with the desired size.
   */
  public ByteBuff allocate(int size) {
    if (size < 0) {
      throw new IllegalArgumentException("size to allocate should >=0");
    }
    // If disabled the reservoir, just allocate it from on-heap.
    if (!isReservoirEnabled() || size == 0) {
      return new SingleByteBuff(NONE, ByteBuffer.allocate(size));
    }
    int reminder = size % bufSize;
    int len = size / bufSize + (reminder > 0 ? 1 : 0);
    List<ByteBuffer> bbs = new ArrayList<>(len);
    // Allocate from ByteBufferPool until the remaining is less than minSizeForReservoirUse or
    // reservoir is exhausted.
    int remain = size;
    while (remain >= minSizeForReservoirUse) {
      ByteBuffer bb = this.getBuffer();
      if (bb == null) {
        break;
      }
      bbs.add(bb);
      remain -= bufSize;
    }
    int lenFromReservoir = bbs.size();
    if (remain > 0) {
      // If the last ByteBuffer is too small or the reservoir can not provide more ByteBuffers, we
      // just allocate the ByteBuffer from on-heap.
      bbs.add(ByteBuffer.allocate(remain));
    }
    ByteBuff bb = wrap(bbs, () -> {
      for (int i = 0; i < lenFromReservoir; i++) {
        this.putbackBuffer(bbs.get(i));
      }
    });
    bb.limit(size);
    return bb;
  }

  public static ByteBuff wrap(ByteBuffer[] buffers, Recycler recycler) {
    if (buffers == null || buffers.length == 0) {
      throw new IllegalArgumentException("buffers shouldn't be null or empty");
    }
    return buffers.length == 1 ? new SingleByteBuff(recycler, buffers[0])
        : new MultiByteBuff(recycler, buffers);
  }

  public static ByteBuff wrap(ByteBuffer[] buffers) {
    return wrap(buffers, NONE);
  }

  public static ByteBuff wrap(List<ByteBuffer> buffers, Recycler recycler) {
    if (buffers == null || buffers.size() == 0) {
      throw new IllegalArgumentException("buffers shouldn't be null or empty");
    }
    return buffers.size() == 1 ? new SingleByteBuff(recycler, buffers.get(0))
        : new MultiByteBuff(recycler, buffers.toArray(new ByteBuffer[0]));
  }

  public static ByteBuff wrap(List<ByteBuffer> buffers) {
    return wrap(buffers, NONE);
  }

  /**
   * @return One free DirectByteBuffer from the pool. If no free ByteBuffer and we have not reached
   *         the maximum pool size, it will create a new one and return. In case of max pool size
   *         also reached, will return null. When pool returned a ByteBuffer, make sure to return it
   *         back to pool after use.
   */
  private ByteBuffer getBuffer() {
    ByteBuffer bb = buffers.poll();
    if (bb != null) {
      // To reset the limit to capacity and position to 0, must clear here.
      bb.clear();
      return bb;
    }
    while (true) {
      int c = this.usedBufCount.intValue();
      if (c >= this.maxBufCount) {
        if (!maxPoolSizeInfoLevelLogged) {
          LOG.info("Pool already reached its max capacity : {} and no free buffers now. Consider "
              + "increasing the value for '{}' ?",
            maxBufCount, MAX_BUFFER_COUNT_KEY);
          maxPoolSizeInfoLevelLogged = true;
        }
        return null;
      }
      if (!this.usedBufCount.compareAndSet(c, c + 1)) {
        continue;
      }
      return ByteBuffer.allocateDirect(bufSize);
    }
  }

  /**
   * Return back a ByteBuffer after its use. Don't read/write the ByteBuffer after the returning.
   * @param buf ByteBuffer to return.
   */
  private void putbackBuffer(ByteBuffer buf) {
    if (buf.capacity() != bufSize || (reservoirEnabled ^ buf.isDirect())) {
      LOG.warn("Trying to put a buffer, not created by this pool! Will be just ignored");
      return;
    }
    buffers.offer(buf);
  }
}
