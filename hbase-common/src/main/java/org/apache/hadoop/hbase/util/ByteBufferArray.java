/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.util;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.util.StringUtils;

/**
 * This class manages an array of ByteBuffers with a default size 4MB. These
 * buffers are sequential and could be considered as a large buffer.It supports
 * reading/writing data from this large buffer with a position and offset
 */
@InterfaceAudience.Private
public class ByteBufferArray {
  private static final Log LOG = LogFactory.getLog(ByteBufferArray.class);

  static final int DEFAULT_BUFFER_SIZE = 4 * 1024 * 1024;
  @VisibleForTesting
  ByteBuffer[] buffers;
  private int bufferSize;
  @VisibleForTesting
  int bufferCount;

  /**
   * We allocate a number of byte buffers as the capacity. In order not to out
   * of the array bounds for the last byte(see {@link ByteBufferArray#multiple}),
   * we will allocate one additional buffer with capacity 0;
   * @param capacity total size of the byte buffer array
   * @param directByteBuffer true if we allocate direct buffer
   */
  public ByteBufferArray(long capacity, boolean directByteBuffer, ByteBufferAllocator allocator)
      throws IOException {
    this.bufferSize = DEFAULT_BUFFER_SIZE;
    if (this.bufferSize > (capacity / 16))
      this.bufferSize = (int) roundUp(capacity / 16, 32768);
    this.bufferCount = (int) (roundUp(capacity, bufferSize) / bufferSize);
    LOG.info("Allocating buffers total=" + StringUtils.byteDesc(capacity)
        + ", sizePerBuffer=" + StringUtils.byteDesc(bufferSize) + ", count="
        + bufferCount + ", direct=" + directByteBuffer);
    buffers = new ByteBuffer[bufferCount + 1];
    createBuffers(directByteBuffer, allocator);
  }

  @VisibleForTesting
  void createBuffers(boolean directByteBuffer, ByteBufferAllocator allocator)
      throws IOException {
    int threadCount = getThreadCount();
    ExecutorService service = new ThreadPoolExecutor(threadCount, threadCount, 0L,
        TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    int perThreadCount = (int)Math.floor((double) (bufferCount) / threadCount);
    int lastThreadCount = bufferCount - (perThreadCount * (threadCount - 1));
    Future<ByteBuffer[]>[] futures = new Future[threadCount];
    try {
      for (int i = 0; i < threadCount; i++) {
        // Last thread will have to deal with a different number of buffers
        int buffersToCreate = (i == threadCount - 1) ? lastThreadCount : perThreadCount;
        futures[i] = service.submit(
          new BufferCreatorCallable(bufferSize, directByteBuffer, buffersToCreate, allocator));
      }
      int bufferIndex = 0;
      for (Future<ByteBuffer[]> future : futures) {
        try {
          ByteBuffer[] buffers = future.get();
          for (ByteBuffer buffer : buffers) {
            this.buffers[bufferIndex++] = buffer;
          }
        } catch (InterruptedException | ExecutionException e) {
          LOG.error("Buffer creation interrupted", e);
          throw new IOException(e);
        }
      }
    } finally {
      service.shutdownNow();
    }
    // always create on heap empty dummy buffer at last
    this.buffers[bufferCount] = ByteBuffer.allocate(0);
  }

  @VisibleForTesting
  int getThreadCount() {
    return Runtime.getRuntime().availableProcessors();
  }

  /**
   * A callable that creates buffers of the specified length either onheap/offheap using the
   * {@link ByteBufferAllocator}
   */
  private static class BufferCreatorCallable implements Callable<ByteBuffer[]> {
    private final int bufferCapacity;
    private final boolean directByteBuffer;
    private final int bufferCount;
    private final ByteBufferAllocator allocator;

    BufferCreatorCallable(int bufferCapacity, boolean directByteBuffer, int bufferCount,
        ByteBufferAllocator allocator) {
      this.bufferCapacity = bufferCapacity;
      this.directByteBuffer = directByteBuffer;
      this.bufferCount = bufferCount;
      this.allocator = allocator;
    }

    @Override
    public ByteBuffer[] call() throws Exception {
      ByteBuffer[] buffers = new ByteBuffer[this.bufferCount];
      for (int i = 0; i < this.bufferCount; i++) {
        buffers[i] = allocator.allocate(this.bufferCapacity, this.directByteBuffer);
      }
      return buffers;

    }
  }

  private long roundUp(long n, long to) {
    return ((n + to - 1) / to) * to;
  }

  /**
   * Transfers bytes from this buffer array into the given destination array
   * @param start start position in the ByteBufferArray
   * @param len The maximum number of bytes to be written to the given array
   * @param dstArray The array into which bytes are to be written
   * @return number of bytes read
   */
  public int getMultiple(long start, int len, byte[] dstArray) {
    return getMultiple(start, len, dstArray, 0);
  }

  /**
   * Transfers bytes from this buffer array into the given destination array
   * @param start start offset of this buffer array
   * @param len The maximum number of bytes to be written to the given array
   * @param dstArray The array into which bytes are to be written
   * @param dstOffset The offset within the given array of the first byte to be
   *          written
   * @return number of bytes read
   */
  public int getMultiple(long start, int len, byte[] dstArray, int dstOffset) {
    multiple(start, len, dstArray, dstOffset, GET_MULTIPLE_VISTOR);
    return len;
  }

  private final static Visitor GET_MULTIPLE_VISTOR = new Visitor() {
    @Override
    public void visit(ByteBuffer bb, int pos, byte[] array, int arrayIdx, int len) {
      ByteBufferUtils.copyFromBufferToArray(array, bb, pos, arrayIdx, len);
    }
  };

  /**
   * Transfers bytes from the given source array into this buffer array
   * @param start start offset of this buffer array
   * @param len The maximum number of bytes to be read from the given array
   * @param srcArray The array from which bytes are to be read
   */
  public void putMultiple(long start, int len, byte[] srcArray) {
    putMultiple(start, len, srcArray, 0);
  }

  /**
   * Transfers bytes from the given source array into this buffer array
   * @param start start offset of this buffer array
   * @param len The maximum number of bytes to be read from the given array
   * @param srcArray The array from which bytes are to be read
   * @param srcOffset The offset within the given array of the first byte to be
   *          read
   */
  public void putMultiple(long start, int len, byte[] srcArray, int srcOffset) {
    multiple(start, len, srcArray, srcOffset, PUT_MULTIPLE_VISITOR);
  }

  private final static Visitor PUT_MULTIPLE_VISITOR = new Visitor() {
    @Override
    public void visit(ByteBuffer bb, int pos, byte[] array, int arrayIdx, int len) {
      ByteBufferUtils.copyFromArrayToBuffer(bb, pos, array, arrayIdx, len);
    }
  };

  private interface Visitor {
    /**
     * Visit the given byte buffer, if it is a read action, we will transfer the
     * bytes from the buffer to the destination array, else if it is a write
     * action, we will transfer the bytes from the source array to the buffer
     * @param bb byte buffer
     * @param pos Start position in ByteBuffer
     * @param array a source or destination byte array
     * @param arrayOffset offset of the byte array
     * @param len read/write length
     */
    void visit(ByteBuffer bb, int pos, byte[] array, int arrayOffset, int len);
  }

  /**
   * Access(read or write) this buffer array with a position and length as the
   * given array. Here we will only lock one buffer even if it may be need visit
   * several buffers. The consistency is guaranteed by the caller.
   * @param start start offset of this buffer array
   * @param len The maximum number of bytes to be accessed
   * @param array The array from/to which bytes are to be read/written
   * @param arrayOffset The offset within the given array of the first byte to
   *          be read or written
   * @param visitor implement of how to visit the byte buffer
   */
  void multiple(long start, int len, byte[] array, int arrayOffset, Visitor visitor) {
    assert len >= 0;
    long end = start + len;
    int startBuffer = (int) (start / bufferSize), startOffset = (int) (start % bufferSize);
    int endBuffer = (int) (end / bufferSize), endOffset = (int) (end % bufferSize);
    assert array.length >= len + arrayOffset;
    assert startBuffer >= 0 && startBuffer < bufferCount;
    assert (endBuffer >= 0 && endBuffer < bufferCount)
        || (endBuffer == bufferCount && endOffset == 0);
    if (startBuffer >= buffers.length || startBuffer < 0) {
      String msg = "Failed multiple, start=" + start + ",startBuffer="
          + startBuffer + ",bufferSize=" + bufferSize;
      LOG.error(msg);
      throw new RuntimeException(msg);
    }
    int srcIndex = 0, cnt = -1;
    for (int i = startBuffer; i <= endBuffer; ++i) {
      ByteBuffer bb = buffers[i].duplicate();
      int pos = 0;
      if (i == startBuffer) {
        cnt = bufferSize - startOffset;
        if (cnt > len) {
          cnt = len;
        }
        pos = startOffset;
      } else if (i == endBuffer) {
        cnt = endOffset;
      } else {
        cnt = bufferSize;
      }
      visitor.visit(bb, pos, array, srcIndex + arrayOffset, cnt);
      srcIndex += cnt;
    }
    assert srcIndex == len;
  }
}
