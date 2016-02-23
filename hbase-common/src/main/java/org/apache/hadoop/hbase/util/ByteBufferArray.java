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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.MultiByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.util.StringUtils;

/**
 * This class manages an array of ByteBuffers with a default size 4MB. These
 * buffers are sequential and could be considered as a large buffer.It supports
 * reading/writing data from this large buffer with a position and offset
 */
@InterfaceAudience.Private
public final class ByteBufferArray {
  private static final Log LOG = LogFactory.getLog(ByteBufferArray.class);

  public static final int DEFAULT_BUFFER_SIZE = 4 * 1024 * 1024;
  private ByteBuffer buffers[];
  private Lock locks[];
  private int bufferSize;
  private int bufferCount;
  private ByteBufferAllocator allocator;
  /**
   * We allocate a number of byte buffers as the capacity. In order not to out
   * of the array bounds for the last byte(see {@link ByteBufferArray#multiple}),
   * we will allocate one additional buffer with capacity 0;
   * @param capacity total size of the byte buffer array
   * @param directByteBuffer true if we allocate direct buffer
   * @param allocator the ByteBufferAllocator that will create the buffers
   * @throws IOException throws IOException if there is an exception thrown by the allocator
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
    locks = new Lock[bufferCount + 1];
    this.allocator = allocator;
    for (int i = 0; i <= bufferCount; i++) {
      locks[i] = new ReentrantLock();
      if (i < bufferCount) {
        buffers[i] = allocator.allocate(bufferSize, directByteBuffer);
      } else {
        // always create on heap
        buffers[i] = ByteBuffer.allocate(0);
      }
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
    public void visit(ByteBuffer bb, byte[] array, int arrayIdx, int len) {
      bb.get(array, arrayIdx, len);
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
    public void visit(ByteBuffer bb, byte[] array, int arrayIdx, int len) {
      bb.put(array, arrayIdx, len);
    }
  };

  private interface Visitor {
    /**
     * Visit the given byte buffer, if it is a read action, we will transfer the
     * bytes from the buffer to the destination array, else if it is a write
     * action, we will transfer the bytes from the source array to the buffer
     * @param bb byte buffer
     * @param array a source or destination byte array
     * @param arrayOffset offset of the byte array
     * @param len read/write length
     */
    void visit(ByteBuffer bb, byte[] array, int arrayOffset, int len);
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
    assert endBuffer >= 0 && endBuffer < bufferCount
        || (endBuffer == bufferCount && endOffset == 0);
    if (startBuffer >= locks.length || startBuffer < 0) {
      String msg = "Failed multiple, start=" + start + ",startBuffer="
          + startBuffer + ",bufferSize=" + bufferSize;
      LOG.error(msg);
      throw new RuntimeException(msg);
    }
    int srcIndex = 0, cnt = -1;
    for (int i = startBuffer; i <= endBuffer; ++i) {
      Lock lock = locks[i];
      lock.lock();
      try {
        ByteBuffer bb = buffers[i];
        if (i == startBuffer) {
          cnt = bufferSize - startOffset;
          if (cnt > len) cnt = len;
          bb.limit(startOffset + cnt).position(startOffset);
        } else if (i == endBuffer) {
          cnt = endOffset;
          bb.limit(cnt).position(0);
        } else {
          cnt = bufferSize;
          bb.limit(cnt).position(0);
        }
        visitor.visit(bb, array, srcIndex + arrayOffset, cnt);
        srcIndex += cnt;
      } finally {
        lock.unlock();
      }
    }
    assert srcIndex == len;
  }

  /**
   * Creates a ByteBuff from a given array of ByteBuffers from the given offset to the
   * length specified. For eg, if there are 4 buffers forming an array each with length 10 and
   * if we call asSubBuffer(5, 10) then we will create an MBB consisting of two BBs
   * and the first one be a BB from 'position' 5 to a 'length' 5 and the 2nd BB will be from
   * 'position' 0 to 'length' 5.
   * @param offset
   * @param len
   * @return a ByteBuff formed from the underlying ByteBuffers
   */
  public ByteBuff asSubByteBuff(long offset, int len) {
    assert len >= 0;
    long end = offset + len;
    int startBuffer = (int) (offset / bufferSize), startBufferOffset = (int) (offset % bufferSize);
    int endBuffer = (int) (end / bufferSize), endBufferOffset = (int) (end % bufferSize);
    // Last buffer in the array is a dummy one with 0 capacity. Avoid sending back that
    if (endBuffer == this.bufferCount) {
      endBuffer--;
      endBufferOffset = bufferSize;
    }
    assert startBuffer >= 0 && startBuffer < bufferCount;
    assert endBuffer >= 0 && endBuffer < bufferCount
        || (endBuffer == bufferCount && endBufferOffset == 0);
    if (startBuffer >= locks.length || startBuffer < 0) {
      String msg = "Failed subArray, start=" + offset + ",startBuffer=" + startBuffer
          + ",bufferSize=" + bufferSize;
      LOG.error(msg);
      throw new RuntimeException(msg);
    }
    int srcIndex = 0, cnt = -1;
    ByteBuffer[] mbb = new ByteBuffer[endBuffer - startBuffer + 1];
    for (int i = startBuffer,j=0; i <= endBuffer; ++i,j++) {
      Lock lock = locks[i];
      lock.lock();
      try {
        ByteBuffer bb = buffers[i];
        if (i == startBuffer) {
          cnt = bufferSize - startBufferOffset;
          if (cnt > len) cnt = len;
          ByteBuffer dup = bb.duplicate();
          dup.limit(startBufferOffset + cnt).position(startBufferOffset);
          mbb[j] = dup.slice();
        } else if (i == endBuffer) {
          cnt = endBufferOffset;
          ByteBuffer dup = bb.duplicate();
          dup.position(0).limit(cnt);
          mbb[j] = dup.slice();
        } else {
          cnt = bufferSize ;
          ByteBuffer dup = bb.duplicate();
          dup.position(0).limit(cnt);
          mbb[j] = dup.slice();
        }
        srcIndex += cnt;
      } finally {
        lock.unlock();
      }
    }
    assert srcIndex == len;
    if (mbb.length > 1) {
      return new MultiByteBuff(mbb);
    } else {
      return new SingleByteBuff(mbb[0]);
    }
  }
}
