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
package org.apache.hadoop.hbase.io.hfile.bucket;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.CacheableDeserializer;
import org.apache.hadoop.hbase.io.hfile.Cacheable.MemoryType;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.util.ByteBufferAllocator;
import org.apache.hadoop.hbase.util.ByteBufferArray;

/**
 * IO engine that stores data in memory using an array of ByteBuffers
 * {@link ByteBufferArray}.
 *
 *<h2>How it Works</h2>
 * First, see {@link ByteBufferArray} and how it gives a view across multiple ByteBuffers managed
 * by it internally. This class does the physical BB create and the write and read to the
 * underlying BBs. So we will create N BBs based on the total BC capacity specified on create
 * of the ByteBufferArray. So say we have 10 GB of off heap BucketCache, we will create 2560 such
 * BBs inside our ByteBufferArray.
 * 
 * <p>Now the way BucketCache works is that the entire 10 GB is split into diff sized buckets: by
 * default from 5 KB to 513 KB. Within each bucket of a particular size, there are
 * usually more than one bucket 'block'. The way it is calculate in bucketcache is that the total
 * bucketcache size is divided by 4 (hard-coded currently) * max size option. So using defaults,
 * buckets will be is 4 * 513kb (the biggest default value) = 2052kb. A bucket of 2052kb at offset
 * zero will serve out bucket 'blocks' of 5kb, the next bucket will do the next size up and so on
 * up to the maximum (default) of 513kb).
 * 
 * <p>When we write blocks to the bucketcache, we will see which bucket size group it best fits.
 * So a 4 KB block size goes to the 5 KB size group. Each of the block writes, writes within its
 * appropriate bucket. Though the bucket is '4kb' in size, it will occupy one of the 
 * 5 KB bucket 'blocks' (even if actual size of the bucket is less). Bucket 'blocks' will not span
 * buckets.
 * 
 * <p>But you can see the physical memory under the bucket 'blocks' can be split across the
 * underlying backing BBs from ByteBufferArray. All is split into 4 MB sized BBs.
 * 
 * <p>Each Bucket knows its offset in the entire space of BC and when block is written the offset
 * arrives at ByteBufferArray and it figures which BB to write to. It may so happen that the entire
 * block to be written does not fit a particular backing ByteBufferArray so the remainder goes to
 * another BB. See {@link ByteBufferArray#putMultiple(long, int, byte[])}.

So said all these, when we read a block it may be possible that the bytes of that blocks is physically placed in 2 adjucent BBs.  In such case also, we avoid any copy need by having the MBB...
 */
@InterfaceAudience.Private
public class ByteBufferIOEngine implements IOEngine {
  private ByteBufferArray bufferArray;
  private final long capacity;
  private final boolean direct;

  /**
   * Construct the ByteBufferIOEngine with the given capacity
   * @param capacity
   * @param direct true if allocate direct buffer
   * @throws IOException ideally here no exception to be thrown from the allocator
   */
  public ByteBufferIOEngine(long capacity, boolean direct)
      throws IOException {
    this.capacity = capacity;
    this.direct = direct;
    ByteBufferAllocator allocator = new ByteBufferAllocator() {
      @Override
      public ByteBuffer allocate(long size, boolean directByteBuffer)
          throws IOException {
        if (directByteBuffer) {
          return ByteBuffer.allocateDirect((int) size);
        } else {
          return ByteBuffer.allocate((int) size);
        }
      }
    };
    bufferArray = new ByteBufferArray(capacity, direct, allocator);
  }

  @Override
  public String toString() {
    return "ioengine=" + this.getClass().getSimpleName() + ", capacity=" +
      String.format("%,d", this.capacity) + ", direct=" + this.direct;
  }

  /**
   * Memory IO engine is always unable to support persistent storage for the
   * cache
   * @return false
   */
  @Override
  public boolean isPersistent() {
    return false;
  }

  @Override
  public Cacheable read(long offset, int length, CacheableDeserializer<Cacheable> deserializer)
      throws IOException {
    ByteBuff dstBuffer = bufferArray.asSubByteBuff(offset, length);
    // Here the buffer that is created directly refers to the buffer in the actual buckets.
    // When any cell is referring to the blocks created out of these buckets then it means that
    // those cells are referring to a shared memory area which if evicted by the BucketCache would
    // lead to corruption of results. Hence we set the type of the buffer as SHARED_MEMORY
    // so that the readers using this block are aware of this fact and do the necessary action
    // to prevent eviction till the results are either consumed or copied
    return deserializer.deserialize(dstBuffer, true, MemoryType.SHARED);
  }

  /**
   * Transfers data from the given byte buffer to the buffer array
   * @param srcBuffer the given byte buffer from which bytes are to be read
   * @param offset The offset in the ByteBufferArray of the first byte to be
   *          written
   * @throws IOException throws IOException if writing to the array throws exception
   */
  @Override
  public void write(ByteBuffer srcBuffer, long offset) throws IOException {
    assert srcBuffer.hasArray();
    bufferArray.putMultiple(offset, srcBuffer.remaining(), srcBuffer.array(),
        srcBuffer.arrayOffset());
  }

  @Override
  public void write(ByteBuff srcBuffer, long offset) throws IOException {
    // When caching block into BucketCache there will be single buffer backing for this HFileBlock.
    // This will work for now. But from the DFS itself if we get DBB then this may not hold true.
    assert srcBuffer.hasArray();
    bufferArray.putMultiple(offset, srcBuffer.remaining(), srcBuffer.array(),
        srcBuffer.arrayOffset());
  }
  /**
   * No operation for the sync in the memory IO engine
   */
  @Override
  public void sync() {
    // Nothing to do.
  }

  /**
   * No operation for the shutdown in the memory IO engine
   */
  @Override
  public void shutdown() {
    // Nothing to do.
  }
}
