/*
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

package org.apache.hadoop.hbase.ccsmap;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

@InterfaceAudience.Private
public abstract class AbstractHeapChunk implements HeapChunk {

  private final long chunkID;
  private final int capacity;
  private final boolean isPooled;

  private final AtomicInteger alignOccupancy = new AtomicInteger(0);

  protected final AtomicInteger nextFreeOffset = new AtomicInteger(0);
  protected ByteBuffer chunk;

  protected AbstractHeapChunk(long chunkID, int capacity, boolean isPooled) {
    this.chunkID = chunkID;
    this.capacity = capacity;
    this.isPooled = isPooled;
  }

  @Override
  public long getChunkID() {
    return chunkID;
  }

  @Override
  public int getPosition() {
    return nextFreeOffset.get();
  }

  @Override
  public int allocate(int len) {
    int oldLen = len;
    //TODO reuse the removed node's space.
    //TODO add config for support unalign
    //align
    len = align(len);

    while (true) {
      int oldOffset = nextFreeOffset.get();
      if (oldOffset + len > getLimit()) {
        return -1; // alloc doesn't fit
      }
      // Try to atomically claim this chunk
      if (nextFreeOffset.compareAndSet(oldOffset, oldOffset + len)) {
        // we got the alloc
        alignOccupancy.addAndGet(oldLen - len);
        return oldOffset;
      }
    }
  }

  private int align(int len) {
    return (len % 8 != 0) ? ((len / 8 + 1) * 8) : len;
  }

  @Override
  public int getLimit() {
    return capacity;
  }

  @Override
  public ByteBuffer getByteBuffer() {
    return chunk;
  }

  @Override
  public boolean isPooledChunk() {
    return isPooled;
  }

  @Override
  public ByteBuffer asSubByteBuffer(int offset, int len) {
    ByteBuffer duplicate = chunk.duplicate();
    duplicate.limit(offset + len);
    duplicate.position(offset);
    return duplicate.slice();
  }

  public abstract HeapMode getHeapMode();

  @Override
  public int occupancy() {
    return getLimit() - getPosition();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof AbstractHeapChunk)) {
      return false;
    }
    AbstractHeapChunk that = (AbstractHeapChunk) obj;
    return getChunkID() == that.getChunkID();
  }

  @Override
  public int hashCode() {
    return (int) (getChunkID() & CCSMapUtils.FOUR_BYTES_MARK);
  }

}
