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
import org.apache.hadoop.hbase.util.ByteBufferArray;

/**
 * IO engine that stores data on the memory using an array of ByteBuffers
 * {@link ByteBufferArray}
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
   * @throws IOException
   */
  public ByteBufferIOEngine(long capacity, boolean direct)
      throws IOException {
    this.capacity = capacity;
    this.direct = direct;
    bufferArray = new ByteBufferArray(capacity, direct);
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

  /**
   * Transfers data from the buffer array to the given byte buffer
   * @param dstBuffer the given byte buffer into which bytes are to be written
   * @param offset The offset in the ByteBufferArray of the first byte to be
   *          read
   * @return number of bytes read
   * @throws IOException
   */
  @Override
  public int read(ByteBuffer dstBuffer, long offset) throws IOException {
    assert dstBuffer.hasArray();
    return bufferArray.getMultiple(offset, dstBuffer.remaining(), dstBuffer.array(),
        dstBuffer.arrayOffset());
  }

  /**
   * Transfers data from the given byte buffer to the buffer array
   * @param srcBuffer the given byte buffer from which bytes are to be read
   * @param offset The offset in the ByteBufferArray of the first byte to be
   *          written
   * @throws IOException
   */
  @Override
  public void write(ByteBuffer srcBuffer, long offset) throws IOException {
    assert srcBuffer.hasArray();
    bufferArray.putMultiple(offset, srcBuffer.remaining(), srcBuffer.array(),
        srcBuffer.arrayOffset());
  }

  /**
   * No operation for the sync in the memory IO engine
   */
  @Override
  public void sync() {

  }

  /**
   * No operation for the shutdown in the memory IO engine
   */
  @Override
  public void shutdown() {

  }
}
