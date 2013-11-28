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

import org.apache.hadoop.hbase.util.ByteBufferArray;

/**
 * IO engine that stores data on the memory using an array of ByteBuffers
 * {@link ByteBufferArray}
 */
public class ByteBufferIOEngine implements IOEngine {

  private ByteBufferArray bufferArray;

  /**
   * Construct the ByteBufferIOEngine with the given capacity
   * @param capacity
   * @param direct true if allocate direct buffer
   * @throws IOException
   */
  public ByteBufferIOEngine(long capacity, int bufferSize, boolean direct)
      throws IOException {
    bufferArray = new ByteBufferArray(capacity, bufferSize, direct);
  }

  /**
   * Transfers data from the buffer array to the given byte buffer
   * @param dst the given byte array into which bytes are to be written
   * @param offset The offset in the ByteBufferArray of the first byte to be
   *          read
   * @throws IOException
   */
  @Override
  public void read(byte[] dst, long offset) throws IOException {
    bufferArray.getMultiple(offset, dst.length, dst, 0);
  }

  /**
   * Transfers data from the given byte buffer to the buffer array
   * @param src the given byte array from which bytes are to be read
   * @param offset The offset in the ByteBufferArray of the first byte to be
   *          written
   * @throws IOException
   */
  @Override
  public void write(byte[] src, long offset) throws IOException {
    bufferArray.putMultiple(offset, src.length, src, 0);
  }

  /**
   * No operation for the sync in the memory IO engine
   */
  @Override
  public void sync() {
  }

  /**
   * Shutdown the memory IO engine by de-referencing the buffer array.
   */
  @Override
  public void shutdown() {
    bufferArray = null;
  }
}
