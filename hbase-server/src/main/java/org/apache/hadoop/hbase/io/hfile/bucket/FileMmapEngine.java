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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.Cacheable.MemoryType;
import org.apache.hadoop.hbase.io.hfile.CacheableDeserializer;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.hbase.util.ByteBufferAllocator;
import org.apache.hadoop.hbase.util.ByteBufferArray;
import org.apache.hadoop.util.StringUtils;

/**
 * IO engine that stores data to a file on the local file system using memory mapping
 * mechanism
 */
@InterfaceAudience.Private
public class FileMmapEngine implements IOEngine {
  static final Log LOG = LogFactory.getLog(FileMmapEngine.class);

  private final String path;
  private long size;
  private ByteBufferArray bufferArray;
  private final FileChannel fileChannel;
  private RandomAccessFile raf = null;

  public FileMmapEngine(String filePath, long capacity) throws IOException {
    this.path = filePath;
    this.size = capacity;
    long fileSize = 0;
    try {
      raf = new RandomAccessFile(filePath, "rw");
      fileSize = roundUp(capacity, ByteBufferArray.DEFAULT_BUFFER_SIZE);
      raf.setLength(fileSize);
      fileChannel = raf.getChannel();
      LOG.info("Allocating " + StringUtils.byteDesc(fileSize) + ", on the path:" + filePath);
    } catch (java.io.FileNotFoundException fex) {
      LOG.error("Can't create bucket cache file " + filePath, fex);
      throw fex;
    } catch (IOException ioex) {
      LOG.error("Can't extend bucket cache file; insufficient space for "
          + StringUtils.byteDesc(fileSize), ioex);
      shutdown();
      throw ioex;
    }
    ByteBufferAllocator allocator = new ByteBufferAllocator() {
      int pos = 0;
      @Override
      public ByteBuffer allocate(long size, boolean directByteBuffer) throws IOException {
        ByteBuffer buffer = null;
        if (directByteBuffer) {
          buffer = fileChannel.map(java.nio.channels.FileChannel.MapMode.READ_WRITE, pos * size,
              size);
        } else {
          throw new IllegalArgumentException(
              "Only Direct Bytebuffers allowed with FileMMap engine");
        }
        pos++;
        return buffer;
      }
    };
    bufferArray = new ByteBufferArray(fileSize, true, allocator);
  }

  private long roundUp(long n, long to) {
    return ((n + to - 1) / to) * to;
  }

  @Override
  public String toString() {
    return "ioengine=" + this.getClass().getSimpleName() + ", path=" + this.path +
      ", size=" + String.format("%,d", this.size);
  }

  /**
   * File IO engine is always able to support persistent storage for the cache
   * @return true
   */
  @Override
  public boolean isPersistent() {
    return true;
  }

  @Override
  public boolean isSegmented() {
    return false;
  }

  @Override
  public boolean allocationCrossedSegments(long offset, long len) {
    return false;
  }

  @Override
  public Cacheable read(long offset, int length, CacheableDeserializer<Cacheable> deserializer)
      throws IOException {
    byte[] dst = new byte[length];
    bufferArray.getMultiple(offset, length, dst);
    return deserializer.deserialize(new SingleByteBuff(ByteBuffer.wrap(dst)), true,
        MemoryType.EXCLUSIVE);
  }

  /**
   * Transfers data from the given byte buffer to file
   * @param srcBuffer the given byte buffer from which bytes are to be read
   * @param offset The offset in the file where the first byte to be written
   * @throws IOException
   */
  @Override
  public void write(ByteBuffer srcBuffer, long offset) throws IOException {
    assert srcBuffer.hasArray();
    bufferArray.putMultiple(offset, srcBuffer.remaining(), srcBuffer.array(),
        srcBuffer.arrayOffset());
  }

  @Override
  public void write(ByteBuff srcBuffer, long offset) throws IOException {
    // This singleByteBuff can be considered to be array backed
    assert srcBuffer.hasArray();
    bufferArray.putMultiple(offset, srcBuffer.remaining(), srcBuffer.array(),
        srcBuffer.arrayOffset());

  }
  /**
   * Sync the data to file after writing
   * @throws IOException
   */
  @Override
  public void sync() throws IOException {
    if (fileChannel != null) {
      fileChannel.force(true);
    }
  }

  /**
   * Close the file
   */
  @Override
  public void shutdown() {
    try {
      fileChannel.close();
    } catch (IOException ex) {
      LOG.error("Can't shutdown cleanly", ex);
    }
    try {
      raf.close();
    } catch (IOException ex) {
      LOG.error("Can't shutdown cleanly", ex);
    }
  }
}