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
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.util.StringUtils;

/**
 * IO engine that stores data to a file on the local file system.
 */
@InterfaceAudience.Private
public class FileIOEngine implements IOEngine {
  private static final Log LOG = LogFactory.getLog(FileIOEngine.class);
  private final RandomAccessFile raf;
  private final FileChannel fileChannel;
  private final String path;
  private long size;

  public FileIOEngine(String filePath, long fileSize) throws IOException {
    this.path = filePath;
    this.size = fileSize;
    try {
      raf = new RandomAccessFile(filePath, "rw");
    } catch (java.io.FileNotFoundException fex) {
      LOG.error("Can't create bucket cache file " + filePath, fex);
      throw fex;
    }

    try {
      raf.setLength(fileSize);
    } catch (IOException ioex) {
      LOG.error("Can't extend bucket cache file; insufficient space for "
          + StringUtils.byteDesc(fileSize), ioex);
      raf.close();
      throw ioex;
    }

    fileChannel = raf.getChannel();
    LOG.info("Allocating " + StringUtils.byteDesc(fileSize) + ", on the path:" + filePath);
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

  /**
   * Transfers data from file to the given byte buffer
   * @param dstBuffer the given byte buffer into which bytes are to be written
   * @param offset The offset in the file where the first byte to be read
   * @return number of bytes read
   * @throws IOException
   */
  @Override
  public int read(ByteBuffer dstBuffer, long offset) throws IOException {
    return fileChannel.read(dstBuffer, offset);
  }

  /**
   * Transfers data from the given byte buffer to file
   * @param srcBuffer the given byte buffer from which bytes are to be read
   * @param offset The offset in the file where the first byte to be written
   * @throws IOException
   */
  @Override
  public void write(ByteBuffer srcBuffer, long offset) throws IOException {
    fileChannel.write(srcBuffer, offset);
  }

  /**
   * Sync the data to file after writing
   * @throws IOException
   */
  @Override
  public void sync() throws IOException {
    fileChannel.force(true);
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

  @Override
  public ByteBuff read(long offset, int len) throws IOException {
    ByteBuffer dstBuffer = ByteBuffer.allocate(len);
    int read = read(dstBuffer, offset);
    dstBuffer.limit(read);
    return new SingleByteBuff(dstBuffer);
  }

  @Override
  public void write(ByteBuff srcBuffer, long offset) throws IOException {
    // When caching block into BucketCache there will be single buffer backing for this HFileBlock.
    assert srcBuffer.hasArray();
    fileChannel.write(
        ByteBuffer.wrap(srcBuffer.array(), srcBuffer.arrayOffset(), srcBuffer.remaining()), offset);
  }
}
