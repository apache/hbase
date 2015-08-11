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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.Cacheable.MemoryType;
import org.apache.hadoop.hbase.io.hfile.CacheableDeserializer;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Basic test for {@link ByteBufferIOEngine}
 */
@Category({IOTests.class, SmallTests.class})
public class TestByteBufferIOEngine {

  @Test
  public void testByteBufferIOEngine() throws Exception {
    int capacity = 32 * 1024 * 1024; // 32 MB
    int testNum = 100;
    int maxBlockSize = 64 * 1024;
    ByteBufferIOEngine ioEngine = new ByteBufferIOEngine(capacity, false);
    int testOffsetAtStartNum = testNum / 10;
    int testOffsetAtEndNum = testNum / 10;
    for (int i = 0; i < testNum; i++) {
      byte val = (byte) (Math.random() * 255);
      int blockSize = (int) (Math.random() * maxBlockSize);
      if (blockSize == 0) {
        blockSize = 1;
      }
      byte[] byteArray = new byte[blockSize];
      for (int j = 0; j < byteArray.length; ++j) {
        byteArray[j] = val;
      }
      ByteBuffer srcBuffer = ByteBuffer.wrap(byteArray);
      int offset = 0;
      if (testOffsetAtStartNum > 0) {
        testOffsetAtStartNum--;
        offset = 0;
      } else if (testOffsetAtEndNum > 0) {
        testOffsetAtEndNum--;
        offset = capacity - blockSize;
      } else {
        offset = (int) (Math.random() * (capacity - maxBlockSize));
      }
      ioEngine.write(srcBuffer, offset);
      BufferGrabbingDeserializer deserializer = new BufferGrabbingDeserializer();
      ioEngine.read(offset, blockSize, deserializer);
      ByteBuff dstBuffer = deserializer.buf;
      for (int j = 0; j < byteArray.length; ++j) {
        assertTrue(byteArray[j] == dstBuffer.get(j));
      }
    }
    assert testOffsetAtStartNum == 0;
    assert testOffsetAtEndNum == 0;
  }

  /**
   * A CacheableDeserializer implementation which just store reference to the {@link ByteBuff} to be
   * deserialized. Use {@link #getDeserializedByteBuff()} to get this reference.
   */
  static class BufferGrabbingDeserializer implements CacheableDeserializer<Cacheable> {
    private ByteBuff buf;

    @Override
    public Cacheable deserialize(ByteBuff b) throws IOException {
      return null;
    }

    @Override
    public Cacheable deserialize(final ByteBuff b, boolean reuse, MemoryType memType)
        throws IOException {
      this.buf = b;
      return null;
    }

    @Override
    public int getDeserialiserIdentifier() {
      return 0;
    }

    public ByteBuff getDeserializedByteBuff() {
      return this.buf;
    }
  }

  @Test
  public void testByteBufferIOEngineWithMBB() throws Exception {
    int capacity = 32 * 1024 * 1024; // 32 MB
    int testNum = 100;
    int maxBlockSize = 64 * 1024;
    ByteBufferIOEngine ioEngine = new ByteBufferIOEngine(capacity, false);
    int testOffsetAtStartNum = testNum / 10;
    int testOffsetAtEndNum = testNum / 10;
    for (int i = 0; i < testNum; i++) {
      byte val = (byte) (Math.random() * 255);
      int blockSize = (int) (Math.random() * maxBlockSize);
      if (blockSize == 0) {
        blockSize = 1;
      }
      byte[] byteArray = new byte[blockSize];
      for (int j = 0; j < byteArray.length; ++j) {
        byteArray[j] = val;
      }
      ByteBuffer srcBuffer = ByteBuffer.wrap(byteArray);
      int offset = 0;
      if (testOffsetAtStartNum > 0) {
        testOffsetAtStartNum--;
        offset = 0;
      } else if (testOffsetAtEndNum > 0) {
        testOffsetAtEndNum--;
        offset = capacity - blockSize;
      } else {
        offset = (int) (Math.random() * (capacity - maxBlockSize));
      }
      ioEngine.write(srcBuffer, offset);
      BufferGrabbingDeserializer deserializer = new BufferGrabbingDeserializer();
      ioEngine.read(offset, blockSize, deserializer);
      ByteBuff dstBuffer = deserializer.buf;
      for (int j = 0; j < byteArray.length; ++j) {
        assertTrue(srcBuffer.get(j) == dstBuffer.get(j));
      }
    }
    assert testOffsetAtStartNum == 0;
    assert testOffsetAtEndNum == 0;
  }
}
