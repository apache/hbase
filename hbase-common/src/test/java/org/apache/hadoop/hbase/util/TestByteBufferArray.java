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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, SmallTests.class})
public class TestByteBufferArray {

  @Test
  public void testByteBufferCreation() throws Exception {
    int capacity = 470 * 1021 * 1023;
    ByteBufferAllocator allocator = new ByteBufferAllocator() {
      @Override
      public ByteBuffer allocate(long size, boolean directByteBuffer) throws IOException {
        if (directByteBuffer) {
          return ByteBuffer.allocateDirect((int) size);
        } else {
          return ByteBuffer.allocate((int) size);
        }
      }
    };
    ByteBufferArray array = new ByteBufferArray(capacity, false, allocator);
    assertEquals(119, array.buffers.length);
    for (int i = 0; i < array.buffers.length; i++) {
      if (i == array.buffers.length - 1) {
        assertEquals(array.buffers[i].capacity(), 0);
      } else {
        assertEquals(array.buffers[i].capacity(), ByteBufferArray.DEFAULT_BUFFER_SIZE);
      }
    }
  }

  @Test
  public void testByteBufferCreation1() throws Exception {
    ByteBufferAllocator allocator = new ByteBufferAllocator() {
      @Override
      public ByteBuffer allocate(long size, boolean directByteBuffer) throws IOException {
        if (directByteBuffer) {
          return ByteBuffer.allocateDirect((int) size);
        } else {
          return ByteBuffer.allocate((int) size);
        }
      }
    };
    ByteBufferArray array = new DummyByteBufferArray(7 * 1024 * 1024, false, allocator);
    // overwrite
    array.bufferCount = 25;
    array.buffers = new ByteBuffer[array.bufferCount + 1];
    array.createBuffers(true, allocator);
    for (int i = 0; i < array.buffers.length; i++) {
      if (i == array.buffers.length - 1) {
        assertEquals(array.buffers[i].capacity(), 0);
      } else {
        assertEquals(array.buffers[i].capacity(), 458752);
      }
    }
  }

  private static class DummyByteBufferArray extends ByteBufferArray {

    public DummyByteBufferArray(long capacity, boolean directByteBuffer,
        ByteBufferAllocator allocator) throws IOException {
      super(capacity, directByteBuffer, allocator);
    }

    @Override
    int getThreadCount() {
      return 16;
    }
  }
}
