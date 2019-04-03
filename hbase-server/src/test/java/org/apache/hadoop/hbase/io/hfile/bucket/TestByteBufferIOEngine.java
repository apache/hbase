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
package org.apache.hadoop.hbase.io.hfile.bucket;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.Cacheable.MemoryType;
import org.apache.hadoop.hbase.io.hfile.CacheableDeserializer;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Basic test for {@link ByteBufferIOEngine}
 */
@Category({IOTests.class, SmallTests.class})
public class TestByteBufferIOEngine {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestByteBufferIOEngine.class);

  @Test
  public void testByteBufferIOEngine() throws Exception {
    int capacity = 32 * 1024 * 1024; // 32 MB
    int testNum = 100;
    int maxBlockSize = 64 * 1024;
    ByteBufferIOEngine ioEngine = new ByteBufferIOEngine(capacity);
    int testOffsetAtStartNum = testNum / 10;
    int testOffsetAtEndNum = testNum / 10;
    for (int i = 0; i < testNum; i++) {
      byte val = (byte) (Math.random() * 255);
      int blockSize = (int) (Math.random() * maxBlockSize);
      if (blockSize == 0) {
        blockSize = 1;
      }

      ByteBuff src = createByteBuffer(blockSize, val, i % 2 == 0);
      int pos = src.position(), lim = src.limit();
      int offset;
      if (testOffsetAtStartNum > 0) {
        testOffsetAtStartNum--;
        offset = 0;
      } else if (testOffsetAtEndNum > 0) {
        testOffsetAtEndNum--;
        offset = capacity - blockSize;
      } else {
        offset = (int) (Math.random() * (capacity - maxBlockSize));
      }
      ioEngine.write(src, offset);
      src.position(pos).limit(lim);

      BufferGrabbingDeserializer deserializer = new BufferGrabbingDeserializer();
      ioEngine.read(offset, blockSize, deserializer);
      ByteBuff dst = deserializer.buf;
      Assert.assertEquals(src.remaining(), blockSize);
      Assert.assertEquals(dst.remaining(), blockSize);
      Assert.assertEquals(0, ByteBuff.compareTo(src, src.position(), src.remaining(), dst,
        dst.position(), dst.remaining()));
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

  static ByteBuff createByteBuffer(int len, int val, boolean useHeap) {
    ByteBuffer b = useHeap ? ByteBuffer.allocate(2 * len) : ByteBuffer.allocateDirect(2 * len);
    int pos = (int) (Math.random() * len);
    b.position(pos).limit(pos + len);
    for (int i = pos; i < pos + len; i++) {
      b.put(i, (byte) val);
    }
    return ByteBuff.wrap(b);
  }

  @Test
  public void testByteBufferIOEngineWithMBB() throws Exception {
    int capacity = 32 * 1024 * 1024; // 32 MB
    int testNum = 100;
    int maxBlockSize = 64 * 1024;
    ByteBufferIOEngine ioEngine = new ByteBufferIOEngine(capacity);
    int testOffsetAtStartNum = testNum / 10;
    int testOffsetAtEndNum = testNum / 10;
    for (int i = 0; i < testNum; i++) {
      byte val = (byte) (Math.random() * 255);
      int blockSize = (int) (Math.random() * maxBlockSize);
      if (blockSize == 0) {
        blockSize = 1;
      }
      ByteBuff src = createByteBuffer(blockSize, val, i % 2 == 0);
      int pos = src.position(), lim = src.limit();
      int offset;
      if (testOffsetAtStartNum > 0) {
        testOffsetAtStartNum--;
        offset = 0;
      } else if (testOffsetAtEndNum > 0) {
        testOffsetAtEndNum--;
        offset = capacity - blockSize;
      } else {
        offset = (int) (Math.random() * (capacity - maxBlockSize));
      }
      ioEngine.write(src, offset);
      src.position(pos).limit(lim);

      BufferGrabbingDeserializer deserializer = new BufferGrabbingDeserializer();
      ioEngine.read(offset, blockSize, deserializer);
      ByteBuff dst = deserializer.buf;
      Assert.assertEquals(src.remaining(), blockSize);
      Assert.assertEquals(dst.remaining(), blockSize);
      Assert.assertEquals(0, ByteBuff.compareTo(src, src.position(), src.remaining(), dst,
        dst.position(), dst.remaining()));
    }
    assert testOffsetAtStartNum == 0;
    assert testOffsetAtEndNum == 0;
  }
}
