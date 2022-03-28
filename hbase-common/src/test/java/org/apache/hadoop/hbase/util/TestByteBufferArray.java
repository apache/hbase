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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.MultiByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, SmallTests.class })
public class TestByteBufferArray {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestByteBufferArray.class);

  private static final ByteBufferAllocator ALLOC = (size) -> ByteBuffer.allocateDirect((int) size);

  @Test
  public void testAsSubBufferWhenEndOffsetLandInLastBuffer() throws Exception {
    int capacity = 4 * 1024 * 1024;
    ByteBufferArray array = new ByteBufferArray(capacity, ALLOC);
    ByteBuff subBuf = ByteBuff.wrap(array.asSubByteBuffers(0, capacity));
    subBuf.position(capacity - 1);// Position to the last byte
    assertTrue(subBuf.hasRemaining());
    // Read last byte
    subBuf.get();
    assertFalse(subBuf.hasRemaining());
  }

  @Test
  public void testByteBufferCreation() throws Exception {
    int capacity = 470 * 1021 * 1023;
    ByteBufferArray array = new ByteBufferArray(capacity, ALLOC);
    assertEquals(118, array.buffers.length);
    for (int i = 0; i < array.buffers.length; i++) {
      assertEquals(ByteBufferArray.DEFAULT_BUFFER_SIZE, array.buffers[i].capacity());
    }
  }

  @Test
  public void testByteBufferCreation1() throws Exception {
    long cap = 7 * 1024L * 1024L;
    int bufferSize = ByteBufferArray.getBufferSize(cap), bufferCount = 25;
    ByteBufferArray array = new ByteBufferArray(bufferSize, bufferCount, 16, cap, ALLOC);
    for (int i = 0; i < array.buffers.length; i++) {
      assertEquals(458752, array.buffers[i].capacity());
    }
  }

  private static void fill(ByteBuff buf, byte val) {
    for (int i = buf.position(); i < buf.limit(); i++) {
      buf.put(i, val);
    }
  }

  private ByteBuff createByteBuff(int len) {
    assert len >= 0;
    int pos = len == 0 ? 0 : ThreadLocalRandom.current().nextInt(len);
    ByteBuff b = ByteBuff.wrap(ByteBuffer.allocate(2 * len));
    b.position(pos).limit(pos + len);
    return b;
  }

  private interface Call {
    void run() throws IOException;
  }

  @SuppressWarnings("TryFailThrowable")
  private void expectedAssert(Call r) throws IOException {
    try {
      r.run();
      fail();
    } catch (AssertionError e) {
      // Ignore
    }
  }


  @Test
  public void testArrayIO() throws IOException {
    int cap = 9 * 1024 * 1024, bufferSize = ByteBufferArray.getBufferSize(cap);
    ByteBufferArray array = new ByteBufferArray(cap, ALLOC);
    testReadAndWrite(array, 0, 512, (byte) 2);
    testReadAndWrite(array, cap - 512, 512, (byte) 3);
    testReadAndWrite(array, 4 * 1024 * 1024, 5 * 1024 * 1024, (byte) 4);
    testReadAndWrite(array, 256, 256, (byte) 5);
    testReadAndWrite(array, 257, 513, (byte) 6);
    testReadAndWrite(array, 0, cap, (byte) 7);
    testReadAndWrite(array, cap, 0, (byte) 8);
    testReadAndWrite(array, cap - 1, 1, (byte) 9);
    testReadAndWrite(array, cap - 2, 2, (byte) 10);

    expectedAssert(() -> testReadAndWrite(array, cap - 2, 3, (byte) 11));
    expectedAssert(() -> testReadAndWrite(array, cap + 1, 0, (byte) 12));
    expectedAssert(() -> testReadAndWrite(array, 0, cap + 1, (byte) 12));
    expectedAssert(() -> testReadAndWrite(array, -1, 0, (byte) 13));
    expectedAssert(() -> testReadAndWrite(array, 0, -23, (byte) 14));
    expectedAssert(() -> testReadAndWrite(array, 0, 0, (byte) 15));
    expectedAssert(() -> testReadAndWrite(array, 4096, cap - 4096 + 1, (byte) 16));

    testAsSubByteBuff(array, 0, cap, true);
    testAsSubByteBuff(array, 0, 0, false);
    testAsSubByteBuff(array, 0, 1, false);
    testAsSubByteBuff(array, 0, bufferSize - 1, false);
    testAsSubByteBuff(array, 0, bufferSize, false);
    testAsSubByteBuff(array, 0, bufferSize + 1, true);
    testAsSubByteBuff(array, 0, 2 * bufferSize, true);
    testAsSubByteBuff(array, 0, 5 * bufferSize, true);
    testAsSubByteBuff(array, cap - bufferSize - 1, bufferSize, true);
    testAsSubByteBuff(array, cap - bufferSize, bufferSize, false);
    testAsSubByteBuff(array, cap - bufferSize, 0, false);
    testAsSubByteBuff(array, cap - bufferSize, 1, false);
    testAsSubByteBuff(array, cap - bufferSize, bufferSize - 1, false);
    testAsSubByteBuff(array, cap - 2 * bufferSize, 2 * bufferSize, true);
    testAsSubByteBuff(array, cap - 2 * bufferSize, bufferSize + 1, true);
    testAsSubByteBuff(array, cap - 2 * bufferSize, bufferSize - 1, false);
    testAsSubByteBuff(array, cap - 2 * bufferSize, 0, false);

    expectedAssert(() -> testAsSubByteBuff(array, 0, cap + 1, false));
    expectedAssert(() -> testAsSubByteBuff(array, 0, -1, false));
    expectedAssert(() -> testAsSubByteBuff(array, -1, -1, false));
    expectedAssert(() -> testAsSubByteBuff(array, cap - bufferSize, bufferSize + 1, false));
    expectedAssert(() -> testAsSubByteBuff(array, 2 * bufferSize, cap - 2 * bufferSize + 1, false));
  }

  private void testReadAndWrite(ByteBufferArray array, int off, int dataSize, byte val) {
    ByteBuff src = createByteBuff(dataSize);
    int pos = src.position(), lim = src.limit();
    fill(src, val);
    assertEquals(src.remaining(), dataSize);
    try {
      assertEquals(dataSize, array.write(off, src));
      assertEquals(0, src.remaining());
    } finally {
      src.position(pos).limit(lim);
    }

    ByteBuff dst = createByteBuff(dataSize);
    pos = dst.position();
    lim = dst.limit();
    try {
      assertEquals(dataSize, array.read(off, dst));
      assertEquals(0, dst.remaining());
    } finally {
      dst.position(pos).limit(lim);
    }
    assertByteBuffEquals(src, dst);
  }

  private void testAsSubByteBuff(ByteBufferArray array, int off, int len, boolean isMulti) {
    ByteBuff ret = ByteBuff.wrap(array.asSubByteBuffers(off, len));
    if (isMulti) {
      assertTrue(ret instanceof MultiByteBuff);
    } else {
      assertTrue(ret instanceof SingleByteBuff);
    }
    assertTrue(!ret.hasArray());
    assertEquals(len, ret.remaining());

    ByteBuff tmp = createByteBuff(len);
    int pos = tmp.position(), lim = tmp.limit();
    try {
      assertEquals(len, array.read(off, tmp));
      assertEquals(0, tmp.remaining());
    } finally {
      tmp.position(pos).limit(lim);
    }

    assertByteBuffEquals(ret, tmp);
  }

  private void assertByteBuffEquals(ByteBuff a, ByteBuff b) {
    assertEquals(a.remaining(), b.remaining());
    for (int i = a.position(), j = b.position(); i < a.limit(); i++, j++) {
      assertEquals(a.get(i), b.get(j));
    }
  }
}
