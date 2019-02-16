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

package org.apache.hadoop.hbase.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.MultiByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RPCTests.class, SmallTests.class })
public class TestByteBuffAllocator {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestByteBuffAllocator.class);

  @Test
  public void testAllocateByteBuffToReadInto() {
    int maxBuffersInPool = 10;
    int bufSize = 6 * 1024;
    ByteBuffAllocator alloc = new ByteBuffAllocator(true, maxBuffersInPool, bufSize, bufSize / 6);
    ByteBuff buff = alloc.allocate(10 * bufSize);
    buff.release();
    // When the request size is less than 1/6th of the pool buffer size. We should use on demand
    // created on heap Buffer
    buff = alloc.allocate(200);
    assertTrue(buff.hasArray());
    assertEquals(maxBuffersInPool, alloc.getQueueSize());
    buff.release();
    // When the request size is > 1/6th of the pool buffer size.
    buff = alloc.allocate(1024);
    assertFalse(buff.hasArray());
    assertEquals(maxBuffersInPool - 1, alloc.getQueueSize());
    buff.release();// ByteBuffDeallocaor#free should put back the BB to pool.
    assertEquals(maxBuffersInPool, alloc.getQueueSize());
    // Request size> pool buffer size
    buff = alloc.allocate(7 * 1024);
    assertFalse(buff.hasArray());
    assertTrue(buff instanceof MultiByteBuff);
    ByteBuffer[] bbs = buff.nioByteBuffers();
    assertEquals(2, bbs.length);
    assertTrue(bbs[0].isDirect());
    assertTrue(bbs[1].isDirect());
    assertEquals(6 * 1024, bbs[0].limit());
    assertEquals(1024, bbs[1].limit());
    assertEquals(maxBuffersInPool - 2, alloc.getQueueSize());
    buff.release();
    assertEquals(maxBuffersInPool, alloc.getQueueSize());

    buff = alloc.allocate(6 * 1024 + 200);
    assertFalse(buff.hasArray());
    assertTrue(buff instanceof MultiByteBuff);
    bbs = buff.nioByteBuffers();
    assertEquals(2, bbs.length);
    assertTrue(bbs[0].isDirect());
    assertFalse(bbs[1].isDirect());
    assertEquals(6 * 1024, bbs[0].limit());
    assertEquals(200, bbs[1].limit());
    assertEquals(maxBuffersInPool - 1, alloc.getQueueSize());
    buff.release();
    assertEquals(maxBuffersInPool, alloc.getQueueSize());

    alloc.allocate(bufSize * (maxBuffersInPool - 1));
    buff = alloc.allocate(20 * 1024);
    assertFalse(buff.hasArray());
    assertTrue(buff instanceof MultiByteBuff);
    bbs = buff.nioByteBuffers();
    assertEquals(2, bbs.length);
    assertTrue(bbs[0].isDirect());
    assertFalse(bbs[1].isDirect());
    assertEquals(6 * 1024, bbs[0].limit());
    assertEquals(14 * 1024, bbs[1].limit());
    assertEquals(0, alloc.getQueueSize());
    buff.release();
    assertEquals(1, alloc.getQueueSize());
    alloc.allocateOneBuffer();

    buff = alloc.allocate(7 * 1024);
    assertTrue(buff.hasArray());
    assertTrue(buff instanceof SingleByteBuff);
    assertEquals(7 * 1024, buff.nioByteBuffers()[0].limit());
    buff.release();
  }

  @Test
  public void testNegativeAllocatedSize() {
    int maxBuffersInPool = 10;
    ByteBuffAllocator allocator =
        new ByteBuffAllocator(true, maxBuffersInPool, 6 * 1024, 1024);
    try {
      allocator.allocate(-1);
      fail("Should throw exception when size < 0");
    } catch (IllegalArgumentException e) {
      // expected exception
    }
    ByteBuff bb = allocator.allocate(0);
    bb.release();
  }

  @Test
  public void testAllocateOneBuffer() {
    // Allocate from on-heap
    ByteBuffAllocator allocator = ByteBuffAllocator.createOnHeap();
    ByteBuff buf = allocator.allocateOneBuffer();
    assertTrue(buf.hasArray());
    assertEquals(ByteBuffAllocator.DEFAULT_BUFFER_SIZE, buf.remaining());
    buf.release();

    // Allocate from off-heap
    int bufSize = 10;
    allocator = new ByteBuffAllocator(true, 1, 10, 3);
    buf = allocator.allocateOneBuffer();
    assertFalse(buf.hasArray());
    assertEquals(buf.remaining(), bufSize);
    // The another one will be allocated from on-heap because the pool has only one ByteBuffer,
    // and still not be cleaned.
    ByteBuff buf2 = allocator.allocateOneBuffer();
    assertTrue(buf2.hasArray());
    assertEquals(buf2.remaining(), bufSize);
    // free the first one
    buf.release();
    // The next one will be off-heap again.
    buf = allocator.allocateOneBuffer();
    assertFalse(buf.hasArray());
    assertEquals(buf.remaining(), bufSize);
    buf.release();
  }

  @Test
  public void testReferenceCount() {
    int bufSize = 64;
    ByteBuffAllocator alloc = new ByteBuffAllocator(true, 2, bufSize, 3);
    ByteBuff buf1 = alloc.allocate(bufSize * 2);
    assertFalse(buf1.hasArray());
    // The next one will be allocated from heap
    ByteBuff buf2 = alloc.allocateOneBuffer();
    assertTrue(buf2.hasArray());

    // duplicate the buf2, if the dup released, buf2 will also be released (SingleByteBuffer)
    ByteBuff dup2 = buf2.duplicate();
    dup2.release();
    assertEquals(0, buf2.refCnt());
    assertEquals(0, dup2.refCnt());
    assertEquals(0, alloc.getQueueSize());
    assertException(dup2::position);
    assertException(buf2::position);

    // duplicate the buf1, if the dup1 released, buf1 will also be released (MultipleByteBuffer)
    ByteBuff dup1 = buf1.duplicate();
    dup1.release();
    assertEquals(0, buf1.refCnt());
    assertEquals(0, dup1.refCnt());
    assertEquals(2, alloc.getQueueSize());
    assertException(dup1::position);
    assertException(buf1::position);

    // slice the buf3, if the slice3 released, buf3 will also be released (SingleByteBuffer)
    ByteBuff buf3 = alloc.allocateOneBuffer();
    assertFalse(buf3.hasArray());
    ByteBuff slice3 = buf3.slice();
    slice3.release();
    assertEquals(0, buf3.refCnt());
    assertEquals(0, slice3.refCnt());
    assertEquals(2, alloc.getQueueSize());

    // slice the buf4, if the slice4 released, buf4 will also be released (MultipleByteBuffer)
    ByteBuff buf4 = alloc.allocate(bufSize * 2);
    assertFalse(buf4.hasArray());
    ByteBuff slice4 = buf4.slice();
    slice4.release();
    assertEquals(0, buf4.refCnt());
    assertEquals(0, slice4.refCnt());
    assertEquals(2, alloc.getQueueSize());

    // Test multiple reference for the same ByteBuff (SingleByteBuff)
    ByteBuff buf5 = alloc.allocateOneBuffer();
    ByteBuff slice5 = buf5.duplicate().duplicate().duplicate().slice().slice();
    slice5.release();
    assertEquals(0, buf5.refCnt());
    assertEquals(0, slice5.refCnt());
    assertEquals(2, alloc.getQueueSize());
    assertException(slice5::position);
    assertException(buf5::position);

    // Test multiple reference for the same ByteBuff (SingleByteBuff)
    ByteBuff buf6 = alloc.allocate(bufSize >> 2);
    ByteBuff slice6 = buf6.duplicate().duplicate().duplicate().slice().slice();
    slice6.release();
    assertEquals(0, buf6.refCnt());
    assertEquals(0, slice6.refCnt());
    assertEquals(2, alloc.getQueueSize());

    // Test retain the parent SingleByteBuff (duplicate)
    ByteBuff parent = alloc.allocateOneBuffer();
    ByteBuff child = parent.duplicate();
    child.retain();
    parent.release();
    assertEquals(1, child.refCnt());
    assertEquals(1, parent.refCnt());
    assertEquals(1, alloc.getQueueSize());
    parent.release();
    assertEquals(0, child.refCnt());
    assertEquals(0, parent.refCnt());
    assertEquals(2, alloc.getQueueSize());

    // Test retain parent MultiByteBuff (duplicate)
    parent = alloc.allocate(bufSize << 1);
    child = parent.duplicate();
    child.retain();
    parent.release();
    assertEquals(1, child.refCnt());
    assertEquals(1, parent.refCnt());
    assertEquals(0, alloc.getQueueSize());
    parent.release();
    assertEquals(0, child.refCnt());
    assertEquals(0, parent.refCnt());
    assertEquals(2, alloc.getQueueSize());

    // Test retain the parent SingleByteBuff (slice)
    parent = alloc.allocateOneBuffer();
    child = parent.slice();
    child.retain();
    parent.release();
    assertEquals(1, child.refCnt());
    assertEquals(1, parent.refCnt());
    assertEquals(1, alloc.getQueueSize());
    parent.release();
    assertEquals(0, child.refCnt());
    assertEquals(0, parent.refCnt());
    assertEquals(2, alloc.getQueueSize());

    // Test retain parent MultiByteBuff (slice)
    parent = alloc.allocate(bufSize << 1);
    child = parent.slice();
    child.retain();
    parent.release();
    assertEquals(1, child.refCnt());
    assertEquals(1, parent.refCnt());
    assertEquals(0, alloc.getQueueSize());
    parent.release();
    assertEquals(0, child.refCnt());
    assertEquals(0, parent.refCnt());
    assertEquals(2, alloc.getQueueSize());
  }

  @Test
  public void testReverseRef() {
    int bufSize = 64;
    ByteBuffAllocator alloc = new ByteBuffAllocator(true, 1, bufSize, 3);
    ByteBuff buf1 = alloc.allocate(bufSize);
    ByteBuff dup1 = buf1.duplicate();
    assertEquals(1, buf1.refCnt());
    assertEquals(1, dup1.refCnt());
    buf1.release();
    assertEquals(0, buf1.refCnt());
    assertEquals(0, dup1.refCnt());
    assertEquals(1, alloc.getQueueSize());
    assertException(buf1::position);
    assertException(dup1::position);
  }

  @Test
  public void testByteBuffUnsupportedMethods() {
    int bufSize = 64;
    ByteBuffAllocator alloc = new ByteBuffAllocator(true, 1, bufSize, 3);
    ByteBuff buf = alloc.allocate(bufSize);
    assertException(() -> buf.retain(2));
    assertException(() -> buf.release(2));
    assertException(() -> buf.touch());
    assertException(() -> buf.touch(new Object()));
  }

  private void assertException(Runnable r) {
    try {
      r.run();
      fail();
    } catch (Exception e) {
      // expected exception.
    }
  }
}
