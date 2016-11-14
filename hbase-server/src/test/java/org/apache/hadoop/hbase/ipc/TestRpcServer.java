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
package org.apache.hadoop.hbase.ipc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.io.ByteBufferPool;
import org.apache.hadoop.hbase.ipc.RpcServer.CallCleanup;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.MultiByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RPCTests.class, SmallTests.class })
public class TestRpcServer {

  @Test
  public void testAllocateByteBuffToReadInto() throws Exception {
    System.out.println(Long.MAX_VALUE);
    int maxBuffersInPool = 10;
    ByteBufferPool pool = new ByteBufferPool(6 * 1024, maxBuffersInPool);
    initPoolWithAllBuffers(pool, maxBuffersInPool);
    ByteBuff buff = null;
    Pair<ByteBuff, CallCleanup> pair;
    // When the request size is less than 1/6th of the pool buffer size. We should use on demand
    // created on heap Buffer
    pair = RpcServer.allocateByteBuffToReadInto(pool, RpcServer.getMinSizeForReservoirUse(pool),
        200);
    buff = pair.getFirst();
    assertTrue(buff.hasArray());
    assertEquals(maxBuffersInPool, pool.getQueueSize());
    assertNull(pair.getSecond());
    // When the request size is > 1/6th of the pool buffer size.
    pair = RpcServer.allocateByteBuffToReadInto(pool, RpcServer.getMinSizeForReservoirUse(pool),
        1024);
    buff = pair.getFirst();
    assertFalse(buff.hasArray());
    assertEquals(maxBuffersInPool - 1, pool.getQueueSize());
    assertNotNull(pair.getSecond());
    pair.getSecond().run();// CallCleanup#run should put back the BB to pool.
    assertEquals(maxBuffersInPool, pool.getQueueSize());
    // Request size> pool buffer size
    pair = RpcServer.allocateByteBuffToReadInto(pool, RpcServer.getMinSizeForReservoirUse(pool),
        7 * 1024);
    buff = pair.getFirst();
    assertFalse(buff.hasArray());
    assertTrue(buff instanceof MultiByteBuff);
    ByteBuffer[] bbs = ((MultiByteBuff) buff).getEnclosingByteBuffers();
    assertEquals(2, bbs.length);
    assertTrue(bbs[0].isDirect());
    assertTrue(bbs[1].isDirect());
    assertEquals(6 * 1024, bbs[0].limit());
    assertEquals(1024, bbs[1].limit());
    assertEquals(maxBuffersInPool - 2, pool.getQueueSize());
    assertNotNull(pair.getSecond());
    pair.getSecond().run();// CallCleanup#run should put back the BB to pool.
    assertEquals(maxBuffersInPool, pool.getQueueSize());

    pair = RpcServer.allocateByteBuffToReadInto(pool, RpcServer.getMinSizeForReservoirUse(pool),
        6 * 1024 + 200);
    buff = pair.getFirst();
    assertFalse(buff.hasArray());
    assertTrue(buff instanceof MultiByteBuff);
    bbs = ((MultiByteBuff) buff).getEnclosingByteBuffers();
    assertEquals(2, bbs.length);
    assertTrue(bbs[0].isDirect());
    assertFalse(bbs[1].isDirect());
    assertEquals(6 * 1024, bbs[0].limit());
    assertEquals(200, bbs[1].limit());
    assertEquals(maxBuffersInPool - 1, pool.getQueueSize());
    assertNotNull(pair.getSecond());
    pair.getSecond().run();// CallCleanup#run should put back the BB to pool.
    assertEquals(maxBuffersInPool, pool.getQueueSize());

    ByteBuffer[] buffers = new ByteBuffer[maxBuffersInPool - 1];
    for (int i = 0; i < maxBuffersInPool - 1; i++) {
      buffers[i] = pool.getBuffer();
    }
    pair = RpcServer.allocateByteBuffToReadInto(pool, RpcServer.getMinSizeForReservoirUse(pool),
        20 * 1024);
    buff = pair.getFirst();
    assertFalse(buff.hasArray());
    assertTrue(buff instanceof MultiByteBuff);
    bbs = ((MultiByteBuff) buff).getEnclosingByteBuffers();
    assertEquals(2, bbs.length);
    assertTrue(bbs[0].isDirect());
    assertFalse(bbs[1].isDirect());
    assertEquals(6 * 1024, bbs[0].limit());
    assertEquals(14 * 1024, bbs[1].limit());
    assertEquals(0, pool.getQueueSize());
    assertNotNull(pair.getSecond());
    pair.getSecond().run();// CallCleanup#run should put back the BB to pool.
    assertEquals(1, pool.getQueueSize());
    pool.getBuffer();
    pair = RpcServer.allocateByteBuffToReadInto(pool, RpcServer.getMinSizeForReservoirUse(pool),
        7 * 1024);
    buff = pair.getFirst();
    assertTrue(buff.hasArray());
    assertTrue(buff instanceof SingleByteBuff);
    assertEquals(7 * 1024, ((SingleByteBuff) buff).getEnclosingByteBuffer().limit());
    assertNull(pair.getSecond());
  }

  private void initPoolWithAllBuffers(ByteBufferPool pool, int maxBuffersInPool) {
    ByteBuffer[] buffers = new ByteBuffer[maxBuffersInPool];
    // Just call getBuffer() on pool 'maxBuffersInPool' so as to init all buffers and then put back
    // all. Makes pool with max #buffers.
    for (int i = 0; i < maxBuffersInPool; i++) {
      buffers[i] = pool.getBuffer();
    }
    for (ByteBuffer buf : buffers) {
      pool.putbackBuffer(buf);
    }
  }
}