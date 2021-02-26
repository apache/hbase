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
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ IOTests.class, SmallTests.class })
public class TestByteBufferListOutputStream {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestByteBufferListOutputStream.class);

  @Test
  public void testWrites() throws Exception {
    ByteBuffAllocator alloc = new ByteBuffAllocator(true, 3, 10, 10 / 6);
    ByteBufferListOutputStream bbos = new ByteBufferListOutputStream(alloc);
    bbos.write(2);// Write a byte
    bbos.writeInt(100);// Write an int
    byte[] b = Bytes.toBytes("row123");// 6 bytes
    bbos.write(b);
    assertEquals(2, bbos.allBufs.size());
    // Just use the 3rd BB from pool so that pabos, on request, wont get one
    ByteBuff bb1 = alloc.allocateOneBuffer();
    ByteBuffer bb = ByteBuffer.wrap(Bytes.toBytes("row123_cf1_q1"));// 13 bytes
    bbos.write(bb, 0, bb.capacity());
    bb1.release();
    bbos.writeInt(123);
    bbos.writeInt(124);
    assertEquals(0, alloc.getFreeBufferCount());
    List<ByteBuffer> allBufs = bbos.getByteBuffers();
    assertEquals(4, allBufs.size());
    assertEquals(4, bbos.allBufs.size());
    ByteBuffer b1 = allBufs.get(0);
    assertEquals(10, b1.remaining());
    assertEquals(2, b1.get());
    assertEquals(100, b1.getInt());
    byte[] bActual = new byte[b.length];
    b1.get(bActual, 0, 5);// 5 bytes in 1st BB
    ByteBuffer b2 = allBufs.get(1);
    assertEquals(10, b2.remaining());
    b2.get(bActual, 5, 1);// Remaining 1 byte in 2nd BB
    assertTrue(Bytes.equals(b, bActual));
    bActual = new byte[bb.capacity()];
    b2.get(bActual, 0, 9);
    ByteBuffer b3 = allBufs.get(2);
    assertEquals(8, b3.remaining());
    b3.get(bActual, 9, 4);
    assertTrue(ByteBufferUtils.equals(bb, 0, bb.capacity(), bActual, 0, bActual.length));
    assertEquals(123, b3.getInt());
    ByteBuffer b4 = allBufs.get(3);
    assertEquals(4, b4.remaining());
    assertEquals(124, b4.getInt());
    bbos.releaseResources();
    assertEquals(3, alloc.getFreeBufferCount());
  }
}
