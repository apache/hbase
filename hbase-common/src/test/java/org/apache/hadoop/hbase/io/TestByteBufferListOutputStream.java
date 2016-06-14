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

import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ IOTests.class, SmallTests.class })
public class TestByteBufferListOutputStream {

  @Test
  public void testWrites() throws Exception {
    ByteBufferPool pool = new ByteBufferPool(10, 3);
    ByteBufferListOutputStream bbos = new ByteBufferListOutputStream(pool);
    bbos.write(2);// Write a byte
    bbos.writeInt(100);// Write an int
    byte[] b = Bytes.toBytes("row123");// 6 bytes
    bbos.write(b);
    // Just use the 3rd BB from pool so that pabos, on request, wont get one
    ByteBuffer bb1 = pool.getBuffer();
    ByteBuffer bb = ByteBuffer.wrap(Bytes.toBytes("row123_cf1_q1"));// 13 bytes
    bbos.write(bb, 0, bb.capacity());
    pool.putbackBuffer(bb1);
    bbos.writeInt(123);
    bbos.writeInt(124);
    assertEquals(0, pool.getQueueSize());
    List<ByteBuffer> allBufs = bbos.getByteBuffers();
    assertEquals(4, allBufs.size());
    assertEquals(3, bbos.bufsFromPool.size());
    ByteBuffer b1 = allBufs.get(0);
    assertEquals(10, b1.remaining());
    assertEquals(2, b1.get());
    assertEquals(100, b1.getInt());
    byte[] bActual = new byte[b.length];
    b1.get(bActual, 0, 5);//5 bytes in 1st BB
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
    assertEquals(3, pool.getQueueSize());
  }
}
