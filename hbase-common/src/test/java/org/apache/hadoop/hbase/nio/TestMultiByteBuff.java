/**
 * Copyright The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.nio;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ObjectIntPair;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, SmallTests.class })
public class TestMultiByteBuff {

  @Test
  public void testWritesAndReads() {
    // Absolute reads
    ByteBuffer bb1 = ByteBuffer.allocate(15);
    ByteBuffer bb2 = ByteBuffer.allocate(15);
    int i1 = 4;
    bb1.putInt(i1);
    long l1 = 45L, l2 = 100L, l3 = 12345L;
    bb1.putLong(l1);
    short s1 = 2;
    bb1.putShort(s1);
    byte[] b = Bytes.toBytes(l2);
    bb1.put(b, 0, 1);
    bb2.put(b, 1, 7);
    bb2.putLong(l3);
    MultiByteBuff mbb = new MultiByteBuff(bb1, bb2);
    assertEquals(l1, mbb.getLong(4));
    assertEquals(l2, mbb.getLong(14));
    assertEquals(l3, mbb.getLong(22));
    assertEquals(i1, mbb.getInt(0));
    assertEquals(s1, mbb.getShort(12));
    // Relative reads
    assertEquals(i1, mbb.getInt());
    assertEquals(l1, mbb.getLong());
    assertEquals(s1, mbb.getShort());
    assertEquals(l2, mbb.getLong());
    assertEquals(l3, mbb.getLong());
    // Absolute writes
    bb1 = ByteBuffer.allocate(15);
    bb2 = ByteBuffer.allocate(15);
    mbb = new MultiByteBuff(bb1, bb2);
    byte b1 = 5, b2 = 31;
    mbb.put(b1);
    mbb.putLong(l1);
    mbb.putInt(i1);
    mbb.putLong(l2);
    mbb.put(b2);
    mbb.position(mbb.position() + 2);
    try {
      mbb.putLong(l3);
      fail("'Should have thrown BufferOverflowException");
    } catch (BufferOverflowException e) {
    }
    mbb.position(mbb.position() - 2);
    mbb.putLong(l3);
    mbb.rewind();
    assertEquals(b1, mbb.get());
    assertEquals(l1, mbb.getLong());
    assertEquals(i1, mbb.getInt());
    assertEquals(l2, mbb.getLong());
    assertEquals(b2, mbb.get());
    assertEquals(l3, mbb.getLong());
    mbb.put(21, b1);
    mbb.position(21);
    assertEquals(b1, mbb.get());
    mbb.put(b);
    assertEquals(l2, mbb.getLong(22));
  }

  @Test
  public void testPutPrimitives() {
    ByteBuffer bb = ByteBuffer.allocate(10);
    SingleByteBuff s = new SingleByteBuff(bb);
    s.putLong(-4465109508325701663l);
    bb.rewind();
    long long1 = bb.getLong();
    assertEquals(long1, -4465109508325701663l);
    s.position(8);
  }

  @Test
  public void testArrayBasedMethods() {
    byte[] b = new byte[15];
    ByteBuffer bb1 = ByteBuffer.wrap(b, 1, 10).slice();
    ByteBuffer bb2 = ByteBuffer.allocate(15);
    ByteBuff mbb1 = new MultiByteBuff(bb1, bb2);
    assertFalse(mbb1.hasArray());
    try {
      mbb1.array();
      fail();
    } catch (UnsupportedOperationException e) {
    }
    try {
      mbb1.arrayOffset();
      fail();
    } catch (UnsupportedOperationException e) {
    }
    mbb1 = new SingleByteBuff(bb1);
    assertTrue(mbb1.hasArray());
    assertEquals(1, mbb1.arrayOffset());
    assertEquals(b, mbb1.array());
    mbb1 = new SingleByteBuff(ByteBuffer.allocateDirect(10));
    assertFalse(mbb1.hasArray());
    try {
      mbb1.array();
      fail();
    } catch (UnsupportedOperationException e) {
    }
    try {
      mbb1.arrayOffset();
      fail();
    } catch (UnsupportedOperationException e) {
    }
  }

  @Test
  public void testMarkAndResetWithMBB() {
    ByteBuffer bb1 = ByteBuffer.allocateDirect(15);
    ByteBuffer bb2 = ByteBuffer.allocateDirect(15);
    bb1.putInt(4);
    long l1 = 45L, l2 = 100L, l3 = 12345L;
    bb1.putLong(l1);
    bb1.putShort((short) 2);
    byte[] b = Bytes.toBytes(l2);
    bb1.put(b, 0, 1);
    bb2.put(b, 1, 7);
    bb2.putLong(l3);
    ByteBuff multi = new MultiByteBuff(bb1, bb2);
    assertEquals(4, multi.getInt());
    assertEquals(l1, multi.getLong());
    multi.mark();
    assertEquals((short) 2, multi.getShort());
    multi.reset();
    assertEquals((short) 2, multi.getShort());
    multi.mark();
    assertEquals(l2, multi.getLong());
    multi.reset();
    assertEquals(l2, multi.getLong());
    multi.mark();
    assertEquals(l3, multi.getLong());
    multi.reset();
    assertEquals(l3, multi.getLong());
    // Try absolute gets with mark and reset
    multi.mark();
    assertEquals(l2, multi.getLong(14));
    multi.reset();
    assertEquals(l3, multi.getLong(22));
    // Just reset to see what happens
    multi.reset();
    assertEquals(l2, multi.getLong(14));
    multi.mark();
    assertEquals(l3, multi.getLong(22));
    multi.reset();
  }

  @Test
  public void testSkipNBytes() {
    ByteBuffer bb1 = ByteBuffer.allocate(15);
    ByteBuffer bb2 = ByteBuffer.allocate(15);
    bb1.putInt(4);
    long l1 = 45L, l2 = 100L, l3 = 12345L;
    bb1.putLong(l1);
    bb1.putShort((short) 2);
    byte[] b = Bytes.toBytes(l2);
    bb1.put(b, 0, 1);
    bb2.put(b, 1, 7);
    bb2.putLong(l3);
    MultiByteBuff multi = new MultiByteBuff(bb1, bb2);
    assertEquals(4, multi.getInt());
    assertEquals(l1, multi.getLong());
    multi.skip(10);
    assertEquals(l3, multi.getLong());
  }

  @Test
  public void testMoveBack() {
    ByteBuffer bb1 = ByteBuffer.allocate(15);
    ByteBuffer bb2 = ByteBuffer.allocate(15);
    bb1.putInt(4);
    long l1 = 45L, l2 = 100L, l3 = 12345L;
    bb1.putLong(l1);
    bb1.putShort((short) 2);
    byte[] b = Bytes.toBytes(l2);
    bb1.put(b, 0, 1);
    bb2.put(b, 1, 7);
    bb2.putLong(l3);
    MultiByteBuff multi = new MultiByteBuff(bb1, bb2);
    assertEquals(4, multi.getInt());
    assertEquals(l1, multi.getLong());
    multi.skip(10);
    multi.moveBack(4);
    multi.moveBack(6);
    multi.moveBack(8);
    assertEquals(l1, multi.getLong());
  }

  @Test
  public void testSubBuffer() {
    ByteBuffer bb1 = ByteBuffer.allocateDirect(10);
    ByteBuffer bb2 = ByteBuffer.allocateDirect(10);
    MultiByteBuff multi = new MultiByteBuff(bb1, bb2);
    long l1 = 1234L, l2 = 100L;
    multi.putLong(l1);
    multi.putLong(l2);
    multi.rewind();
    ByteBuffer sub = multi.asSubByteBuffer(Bytes.SIZEOF_LONG);
    assertTrue(bb1 == sub);
    assertEquals(l1, ByteBufferUtils.toLong(sub, sub.position()));
    multi.skip(Bytes.SIZEOF_LONG);
    sub = multi.asSubByteBuffer(Bytes.SIZEOF_LONG);
    assertFalse(bb1 == sub);
    assertFalse(bb2 == sub);
    assertEquals(l2, ByteBufferUtils.toLong(sub, sub.position()));
    multi.rewind();
    ObjectIntPair<ByteBuffer> p = new ObjectIntPair<ByteBuffer>();
    multi.asSubByteBuffer(8, Bytes.SIZEOF_LONG, p);
    assertFalse(bb1 == p.getFirst());
    assertFalse(bb2 == p.getFirst());
    assertEquals(0, p.getSecond());
    assertEquals(l2, ByteBufferUtils.toLong(sub, p.getSecond()));
  }

  @Test
  public void testSliceDuplicateMethods() throws Exception {
    ByteBuffer bb1 = ByteBuffer.allocateDirect(10);
    ByteBuffer bb2 = ByteBuffer.allocateDirect(15);
    MultiByteBuff multi = new MultiByteBuff(bb1, bb2);
    long l1 = 1234L, l2 = 100L;
    multi.put((byte) 2);
    multi.putLong(l1);
    multi.putLong(l2);
    multi.putInt(45);
    multi.position(1);
    multi.limit(multi.position() + (2 * Bytes.SIZEOF_LONG));
    MultiByteBuff sliced = multi.slice();
    assertEquals(0, sliced.position());
    assertEquals((2 * Bytes.SIZEOF_LONG), sliced.limit());
    assertEquals(l1, sliced.getLong());
    assertEquals(l2, sliced.getLong());
    MultiByteBuff dup = multi.duplicate();
    assertEquals(1, dup.position());
    assertEquals(dup.position() + (2 * Bytes.SIZEOF_LONG), dup.limit());
    assertEquals(l1, dup.getLong());
    assertEquals(l2, dup.getLong());
  }

  @Test
  public void testGetWithPosOnMultiBuffers() throws IOException {
    byte[] b = new byte[4];
    byte[] b1 = new byte[4];
    ByteBuffer bb1 = ByteBuffer.wrap(b);
    ByteBuffer bb2 = ByteBuffer.wrap(b1);
    MultiByteBuff mbb1 = new MultiByteBuff(bb1, bb2);
    mbb1.position(2);
    mbb1.putInt(4);
    int res = mbb1.getInt(2);
    byte[] bres = new byte[4];
    bres[0] = mbb1.get(2);
    bres[1] = mbb1.get(3);
    bres[2] = mbb1.get(4);
    bres[3] = mbb1.get(5);
    int expected = Bytes.toInt(bres);
    assertEquals(res, expected);
  }

  @Test
  public void testGetIntStrictlyForwardWithPosOnMultiBuffers() throws IOException {
    byte[] b = new byte[4];
    byte[] b1 = new byte[8];
    ByteBuffer bb1 = ByteBuffer.wrap(b);
    ByteBuffer bb2 = ByteBuffer.wrap(b1);
    MultiByteBuff mbb1 = new MultiByteBuff(bb1, bb2);
    mbb1.position(2);
    mbb1.putInt(4);
    mbb1.position(7);
    mbb1.put((byte) 2);
    mbb1.putInt(3);
    mbb1.rewind();
    mbb1.getIntAfterPosition(4);
    byte res = mbb1.get(7);
    assertEquals((byte) 2, res);
    mbb1.position(7);
    int intRes = mbb1.getIntAfterPosition(1);
    assertEquals(3, intRes);
  }

  @Test
  public void testPositonalCopyToByteArray() throws Exception {
    byte[] b = new byte[4];
    byte[] b1 = new byte[8];
    ByteBuffer bb1 = ByteBuffer.wrap(b);
    ByteBuffer bb2 = ByteBuffer.wrap(b1);
    MultiByteBuff mbb1 = new MultiByteBuff(bb1, bb2);
    mbb1.position(2);
    mbb1.putInt(4);
    mbb1.position(7);
    mbb1.put((byte) 2);
    mbb1.putInt(3);
    byte[] dst = new byte[4];
    mbb1.get(2, dst, 0, 4);
    assertEquals(4, Bytes.toInt(dst));
    assertEquals(12, mbb1.position());
    mbb1.position(1);
    dst = new byte[4];
    mbb1.get(8, dst, 0, 4);
    assertEquals(3, Bytes.toInt(dst));
    assertEquals(1, mbb1.position());
    mbb1.position(12);
    dst = new byte[1];
    mbb1.get(7, dst, 0, 1);
    assertEquals(2, dst[0]);
    assertEquals(12, mbb1.position());
  }

  @Test
  public void testToBytes() throws Exception {
    byte[] b = new byte[4];
    byte[] b1 = new byte[8];
    for (int i = 0; i < b.length; i++) {
      b[i] = (byte) i;
    }
    for (int i = 0; i < b1.length; i++) {
      b1[i] = (byte) (b1.length + i);
    }
    ByteBuffer bb1 = ByteBuffer.wrap(b);
    ByteBuffer bb2 = ByteBuffer.wrap(b1);
    MultiByteBuff mbb1 = new MultiByteBuff(bb1, bb2);

    // Test 1 Offset hitting exclusive second element
    byte[] actual = mbb1.toBytes(6, 4);
    assertTrue(Bytes.equals(actual, 0, actual.length,
            b1, 2, 4));
    // Test 2 offset hitting exclusive second element
    // but continuing to the end of the second one
    actual = mbb1.toBytes(5, 7);
    assertTrue(Bytes.equals(actual, 0, actual.length,
            b1, 1, 7));
    // Test 3 with offset hitting in first element,
    // continuing to next
    actual = mbb1.toBytes(2, 7);
    byte[] expected = new byte[7];
    System.arraycopy(b, 2, expected, 0,  2);
    System.arraycopy(b1, 0, expected, 2, 5);
    assertTrue(Bytes.equals(actual, expected));
    // Test 4 hitting only in first exclusively
    actual = mbb1.toBytes(1, 3);
    assertTrue(Bytes.equals(actual, 0, actual.length,
            b, 1, 3));
  }
}
