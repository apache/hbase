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

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.WritableUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, SmallTests.class })
public class TestMultiByteBuffer {

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
    MultiByteBuffer mbb = new MultiByteBuffer(bb1, bb2);
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
    mbb = new MultiByteBuffer(bb1, bb2);
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
  public void testGetVlong() throws IOException {
    long vlong = 453478;
    ByteArrayOutputStream baos = new ByteArrayOutputStream(10);
    DataOutput out = new DataOutputStream(baos);
    WritableUtils.writeVLong(out, vlong);
    ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());
    MultiByteBuffer mbb = new MultiByteBuffer(bb);
    assertEquals(vlong, mbb.getVLong());
  }

  @Test
  public void testArrayBasedMethods() {
    byte[] b = new byte[15];
    ByteBuffer bb1 = ByteBuffer.wrap(b, 1, 10).slice();
    ByteBuffer bb2 = ByteBuffer.allocate(15);
    MultiByteBuffer mbb1 = new MultiByteBuffer(bb1, bb2);
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
    mbb1 = new MultiByteBuffer(bb1);
    assertTrue(mbb1.hasArray());
    assertEquals(1, mbb1.arrayOffset());
    assertEquals(b, mbb1.array());
    mbb1 = new MultiByteBuffer(ByteBuffer.allocateDirect(10));
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
  public void testMarkAndReset() {
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
    MultiByteBuffer multi = new MultiByteBuffer(bb1, bb2);
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
    MultiByteBuffer multi = new MultiByteBuffer(bb1, bb2);
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
    MultiByteBuffer multi = new MultiByteBuffer(bb1, bb2);
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
    MultiByteBuffer multi = new MultiByteBuffer(bb1, bb2);
    long l1 = 1234L, l2 = 100L;
    multi.putLong(l1);
    multi.putLong(l2);
    multi.rewind();
    ByteBuffer sub = multi.asSubBuffer(Bytes.SIZEOF_LONG);
    assertTrue(bb1 == sub);
    assertEquals(l1, ByteBufferUtils.toLong(sub, sub.position()));
    multi.skip(Bytes.SIZEOF_LONG);
    sub = multi.asSubBuffer(Bytes.SIZEOF_LONG);
    assertFalse(bb1 == sub);
    assertFalse(bb2 == sub);
    assertEquals(l2, ByteBufferUtils.toLong(sub, sub.position()));
    multi.rewind();
    Pair<ByteBuffer, Integer> p = multi.asSubBuffer(8, Bytes.SIZEOF_LONG);
    assertFalse(bb1 == p.getFirst());
    assertFalse(bb2 == p.getFirst());
    assertEquals(0, p.getSecond().intValue());
    assertEquals(l2, ByteBufferUtils.toLong(sub, p.getSecond()));
  }

  @Test
  public void testSliceDuplicateMethods() throws Exception {
    ByteBuffer bb1 = ByteBuffer.allocateDirect(10);
    ByteBuffer bb2 = ByteBuffer.allocateDirect(15);
    MultiByteBuffer multi = new MultiByteBuffer(bb1, bb2);
    long l1 = 1234L, l2 = 100L;
    multi.put((byte) 2);
    multi.putLong(l1);
    multi.putLong(l2);
    multi.putInt(45);
    multi.position(1);
    multi.limit(multi.position() + (2 * Bytes.SIZEOF_LONG));
    MultiByteBuffer sliced = multi.slice();
    assertEquals(0, sliced.position());
    assertEquals((2 * Bytes.SIZEOF_LONG), sliced.limit());
    assertEquals(l1, sliced.getLong());
    assertEquals(l2, sliced.getLong());
    MultiByteBuffer dup = multi.duplicate();
    assertEquals(1, dup.position());
    assertEquals(dup.position() + (2 * Bytes.SIZEOF_LONG), dup.limit());
    assertEquals(l1, dup.getLong());
    assertEquals(l2, dup.getLong());
  }
}
