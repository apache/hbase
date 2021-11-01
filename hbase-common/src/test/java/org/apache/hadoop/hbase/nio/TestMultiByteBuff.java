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
package org.apache.hadoop.hbase.nio;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ObjectIntPair;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, SmallTests.class })
public class TestMultiByteBuff {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMultiByteBuff.class);

  /**
   * Test right answer though we span many sub-buffers.
   */
  @Test
  public void testGetShort() {
    ByteBuffer bb1 = ByteBuffer.allocate(1);
    bb1.put((byte)1);
    ByteBuffer bb2 = ByteBuffer.allocate(1);
    bb2.put((byte)0);
    ByteBuffer bb3 = ByteBuffer.allocate(1);
    bb3.put((byte)2);
    ByteBuffer bb4 = ByteBuffer.allocate(1);
    bb4.put((byte)3);
    MultiByteBuff mbb = new MultiByteBuff(bb1, bb2, bb3, bb4);
    assertEquals(256, mbb.getShortAfterPosition(0));
    assertEquals(2, mbb.getShortAfterPosition(1));
    assertEquals(515, mbb.getShortAfterPosition(2));
  }

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
    s.putLong(-4465109508325701663L);
    bb.rewind();
    long long1 = bb.getLong();
    assertEquals(-4465109508325701663L, long1);
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
    assertEquals(bb1, sub);
    assertEquals(l1, ByteBufferUtils.toLong(sub, sub.position()));
    multi.skip(Bytes.SIZEOF_LONG);
    sub = multi.asSubByteBuffer(Bytes.SIZEOF_LONG);
    assertNotEquals(bb1, sub);
    assertNotEquals(bb2, sub);
    assertEquals(l2, ByteBufferUtils.toLong(sub, sub.position()));
    multi.rewind();
    ObjectIntPair<ByteBuffer> p = new ObjectIntPair<>();
    multi.asSubByteBuffer(8, Bytes.SIZEOF_LONG, p);
    assertNotEquals(bb1, p.getFirst());
    assertNotEquals(bb2, p.getFirst());
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
    ByteBuff sliced = multi.slice();
    assertEquals(0, sliced.position());
    assertEquals((2 * Bytes.SIZEOF_LONG), sliced.limit());
    assertEquals(l1, sliced.getLong());
    assertEquals(l2, sliced.getLong());
    ByteBuff dup = multi.duplicate();
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
    assertEquals(expected, res);
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

  @Test
  public void testHasRemaining() {
    ByteBuffer b1 = ByteBuffer.allocate(8);
    ByteBuffer b2 = ByteBuffer.allocate(8);
    ByteBuffer b3 = ByteBuffer.allocate(8);
    MultiByteBuff mbb1 = new MultiByteBuff(b1, b2, b3);
    assertTrue(mbb1.hasRemaining());
    mbb1.limit(20); // Limit in mid of last of BB
    mbb1.position(15);
    mbb1.get();// We are at the end of second BB
    assertTrue(mbb1.hasRemaining());
    mbb1.position(20);
    assertFalse(mbb1.hasRemaining());
    mbb1.limit(12); // Limit in mid of second BB
    mbb1.position(11);
    assertTrue(mbb1.hasRemaining());
    mbb1.get(); // Now we have reached the limit
    assertFalse(mbb1.hasRemaining());
    mbb1.limit(16);// Limit at begin of the last BB
    mbb1.position(15);
    assertTrue(mbb1.hasRemaining());
    mbb1.get(); // Now we have reached the limit
    assertFalse(mbb1.hasRemaining());
  }

  @Test
  public void testGetPrimitivesWithSmallIndividualBBs() {
    short s = 45;
    int i = 2345;
    long l = 75681526L;
    ByteBuffer bb = ByteBuffer.allocate(14);
    bb.putShort(s);
    bb.putInt(i);
    bb.putLong(l);

    ByteBuffer bb1 = ((ByteBuffer) bb.duplicate().position(0).limit(1)).slice();
    ByteBuffer bb2 = ((ByteBuffer) bb.duplicate().position(1).limit(3)).slice();
    ByteBuffer bb3 = ((ByteBuffer) bb.duplicate().position(3).limit(5)).slice();
    ByteBuffer bb4 = ((ByteBuffer) bb.duplicate().position(5).limit(11)).slice();
    ByteBuffer bb5 = ((ByteBuffer) bb.duplicate().position(11).limit(12)).slice();
    ByteBuffer bb6 = ((ByteBuffer) bb.duplicate().position(12).limit(14)).slice();
    MultiByteBuff mbb = new MultiByteBuff(bb1, bb2, bb3, bb4, bb5, bb6);
    assertEquals(s, mbb.getShortAfterPosition(0));
    assertEquals(i, mbb.getIntAfterPosition(2));
    assertEquals(l, mbb.getLongAfterPosition(6));

    assertEquals(s, mbb.getShort(0));
    assertEquals(i, mbb.getInt(2));
    assertEquals(l, mbb.getLong(6));

    mbb.position(0);
    assertEquals(s, mbb.getShort());
    assertEquals(i, mbb.getInt());
    assertEquals(l, mbb.getLong());
  }

  @Test
  public void testGetByteBufferWithOffsetAndPos() {
    byte[] a = Bytes.toBytes("abcd");
    byte[] b = Bytes.toBytes("efghijkl");
    ByteBuffer aa = ByteBuffer.wrap(a);
    ByteBuffer bb = ByteBuffer.wrap(b);
    MultiByteBuff mbb = new MultiByteBuff(aa, bb);
    ByteBuffer out = ByteBuffer.allocate(12);
    mbb.get(out, 0, 1);
    assertEquals(out.position(), 1);
    assertTrue(Bytes.equals(Bytes.toBytes("a"), 0, 1, out.array(), 0, 1));

    mbb.get(out, 1, 4);
    assertEquals(out.position(), 5);
    assertTrue(Bytes.equals(Bytes.toBytes("abcde"), 0, 5, out.array(), 0, 5));

    mbb.get(out, 10, 1);
    assertEquals(out.position(), 6);
    assertTrue(Bytes.equals(Bytes.toBytes("abcdek"), 0, 6, out.array(), 0, 6));

    mbb.get(out, 0, 6);
    assertEquals(out.position(), 12);
    assertTrue(Bytes.equals(Bytes.toBytes("abcdekabcdef"), 0, 12, out.array(), 0, 12));
  }

  @Test
  public void testPositionalPutByteBuff() throws Exception {
    ByteBuffer bb1 = ByteBuffer.allocate(100);
    ByteBuffer bb2 = ByteBuffer.allocate(100);
    MultiByteBuff srcMultiByteBuff = new MultiByteBuff(bb1, bb2);
    for (int i = 0; i < 25; i++) {
      srcMultiByteBuff.putLong(i * 8L);
    }
    // Test MultiByteBuff To MultiByteBuff
    doTestPositionalPutByteBuff(srcMultiByteBuff);

    ByteBuffer bb3 = ByteBuffer.allocate(200);
    SingleByteBuff srcSingleByteBuff = new SingleByteBuff(bb3);
    for (int i = 0; i < 25; i++) {
      srcSingleByteBuff.putLong(i * 8L);
    }
    // Test SingleByteBuff To MultiByteBuff
    doTestPositionalPutByteBuff(srcSingleByteBuff);
  }

  private void doTestPositionalPutByteBuff(ByteBuff srcByteBuff) throws Exception {
    ByteBuffer bb3 = ByteBuffer.allocate(50);
    ByteBuffer bb4 = ByteBuffer.allocate(50);
    ByteBuffer bb5 = ByteBuffer.allocate(50);
    ByteBuffer bb6 = ByteBuffer.allocate(50);
    MultiByteBuff destMultiByteBuff = new MultiByteBuff(bb3, bb4, bb5, bb6);

    // full copy
    destMultiByteBuff.put(0, srcByteBuff, 0, 200);
    int compareTo = ByteBuff.compareTo(srcByteBuff, 0, 200, destMultiByteBuff, 0, 200);
    assertTrue(compareTo == 0);

    // Test src to dest first ByteBuffer
    destMultiByteBuff.put(0, srcByteBuff, 32, 63);
    compareTo = ByteBuff.compareTo(srcByteBuff, 32, 63, destMultiByteBuff, 0, 63);
    assertTrue(compareTo == 0);

    // Test src to dest first and second ByteBuffer
    destMultiByteBuff.put(0, srcByteBuff, 0, 63);
    compareTo = ByteBuff.compareTo(srcByteBuff, 0, 63, destMultiByteBuff, 0, 63);
    assertTrue(compareTo == 0);

    // Test src to dest third ByteBuffer
    destMultiByteBuff.put(100, srcByteBuff, 100, 50);
    compareTo = ByteBuff.compareTo(srcByteBuff, 100, 50, destMultiByteBuff, 100, 50);
    assertTrue(compareTo == 0);

    // Test src to dest first,second and third ByteBuffer
    destMultiByteBuff.put(48, srcByteBuff, 32, 63);
    compareTo = ByteBuff.compareTo(srcByteBuff, 32, 63, destMultiByteBuff, 48, 63);
    assertTrue(compareTo == 0);

    // Test src to dest first,second,third and fourth ByteBuffer
    destMultiByteBuff.put(48, srcByteBuff, 32, 120);
    compareTo = ByteBuff.compareTo(srcByteBuff, 32, 120, destMultiByteBuff, 48, 120);
    assertTrue(compareTo == 0);

    // Test src to dest first and second ByteBuffer
    destMultiByteBuff.put(0, srcByteBuff, 132, 63);
    compareTo = ByteBuff.compareTo(srcByteBuff, 132, 63, destMultiByteBuff, 0, 63);
    assertTrue(compareTo == 0);

    // Test src to dest second,third and fourth ByteBuffer
    destMultiByteBuff.put(95, srcByteBuff, 132, 67);
    compareTo = ByteBuff.compareTo(srcByteBuff, 132, 67, destMultiByteBuff, 95, 67);
    assertTrue(compareTo == 0);

    // Test src to dest fourth ByteBuffer
    destMultiByteBuff.put(162, srcByteBuff, 132, 24);
    compareTo = ByteBuff.compareTo(srcByteBuff, 132, 24, destMultiByteBuff, 162, 24);
    assertTrue(compareTo == 0);

    // Test src BufferUnderflowException
    try {
      destMultiByteBuff.put(0, srcByteBuff, 0, 300);
      fail();
    } catch (BufferUnderflowException e) {
      assertTrue(e != null);
    }

    try {
      destMultiByteBuff.put(95, srcByteBuff, 132, 89);
      fail();
    } catch (BufferUnderflowException e) {
      assertTrue(e != null);
    }

    // Test dest BufferOverflowException
    try {
      destMultiByteBuff.put(100, srcByteBuff, 0, 101);
      fail();
    } catch (BufferOverflowException e) {
      assertTrue(e != null);
    }

    try {
      destMultiByteBuff.put(151, srcByteBuff, 132, 68);
      fail();
    } catch (BufferOverflowException e) {
      assertTrue(e != null);
    }

    destMultiByteBuff = new MultiByteBuff(bb3, bb4);
    try {
      destMultiByteBuff.put(0, srcByteBuff, 0, 101);
      fail();
    } catch (BufferOverflowException e) {
      assertTrue(e != null);
    }
  }

  @Test
  public void testPositionalPutByte() throws Exception {
    ByteBuffer bb1 = ByteBuffer.allocate(50);
    ByteBuffer bb2 = ByteBuffer.allocate(50);
    ByteBuffer bb3 = ByteBuffer.allocate(50);
    ByteBuffer bb4 = ByteBuffer.allocate(50);
    MultiByteBuff srcMultiByteBuff = new MultiByteBuff(bb1, bb2, bb3, bb4);
    for (int i = 1; i <= 200; i++) {
      srcMultiByteBuff.put((byte) 0xff);
    }

    srcMultiByteBuff.put(20, (byte) 0);
    byte val = srcMultiByteBuff.get(20);
    assertTrue(val == 0);

    srcMultiByteBuff.put(50, (byte) 0);
    val = srcMultiByteBuff.get(50);
    assertTrue(val == 0);

    srcMultiByteBuff.put(80, (byte) 0);
    val = srcMultiByteBuff.get(80);
    assertTrue(val == 0);

    srcMultiByteBuff.put(100, (byte) 0);
    val = srcMultiByteBuff.get(100);
    assertTrue(val == 0);

    srcMultiByteBuff.put(121, (byte) 0);
    val = srcMultiByteBuff.get(121);
    assertTrue(val == 0);

    srcMultiByteBuff.put(150, (byte) 0);
    val = srcMultiByteBuff.get(150);
    assertTrue(val == 0);

    srcMultiByteBuff.put(180, (byte) 0);
    val = srcMultiByteBuff.get(180);
    assertTrue(val == 0);

    try {
      srcMultiByteBuff.put(200, (byte) 0);
      fail();
    } catch (IndexOutOfBoundsException e) {
      assertTrue(e != null);
    }

    try {
      srcMultiByteBuff.put(260, (byte) 0);
      fail();
    } catch (IndexOutOfBoundsException e) {
      assertTrue(e != null);
    }
  }
}
