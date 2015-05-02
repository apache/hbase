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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.io.WritableUtils;
import org.junit.Assert;
import org.junit.experimental.categories.Category;


@Category({MiscTests.class, SmallTests.class})
public class TestBytes extends TestCase {
  public void testNullHashCode() {
    byte [] b = null;
    Exception ee = null;
    try {
      Bytes.hashCode(b);
    } catch (Exception e) {
      ee = e;
    }
    assertNotNull(ee);
  }

  public void testAdd () throws Exception {
    byte[] a = {0,0,0,0,0,0,0,0,0,0};
    byte[] b = {1,1,1,1,1,1,1,1,1,1,1};
    byte[] c = {2,2,2,2,2,2,2,2,2,2,2,2};
    byte[] d = {3,3,3,3,3,3,3,3,3,3,3,3,3};
    byte[] result1 = Bytes.add (a, b, c);
    byte[] result2 = Bytes.add (new byte[][] {a, b, c});
    assertEquals(0, Bytes.compareTo(result1, result2));
    byte[] result4 = Bytes.add (result1, d);
    byte[] result5 = Bytes.add (new byte[][] {result1, d});
    assertEquals(0, Bytes.compareTo(result1, result2));
  }

  public void testSplit() throws Exception {
    byte [] lowest = Bytes.toBytes("AAA");
    byte [] middle = Bytes.toBytes("CCC");
    byte [] highest = Bytes.toBytes("EEE");
    byte [][] parts = Bytes.split(lowest, highest, 1);
    for (int i = 0; i < parts.length; i++) {
      System.out.println(Bytes.toString(parts[i]));
    }
    assertEquals(3, parts.length);
    assertTrue(Bytes.equals(parts[1], middle));
    // Now divide into three parts.  Change highest so split is even.
    highest = Bytes.toBytes("DDD");
    parts = Bytes.split(lowest, highest, 2);
    for (int i = 0; i < parts.length; i++) {
      System.out.println(Bytes.toString(parts[i]));
    }
    assertEquals(4, parts.length);
    // Assert that 3rd part is 'CCC'.
    assertTrue(Bytes.equals(parts[2], middle));
  }

  public void testSplit2() throws Exception {
    // More split tests.
    byte [] lowest = Bytes.toBytes("http://A");
    byte [] highest = Bytes.toBytes("http://z");
    byte [] middle = Bytes.toBytes("http://]");
    byte [][] parts = Bytes.split(lowest, highest, 1);
    for (int i = 0; i < parts.length; i++) {
      System.out.println(Bytes.toString(parts[i]));
    }
    assertEquals(3, parts.length);
    assertTrue(Bytes.equals(parts[1], middle));
  }

  public void testSplit3() throws Exception {
    // Test invalid split cases
    byte [] low = { 1, 1, 1 };
    byte [] high = { 1, 1, 3 };

    // If swapped, should throw IAE
    try {
      Bytes.split(high, low, 1);
      assertTrue("Should not be able to split if low > high", false);
    } catch(IllegalArgumentException iae) {
      // Correct
    }

    // Single split should work
    byte [][] parts = Bytes.split(low, high, 1);
    for (int i = 0; i < parts.length; i++) {
      System.out.println("" + i + " -> " + Bytes.toStringBinary(parts[i]));
    }
    assertTrue("Returned split should have 3 parts but has " + parts.length, parts.length == 3);

    // If split more than once, use additional byte to split
    parts = Bytes.split(low, high, 2);
    assertTrue("Split with an additional byte", parts != null);
    assertEquals(parts.length, low.length + 1);

    // Split 0 times should throw IAE
    try {
      parts = Bytes.split(low, high, 0);
      assertTrue("Should not be able to split 0 times", false);
    } catch(IllegalArgumentException iae) {
      // Correct
    }
  }

  public void testToInt() throws Exception {
    int [] ints = {-1, 123, Integer.MIN_VALUE, Integer.MAX_VALUE};
    for (int i = 0; i < ints.length; i++) {
      byte [] b = Bytes.toBytes(ints[i]);
      assertEquals(ints[i], Bytes.toInt(b));
      byte [] b2 = bytesWithOffset(b);
      assertEquals(ints[i], Bytes.toInt(b2, 1));
      assertEquals(ints[i], Bytes.toInt(b2, 1, Bytes.SIZEOF_INT));
    }
  }

  public void testToLong() throws Exception {
    long [] longs = {-1l, 123l, Long.MIN_VALUE, Long.MAX_VALUE};
    for (int i = 0; i < longs.length; i++) {
      byte [] b = Bytes.toBytes(longs[i]);
      assertEquals(longs[i], Bytes.toLong(b));
      byte [] b2 = bytesWithOffset(b);
      assertEquals(longs[i], Bytes.toLong(b2, 1));
      assertEquals(longs[i], Bytes.toLong(b2, 1, Bytes.SIZEOF_LONG));
    }
  }

  public void testToFloat() throws Exception {
    float [] floats = {-1f, 123.123f, Float.MAX_VALUE};
    for (int i = 0; i < floats.length; i++) {
      byte [] b = Bytes.toBytes(floats[i]);
      assertEquals(floats[i], Bytes.toFloat(b));
      byte [] b2 = bytesWithOffset(b);
      assertEquals(floats[i], Bytes.toFloat(b2, 1));
    }
  }

  public void testToDouble() throws Exception {
    double [] doubles = {Double.MIN_VALUE, Double.MAX_VALUE};
    for (int i = 0; i < doubles.length; i++) {
      byte [] b = Bytes.toBytes(doubles[i]);
      assertEquals(doubles[i], Bytes.toDouble(b));
      byte [] b2 = bytesWithOffset(b);
      assertEquals(doubles[i], Bytes.toDouble(b2, 1));
    }
  }

  public void testToBigDecimal() throws Exception {
    BigDecimal [] decimals = {new BigDecimal("-1"), new BigDecimal("123.123"),
      new BigDecimal("123123123123")};
    for (int i = 0; i < decimals.length; i++) {
      byte [] b = Bytes.toBytes(decimals[i]);
      assertEquals(decimals[i], Bytes.toBigDecimal(b));
      byte [] b2 = bytesWithOffset(b);
      assertEquals(decimals[i], Bytes.toBigDecimal(b2, 1, b.length));
    }
  }

  private byte [] bytesWithOffset(byte [] src) {
    // add one byte in front to test offset
    byte [] result = new byte[src.length + 1];
    result[0] = (byte) 0xAA;
    System.arraycopy(src, 0, result, 1, src.length);
    return result;
  }

  public void testToBytesForByteBuffer() {
    byte[] array = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    ByteBuffer target = ByteBuffer.wrap(array);
    target.position(2);
    target.limit(7);

    byte[] actual = Bytes.toBytes(target);
    byte[] expected = { 0, 1, 2, 3, 4, 5, 6 };
    assertTrue(Arrays.equals(expected,  actual));
    assertEquals(2, target.position());
    assertEquals(7, target.limit());

    ByteBuffer target2 = target.slice();
    assertEquals(0, target2.position());
    assertEquals(5, target2.limit());

    byte[] actual2 = Bytes.toBytes(target2);
    byte[] expected2 = { 2, 3, 4, 5, 6 };
    assertTrue(Arrays.equals(expected2, actual2));
    assertEquals(0, target2.position());
    assertEquals(5, target2.limit());
  }

  public void testGetBytesForByteBuffer() {
    byte[] array = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    ByteBuffer target = ByteBuffer.wrap(array);
    target.position(2);
    target.limit(7);

    byte[] actual = Bytes.getBytes(target);
    byte[] expected = { 2, 3, 4, 5, 6 };
    assertTrue(Arrays.equals(expected,  actual));
    assertEquals(2, target.position());
    assertEquals(7, target.limit());
  }

  public void testReadAsVLong() throws Exception {
    long [] longs = {-1l, 123l, Long.MIN_VALUE, Long.MAX_VALUE};
    for (int i = 0; i < longs.length; i++) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream output = new DataOutputStream(baos);
      WritableUtils.writeVLong(output, longs[i]);
      byte[] long_bytes_no_offset = baos.toByteArray();
      assertEquals(longs[i], Bytes.readAsVLong(long_bytes_no_offset, 0));
      byte[] long_bytes_with_offset = bytesWithOffset(long_bytes_no_offset);
      assertEquals(longs[i], Bytes.readAsVLong(long_bytes_with_offset, 1));
    }
  }

  public void testToStringBinaryForBytes() {
    byte[] array = { '0', '9', 'a', 'z', 'A', 'Z', '@', 1 };
    String actual = Bytes.toStringBinary(array);
    String expected = "09azAZ@\\x01";
    assertEquals(expected, actual);

    String actual2 = Bytes.toStringBinary(array, 2, 3);
    String expected2 = "azA";
    assertEquals(expected2, actual2);
  }

  public void testToStringBinaryForArrayBasedByteBuffer() {
    byte[] array = { '0', '9', 'a', 'z', 'A', 'Z', '@', 1 };
    ByteBuffer target = ByteBuffer.wrap(array);
    String actual = Bytes.toStringBinary(target);
    String expected = "09azAZ@\\x01";
    assertEquals(expected, actual);
  }

  public void testToStringBinaryForReadOnlyByteBuffer() {
    byte[] array = { '0', '9', 'a', 'z', 'A', 'Z', '@', 1 };
    ByteBuffer target = ByteBuffer.wrap(array).asReadOnlyBuffer();
    String actual = Bytes.toStringBinary(target);
    String expected = "09azAZ@\\x01";
    assertEquals(expected, actual);
  }

  public void testBinarySearch() throws Exception {
    byte [][] arr = {
        {1},
        {3},
        {5},
        {7},
        {9},
        {11},
        {13},
        {15},
    };
    byte [] key1 = {3,1};
    byte [] key2 = {4,9};
    byte [] key2_2 = {4};
    byte [] key3 = {5,11};
    byte [] key4 = {0};
    byte [] key5 = {2};

    assertEquals(1, Bytes.binarySearch(arr, key1, 0, 1,
      Bytes.BYTES_RAWCOMPARATOR));
    assertEquals(0, Bytes.binarySearch(arr, key1, 1, 1,
      Bytes.BYTES_RAWCOMPARATOR));
    assertEquals(-(2+1), Arrays.binarySearch(arr, key2_2,
      Bytes.BYTES_COMPARATOR));
    assertEquals(-(2+1), Bytes.binarySearch(arr, key2, 0, 1,
      Bytes.BYTES_RAWCOMPARATOR));
    assertEquals(4, Bytes.binarySearch(arr, key2, 1, 1,
      Bytes.BYTES_RAWCOMPARATOR));
    assertEquals(2, Bytes.binarySearch(arr, key3, 0, 1,
      Bytes.BYTES_RAWCOMPARATOR));
    assertEquals(5, Bytes.binarySearch(arr, key3, 1, 1,
      Bytes.BYTES_RAWCOMPARATOR));
    assertEquals(-1,
      Bytes.binarySearch(arr, key4, 0, 1, Bytes.BYTES_RAWCOMPARATOR));
    assertEquals(-2,
      Bytes.binarySearch(arr, key5, 0, 1, Bytes.BYTES_RAWCOMPARATOR));

    // Search for values to the left and to the right of each item in the array.
    for (int i = 0; i < arr.length; ++i) {
      assertEquals(-(i + 1), Bytes.binarySearch(arr,
          new byte[] { (byte) (arr[i][0] - 1) }, 0, 1,
          Bytes.BYTES_RAWCOMPARATOR));
      assertEquals(-(i + 2), Bytes.binarySearch(arr,
          new byte[] { (byte) (arr[i][0] + 1) }, 0, 1,
          Bytes.BYTES_RAWCOMPARATOR));
    }
  }

  public void testToStringBytesBinaryReversible() {
    //  let's run test with 1000 randomly generated byte arrays
    Random rand = new Random(System.currentTimeMillis());
    byte[] randomBytes = new byte[1000];
    for (int i = 0; i < 1000; i++) {
      rand.nextBytes(randomBytes);
      verifyReversibleForBytes(randomBytes);
    }

    //  some specific cases
    verifyReversibleForBytes(new  byte[] {});
    verifyReversibleForBytes(new  byte[] {'\\', 'x', 'A', 'D'});
    verifyReversibleForBytes(new  byte[] {'\\', 'x', 'A', 'D', '\\'});
  }

  private void verifyReversibleForBytes(byte[] originalBytes) {
    String convertedString = Bytes.toStringBinary(originalBytes);
    byte[] convertedBytes = Bytes.toBytesBinary(convertedString);
    if (Bytes.compareTo(originalBytes, convertedBytes) != 0) {
      fail("Not reversible for\nbyte[]: " + Arrays.toString(originalBytes) +
          ",\nStringBinary: " + convertedString);
    }
  }

  public void testStartsWith() {
    assertTrue(Bytes.startsWith(Bytes.toBytes("hello"), Bytes.toBytes("h")));
    assertTrue(Bytes.startsWith(Bytes.toBytes("hello"), Bytes.toBytes("")));
    assertTrue(Bytes.startsWith(Bytes.toBytes("hello"), Bytes.toBytes("hello")));
    assertFalse(Bytes.startsWith(Bytes.toBytes("hello"), Bytes.toBytes("helloworld")));
    assertFalse(Bytes.startsWith(Bytes.toBytes(""), Bytes.toBytes("hello")));
  }

  public void testIncrementBytes() throws IOException {

    assertTrue(checkTestIncrementBytes(10, 1));
    assertTrue(checkTestIncrementBytes(12, 123435445));
    assertTrue(checkTestIncrementBytes(124634654, 1));
    assertTrue(checkTestIncrementBytes(10005460, 5005645));
    assertTrue(checkTestIncrementBytes(1, -1));
    assertTrue(checkTestIncrementBytes(10, -1));
    assertTrue(checkTestIncrementBytes(10, -5));
    assertTrue(checkTestIncrementBytes(1005435000, -5));
    assertTrue(checkTestIncrementBytes(10, -43657655));
    assertTrue(checkTestIncrementBytes(-1, 1));
    assertTrue(checkTestIncrementBytes(-26, 5034520));
    assertTrue(checkTestIncrementBytes(-10657200, 5));
    assertTrue(checkTestIncrementBytes(-12343250, 45376475));
    assertTrue(checkTestIncrementBytes(-10, -5));
    assertTrue(checkTestIncrementBytes(-12343250, -5));
    assertTrue(checkTestIncrementBytes(-12, -34565445));
    assertTrue(checkTestIncrementBytes(-1546543452, -34565445));
  }

  private static boolean checkTestIncrementBytes(long val, long amount)
  throws IOException {
    byte[] value = Bytes.toBytes(val);
    byte [] testValue = {-1, -1, -1, -1, -1, -1, -1, -1};
    if (value[0] > 0) {
      testValue = new byte[Bytes.SIZEOF_LONG];
    }
    System.arraycopy(value, 0, testValue, testValue.length - value.length,
        value.length);

    long incrementResult = Bytes.toLong(Bytes.incrementBytes(value, amount));

    return (Bytes.toLong(testValue) + amount) == incrementResult;
  }

  public void testFixedSizeString() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    Bytes.writeStringFixedSize(dos, "Hello", 5);
    Bytes.writeStringFixedSize(dos, "World", 18);
    Bytes.writeStringFixedSize(dos, "", 9);

    try {
      // Use a long dash which is three bytes in UTF-8. If encoding happens
      // using ISO-8859-1, this will fail.
      Bytes.writeStringFixedSize(dos, "Too\u2013Long", 9);
      fail("Exception expected");
    } catch (IOException ex) {
      assertEquals(
          "Trying to write 10 bytes (Too\\xE2\\x80\\x93Long) into a field of " +
          "length 9", ex.getMessage());
    }

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    assertEquals("Hello", Bytes.readStringFixedSize(dis, 5));
    assertEquals("World", Bytes.readStringFixedSize(dis, 18));
    assertEquals("", Bytes.readStringFixedSize(dis, 9));
  }

  public void testCopy() throws Exception {
    byte [] bytes = Bytes.toBytes("ABCDEFGHIJKLMNOPQRSTUVWXYZ");
    byte [] copy =  Bytes.copy(bytes);
    assertFalse(bytes == copy);
    assertTrue(Bytes.equals(bytes, copy));
  }

  public void testToBytesBinaryTrailingBackslashes() throws Exception {
    try {
      Bytes.toBytesBinary("abc\\x00\\x01\\");
    } catch (StringIndexOutOfBoundsException ex) {
      fail("Illegal string access: " + ex.getMessage());
    }
  }

  public void testToStringBinary_toBytesBinary_Reversable() throws Exception {
    String bytes = Bytes.toStringBinary(Bytes.toBytes(2.17));
    assertEquals(2.17, Bytes.toDouble(Bytes.toBytesBinary(bytes)), 0);        
  }

  public void testUnsignedBinarySearch(){
    byte[] bytes = new byte[]{0,5,123,127,-128,-100,-1};
    Assert.assertEquals(Bytes.unsignedBinarySearch(bytes, 0, bytes.length, (byte)5), 1);
    Assert.assertEquals(Bytes.unsignedBinarySearch(bytes, 0, bytes.length, (byte)127), 3);
    Assert.assertEquals(Bytes.unsignedBinarySearch(bytes, 0, bytes.length, (byte)-128), 4);
    Assert.assertEquals(Bytes.unsignedBinarySearch(bytes, 0, bytes.length, (byte)-100), 5);
    Assert.assertEquals(Bytes.unsignedBinarySearch(bytes, 0, bytes.length, (byte)-1), 6);
    Assert.assertEquals(Bytes.unsignedBinarySearch(bytes, 0, bytes.length, (byte)2), -1-1);
    Assert.assertEquals(Bytes.unsignedBinarySearch(bytes, 0, bytes.length, (byte)-5), -6-1);
  }

  public void testUnsignedIncrement(){
    byte[] a = Bytes.toBytes(0);
    int a2 = Bytes.toInt(Bytes.unsignedCopyAndIncrement(a), 0);
    Assert.assertTrue(a2==1);

    byte[] b = Bytes.toBytes(-1);
    byte[] actual = Bytes.unsignedCopyAndIncrement(b);
    Assert.assertNotSame(b, actual);
    byte[] expected = new byte[]{1,0,0,0,0};
    Assert.assertArrayEquals(expected, actual);

    byte[] c = Bytes.toBytes(255);//should wrap to the next significant byte
    int c2 = Bytes.toInt(Bytes.unsignedCopyAndIncrement(c), 0);
    Assert.assertTrue(c2==256);
  }
  
  public void testIndexOf() {
    byte[] array = Bytes.toBytes("hello");
    assertEquals(1, Bytes.indexOf(array, (byte) 'e'));
    assertEquals(4, Bytes.indexOf(array, (byte) 'o'));
    assertEquals(-1, Bytes.indexOf(array, (byte) 'a'));
    assertEquals(0, Bytes.indexOf(array, Bytes.toBytes("hel")));
    assertEquals(2, Bytes.indexOf(array, Bytes.toBytes("ll")));
    assertEquals(-1, Bytes.indexOf(array, Bytes.toBytes("hll")));
  }
  
  public void testContains() {
    byte[] array = Bytes.toBytes("hello world");
    assertTrue(Bytes.contains(array, (byte) 'e'));
    assertTrue(Bytes.contains(array, (byte) 'd'));
    assertFalse( Bytes.contains(array, (byte) 'a'));
    assertTrue(Bytes.contains(array, Bytes.toBytes("world")));
    assertTrue(Bytes.contains(array, Bytes.toBytes("ello")));
    assertFalse(Bytes.contains(array, Bytes.toBytes("owo")));
  }
  
  public void testZero() {
    byte[] array = Bytes.toBytes("hello");
    Bytes.zero(array);
    for (int i = 0; i < array.length; i++) {
      assertEquals(0, array[i]);
    }
    array = Bytes.toBytes("hello world");
    Bytes.zero(array, 2, 7);
    assertFalse(array[0] == 0);
    assertFalse(array[1] == 0);
    for (int i = 2; i < 9; i++) {
      assertEquals(0, array[i]);
    }
    for (int i = 9; i < array.length; i++) {
      assertFalse(array[i] == 0);
    }
  }

  public void testPutBuffer() {
    byte[] b = new byte[100];
    for (byte i = 0; i < 100; i++) {
      Bytes.putByteBuffer(b, i, ByteBuffer.wrap(new byte[]{i}));
    }
    for (byte i = 0; i < 100; i++) {
      Assert.assertEquals(i, b[i]);
    }
  }
}

