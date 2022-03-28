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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.unsafe.HBasePlatformDependent;
import org.apache.hadoop.io.WritableUtils;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, MediumTests.class})
public class TestBytes {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestBytes.class);

  private static void setUnsafe(boolean value) throws Exception {
    Field field = Bytes.class.getDeclaredField("UNSAFE_UNALIGNED");
    field.setAccessible(true);

    Field modifiersField = ReflectionUtils.getModifiersField();
    modifiersField.setAccessible(true);
    int oldModifiers = field.getModifiers();
    modifiersField.setInt(field, oldModifiers & ~Modifier.FINAL);
    try {
      field.set(null, value);
    } finally {
      modifiersField.setInt(field, oldModifiers);
    }
    assertEquals(Bytes.UNSAFE_UNALIGNED, value);
  }

  @Test
  public void testShort() throws Exception  {
    testShort(false);
  }

  @Test
  public void testShortUnsafe() throws Exception  {
    testShort(true);
  }

  private static void testShort(boolean unsafe) throws Exception  {
    setUnsafe(unsafe);
    try {
      for (short n : Arrays.asList(
              Short.MIN_VALUE,
              (short) -100,
              (short) -1,
              (short) 0,
              (short) 1,
              (short) 300,
              Short.MAX_VALUE)) {
        byte[] bytes = Bytes.toBytes(n);
        assertEquals(Bytes.toShort(bytes, 0, bytes.length), n);
      }
    } finally {
      setUnsafe(HBasePlatformDependent.unaligned());
    }
  }

  @Test
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

  @Test
  public void testAdd() {
    byte[] a = {0,0,0,0,0,0,0,0,0,0};
    byte[] b = {1,1,1,1,1,1,1,1,1,1,1};
    byte[] c = {2,2,2,2,2,2,2,2,2,2,2,2};
    byte[] d = {3,3,3,3,3,3,3,3,3,3,3,3,3};
    byte[] result1 = Bytes.add(a, b, c);
    byte[] result2 = Bytes.add(new byte[][] {a, b, c});
    assertEquals(0, Bytes.compareTo(result1, result2));
    byte[] result4 = Bytes.add(result1, d);
    byte[] result5 = Bytes.add(new byte[][] {result1, d});
    assertEquals(0, Bytes.compareTo(result1, result2));
  }

  @Test
  public void testSplit() {
    byte[] lowest = Bytes.toBytes("AAA");
    byte[] middle = Bytes.toBytes("CCC");
    byte[] highest = Bytes.toBytes("EEE");
    byte[][] parts = Bytes.split(lowest, highest, 1);
    for (byte[] bytes : parts) {
      System.out.println(Bytes.toString(bytes));
    }
    assertEquals(3, parts.length);
    assertTrue(Bytes.equals(parts[1], middle));
    // Now divide into three parts.  Change highest so split is even.
    highest = Bytes.toBytes("DDD");
    parts = Bytes.split(lowest, highest, 2);
    for (byte[] part : parts) {
      System.out.println(Bytes.toString(part));
    }
    assertEquals(4, parts.length);
    // Assert that 3rd part is 'CCC'.
    assertTrue(Bytes.equals(parts[2], middle));
  }

  @Test
  public void testSplit2() {
    // More split tests.
    byte [] lowest = Bytes.toBytes("http://A");
    byte [] highest = Bytes.toBytes("http://z");
    byte [] middle = Bytes.toBytes("http://]");
    byte [][] parts = Bytes.split(lowest, highest, 1);
    for (byte[] part : parts) {
      System.out.println(Bytes.toString(part));
    }
    assertEquals(3, parts.length);
    assertTrue(Bytes.equals(parts[1], middle));
  }

  @Test
  public void testSplit3() {
    // Test invalid split cases
    byte[] low = { 1, 1, 1 };
    byte[] high = { 1, 1, 3 };

    // If swapped, should throw IAE
    try {
      Bytes.split(high, low, 1);
      fail("Should not be able to split if low > high");
    } catch(IllegalArgumentException iae) {
      // Correct
    }

    // Single split should work
    byte[][] parts = Bytes.split(low, high, 1);
    for (int i = 0; i < parts.length; i++) {
      System.out.println("" + i + " -> " + Bytes.toStringBinary(parts[i]));
    }
    assertEquals("Returned split should have 3 parts but has " + parts.length, 3, parts.length);

    // If split more than once, use additional byte to split
    parts = Bytes.split(low, high, 2);
    assertNotNull("Split with an additional byte", parts);
    assertEquals(parts.length, low.length + 1);

    // Split 0 times should throw IAE
    try {
      Bytes.split(low, high, 0);
      fail("Should not be able to split 0 times");
    } catch(IllegalArgumentException iae) {
      // Correct
    }
  }

  @Test
  public void testToInt() {
    int[] ints = { -1, 123, Integer.MIN_VALUE, Integer.MAX_VALUE };
    for (int anInt : ints) {
      byte[] b = Bytes.toBytes(anInt);
      assertEquals(anInt, Bytes.toInt(b));
      byte[] b2 = bytesWithOffset(b);
      assertEquals(anInt, Bytes.toInt(b2, 1));
      assertEquals(anInt, Bytes.toInt(b2, 1, Bytes.SIZEOF_INT));
    }
  }

  @Test
  public void testToLong() {
    long[] longs = { -1L, 123L, Long.MIN_VALUE, Long.MAX_VALUE };
    for (long aLong : longs) {
      byte[] b = Bytes.toBytes(aLong);
      assertEquals(aLong, Bytes.toLong(b));
      byte[] b2 = bytesWithOffset(b);
      assertEquals(aLong, Bytes.toLong(b2, 1));
      assertEquals(aLong, Bytes.toLong(b2, 1, Bytes.SIZEOF_LONG));
    }
  }

  @Test
  public void testToFloat() {
    float[] floats = { -1f, 123.123f, Float.MAX_VALUE };
    for (float aFloat : floats) {
      byte[] b = Bytes.toBytes(aFloat);
      assertEquals(aFloat, Bytes.toFloat(b), 0.0f);
      byte[] b2 = bytesWithOffset(b);
      assertEquals(aFloat, Bytes.toFloat(b2, 1), 0.0f);
    }
  }

  @Test
  public void testToDouble() {
    double [] doubles = {Double.MIN_VALUE, Double.MAX_VALUE};
    for (double aDouble : doubles) {
      byte[] b = Bytes.toBytes(aDouble);
      assertEquals(aDouble, Bytes.toDouble(b), 0.0);
      byte[] b2 = bytesWithOffset(b);
      assertEquals(aDouble, Bytes.toDouble(b2, 1), 0.0);
    }
  }

  @Test
  public void testToBigDecimal() {
    BigDecimal[] decimals = { new BigDecimal("-1"), new BigDecimal("123.123"),
      new BigDecimal("123123123123") };
    for (BigDecimal decimal : decimals) {
      byte[] b = Bytes.toBytes(decimal);
      assertEquals(decimal, Bytes.toBigDecimal(b));
      byte[] b2 = bytesWithOffset(b);
      assertEquals(decimal, Bytes.toBigDecimal(b2, 1, b.length));
    }
  }

  private byte[] bytesWithOffset(byte[] src) {
    // add one byte in front to test offset
    byte [] result = new byte[src.length + 1];
    result[0] = (byte) 0xAA;
    System.arraycopy(src, 0, result, 1, src.length);
    return result;
  }

  @Test
  public void testToBytesForByteBuffer() {
    byte[] array = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    ByteBuffer target = ByteBuffer.wrap(array);
    target.position(2);
    target.limit(7);

    byte[] actual = Bytes.toBytes(target);
    byte[] expected = { 0, 1, 2, 3, 4, 5, 6 };
    assertArrayEquals(expected, actual);
    assertEquals(2, target.position());
    assertEquals(7, target.limit());

    ByteBuffer target2 = target.slice();
    assertEquals(0, target2.position());
    assertEquals(5, target2.limit());

    byte[] actual2 = Bytes.toBytes(target2);
    byte[] expected2 = { 2, 3, 4, 5, 6 };
    assertArrayEquals(expected2, actual2);
    assertEquals(0, target2.position());
    assertEquals(5, target2.limit());
  }

  @Test
  public void testGetBytesForByteBuffer() {
    byte[] array = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    ByteBuffer target = ByteBuffer.wrap(array);
    target.position(2);
    target.limit(7);

    byte[] actual = Bytes.getBytes(target);
    byte[] expected = { 2, 3, 4, 5, 6 };
    assertArrayEquals(expected, actual);
    assertEquals(2, target.position());
    assertEquals(7, target.limit());
  }

  @Test
  public void testReadAsVLong() throws Exception {
    long[] longs = { -1L, 123L, Long.MIN_VALUE, Long.MAX_VALUE };
    for (long aLong : longs) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream output = new DataOutputStream(baos);
      WritableUtils.writeVLong(output, aLong);
      byte[] long_bytes_no_offset = baos.toByteArray();
      assertEquals(aLong, Bytes.readAsVLong(long_bytes_no_offset, 0));
      byte[] long_bytes_with_offset = bytesWithOffset(long_bytes_no_offset);
      assertEquals(aLong, Bytes.readAsVLong(long_bytes_with_offset, 1));
    }
  }

  @Test
  public void testToStringBinaryForBytes() {
    byte[] array = { '0', '9', 'a', 'z', 'A', 'Z', '@', 1 };
    String actual = Bytes.toStringBinary(array);
    String expected = "09azAZ@\\x01";
    assertEquals(expected, actual);

    String actual2 = Bytes.toStringBinary(array, 2, 3);
    String expected2 = "azA";
    assertEquals(expected2, actual2);
  }

  @Test
  public void testToStringBinaryForArrayBasedByteBuffer() {
    byte[] array = { '0', '9', 'a', 'z', 'A', 'Z', '@', 1 };
    ByteBuffer target = ByteBuffer.wrap(array);
    String actual = Bytes.toStringBinary(target);
    String expected = "09azAZ@\\x01";
    assertEquals(expected, actual);
  }

  @Test
  public void testToStringBinaryForReadOnlyByteBuffer() {
    byte[] array = { '0', '9', 'a', 'z', 'A', 'Z', '@', 1 };
    ByteBuffer target = ByteBuffer.wrap(array).asReadOnlyBuffer();
    String actual = Bytes.toStringBinary(target);
    String expected = "09azAZ@\\x01";
    assertEquals(expected, actual);
  }

  @Test
  public void testBinarySearch() {
    byte[][] arr = {
        { 1 },
        { 3 },
        { 5 },
        { 7 },
        { 9 },
        { 11 },
        { 13 },
        { 15 },
    };
    byte[] key1 = { 3, 1 };
    byte[] key2 = { 4, 9 };
    byte[] key2_2 = { 4 };
    byte[] key3 = { 5, 11 };
    byte[] key4 = { 0 };
    byte[] key5 = { 2 };

    assertEquals(1, Bytes.binarySearch(arr, key1, 0, 1));
    assertEquals(0, Bytes.binarySearch(arr, key1, 1, 1));
    assertEquals(-(2+1), Arrays.binarySearch(arr, key2_2,
      Bytes.BYTES_COMPARATOR));
    assertEquals(-(2+1), Bytes.binarySearch(arr, key2, 0, 1));
    assertEquals(4, Bytes.binarySearch(arr, key2, 1, 1));
    assertEquals(2, Bytes.binarySearch(arr, key3, 0, 1));
    assertEquals(5, Bytes.binarySearch(arr, key3, 1, 1));
    assertEquals(-1,
      Bytes.binarySearch(arr, key4, 0, 1));
    assertEquals(-2,
      Bytes.binarySearch(arr, key5, 0, 1));

    // Search for values to the left and to the right of each item in the array.
    for (int i = 0; i < arr.length; ++i) {
      assertEquals(-(i + 1), Bytes.binarySearch(arr,
          new byte[] { (byte) (arr[i][0] - 1) }, 0, 1));
      assertEquals(-(i + 2), Bytes.binarySearch(arr,
          new byte[] { (byte) (arr[i][0] + 1) }, 0, 1));
    }
  }

  @Test
  public void testToStringBytesBinaryReversible() {
    byte[] randomBytes = new byte[1000];
    for (int i = 0; i < 1000; i++) {
      Bytes.random(randomBytes);
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

  @Test
  public void testStartsWith() {
    assertTrue(Bytes.startsWith(Bytes.toBytes("hello"), Bytes.toBytes("h")));
    assertTrue(Bytes.startsWith(Bytes.toBytes("hello"), Bytes.toBytes("")));
    assertTrue(Bytes.startsWith(Bytes.toBytes("hello"), Bytes.toBytes("hello")));
    assertFalse(Bytes.startsWith(Bytes.toBytes("hello"), Bytes.toBytes("helloworld")));
    assertFalse(Bytes.startsWith(Bytes.toBytes(""), Bytes.toBytes("hello")));
  }

  @Test
  public void testIncrementBytes() {
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

  private static boolean checkTestIncrementBytes(long val, long amount) {
    byte[] value = Bytes.toBytes(val);
    byte[] testValue = { -1, -1, -1, -1, -1, -1, -1, -1 };
    if (value[0] > 0) {
      testValue = new byte[Bytes.SIZEOF_LONG];
    }
    System.arraycopy(value, 0, testValue, testValue.length - value.length,
        value.length);

    long incrementResult = Bytes.toLong(Bytes.incrementBytes(value, amount));

    return (Bytes.toLong(testValue) + amount) == incrementResult;
  }

  @Test
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

  @Test
  public void testCopy() {
    byte[] bytes = Bytes.toBytes("ABCDEFGHIJKLMNOPQRSTUVWXYZ");
    byte[] copy =  Bytes.copy(bytes);
    assertNotSame(bytes, copy);
    assertTrue(Bytes.equals(bytes, copy));
  }

  @Test
  public void testToBytesBinaryTrailingBackslashes() {
    try {
      Bytes.toBytesBinary("abc\\x00\\x01\\");
    } catch (StringIndexOutOfBoundsException ex) {
      fail("Illegal string access: " + ex.getMessage());
    }
  }

  @Test
  public void testToStringBinary_toBytesBinary_Reversable() {
    String bytes = Bytes.toStringBinary(Bytes.toBytes(2.17));
    assertEquals(2.17, Bytes.toDouble(Bytes.toBytesBinary(bytes)), 0);
  }

  @Test
  public void testUnsignedBinarySearch(){
    byte[] bytes = new byte[] { 0,5,123,127,-128,-100,-1 };
    Assert.assertEquals(1, Bytes.unsignedBinarySearch(bytes, 0, bytes.length, (byte)5));
    Assert.assertEquals(3, Bytes.unsignedBinarySearch(bytes, 0, bytes.length, (byte)127));
    Assert.assertEquals(4, Bytes.unsignedBinarySearch(bytes, 0, bytes.length, (byte)-128));
    Assert.assertEquals(5, Bytes.unsignedBinarySearch(bytes, 0, bytes.length, (byte)-100));
    Assert.assertEquals(6, Bytes.unsignedBinarySearch(bytes, 0, bytes.length, (byte)-1));
    Assert.assertEquals(-1-1, Bytes.unsignedBinarySearch(bytes, 0, bytes.length, (byte)2));
    Assert.assertEquals(-6-1, Bytes.unsignedBinarySearch(bytes, 0, bytes.length, (byte)-5));
  }

  @Test
  public void testUnsignedIncrement(){
    byte[] a = Bytes.toBytes(0);
    int a2 = Bytes.toInt(Bytes.unsignedCopyAndIncrement(a), 0);
    Assert.assertEquals(1, a2);

    byte[] b = Bytes.toBytes(-1);
    byte[] actual = Bytes.unsignedCopyAndIncrement(b);
    Assert.assertNotSame(b, actual);
    byte[] expected = new byte[]{1,0,0,0,0};
    assertArrayEquals(expected, actual);

    byte[] c = Bytes.toBytes(255);//should wrap to the next significant byte
    int c2 = Bytes.toInt(Bytes.unsignedCopyAndIncrement(c), 0);
    Assert.assertEquals(256, c2);
  }

  @Test
  public void testIndexOf() {
    byte[] array = Bytes.toBytes("hello");
    assertEquals(1, Bytes.indexOf(array, (byte) 'e'));
    assertEquals(4, Bytes.indexOf(array, (byte) 'o'));
    assertEquals(-1, Bytes.indexOf(array, (byte) 'a'));
    assertEquals(0, Bytes.indexOf(array, Bytes.toBytes("hel")));
    assertEquals(2, Bytes.indexOf(array, Bytes.toBytes("ll")));
    assertEquals(-1, Bytes.indexOf(array, Bytes.toBytes("hll")));
  }

  @Test
  public void testContains() {
    byte[] array = Bytes.toBytes("hello world");
    assertTrue(Bytes.contains(array, (byte) 'e'));
    assertTrue(Bytes.contains(array, (byte) 'd'));
    assertFalse(Bytes.contains(array, (byte) 'a'));
    assertTrue(Bytes.contains(array, Bytes.toBytes("world")));
    assertTrue(Bytes.contains(array, Bytes.toBytes("ello")));
    assertFalse(Bytes.contains(array, Bytes.toBytes("owo")));
  }

  @Test
  public void testZero() {
    byte[] array = Bytes.toBytes("hello");
    Bytes.zero(array);
    for (byte b : array) {
      assertEquals(0, b);
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

  @Test
  public void testPutBuffer() {
    byte[] b = new byte[100];
    for (byte i = 0; i < 100; i++) {
      Bytes.putByteBuffer(b, i, ByteBuffer.wrap(new byte[] { i }));
    }
    for (byte i = 0; i < 100; i++) {
      Assert.assertEquals(i, b[i]);
    }
  }

  @Test
  public void testToFromHex() {
    List<String> testStrings = new ArrayList<>(8);
    testStrings.addAll(Arrays.asList("", "00", "A0", "ff", "FFffFFFFFFFFFF", "12",
      "0123456789abcdef", "283462839463924623984692834692346ABCDFEDDCA0"));
    for (String testString : testStrings) {
      byte[] byteData = Bytes.fromHex(testString);
      Assert.assertEquals(testString.length() / 2, byteData.length);
      String result = Bytes.toHex(byteData);
      Assert.assertTrue(testString.equalsIgnoreCase(result));
    }

    List<byte[]> testByteData = new ArrayList<>(5);
    testByteData.addAll(Arrays.asList(new byte[0], new byte[1], new byte[10],
      new byte[] { 1, 2, 3, 4, 5 }, new byte[] { (byte) 0xFF }));
    Random rand = ThreadLocalRandom.current();
    for (int i = 0; i < 20; i++) {
      byte[] bytes = new byte[rand.nextInt(100)];
      Bytes.random(bytes);
      testByteData.add(bytes);
    }

    for (byte[] testData : testByteData) {
      String hexString = Bytes.toHex(testData);
      Assert.assertEquals(testData.length * 2, hexString.length());
      byte[] result = Bytes.fromHex(hexString);
      assertArrayEquals(testData, result);
    }
  }
}

