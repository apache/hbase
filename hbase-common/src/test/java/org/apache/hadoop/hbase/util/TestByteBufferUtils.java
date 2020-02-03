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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.io.WritableUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@Category({MiscTests.class, MediumTests.class})
@RunWith(Parameterized.class)
public class TestByteBufferUtils {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestByteBufferUtils.class);

  private static final String UNSAFE_AVAIL_NAME = "UNSAFE_AVAIL";
  private static final String UNSAFE_UNALIGNED_NAME = "UNSAFE_UNALIGNED";
  private byte[] array;

  @AfterClass
  public static void afterClass() throws Exception {
    detectAvailabilityOfUnsafe();
  }

  @Parameterized.Parameters
  public static Collection<Object[]> parameters() {
    return HBaseCommonTestingUtility.BOOLEAN_PARAMETERIZED;
  }

  private static void setUnsafe(String fieldName, boolean value) throws Exception {
    Field field = ByteBufferUtils.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    int oldModifiers = field.getModifiers();
    modifiersField.setInt(field, oldModifiers & ~Modifier.FINAL);
    try {
      field.set(null, value);
    } finally {
      modifiersField.setInt(field, oldModifiers);
    }
  }

  static void disableUnsafe() throws Exception {
    if (ByteBufferUtils.UNSAFE_AVAIL) {
      setUnsafe(UNSAFE_AVAIL_NAME, false);
    }
    if (ByteBufferUtils.UNSAFE_UNALIGNED) {
      setUnsafe(UNSAFE_UNALIGNED_NAME, false);
    }
    assertFalse(ByteBufferUtils.UNSAFE_AVAIL);
    assertFalse(ByteBufferUtils.UNSAFE_UNALIGNED);
  }

  static void detectAvailabilityOfUnsafe() throws Exception {
    if (ByteBufferUtils.UNSAFE_AVAIL != UnsafeAvailChecker.isAvailable()) {
      setUnsafe(UNSAFE_AVAIL_NAME, UnsafeAvailChecker.isAvailable());
    }
    if (ByteBufferUtils.UNSAFE_UNALIGNED != UnsafeAvailChecker.unaligned()) {
      setUnsafe(UNSAFE_UNALIGNED_NAME, UnsafeAvailChecker.unaligned());
    }
    assertEquals(ByteBufferUtils.UNSAFE_AVAIL, UnsafeAvailChecker.isAvailable());
    assertEquals(ByteBufferUtils.UNSAFE_UNALIGNED, UnsafeAvailChecker.unaligned());
  }

  public TestByteBufferUtils(boolean useUnsafeIfPossible) throws Exception {
    if (useUnsafeIfPossible) {
      detectAvailabilityOfUnsafe();
    } else {
      disableUnsafe();
    }
  }

  /**
   * Create an array with sample data.
   */
  @Before
  public void setUp() {
    array = new byte[8];
    for (int i = 0; i < array.length; ++i) {
      array[i] = (byte) ('a' + i);
    }
  }

  private static final int MAX_VLONG_LENGTH = 9;
  private static final Collection<Long> testNumbers;

  private static void addNumber(Set<Long> a, long l) {
    if (l != Long.MIN_VALUE) {
      a.add(l - 1);
    }
    a.add(l);
    if (l != Long.MAX_VALUE) {
      a.add(l + 1);
    }
    for (long divisor = 3; divisor <= 10; ++divisor) {
      for (long delta = -1; delta <= 1; ++delta) {
        a.add(l / divisor + delta);
      }
    }
  }

  static {
    SortedSet<Long> a = new TreeSet<>();
    for (int i = 0; i <= 63; ++i) {
      long v = (-1L) << i;
      assertTrue(v < 0);
      addNumber(a, v);
      v = (1L << i) - 1;
      assertTrue(v >= 0);
      addNumber(a, v);
    }

    testNumbers = Collections.unmodifiableSet(a);
    System.err.println("Testing variable-length long serialization using: "
        + testNumbers + " (count: " + testNumbers.size() + ")");
    assertEquals(1753, testNumbers.size());
    assertEquals(Long.MIN_VALUE, a.first().longValue());
    assertEquals(Long.MAX_VALUE, a.last().longValue());
  }

  @Test
  public void testReadWriteVLong() {
    for (long l : testNumbers) {
      ByteBuffer b = ByteBuffer.allocate(MAX_VLONG_LENGTH);
      ByteBufferUtils.writeVLong(b, l);
      b.flip();
      assertEquals(l, ByteBufferUtils.readVLong(b));
    }
  }

  @Test
  public void testConsistencyWithHadoopVLong() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    for (long l : testNumbers) {
      baos.reset();
      ByteBuffer b = ByteBuffer.allocate(MAX_VLONG_LENGTH);
      ByteBufferUtils.writeVLong(b, l);
      String bufStr = Bytes.toStringBinary(b.array(),
          b.arrayOffset(), b.position());
      WritableUtils.writeVLong(dos, l);
      String baosStr = Bytes.toStringBinary(baos.toByteArray());
      assertEquals(baosStr, bufStr);
    }
  }

  /**
   * Test copying to stream from buffer.
   */
  @Test
  public void testMoveBufferToStream() {
    final int arrayOffset = 7;
    final int initialPosition = 10;
    final int endPadding = 5;
    byte[] arrayWrapper =
        new byte[arrayOffset + initialPosition + array.length + endPadding];
    System.arraycopy(array, 0, arrayWrapper,
        arrayOffset + initialPosition, array.length);
    ByteBuffer buffer = ByteBuffer.wrap(arrayWrapper, arrayOffset,
        initialPosition + array.length).slice();
    assertEquals(initialPosition + array.length, buffer.limit());
    assertEquals(0, buffer.position());
    buffer.position(initialPosition);
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
      ByteBufferUtils.moveBufferToStream(bos, buffer, array.length);
    } catch (IOException e) {
      fail("IOException in testCopyToStream()");
    }
    assertArrayEquals(array, bos.toByteArray());
    assertEquals(initialPosition + array.length, buffer.position());
  }

  /**
   * Test copying to stream from buffer with offset.
   * @throws IOException On test failure.
   */
  @Test
  public void testCopyToStreamWithOffset() throws IOException {
    ByteBuffer buffer = ByteBuffer.wrap(array);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();

    ByteBufferUtils.copyBufferToStream(bos, buffer, array.length / 2,
        array.length / 2);

    byte[] returnedArray = bos.toByteArray();
    for (int i = 0; i < array.length / 2; ++i) {
      int pos = array.length / 2 + i;
      assertEquals(returnedArray[i], array[pos]);
    }
  }

  /**
   * Test copying data from stream.
   * @throws IOException On test failure.
   */
  @Test
  public void testCopyFromStream() throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(array.length);
    ByteArrayInputStream bis = new ByteArrayInputStream(array);
    DataInputStream dis = new DataInputStream(bis);

    ByteBufferUtils.copyFromStreamToBuffer(buffer, dis, array.length / 2);
    ByteBufferUtils.copyFromStreamToBuffer(buffer, dis,
        array.length - array.length / 2);
    for (int i = 0; i < array.length; ++i) {
      assertEquals(array[i], buffer.get(i));
    }
  }

  /**
   * Test copying from buffer.
   */
  @Test
  public void testCopyFromBuffer() {
    ByteBuffer srcBuffer = ByteBuffer.allocate(array.length);
    ByteBuffer dstBuffer = ByteBuffer.allocate(array.length);
    srcBuffer.put(array);

    ByteBufferUtils.copyFromBufferToBuffer(srcBuffer, dstBuffer,
        array.length / 2, array.length / 4);
    for (int i = 0; i < array.length / 4; ++i) {
      assertEquals(srcBuffer.get(i + array.length / 2),
          dstBuffer.get(i));
    }
  }

  /**
   * Test 7-bit encoding of integers.
   * @throws IOException On test failure.
   */
  @Test
  public void testCompressedInt() throws IOException {
    testCompressedInt(0);
    testCompressedInt(Integer.MAX_VALUE);
    testCompressedInt(Integer.MIN_VALUE);

    for (int i = 0; i < 3; i++) {
      testCompressedInt((128 << i) - 1);
    }

    for (int i = 0; i < 3; i++) {
      testCompressedInt((128 << i));
    }
  }

  /**
   * Test how much bytes we need to store integer.
   */
  @Test
  public void testIntFitsIn() {
    assertEquals(1, ByteBufferUtils.intFitsIn(0));
    assertEquals(1, ByteBufferUtils.intFitsIn(1));
    assertEquals(2, ByteBufferUtils.intFitsIn(1 << 8));
    assertEquals(3, ByteBufferUtils.intFitsIn(1 << 16));
    assertEquals(4, ByteBufferUtils.intFitsIn(-1));
    assertEquals(4, ByteBufferUtils.intFitsIn(Integer.MAX_VALUE));
    assertEquals(4, ByteBufferUtils.intFitsIn(Integer.MIN_VALUE));
  }

  /**
   * Test how much bytes we need to store long.
   */
  @Test
  public void testLongFitsIn() {
    assertEquals(1, ByteBufferUtils.longFitsIn(0));
    assertEquals(1, ByteBufferUtils.longFitsIn(1));
    assertEquals(3, ByteBufferUtils.longFitsIn(1L << 16));
    assertEquals(5, ByteBufferUtils.longFitsIn(1L << 32));
    assertEquals(8, ByteBufferUtils.longFitsIn(-1));
    assertEquals(8, ByteBufferUtils.longFitsIn(Long.MIN_VALUE));
    assertEquals(8, ByteBufferUtils.longFitsIn(Long.MAX_VALUE));
  }

  /**
   * Test if we are comparing equal bytes.
   */
  @Test
  public void testArePartEqual() {
    byte[] array = new byte[] { 1, 2, 3, 4, 5, 1, 2, 3, 4 };
    ByteBuffer buffer = ByteBuffer.wrap(array);
    assertTrue(ByteBufferUtils.arePartsEqual(buffer, 0, 4, 5, 4));
    assertTrue(ByteBufferUtils.arePartsEqual(buffer, 1, 2, 6, 2));
    assertFalse(ByteBufferUtils.arePartsEqual(buffer, 1, 2, 6, 3));
    assertFalse(ByteBufferUtils.arePartsEqual(buffer, 1, 3, 6, 2));
    assertFalse(ByteBufferUtils.arePartsEqual(buffer, 0, 3, 6, 3));
  }

  /**
   * Test serializing int to bytes
   */
  @Test
  public void testPutInt() {
    testPutInt(0);
    testPutInt(Integer.MAX_VALUE);

    for (int i = 0; i < 3; i++) {
      testPutInt((128 << i) - 1);
    }

    for (int i = 0; i < 3; i++) {
      testPutInt((128 << i));
    }
  }

  // Utility methods invoked from test methods

  private void testCompressedInt(int value) throws IOException {
    int parsedValue = 0;

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ByteBufferUtils.putCompressedInt(bos, value);

    ByteArrayInputStream bis = new ByteArrayInputStream(
        bos.toByteArray());
    parsedValue = ByteBufferUtils.readCompressedInt(bis);

    assertEquals(value, parsedValue);
  }

  private void testPutInt(int value) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      ByteBufferUtils.putInt(baos, value);
    } catch (IOException e) {
      throw new RuntimeException("Bug in putIn()", e);
    }

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    try {
      assertEquals(dis.readInt(), value);
    } catch (IOException e) {
      throw new RuntimeException("Bug in test!", e);
    }
  }

  @Test
  public void testToBytes(){
    ByteBuffer buffer = ByteBuffer.allocate(5);
    buffer.put(new byte[]{0,1,2,3,4});
    assertEquals(5, buffer.position());
    assertEquals(5, buffer.limit());
    byte[] copy = ByteBufferUtils.toBytes(buffer, 2);
    assertArrayEquals(new byte[]{2,3,4}, copy);
    assertEquals(5, buffer.position());
    assertEquals(5, buffer.limit());
  }

  @Test
  public void testToPrimitiveTypes() {
    ByteBuffer buffer = ByteBuffer.allocate(15);
    long l = 988L;
    int i = 135;
    short s = 7;
    buffer.putLong(l);
    buffer.putShort(s);
    buffer.putInt(i);
    assertEquals(l, ByteBufferUtils.toLong(buffer, 0));
    assertEquals(s, ByteBufferUtils.toShort(buffer, 8));
    assertEquals(i, ByteBufferUtils.toInt(buffer, 10));
  }

  @Test
  public void testCopyFromArrayToBuffer() {
    byte[] b = new byte[15];
    b[0] = -1;
    long l = 988L;
    int i = 135;
    short s = 7;
    Bytes.putLong(b, 1, l);
    Bytes.putShort(b, 9, s);
    Bytes.putInt(b, 11, i);
    ByteBuffer buffer = ByteBuffer.allocate(14);
    ByteBufferUtils.copyFromArrayToBuffer(buffer, b, 1, 14);
    buffer.rewind();
    assertEquals(l, buffer.getLong());
    assertEquals(s, buffer.getShort());
    assertEquals(i, buffer.getInt());
  }

  private void testCopyFromSrcToDestWithThreads(Object input, Object output,
    List<Integer> lengthes, List<Integer> offsets) throws InterruptedException {
    assertTrue((input instanceof ByteBuffer) || (input instanceof byte[]));
    assertTrue((output instanceof ByteBuffer) || (output instanceof byte[]));
    assertEquals(lengthes.size(), offsets.size());

    final int threads = lengthes.size();
    CountDownLatch latch = new CountDownLatch(1);
    List<Runnable> exes = new ArrayList<>(threads);
    int oldInputPos = (input instanceof ByteBuffer) ? ((ByteBuffer) input).position() : 0;
    int oldOutputPos = (output instanceof ByteBuffer) ? ((ByteBuffer) output).position() : 0;
    for (int i = 0; i != threads; ++i) {
      int offset = offsets.get(i);
      int length = lengthes.get(i);
      exes.add(() -> {
        try {
          latch.await();
          if (input instanceof ByteBuffer && output instanceof byte[]) {
            ByteBufferUtils.copyFromBufferToArray((byte[]) output,
                (ByteBuffer) input, offset, offset, length);
          }
          if (input instanceof byte[] && output instanceof ByteBuffer) {
            ByteBufferUtils.copyFromArrayToBuffer((ByteBuffer) output,
                offset, (byte[]) input, offset, length);
          }
          if (input instanceof ByteBuffer && output instanceof ByteBuffer) {
            ByteBufferUtils.copyFromBufferToBuffer((ByteBuffer) input,
                (ByteBuffer) output, offset, offset, length);
          }
        } catch (InterruptedException ex) {
          throw new RuntimeException(ex);
        }
      });
    }
    ExecutorService service = Executors.newFixedThreadPool(threads);
    exes.forEach(service::execute);
    latch.countDown();
    service.shutdown();
    assertTrue(service.awaitTermination(5, TimeUnit.SECONDS));
    if (input instanceof ByteBuffer) {
      assertEquals(oldInputPos, ((ByteBuffer) input).position());
    }
    if (output instanceof ByteBuffer) {
      assertEquals(oldOutputPos, ((ByteBuffer) output).position());
    }
    String inputString = (input instanceof ByteBuffer) ?
      Bytes.toString(Bytes.toBytes((ByteBuffer) input)) : Bytes.toString((byte[]) input);
    String outputString = (output instanceof ByteBuffer) ?
      Bytes.toString(Bytes.toBytes((ByteBuffer) output)) : Bytes.toString((byte[]) output);
    assertEquals(inputString, outputString);
  }

  @Test
  public void testCopyFromSrcToDestWithThreads() throws InterruptedException {
    List<byte[]> words = Arrays.asList(
      Bytes.toBytes("with"),
      Bytes.toBytes("great"),
      Bytes.toBytes("power"),
      Bytes.toBytes("comes"),
      Bytes.toBytes("great"),
      Bytes.toBytes("responsibility")
    );
    List<Integer> lengthes = words.stream().map(v -> v.length).collect(Collectors.toList());
    List<Integer> offsets = new ArrayList<>(words.size());
    for (int i = 0; i != words.size(); ++i) {
      offsets.add(words.subList(0, i).stream().mapToInt(v -> v.length).sum());
    }

    int totalSize = words.stream().mapToInt(v -> v.length).sum();
    byte[] fullContent = new byte[totalSize];
    int offset = 0;
    for (byte[] w : words) {
      offset = Bytes.putBytes(fullContent, offset, w, 0, w.length);
    }

    // test copyFromBufferToArray
    for (ByteBuffer input : Arrays.asList(
            ByteBuffer.allocateDirect(totalSize),
            ByteBuffer.allocate(totalSize))) {
      words.forEach(input::put);
      byte[] output = new byte[totalSize];
      testCopyFromSrcToDestWithThreads(input, output, lengthes, offsets);
    }

    // test copyFromArrayToBuffer
    for (ByteBuffer output : Arrays.asList(
            ByteBuffer.allocateDirect(totalSize),
            ByteBuffer.allocate(totalSize))) {
      byte[] input = fullContent;
      testCopyFromSrcToDestWithThreads(input, output, lengthes, offsets);
    }

    // test copyFromBufferToBuffer
    for (ByteBuffer input : Arrays.asList(
            ByteBuffer.allocateDirect(totalSize),
            ByteBuffer.allocate(totalSize))) {
      words.forEach(input::put);
      for (ByteBuffer output : Arrays.asList(
            ByteBuffer.allocateDirect(totalSize),
            ByteBuffer.allocate(totalSize))) {
        testCopyFromSrcToDestWithThreads(input, output, lengthes, offsets);
      }
    }
  }

  @Test
  public void testCopyFromBufferToArray() {
    ByteBuffer buffer = ByteBuffer.allocate(15);
    buffer.put((byte) -1);
    long l = 988L;
    int i = 135;
    short s = 7;
    buffer.putShort(s);
    buffer.putInt(i);
    buffer.putLong(l);
    byte[] b = new byte[15];
    ByteBufferUtils.copyFromBufferToArray(b, buffer, 1, 1, 14);
    assertEquals(s, Bytes.toShort(b, 1));
    assertEquals(i, Bytes.toInt(b, 3));
    assertEquals(l, Bytes.toLong(b, 7));
  }

  @Test
  public void testRelativeCopyFromBuffertoBuffer() {
    ByteBuffer bb1 = ByteBuffer.allocate(135);
    ByteBuffer bb2 = ByteBuffer.allocate(135);
    fillBB(bb1, (byte) 5);
    ByteBufferUtils.copyFromBufferToBuffer(bb1, bb2);
    assertTrue(bb1.position() == bb2.position());
    assertTrue(bb1.limit() == bb2.limit());
    bb1 = ByteBuffer.allocateDirect(135);
    bb2 = ByteBuffer.allocateDirect(135);
    fillBB(bb1, (byte) 5);
    ByteBufferUtils.copyFromBufferToBuffer(bb1, bb2);
    assertTrue(bb1.position() == bb2.position());
    assertTrue(bb1.limit() == bb2.limit());
  }
  @Test
  public void testCompareTo() {
    ByteBuffer bb1 = ByteBuffer.allocate(135);
    ByteBuffer bb2 = ByteBuffer.allocate(135);
    byte[] b = new byte[71];
    fillBB(bb1, (byte) 5);
    fillBB(bb2, (byte) 5);
    fillArray(b, (byte) 5);
    assertEquals(0, ByteBufferUtils.compareTo(bb1, 0, bb1.remaining(), bb2, 0, bb2.remaining()));
    assertTrue(ByteBufferUtils.compareTo(bb1, 0, bb1.remaining(), b, 0, b.length) > 0);
    bb2.put(134, (byte) 6);
    assertTrue(ByteBufferUtils.compareTo(bb1, 0, bb1.remaining(), bb2, 0, bb2.remaining()) < 0);
    bb2.put(6, (byte) 4);
    assertTrue(ByteBufferUtils.compareTo(bb1, 0, bb1.remaining(), bb2, 0, bb2.remaining()) > 0);
    // Assert reverse comparing BB and bytearray works.
    ByteBuffer bb3 = ByteBuffer.allocate(135);
    fillBB(bb3, (byte)0);
    byte[] b3 = new byte[135];
    fillArray(b3, (byte)1);
    int result = ByteBufferUtils.compareTo(b3, 0, b3.length, bb3, 0, bb3.remaining());
    assertTrue(result > 0);
    result = ByteBufferUtils.compareTo(bb3, 0, bb3.remaining(), b3, 0, b3.length);
    assertTrue(result < 0);

    byte[] b4 = Bytes.toBytes("123");
    ByteBuffer bb4 = ByteBuffer.allocate(10 + b4.length);
    for (int i = 10; i < (bb4.capacity()); ++i) {
      bb4.put(i, b4[i - 10]);
    }
    result = ByteBufferUtils.compareTo(b4, 0, b4.length, bb4, 10, b4.length);
    assertEquals(0, result);
  }

  @Test
  public void testEquals() {
    byte[] a = Bytes.toBytes("http://A");
    ByteBuffer bb = ByteBuffer.wrap(a);

    assertTrue(ByteBufferUtils.equals(HConstants.EMPTY_BYTE_BUFFER, 0, 0,
        HConstants.EMPTY_BYTE_BUFFER, 0, 0));

    assertFalse(ByteBufferUtils.equals(HConstants.EMPTY_BYTE_BUFFER, 0, 0, bb,
        0, a.length));

    assertFalse(ByteBufferUtils.equals(bb, 0, 0, HConstants.EMPTY_BYTE_BUFFER,
        0, a.length));

    assertTrue(ByteBufferUtils.equals(bb, 0, a.length, bb, 0, a.length));

    assertTrue(ByteBufferUtils.equals(HConstants.EMPTY_BYTE_BUFFER, 0, 0,
        HConstants.EMPTY_BYTE_ARRAY, 0, 0));

    assertFalse(ByteBufferUtils.equals(HConstants.EMPTY_BYTE_BUFFER, 0, 0, a,
        0, a.length));

    assertFalse(ByteBufferUtils.equals(bb, 0, a.length,
        HConstants.EMPTY_BYTE_ARRAY, 0, 0));

    assertTrue(ByteBufferUtils.equals(bb, 0, a.length, a, 0, a.length));
  }

  private static void fillBB(ByteBuffer bb, byte b) {
    for (int i = bb.position(); i < bb.limit(); i++) {
      bb.put(i, b);
    }
  }

  private static void fillArray(byte[] bb, byte b) {
    for (int i = 0; i < bb.length; i++) {
      bb[i] = b;
    }
  }

}
