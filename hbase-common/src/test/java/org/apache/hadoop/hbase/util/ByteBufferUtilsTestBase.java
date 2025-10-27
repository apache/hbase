/*
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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
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
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.io.WritableUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ByteBufferUtilsTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(ByteBufferUtilsTestBase.class);

  private static int MAX_VLONG_LENGTH = 9;
  private static Collection<Long> testNumbers;

  private byte[] array;

  @BeforeAll
  public static void setUpBeforeAll() {
    SortedSet<Long> a = new TreeSet<>();
    for (int i = 0; i <= 63; ++i) {
      long v = -1L << i;
      assertTrue(v < 0);
      addNumber(a, v);
      v = (1L << i) - 1;
      assertTrue(v >= 0);
      addNumber(a, v);
    }

    testNumbers = Collections.unmodifiableSet(a);
    LOG.info("Testing variable-length long serialization using: {} (count: {})", testNumbers,
      testNumbers.size());
    assertEquals(1753, testNumbers.size());
    assertEquals(Long.MIN_VALUE, a.first().longValue());
    assertEquals(Long.MAX_VALUE, a.last().longValue());
  }

  /**
   * Create an array with sample data.
   */
  @BeforeEach
  public void setUp() {
    array = new byte[8];
    for (int i = 0; i < array.length; ++i) {
      array[i] = (byte) ('a' + i);
    }
  }

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

  @Test
  public void testReadWriteVLong() {
    for (long l : testNumbers) {
      ByteBuffer b = ByteBuffer.allocate(MAX_VLONG_LENGTH);
      ByteBufferUtils.writeVLong(b, l);
      b.flip();
      assertEquals(l, ByteBufferUtils.readVLong(b));
      b.flip();
      assertEquals(l, ByteBufferUtils.readVLong(ByteBuff.wrap(b)));
    }
  }

  @Test
  public void testReadWriteConsecutiveVLong() {
    for (long l : testNumbers) {
      ByteBuffer b = ByteBuffer.allocate(2 * MAX_VLONG_LENGTH);
      ByteBufferUtils.writeVLong(b, l);
      ByteBufferUtils.writeVLong(b, l - 4);
      b.flip();
      assertEquals(l, ByteBufferUtils.readVLong(b));
      assertEquals(l - 4, ByteBufferUtils.readVLong(b));
      b.flip();
      assertEquals(l, ByteBufferUtils.readVLong(ByteBuff.wrap(b)));
      assertEquals(l - 4, ByteBufferUtils.readVLong(ByteBuff.wrap(b)));
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
      String bufStr = Bytes.toStringBinary(b.array(), b.arrayOffset(), b.position());
      WritableUtils.writeVLong(dos, l);
      String baosStr = Bytes.toStringBinary(baos.toByteArray());
      assertEquals(baosStr, bufStr);
    }
  }

  /**
   * Test copying to stream from buffer.
   */
  @Test
  public void testMoveBufferToStream() throws IOException {
    final int arrayOffset = 7;
    final int initialPosition = 10;
    final int endPadding = 5;
    byte[] arrayWrapper = new byte[arrayOffset + initialPosition + array.length + endPadding];
    System.arraycopy(array, 0, arrayWrapper, arrayOffset + initialPosition, array.length);
    ByteBuffer buffer =
      ByteBuffer.wrap(arrayWrapper, arrayOffset, initialPosition + array.length).slice();
    assertEquals(initialPosition + array.length, buffer.limit());
    assertEquals(0, buffer.position());
    buffer.position(initialPosition);
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ByteBufferUtils.moveBufferToStream(bos, buffer, array.length);
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

    ByteBufferUtils.copyBufferToStream(bos, buffer, array.length / 2, array.length / 2);

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
    ByteBufferUtils.copyFromStreamToBuffer(buffer, dis, array.length - array.length / 2);
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

    ByteBufferUtils.copyFromBufferToBuffer(srcBuffer, dstBuffer, array.length / 2,
      array.length / 4);
    for (int i = 0; i < array.length / 4; ++i) {
      assertEquals(srcBuffer.get(i + array.length / 2), dstBuffer.get(i));
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

  @Test
  public void testToBytes() {
    ByteBuffer buffer = ByteBuffer.allocate(5);
    buffer.put(new byte[] { 0, 1, 2, 3, 4 });
    assertEquals(5, buffer.position());
    assertEquals(5, buffer.limit());
    byte[] copy = ByteBufferUtils.toBytes(buffer, 2);
    assertArrayEquals(new byte[] { 2, 3, 4 }, copy);
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

  private void testCopyFromSrcToDestWithThreads(Object input, Object output, List<Integer> lengthes,
    List<Integer> offsets) throws InterruptedException {
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
            ByteBufferUtils.copyFromBufferToArray((byte[]) output, (ByteBuffer) input, offset,
              offset, length);
          }
          if (input instanceof byte[] && output instanceof ByteBuffer) {
            ByteBufferUtils.copyFromArrayToBuffer((ByteBuffer) output, offset, (byte[]) input,
              offset, length);
          }
          if (input instanceof ByteBuffer && output instanceof ByteBuffer) {
            ByteBufferUtils.copyFromBufferToBuffer((ByteBuffer) input, (ByteBuffer) output, offset,
              offset, length);
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
    String inputString = (input instanceof ByteBuffer)
      ? Bytes.toString(Bytes.toBytes((ByteBuffer) input))
      : Bytes.toString((byte[]) input);
    String outputString = (output instanceof ByteBuffer)
      ? Bytes.toString(Bytes.toBytes((ByteBuffer) output))
      : Bytes.toString((byte[]) output);
    assertEquals(inputString, outputString);
  }

  @Test
  public void testCopyFromSrcToDestWithThreads() throws InterruptedException {
    List<byte[]> words =
      Arrays.asList(Bytes.toBytes("with"), Bytes.toBytes("great"), Bytes.toBytes("power"),
        Bytes.toBytes("comes"), Bytes.toBytes("great"), Bytes.toBytes("responsibility"));
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
    for (ByteBuffer input : Arrays.asList(ByteBuffer.allocateDirect(totalSize),
      ByteBuffer.allocate(totalSize))) {
      words.forEach(input::put);
      byte[] output = new byte[totalSize];
      testCopyFromSrcToDestWithThreads(input, output, lengthes, offsets);
    }

    // test copyFromArrayToBuffer
    for (ByteBuffer output : Arrays.asList(ByteBuffer.allocateDirect(totalSize),
      ByteBuffer.allocate(totalSize))) {
      byte[] input = fullContent;
      testCopyFromSrcToDestWithThreads(input, output, lengthes, offsets);
    }

    // test copyFromBufferToBuffer
    for (ByteBuffer input : Arrays.asList(ByteBuffer.allocateDirect(totalSize),
      ByteBuffer.allocate(totalSize))) {
      words.forEach(input::put);
      for (ByteBuffer output : Arrays.asList(ByteBuffer.allocateDirect(totalSize),
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
    fillBB(bb3, (byte) 0);
    byte[] b3 = new byte[135];
    fillArray(b3, (byte) 1);
    int result = ByteBufferUtils.compareTo(b3, 0, b3.length, bb3, 0, bb3.remaining());
    assertTrue(result > 0);
    result = ByteBufferUtils.compareTo(bb3, 0, bb3.remaining(), b3, 0, b3.length);
    assertTrue(result < 0);
    byte[] b4 = Bytes.toBytes("123");
    ByteBuffer bb4 = ByteBuffer.allocate(10 + b4.length);
    for (int i = 10; i < bb4.capacity(); ++i) {
      bb4.put(i, b4[i - 10]);
    }
    result = ByteBufferUtils.compareTo(b4, 0, b4.length, bb4, 10, b4.length);
    assertEquals(0, result);
  }

  @Test
  public void testCompareToEmptyBuffers() {
    ByteBuffer empty1 = ByteBuffer.allocate(0);
    ByteBuffer empty2 = ByteBuffer.allocate(0);
    ByteBuffer nonEmpty = ByteBuffer.allocate(10);
    fillBB(nonEmpty, (byte) 1);
    byte[] emptyArray = new byte[0];
    byte[] nonEmptyArray = new byte[10];
    fillArray(nonEmptyArray, (byte) 1);

    // two empty buffers are equal
    assertEquals(0, ByteBufferUtils.compareTo(empty1, 0, 0, empty2, 0, 0));
    // nonempty buffer is greater than empty
    assertTrue(ByteBufferUtils.compareTo(nonEmpty, 0, 10, empty1, 0, 0) > 0);
    // empty buffer is less than nonempty
    assertTrue(ByteBufferUtils.compareTo(empty1, 0, 0, nonEmpty, 0, 10) < 0);
    // empty buffer is equal to empty array
    assertEquals(0, ByteBufferUtils.compareTo(empty1, 0, 0, emptyArray, 0, 0));
    // nonempty buffer is greater than empty array
    assertTrue(ByteBufferUtils.compareTo(nonEmpty, 0, 10, emptyArray, 0, 0) > 0);
    // empty buffer is less than nonempty array
    assertTrue(ByteBufferUtils.compareTo(empty1, 0, 0, nonEmptyArray, 0, 10) < 0);
  }

  @Test
  public void testCompareToSingleByte() {
    ByteBuffer bb1 = ByteBuffer.allocate(1);
    ByteBuffer bb2 = ByteBuffer.allocate(1);
    byte[] b1 = new byte[1];
    byte[] b2 = new byte[1];

    bb1.put(0, (byte) 5);
    bb2.put(0, (byte) 10);
    b1[0] = (byte) 5;
    b2[0] = (byte) 10;

    // 5 < 10
    assertTrue(ByteBufferUtils.compareTo(bb1, 0, 1, bb2, 0, 1) < 0);
    // 5 < 10
    assertTrue(ByteBufferUtils.compareTo(bb1, 0, 1, b2, 0, 1) < 0);
    // 10 > 5
    assertTrue(ByteBufferUtils.compareTo(bb2, 0, 1, b1, 0, 1) > 0);
    // 5 == 5
    assertEquals(0, ByteBufferUtils.compareTo(bb1, 0, 1, b1, 0, 1));
  }

  @Test
  public void testCompareToLengthDifferences() {
    ByteBuffer longer = ByteBuffer.allocate(20);
    ByteBuffer shorter = ByteBuffer.allocate(10);
    fillBB(longer, (byte) 5);
    fillBB(shorter, (byte) 5);

    // long buffer > short buffer
    assertTrue(ByteBufferUtils.compareTo(longer, 0, 20, shorter, 0, 10) > 0);
    // short buffer < long buffer
    assertTrue(ByteBufferUtils.compareTo(shorter, 0, 10, longer, 0, 20) < 0);
    // short slice of long buffer == short buffer
    assertEquals(0, ByteBufferUtils.compareTo(longer, 0, 10, shorter, 0, 10));

    byte[] longerArray = new byte[20];
    byte[] shorterArray = new byte[10];
    fillArray(longerArray, (byte) 5);
    fillArray(shorterArray, (byte) 5);

    // long buffer > short array
    assertTrue(ByteBufferUtils.compareTo(longer, 0, 20, shorterArray, 0, 10) > 0);
    // short buffer < long array
    assertTrue(ByteBufferUtils.compareTo(shorter, 0, 10, longerArray, 0, 20) < 0);
  }

  @Test
  public void testCompareToWithOffsets() {
    ByteBuffer bb = ByteBuffer.allocate(30);
    for (int i = 0; i < 30; i++) {
      bb.put(i, (byte) (i % 10));
    }

    // equal slices of equal buffers are equal
    assertEquals(0, ByteBufferUtils.compareTo(bb, 5, 10, bb, 5, 10));
    assertEquals(0, ByteBufferUtils.compareTo(bb, 0, 10, bb, 10, 10));

    // nonequal slices of equal buffers are nonequal
    assertTrue(ByteBufferUtils.compareTo(bb, 0, 10, bb, 11, 10) < 0);
    assertTrue(ByteBufferUtils.compareTo(bb, 11, 10, bb, 0, 10) > 0);

    byte[] array = new byte[10];
    for (int i = 0; i < 10; i++) {
      array[i] = (byte) (i % 10);
    }
    // equal slices of buffers + arrays are equal
    assertEquals(0, ByteBufferUtils.compareTo(bb, 0, 10, array, 0, 10));
    assertEquals(0, ByteBufferUtils.compareTo(bb, 10, 10, array, 0, 10));
  }

  @Test
  public void testCompareToDirectVsHeapBuffers() {
    ByteBuffer heap = ByteBuffer.allocate(100);
    ByteBuffer direct = ByteBuffer.allocateDirect(100);
    fillBB(heap, (byte) 42);
    fillBB(direct, (byte) 42);

    // equal buffers are equal, even if direct versus heap
    assertEquals(0, ByteBufferUtils.compareTo(heap, 0, 100, direct, 0, 100));
    assertEquals(0, ByteBufferUtils.compareTo(direct, 0, 100, heap, 0, 100));

    heap.put(50, (byte) 41);
    assertTrue(ByteBufferUtils.compareTo(heap, 0, 100, direct, 0, 100) < 0);
    assertTrue(ByteBufferUtils.compareTo(direct, 0, 100, heap, 0, 100) > 0);

    heap.put(50, (byte) 43);
    assertTrue(ByteBufferUtils.compareTo(heap, 0, 100, direct, 0, 100) > 0);
    assertTrue(ByteBufferUtils.compareTo(direct, 0, 100, heap, 0, 100) < 0);
  }

  @Test
  public void testCompareToStrideBoundaries() {
    ByteBuffer bb1 = ByteBuffer.allocate(100);
    ByteBuffer bb2 = ByteBuffer.allocate(100);
    fillBB(bb1, (byte) 5);
    fillBB(bb2, (byte) 5);

    bb1.put(7, (byte) 6);
    assertTrue(ByteBufferUtils.compareTo(bb1, 0, 100, bb2, 0, 100) > 0);

    fillBB(bb1, (byte) 5);
    bb1.put(8, (byte) 6);
    assertTrue(ByteBufferUtils.compareTo(bb1, 0, 100, bb2, 0, 100) > 0);

    fillBB(bb1, (byte) 5);
    bb1.put(16, (byte) 6);
    assertTrue(ByteBufferUtils.compareTo(bb1, 0, 100, bb2, 0, 100) > 0);

    fillBB(bb1, (byte) 5);
    bb1.put(24, (byte) 6);
    assertTrue(ByteBufferUtils.compareTo(bb1, 0, 100, bb2, 0, 100) > 0);

    fillBB(bb1, (byte) 5);
    bb1.put(31, (byte) 6);
    assertTrue(ByteBufferUtils.compareTo(bb1, 0, 100, bb2, 0, 100) > 0);
  }

  @Test
  public void testCompareToUnsignedBytes() {
    ByteBuffer bb1 = ByteBuffer.allocate(10);
    ByteBuffer bb2 = ByteBuffer.allocate(10);
    fillBB(bb1, (byte) 0);
    fillBB(bb2, (byte) 0);

    bb1.put(5, (byte) 127);
    bb2.put(5, (byte) -128);
    assertTrue(ByteBufferUtils.compareTo(bb1, 0, 10, bb2, 0, 10) < 0);

    fillBB(bb1, (byte) 0);
    fillBB(bb2, (byte) 0);
    bb1.put(5, (byte) -1);
    bb2.put(5, (byte) 0);
    assertTrue(ByteBufferUtils.compareTo(bb1, 0, 10, bb2, 0, 10) > 0);

    byte[] b1 = new byte[10];
    byte[] b2 = new byte[10];
    Arrays.fill(b1, (byte) 0);
    Arrays.fill(b2, (byte) 0);
    b1[5] = (byte) -1;
    b2[5] = (byte) 0;
    assertTrue(ByteBufferUtils.compareTo(bb1, 0, 10, b2, 0, 10) > 0);
  }

  @Test
  public void testEquals() {
    byte[] a = Bytes.toBytes("http://A");
    ByteBuffer bb = ByteBuffer.wrap(a);

    assertTrue(ByteBufferUtils.equals(HConstants.EMPTY_BYTE_BUFFER, 0, 0,
      HConstants.EMPTY_BYTE_BUFFER, 0, 0));

    assertFalse(ByteBufferUtils.equals(HConstants.EMPTY_BYTE_BUFFER, 0, 0, bb, 0, a.length));

    assertFalse(ByteBufferUtils.equals(bb, 0, 0, HConstants.EMPTY_BYTE_BUFFER, 0, a.length));

    assertTrue(ByteBufferUtils.equals(bb, 0, a.length, bb, 0, a.length));

    assertTrue(ByteBufferUtils.equals(HConstants.EMPTY_BYTE_BUFFER, 0, 0,
      HConstants.EMPTY_BYTE_ARRAY, 0, 0));

    assertFalse(ByteBufferUtils.equals(HConstants.EMPTY_BYTE_BUFFER, 0, 0, a, 0, a.length));

    assertFalse(ByteBufferUtils.equals(bb, 0, a.length, HConstants.EMPTY_BYTE_ARRAY, 0, 0));

    assertTrue(ByteBufferUtils.equals(bb, 0, a.length, a, 0, a.length));
  }

  @Test
  public void testFindCommonPrefix() {
    ByteBuffer bb1 = ByteBuffer.allocate(135);
    ByteBuffer bb2 = ByteBuffer.allocate(135);
    ByteBuffer bb3 = ByteBuffer.allocateDirect(135);
    byte[] b = new byte[71];

    fillBB(bb1, (byte) 5);
    fillBB(bb2, (byte) 5);
    fillBB(bb3, (byte) 5);
    fillArray(b, (byte) 5);

    assertEquals(135,
      ByteBufferUtils.findCommonPrefix(bb1, 0, bb1.remaining(), bb2, 0, bb2.remaining()));
    assertEquals(71, ByteBufferUtils.findCommonPrefix(bb1, 0, bb1.remaining(), b, 0, b.length));
    assertEquals(135,
      ByteBufferUtils.findCommonPrefix(bb1, 0, bb1.remaining(), bb3, 0, bb3.remaining()));
    assertEquals(71, ByteBufferUtils.findCommonPrefix(bb3, 0, bb3.remaining(), b, 0, b.length));

    b[13] = 9;
    assertEquals(13, ByteBufferUtils.findCommonPrefix(bb1, 0, bb1.remaining(), b, 0, b.length));

    bb2.put(134, (byte) 6);
    assertEquals(134,
      ByteBufferUtils.findCommonPrefix(bb1, 0, bb1.remaining(), bb2, 0, bb2.remaining()));

    bb2.put(6, (byte) 4);
    assertEquals(6,
      ByteBufferUtils.findCommonPrefix(bb1, 0, bb1.remaining(), bb2, 0, bb2.remaining()));
  }

  @Test
  public void testConverterToShort() {
    ByteBuffer heap = ByteBuffer.allocate(20);
    ByteBuffer direct = ByteBuffer.allocateDirect(20);

    short[] testValues =
      { 0, 1, -1, Short.MIN_VALUE, Short.MAX_VALUE, 127, -128, 255, -255, 1000, -1000 };

    for (short value : testValues) {
      heap.clear();
      direct.clear();

      heap.putShort(0, value);
      heap.putShort(5, value);
      heap.putShort(10, value);
      direct.putShort(0, value);
      direct.putShort(5, value);
      direct.putShort(10, value);

      assertEquals(value, ByteBufferUtils.toShort(heap, 0));
      assertEquals(value, ByteBufferUtils.toShort(heap, 5));
      assertEquals(value, ByteBufferUtils.toShort(heap, 10));
      assertEquals(value, ByteBufferUtils.toShort(direct, 0));
      assertEquals(value, ByteBufferUtils.toShort(direct, 5));
      assertEquals(value, ByteBufferUtils.toShort(direct, 10));
    }
  }

  @Test
  public void testConverterToIntWithPosition() {
    ByteBuffer heap = ByteBuffer.allocate(20);
    ByteBuffer direct = ByteBuffer.allocateDirect(20);

    int[] testValues = { 0, 1, -1, Integer.MIN_VALUE, Integer.MAX_VALUE, 127, -128, 65535, -65535,
      1000000, -1000000 };

    for (int value : testValues) {
      heap.clear();
      direct.clear();

      heap.putInt(0, value);
      heap.putInt(4, value + 1);
      heap.putInt(8, value + 2);
      direct.putInt(0, value);
      direct.putInt(4, value + 1);
      direct.putInt(8, value + 2);

      heap.position(0);
      assertEquals(value, ByteBufferUtils.toInt(heap));
      assertEquals(4, heap.position());
      assertEquals(value + 1, ByteBufferUtils.toInt(heap));
      assertEquals(8, heap.position());

      direct.position(0);
      assertEquals(value, ByteBufferUtils.toInt(direct));
      assertEquals(4, direct.position());
      assertEquals(value + 1, ByteBufferUtils.toInt(direct));
      assertEquals(8, direct.position());
    }
  }

  @Test
  public void testConverterToIntWithOffset() {
    ByteBuffer heap = ByteBuffer.allocate(20);
    ByteBuffer direct = ByteBuffer.allocateDirect(20);

    int[] testValues = { 0, 1, -1, Integer.MIN_VALUE, Integer.MAX_VALUE, 127, -128, 65535, -65535,
      1000000, -1000000 };

    for (int value : testValues) {
      heap.clear();
      direct.clear();

      heap.putInt(0, value);
      heap.putInt(4, value);
      heap.putInt(12, value);
      direct.putInt(0, value);
      direct.putInt(4, value);
      direct.putInt(12, value);

      assertEquals(value, ByteBufferUtils.toInt(heap, 0));
      assertEquals(value, ByteBufferUtils.toInt(heap, 4));
      assertEquals(value, ByteBufferUtils.toInt(heap, 12));
      assertEquals(value, ByteBufferUtils.toInt(direct, 0));
      assertEquals(value, ByteBufferUtils.toInt(direct, 4));
      assertEquals(value, ByteBufferUtils.toInt(direct, 12));
    }
  }

  @Test
  public void testConverterToLong() {
    ByteBuffer heap = ByteBuffer.allocate(32);
    ByteBuffer direct = ByteBuffer.allocateDirect(32);

    long[] testValues = { 0L, 1L, -1L, Long.MIN_VALUE, Long.MAX_VALUE, 127L, -128L, 65535L, -65535L,
      Integer.MAX_VALUE + 1L, Integer.MIN_VALUE - 1L, 1000000000000L, -1000000000000L };

    for (long value : testValues) {
      heap.clear();
      direct.clear();

      heap.putLong(0, value);
      heap.putLong(8, value);
      heap.putLong(16, value);
      direct.putLong(0, value);
      direct.putLong(8, value);
      direct.putLong(16, value);

      assertEquals(value, ByteBufferUtils.toLong(heap, 0));
      assertEquals(value, ByteBufferUtils.toLong(heap, 8));
      assertEquals(value, ByteBufferUtils.toLong(heap, 16));
      assertEquals(value, ByteBufferUtils.toLong(direct, 0));
      assertEquals(value, ByteBufferUtils.toLong(direct, 8));
      assertEquals(value, ByteBufferUtils.toLong(direct, 16));
    }
  }

  @Test
  public void testConverterPutShortWithPosition() {
    ByteBuffer heap = ByteBuffer.allocate(20);
    ByteBuffer direct = ByteBuffer.allocateDirect(20);

    short[] testValues =
      { 0, 1, -1, Short.MIN_VALUE, Short.MAX_VALUE, 127, -128, 255, -255, 1000, -1000 };

    for (short value : testValues) {
      heap.clear();
      direct.clear();

      heap.position(0);
      ByteBufferUtils.putShort(heap, value);
      assertEquals(2, heap.position());
      ByteBufferUtils.putShort(heap, (short) (value + 1));
      assertEquals(4, heap.position());

      heap.position(0);
      assertEquals(value, heap.getShort());
      assertEquals((short) (value + 1), heap.getShort());

      direct.position(0);
      ByteBufferUtils.putShort(direct, value);
      assertEquals(2, direct.position());
      ByteBufferUtils.putShort(direct, (short) (value + 1));
      assertEquals(4, direct.position());

      direct.position(0);
      assertEquals(value, direct.getShort());
      assertEquals((short) (value + 1), direct.getShort());
    }
  }

  @Test
  public void testConverterPutShortWithIndex() {
    ByteBuffer heap = ByteBuffer.allocate(20);
    ByteBuffer direct = ByteBuffer.allocateDirect(20);

    short[] testValues =
      { 0, 1, -1, Short.MIN_VALUE, Short.MAX_VALUE, 127, -128, 255, -255, 1000, -1000 };

    for (short value : testValues) {
      heap.clear();
      direct.clear();

      heap.position(5);
      int newIndex = ByteBufferUtils.putShort(heap, 0, value);
      assertEquals(0 + Short.BYTES, newIndex);
      assertEquals(5, heap.position());
      assertEquals(value, heap.getShort(5));

      direct.position(5);
      newIndex = ByteBufferUtils.putShort(direct, 0, value);
      assertEquals(0 + Short.BYTES, newIndex);
      assertEquals(5, direct.position());
      assertEquals(value, direct.getShort(5));
    }
  }

  @Test
  public void testConverterPutIntWithPosition() {
    ByteBuffer heap = ByteBuffer.allocate(20);
    ByteBuffer direct = ByteBuffer.allocateDirect(20);

    int[] testValues = { 0, 1, -1, Integer.MIN_VALUE, Integer.MAX_VALUE, 127, -128, 65535, -65535,
      1000000, -1000000 };

    for (int value : testValues) {
      heap.clear();
      direct.clear();

      heap.position(0);
      ByteBufferUtils.putInt(heap, value);
      assertEquals(4, heap.position());
      ByteBufferUtils.putInt(heap, value + 1);
      assertEquals(8, heap.position());

      heap.position(0);
      assertEquals(value, heap.getInt());
      assertEquals(value + 1, heap.getInt());

      direct.position(0);
      ByteBufferUtils.putInt(direct, value);
      assertEquals(4, direct.position());
      ByteBufferUtils.putInt(direct, value + 1);
      assertEquals(8, direct.position());

      direct.position(0);
      assertEquals(value, direct.getInt());
      assertEquals(value + 1, direct.getInt());
    }
  }

  @Test
  public void testConverterPutIntWithIndex() {
    ByteBuffer heap = ByteBuffer.allocate(20);
    ByteBuffer direct = ByteBuffer.allocateDirect(20);

    int[] testValues = { 0, 1, -1, Integer.MIN_VALUE, Integer.MAX_VALUE, 127, -128, 65535, -65535,
      1000000, -1000000 };

    for (int value : testValues) {
      heap.clear();
      direct.clear();

      heap.position(5);
      int newIndex = ByteBufferUtils.putInt(heap, 0, value);
      assertEquals(0 + Integer.BYTES, newIndex);
      assertEquals(5, heap.position());
      assertEquals(value, heap.getInt(5));

      direct.position(5);
      newIndex = ByteBufferUtils.putInt(direct, 0, value);
      assertEquals(0 + Integer.BYTES, newIndex);
      assertEquals(5, direct.position());
      assertEquals(value, direct.getInt(5));
    }
  }

  @Test
  public void testConverterPutLongWithPosition() {
    ByteBuffer heap = ByteBuffer.allocate(32);
    ByteBuffer direct = ByteBuffer.allocateDirect(32);

    long[] testValues = { 0L, 1L, -1L, Long.MIN_VALUE, Long.MAX_VALUE, 127L, -128L, 65535L, -65535L,
      Integer.MAX_VALUE + 1L, Integer.MIN_VALUE - 1L, 1000000000000L, -1000000000000L };

    for (long value : testValues) {
      heap.clear();
      direct.clear();

      heap.position(0);
      ByteBufferUtils.putLong(heap, value);
      assertEquals(8, heap.position());
      ByteBufferUtils.putLong(heap, value + 1);
      assertEquals(16, heap.position());

      heap.position(0);
      assertEquals(value, heap.getLong());
      assertEquals(value + 1, heap.getLong());

      direct.position(0);
      ByteBufferUtils.putLong(direct, value);
      assertEquals(8, direct.position());
      ByteBufferUtils.putLong(direct, value + 1);
      assertEquals(16, direct.position());

      direct.position(0);
      assertEquals(value, direct.getLong());
      assertEquals(value + 1, direct.getLong());
    }
  }

  @Test
  public void testConverterPutLongWithIndex() {
    ByteBuffer heap = ByteBuffer.allocate(32);
    ByteBuffer direct = ByteBuffer.allocateDirect(32);

    long[] testValues = { 0L, 1L, -1L, Long.MIN_VALUE, Long.MAX_VALUE, 127L, -128L, 65535L, -65535L,
      Integer.MAX_VALUE + 1L, Integer.MIN_VALUE - 1L, 1000000000000L, -1000000000000L };

    for (long value : testValues) {
      heap.clear();
      direct.clear();

      heap.position(8);
      int newIndex = ByteBufferUtils.putLong(heap, 0, value);
      assertEquals(0 + Long.BYTES, newIndex);
      assertEquals(8, heap.position());
      assertEquals(value, heap.getLong(8));

      direct.position(8);
      newIndex = ByteBufferUtils.putLong(direct, 0, value);
      assertEquals(0 + Long.BYTES, newIndex);
      assertEquals(8, direct.position());
      assertEquals(value, direct.getLong(8));
    }
  }

  @Test
  public void testConverterRoundTripShort() {
    ByteBuffer heap = ByteBuffer.allocate(20);
    ByteBuffer direct = ByteBuffer.allocateDirect(20);

    short[] testValues =
      { 0, 1, -1, Short.MIN_VALUE, Short.MAX_VALUE, 127, -128, 255, -255, 1000, -1000 };

    for (short value : testValues) {
      heap.clear();
      direct.clear();

      heap.position(5);
      ByteBufferUtils.putShort(heap, value);
      assertEquals(value, ByteBufferUtils.toShort(heap, 5));

      direct.position(5);
      ByteBufferUtils.putShort(direct, value);
      assertEquals(value, ByteBufferUtils.toShort(direct, 5));
    }
  }

  @Test
  public void testConverterRoundTripInt() {
    ByteBuffer heap = ByteBuffer.allocate(20);
    ByteBuffer direct = ByteBuffer.allocateDirect(20);

    int[] testValues = { 0, 1, -1, Integer.MIN_VALUE, Integer.MAX_VALUE, 127, -128, 65535, -65535,
      1000000, -1000000 };

    for (int value : testValues) {
      heap.clear();
      direct.clear();

      heap.position(5);
      ByteBufferUtils.putInt(heap, value);
      assertEquals(value, ByteBufferUtils.toInt(heap, 5));
      heap.position(5);
      assertEquals(value, ByteBufferUtils.toInt(heap));

      direct.position(5);
      ByteBufferUtils.putInt(direct, value);
      assertEquals(value, ByteBufferUtils.toInt(direct, 5));
      direct.position(5);
      assertEquals(value, ByteBufferUtils.toInt(direct));
    }
  }

  @Test
  public void testConverterRoundTripLong() {
    ByteBuffer heap = ByteBuffer.allocate(32);
    ByteBuffer direct = ByteBuffer.allocateDirect(32);

    long[] testValues = { 0L, 1L, -1L, Long.MIN_VALUE, Long.MAX_VALUE, 127L, -128L, 65535L, -65535L,
      Integer.MAX_VALUE + 1L, Integer.MIN_VALUE - 1L, 1000000000000L, -1000000000000L };

    for (long value : testValues) {
      heap.clear();
      direct.clear();

      heap.position(8);
      ByteBufferUtils.putLong(heap, value);
      assertEquals(value, ByteBufferUtils.toLong(heap, 8));

      direct.position(8);
      ByteBufferUtils.putLong(direct, value);
      assertEquals(value, ByteBufferUtils.toLong(direct, 8));
    }
  }

  @Test
  public void testConverterMultipleOperations() {
    ByteBuffer heap = ByteBuffer.allocate(50);
    ByteBuffer direct = ByteBuffer.allocateDirect(50);

    for (ByteBuffer buffer : Arrays.asList(heap, direct)) {
      buffer.clear();
      buffer.position(0);

      ByteBufferUtils.putShort(buffer, (short) 100);
      ByteBufferUtils.putInt(buffer, 200000);
      ByteBufferUtils.putLong(buffer, 3000000000L);
      ByteBufferUtils.putShort(buffer, Short.MIN_VALUE);
      ByteBufferUtils.putInt(buffer, Integer.MAX_VALUE);
      ByteBufferUtils.putLong(buffer, Long.MIN_VALUE);

      assertEquals(28, buffer.position());

      assertEquals((short) 100, ByteBufferUtils.toShort(buffer, 0));
      assertEquals(200000, ByteBufferUtils.toInt(buffer, 2));
      assertEquals(3000000000L, ByteBufferUtils.toLong(buffer, 6));
      assertEquals(Short.MIN_VALUE, ByteBufferUtils.toShort(buffer, 14));
      assertEquals(Integer.MAX_VALUE, ByteBufferUtils.toInt(buffer, 16));
      assertEquals(Long.MIN_VALUE, ByteBufferUtils.toLong(buffer, 20));

      assertEquals((short) 100, ByteBufferUtils.toShort(buffer, 0));
      buffer.position(2);
      assertEquals(200000, ByteBufferUtils.toInt(buffer));
      assertEquals(3000000000L, ByteBufferUtils.toLong(buffer, 6));
    }
  }

  @Test
  public void testConverterBigEndianByteOrder() {
    ByteBuffer heap = ByteBuffer.allocate(20);
    ByteBuffer direct = ByteBuffer.allocateDirect(20);

    for (ByteBuffer buffer : Arrays.asList(heap, direct)) {
      buffer.clear();

      buffer.position(0);
      ByteBufferUtils.putInt(buffer, 0x01020304);
      assertEquals((byte) 0x01, buffer.get(0));
      assertEquals((byte) 0x02, buffer.get(1));
      assertEquals((byte) 0x03, buffer.get(2));
      assertEquals((byte) 0x04, buffer.get(3));

      buffer.position(0);
      ByteBufferUtils.putShort(buffer, (short) 0x0102);
      assertEquals((byte) 0x01, buffer.get(0));
      assertEquals((byte) 0x02, buffer.get(1));

      buffer.position(0);
      ByteBufferUtils.putLong(buffer, 0x0102030405060708L);
      assertEquals((byte) 0x01, buffer.get(0));
      assertEquals((byte) 0x02, buffer.get(1));
      assertEquals((byte) 0x03, buffer.get(2));
      assertEquals((byte) 0x04, buffer.get(3));
      assertEquals((byte) 0x05, buffer.get(4));
      assertEquals((byte) 0x06, buffer.get(5));
      assertEquals((byte) 0x07, buffer.get(6));
      assertEquals((byte) 0x08, buffer.get(7));
    }
  }

  @Test
  public void testCommonPrefixerIdenticalSequences() {
    ByteBuffer heap = ByteBuffer.allocate(100);
    ByteBuffer direct = ByteBuffer.allocateDirect(100);
    byte[] array = new byte[100];

    for (int i = 0; i < 100; i++) {
      heap.put(i, (byte) i);
      direct.put(i, (byte) i);
      array[i] = (byte) i;
    }

    assertEquals(100, ByteBufferUtils.findCommonPrefix(heap, 0, 100, heap, 0, 100));
    assertEquals(100, ByteBufferUtils.findCommonPrefix(direct, 0, 100, direct, 0, 100));
    assertEquals(100, ByteBufferUtils.findCommonPrefix(heap, 0, 100, direct, 0, 100));
    assertEquals(100, ByteBufferUtils.findCommonPrefix(heap, 0, 100, array, 0, 100));
    assertEquals(100, ByteBufferUtils.findCommonPrefix(direct, 0, 100, array, 0, 100));
  }

  @Test
  public void testCommonPrefixerEmptySequences() {
    ByteBuffer heap = ByteBuffer.allocate(10);
    ByteBuffer direct = ByteBuffer.allocateDirect(10);
    byte[] array = new byte[10];

    assertEquals(0, ByteBufferUtils.findCommonPrefix(heap, 0, 0, heap, 0, 0));
    assertEquals(0, ByteBufferUtils.findCommonPrefix(direct, 0, 0, direct, 0, 0));
    assertEquals(0, ByteBufferUtils.findCommonPrefix(heap, 0, 0, array, 0, 0));
    assertEquals(0, ByteBufferUtils.findCommonPrefix(direct, 0, 0, array, 0, 0));
  }

  @Test
  public void testCommonPrefixerDifferentLengths() {
    ByteBuffer heap = ByteBuffer.allocate(100);
    ByteBuffer direct = ByteBuffer.allocateDirect(100);
    byte[] shortArray = new byte[50];
    byte[] longArray = new byte[100];

    for (int i = 0; i < 100; i++) {
      byte value = (byte) i;
      heap.put(i, value);
      direct.put(i, value);
      if (i < 50) {
        shortArray[i] = value;
      }
      longArray[i] = value;
    }

    assertEquals(50, ByteBufferUtils.findCommonPrefix(heap, 0, 100, shortArray, 0, 50));
    assertEquals(50, ByteBufferUtils.findCommonPrefix(direct, 0, 100, shortArray, 0, 50));
    assertEquals(50, ByteBufferUtils.findCommonPrefix(heap, 0, 50, longArray, 0, 100));
    assertEquals(50, ByteBufferUtils.findCommonPrefix(direct, 0, 50, longArray, 0, 100));
  }

  @Test
  public void testCommonPrefixerDifferenceAtStrideBoundaries() {
    ByteBuffer heap1 = ByteBuffer.allocate(100);
    ByteBuffer heap2 = ByteBuffer.allocate(100);
    ByteBuffer direct1 = ByteBuffer.allocateDirect(100);
    ByteBuffer direct2 = ByteBuffer.allocateDirect(100);

    int[] boundaryPositions = { 0, 1, 7, 8, 9, 15, 16, 17, 23, 24, 31, 32 };

    for (int diffPos : boundaryPositions) {
      fillBB(heap1, (byte) 5);
      fillBB(heap2, (byte) 5);
      fillBB(direct1, (byte) 5);
      fillBB(direct2, (byte) 5);

      heap2.put(diffPos, (byte) 99);
      direct2.put(diffPos, (byte) 99);

      assertEquals(diffPos, ByteBufferUtils.findCommonPrefix(heap1, 0, 100, heap2, 0, 100));
      assertEquals(diffPos, ByteBufferUtils.findCommonPrefix(direct1, 0, 100, direct2, 0, 100));
      assertEquals(diffPos, ByteBufferUtils.findCommonPrefix(heap1, 0, 100, direct2, 0, 100));
    }
  }

  @Test
  public void testCommonPrefixerDifferenceAtStrideBoundariesWithArray() {
    ByteBuffer heap = ByteBuffer.allocate(100);
    ByteBuffer direct = ByteBuffer.allocateDirect(100);
    byte[] array = new byte[100];

    int[] boundaryPositions = { 0, 1, 7, 8, 9, 15, 16, 17, 23, 24, 31, 32 };

    for (int diffPos : boundaryPositions) {
      fillBB(heap, (byte) 5);
      fillBB(direct, (byte) 5);
      fillArray(array, (byte) 5);

      array[diffPos] = (byte) 99;

      assertEquals(diffPos, ByteBufferUtils.findCommonPrefix(heap, 0, 100, array, 0, 100));
      assertEquals(diffPos, ByteBufferUtils.findCommonPrefix(direct, 0, 100, array, 0, 100));
    }
  }

  @Test
  public void testCommonPrefixerWithOffsets() {
    ByteBuffer heap = ByteBuffer.allocate(120);
    ByteBuffer direct = ByteBuffer.allocateDirect(120);
    byte[] array = new byte[120];

    for (int i = 0; i < 120; i++) {
      heap.put(i, (byte) 42);
      direct.put(i, (byte) 42);
      array[i] = (byte) 42;
    }

    heap.put(70, (byte) 99);
    direct.put(70, (byte) 99);
    array[70] = (byte) 99;

    assertEquals(50, ByteBufferUtils.findCommonPrefix(heap, 10, 60, heap, 20, 60));
    assertEquals(50, ByteBufferUtils.findCommonPrefix(direct, 10, 60, direct, 20, 60));
    assertEquals(50, ByteBufferUtils.findCommonPrefix(heap, 10, 60, array, 20, 60));
    assertEquals(50, ByteBufferUtils.findCommonPrefix(direct, 10, 60, array, 20, 60));
  }

  @Test
  public void testCommonPrefixerSingleByteDifferences() {
    ByteBuffer heap1 = ByteBuffer.allocate(50);
    ByteBuffer heap2 = ByteBuffer.allocate(50);
    ByteBuffer direct1 = ByteBuffer.allocateDirect(50);
    ByteBuffer direct2 = ByteBuffer.allocateDirect(50);
    byte[] array = new byte[50];

    for (int diffPos = 0; diffPos < 50; diffPos++) {
      fillBB(heap1, (byte) 10);
      fillBB(heap2, (byte) 10);
      fillBB(direct1, (byte) 10);
      fillBB(direct2, (byte) 10);
      fillArray(array, (byte) 10);

      heap2.put(diffPos, (byte) 20);
      direct2.put(diffPos, (byte) 20);
      array[diffPos] = (byte) 20;

      assertEquals(diffPos, ByteBufferUtils.findCommonPrefix(heap1, 0, 50, heap2, 0, 50));
      assertEquals(diffPos, ByteBufferUtils.findCommonPrefix(direct1, 0, 50, direct2, 0, 50));
      assertEquals(diffPos, ByteBufferUtils.findCommonPrefix(heap1, 0, 50, direct2, 0, 50));
      assertEquals(diffPos, ByteBufferUtils.findCommonPrefix(heap1, 0, 50, array, 0, 50));
      assertEquals(diffPos, ByteBufferUtils.findCommonPrefix(direct1, 0, 50, array, 0, 50));
    }
  }

  @Test
  public void testCommonPrefixerEpilogueBytes() {
    for (int length = 1; length <= 15; length++) {
      ByteBuffer heap1 = ByteBuffer.allocate(length);
      ByteBuffer heap2 = ByteBuffer.allocate(length);
      ByteBuffer direct1 = ByteBuffer.allocateDirect(length);
      ByteBuffer direct2 = ByteBuffer.allocateDirect(length);
      byte[] array = new byte[length];

      for (int i = 0; i < length; i++) {
        heap1.put(i, (byte) i);
        heap2.put(i, (byte) i);
        direct1.put(i, (byte) i);
        direct2.put(i, (byte) i);
        array[i] = (byte) i;
      }

      assertEquals(length, ByteBufferUtils.findCommonPrefix(heap1, 0, length, heap2, 0, length));
      assertEquals(length,
        ByteBufferUtils.findCommonPrefix(direct1, 0, length, direct2, 0, length));
      assertEquals(length, ByteBufferUtils.findCommonPrefix(heap1, 0, length, array, 0, length));
      assertEquals(length, ByteBufferUtils.findCommonPrefix(direct1, 0, length, array, 0, length));
    }
  }

  @Test
  public void testCommonPrefixerEpilogueDifference() {
    for (int length = 9; length <= 15; length++) {
      for (int diffPos = 8; diffPos < length; diffPos++) {
        ByteBuffer heap1 = ByteBuffer.allocate(length);
        ByteBuffer heap2 = ByteBuffer.allocate(length);
        ByteBuffer direct1 = ByteBuffer.allocateDirect(length);
        ByteBuffer direct2 = ByteBuffer.allocateDirect(length);
        byte[] array = new byte[length];

        for (int i = 0; i < length; i++) {
          heap1.put(i, (byte) 5);
          heap2.put(i, (byte) 5);
          direct1.put(i, (byte) 5);
          direct2.put(i, (byte) 5);
          array[i] = (byte) 5;
        }

        heap2.put(diffPos, (byte) 99);
        direct2.put(diffPos, (byte) 99);
        array[diffPos] = (byte) 99;

        assertEquals(diffPos, ByteBufferUtils.findCommonPrefix(heap1, 0, length, heap2, 0, length));
        assertEquals(diffPos,
          ByteBufferUtils.findCommonPrefix(direct1, 0, length, direct2, 0, length));
        assertEquals(diffPos, ByteBufferUtils.findCommonPrefix(heap1, 0, length, array, 0, length));
        assertEquals(diffPos,
          ByteBufferUtils.findCommonPrefix(direct1, 0, length, array, 0, length));
      }
    }
  }

  @Test
  public void testCommonPrefixerExtremeValues() {
    ByteBuffer heap1 = ByteBuffer.allocate(50);
    ByteBuffer heap2 = ByteBuffer.allocate(50);
    ByteBuffer direct1 = ByteBuffer.allocateDirect(50);
    ByteBuffer direct2 = ByteBuffer.allocateDirect(50);
    byte[] array = new byte[50];

    byte[] extremeValues = { Byte.MIN_VALUE, Byte.MAX_VALUE, 0, -1, 1, 127, -128, (byte) 0xFF };

    for (byte value : extremeValues) {
      fillBB(heap1, value);
      fillBB(heap2, value);
      fillBB(direct1, value);
      fillBB(direct2, value);
      fillArray(array, value);

      assertEquals(50, ByteBufferUtils.findCommonPrefix(heap1, 0, 50, heap2, 0, 50));
      assertEquals(50, ByteBufferUtils.findCommonPrefix(direct1, 0, 50, direct2, 0, 50));
      assertEquals(50, ByteBufferUtils.findCommonPrefix(heap1, 0, 50, direct2, 0, 50));
      assertEquals(50, ByteBufferUtils.findCommonPrefix(heap1, 0, 50, array, 0, 50));
      assertEquals(50, ByteBufferUtils.findCommonPrefix(direct1, 0, 50, array, 0, 50));
    }
  }

  @Test
  public void testCommonPrefixerMultipleStrides() {
    ByteBuffer heap1 = ByteBuffer.allocate(100);
    ByteBuffer heap2 = ByteBuffer.allocate(100);
    ByteBuffer direct1 = ByteBuffer.allocateDirect(100);
    ByteBuffer direct2 = ByteBuffer.allocateDirect(100);
    byte[] array = new byte[100];

    for (int i = 0; i < 100; i++) {
      heap1.put(i, (byte) (i % 10));
      heap2.put(i, (byte) (i % 10));
      direct1.put(i, (byte) (i % 10));
      direct2.put(i, (byte) (i % 10));
      array[i] = (byte) (i % 10);
    }

    int[] diffPositions = { 72, 73, 74, 75, 76, 77, 78, 79, 80 };

    for (int diffPos : diffPositions) {
      heap2.put(diffPos, (byte) 99);
      direct2.put(diffPos, (byte) 99);
      array[diffPos] = (byte) 99;

      assertEquals(diffPos, ByteBufferUtils.findCommonPrefix(heap1, 0, 100, heap2, 0, 100));
      assertEquals(diffPos, ByteBufferUtils.findCommonPrefix(direct1, 0, 100, direct2, 0, 100));
      assertEquals(diffPos, ByteBufferUtils.findCommonPrefix(heap1, 0, 100, array, 0, 100));
      assertEquals(diffPos, ByteBufferUtils.findCommonPrefix(direct1, 0, 100, array, 0, 100));

      heap2.put(diffPos, (byte) (diffPos % 10));
      direct2.put(diffPos, (byte) (diffPos % 10));
      array[diffPos] = (byte) (diffPos % 10);
    }
  }

  @Test
  public void testCommonPrefixerHeapVsDirectConsistency() {
    ByteBuffer heap1 = ByteBuffer.allocate(100);
    ByteBuffer heap2 = ByteBuffer.allocate(100);
    ByteBuffer direct1 = ByteBuffer.allocateDirect(100);
    ByteBuffer direct2 = ByteBuffer.allocateDirect(100);

    for (int i = 0; i < 100; i++) {
      byte value = (byte) (i * 3 % 256);
      heap1.put(i, value);
      heap2.put(i, value);
      direct1.put(i, value);
      direct2.put(i, value);
    }

    heap2.put(42, (byte) 0);
    direct2.put(42, (byte) 0);

    int heapResult = ByteBufferUtils.findCommonPrefix(heap1, 0, 100, heap2, 0, 100);
    int directResult = ByteBufferUtils.findCommonPrefix(direct1, 0, 100, direct2, 0, 100);
    int mixedResult1 = ByteBufferUtils.findCommonPrefix(heap1, 0, 100, direct2, 0, 100);
    int mixedResult2 = ByteBufferUtils.findCommonPrefix(direct1, 0, 100, heap2, 0, 100);

    assertEquals(heapResult, directResult);
    assertEquals(heapResult, mixedResult1);
    assertEquals(heapResult, mixedResult2);
    assertEquals(42, heapResult);
  }

  @Test
  public void testCommonPrefixerPartialMatches() {
    ByteBuffer heap = ByteBuffer.allocate(64);
    ByteBuffer direct = ByteBuffer.allocateDirect(64);
    byte[] array = new byte[64];

    for (int i = 0; i < 32; i++) {
      heap.put(i, (byte) i);
      direct.put(i, (byte) i);
      array[i] = (byte) i;
    }
    for (int i = 32; i < 64; i++) {
      heap.put(i, (byte) (i + 100));
      direct.put(i, (byte) (i + 100));
      array[i] = (byte) (i + 100);
    }

    ByteBuffer heap2 = ByteBuffer.allocate(64);
    ByteBuffer direct2 = ByteBuffer.allocateDirect(64);
    byte[] array2 = new byte[64];

    for (int i = 0; i < 32; i++) {
      heap2.put(i, (byte) i);
      direct2.put(i, (byte) i);
      array2[i] = (byte) i;
    }
    for (int i = 32; i < 64; i++) {
      heap2.put(i, (byte) (i + 200));
      direct2.put(i, (byte) (i + 200));
      array2[i] = (byte) (i + 200);
    }

    assertEquals(32, ByteBufferUtils.findCommonPrefix(heap, 0, 64, heap2, 0, 64));
    assertEquals(32, ByteBufferUtils.findCommonPrefix(direct, 0, 64, direct2, 0, 64));
    assertEquals(32, ByteBufferUtils.findCommonPrefix(heap, 0, 64, array2, 0, 64));
    assertEquals(32, ByteBufferUtils.findCommonPrefix(direct, 0, 64, array2, 0, 64));
  }

  // Below are utility methods invoked from test methods
  private static void testCompressedInt(int value) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ByteBufferUtils.putCompressedInt(bos, value);
    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    int parsedValue = ByteBufferUtils.readCompressedInt(bis);
    assertEquals(value, parsedValue);
  }

  private static void testPutInt(int value) {
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
