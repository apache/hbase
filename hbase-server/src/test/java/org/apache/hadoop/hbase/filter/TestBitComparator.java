/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.filter;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.testclassification.FilterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the bit comparator
 */
@Category({FilterTests.class, SmallTests.class})
public class TestBitComparator {

  private static byte[] zeros = new byte[]{0, 0, 0, 0, 0, 0};
  private static ByteBuffer zeros_bb = ByteBuffer.wrap(zeros);
  private static byte[] ones = new byte[]{1, 1, 1, 1, 1, 1};
  private static ByteBuffer ones_bb = ByteBuffer.wrap(ones);
  private static byte[] data0 = new byte[]{0, 1, 2, 4, 8, 15};
  private static byte[] data1 = new byte[]{15, 0, 0, 0, 0, 0};
  private static ByteBuffer data1_bb = ByteBuffer.wrap(data1);
  private static byte[] data2 = new byte[]{0, 0, 0, 0, 0, 15};
  private static ByteBuffer data2_bb = ByteBuffer.wrap(data2);
  private static byte[] data3 = new byte[]{15, 15, 15, 15, 15};
  
  // data for testing compareTo method with offset and length parameters
  private static byte[] data1_2 = new byte[]{15, 15, 0, 0, 0, 0, 0, 15};
  private static ByteBuffer data1_2_bb = ByteBuffer.wrap(data1_2);
  private static byte[] data2_2 = new byte[]{15, 0, 0, 0, 0, 0, 15, 15};
  private static ByteBuffer data2_2_bb = ByteBuffer.wrap(data2_2);
  
  private final int Equal = 0;
  private final int NotEqual = 1;

  @Test
  public void testANDOperation() {
    testOperation(zeros, ones, BitComparator.BitwiseOp.AND, NotEqual);
    testOperation(data1, ones, BitComparator.BitwiseOp.AND, Equal);
    testOperation(data1, data0, BitComparator.BitwiseOp.AND, NotEqual);
    testOperation(data2, data1, BitComparator.BitwiseOp.AND, NotEqual);
    testOperation(ones, data0, BitComparator.BitwiseOp.AND, Equal);
    testOperation(ones, data3, BitComparator.BitwiseOp.AND, NotEqual);

    testOperation(zeros_bb, ones, BitComparator.BitwiseOp.AND, NotEqual);
    testOperation(data1_bb, ones, BitComparator.BitwiseOp.AND, Equal);
    testOperation(data1_bb, data0, BitComparator.BitwiseOp.AND, NotEqual);
    testOperation(data2_bb, data1, BitComparator.BitwiseOp.AND, NotEqual);
    testOperation(ones_bb, data0, BitComparator.BitwiseOp.AND, Equal);
    testOperation(ones_bb, data3, BitComparator.BitwiseOp.AND, NotEqual);
  }

  @Test
  public void testOROperation() {
    testOperation(ones, zeros, BitComparator.BitwiseOp.OR, Equal);
    testOperation(zeros, zeros, BitComparator.BitwiseOp.OR, NotEqual);
    testOperation(data1, zeros, BitComparator.BitwiseOp.OR, Equal);
    testOperation(data2, data1, BitComparator.BitwiseOp.OR, Equal);
    testOperation(ones, data3, BitComparator.BitwiseOp.OR, NotEqual);

    testOperation(ones_bb, zeros, BitComparator.BitwiseOp.OR, Equal);
    testOperation(zeros_bb, zeros, BitComparator.BitwiseOp.OR, NotEqual);
    testOperation(data1_bb, zeros, BitComparator.BitwiseOp.OR, Equal);
    testOperation(data2_bb, data1, BitComparator.BitwiseOp.OR, Equal);
    testOperation(ones_bb, data3, BitComparator.BitwiseOp.OR, NotEqual);
  }

  @Test
  public void testXOROperation() {
    testOperation(ones, zeros, BitComparator.BitwiseOp.XOR, Equal);
    testOperation(zeros, zeros, BitComparator.BitwiseOp.XOR, NotEqual);
    testOperation(ones, ones, BitComparator.BitwiseOp.XOR, NotEqual);
    testOperation(data2, data1, BitComparator.BitwiseOp.XOR, Equal);
    testOperation(ones, data3, BitComparator.BitwiseOp.XOR, NotEqual);

    testOperation(ones_bb, zeros, BitComparator.BitwiseOp.XOR, Equal);
    testOperation(zeros_bb, zeros, BitComparator.BitwiseOp.XOR, NotEqual);
    testOperation(ones_bb, ones, BitComparator.BitwiseOp.XOR, NotEqual);
    testOperation(data2_bb, data1, BitComparator.BitwiseOp.XOR, Equal);
    testOperation(ones_bb, data3, BitComparator.BitwiseOp.XOR, NotEqual);
  }

  private void testOperation(byte[] data, byte[] comparatorBytes, BitComparator.BitwiseOp operator,
      int expected) {
    BitComparator comparator = new BitComparator(comparatorBytes, operator);
    assertEquals(comparator.compareTo(data), expected);
  }

  private void testOperation(ByteBuffer data, byte[] comparatorBytes,
      BitComparator.BitwiseOp operator, int expected) {
    BitComparator comparator = new BitComparator(comparatorBytes, operator);
    assertEquals(comparator.compareTo(data, 0, data.capacity()), expected);
  }

  @Test
  public void testANDOperationWithOffset() {
    testOperationWithOffset(data1_2, ones, BitComparator.BitwiseOp.AND, Equal);
    testOperationWithOffset(data1_2, data0, BitComparator.BitwiseOp.AND, NotEqual);
    testOperationWithOffset(data2_2, data1, BitComparator.BitwiseOp.AND, NotEqual);

    testOperationWithOffset(data1_2_bb, ones, BitComparator.BitwiseOp.AND, Equal);
    testOperationWithOffset(data1_2_bb, data0, BitComparator.BitwiseOp.AND, NotEqual);
    testOperationWithOffset(data2_2_bb, data1, BitComparator.BitwiseOp.AND, NotEqual);
  }

  @Test
  public void testOROperationWithOffset() {
    testOperationWithOffset(data1_2, zeros, BitComparator.BitwiseOp.OR, Equal);
    testOperationWithOffset(data2_2, data1, BitComparator.BitwiseOp.OR, Equal);

    testOperationWithOffset(data1_2_bb, zeros, BitComparator.BitwiseOp.OR, Equal);
    testOperationWithOffset(data2_2_bb, data1, BitComparator.BitwiseOp.OR, Equal);
  }

  @Test
  public void testXOROperationWithOffset() {
    testOperationWithOffset(data2_2, data1, BitComparator.BitwiseOp.XOR, Equal);

    testOperationWithOffset(data2_2_bb, data1, BitComparator.BitwiseOp.XOR, Equal);
  }

  private void testOperationWithOffset(byte[] data, byte[] comparatorBytes,
      BitComparator.BitwiseOp operator, int expected) {
    BitComparator comparator = new BitComparator(comparatorBytes, operator);
    assertEquals(comparator.compareTo(data, 1, comparatorBytes.length), expected);
  }

  private void testOperationWithOffset(ByteBuffer data, byte[] comparatorBytes,
      BitComparator.BitwiseOp operator, int expected) {
    BitComparator comparator = new BitComparator(comparatorBytes, operator);
    assertEquals(comparator.compareTo(data, 1, comparatorBytes.length), expected);
  }
}

