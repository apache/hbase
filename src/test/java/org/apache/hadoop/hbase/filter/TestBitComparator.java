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

import junit.framework.TestCase;
import org.apache.hadoop.hbase.SmallTests;
import org.junit.experimental.categories.Category;

/**
 * Tests for the bit comparator
 */
@Category(SmallTests.class)
public class TestBitComparator extends TestCase {

  private static byte[] zeros = new byte[]{0, 0, 0, 0, 0, 0};
  private static byte[] ones = new byte[]{1, 1, 1, 1, 1, 1};
  private static byte[] data0 = new byte[]{0, 1, 2, 4, 8, 15};
  private static byte[] data1 = new byte[]{15, 0, 0, 0, 0, 0};
  private static byte[] data2 = new byte[]{0, 0, 0, 0, 0, 15};
  private static byte[] data3 = new byte[]{15, 15, 15, 15, 15};
  
  // data for testing compareTo method with offset and length parameters
  private static byte[] data1_2 = new byte[]{15, 15, 0, 0, 0, 0, 0, 15};
  private static byte[] data2_2 = new byte[]{15, 0, 0, 0, 0, 0, 15, 15};
  
  private final int Equal = 0;
  private final int NotEqual = 1;

  public void testANDOperation() {
    testOperation(zeros, ones, BitComparator.BitwiseOp.AND, NotEqual);
    testOperation(data1, ones, BitComparator.BitwiseOp.AND, Equal);
    testOperation(data1, data0, BitComparator.BitwiseOp.AND, NotEqual);
    testOperation(data2, data1, BitComparator.BitwiseOp.AND, NotEqual);
    testOperation(ones, data0, BitComparator.BitwiseOp.AND, Equal);
    testOperation(ones, data3, BitComparator.BitwiseOp.AND, NotEqual);
  }

  public void testOROperation() {
    testOperation(ones, zeros, BitComparator.BitwiseOp.OR, Equal);
    testOperation(zeros, zeros, BitComparator.BitwiseOp.OR, NotEqual);
    testOperation(data1, zeros, BitComparator.BitwiseOp.OR, Equal);
    testOperation(data2, data1, BitComparator.BitwiseOp.OR, Equal);
    testOperation(ones, data3, BitComparator.BitwiseOp.OR, NotEqual);
  }

  public void testXOROperation() {
    testOperation(ones, zeros, BitComparator.BitwiseOp.XOR, Equal);
    testOperation(zeros, zeros, BitComparator.BitwiseOp.XOR, NotEqual);
    testOperation(ones, ones, BitComparator.BitwiseOp.XOR, NotEqual);
    testOperation(data2, data1, BitComparator.BitwiseOp.XOR, Equal);
    testOperation(ones, data3, BitComparator.BitwiseOp.XOR, NotEqual);
  }

  private void testOperation(byte[] data, byte[] comparatorBytes, BitComparator.BitwiseOp operator, int expected) {
    BitComparator comparator = new BitComparator(comparatorBytes, operator);
    assertEquals(comparator.compareTo(data), expected);
  }

  public void testANDOperationWithOffset() {
    testOperationWithOffset(data1_2, ones, BitComparator.BitwiseOp.AND, Equal);
    testOperationWithOffset(data1_2, data0, BitComparator.BitwiseOp.AND, NotEqual);
    testOperationWithOffset(data2_2, data1, BitComparator.BitwiseOp.AND, NotEqual);
  }

  public void testOROperationWithOffset() {
    testOperationWithOffset(data1_2, zeros, BitComparator.BitwiseOp.OR, Equal);
    testOperationWithOffset(data2_2, data1, BitComparator.BitwiseOp.OR, Equal);
  }

  public void testXOROperationWithOffset() {
    testOperationWithOffset(data2_2, data1, BitComparator.BitwiseOp.XOR, Equal);
  }

  private void testOperationWithOffset(byte[] data, byte[] comparatorBytes, BitComparator.BitwiseOp operator, int expected) {
    BitComparator comparator = new BitComparator(comparatorBytes, operator);
    assertEquals(comparator.compareTo(data, 1, comparatorBytes.length), expected);
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

