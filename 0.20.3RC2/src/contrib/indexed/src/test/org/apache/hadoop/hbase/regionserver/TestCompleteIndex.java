/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.regionserver;

import junit.framework.Assert;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.idx.IdxColumnDescriptor;
import org.apache.hadoop.hbase.client.idx.IdxIndexDescriptor;
import org.apache.hadoop.hbase.client.idx.IdxQualifierType;
import org.apache.hadoop.hbase.regionserver.idx.support.arrays.IntegerArrayList;
import org.apache.hadoop.hbase.regionserver.idx.support.sets.IntSet;
import org.apache.hadoop.hbase.regionserver.idx.support.sets.TestBitSet;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

/**
 * Tests the complete index implemenation.
 */
public class TestCompleteIndex extends HBaseTestCase {
  private static final int NUM_KEY_VALUES = 1017;
  private static final byte[] FAMILY = Bytes.toBytes("family");
  private static final byte[] QUALIFIER = Bytes.toBytes("qualifier");
  private static final byte[] ROW = Bytes.toBytes("row");


  /**
   * Test that the index constructed correctly.
   */
  public void testConstruction() {
    IdxColumnDescriptor columnDescriptor = new IdxColumnDescriptor(FAMILY);
    byte[] value;
    int id;
    CompleteIndexBuilder bldr;

    /**
     * Test long type.
     */
    value = Bytes.toBytes(98L);
    id = 35;
    bldr = new CompleteIndexBuilder(columnDescriptor,
      new IdxIndexDescriptor(QUALIFIER, IdxQualifierType.LONG));
    bldr.addKeyValue(new KeyValue(ROW, FAMILY, QUALIFIER, value), id);
    Assert.assertTrue(
      bldr.finalizeIndex(NUM_KEY_VALUES).lookup(value).contains(id));

    /**
     * Test double type
     */
    value = Bytes.toBytes(9.8D);
    id = 899;
    bldr = new CompleteIndexBuilder(columnDescriptor,
      new IdxIndexDescriptor(QUALIFIER, IdxQualifierType.DOUBLE));
    bldr.addKeyValue(new KeyValue(ROW, FAMILY, QUALIFIER, value), id);
    Assert.assertTrue(
      bldr.finalizeIndex(NUM_KEY_VALUES).lookup(value).contains(id));

    /**
     * Test the byte array type
     */
    value = Bytes.toBytes(this.getClass().getName());
    id = 1016;
    bldr = new CompleteIndexBuilder(columnDescriptor,
      new IdxIndexDescriptor(QUALIFIER, IdxQualifierType.BYTE_ARRAY));
    bldr.addKeyValue(new KeyValue(ROW, FAMILY, QUALIFIER, value), id);
    Assert.assertTrue(
      bldr.finalizeIndex(NUM_KEY_VALUES).lookup(value).contains(id));

  }

  /**
   * Tests that the lookup method returns correct results.
   */
  public void testLookup() {
    CompleteIndex index;

    /**
     * All entries have different values
     */
    long[] values = {60, 80, 40, 50, 0, 20, 10, 70, 30, 90};
    int[] ids = {7, 80, 81, 200, 235, 490, 601, 698, 888, 965};
    index = fillIndex(values, ids);
    for (int i = 0; i < values.length; i++) {
      IntSet lookupResult = index.lookup(Bytes.toBytes(values[i]));
      Assert.assertEquals(1, lookupResult.size());
      Assert.assertEquals(true, lookupResult.contains(ids[i]));
    }
    Assert.assertTrue(index.lookup(Bytes.toBytes(35L)).isEmpty());

    /**
     * All the entries have the samae value
     */
    values = new long[]{100, 100, 100, 100, 100, 100, 100, 100, 100, 100};
    index = fillIndex(values, ids);
    IntSet lookupResult = index.lookup(Bytes.toBytes(100L));
    Assert.assertEquals(10, lookupResult.size());
    for (int id : ids) {
      Assert.assertEquals(true, lookupResult.contains(id));
    }
    Assert.assertTrue(index.lookup(Bytes.toBytes(35L)).isEmpty());

    /**
     * Some of the entries have one value on the others have another value
     */
    values = new long[]{100, 99, 50, 99, 100, 50, 100, 100, 99, 99};
    index = fillIndex(values, ids);
    lookupResult = index.lookup(Bytes.toBytes(100L));
    Assert.assertEquals(4, lookupResult.size());
    for (int id : new int[]{7, 235, 601, 698}) {
      Assert.assertEquals(true, lookupResult.contains(id));
    }

    lookupResult = index.lookup(Bytes.toBytes(99L));
    Assert.assertEquals(4, lookupResult.size());
    for (int id : new int[]{80, 200, 888, 965}) {
      Assert.assertEquals(true, lookupResult.contains(id));
    }

    lookupResult = index.lookup(Bytes.toBytes(50L));
    Assert.assertEquals(2, lookupResult.size());
    for (int id : new int[]{81, 490}) {
      Assert.assertEquals(true, lookupResult.contains(id));
    }
    Assert.assertTrue(index.lookup(Bytes.toBytes(35L)).isEmpty());
  }


  /**
   * Tests that the lookup method returns correct results.
   */
  public void testTail() {
    CompleteIndex index;

    /**
     * All entries have different values
     */
    long[] values = {60, 80, 40, 50, 0, 20, 10, 70, 30, 90};
    int[] ids = {7, 80, 81, 200, 235, 490, 601, 698, 888, 965};
    index = fillIndex(values, ids);
    for (long size = 10, i = 0; i < 100; i += 10, size--) {
      Assert.assertEquals(size, index.tail(Bytes.toBytes(i), true).size());
      Assert.assertEquals(size - 1, index.tail(Bytes.toBytes(i), false).size());
    }

    for (long size = 10, i = -5; i < 110; i += 10, size--) {
      Assert.assertEquals(Math.max(size, 0),
        index.tail(Bytes.toBytes(i), true).size());
      Assert.assertEquals(Math.max(size, 0),
        index.tail(Bytes.toBytes(i), false).size());
    }

    // check explicit value setting
    Assert.assertEquals(index.tail(Bytes.toBytes(50L), true),
      makeSet(7, 80, 200, 698, 965));
    Assert.assertEquals(index.tail(Bytes.toBytes(50L), false),
      makeSet(7, 80, 698, 965));

    Assert.assertEquals(index.tail(Bytes.toBytes(45L), true),
      makeSet(7, 80, 200, 698, 965));
    Assert.assertEquals(index.tail(Bytes.toBytes(45L), false),
      makeSet(7, 80, 200, 698, 965));

    /**
     * All the entries have the samae value
     */
    values = new long[]{100, 100, 100, 100, 100, 100, 100, 100, 100, 100};
    index = fillIndex(values, ids);
    Assert.assertEquals(10, index.tail(Bytes.toBytes(100L), true).size());
    Assert.assertEquals(0, index.tail(Bytes.toBytes(100L), false).size());
    Assert.assertEquals(10, index.tail(Bytes.toBytes(99L), false).size());
    Assert.assertEquals(10, index.tail(Bytes.toBytes(99L), true).size());
    Assert.assertEquals(0, index.tail(Bytes.toBytes(101L), false).size());
    Assert.assertEquals(0, index.tail(Bytes.toBytes(101L), true).size());

    /**
     * Some of the entries have one value on the others have another value
     */
    values = new long[]{100, 99, 50, 99, 100, 50, 100, 100, 99, 99};
    index = fillIndex(values, ids);
    Assert.assertEquals(0, index.tail(Bytes.toBytes(101L), true).size());
    Assert.assertEquals(0, index.tail(Bytes.toBytes(101L), false).size());
    Assert.assertEquals(4, index.tail(Bytes.toBytes(100L), true).size());
    Assert.assertEquals(0, index.tail(Bytes.toBytes(100L), false).size());
    Assert.assertEquals(8, index.tail(Bytes.toBytes(99L), true).size());
    Assert.assertEquals(4, index.tail(Bytes.toBytes(99L), false).size());
    Assert.assertEquals(8, index.tail(Bytes.toBytes(98L), true).size());
    Assert.assertEquals(8, index.tail(Bytes.toBytes(98L), false).size());
    Assert.assertEquals(10, index.tail(Bytes.toBytes(50L), true).size());
    Assert.assertEquals(8, index.tail(Bytes.toBytes(50L), false).size());
    Assert.assertEquals(10, index.tail(Bytes.toBytes(49L), true).size());
    Assert.assertEquals(10, index.tail(Bytes.toBytes(49L), false).size());
  }

  /**
   * Tests that the lookup method returns correct results.
   */
  public void testHead() {
    CompleteIndex index;

    /**
     * All entries have different values
     */
    long[] values = {60, 80, 40, 50, 0, 20, 10, 70, 30, 90};
    int[] ids = {7, 80, 81, 200, 235, 490, 601, 698, 888, 965};
    index = fillIndex(values, ids);
    for (long size = 0, i = 0; i < 100; i += 10, size++) {
      Assert.assertEquals(size + 1, index.head(Bytes.toBytes(i), true).size());
      Assert.assertEquals(size, index.head(Bytes.toBytes(i), false).size());
    }

    for (long size = 0, i = -5; i < 110; i += 10, size++) {
      Assert.assertEquals(Math.min(size, 10),
        index.head(Bytes.toBytes(i), true).size());
      Assert.assertEquals(Math.min(size, 10),
        index.head(Bytes.toBytes(i), false).size());
    }

    // check explicit value setting
    Assert.assertEquals(index.head(Bytes.toBytes(50L), true),
      makeSet(81, 200, 235, 490, 601, 888));
    Assert.assertEquals(index.head(Bytes.toBytes(50L), false),
      makeSet(81, 235, 490, 601, 888));

    Assert.assertEquals(index.head(Bytes.toBytes(45L), true),
      makeSet(81, 235, 490, 601, 888));
    Assert.assertEquals(index.head(Bytes.toBytes(45L), false),
      makeSet(81, 235, 490, 601, 888));

    /**
     * All the entries have the samae value
     */
    values = new long[]{100, 100, 100, 100, 100, 100, 100, 100, 100, 100};
    index = fillIndex(values, ids);
    Assert.assertEquals(10, index.head(Bytes.toBytes(100L), true).size());
    Assert.assertEquals(0, index.head(Bytes.toBytes(100L), false).size());
    Assert.assertEquals(0, index.head(Bytes.toBytes(99L), false).size());
    Assert.assertEquals(0, index.head(Bytes.toBytes(99L), true).size());
    Assert.assertEquals(10, index.head(Bytes.toBytes(101L), false).size());
    Assert.assertEquals(10, index.head(Bytes.toBytes(101L), true).size());

    /**
     * Some of the entries have one value on the others have another value
     */
    values = new long[]{100, 99, 50, 99, 100, 50, 100, 100, 99, 99};
    index = fillIndex(values, ids);
    Assert.assertEquals(10, index.head(Bytes.toBytes(101L), true).size());
    Assert.assertEquals(10, index.head(Bytes.toBytes(101L), false).size());
    Assert.assertEquals(10, index.head(Bytes.toBytes(100L), true).size());
    Assert.assertEquals(6, index.head(Bytes.toBytes(100L), false).size());
    Assert.assertEquals(6, index.head(Bytes.toBytes(99L), true).size());
    Assert.assertEquals(2, index.head(Bytes.toBytes(99L), false).size());
    Assert.assertEquals(2, index.head(Bytes.toBytes(98L), true).size());
    Assert.assertEquals(2, index.head(Bytes.toBytes(98L), false).size());
    Assert.assertEquals(2, index.head(Bytes.toBytes(50L), true).size());
    Assert.assertEquals(0, index.head(Bytes.toBytes(50L), false).size());
    Assert.assertEquals(0, index.head(Bytes.toBytes(49L), true).size());
    Assert.assertEquals(0, index.head(Bytes.toBytes(49L), false).size());
  }


  /**
   * Tests that the heap size estimate of the fixed parts matches the
   * FIXED SIZE constant.
   */
  public void testHeapSize() {
    assertEquals(ClassSize.estimateBase(CompleteIndex.class, false),
      CompleteIndex.FIXED_SIZE);
    assertEquals(ClassSize.estimateBase(EmptyIndex.class, false),
      new EmptyIndex(new IntegerArrayList(), 100).heapSize());
  }

  private IntSet makeSet(int... items) {
    return TestBitSet.createBitSet(NUM_KEY_VALUES, items);
  }

  private static CompleteIndex fillIndex(long[] values, int[] ids) {
    Assert.assertEquals(values.length, ids.length);
    HColumnDescriptor columnDescriptor = new HColumnDescriptor(FAMILY);
    CompleteIndexBuilder completeIndex =
      new CompleteIndexBuilder(columnDescriptor,
        new IdxIndexDescriptor(QUALIFIER, IdxQualifierType.LONG));
    for (int i = 0; i < values.length; i++) {
      completeIndex.addKeyValue(new KeyValue(Bytes.toBytes(ids[i]), FAMILY,
        QUALIFIER, Bytes.toBytes(values[i])), ids[i]);
    }
    return (CompleteIndex) completeIndex.finalizeIndex(NUM_KEY_VALUES);
  }

}
