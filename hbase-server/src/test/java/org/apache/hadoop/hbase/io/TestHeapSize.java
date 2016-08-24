/*
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

package org.apache.hadoop.hbase.io;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.LruBlockCache;
import org.apache.hadoop.hbase.io.hfile.LruCachedBlock;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.ClassSize;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Testing the sizing that HeapSize offers and compares to the size given by
 * ClassSize.
 */
@Category({IOTests.class, SmallTests.class})
public class TestHeapSize  {
  private static final Log LOG = LogFactory.getLog(TestHeapSize.class);
  // List of classes implementing HeapSize
  // BatchOperation, BatchUpdate, BlockIndex, Entry, Entry<K,V>, HStoreKey
  // KeyValue, LruBlockCache, LruHashMap<K,V>, Put, WALKey

  @BeforeClass
  public static void beforeClass() throws Exception {
    // Print detail on jvm so we know what is different should below test fail.
    RuntimeMXBean b = ManagementFactory.getRuntimeMXBean();
    LOG.info("name=" + b.getName());
    LOG.info("specname=" + b.getSpecName());
    LOG.info("specvendor=" + b.getSpecVendor());
    LOG.info("vmname=" + b.getVmName());
    LOG.info("vmversion=" + b.getVmVersion());
    LOG.info("vmvendor=" + b.getVmVendor());
    Map<String, String> p = b.getSystemProperties();
    LOG.info("properties=" + p);
  }

  /**
   * Test our hard-coded sizing of native java objects
   */
  @Test
  public void testNativeSizes() throws IOException {
    Class<?> cl;
    long expected;
    long actual;

    // ArrayList
    cl = ArrayList.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.ARRAYLIST;
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // ByteBuffer
    cl = ByteBuffer.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.BYTE_BUFFER;
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // Integer
    cl = Integer.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.INTEGER;
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // Map.Entry
    // Interface is public, all others are not.  Hard to size via ClassSize
//    cl = Map.Entry.class;
//    expected = ClassSize.estimateBase(cl, false);
//    actual = ClassSize.MAP_ENTRY;
//    if(expected != actual) {
//      ClassSize.estimateBase(cl, true);
//      assertEquals(expected, actual);
//    }

    // Object
    cl = Object.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.align(ClassSize.OBJECT);
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // TreeMap
    cl = TreeMap.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.TREEMAP;
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // String
    cl = String.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.STRING;
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // ConcurrentHashMap
    cl = ConcurrentHashMap.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.CONCURRENT_HASHMAP;
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // ConcurrentSkipListMap
    cl = ConcurrentSkipListMap.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.CONCURRENT_SKIPLISTMAP;
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // CellArrayMap
    cl = CellArrayMap.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.CELL_ARRAY_MAP;
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // ReentrantReadWriteLock
    cl = ReentrantReadWriteLock.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.REENTRANT_LOCK;
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // AtomicLong
    cl = AtomicLong.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.ATOMIC_LONG;
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // AtomicInteger
    cl = AtomicInteger.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.ATOMIC_INTEGER;
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // AtomicBoolean
    cl = AtomicBoolean.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.ATOMIC_BOOLEAN;
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // CopyOnWriteArraySet
    cl = CopyOnWriteArraySet.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.COPYONWRITE_ARRAYSET;
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // CopyOnWriteArrayList
    cl = CopyOnWriteArrayList.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.COPYONWRITE_ARRAYLIST;
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // TimeRangeTracker
    cl = TimeRangeTracker.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.TIMERANGE_TRACKER;
    if (expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // CellSet
    cl = CellSet.class;
    expected = ClassSize.estimateBase(cl, false);
    actual = ClassSize.CELL_SET;
    if (expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }
  }

  /**
   * Testing the classes that implements HeapSize and are a part of 0.20.
   * Some are not tested here for example BlockIndex which is tested in
   * TestHFile since it is a non public class
   * @throws IOException
   */
  @Test
  public void testSizes() throws IOException {
    Class<?> cl;
    long expected;
    long actual;

    //KeyValue
    cl = KeyValue.class;
    expected = ClassSize.estimateBase(cl, false);
    KeyValue kv = new KeyValue();
    actual = kv.heapSize();
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    //LruBlockCache Overhead
    cl = LruBlockCache.class;
    actual = LruBlockCache.CACHE_FIXED_OVERHEAD;
    expected = ClassSize.estimateBase(cl, false);
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // CachedBlock Fixed Overhead
    // We really need "deep" sizing but ClassSize does not do this.
    // Perhaps we should do all these more in this style....
    cl = LruCachedBlock.class;
    actual = LruCachedBlock.PER_BLOCK_OVERHEAD;
    expected = ClassSize.estimateBase(cl, false);
    expected += ClassSize.estimateBase(String.class, false);
    expected += ClassSize.estimateBase(ByteBuffer.class, false);
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      ClassSize.estimateBase(String.class, true);
      ClassSize.estimateBase(ByteBuffer.class, true);
      assertEquals(expected, actual);
    }

    // DefaultMemStore Overhead
    cl = DefaultMemStore.class;
    actual = DefaultMemStore.FIXED_OVERHEAD;
    expected = ClassSize.estimateBase(cl, false);
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // DefaultMemStore Deep Overhead
    actual = DefaultMemStore.DEEP_OVERHEAD;
    expected = ClassSize.estimateBase(cl, false);
    expected += ClassSize.estimateBase(AtomicLong.class, false);
    expected += ClassSize.estimateBase(CellSet.class, false);
    expected += ClassSize.estimateBase(ConcurrentSkipListMap.class, false);
    expected += ClassSize.estimateBase(TimeRangeTracker.class, false);
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      ClassSize.estimateBase(AtomicLong.class, true);
      ClassSize.estimateBase(CellSet.class, true);
      ClassSize.estimateBase(ConcurrentSkipListMap.class, true);
      ClassSize.estimateBase(TimeRangeTracker.class, true);
      assertEquals(expected, actual);
    }

    // Store Overhead
    cl = HStore.class;
    actual = HStore.FIXED_OVERHEAD;
    expected = ClassSize.estimateBase(cl, false);
    if(expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // Region Overhead
    cl = HRegion.class;
    actual = HRegion.FIXED_OVERHEAD;
    expected = ClassSize.estimateBase(cl, false);
    if (expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // Block cache key overhead. Only tests fixed overhead as estimating heap
    // size of strings is hard.
    cl = BlockCacheKey.class;
    actual = BlockCacheKey.FIXED_OVERHEAD;
    expected  = ClassSize.estimateBase(cl, false);
    if (expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    // Currently NOT testing Deep Overheads of many of these classes.
    // Deep overheads cover a vast majority of stuff, but will not be 100%
    // accurate because it's unclear when we're referencing stuff that's already
    // accounted for.  But we have satisfied our two core requirements.
    // Sizing is quite accurate now, and our tests will throw errors if
    // any of these classes are modified without updating overhead sizes.
  }

  @Test
  public void testMutations(){
    Class<?> cl;
    long expected;
    long actual;

    cl = TimeRange.class;
    actual = ClassSize.TIMERANGE;
    expected  = ClassSize.estimateBase(cl, false);
    if (expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    byte[] row = new byte[] { 0 };
    cl = Put.class;
    actual = Mutation.MUTATION_OVERHEAD + ClassSize.align(ClassSize.ARRAY);
    expected = ClassSize.estimateBase(cl, false);
    //The actual TreeMap is not included in the above calculation
    expected += ClassSize.align(ClassSize.TREEMAP);
    if (expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }

    cl = Delete.class;
    actual = Mutation.MUTATION_OVERHEAD + ClassSize.align(ClassSize.ARRAY);
    expected  = ClassSize.estimateBase(cl, false);
    //The actual TreeMap is not included in the above calculation
    expected += ClassSize.align(ClassSize.TREEMAP);
    if (expected != actual) {
      ClassSize.estimateBase(cl, true);
      assertEquals(expected, actual);
    }
  }

  @Test
  public void testReferenceSize() {
    LOG.info("ClassSize.REFERENCE is " + ClassSize.REFERENCE);
    // oop should be either 4 or 8
    assertTrue(ClassSize.REFERENCE == 4 || ClassSize.REFERENCE == 8);
  }

  @Test
  public void testObjectSize() throws IOException {
    LOG.info("header:" + ClassSize.OBJECT);
    LOG.info("array header:" + ClassSize.ARRAY);

    if (ClassSize.is32BitJVM()) {
      assertEquals(ClassSize.OBJECT, 8);
    } else {
      assertTrue(ClassSize.OBJECT == 12 || ClassSize.OBJECT == 16); // depending on CompressedOops
    }
    assertEquals(ClassSize.OBJECT + 4, ClassSize.ARRAY);
  }

}

