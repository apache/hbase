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
package org.apache.hadoop.hbase.regionserver.idx.support.arrays;

import junit.framework.Assert;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.commons.lang.ArrayUtils;

import java.lang.reflect.Field;
import java.util.ArrayList;


public class TestLongArrayList extends HBaseTestCase {



  private static final int[] INVALID_INDEXES = {0, -1, 1};

  /**
   * Verifies that the initial size constructor initialises as expected.
   */
  public void testInitialSizeAndEmpty() {
    LongArrayList test = new LongArrayList();
    checkSizeAndCapacity(test, 0, 1);
    Assert.assertTrue(test.isEmpty());

    test = new LongArrayList(1000);
    checkSizeAndCapacity(test, 0, 1000);
    Assert.assertTrue(test.isEmpty());

    test.add((long) 5);
    Assert.assertFalse(test.isEmpty());
  }

  /**
   * Verifies copy constructor.
   */
  public void testCopyConstructor() {
    // Create an original with a capacity of 2, but only one entry
    LongArrayList original = new LongArrayList(2);
    original.add((long) 1);
    long[] values = (long[]) getField(original, "values");
    Assert.assertEquals(values.length, 2);
    Assert.assertEquals(original.size(), 1);

    // Create a copy of the original and check that its size + capacity are the minimum
    LongArrayList copy = new LongArrayList(original);
    Assert.assertEquals(copy.size(), 1);
    Assert.assertEquals(copy.get(0), (long) 1);
    values = (long[]) getField(copy, "values");
    Assert.assertEquals(values.length, 1);
  }

  /**
   * Ensures the equals() method behaves as expected.
   */
  public void testEquals() {
    LongArrayList test1a = new LongArrayList();
    test1a.add((long) 1);
    LongArrayList test1b = new LongArrayList();
    test1b.add((long) 1);
    LongArrayList test2 = new LongArrayList();
    test2.add((long) 2);

    Assert.assertTrue(test1a.equals(test1b));
    Assert.assertFalse(test1a.equals(test2));
  }


  /**
   * Ensures the number of elements in the list and its backing capacity are what we expect.
   *
   * @param test     the list to test
   * @param size     the expected number of elements in the list
   * @param capacity the expected capacity
   */
  private void checkSizeAndCapacity(LongArrayList test, int size, int capacity) {
    Assert.assertEquals(test.size(), size);

    long[] values = (long[]) getField(test, "values");

    Assert.assertEquals(values.length, capacity);
  }

  /**
   * Tests that adding elements grows the array size and capacity as expected.
   */
  public void testAddGetAndGrow() {
    // Initialise
    LongArrayList test = new LongArrayList();
    checkSizeAndCapacity(test, 0, 1);

    // Add the first element and we expect the capacity to be unchanged since we don't have any spots consumed.
    test.add((long) 1);
    Assert.assertEquals(test.get(0), (long) 1);
    checkSizeAndCapacity(test, 1, 1);

    // Add the next element and we expect the capacity to grow by one only
    test.add((long) 2);
    Assert.assertEquals(test.get(1), (long) 2);
    checkSizeAndCapacity(test, 2, 2);

    // Add the next element and we expect the capacity to grow by two
    test.add((long) 3);
    Assert.assertEquals(test.get(2), (long) 3);
    checkSizeAndCapacity(test, 3, 4);

    // Add the next element and we expect the capacity to be unchanged
    test.add((long) 4);
    Assert.assertEquals(test.get(3), (long) 4);
    checkSizeAndCapacity(test, 4, 4);

    // Add the next element and we expect the capacity to be 1.5+1 times larger
    test.add((long) 5);
    Assert.assertEquals(test.get(4), (long) 5);
    checkSizeAndCapacity(test, 5, 7);
  }

  /**
   * Tests get() with various invalid ranges.
   */
  public void testInvalidGet() {
    for (int index : INVALID_INDEXES) {
      try {
        LongArrayList test = new LongArrayList();
        test.get(index);
      } catch (ArrayIndexOutOfBoundsException ignored) {
         continue;
      }
      Assert.fail("Expected an array index out of bounds exception");
    }
  }


  /**
   * Tests the indexOf() and set() methods.
   */
  public void testIndexOfAndSet() {
    LongArrayList test = new LongArrayList();

    // Test with first value added to list
    long testValue = (long) 42;
    Assert.assertEquals(test.indexOf(testValue), -1);
    test.add(testValue);
    Assert.assertEquals(test.indexOf(testValue), 0);

    // Add a second one
    testValue = (long) 43;
    Assert.assertEquals(test.indexOf(testValue), -1);
    test.add(testValue);
    Assert.assertEquals(test.indexOf(testValue), 1);

    // Change the first to a new value
    testValue = (long) 41;
    Assert.assertEquals(test.indexOf(testValue), -1);
    test.set(0, testValue);
    Assert.assertEquals(test.indexOf(testValue), 0);
  }

  /**
   * Tests the Searchable implementation.
   */
  public void testSearchable() {
    LongArrayList test = new LongArrayList();

    // Test with first value added to list
    long testValue = (long) 42;
    Assert.assertEquals(BinarySearch.search(test, test.size(), testValue), -1);
    test.add(testValue);
    Assert.assertEquals(BinarySearch.search(test, test.size(), testValue), 0);

    // Add a second one
    testValue = (long) 43;
    Assert.assertEquals(BinarySearch.search(test, test.size(), testValue), -2);
    test.add(testValue);
    Assert.assertEquals(BinarySearch.search(test, test.size(), testValue), 1);

    // Search for something off the start
    testValue = (long) 41;
    Assert.assertEquals(BinarySearch.search(test, test.size(), testValue), -1);
  }

  /**
   * Tests set() with various invalid ranges.
   */
  public void testInvalidSet() {
    for (int index : INVALID_INDEXES) {
      try {
        LongArrayList test = new LongArrayList();
        test.set(index, (long) 0);
      } catch (ArrayIndexOutOfBoundsException ignored) {
        continue;
      }
      Assert.fail("Expected an array index out of bounds exception");
    }
  }


  /**
   * Tests iteration via the Iterable interface.
   */
  public void testIterable() {
    final java.util.List<Long> testData = new ArrayList<Long>();

    // Test with no content first
    LongArrayList test = new LongArrayList();
    testData.clear();
    for (long item : test) {
      testData.add(item);
    }
    Assert.assertEquals(testData.size(), 0);

    // Add a value and ensure it is returned
    test.add((long) 1);
    testData.clear();
    for (long item : test) {
      testData.add(item);
    }
    Assert.assertTrue(ArrayUtils.isEquals(testData.toArray(),
      new Object[]{(long) 1}));

    // Add another value and ensure it is returned
    test.add((long) 1);
    testData.clear();
    for (long item : test) {
      testData.add(item);
    }
    Assert.assertTrue(ArrayUtils.isEquals(testData.toArray(),
      new Object[]{(long) 1, (long) 1}));
  }

  /**
   * Tests the remove() method.
   */
  public void testRemove() {
    LongArrayList test = new LongArrayList();
    test.add((long) 1);
    Assert.assertEquals(test.get(0), (long) 1);
    //Assert.assertEquals(test.get(0), (long) 1);
    test.remove(0);
    Assert.assertTrue(test.isEmpty());

    // Add some
    test.add((long) 0);
    test.add((long) 1);
    test.add((long) 2);

    // Remove a value from the middle and ensure correct operation
    Assert.assertEquals(test.remove(1), (long) 1);
    Assert.assertEquals(test.get(0), (long) 0);
    Assert.assertEquals(test.get(1), (long) 2);
  }

  /**
   * Tests the remove() method.
   */
  public void testInsert() {
    LongArrayList test = new LongArrayList();
    test.insert(0, (long) 1);
    Assert.assertEquals(test.get(0), (long) 1);
    Assert.assertEquals(test.size(), 1);

    test.insert(0, (long) 0);
    Assert.assertEquals(test.get(0), (long) 0);
    Assert.assertEquals(test.get(1), (long) 1);
    Assert.assertEquals(test.size(), 2);

    test.insert(1, (long) 2);
    Assert.assertEquals(test.get(0), (long) 0);
    Assert.assertEquals(test.get(1), (long) 2);
    Assert.assertEquals(test.get(2), (long) 1);
    Assert.assertEquals(test.size(), 3);

    test.insert(3, (long) 3);
    Assert.assertEquals(test.get(0), (long) 0);
    Assert.assertEquals(test.get(1), (long) 2);
    Assert.assertEquals(test.get(2), (long) 1);
    Assert.assertEquals(test.get(3), (long) 3);
    Assert.assertEquals(test.size(), 4);
  }

  /**
   * Verifies the removeLast() method works as expected.
   */
  public void testRemoveLast() {
    LongArrayList test = new LongArrayList();
    test.add((long) 1);
    test.add((long) 2);

    Assert.assertEquals(test.removeLast(), (long) 2);
    Assert.assertEquals(test.get(0), (long) 1);

    Assert.assertEquals(test.removeLast(), (long) 1);
    Assert.assertTrue(test.isEmpty());
  }

  /**
   * Tests remove() with various invalid ranges.
   */
  public void testInvalidRemove() {
    for (int index : INVALID_INDEXES) {
      try {
        LongArrayList test = new LongArrayList();
        test.remove(index);
      } catch (ArrayIndexOutOfBoundsException ignored) {
        continue;
      }
      Assert.fail("Expected an array index out of bounds exception");
    }
  }

  /**
   * Extracts a declared field from a given object.
   *
   * @param target the object from which to extract the field
   * @param name   the name of the field
   * @return the declared field
   */
  public static Object getField(Object target, String name) {
    try {
      Field field = target.getClass().getDeclaredField(name);
      field.setAccessible(true);
      return field.get(target);
    } catch (IllegalAccessException e) {
      Assert.fail("Exception " + e);
    } catch (NoSuchFieldException e) {
      Assert.fail("Exception " + e);
    }
    return null;
  }

}
