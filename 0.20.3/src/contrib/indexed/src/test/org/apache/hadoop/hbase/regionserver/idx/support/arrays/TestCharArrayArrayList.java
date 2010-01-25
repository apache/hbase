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
import java.util.Arrays;


public class TestCharArrayArrayList extends HBaseTestCase {



  private static final int[] INVALID_INDEXES = {0, -1, 1};

  /**
   * Verifies that the initial size constructor initialises as expected.
   */
  public void testInitialSizeAndEmpty() {
    CharArrayArrayList test = new CharArrayArrayList();
    checkSizeAndCapacity(test, 0, 1);
    Assert.assertTrue(test.isEmpty());

    test = new CharArrayArrayList(1000);
    checkSizeAndCapacity(test, 0, 1000);
    Assert.assertTrue(test.isEmpty());

    test.add(new char[]{5});
    Assert.assertFalse(test.isEmpty());
  }

  /**
   * Verifies copy constructor.
   */
  public void testCopyConstructor() {
    // Create an original with a capacity of 2, but only one entry
    CharArrayArrayList original = new CharArrayArrayList(2);
    original.add(new char[]{1});
    char[][] values = (char[][]) getField(original, "values");
    Assert.assertEquals(values.length, 2);
    Assert.assertEquals(original.size(), 1);

    // Create a copy of the original and check that its size + capacity are the minimum
    CharArrayArrayList copy = new CharArrayArrayList(original);
    Assert.assertEquals(copy.size(), 1);
    Assert.assertTrue(Arrays.equals(copy.get(0), new char[]{1}));
    values = (char[][]) getField(copy, "values");
    Assert.assertEquals(values.length, 1);
  }

  /**
   * Ensures the equals() method behaves as expected.
   */
  public void testEquals() {
    CharArrayArrayList test1a = new CharArrayArrayList();
    test1a.add(new char[]{1});
    CharArrayArrayList test1b = new CharArrayArrayList();
    test1b.add(new char[]{1});
    CharArrayArrayList test2 = new CharArrayArrayList();
    test2.add(new char[]{2});

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
  private void checkSizeAndCapacity(CharArrayArrayList test, int size, int capacity) {
    Assert.assertEquals(test.size(), size);

    char[][] values = (char[][]) getField(test, "values");

    Assert.assertEquals(values.length, capacity);
  }

  /**
   * Tests that adding elements grows the array size and capacity as expected.
   */
  public void testAddGetAndGrow() {
    // Initialise
    CharArrayArrayList test = new CharArrayArrayList();
    checkSizeAndCapacity(test, 0, 1);

    // Add the first element and we expect the capacity to be unchanged since we don't have any spots consumed.
    test.add(new char[]{1});
    Assert.assertTrue(Arrays.equals(test.get(0), new char[]{1}));
    checkSizeAndCapacity(test, 1, 1);

    // Add the next element and we expect the capacity to grow by one only
    test.add(new char[]{2});
    Assert.assertTrue(Arrays.equals(test.get(1), new char[]{2}));
    checkSizeAndCapacity(test, 2, 2);

    // Add the next element and we expect the capacity to grow by two
    test.add(new char[]{3});
    Assert.assertTrue(Arrays.equals(test.get(2), new char[]{3}));
    checkSizeAndCapacity(test, 3, 4);

    // Add the next element and we expect the capacity to be unchanged
    test.add(new char[]{4});
    Assert.assertTrue(Arrays.equals(test.get(3), new char[]{4}));
    checkSizeAndCapacity(test, 4, 4);

    // Add the next element and we expect the capacity to be 1.5+1 times larger
    test.add(new char[]{5});
    Assert.assertTrue(Arrays.equals(test.get(4), new char[]{5}));
    checkSizeAndCapacity(test, 5, 7);
  }

  /**
   * Tests get() with various invalid ranges.
   */
  public void testInvalidGet() {
    for (int index : INVALID_INDEXES) {
      try {
        CharArrayArrayList test = new CharArrayArrayList();
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
    CharArrayArrayList test = new CharArrayArrayList();

    // Test with first value added to list
    char[] testValue = new char[]{4, 2, 3};
    Assert.assertEquals(test.indexOf(testValue), -1);
    test.add(testValue);
    Assert.assertEquals(test.indexOf(testValue), 0);

    // Add a second one
    testValue = new char[]{4, 3, 2};
    Assert.assertEquals(test.indexOf(testValue), -1);
    test.add(testValue);
    Assert.assertEquals(test.indexOf(testValue), 1);

    // Change the first to a new value
    testValue = new char[]{4, 1, 5, 6};
    Assert.assertEquals(test.indexOf(testValue), -1);
    test.set(0, testValue);
    Assert.assertEquals(test.indexOf(testValue), 0);
  }

  /**
   * Tests the Searchable implementation.
   */
  public void testSearchable() {
    CharArrayArrayList test = new CharArrayArrayList();

    // Test with first value added to list
    char[] testValue = new char[]{4, 2, 3};
    Assert.assertEquals(BinarySearch.search(test, test.size(), testValue), -1);
    test.add(testValue);
    Assert.assertEquals(BinarySearch.search(test, test.size(), testValue), 0);

    // Add a second one
    testValue = new char[]{4, 3, 2};
    Assert.assertEquals(BinarySearch.search(test, test.size(), testValue), -2);
    test.add(testValue);
    Assert.assertEquals(BinarySearch.search(test, test.size(), testValue), 1);

    // Search for something off the start
    testValue = new char[]{4, 1, 5, 6};
    Assert.assertEquals(BinarySearch.search(test, test.size(), testValue), -1);
  }

  /**
   * Tests set() with various invalid ranges.
   */
  public void testInvalidSet() {
    for (int index : INVALID_INDEXES) {
      try {
        CharArrayArrayList test = new CharArrayArrayList();
        test.set(index, new char[]{0});
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
    final java.util.List<char[]> testData = new ArrayList<char[]>();

    // Test with no content first
    CharArrayArrayList test = new CharArrayArrayList();
    testData.clear();
    for (char[] item : test) {
      testData.add(item);
    }
    Assert.assertEquals(testData.size(), 0);

    // Add a value and ensure it is returned
    test.add(new char[]{1});
    testData.clear();
    for (char[] item : test) {
      testData.add(item);
    }
    Assert.assertTrue(ArrayUtils.isEquals(testData.toArray(),
      new Object[]{new char[]{1}}));

    // Add another value and ensure it is returned
    test.add(new char[]{1});
    testData.clear();
    for (char[] item : test) {
      testData.add(item);
    }
    Assert.assertTrue(ArrayUtils.isEquals(testData.toArray(),
      new Object[]{new char[]{1}, new char[]{1}}));
  }

  /**
   * Tests the remove() method.
   */
  public void testRemove() {
    CharArrayArrayList test = new CharArrayArrayList();
    test.add(new char[]{1});
    Assert.assertTrue(Arrays.equals(test.get(0), new char[]{1}));
    //Assert.assertEquals(test.get(0), new char[]{1});
    test.remove(0);
    Assert.assertTrue(test.isEmpty());

    // Add some
    test.add(new char[]{0});
    test.add(new char[]{1});
    test.add(new char[]{2});

    // Remove a value from the middle and ensure correct operation
    Assert.assertTrue(Arrays.equals(test.remove(1), new char[]{1}));
    Assert.assertTrue(Arrays.equals(test.get(0), new char[]{0}));
    Assert.assertTrue(Arrays.equals(test.get(1), new char[]{2}));
  }

  /**
   * Tests the remove() method.
   */
  public void testInsert() {
    CharArrayArrayList test = new CharArrayArrayList();
    test.insert(0, new char[]{1});
    Assert.assertTrue(Arrays.equals(test.get(0), new char[]{1}));
    Assert.assertEquals(test.size(), 1);

    test.insert(0, new char[]{0});
    Assert.assertTrue(Arrays.equals(test.get(0), new char[]{0}));
    Assert.assertTrue(Arrays.equals(test.get(1), new char[]{1}));
    Assert.assertEquals(test.size(), 2);

    test.insert(1, new char[]{2});
    Assert.assertTrue(Arrays.equals(test.get(0), new char[]{0}));
    Assert.assertTrue(Arrays.equals(test.get(1), new char[]{2}));
    Assert.assertTrue(Arrays.equals(test.get(2), new char[]{1}));
    Assert.assertEquals(test.size(), 3);

    test.insert(3, new char[]{3});
    Assert.assertTrue(Arrays.equals(test.get(0), new char[]{0}));
    Assert.assertTrue(Arrays.equals(test.get(1), new char[]{2}));
    Assert.assertTrue(Arrays.equals(test.get(2), new char[]{1}));
    Assert.assertTrue(Arrays.equals(test.get(3), new char[]{3}));
    Assert.assertEquals(test.size(), 4);
  }

  /**
   * Verifies the removeLast() method works as expected.
   */
  public void testRemoveLast() {
    CharArrayArrayList test = new CharArrayArrayList();
    test.add(new char[]{1});
    test.add(new char[]{2});

    Assert.assertTrue(Arrays.equals(test.removeLast(), new char[]{2}));
    Assert.assertTrue(Arrays.equals(test.get(0), new char[]{1}));

    Assert.assertTrue(Arrays.equals(test.removeLast(), new char[]{1}));
    Assert.assertTrue(test.isEmpty());
  }

  /**
   * Tests remove() with various invalid ranges.
   */
  public void testInvalidRemove() {
    for (int index : INVALID_INDEXES) {
      try {
        CharArrayArrayList test = new CharArrayArrayList();
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
