<@pp.dropOutputFile />
<#list types as T>
<@pp.changeOutputFile name="Test"+T.displayName+"ArrayList.java" />
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
<#if (T.kind == "integerArray")>
import java.util.Arrays;
</#if>
<#if T.clazz == "BigDecimal">
import java.math.BigDecimal;
</#if>


public class Test${T.displayName}ArrayList extends HBaseTestCase {

<#if (T.kind == "integerArray")>
<#macro assertEquals expr1 expr2>
    Assert.assertTrue(Arrays.equals(${expr1}, ${expr2}));
</#macro>
<#macro assertNotEquals expr1 expr2>
    Assert.assertFalse(Arrays.equals(${expr1}, ${expr2}));
</#macro>
<#else>
<#macro assertEquals expr1 expr2>
    Assert.assertEquals(${expr1}, ${expr2});
</#macro>
<#macro assertNotEquals expr1 expr2>
    Assert.assertFalse(${expr1}.equals(${expr2}));
</#macro>
</#if>


  private static final int[] INVALID_INDEXES = {0, -1, 1};

<#if (T.kind == "integer" || T.kind == "floatingPoint")>
  <#assign zero = "("+T.primitive+") 0">
  <#assign one = "("+T.primitive+") 1">
  <#assign two = "("+T.primitive+") 2">
  <#assign three = "("+T.primitive+") 3">
  <#assign four = "("+T.primitive+") 4">
  <#assign five = "("+T.primitive+") 5">
  <#assign forty_one = "("+T.primitive+") 41">
  <#assign forty_two = "("+T.primitive+") 42">
  <#assign forty_three = "("+T.primitive+") 43">
<#elseif (T.kind == "integerArray")>
  <#assign zero="new "+T.primitive+"{0}">
  <#assign one="new "+T.primitive+"{1}">
  <#assign two="new "+T.primitive+"{2}">
  <#assign three="new "+T.primitive+"{3}">
  <#assign four="new "+T.primitive+"{4}">
  <#assign five="new "+T.primitive+"{5}">
  <#assign forty_one="new "+T.primitive+"{4, 1, 5, 6}">
  <#assign forty_two="new "+T.primitive+"{4, 2, 3}">
  <#assign forty_three="new "+T.primitive+"{4, 3, 2}">
<#else>
  <#assign zero="new "+T.clazz+"(0)">
  <#assign one="new "+T.clazz+"(1)">
  <#assign two="new "+T.clazz+"(2)">
  <#assign three="new "+T.clazz+"(3)">
  <#assign four="new "+T.clazz+"(4)">
  <#assign five="new "+T.clazz+"(5)">
  <#assign forty_one="new "+T.clazz+"(41)">
  <#assign forty_two="new "+T.clazz+"(42)">
  <#assign forty_three="new "+T.clazz+"(43)">
</#if>
  /**
   * Verifies that the initial size constructor initialises as expected.
   */
  public void testInitialSizeAndEmpty() {
    ${T.displayName}ArrayList test = new ${T.displayName}ArrayList();
    checkSizeAndCapacity(test, 0, 1);
    Assert.assertTrue(test.isEmpty());

    test = new ${T.displayName}ArrayList(1000);
    checkSizeAndCapacity(test, 0, 1000);
    Assert.assertTrue(test.isEmpty());

    test.add(${five});
    Assert.assertFalse(test.isEmpty());
  }

  /**
   * Verifies copy constructor.
   */
  public void testCopyConstructor() {
    // Create an original with a capacity of 2, but only one entry
    ${T.displayName}ArrayList original = new ${T.displayName}ArrayList(2);
    original.add(${one});
    ${T.primitive}[] values = (${T.primitive}[]) getField(original, "values");
    Assert.assertEquals(values.length, 2);
    Assert.assertEquals(original.size(), 1);

    // Create a copy of the original and check that its size + capacity are the minimum
    ${T.displayName}ArrayList copy = new ${T.displayName}ArrayList(original);
    Assert.assertEquals(copy.size(), 1);
    <@assertEquals expr1="copy.get(0)" expr2=one />
    values = (${T.primitive}[]) getField(copy, "values");
    Assert.assertEquals(values.length, 1);
  }

  /**
   * Ensures the equals() method behaves as expected.
   */
  public void testEquals() {
    ${T.displayName}ArrayList test1a = new ${T.displayName}ArrayList();
    test1a.add(${one});
    ${T.displayName}ArrayList test1b = new ${T.displayName}ArrayList();
    test1b.add(${one});
    ${T.displayName}ArrayList test2 = new ${T.displayName}ArrayList();
    test2.add(${two});

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
  private void checkSizeAndCapacity(${T.displayName}ArrayList test, int size, int capacity) {
    Assert.assertEquals(test.size(), size);

    ${T.primitive}[] values = (${T.primitive}[]) getField(test, "values");

    Assert.assertEquals(values.length, capacity);
  }

  /**
   * Tests that adding elements grows the array size and capacity as expected.
   */
  public void testAddGetAndGrow() {
    // Initialise
    ${T.displayName}ArrayList test = new ${T.displayName}ArrayList();
    checkSizeAndCapacity(test, 0, 1);

    // Add the first element and we expect the capacity to be unchanged since we don't have any spots consumed.
    test.add(${one});
    <@assertEquals expr1="test.get(0)" expr2=one />
    checkSizeAndCapacity(test, 1, 1);

    // Add the next element and we expect the capacity to grow by one only
    test.add(${two});
    <@assertEquals expr1="test.get(1)" expr2=two />
    checkSizeAndCapacity(test, 2, 2);

    // Add the next element and we expect the capacity to grow by two
    test.add(${three});
    <@assertEquals expr1="test.get(2)" expr2=three />
    checkSizeAndCapacity(test, 3, 4);

    // Add the next element and we expect the capacity to be unchanged
    test.add(${four});
    <@assertEquals expr1="test.get(3)" expr2=four />
    checkSizeAndCapacity(test, 4, 4);

    // Add the next element and we expect the capacity to be 1.5+1 times larger
    test.add(${five});
    <@assertEquals expr1="test.get(4)" expr2=five />
    checkSizeAndCapacity(test, 5, 7);
  }

  /**
   * Tests get() with various invalid ranges.
   */
  public void testInvalidGet() {
    for (int index : INVALID_INDEXES) {
      try {
        ${T.displayName}ArrayList test = new ${T.displayName}ArrayList();
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
    ${T.displayName}ArrayList test = new ${T.displayName}ArrayList();

    // Test with first value added to list
    ${T.primitive} testValue = ${forty_two};
    Assert.assertEquals(test.indexOf(testValue), -1);
    test.add(testValue);
    Assert.assertEquals(test.indexOf(testValue), 0);

    // Add a second one
    testValue = ${forty_three};
    Assert.assertEquals(test.indexOf(testValue), -1);
    test.add(testValue);
    Assert.assertEquals(test.indexOf(testValue), 1);

    // Change the first to a new value
    testValue = ${forty_one};
    Assert.assertEquals(test.indexOf(testValue), -1);
    test.set(0, testValue);
    Assert.assertEquals(test.indexOf(testValue), 0);
  }

  /**
   * Tests the Searchable implementation.
   */
  public void testSearchable() {
<#if T.clazz != "byte[]">
    ${T.displayName}ArrayList test = new ${T.displayName}ArrayList();
<#else>
    List<?> test = new ${T.displayName}ArrayList();
</#if>

    // Test with first value added to list
    ${T.primitive} testValue = ${forty_two};
    Assert.assertEquals(BinarySearch.search(test, test.size(), testValue), -1);
    test.add(testValue);
    Assert.assertEquals(BinarySearch.search(test, test.size(), testValue), 0);

    // Add a second one
    testValue = ${forty_three};
    Assert.assertEquals(BinarySearch.search(test, test.size(), testValue), -2);
    test.add(testValue);
    Assert.assertEquals(BinarySearch.search(test, test.size(), testValue), 1);

    // Search for something off the start
    testValue = ${forty_one};
    Assert.assertEquals(BinarySearch.search(test, test.size(), testValue), -1);
  }

  /**
   * Tests set() with various invalid ranges.
   */
  public void testInvalidSet() {
    for (int index : INVALID_INDEXES) {
      try {
        ${T.displayName}ArrayList test = new ${T.displayName}ArrayList();
        test.set(index, ${zero});
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
    final java.util.List<${T.clazz}> testData = new ArrayList<${T.clazz}>();

    // Test with no content first
    ${T.displayName}ArrayList test = new ${T.displayName}ArrayList();
    testData.clear();
    for (${T.primitive} item : test) {
      testData.add(item);
    }
    Assert.assertEquals(testData.size(), 0);

    // Add a value and ensure it is returned
    test.add(${one});
    testData.clear();
    for (${T.primitive} item : test) {
      testData.add(item);
    }
    Assert.assertTrue(ArrayUtils.isEquals(testData.toArray(),
      new Object[]{${one}}));

    // Add another value and ensure it is returned
    test.add(${one});
    testData.clear();
    for (${T.primitive} item : test) {
      testData.add(item);
    }
    Assert.assertTrue(ArrayUtils.isEquals(testData.toArray(),
      new Object[]{${one}, ${one}}));
  }

  /**
   * Tests the remove() method.
   */
  public void testRemove() {
    ${T.displayName}ArrayList test = new ${T.displayName}ArrayList();
    test.add(${one});
    <@assertEquals expr1="test.get(0)" expr2=one />
    //Assert.assertEquals(test.get(0), ${one});
    test.remove(0);
    Assert.assertTrue(test.isEmpty());

    // Add some
    test.add(${zero});
    test.add(${one});
    test.add(${two});

    // Remove a value from the middle and ensure correct operation
    <@assertEquals expr1="test.remove(1)" expr2=one />
    <@assertEquals expr1="test.get(0)" expr2=zero />
    <@assertEquals expr1="test.get(1)" expr2=two />
  }

  /**
   * Tests the remove() method.
   */
  public void testInsert() {
    ${T.displayName}ArrayList test = new ${T.displayName}ArrayList();
    test.insert(0, ${one});
    <@assertEquals expr1="test.get(0)" expr2=one />
    Assert.assertEquals(test.size(), 1);

    test.insert(0, ${zero});
    <@assertEquals expr1="test.get(0)" expr2=zero />
    <@assertEquals expr1="test.get(1)" expr2=one/>
    Assert.assertEquals(test.size(), 2);

    test.insert(1, ${two});
    <@assertEquals expr1="test.get(0)" expr2=zero />
    <@assertEquals expr1="test.get(1)" expr2=two />
    <@assertEquals expr1="test.get(2)" expr2=one />
    Assert.assertEquals(test.size(), 3);

    test.insert(3, ${three});
    <@assertEquals expr1="test.get(0)" expr2=zero />
    <@assertEquals expr1="test.get(1)" expr2=two />
    <@assertEquals expr1="test.get(2)" expr2=one />
    <@assertEquals expr1="test.get(3)" expr2=three />
    Assert.assertEquals(test.size(), 4);
  }

  /**
   * Verifies the removeLast() method works as expected.
   */
  public void testRemoveLast() {
    ${T.displayName}ArrayList test = new ${T.displayName}ArrayList();
    test.add(${one});
    test.add(${two});

    <@assertEquals expr1="test.removeLast()" expr2=two />
    <@assertEquals expr1="test.get(0)" expr2=one />

    <@assertEquals expr1="test.removeLast()" expr2=one />
    Assert.assertTrue(test.isEmpty());
  }

  /**
   * Tests remove() with various invalid ranges.
   */
  public void testInvalidRemove() {
    for (int index : INVALID_INDEXES) {
      try {
        ${T.displayName}ArrayList test = new ${T.displayName}ArrayList();
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
</#list>