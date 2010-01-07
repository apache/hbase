/*
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
package org.apache.hadoop.hbase.util;

import junit.framework.Assert;
import org.apache.hadoop.hbase.HBaseTestCase;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

/**
 * `
 * Tests for the pair class.
 */
public class TestPair extends HBaseTestCase {

  /**
   * Test of factory method.
   * Verifies that <code>null</code> args are permitted.
   */
  public void testOf() {
    Pair<Integer, Double> p1 = Pair.of(1, 1.2);
    Assert.assertEquals(1, (int) p1.getFirst());
    Assert.assertEquals(1.2, p1.getSecond());

    Pair<String, Integer> p2 = Pair.of("a", 1);
    Assert.assertEquals("a", p2.getFirst());
    Assert.assertEquals(1, (int) p2.getSecond());

    p2 = Pair.of(null, (Integer) null);
    Assert.assertNull(p2.getFirst());
    Assert.assertNull(p2.getSecond());
  }


  /**
   * Test equals() method works as expected.
   */
  public void testEquals() {
    // Set up test data
    Pair<?, ?> item1a = Pair.of(1, 1);
    Pair<?, ?> item1b = Pair.of(1, 1);
    Pair<?, ?> item2 = Pair.of(1, 2);
    Pair<?, ?> item3 = Pair.of(2, 1);

    // One should be equal
    Assert.assertTrue(item1a.equals(item1b));

    // Others should not be
    Assert.assertFalse(item1a.equals(item2));
    Assert.assertFalse(item1a.equals(item3));

    // check array equals
    Pair<?,?> item4 = Pair.of(new byte[]{3,27}, new String[]{"foo","bar"});
    Pair<?,?> item5 = Pair.of(new byte[]{3,27}, new String[]{"foo","bar"});
    Assert.assertTrue(item4.equals(item5));

  }

  /**
   * Test equals() method works as expected.
   */
  public void testEqualsNullFirst() {
    // Set up test data
    Pair<?, ?> item1 = Pair.of(1, 1);
    Pair<?, ?> item2 = Pair.of(null, 2);

    Assert.assertFalse(item1.equals(item2));
    Assert.assertFalse(item2.equals(item1));
  }

  /**
   * Test equals() method works as expected.
   */
  public void testEqualsNullSecond() {
    // Set up test data
    Pair<?, ?> item1 = Pair.of(1, 1);
    Pair<?, ?> item2 = Pair.of(1, null);

    Assert.assertFalse(item1.equals(item2));
    Assert.assertFalse(item2.equals(item1));
  }

  /**
   * Test equals() method works as expected.
   */
  public void testEqualsNullBoth() {
    // Set up test data
    Pair<?, ?> item1 = Pair.of(null, null);
    Pair<?, ?> item2 = Pair.of(null, null);

    Assert.assertTrue(item1.equals(item2));
    Assert.assertTrue(item2.equals(item1));
  }

  /**
   * Test hashCode() method works correctly.
   */
  public void testHashCode() {
    // Set up test data
    Set<Pair<?, ?>> testSet = new HashSet<Pair<?, ?>>();
    Pair<?, ?> item1a = Pair.of(1, 1);
    Pair<?, ?> item1b = Pair.of(1, 1);
    Pair<?, ?> item2 = Pair.of(1, 2);
    Pair<?, ?> item3 = Pair.of(2, 1);

    // Item one should be found since they share the same content.
    testSet.add(item1a);
    Assert.assertTrue(testSet.contains(item1b));

    // Others should not be found
    Assert.assertFalse(testSet.contains(item2));
    Assert.assertFalse(testSet.contains(item3));
  }

  /**
   * Test hashCode() method works correctly with primitive arrays.
   */
  public void testHashCodeWithPrimitiveArray() {
    // Set up test data
    Pair<?, ?> pair1 = Pair.of(new byte[] {1, 2, 3}, new byte[] {4, 5, 6});
    Pair<?, ?> pair2 = Pair.of(new byte[] {1, 2, 3}, new byte[] {4, 5, 6});

    Assert.assertEquals(pair1.hashCode(), pair2.hashCode());
    Assert.assertFalse(pair1.hashCode() == Pair.of(1, 2).hashCode());
  }

  /**
   * Test hashCode() method works correctly with object arrays.
   */
  public void testHashCodeWithObjectArray() {
    // Set up test data
    Pair<?, ?> pair1 = Pair.of(new Object[] {"one", "two", "three"}, new Object[] {"four", "five", "six"});
    Pair<?, ?> pair2 = Pair.of(new Object[] {"one", "two", "three"}, new Object[] {"four", "five", "six"});

    Assert.assertEquals(pair1.hashCode(), pair2.hashCode());
    Assert.assertFalse(pair1.hashCode() == Pair.of(1, 2).hashCode());
  }

  /**
   * Tests the the pair class serializes and deserializes without a problem.
   *
   * @throws IOException            exception - should not occur
   * @throws ClassNotFoundException exception - should not occur
   */
  public void testSerializable() throws IOException, ClassNotFoundException {
    Pair<?, ?> item1 = Pair.of(5, "bla");
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(bout);
    out.writeObject(item1);
    ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bout.toByteArray()));
    Assert.assertEquals(item1, in.readObject());
  }

}
