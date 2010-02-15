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
package org.apache.hadoop.hbase.regionserver.idx.support.sets;

import junit.framework.Assert;
import junit.framework.AssertionFailedError;
import org.apache.hadoop.hbase.HBaseTestCase;

/**
 * A base test-case of {@link IntSets}.
 */
public abstract class IntSetBaseTestCase extends HBaseTestCase {

  private static final int[] SOME = new int[]{0, 10, 63, 64, 99, 103, 104, 200,
    800, 805};

  /**
   * Creates a sparse bitset with the given elements and maximum.
   *
   * @param capacity       the maximum
   * @param sortedElements the elements assumed to be sorted
   * @return the new sparse bitset.
   */
  public static IntSetBase createSparseBitSet(int capacity,
    int... sortedElements) {
    SparseBitSet bitSet = new SparseBitSet();
    for (int element : sortedElements) {
      bitSet.addNext(element);
    }
    bitSet.setCapacity(capacity);
    return bitSet;
  }

  /**
   * Creates a bitset with the given elements and maximum.
   *
   * @param capacity       the maximum
   * @param sortedElements the elements assumed to be sorted
   * @return the new bitset.
   */
  public static IntSetBase createBitSet(int capacity, int... sortedElements) {
    BitSet bitSet = new BitSet(capacity);
    for (int element : sortedElements) {
      bitSet.addNext(element);
    }
    return bitSet;
  }

  protected abstract IntSetBase newSet(int capacity, int... sortedElements);

  protected void addSome(IntSetBase bitSet) {
    for (int next : SOME) {
      bitSet.addNext(next);
    }
  }

  protected static void fill(IntSetBase bitSet) {
    for (int i = 0; i < bitSet.capacity(); i++) {
      bitSet.addNext(i);
    }
  }


  protected void assertSetsEqual(IntSet set1, IntSet set2) {
    Assert.assertEquals(set1.capacity(), set2.capacity());
    Assert.assertEquals(set1.size(), set2.size());
    IntSet.IntSetIterator iter1 = set1.iterator(), iter2 = set2.iterator();
    while (iter1.hasNext() || iter2.hasNext()) {
      Assert.assertEquals(iter1.next(), iter2.next());
    }
  }

  protected void assertSetsNotEqual(IntSetBase set1, IntSetBase set2) {
    try {
      assertSetsEqual(set1, set2);
      fail("Sets are equal");
    } catch (AssertionFailedError ignored) {
    }
  }

  public void testAdd() {
    IntSetBase bitSet = newSet(1000);

    try {
      bitSet.addNext(-1);
      Assert.fail("expected an error");
    } catch (AssertionError ignored) {
    }

    addSome(bitSet);
    Assert.assertEquals(bitSet.size(), SOME.length);

    try {
      bitSet.addNext(805);
      Assert.fail("expected an error");
    } catch (AssertionError ignored) {
    }

    try {
      bitSet.addNext(1000);
      Assert.fail("expected an error");
    } catch (AssertionError ignored) {
    }
  }

  public void testContains() {
    IntSetBase intSet = newSet(1000);
    addSome(intSet);
    for (int next : SOME) {
      Assert.assertTrue(intSet.contains(next));
    }

    int sum = 0;
    for (int i = 0; i < intSet.capacity(); i++) {
      sum += intSet.contains(i) ? 1 : 0;
    }
    Assert.assertEquals(sum, SOME.length);
  }

  public void testClear() {
    IntSetBase intSet = newSet(1000);
    Assert.assertEquals(intSet.size(), 0);
    Assert.assertTrue(intSet.isEmpty());
    intSet.clear();

    addClearAndCheck(intSet);
    addClearAndCheck(intSet);
  }


  private void addClearAndCheck(IntSetBase intSetBase) {
    addSome(intSetBase);
    Assert.assertEquals(intSetBase.size(), SOME.length);
    Assert.assertFalse(intSetBase.isEmpty());
    intSetBase.clear();
    Assert.assertEquals(intSetBase.size(), 0);
    Assert.assertTrue(intSetBase.isEmpty());
  }

  public void testClone() {
    IntSetBase intSet = newSet(10000);
    IntSetBase otherIntSet = (IntSetBase) intSet.clone();
    assertSetsEqual(intSet, otherIntSet);

    addSome(intSet);
    assertSetsNotEqual(intSet, otherIntSet);

    otherIntSet = (IntSetBase) intSet.clone();
    assertSetsEqual(intSet, otherIntSet);

    intSet.addNext(1001);
    Assert.assertEquals(intSet.size(), otherIntSet.size() + 1);
    Assert.assertFalse(otherIntSet.contains(1001));
  }

  public void testIterator() {
    IntSetBase intSet = newSet(1000);
    IntSet.IntSetIterator iter = intSet.iterator();
    Assert.assertFalse(iter.hasNext());

    addSome(intSet);
    iter = intSet.iterator();
    for (int num : SOME) {
      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals(num, iter.next());
    }
    Assert.assertFalse(iter.hasNext());

    intSet = new BitSet(1000);
    fill(intSet);
    iter = intSet.iterator();
    for (int num = 0; num < 1000; num++) {
      Assert.assertTrue(iter.hasNext());
      Assert.assertEquals(num, iter.next());
    }
    Assert.assertFalse(iter.hasNext());
  }


  public void testComplement() {
    IntSetBase emptySet = newSet(0);
    Assert.assertEquals(emptySet.complement().size(), emptySet.size());

    for (int capacity = 950; capacity < 1050; capacity++) {
      IntSetBase intSet = newSet(capacity);
      Assert.assertEquals(intSet.size(), 0);
      Assert.assertEquals(intSet.complement().size(), capacity);
    }

    IntSetBase intSet = newSet(1001);
    addSome(intSet);
    BitSet cBitSet = (BitSet) intSet.clone().complement();
    Assert.assertEquals(cBitSet.size() + intSet.size(), 1001);
    for (int i = 0; i < 1001; i++) {
      Assert.assertTrue(intSet.contains(i) != cBitSet.contains(i));
    }
  }

  public void testIntersect() {
    IntSetBase intset1 = newSet(1013, 3, 7, 34, 87, 178, 244, 507, 643, 765,
      999);
    IntSetBase intset2 = newSet(1013);

    Assert.assertTrue(intset1.clone().intersect(intset2).isEmpty());
    Assert.assertTrue(intset2.clone().intersect(intset1).isEmpty());

    assertSetsEqual(intset1.clone().intersect(intset1.clone()), intset1);
    intset2 = newSet(1013);
    fill(intset2);
    assertSetsEqual(intset1.clone().intersect(intset2), intset1);

    assertSetsEqual(intset1.clone().intersect(newSet(1013, 34, 63, 64, 65, 107,
      244, 340, 765, 894, 1012)),
      newSet(1013, 34, 244, 765));
  }


  public void testUnite() {
    IntSetBase intset1 = newSet(1013, 3, 7, 34, 87, 178, 244, 507, 643, 765,
      999);
    IntSetBase intset2 = newSet(1013);

    assertSetsEqual(intset1.clone().unite(intset2), intset1);
    assertSetsEqual(intset2.clone().unite(intset1), intset1);

    assertSetsEqual(intset1.clone().unite(intset1.clone()), intset1);
    intset2 = newSet(1013);
    fill(intset2);
    assertSetsEqual(intset1.clone().unite(intset2), intset2);

    assertSetsEqual(intset1.clone().unite(newSet(1013, 34, 63, 64, 65, 107, 244,
      340, 765, 894, 1012)),
      newSet(1013, 3, 7, 34, 63, 64, 65, 87, 107, 178, 244, 340, 507, 643, 765
        , 894, 999, 1012));
  }

  public void testSubtract() {
    IntSetBase intset1 = newSet(1013, 3, 7, 34, 87, 178, 244, 507, 643,
      765, 999);
    IntSetBase intset2 = newSet(1013);

    assertSetsEqual(intset1.clone().subtract(intset2), intset1);
    assertSetsEqual(intset2.clone().subtract(intset1), intset2);

    assertSetsEqual(intset1.clone().subtract(intset1.clone()), intset2);
    intset2 = newSet(1013);
    fill(intset2);
    assertSetsEqual(intset1.clone().subtract(intset2), newSet(1013));
    assertSetsEqual(intset2.clone().subtract(intset1),
      intset1.clone().complement());

    assertSetsEqual(intset1.clone().subtract(newSet(1013, 34, 63, 64, 65,
      107, 244, 340, 765, 894, 1012)),
      newSet(1013, 3, 7, 87, 178, 507, 643, 999));
  }

  public void testDifference() {
    IntSetBase intset1 = newSet(1013, 3, 7, 34, 87, 178, 244, 507, 643,
      765, 999);
    IntSetBase intset2 = newSet(1013);

    assertSetsEqual(intset1.clone().difference(intset2), intset1);
    assertSetsEqual(intset2.clone().difference(intset1), intset1);

    assertSetsEqual(intset1.clone().difference(intset1.clone()), intset2);
    intset2 = newSet(1013);
    fill(intset2);
    assertSetsEqual(intset1.clone().difference(intset2),
      intset1.clone().complement());
    assertSetsEqual(intset2.clone().difference(intset1),
      intset1.clone().complement());

    assertSetsEqual(intset1.clone().difference(newSet(1013, 34, 63, 64, 65,
      107, 244, 340, 765, 894, 1012)),
      newSet(1013, 3, 7, 63, 64, 65, 87, 107, 178, 340, 507, 643, 894,
        999, 1012));
  }
}
