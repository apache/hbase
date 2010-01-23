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

import org.apache.hadoop.hbase.io.HeapSize;

/**
 * A set of integers ranged between 0 and MAX. Elements to this set have to be
 * added in order.
 */
public interface IntSet extends HeapSize, Cloneable {

  /**
   * The value of the smallest element which may be added to this set.
   */
  int SMALLEST = 0;
  /**
   * The value of the maximal element which may be added to an IntSet.
   */
  int LARGEST = Integer.MAX_VALUE;

  /**
   * Counts the elements in the set.
   * @return number of elements in the set
   */
  int size();

  /**
   * Empty check.
   * @return true if the set is empty.
   */
  boolean isEmpty();

  /**
   * The number of elements which this set may contain.
   * The elements can be any in the range of [0, capacity()-1]
   * @return the maximum element in the set.
   */
  int capacity();

  /**
   * Checks whether an element is contained in the set.
   * @param element an intefer in the range of 0 and {@link #capacity()}
   * @return true if the set contains this element.
   */
  boolean contains(int element);

  /**
   * Clear the set.
   */
  void clear();

  /**
   * Inteverts this set to the 'complementary set' e.g. to a set which contains
   * exactly the set of elements not contained in this set. This operation is
   * unsafe, it may change this set.
   * @return the complementary set.
   */
  IntSet complement();

  /**
   * Intersect this int set with another int set. This operation is unsafe, it
   * may change this set.
   * @param other the set to intersect with (not affected by this operation).
   * @return the intersection (may be a reference to this set).
   */
  IntSet intersect(IntSet other);

  /**
   * Unite this intset with another int set.  This operation is unsafe, it may
   * change this set.
   * @param other the set to unite with.
   * @return the united set, my be a reference to this set.
   */
  IntSet unite(IntSet other);

  /**
   * Subtract all the elements of another set from this one leaving only
   * elements which do not exist in the other set. This operation is unsafe, it
   * may change this set.
   * @param other the set to subtract from this one
   * @return the subtracted set, may be a referene to this one
   */
  IntSet subtract(IntSet other);

  /**
   * The difference between the two sets, all the elements which are set in
   * either but not in both. This operation is unsafe, it may change this set.
   * @param other the other set
   * @return the difference set, may be a referene to this one
   */
  IntSet difference(IntSet other);

  /**
   * Clone this set. Implementing classes must be able to clone themsevles.
   * @return the cloned set.
   */
  IntSet clone();

  /**
   * Returns an iterator over the int set.
   * @return an iterator
   */
  IntSetIterator iterator();

  /**
   * An iterator over an {@link IntSet} that avoids auto-boxing.
   */
  public interface IntSetIterator {
    /**
     * Returns true if the iteration has more elements.
     * @return true if the iterator has more elements, otherwise false
     */
    boolean hasNext();

    /**
     * Returns the next element in the iteration.
     * @return the next element in the iteration
     * @throws IndexOutOfBoundsException iteration has no more elements
     */
    int next();
  }
}
