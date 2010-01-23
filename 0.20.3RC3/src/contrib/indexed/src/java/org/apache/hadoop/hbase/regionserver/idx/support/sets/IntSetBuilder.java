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

/**
 * Encapsultes {@link IntSet} building strategy.
 * May switch set implementations 'behind the scenes'.
 * To prevent extra memory allocation in the index this class implements the
 * intset interface, please note however that
 * all the implemented methods throw {@link UnsupportedOperationException}.
 */
public class IntSetBuilder {

  private SparseBitSet sparseBitSet;

  /**
   * Create a new empty int set.
   *
   * @param capacity the capacity of the set.
   * @return the new set
   */
  public static IntSet newEmptyIntSet(int capacity) {
    SparseBitSet intSet = new SparseBitSet();
    intSet.setCapacity(capacity);
    return intSet;
  }

  /**
   * Calculates the total size of the elements of an IntSet array.
   *
   * @param intSets the intset array to calculate
   * @return the total size
   */
  public static long calcHeapSize(IntSet[] intSets) {
    int size = 0;
    for (IntSet set : intSets) {
      size += set.heapSize();
    }
    return size;
  }

  /**
   * Start building the intset.
   *
   * @return this
   */
  public IntSetBuilder start() {
    sparseBitSet = new SparseBitSet();
    return this;
  }

  /**
   * Adds the next item to this set. Items must be added in order.
   *
   * @param element the item to add
   * @return this
   */
  public IntSetBuilder addNext(int element) {
    sparseBitSet.addNext(element);
    return this;
  }

  /**
   * Convenience method that adds one or more elements.
   *
   * @param element  a mandatory element
   * @param elements an array of optional elements
   * @return this
   * @see #addNext(int)
   */
  public IntSetBuilder addAll(int element, int... elements) {
    addNext(element);
    if (elements != null) {
      for (int i : elements) {
        addNext(i);
      }
    }
    return this;
  }


  /**
   * Finalize the bitset.
   *
   * @param numKeys the number of keys in the final bitset
   * @return the underlying bitset.
   */
  public IntSet finish(int numKeys) {
    sparseBitSet.setCapacity(numKeys);
    return sparseBitSet;
  }
}
