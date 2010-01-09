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
package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.regionserver.idx.support.arrays.BinarySearch;
import org.apache.hadoop.hbase.regionserver.idx.support.arrays.List;
import org.apache.hadoop.hbase.regionserver.idx.support.sets.IntSet;
import org.apache.hadoop.hbase.regionserver.idx.support.sets.IntSetBuilder;
import static org.apache.hadoop.hbase.regionserver.idx.support.sets.IntSetBuilder.calcHeapSize;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

/**
 * A complete index implementation - all keys are put in the keystore, no skips.
 */
class CompleteIndex implements IdxIndex {
  /**
   * The fixed part in the heap size calcualtion.
   */
  static final long FIXED_SIZE = ClassSize.align(ClassSize.OBJECT +
    ClassSize.REFERENCE + 3 * (ClassSize.ARRAY + ClassSize.REFERENCE) +
    Bytes.SIZEOF_LONG + 2 * Bytes.SIZEOF_INT
  );

  /**
   * The capacity of the sets.
   */
  private int numKeyValues;
  /**
   * The key store - holds the col:qual values.
   */
  private List<?> keyStore;
  /**
   * The value store - holds sets with {@link numKeyValues} capacity.
   */
  private IntSet[] valueStore;
  /**
   * Sets containing partial calculations of the tail operation.
   */
  private IntSet[] heads;
  /**
   * Sets containing partial calculations of the head operation.
   */
  private IntSet[] tails;
  /**
   * The partial calculation interval (used to determine up to which point
   * to use the valueStore before grabbing a pre-calculated set.
   */
  private int precalcInterval;
  /**
   * The heap size.
   */
  private long heapSize;

  /**
   * Construct a new complete index.
   *
   * @param keyStore        the key store
   * @param valueStore      the value store
   * @param heads           a list of precalculated heads
   * @param tails           a list of precalculated tails
   * @param numKeyValues    the total number of KeyValues for this region
   * @param precalcInterval the interval by which tails/heads are precalculated
   */
  CompleteIndex(List<?> keyStore, IntSet[] valueStore,
    IntSet[] heads, IntSet[] tails,
    int numKeyValues, int precalcInterval) {
    this.keyStore = keyStore;
    this.valueStore = valueStore;
    this.heads = heads;
    this.tails = tails;
    this.numKeyValues = numKeyValues;
    this.precalcInterval = precalcInterval;
    heapSize = FIXED_SIZE + keyStore.heapSize() + calcHeapSize(valueStore) +
      calcHeapSize(heads) + calcHeapSize(tails);
  }

  /**
   * Looks up a key in the index.
   *
   * @param probe the probe to lookup
   * @return the set of results, may be empty
   */
  @Override
  public IntSet lookup(byte[] probe) {
    int index = BinarySearch.search(keyStore, keyStore.size(), probe);
    return index >= 0 ? valueStore[index].clone() :
      IntSetBuilder.newEmptyIntSet(numKeyValues);
  }

  /**
   * Find all the results which are greater and perhaps equal to the probe.
   *
   * @param probe     the probe to lookup
   * @param inclusive if greater equal
   * @return a possibly empty set of results
   */
  @Override
  public IntSet tail(byte[] probe, boolean inclusive) {
    int index = BinarySearch.search(keyStore, keyStore.size(), probe);
    if (index < 0 || !inclusive) {
      index++;
    }
    if (index < 0) {
      index = -index;
    }
    int tailIndex = index / precalcInterval + 1;
    IntSet result = tailIndex < tails.length ?
      tails[tailIndex].clone() :
      IntSetBuilder.newEmptyIntSet(numKeyValues);
    int stopIndex = Math.min(tailIndex * precalcInterval, valueStore.length);
    for (int i = index; i < stopIndex; i++) {
      result = result.unite(valueStore[i]);
    }
    return result;
  }

  /**
   * Find all the results which are less than and perhaps equal to the probe.
   *
   * @param probe     the probe to lookup
   * @param inclusive if greater equal
   * @return a possibly empty set of results
   */
  @Override
  public IntSet head(byte[] probe, boolean inclusive) {
    int index = BinarySearch.search(keyStore, keyStore.size(), probe);
    if (index >= 0 && inclusive) {
      index++;
    }
    if (index < 0) {
      index = -(index + 1);
    }

    int headIndex = (index - 1) / precalcInterval;
    IntSet result = headIndex > 0 ?
      heads[headIndex].clone() :
      IntSetBuilder.newEmptyIntSet(numKeyValues);
    int startIndex = Math.max(headIndex * precalcInterval, 0);
    for (int i = startIndex; i < index; i++) {
      result = result.unite(valueStore[i]);
    }
    return result;
  }

  @Override
  public long heapSize() {
    return heapSize;
  }

  public String probeToString(byte[] bytes) {
    return ArrayUtils.toString(keyStore.fromBytes(bytes));
  }
}
