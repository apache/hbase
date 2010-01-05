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

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.regionserver.idx.support.arrays.List;
import org.apache.hadoop.hbase.regionserver.idx.support.sets.IntSet;
import org.apache.hadoop.hbase.regionserver.idx.support.sets.IntSetBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

/**
 * An empty index.
 */
public class EmptyIndex implements IdxIndex {

  private static final int HEAP_SIZE = ClassSize.align(ClassSize.OBJECT +
    ClassSize.REFERENCE + Bytes.SIZEOF_INT);

  private List<?> keyStore;
  private int numKeyValues;

  /**
   * Construct a new empty index with a given capacity. All sets genreated by
   * this empty index will be initiazlized using the provided capacity.
   *
   * @param keyStore the key store
   * @param capacity the capacity
   */
  EmptyIndex(List<?> keyStore, int capacity) {
    this.keyStore = keyStore;
    this.numKeyValues = capacity;
  }

  /**
   * {@inheritDoc}
   * <p/>
   * Returns an empty set.
   */
  @Override
  public IntSet lookup(byte[] probe) {
    return IntSetBuilder.newEmptyIntSet(numKeyValues);
  }

  /**
   * {@inheritDoc}
   * <p/>
   * Returns an empty set.
   */
  @Override
  public IntSet tail(byte[] probe, boolean inclusive) {
    return IntSetBuilder.newEmptyIntSet(numKeyValues);
  }

  /**
   * {@inheritDoc}
   * <p/>
   * Returns an empty set.
   */
  @Override
  public IntSet head(byte[] probe, boolean inclusive) {
    return IntSetBuilder.newEmptyIntSet(numKeyValues);
  }

  @Override
  public String probeToString(byte[] bytes) {
    return ArrayUtils.toString(keyStore.fromBytes(bytes));
  }

  @Override
  public long heapSize() {
    return HEAP_SIZE;
  }
}
