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
package org.apache.hadoop.hbase.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.regionserver.NonReversedNonLazyKeyValueScanner;

/**
 * Utility scanner that wraps a sortable collection and serves as a KeyValueScanner.
 */
@InterfaceAudience.Private
public class CollectionBackedScanner extends NonReversedNonLazyKeyValueScanner {
  final private Iterable<Cell> data;
  final CellComparator comparator;
  private Iterator<Cell> iter;
  private Cell current;

  public CollectionBackedScanner(SortedSet<Cell> set) {
    this(set, CellComparator.COMPARATOR);
  }

  public CollectionBackedScanner(SortedSet<Cell> set,
      CellComparator comparator) {
    this.comparator = comparator;
    data = set;
    init();
  }

  public CollectionBackedScanner(List<Cell> list) {
    this(list, CellComparator.COMPARATOR);
  }

  public CollectionBackedScanner(List<Cell> list,
      CellComparator comparator) {
    Collections.sort(list, comparator);
    this.comparator = comparator;
    data = list;
    init();
  }

  public CollectionBackedScanner(CellComparator comparator,
      Cell... array) {
    this.comparator = comparator;

    List<Cell> tmp = new ArrayList<Cell>(array.length);
    Collections.addAll(tmp, array);
    Collections.sort(tmp, comparator);
    data = tmp;
    init();
  }

  private void init() {
    iter = data.iterator();
    if(iter.hasNext()){
      current = iter.next();
    }
  }

  @Override
  public Cell peek() {
    return current;
  }

  @Override
  public Cell next() {
    Cell oldCurrent = current;
    if(iter.hasNext()){
      current = iter.next();
    } else {
      current = null;
    }
    return oldCurrent;
  }

  @Override
  public boolean seek(Cell seekCell) {
    // restart iterator
    iter = data.iterator();
    return reseek(seekCell);
  }

  @Override
  public boolean reseek(Cell seekCell) {
    while(iter.hasNext()){
      Cell next = iter.next();
      int ret = comparator.compare(next, seekCell);
      if(ret >= 0){
        current = next;
        return true;
      }
    }
    return false;
  }

  /**
   * @see org.apache.hadoop.hbase.regionserver.KeyValueScanner#getScannerOrder()
   */
  @Override
  public long getScannerOrder() {
    return 0;
  }

  @Override
  public void close() {
    // do nothing
  }
}
