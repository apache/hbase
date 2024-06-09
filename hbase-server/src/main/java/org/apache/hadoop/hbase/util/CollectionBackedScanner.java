/*
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
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.regionserver.NonReversedNonLazyKeyValueScanner;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Utility scanner that wraps a sortable collection and serves as a KeyValueScanner.
 */
@InterfaceAudience.Private
public class CollectionBackedScanner extends NonReversedNonLazyKeyValueScanner {
  final private Iterable<ExtendedCell> data;
  final CellComparator comparator;
  private Iterator<ExtendedCell> iter;
  private ExtendedCell current;

  public CollectionBackedScanner(SortedSet<ExtendedCell> set) {
    this(set, CellComparator.getInstance());
  }

  public CollectionBackedScanner(SortedSet<ExtendedCell> set, CellComparator comparator) {
    this.comparator = comparator;
    data = set;
    init();
  }

  public CollectionBackedScanner(List<ExtendedCell> list) {
    this(list, CellComparator.getInstance());
  }

  public CollectionBackedScanner(List<ExtendedCell> list, CellComparator comparator) {
    Collections.sort(list, comparator);
    this.comparator = comparator;
    data = list;
    init();
  }

  public CollectionBackedScanner(CellComparator comparator, ExtendedCell... array) {
    this.comparator = comparator;

    List<ExtendedCell> tmp = new ArrayList<>(array.length);
    Collections.addAll(tmp, array);
    Collections.sort(tmp, comparator);
    data = tmp;
    init();
  }

  private void init() {
    iter = data.iterator();
    if (iter.hasNext()) {
      current = iter.next();
    }
  }

  @Override
  public ExtendedCell peek() {
    return current;
  }

  @Override
  public ExtendedCell next() {
    ExtendedCell oldCurrent = current;
    if (iter.hasNext()) {
      current = iter.next();
    } else {
      current = null;
    }
    return oldCurrent;
  }

  @Override
  public boolean seek(ExtendedCell seekCell) {
    // restart iterator
    iter = data.iterator();
    return reseek(seekCell);
  }

  @Override
  public boolean reseek(ExtendedCell seekCell) {
    while (iter.hasNext()) {
      ExtendedCell next = iter.next();
      int ret = comparator.compare(next, seekCell);
      if (ret >= 0) {
        current = next;
        return true;
      }
    }
    return false;
  }

  @Override
  public void close() {
    // do nothing
  }
}
