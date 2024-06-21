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
package org.apache.hadoop.hbase.regionserver;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A {@link java.util.Set} of {@link Cell}s, where an add will overwrite the entry if already exists
 * in the set. The call to add returns true if no value in the backing map or false if there was an
 * entry with same key (though value may be different). implementation is tolerant of concurrent get
 * and set and won't throw ConcurrentModificationException when iterating.
 */
@InterfaceAudience.Private
public class CellSet<T extends Cell> implements NavigableSet<T> {

  public static final int UNKNOWN_NUM_UNIQUES = -1;
  // Implemented on top of a {@link java.util.concurrent.ConcurrentSkipListMap}
  // Differ from CSLS in one respect, where CSLS does "Adds the specified element to this set if it
  // is not already present.", this implementation "Adds the specified element to this set EVEN
  // if it is already present overwriting what was there previous".
  // Otherwise, has same attributes as ConcurrentSkipListSet
  private final NavigableMap<T, T> delegatee; ///

  private final int numUniqueKeys;

  public CellSet(CellComparator c) {
    this.delegatee = new ConcurrentSkipListMap<>(c.getSimpleComparator());
    this.numUniqueKeys = UNKNOWN_NUM_UNIQUES;
  }

  CellSet(final NavigableMap<T, T> m, int numUniqueKeys) {
    this.delegatee = m;
    this.numUniqueKeys = numUniqueKeys;
  }

  CellSet(final NavigableMap<T, T> m) {
    this.delegatee = m;
    this.numUniqueKeys = UNKNOWN_NUM_UNIQUES;
  }

  NavigableMap<T, T> getDelegatee() {
    return delegatee;
  }

  @Override
  public T ceiling(T e) {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public Iterator<T> descendingIterator() {
    return this.delegatee.descendingMap().values().iterator();
  }

  @Override
  public NavigableSet<T> descendingSet() {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public T floor(T e) {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public SortedSet<T> headSet(final T toElement) {
    return headSet(toElement, false);
  }

  @Override
  public NavigableSet<T> headSet(final T toElement, boolean inclusive) {
    return new CellSet<>(this.delegatee.headMap(toElement, inclusive), UNKNOWN_NUM_UNIQUES);
  }

  @Override
  public T higher(T e) {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public Iterator<T> iterator() {
    return this.delegatee.values().iterator();
  }

  @Override
  public T lower(T e) {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public T pollFirst() {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public T pollLast() {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public SortedSet<T> subSet(T fromElement, T toElement) {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public NavigableSet<T> subSet(Cell fromElement, boolean fromInclusive, Cell toElement,
    boolean toInclusive) {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public SortedSet<T> tailSet(T fromElement) {
    return tailSet(fromElement, true);
  }

  @Override
  public NavigableSet<T> tailSet(T fromElement, boolean inclusive) {
    return new CellSet<>(this.delegatee.tailMap(fromElement, inclusive), UNKNOWN_NUM_UNIQUES);
  }

  @Override
  public Comparator<? super T> comparator() {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public T first() {
    return this.delegatee.firstEntry().getValue();
  }

  @Override
  public T last() {
    return this.delegatee.lastEntry().getValue();
  }

  @Override
  public boolean add(T e) {
    return this.delegatee.put(e, e) == null;
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public void clear() {
    this.delegatee.clear();
  }

  @Override
  public boolean contains(Object o) {
    // noinspection SuspiciousMethodCalls
    return this.delegatee.containsKey(o);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public boolean isEmpty() {
    return this.delegatee.isEmpty();
  }

  @Override
  public boolean remove(Object o) {
    return this.delegatee.remove(o) != null;
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  public Cell get(Cell kv) {
    return this.delegatee.get(kv);
  }

  @Override
  public int size() {
    if (delegatee instanceof ConcurrentSkipListMap) {
      throw new UnsupportedOperationException("ConcurrentSkipListMap.size() is time-consuming");
    }
    return this.delegatee.size();
  }

  @Override
  public Object[] toArray() {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public <T> T[] toArray(T[] a) {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  public int getNumUniqueKeys() {
    return numUniqueKeys;
  }
}
