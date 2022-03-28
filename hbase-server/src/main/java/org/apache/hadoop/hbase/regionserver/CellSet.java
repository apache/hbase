/**
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

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.NavigableMap;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A {@link java.util.Set} of {@link Cell}s, where an add will overwrite the entry if already
 * exists in the set.  The call to add returns true if no value in the backing map or false if
 * there was an entry with same key (though value may be different).
 * implementation is tolerant of concurrent get and set and won't throw
 * ConcurrentModificationException when iterating.
 */
@InterfaceAudience.Private
public class CellSet implements NavigableSet<Cell>  {

  public static final int UNKNOWN_NUM_UNIQUES = -1;
  // Implemented on top of a {@link java.util.concurrent.ConcurrentSkipListMap}
  // Differ from CSLS in one respect, where CSLS does "Adds the specified element to this set if it
  // is not already present.", this implementation "Adds the specified element to this set EVEN
  // if it is already present overwriting what was there previous".
  // Otherwise, has same attributes as ConcurrentSkipListSet
  private final NavigableMap<Cell, Cell> delegatee; ///

  private final int numUniqueKeys;

  public CellSet(final CellComparator c) {
    this.delegatee = new ConcurrentSkipListMap<>(c.getSimpleComparator());
    this.numUniqueKeys = UNKNOWN_NUM_UNIQUES;
  }

  CellSet(final NavigableMap<Cell, Cell> m, int numUniqueKeys) {
    this.delegatee = m;
    this.numUniqueKeys = numUniqueKeys;
  }

  CellSet(final NavigableMap<Cell, Cell> m) {
    this.delegatee = m;
    this.numUniqueKeys = UNKNOWN_NUM_UNIQUES;
  }

  NavigableMap<Cell, Cell> getDelegatee() {
    return delegatee;
  }

  @Override
  public Cell ceiling(Cell e) {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public Iterator<Cell> descendingIterator() {
    return this.delegatee.descendingMap().values().iterator();
  }

  @Override
  public NavigableSet<Cell> descendingSet() {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public Cell floor(Cell e) {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public SortedSet<Cell> headSet(final Cell toElement) {
    return headSet(toElement, false);
  }

  @Override
  public NavigableSet<Cell> headSet(final Cell toElement,
      boolean inclusive) {
    return new CellSet(this.delegatee.headMap(toElement, inclusive), UNKNOWN_NUM_UNIQUES);
  }

  @Override
  public Cell higher(Cell e) {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public Iterator<Cell> iterator() {
    return this.delegatee.values().iterator();
  }

  @Override
  public Cell lower(Cell e) {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public Cell pollFirst() {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public Cell pollLast() {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public SortedSet<Cell> subSet(Cell fromElement, Cell toElement) {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public NavigableSet<Cell> subSet(Cell fromElement,
      boolean fromInclusive, Cell toElement, boolean toInclusive) {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public SortedSet<Cell> tailSet(Cell fromElement) {
    return tailSet(fromElement, true);
  }

  @Override
  public NavigableSet<Cell> tailSet(Cell fromElement, boolean inclusive) {
    return new CellSet(this.delegatee.tailMap(fromElement, inclusive), UNKNOWN_NUM_UNIQUES);
  }

  @Override
  public Comparator<? super Cell> comparator() {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public Cell first() {
    return this.delegatee.firstEntry().getValue();
  }

  @Override
  public Cell last() {
    return this.delegatee.lastEntry().getValue();
  }

  @Override
  public boolean add(Cell e) {
    return this.delegatee.put(e, e) == null;
  }

  @Override
  public boolean addAll(Collection<? extends Cell> c) {
    throw new UnsupportedOperationException(HConstants.NOT_IMPLEMENTED);
  }

  @Override
  public void clear() {
    this.delegatee.clear();
  }

  @Override
  public boolean contains(Object o) {
    //noinspection SuspiciousMethodCalls
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
