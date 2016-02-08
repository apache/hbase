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
import java.util.SortedSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * A {@link java.util.Set} of {@link Cell}s, where an add will overwrite the entry if already
 * exists in the set.  The call to add returns true if no value in the backing map or false if
 * there was an entry with same key (though value may be different).
 * implementation is tolerant of concurrent get and set and won't throw
 * ConcurrentModificationException when iterating.
 */
@InterfaceAudience.Private
public class CellSet implements NavigableSet<Cell>  {
  // Implemented on top of a {@link java.util.concurrent.ConcurrentSkipListMap}
  // Differ from CSLS in one respect, where CSLS does "Adds the specified element to this set if it
  // is not already present.", this implementation "Adds the specified element to this set EVEN
  // if it is already present overwriting what was there previous".
  // Otherwise, has same attributes as ConcurrentSkipListSet
  private final ConcurrentNavigableMap<Cell, Cell> delegatee;

  CellSet(final CellComparator c) {
    this.delegatee = new ConcurrentSkipListMap<Cell, Cell>(c);
  }

  CellSet(final ConcurrentNavigableMap<Cell, Cell> m) {
    this.delegatee = m;
  }

  public Cell ceiling(Cell e) {
    throw new UnsupportedOperationException("Not implemented");
  }

  public Iterator<Cell> descendingIterator() {
    return this.delegatee.descendingMap().values().iterator();
  }

  public NavigableSet<Cell> descendingSet() {
    throw new UnsupportedOperationException("Not implemented");
  }

  public Cell floor(Cell e) {
    throw new UnsupportedOperationException("Not implemented");
  }

  public SortedSet<Cell> headSet(final Cell toElement) {
    return headSet(toElement, false);
  }

  public NavigableSet<Cell> headSet(final Cell toElement,
      boolean inclusive) {
    return new CellSet(this.delegatee.headMap(toElement, inclusive));
  }

  public Cell higher(Cell e) {
    throw new UnsupportedOperationException("Not implemented");
  }

  public Iterator<Cell> iterator() {
    return this.delegatee.values().iterator();
  }

  public Cell lower(Cell e) {
    throw new UnsupportedOperationException("Not implemented");
  }

  public Cell pollFirst() {
    throw new UnsupportedOperationException("Not implemented");
  }

  public Cell pollLast() {
    throw new UnsupportedOperationException("Not implemented");
  }

  public SortedSet<Cell> subSet(Cell fromElement, Cell toElement) {
    throw new UnsupportedOperationException("Not implemented");
  }

  public NavigableSet<Cell> subSet(Cell fromElement,
      boolean fromInclusive, Cell toElement, boolean toInclusive) {
    throw new UnsupportedOperationException("Not implemented");
  }

  public SortedSet<Cell> tailSet(Cell fromElement) {
    return tailSet(fromElement, true);
  }

  public NavigableSet<Cell> tailSet(Cell fromElement, boolean inclusive) {
    return new CellSet(this.delegatee.tailMap(fromElement, inclusive));
  }

  public Comparator<? super Cell> comparator() {
    throw new UnsupportedOperationException("Not implemented");
  }

  public Cell first() {
    return this.delegatee.get(this.delegatee.firstKey());
  }

  public Cell last() {
    return this.delegatee.get(this.delegatee.lastKey());
  }

  public boolean add(Cell e) {
    return this.delegatee.put(e, e) == null;
  }

  public boolean addAll(Collection<? extends Cell> c) {
    throw new UnsupportedOperationException("Not implemented");
  }

  public void clear() {
    this.delegatee.clear();
  }

  public boolean contains(Object o) {
    //noinspection SuspiciousMethodCalls
    return this.delegatee.containsKey(o);
  }

  public boolean containsAll(Collection<?> c) {
    throw new UnsupportedOperationException("Not implemented");
  }

  public boolean isEmpty() {
    return this.delegatee.isEmpty();
  }

  public boolean remove(Object o) {
    return this.delegatee.remove(o) != null;
  }

  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException("Not implemented");
  }

  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException("Not implemented");
  }

  public Cell get(Cell kv) {
    return this.delegatee.get(kv);
  }

  public int size() {
    return this.delegatee.size();
  }

  public Object[] toArray() {
    throw new UnsupportedOperationException("Not implemented");
  }

  public <T> T[] toArray(T[] a) {
    throw new UnsupportedOperationException("Not implemented");
  }
}
