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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.RandomAccess;

/**
 * Simple sorted list implementation that uses {@link java.util.ArrayList} as
 * the underlying collection so we can support RandomAccess. All mutations
 * create a new copy of the <code>ArrayList</code> instance, so can be
 * expensive. This class is only intended for use on small, very rarely
 * written collections that expect highly concurrent reads.
 * <p>
 * Read operations are performed on a reference to the internal list at the
 * time of invocation, so will not see any mutations to the collection during
 * their operation. Iterating over list elements manually using the
 * RandomAccess pattern involves multiple operations. For this to be safe get
 * a reference to the internal list first using get(). 
 * <p>
 * If constructed with a {@link java.util.Comparator}, the list will be sorted
 * using the comparator. Adding or changing an element using an index will
 * trigger a resort.
 * <p>
 * Iterators are read-only. They cannot be used to remove elements.
 */
public class SortedList<E> implements List<E>, RandomAccess {
  private volatile List<E> list;
  private final Comparator<? super E> comparator;

  /**
   * Constructs an empty list with the default initial capacity that will be
   * sorted using the given comparator.
   *
   * @param comparator the comparator
   */
  public SortedList(Comparator<? super E> comparator) {
    this.list = Collections.emptyList();
    this.comparator = comparator;
  }

  /**
   * Constructs a list containing the elements of the given collection, in the
   * order returned by the collection's iterator, that will be sorted with the
   * given comparator.
   *
   * @param c the collection
   * @param comparator the comparator
   */
  public SortedList(Collection<? extends E> c, Comparator<? super E> comparator) {
    this.list = Collections.unmodifiableList(new ArrayList<E>(c));
    this.comparator = comparator;
  }

  /**
   * Returns a reference to the unmodifiable list currently backing the SortedList.
   * Changes to the SortedList will not be reflected in this list. Use this
   * method to get a reference for iterating over using the RandomAccess
   * pattern.
   */
  public List<E> get() {
    return list;
  }

  @Override
  public int size() {
    return list.size();
  }

  @Override
  public boolean isEmpty() {
    return list.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return list.contains(o);
  }

  @Override
  public Iterator<E> iterator() {
    return list.iterator();
  }

  @Override
  public Object[] toArray() {
    return list.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return list.toArray(a);
  }

  @Override
  public synchronized boolean add(E e) {
    ArrayList<E> newList = new ArrayList<E>(list);
    boolean changed = newList.add(e);
    if (changed) {
      Collections.sort(newList, comparator);
    }
    list = Collections.unmodifiableList(newList);
    return changed;
  }

  @Override
  public synchronized boolean remove(Object o) {
    ArrayList<E> newList = new ArrayList<E>(list);
    // Removals in ArrayList won't break sorting
    boolean changed = newList.remove(o);
    list = Collections.unmodifiableList(newList);
    return changed;
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return list.containsAll(c);
  }

  @Override
  public synchronized boolean addAll(Collection<? extends E> c) {
    ArrayList<E> newList = new ArrayList<E>(list);
    boolean changed = newList.addAll(c);
    if (changed) {
      Collections.sort(newList, comparator);
    }
    list = Collections.unmodifiableList(newList);
    return changed;
  }

  @Override
  public synchronized boolean addAll(int index, Collection<? extends E> c) {
    ArrayList<E> newList = new ArrayList<E>(list);
    boolean changed = newList.addAll(index, c);
    if (changed) {
      Collections.sort(newList, comparator);
    }
    list = Collections.unmodifiableList(newList);
    return changed;
  }

  @Override
  public synchronized boolean removeAll(Collection<?> c) {
    ArrayList<E> newList = new ArrayList<E>(list);
    // Removals in ArrayList won't break sorting
    boolean changed = newList.removeAll(c);
    list = Collections.unmodifiableList(newList);
    return changed;
  }

  @Override
  public synchronized boolean retainAll(Collection<?> c) {
    ArrayList<E> newList = new ArrayList<E>(list);
    // Removals in ArrayList won't break sorting
    boolean changed = newList.retainAll(c);
    list = Collections.unmodifiableList(newList);
    return changed;
  }

  @Override
  public synchronized void clear() {
    list = Collections.emptyList();
  }

  @Override
  public E get(int index) {
    return list.get(index);
  }

  @Override
  public synchronized E set(int index, E element) {
    ArrayList<E> newList = new ArrayList<E>(list);
    E result = newList.set(index, element);
    Collections.sort(list, comparator);
    list = Collections.unmodifiableList(newList);
    return result;
  }

  @Override
  public synchronized void add(int index, E element) {
    ArrayList<E> newList = new ArrayList<E>(list);
    newList.add(index, element);
    Collections.sort(list, comparator);
    list = Collections.unmodifiableList(newList);
  }

  @Override
  public synchronized E remove(int index) {
    ArrayList<E> newList = new ArrayList<E>(list);
    // Removals in ArrayList won't break sorting
    E result = newList.remove(index);
    list = Collections.unmodifiableList(newList);
    return result;
  }

  @Override
  public int indexOf(Object o) {
    return list.indexOf(o);
  }

  @Override
  public int lastIndexOf(Object o) {
    return list.lastIndexOf(o);
  }

  @Override
  public ListIterator<E> listIterator() {
    return list.listIterator();
  }

  @Override
  public ListIterator<E> listIterator(int index) {
    return list.listIterator(index);
  }

  @Override
  public List<E> subList(int fromIndex, int toIndex) {
    return list.subList(fromIndex, toIndex);
  }
}
