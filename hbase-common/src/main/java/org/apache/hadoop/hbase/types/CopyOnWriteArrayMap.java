/**
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

package org.apache.hadoop.hbase.types;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * A Map that keeps a sorted array in order to provide the concurrent map interface.
 * Keeping a sorted array means that it's much more cache line friendly, making reads faster
 * than the tree version.
 *
 * In order to make concurrent reads and writes safe this does a copy on write.
 * There can only be one concurrent write at a time.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class CopyOnWriteArrayMap<K, V> extends AbstractMap<K, V>
    implements Map<K, V>, ConcurrentNavigableMap<K, V> {
  private final Comparator<? super K> keyComparator;
  private volatile ArrayHolder<K, V> holder;

  public CopyOnWriteArrayMap() {
    this(new Comparator<K>() {
      @Override
      public int compare(K o1, K o2) {
        return ((Comparable) o1).compareTo(o2);
      }
    });
  }

  public CopyOnWriteArrayMap(final Comparator<? super K> keyComparator) {
    this.keyComparator = keyComparator;
    this.holder = new ArrayHolder<>(keyComparator, new Comparator<Entry<K, V>>() {
      @Override
      public int compare(Entry<K, V> o1, Entry<K, V> o2) {
        return keyComparator.compare(o1.getKey(), o2.getKey());
      }
    });
  }

  private CopyOnWriteArrayMap(final Comparator<? super K> keyComparator, ArrayHolder<K, V> holder) {
    this.keyComparator = keyComparator;
    this.holder = holder;
  }

  /*
    Un synchronized read operations.

    No locking.
    No waiting
    No copying.

    These should all be FAST.
   */

  @Override
  public Comparator<? super K> comparator() {
    return keyComparator;
  }

  @Override
  public ConcurrentNavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
    ArrayHolder<K, V> current = this.holder;
    int index = current.find(fromKey);

    if (!inclusive && index >= 0) {
      index++;
    } else if (index < 0) {
      index = -(index + 1);
    }
    return new CopyOnWriteArrayMap<>(
        this.keyComparator,
        new ArrayHolder<>(
            current.entries,
            index,
            current.endIndex,
            current.keyComparator,
            current.comparator));
  }

  @Override
  public ConcurrentNavigableMap<K, V> tailMap(K fromKey) {
    return this.tailMap(fromKey, true);
  }

  @Override
  public K firstKey() {
    ArrayHolder<K, V> current = this.holder;
    if (current.getLength() == 0) {
      return null;
    }
    return current.entries[current.startIndex].getKey();
  }

  @Override
  public K lastKey() {
    ArrayHolder<K, V> current = this.holder;
    if (current.getLength() == 0) {
      return null;
    }
    return current.entries[current.endIndex - 1].getKey();
  }

  @Override
  public Entry<K, V> lowerEntry(K key) {
    ArrayHolder<K, V> current = this.holder;
    if (current.getLength() == 0) {
      return null;
    }

    int index = current.find(key);

    // There's a key exactly equal.
    if (index >= 0) {
      index -= 1;
    } else {
      index = -(index + 1) - 1;
    }

    if (index < current.startIndex || index >= current.endIndex) {
      return null;
    }
    return current.entries[index];
  }

  @Override
  public K lowerKey(K key) {
    Map.Entry<K, V> entry = lowerEntry(key);
    if (entry == null) {
      return null;
    }
    return entry.getKey();
  }

  @Override
  public Entry<K, V> floorEntry(K key) {
    ArrayHolder<K, V> current = this.holder;
    if (current.getLength() == 0) {
      return null;
    }
    int index = current.find(key);
    if (index < 0) {
      index = -(index + 1) - 1;
    }
    if (index < current.startIndex || index >= current.endIndex) {
      return null;
    }

    return current.entries[index];
  }

  @Override
  public K floorKey(K key) {
    Map.Entry<K, V> entry = floorEntry(key);
    if (entry == null) {
      return null;
    }
    return entry.getKey();
  }

  @Override
  public Entry<K, V> ceilingEntry(K key) {
    ArrayHolder<K, V> current = this.holder;
    if (current.getLength() == 0) {
      return null;
    }
    int index = current.find(key);
    if (index < 0) {
      index = -(index + 1);
    }
    if (index < current.startIndex || index >= current.endIndex) {
      return null;
    }

    return current.entries[index];
  }

  @Override
  public K ceilingKey(K key) {
    Map.Entry<K, V> entry = ceilingEntry(key);
    if (entry == null) {
      return null;
    }
    return entry.getKey();
  }

  @Override
  public Entry<K, V> higherEntry(K key) {
    ArrayHolder<K, V> current = this.holder;
    if (current.getLength() == 0) {
      return null;
    }
    int index = current.find(key);

    // There's a key exactly equal.
    if (index >= 0) {
      index += 1;
    } else {
      index = -(index + 1);
    }

    if (index < current.startIndex || index >= current.endIndex) {
      return null;
    }
    return current.entries[index];
  }

  @Override
  public K higherKey(K key) {
    Map.Entry<K, V> entry = higherEntry(key);
    if (entry == null) {
      return null;
    }
    return entry.getKey();
  }

  @Override
  public Entry<K, V> firstEntry() {
    ArrayHolder<K, V> current = this.holder;
    if (current.getLength() == 0) {
      return null;
    }
    return current.entries[current.startIndex];
  }

  @Override
  public Entry<K, V> lastEntry() {
    ArrayHolder<K, V> current = this.holder;
    if (current.getLength() == 0) {
      return null;
    }
    return current.entries[current.endIndex - 1];
  }

  @Override
  public int size() {
    return holder.getLength();
  }

  @Override
  public boolean isEmpty() {
    return holder.getLength() == 0;
  }

  @Override
  public boolean containsKey(Object key) {
    ArrayHolder<K, V> current = this.holder;
    int index = current.find((K) key);
    return index >= 0;
  }

  @Override
  public V get(Object key) {

    ArrayHolder<K, V> current = this.holder;
    int index = current.find((K) key);
    if (index >= 0) {
      return current.entries[index].getValue();
    }
    return null;
  }

  @Override
  public NavigableSet<K> keySet() {
    return new ArrayKeySet<>(this.holder);
  }

  @Override
  public Collection<V> values() {
    return new ArrayValueCollection<>(this.holder);
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    return new ArrayEntrySet<>(this.holder);
  }

  /*
     Synchronized write methods.

     Every method should be synchronized.
     Only one modification at a time.

     These will be slow.
   */


  @Override
  public synchronized V put(K key, V value) {
    ArrayHolder<K, V> current = this.holder;
    int index = current.find(key);
    COWEntry<K, V> newEntry = new COWEntry<>(key, value);
    if (index >= 0) {
      this.holder = current.replace(index, newEntry);
      return current.entries[index].getValue();
    } else {
      this.holder = current.insert(-(index + 1), newEntry);
    }
    return null;
  }

  @Override
  public synchronized V remove(Object key) {

    ArrayHolder<K, V> current = this.holder;
    int index = current.find((K) key);
    if (index >= 0) {
      this.holder = current.remove(index);
      return current.entries[index].getValue();
    }
    return null;
  }

  @Override
  public synchronized void clear() {
    this.holder = new ArrayHolder<>(this.holder.keyComparator, this.holder.comparator);
  }

  @Override
  public synchronized V putIfAbsent(K key, V value) {
    ArrayHolder<K, V> current = this.holder;
    int index = current.find(key);

    if (index < 0) {
      COWEntry<K, V> newEntry = new COWEntry<>(key, value);
      this.holder = current.insert(-(index + 1), newEntry);
      return value;
    }
    return current.entries[index].getValue();
  }

  @Override
  public synchronized boolean remove(Object key, Object value) {
    ArrayHolder<K, V> current = this.holder;
    int index = current.find((K) key);

    if (index >= 0 && current.entries[index].getValue().equals(value)) {
      this.holder = current.remove(index);
      return true;
    }
    return false;
  }

  @Override
  public synchronized boolean replace(K key, V oldValue, V newValue) {
    ArrayHolder<K, V> current = this.holder;
    int index = current.find(key);

    if (index >= 0 && current.entries[index].getValue().equals(oldValue)) {
      COWEntry<K, V> newEntry = new COWEntry<>(key, newValue);
      this.holder = current.replace(index, newEntry);
      return true;
    }
    return false;
  }

  @Override
  public synchronized V replace(K key, V value) {
    ArrayHolder<K, V> current = this.holder;
    int index = current.find(key);

    if (index >= 0) {
      COWEntry<K, V> newEntry = new COWEntry<>(key, value);
      this.holder = current.replace(index, newEntry);
      return current.entries[index].getValue();
    }
    return null;
  }

  @Override
  public Entry<K, V> pollFirstEntry() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Entry<K, V> pollLastEntry() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ConcurrentNavigableMap<K, V> descendingMap() {
    throw new UnsupportedOperationException();
  }

  @Override
  public NavigableSet<K> navigableKeySet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ConcurrentNavigableMap<K, V> subMap(K fromKey, K toKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ConcurrentNavigableMap<K, V> headMap(K toKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ConcurrentNavigableMap<K, V> subMap(K fromKey,
                                             boolean fromInclusive,
                                             K toKey,
                                             boolean toInclusive) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ConcurrentNavigableMap<K, V> headMap(K toKey, boolean inclusive) {
    throw new UnsupportedOperationException();
  }

  @Override
  public NavigableSet<K> descendingKeySet() {
    throw new UnsupportedOperationException();
  }

  private final class ArrayKeySet<K, V> implements NavigableSet<K> {

    private final ArrayHolder<K, V> holder;

    private ArrayKeySet(ArrayHolder<K, V> holder) {
      this.holder = holder;
    }

    @Override
    public int size() {
      return holder.getLength();
    }

    @Override
    public boolean isEmpty() {
      return holder.getLength() == 0;
    }

    @Override
    public boolean contains(Object o) {
      ArrayHolder<K, V> current = this.holder;

      for (int i = current.startIndex; i < current.endIndex; i++) {
        if (current.entries[i].getValue().equals(o)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public K lower(K k) {
      throw new UnsupportedOperationException();
    }

    @Override
    public K floor(K k) {
      throw new UnsupportedOperationException();
    }

    @Override
    public K ceiling(K k) {
      throw new UnsupportedOperationException();
    }

    @Override
    public K higher(K k) {
      throw new UnsupportedOperationException();
    }

    @Override
    public K pollFirst() {
      throw new UnsupportedOperationException();
    }

    @Override
    public K pollLast() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<K> iterator() {
      return new ArrayKeyIterator<>(this.holder);
    }

    @Override
    public NavigableSet<K> descendingSet() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<K> descendingIterator() {
      throw new UnsupportedOperationException();
    }

    @Override
    public NavigableSet<K> subSet(K fromElement,
                                  boolean fromInclusive,
                                  K toElement,
                                  boolean toInclusive) {
      throw new UnsupportedOperationException();
    }

    @Override
    public NavigableSet<K> headSet(K toElement, boolean inclusive) {
      throw new UnsupportedOperationException();
    }

    @Override
    public NavigableSet<K> tailSet(K fromElement, boolean inclusive) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Comparator<? super K> comparator() {
      return (Comparator<? super K>) keyComparator;
    }

    @Override
    public SortedSet<K> subSet(K fromElement, K toElement) {
      return null;
    }

    @Override
    public SortedSet<K> headSet(K toElement) {
      return null;
    }

    @Override
    public SortedSet<K> tailSet(K fromElement) {
      return null;
    }

    @Override
    public K first() {
      ArrayHolder<K, V> current = this.holder;
      if (current.getLength() == 0) {
        return null;
      }
      return current.entries[current.startIndex].getKey();
    }

    @Override
    public K last() {
      ArrayHolder<K, V> current = this.holder;
      if (current.getLength() == 0) {
        return null;
      }
      return current.entries[current.endIndex - 1].getKey();
    }

    @Override
    public Object[] toArray() {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T[] toArray(T[] a) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(K k) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends K> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException();
    }
  }

  private final class ArrayValueCollection<K, V> implements Collection<V> {

    private final ArrayHolder<K, V> holder;

    private ArrayValueCollection(ArrayHolder<K, V> holder) {
      this.holder = holder;
    }

    @Override
    public int size() {
      return holder.getLength();
    }

    @Override
    public boolean isEmpty() {
      return holder.getLength() == 0;
    }

    @Override
    public boolean contains(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<V> iterator() {
      return new ArrayValueIterator<>(this.holder);
    }

    @Override
    public Object[] toArray() {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T[] toArray(T[] a) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(V v) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends V> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o) {
      return false;
    }

    @Override
    public int hashCode() {
      return 0;
    }
  }

  private final class ArrayKeyIterator<K, V> implements Iterator<K> {
    int index;
    private final ArrayHolder<K, V> holder;

    private ArrayKeyIterator(ArrayHolder<K, V> holder) {
      this.holder = holder;
      index = holder.startIndex;
    }


    @Override
    public boolean hasNext() {
      return index < holder.endIndex;
    }

    @Override
    public K next() {
      return holder.entries[index++].getKey();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("remove");
    }
  }

  private final class ArrayValueIterator<K, V> implements Iterator<V> {
    int index;
    private final ArrayHolder<K, V> holder;

    private ArrayValueIterator(ArrayHolder<K, V> holder) {
      this.holder = holder;
      index = holder.startIndex;
    }


    @Override
    public boolean hasNext() {
      return index < holder.endIndex;
    }

    @Override
    public V next() {
      return holder.entries[index++].getValue();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("remove");
    }
  }

  private final class ArrayEntryIterator<K, V> implements Iterator<Map.Entry<K, V>> {

    int index;
    private final ArrayHolder<K, V> holder;

    private ArrayEntryIterator(ArrayHolder<K, V> holder) {
      this.holder = holder;
      this.index = holder.startIndex;
    }

    @Override
    public boolean hasNext() {
      return index < holder.endIndex;
    }

    @Override
    public Entry<K, V> next() {
      return holder.entries[index++];
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("remove");
    }
  }

  private final class ArrayEntrySet<K, V> implements Set<Map.Entry<K, V>> {
    private final ArrayHolder<K, V> holder;

    private ArrayEntrySet(ArrayHolder<K, V> holder) {
      this.holder = holder;
    }

    @Override
    public int size() {
      return holder.getLength();
    }

    @Override
    public boolean isEmpty() {
      return holder.getLength() == 0;
    }

    @Override
    public boolean contains(Object o) {
      return false;
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
      return new ArrayEntryIterator<>(this.holder);
    }

    @Override
    public Object[] toArray() {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T[] toArray(T[] a) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(Entry<K, V> kvEntry) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends Entry<K, V>> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException();
    }
  }

  private final static class ArrayHolder<K, V> {
    private final COWEntry<K, V>[] entries;
    private final int startIndex;
    private final int endIndex;
    private final Comparator<? super K> keyComparator;
    private final Comparator<Map.Entry<K, V>> comparator;


    int getLength() {
      return endIndex - startIndex;
    }


    /**
     * Binary search for a given key
     * @param needle The key to look for in all of the entries
     * @return Same return value as Arrays.binarySearch.
     * Positive numbers mean the index.
     * Otherwise (-1 * insertion point) - 1
     */
    int find(K needle) {
      int begin = startIndex;
      int end = endIndex - 1;

      while (begin <= end) {
        int mid = begin + ((end - begin) / 2);
        K midKey = entries[ mid].key;
        int compareRes = keyComparator.compare(midKey, needle);

        // 0 means equals
        // We found the key.
        if (compareRes == 0) {
          return mid;
        } else if (compareRes < 0) {
          // midKey is less than needle so we need
          // to look at farther up
          begin = mid + 1;
        } else {
          // midKey is greater than needle so we
          // need to look down.
          end = mid - 1;
        }
      }

      return (-1 * begin) - 1;
    }

    ArrayHolder<K, V> replace(int index, COWEntry<K, V> newEntry) {
      // TODO should this restart the array back at start index 0 ?
      COWEntry<K, V>[] newEntries = entries.clone();
      newEntries[index] = newEntry;
      return new ArrayHolder<>(newEntries, startIndex, endIndex, keyComparator, comparator);
    }

    ArrayHolder<K, V> remove(int index) {
      COWEntry<K,V>[] newEntries = new COWEntry[getLength() - 1];
      System.arraycopy(this.entries, startIndex, newEntries, 0, index - startIndex);
      System.arraycopy(this.entries, index + 1, newEntries, index, entries.length - index - 1);
      return new ArrayHolder<>(newEntries, 0, newEntries.length, keyComparator, comparator);
    }

    ArrayHolder<K, V> insert(int index, COWEntry<K, V> newEntry) {
      COWEntry<K,V>[] newEntries = new COWEntry[getLength() + 1];
      System.arraycopy(this.entries, startIndex, newEntries, 0, index - startIndex);
      newEntries[index] = newEntry;
      System.arraycopy(this.entries, index, newEntries, index + 1, getLength() - index);
      return new ArrayHolder<>(newEntries, 0, newEntries.length, keyComparator, comparator);
    }

    private ArrayHolder(
        final Comparator<? super K> keyComparator,
        final Comparator<Map.Entry<K, V>> comparator) {
      this.endIndex = 0;
      this.startIndex = 0;
      this.entries = new COWEntry[] {};
      this.keyComparator = keyComparator;
      this.comparator = comparator;
    }

    private ArrayHolder(COWEntry[] entries,
                        int startIndex, int endIndex,
                        final Comparator<? super K> keyComparator,
                        Comparator<Map.Entry<K, V>> comparator) {
      this.entries = entries;
      this.startIndex = startIndex;
      this.endIndex = endIndex;
      this.keyComparator = keyComparator;
      this.comparator = comparator;
    }
  }

  private final static class COWEntry<K, V> implements Map.Entry<K, V> {
    K key = null;
    V value = null;

    COWEntry(K key, V value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public K getKey() {
      return key;
    }

    @Override
    public V getValue() {
      return value;
    }

    @Override
    public V setValue(V value) {
      V oldValue = this.value;
      this.value = value;
      return oldValue;
    }
  }
}
