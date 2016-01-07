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
package org.apache.hadoop.hbase.util;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * A SortedMap implementation that uses Soft Reference values
 * internally to make it play well with the GC when in a low-memory
 * situation. Use as a cache where you also need SortedMap functionality.
 *
 * @param <K> key class
 * @param <V> value class
 */
public class SoftValueSortedMap<K,V> implements SortedMap<K,V> {
  private final SortedMap<K, SoftValue<K,V>> internalMap;
  private final ReferenceQueue<V> rq = new ReferenceQueue<V>();
  private Object sync;

  /** Constructor */
  public SoftValueSortedMap() {
    this(new TreeMap<K, SoftValue<K,V>>());
  }

  /**
   * Constructor
   * @param c comparator
   */
  public SoftValueSortedMap(final Comparator<K> c) {
    this(new TreeMap<K, SoftValue<K,V>>(c));
  }

  /** Internal constructor
   * @param original object to wrap and synchronize on
   */
  private SoftValueSortedMap(SortedMap<K,SoftValue<K,V>> original) {
    this(original, original);
  }

  /** Internal constructor
   * For headMap, tailMap, and subMap support
   * @param original object to wrap
   * @param sync object to synchronize on
   */
  private SoftValueSortedMap(SortedMap<K,SoftValue<K,V>> original, Object sync) {
    this.internalMap = original;
    this.sync = sync;
  }

  /**
   * Checks soft references and cleans any that have been placed on
   * ReferenceQueue.  Call if get/put etc. are not called regularly.
   * Internally these call checkReferences on each access.
   * @return How many references cleared.
   */
  @SuppressWarnings("unchecked")
  private int checkReferences() {
    int i = 0;
    for (Reference<? extends V> ref; (ref = this.rq.poll()) != null;) {
      i++;
      this.internalMap.remove(((SoftValue<K,V>)ref).key);
    }
    return i;
  }

  public V put(K key, V value) {
    synchronized(sync) {
      checkReferences();
      SoftValue<K,V> oldValue = this.internalMap.put(key,
        new SoftValue<K,V>(key, value, this.rq));
      return oldValue == null ? null : oldValue.get();
    }
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    throw new RuntimeException("Not implemented");
  }

  public V get(Object key) {
    synchronized(sync) {
      checkReferences();
      SoftValue<K,V> value = this.internalMap.get(key);
      if (value == null) {
        return null;
      }
      if (value.get() == null) {
        this.internalMap.remove(key);
        return null;
      }
      return value.get();
    }
  }

  public V remove(Object key) {
    synchronized(sync) {
      checkReferences();
      SoftValue<K,V> value = this.internalMap.remove(key);
      return value == null ? null : value.get();
    }
  }

  public boolean containsKey(Object key) {
    synchronized(sync) {
      checkReferences();
      return this.internalMap.containsKey(key);
    }
  }

  public boolean containsValue(Object value) {
    throw new UnsupportedOperationException("Don't support containsValue!");
  }

  public K firstKey() {
    synchronized(sync) {
      checkReferences();
      return internalMap.firstKey();
    }
  }

  public K lastKey() {
    synchronized(sync) {
      checkReferences();
      return internalMap.lastKey();
    }
  }

  public SoftValueSortedMap<K,V> headMap(K key) {
    synchronized(sync) {
      checkReferences();
      return new SoftValueSortedMap<K,V>(this.internalMap.headMap(key), sync);
    }
  }

  public SoftValueSortedMap<K,V> tailMap(K key) {
    synchronized(sync) {
      checkReferences();
      return new SoftValueSortedMap<K,V>(this.internalMap.tailMap(key), sync);
    }
  }

  public SoftValueSortedMap<K,V> subMap(K fromKey, K toKey) {
    synchronized(sync) {
      checkReferences();
      return new SoftValueSortedMap<K,V>(this.internalMap.subMap(fromKey,
          toKey), sync);
    }
  }

  /*
   * retrieves the value associated with the greatest key strictly less than
   *  the given key, or null if there is no such key
   * @param key the key we're interested in
   */
  public synchronized V lowerValueByKey(K key) {
    synchronized(sync) {
      checkReferences();

      Map.Entry<K,SoftValue<K,V>> entry =
        ((NavigableMap<K, SoftValue<K,V>>) this.internalMap).lowerEntry(key);
      if (entry==null) {
        return null;
      }
      SoftValue<K,V> value=entry.getValue();
      if (value==null) {
        return null;
      }
      if (value.get() == null) {
        this.internalMap.remove(key);
        return null;
      }
      return value.get();
    }
  }
  
  public boolean isEmpty() {
    synchronized(sync) {
      checkReferences();
      return this.internalMap.isEmpty();
    }
  }

  public int size() {
    synchronized(sync) {
      checkReferences();
      return this.internalMap.size();
    }
  }

  public void clear() {
    synchronized(sync) {
      checkReferences();
      this.internalMap.clear();
    }
  }

  public Set<K> keySet() {
    synchronized(sync) {
      checkReferences();
      // this is not correct as per SortedMap contract (keySet should be
      // modifiable)
      // needed here so that another thread cannot modify the keyset
      // without locking
      return Collections.unmodifiableSet(this.internalMap.keySet());
    }
  }

  public Comparator<? super K> comparator() {
    return this.internalMap.comparator();
  }

  public Set<Map.Entry<K,V>> entrySet() {
    synchronized(sync) {
      checkReferences();
      // this is not correct as per SortedMap contract (entrySet should be
      // backed by map)
      Set<Map.Entry<K, V>> realEntries = new LinkedHashSet<Map.Entry<K, V>>();
      for (Map.Entry<K, SoftValue<K, V>> entry : this.internalMap.entrySet()) {
        realEntries.add(entry.getValue());
      }
      return realEntries;
    }
  }

  public Collection<V> values() {
    synchronized(sync) {
      checkReferences();
      ArrayList<V> hardValues = new ArrayList<V>();
      for (SoftValue<K,V> softValue : this.internalMap.values()) {
        hardValues.add(softValue.get());
      }
      return hardValues;
    }
  }

  private static class SoftValue<K,V> extends SoftReference<V> implements Map.Entry<K, V> {
    final K key;

    SoftValue(K key, V value, ReferenceQueue<V> q) {
      super(value, q);
      this.key = key;
    }

    public K getKey() {
      return this.key;
    }

    public V getValue() {
      return get();
    }

    public V setValue(V value) {
      throw new RuntimeException("Not implemented");
    }
  }
}
