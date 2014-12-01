/*
 * Copyright The Apache Software Foundation
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


import com.google.common.base.Supplier;

import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * A simple concurrent map of sets. This is similar in concept to
 * {@link com.google.common.collect.Multiset}, with the following exceptions:
 * <ul>
 *   <li>The set is thread-safe and concurrent: no external locking or
 *   synchronization is required. This is important for the use case where
 *   this class is used to index cached blocks by filename for their
 *   efficient eviction from cache when the file is closed or compacted.</li>
 *   <li>The expectation is that all entries may only be removed for a key
 *   once no more additions of values are being made under that key.</li>
 * </ul>
 * @param <K> Key type
 * @param <V> Value type
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ConcurrentIndex<K, V> {

  /** Container for the sets, indexed by key */
  private final ConcurrentMap<K, Set<V>> container;

  /**
   * A factory that constructs new instances of the sets if no set is
   * associated with a given key.
   */
  private final Supplier<Set<V>> valueSetFactory;

  /**
   * Creates an instance with a specified factory object for sets to be
   * associated with a given key.
   * @param valueSetFactory The factory instance
   */
  public ConcurrentIndex(Supplier<Set<V>> valueSetFactory) {
    this.valueSetFactory = valueSetFactory;
    this.container = new ConcurrentHashMap<K, Set<V>>();
  }

  /**
   * Creates an instance using the DefaultValueSetFactory for sets,
   * which in turn creates instances of {@link ConcurrentSkipListSet}
   * @param valueComparator A {@link Comparator} for value types
   */
  public ConcurrentIndex(Comparator<V> valueComparator) {
    this(new DefaultValueSetFactory<V>(valueComparator));
  }

  /**
   * Associate a new unique value with a specified key. Under the covers, the
   * method employs optimistic concurrency: if no set is associated with a
   * given key, we create a new set; if another thread comes in, creates,
   * and associates a set with the same key in the mean-time, we simply add
   * the value to the already created set.
   * @param key The key
   * @param value An additional unique value we want to associate with a key
   */
  public void put(K key, V value) {
    Set<V> set = container.get(key);
    if (set != null) {
      set.add(value);
    } else {
      set = valueSetFactory.get();
      set.add(value);
      Set<V> existing = container.putIfAbsent(key, set);
      if (existing != null) {
        // If a set is already associated with a key, that means another
        // writer has already come in and created the set for the given key.
        // Pursuant to an optimistic concurrency policy, in this case we will
        // simply add the value to the existing set associated with the key.
        existing.add(value);
      }
    }
  }

  /**
   * Get all values associated with a specified key or null if no values are
   * associated. <b>Note:</b> if the caller wishes to add or removes values
   * to under the specified as they're iterating through the returned value,
   * they should make a defensive copy; otherwise, a
   * {@link java.util.ConcurrentModificationException} may be thrown.
   * @param key The key
   * @return All values associated with the specified key or null if no values
   *         are associated with the key.
   */
  public Set<V> values(K key) {
    return container.get(key);
  }

  /**
   * Removes the association between a specified key and value. If as a
   * result of removing a value a set becomes empty, we remove the given
   * set from the mapping as well.
   * @param key The specified key
   * @param value The value to disassociate with the key
   */
  public boolean remove(K key, V value) {
    Set<V> set = container.get(key);
    boolean success = false;
    if (set != null) {
      success = set.remove(value);
      if (set.isEmpty()) {
        container.remove(key);
      }
    }
    return success;
  }

  /**
   * Default factory class for the sets associated with given keys. Creates
   * a {@link ConcurrentSkipListSet} using the comparator passed into the
   * constructor.
   * @see ConcurrentSkipListSet
   * @see Supplier
   * @param <V> The value type. Should match value type of the
   *           ConcurrentIndex instances of this object are passed to.
   */
  private static class DefaultValueSetFactory<V> implements Supplier<Set<V>> {
    private final Comparator<V> comparator;

    /**
     * Creates an instance that passes a specified comparator to the
     * {@link ConcurrentSkipListSet}
     * @param comparator The specified comparator
     */
    public DefaultValueSetFactory(Comparator<V> comparator) {
      this.comparator = comparator;
    }

    /**
     * Creates a new {@link ConcurrentSkipListSet} instance using the
     * comparator specified when the class instance was constructed.
     * @return The instantiated {@link ConcurrentSkipListSet} object
     */
    @Override
    public Set<V> get() {
      return new ConcurrentSkipListSet<V>(comparator);
    }
  }
}
