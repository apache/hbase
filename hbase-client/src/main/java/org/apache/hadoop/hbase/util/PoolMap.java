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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.yetus.audience.InterfaceAudience;

/**
 *
 * The <code>PoolMap</code> maps a key to a collection of values, the elements
 * of which are managed by a pool. In effect, that collection acts as a shared
 * pool of resources, access to which is closely controlled as per the semantics
 * of the pool.
 *
 * <p>
 * In case the size of the pool is set to a non-zero positive number, that is
 * used to cap the number of resources that a pool may contain for any given
 * key. A size of {@link Integer#MAX_VALUE} is interpreted as an unbounded pool.
 * </p>
 *
 * <p>
 * Pool is not thread-safe. It must be synchronized when used by multiple threads. Pool also does
 * not remove elements automatically. Unused resources must be closed and removed explicitly.
 * </p>
 *
 * @param <K>
 *          the type of the key to the resource
 * @param <V>
 *          the type of the resource being pooled
 */
@InterfaceAudience.Private
public class PoolMap<K, V> implements Map<K, V> {
  private PoolType poolType;

  private int poolMaxSize;

  private Map<K, Pool<V>> pools = new HashMap<>();

  public PoolMap(PoolType poolType) {
    this.poolType = poolType;
  }

  public PoolMap(PoolType poolType, int poolMaxSize) {
    this.poolType = poolType;
    this.poolMaxSize = poolMaxSize;
  }

  @Override
  public V get(Object key) {
    Pool<V> pool = pools.get(key);
    return pool != null ? pool.get() : null;
  }

  @Override
  public V put(K key, V value) {
    Pool<V> pool = pools.get(key);
    if (pool == null) {
      pools.put(key, pool = createPool());
    }
    return pool != null ? pool.put(value) : null;
  }

  @SuppressWarnings("unchecked")
  @Override
  public V remove(Object key) {
    pools.remove(key);
    return null;
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean remove(Object key, Object value) {
    Pool<V> pool = pools.get(key);
    boolean res = false;
    if (pool != null) {
      res = pool.remove((V)value);
      if (res && pool.size() == 0) {
        pools.remove(key);
      }
    }
    return res;
  }

  @Override
  public Collection<V> values() {
    Collection<V> values = new ArrayList<>();
    for (Pool<V> pool : pools.values()) {
      Collection<V> poolValues = pool.values();
      if (poolValues != null) {
        values.addAll(poolValues);
      }
    }
    return values;
  }

  public Collection<V> values(K key) {
    Collection<V> values = new ArrayList<>();
    Pool<V> pool = pools.get(key);
    if (pool != null) {
      Collection<V> poolValues = pool.values();
      if (poolValues != null) {
        values.addAll(poolValues);
      }
    }
    return values;
  }


  @Override
  public boolean isEmpty() {
    return pools.isEmpty();
  }

  @Override
  public int size() {
    return pools.size();
  }

  public int size(K key) {
    Pool<V> pool = pools.get(key);
    return pool != null ? pool.size() : 0;
  }

  @Override
  public boolean containsKey(Object key) {
    return pools.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    if (value == null) {
      return false;
    }
    for (Pool<V> pool : pools.values()) {
      if (value.equals(pool.get())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> map) {
    for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void clear() {
    for (Pool<V> pool : pools.values()) {
      pool.clear();
    }
    pools.clear();
  }

  @Override
  public Set<K> keySet() {
    return pools.keySet();
  }

  @Override
  public Set<Map.Entry<K, V>> entrySet() {
    Set<Map.Entry<K, V>> entries = new HashSet<>();
    for (Map.Entry<K, Pool<V>> poolEntry : pools.entrySet()) {
      final K poolKey = poolEntry.getKey();
      final Pool<V> pool = poolEntry.getValue();
      if (pool != null) {
        for (final V poolValue : pool.values()) {
          entries.add(new Map.Entry<K, V>() {
            @Override
            public K getKey() {
              return poolKey;
            }

            @Override
            public V getValue() {
              return poolValue;
            }

            @Override
            public V setValue(V value) {
              return pool.put(value);
            }
          });
        }
      }
    }
    return entries;
  }

  protected interface Pool<R> {
    R get();

    R put(R resource);

    boolean remove(R resource);

    void clear();

    Collection<R> values();

    int size();
  }

  public enum PoolType {
    ThreadLocal, RoundRobin;

    public static PoolType valueOf(String poolTypeName, PoolType defaultPoolType) {
      PoolType poolType = PoolType.fuzzyMatch(poolTypeName);
      return (poolType != null) ? poolType : defaultPoolType;
    }

    public static String fuzzyNormalize(String name) {
      return name != null ? name.replaceAll("-", "").trim().toLowerCase(Locale.ROOT) : "";
    }

    public static PoolType fuzzyMatch(String name) {
      for (PoolType poolType : values()) {
        if (fuzzyNormalize(name).equals(fuzzyNormalize(poolType.name()))) {
          return poolType;
        }
      }
      return null;
    }
  }

  protected Pool<V> createPool() {
    switch (poolType) {
    case RoundRobin:
      return new RoundRobinPool<>(poolMaxSize);
    case ThreadLocal:
      return new ThreadLocalPool<>();
    }
    return null;
  }

  /**
   * The <code>RoundRobinPool</code> represents a {@link PoolMap.Pool}, which
   * stores its resources in an {@link ArrayList}. It load-balances access to
   * its resources by returning a different resource every time a given key is
   * looked up.
   *
   * <p>
   * If {@link #maxSize} is set to {@link Integer#MAX_VALUE}, then the size of
   * the pool is unbounded. Otherwise, it caps the number of resources in this
   * pool to the (non-zero positive) value specified in {@link #maxSize}.
   * </p>
   *
   * @param <R>
   *          the type of the resource
   *
   */
  @SuppressWarnings("serial")
  static class RoundRobinPool<R> implements Pool<R> {
    private final List<R> resources;
    private final int maxSize;

    private int nextIndex;

    public RoundRobinPool(int maxSize) {
      if (maxSize <= 0) {
        throw new IllegalArgumentException("maxSize must be positive");
      }

      resources = new ArrayList<>(maxSize);
      this.maxSize = maxSize;
    }

    @Override
    public R get() {
      int size = resources.size();

      /* letting pool to grow */
      if (size < maxSize) {
        return null;
      }

      R resource = resources.get(nextIndex);

      /* at this point size cannot be 0 */
      nextIndex = (nextIndex + 1) % size;

      return resource;
    }

    @Override
    public R put(R resource) {
      resources.add(resource);
      return null;
    }

    @Override
    public boolean remove(R resource) {
      return resources.remove(resource);
    }

    @Override
    public void clear() {
      resources.clear();
    }

    @Override
    public Collection<R> values() {
      return resources;
    }

    @Override
    public int size() {
      return resources.size();
    }
  }

  /**
   * The <code>ThreadLocalPool</code> represents a {@link PoolMap.Pool} that
   * works similarly to {@link ThreadLocal} class. It essentially binds the resource
   * to the thread from which it is accessed. It doesn't remove resources when a thread exists,
   * those resources must be closed manually.
   *
   * <p>
   * Note that the size of the pool is essentially bounded by the number of threads
   * that add resources to this pool.
   * </p>
   *
   * @param <R>
   *          the type of the resource
   */
  static class ThreadLocalPool<R> implements Pool<R> {
    private final Map<Thread, R> resources;

    public ThreadLocalPool() {
      resources = new HashMap<>();
    }

    @Override
    public R get() {
      Thread myself = Thread.currentThread();
      return resources.get(myself);
    }

    @Override
    public R put(R resource) {
      Thread myself = Thread.currentThread();
      return resources.put(myself, resource);
    }

    @Override
    public boolean remove(R resource) {
      Thread myself = Thread.currentThread();
      return resources.remove(myself, resource);
    }

    @Override
    public int size() {
      return resources.size();
    }

    @Override
    public void clear() {
      resources.clear();
    }

    @Override
    public Collection<R> values() {
      return resources.values();
    }
  }
}
