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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

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
 * PoolMap is thread-safe. It does not remove elements automatically. Unused resources
 * must be closed and removed explicitly.
 * </p>
 *
 * @param <K>
 *          the type of the key to the resource
 * @param <V>
 *          the type of the resource being pooled
 */
@InterfaceAudience.Private
public class PoolMap<K, V> {
  private final Map<K, Pool<V>> pools;
  private final PoolType poolType;
  private final int poolMaxSize;

   public PoolMap(PoolType poolType, int poolMaxSize) {
     pools = new HashMap<>();
     this.poolType = poolType;
     this.poolMaxSize = poolMaxSize;
  }

  public V getOrCreate(K key, PoolResourceSupplier<V> supplier) throws IOException {
     synchronized (pools) {
       Pool<V> pool = pools.get(key);

       if (pool == null) {
         pool = createPool();
         pools.put(key, pool);
       }

       try {
         return pool.getOrCreate(supplier);
       } catch (IOException | RuntimeException | Error e) {
         if (pool.size() == 0) {
           pools.remove(key);
         }

         throw e;
       }
     }
  }
  public boolean remove(K key, V value) {
    synchronized (pools) {
      Pool<V> pool = pools.get(key);

      if (pool == null) {
        return false;
      }

      boolean removed = pool.remove(value);

      if (removed && pool.size() == 0) {
        pools.remove(key);
      }

      return removed;
    }
  }

  public List<V> values() {
    List<V> values = new ArrayList<>();

    synchronized (pools) {
      for (Pool<V> pool : pools.values()) {
        Collection<V> poolValues = pool.values();
        if (poolValues != null) {
          values.addAll(poolValues);
        }
      }
    }

    return values;
  }

  public void clear() {
    synchronized (pools) {
      for (Pool<V> pool : pools.values()) {
        pool.clear();
      }

      pools.clear();
    }
  }

  public interface PoolResourceSupplier<R> {
     R get() throws IOException;
  }

  protected static <V> V createResource(PoolResourceSupplier<V> supplier) throws IOException {
    V resource = supplier.get();
    return Objects.requireNonNull(resource, "resource cannot be null.");
  }

  protected interface Pool<R> {
    R getOrCreate(PoolResourceSupplier<R> supplier) throws IOException;

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
    default:
      return new RoundRobinPool<>(poolMaxSize);
    }
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
    public R getOrCreate(PoolResourceSupplier<R> supplier) throws IOException {
      int size = resources.size();
      R resource;

      /* letting pool to grow */
      if (size < maxSize) {
        resource = createResource(supplier);
        resources.add(resource);
      } else {
        resource = resources.get(nextIndex);

        /* at this point size cannot be 0 */
        nextIndex = (nextIndex + 1) % size;
      }

      return resource;
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
   * to the thread from which it is accessed. It doesn't remove resources when a thread exits,
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
    public R getOrCreate(PoolResourceSupplier<R> supplier) throws IOException {
      Thread myself = Thread.currentThread();
      R resource = resources.get(myself);

      if (resource == null) {
        resource = createResource(supplier);
        resources.put(myself, resource);
      }

      return resource;
    }

    @Override
    public boolean remove(R resource) {
      /* remove can be called from any thread */
      return resources.values().remove(resource);
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
