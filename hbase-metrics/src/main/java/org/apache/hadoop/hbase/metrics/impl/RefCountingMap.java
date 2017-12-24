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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.metrics.impl;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * A map of K to V, but does ref counting for added and removed values. The values are
 * not added directly, but instead requested from the given Supplier if ref count == 0. Each put()
 * call will increment the ref count, and each remove() will decrement it. The values are removed
 * from the map iff ref count == 0.
 */
@InterfaceAudience.Private
class RefCountingMap<K, V> {

  private ConcurrentHashMap<K, Payload<V>> map = new ConcurrentHashMap<>();
  private static class Payload<V> {
    V v;
    int refCount;
    Payload(V v) {
      this.v = v;
      this.refCount = 1; // create with ref count = 1
    }
  }

  V put(K k, Supplier<V> supplier) {
    return ((Payload<V>)map.compute(k, (k1, oldValue) -> {
      if (oldValue != null) {
        oldValue.refCount++;
        return oldValue;
      } else {
        return new Payload(supplier.get());
      }
    })).v;
  }

  V get(K k) {
    Payload<V> p = map.get(k);
    return p == null ? null : p.v;
  }

  /**
   * Decrements the ref count of k, and removes from map if ref count == 0.
   * @param k the key to remove
   * @return the value associated with the specified key or null if key is removed from map.
   */
  V remove(K k) {
    Payload<V> p = map.computeIfPresent(k, (k1, v) -> --v.refCount <= 0 ? null : v);
    return p == null ? null : p.v;
  }

  void clear() {
    map.clear();
  }

  Set<K> keySet() {
    return map.keySet();
  }

  Collection<V> values() {
    return map.values().stream().map(v -> v.v).collect(Collectors.toList());
  }

  int size() {
    return map.size();
  }
}
