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

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Utility methods for dealing with Collections, including treating null collections as empty.
 */
@InterfaceAudience.Private
public class ConcurrentMapUtils {

  /**
   * In HBASE-16648 we found that ConcurrentHashMap.get is much faster than computeIfAbsent if the
   * value already exists. Notice that the implementation does not guarantee that the supplier will
   * only be executed once.
   */
  public static <K, V> V computeIfAbsent(ConcurrentMap<K, V> map, K key, Supplier<V> supplier) {
    return computeIfAbsent(map, key, supplier, () -> {
    });
  }

  /**
   * A supplier that throws IOException when get.
   */
  @FunctionalInterface
  public interface IOExceptionSupplier<V> {
    V get() throws IOException;
  }

  /**
   * In HBASE-16648 we found that ConcurrentHashMap.get is much faster than computeIfAbsent if the
   * value already exists. So here we copy the implementation of
   * {@link ConcurrentMap#computeIfAbsent(Object, java.util.function.Function)}. It uses get and
   * putIfAbsent to implement computeIfAbsent. And notice that the implementation does not guarantee
   * that the supplier will only be executed once.
   */
  public static <K, V> V computeIfAbsentEx(ConcurrentMap<K, V> map, K key,
      IOExceptionSupplier<V> supplier) throws IOException {
    V v, newValue;
    return ((v = map.get(key)) == null && (newValue = supplier.get()) != null
        && (v = map.putIfAbsent(key, newValue)) == null) ? newValue : v;
  }

  public static <K, V> V computeIfAbsent(ConcurrentMap<K, V> map, K key, Supplier<V> supplier,
      Runnable actionIfAbsent) {
    V v = map.get(key);
    if (v != null) {
      return v;
    }
    V newValue = supplier.get();
    v = map.putIfAbsent(key, newValue);
    if (v != null) {
      return v;
    }
    actionIfAbsent.run();
    return newValue;
  }
}
