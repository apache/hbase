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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Utility methods for dealing with Collections, including treating null collections as empty.
 */
@InterfaceAudience.Private
public class CollectionUtils {

  private static final List<Object> EMPTY_LIST = Collections.unmodifiableList(new ArrayList<>(0));

  
  @SuppressWarnings("unchecked")
  public static <T> Collection<T> nullSafe(Collection<T> in) {
    if (in == null) {
      return (Collection<T>)EMPTY_LIST;
    }
    return in;
  }

  /************************ size ************************************/

  public static <T> int nullSafeSize(Collection<T> collection) {
    if (collection == null) {
      return 0;
    }
    return collection.size();
  }

  public static <A, B> boolean nullSafeSameSize(Collection<A> a, Collection<B> b) {
    return nullSafeSize(a) == nullSafeSize(b);
  }

  /*************************** empty ****************************************/

  public static <T> boolean isEmpty(Collection<T> collection) {
    return collection == null || collection.isEmpty();
  }

  public static <T> boolean notEmpty(Collection<T> collection) {
    return !isEmpty(collection);
  }

  /************************ first/last **************************/

  public static <T> T getFirst(Collection<T> collection) {
    if (CollectionUtils.isEmpty(collection)) {
      return null;
    }
    for (T t : collection) {
      return t;
    }
    return null;
  }
  
  /**
   * @param list any list
   * @return -1 if list is empty, otherwise the max index
   */
  public static int getLastIndex(List<?> list){
    if(isEmpty(list)){
      return -1;
    }
    return list.size() - 1;
  }
  
  /**
   * @param list
   * @param index the index in question
   * @return true if it is the last index or if list is empty and -1 is passed for the index param
   */
  public static boolean isLastIndex(List<?> list, int index){
    return index == getLastIndex(list);
  }

  public static <T> T getLast(List<T> list) {
    if (isEmpty(list)) {
      return null;
    }
    return list.get(list.size() - 1);
  }

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
