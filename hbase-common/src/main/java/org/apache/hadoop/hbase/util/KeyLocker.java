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

package org.apache.hadoop.hbase.util;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * A utility class to manage a set of locks. Each lock is identified by a String which serves
 * as a key. Typical usage is: <pre>
 * class Example {
 *   private final static KeyLocker&lt;String&gt; locker = new Locker&lt;String&gt;();
 *   public void foo(String s){
 *     Lock lock = locker.acquireLock(s);
 *     try {
 *       // whatever
 *     }finally{
 *       lock.unlock();
 *     }
 *   }
 * }
 * </pre>
 */
@InterfaceAudience.Private
public class KeyLocker<K> {
  // The number of lock we want to easily support. It's not a maximum.
  private static final int NB_CONCURRENT_LOCKS = 1000;

  private final WeakObjectPool<K, ReentrantLock> lockPool =
      new WeakObjectPool<>(
          new ObjectPool.ObjectFactory<K, ReentrantLock>() {
            @Override
            public ReentrantLock createObject(K key) {
              return new ReentrantLock();
            }
          },
          NB_CONCURRENT_LOCKS);

  /**
   * Return a lock for the given key. The lock is already locked.
   *
   * @param key
   */
  public ReentrantLock acquireLock(K key) {
    if (key == null) throw new IllegalArgumentException("key must not be null");

    lockPool.purge();
    ReentrantLock lock = lockPool.get(key);

    lock.lock();
    return lock;
  }

  /**
   * Acquire locks for a set of keys. The keys will be
   * sorted internally to avoid possible deadlock.
   *
   * @throws ClassCastException if the given {@code keys}
   *    contains elements that are not mutually comparable
   */
  public Map<K, Lock> acquireLocks(Set<? extends K> keys) {
    Object[] keyArray = keys.toArray();
    Arrays.sort(keyArray);

    lockPool.purge();
    Map<K, Lock> locks = new LinkedHashMap<>(keyArray.length);
    for (Object o : keyArray) {
      @SuppressWarnings("unchecked")
      K key = (K)o;
      ReentrantLock lock = lockPool.get(key);
      locks.put(key, lock);
    }

    for (Lock lock : locks.values()) {
      lock.lock();
    }
    return locks;
  }
}
