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


import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * A utility class to manage a set of locks. Each lock is identified by a String which serves
 * as a key. Typical usage is: <p>
 * class Example{
 * private final static KeyLocker&lt;String&gt; locker = new Locker&lt;String&gt;();
 * </p>
 * <p>
 * public void foo(String s){
 * Lock lock = locker.acquireLock(s);
 * try {
 * // whatever
 * }finally{
 * lock.unlock();
 * }
 * }
 * }
 * </p>
 */
@InterfaceAudience.Private
public class KeyLocker<K extends Comparable<? super K>> {
  private static final Log LOG = LogFactory.getLog(KeyLocker.class);

  // The number of lock we want to easily support. It's not a maximum.
  private static final int NB_CONCURRENT_LOCKS = 1000;

  // We need an atomic counter to manage the number of users using the lock and free it when
  //  it's equal to zero.
  private final Map<K, Pair<KeyLock<K>, AtomicInteger>> locks =
    new HashMap<K, Pair<KeyLock<K>, AtomicInteger>>(NB_CONCURRENT_LOCKS);

  /**
   * Return a lock for the given key. The lock is already locked.
   *
   * @param key
   */
  public ReentrantLock acquireLock(K key) {
    if (key == null) throw new IllegalArgumentException("key must not be null");

    Pair<KeyLock<K>, AtomicInteger> lock;
    synchronized (this) {
      lock = locks.get(key);
      if (lock == null) {
        lock = new Pair<KeyLock<K>, AtomicInteger>(
          new KeyLock<K>(this, key), new AtomicInteger(1));
        locks.put(key, lock);
      } else {
        lock.getSecond().incrementAndGet();
      }
    }
    lock.getFirst().lock();
    return lock.getFirst();
  }

  /**
   * Acquire locks for a set of keys. The keys will be
   * sorted internally to avoid possible deadlock.
   */
  public Map<K, Lock> acquireLocks(final Set<K> keys) {
    Map<K, Lock> locks = new HashMap<K, Lock>(keys.size());
    SortedSet<K> sortedKeys = new TreeSet<K>(keys);
    for (K key : sortedKeys) {
      locks.put(key, acquireLock(key));
    }
    return locks;
  }

  /**
   * Free the lock for the given key.
   */
  private synchronized void releaseLock(K key) {
    Pair<KeyLock<K>, AtomicInteger> lock = locks.get(key);
    if (lock != null) {
      if (lock.getSecond().decrementAndGet() == 0) {
        locks.remove(key);
      }
    } else {
      String message = "Can't release the lock for " + key+", this key is not in the key list." +
        " known keys are: "+ locks.keySet();
      LOG.error(message);
      throw new RuntimeException(message);
    }
  }

  static class KeyLock<K extends Comparable<? super K>> extends ReentrantLock {
    private static final long serialVersionUID = -12432857283423584L;

    private final transient KeyLocker<K> locker;
    private final K lockId;

    private KeyLock(KeyLocker<K> locker, K lockId) {
      super();
      this.locker = locker;
      this.lockId = lockId;
    }

    @Override
    public void unlock() {
      super.unlock();
      locker.releaseLock(lockId);
    }
  }
}
