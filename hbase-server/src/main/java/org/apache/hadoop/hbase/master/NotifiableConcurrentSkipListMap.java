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

package org.apache.hadoop.hbase.master;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * <p>Encapsulate a ConcurrentSkipListMap to ensure that notifications are sent when
 *  the list is modified. Offers only the functions used by the AssignementManager, hence
 *  does not extends ConcurrentSkipListMap.</p>
 *
 * <p>Used only in master package (main & test), so it's package protected.</p>
 *
 * @param <K> - class for the keys
 * @param <V> - class for the values
 */
class NotifiableConcurrentSkipListMap<K, V> {
  private final ConcurrentSkipListMap<K, V> delegatee = new ConcurrentSkipListMap<K, V>();

  public boolean isEmpty() {
    return delegatee.isEmpty();
  }

  public int size() {
    return delegatee.size();
  }

  public void put(K k, V v) {
    synchronized (delegatee) {
      delegatee.put(k, v);
      delegatee.notifyAll();
    }
  }

  public V remove(K k) {
    synchronized (delegatee) {
      V v = delegatee.remove(k);
      if (v != null) {
        delegatee.notifyAll();
      }
      return v;
    }
  }

  public void waitForUpdate(long timeout) throws InterruptedException {
    synchronized (delegatee){
      delegatee.wait(timeout);
    }
  }

  public boolean containsKey(K k) {
    return delegatee.containsKey(k);
  }

  public Collection<?> keySet() {
    return delegatee.keySet();
  }

  public V get(K k) {
    return delegatee.get(k);
  }

  public NavigableMap<K, V> copyMap() {
    return delegatee.clone();
  }

  public Collection<V> copyValues() {
    Collection<V> values = new ArrayList<V>(size());
    synchronized (delegatee) {
      values.addAll(delegatee.values());
    }
    return values;
  }

  public Set<Map.Entry<K, V>> copyEntrySet() {
    Set<Map.Entry<K, V>> entrySet = new TreeSet<Map.Entry<K, V>>();
    synchronized (delegatee) {
      Iterator<Map.Entry<K, V>> it = delegatee.entrySet().iterator();
      while (it.hasNext()) {
        entrySet.add(it.next());
      }
    }
    return entrySet;
  }

  public void waitForUpdate() throws InterruptedException {
    synchronized (delegatee) {
      delegatee.wait();
    }
  }

  public void clear() {
    if (!delegatee.isEmpty()) {
      synchronized (delegatee) {
        delegatee.clear();
        delegatee.notifyAll();
      }
    }
  }
}
