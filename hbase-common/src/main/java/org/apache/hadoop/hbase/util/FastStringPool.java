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

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * A string pool like {@link String#intern()}, but more flexible as we can create multiple instances
 * and use them in difference places, where {@link String#intern()} is global.
 * <p>
 * We use {@link WeakReference} so when there are no actual reference to the String, it will be GCed
 * to reduce memory pressure.
 * <p>
 * The difference between {@link WeakObjectPool} is that, we also need to use {@link WeakReference}
 * as key, not only value, because the key(a String) is exactly what we want to deduplicate.
 */
@InterfaceAudience.Private
public class FastStringPool {

  private static final class WeakKey extends WeakReference<String> {

    private final int hash;

    WeakKey(String referent, ReferenceQueue<String> queue) {
      super(Preconditions.checkNotNull(referent), queue);
      // must calculate it here, as later the referent may be GCed
      this.hash = referent.hashCode();
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof WeakKey)) {
        return false;
      }

      String a = this.get();
      String b = ((WeakKey) obj).get();
      // In ConcurrentHashMap, we will always compare references(like entry.key == key) before
      // calling actual equals method, so this will not cause problems for clean up. And in normal
      // intern path, the reference will never be null, so there is no problem too.
      if (a == null || b == null) {
        return false;
      }
      return a.equals(b);
    }
  }

  private final ConcurrentHashMap<WeakKey, WeakReference<String>> map = new ConcurrentHashMap<>();

  private final ReferenceQueue<String> refQueue = new ReferenceQueue<>();

  private final Lock cleanupLock = new ReentrantLock();

  // only call cleanup every 256 times
  private static final int CLEANUP_MASK = 0xFF;
  private final AtomicInteger counter = new AtomicInteger();

  public String intern(String s) {
    Preconditions.checkNotNull(s);
    maybeCleanup();

    WeakKey lookupKey = new WeakKey(s, null);
    WeakReference<String> ref = map.get(lookupKey);
    if (ref != null) {
      String v = ref.get();
      if (v != null) {
        return v;
      }
    }

    WeakKey storeKey = new WeakKey(s, refQueue);
    WeakReference<String> storeVal = new WeakReference<>(s);
    // Used to store the return value. The return value of compute method is a WeakReference, the
    // value of the WeakReference may be GCed just before we get it for returning.
    MutableObject<String> ret = new MutableObject<>();

    map.compute(storeKey, (k, prevVal) -> {
      if (prevVal == null) {
        ret.setValue(s);
        return storeVal;
      } else {
        String prevRef = prevVal.get();
        if (prevRef != null) {
          ret.setValue(prevRef);
          return prevVal;
        } else {
          ret.setValue(s);
          return storeVal;
        }
      }
    });
    assert ret.get() != null;
    return ret.get();
  }

  private void cleanup() {
    if (!cleanupLock.tryLock()) {
      // a cleanup task is ongoing, give up
      return;
    }
    try {
      for (;;) {
        WeakKey k = (WeakKey) refQueue.poll();
        if (k == null) {
          return;
        }
        map.remove(k);
      }
    } finally {
      cleanupLock.unlock();
    }
  }

  private void maybeCleanup() {
    if ((counter.incrementAndGet() & CLEANUP_MASK) != 0) {
      return;
    }
    cleanup();
  }

  public int size() {
    // size method is not on critical path, so always call cleanup here to reduce memory pressure
    cleanup();
    return map.size();
  }
}
