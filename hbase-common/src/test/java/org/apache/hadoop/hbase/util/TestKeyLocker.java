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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MiscTests.TAG)
@Tag(SmallTests.TAG)
public class TestKeyLocker {

  @Test
  public void testLocker() {
    KeyLocker<String> locker = new KeyLocker<>();
    ReentrantLock lock1 = locker.acquireLock("l1");
    assertTrue(lock1.isHeldByCurrentThread());

    ReentrantLock lock2 = locker.acquireLock("l2");
    assertTrue(lock2.isHeldByCurrentThread());
    assertTrue(lock1 != lock2);

    // same key = same lock
    ReentrantLock lock20 = locker.acquireLock("l2");
    assertTrue(lock20 == lock2);
    assertTrue(lock2.isHeldByCurrentThread());
    assertTrue(lock20.isHeldByCurrentThread());

    // Locks are still reentrant; so with 2 acquires we want two unlocks
    lock20.unlock();
    assertTrue(lock20.isHeldByCurrentThread());

    lock2.unlock();
    assertFalse(lock20.isHeldByCurrentThread());

    // The lock object will be garbage-collected
    // if you free its reference for a long time,
    // and you will get a new one at the next time.
    int lock2Hash = System.identityHashCode(lock2);
    lock2 = null;
    lock20 = null;

    System.gc();
    System.gc();
    System.gc();

    ReentrantLock lock200 = locker.acquireLock("l2");
    assertNotEquals(lock2Hash, System.identityHashCode(lock200));
    lock200.unlock();
    assertFalse(lock200.isHeldByCurrentThread());

    // first lock is still there
    assertTrue(lock1.isHeldByCurrentThread());
    lock1.unlock();
    assertFalse(lock1.isHeldByCurrentThread());
  }
}
