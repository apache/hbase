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

import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, SmallTests.class})
public class TestKeyLocker {
  @Test
  public void testLocker(){
    KeyLocker<String> locker = new KeyLocker();
    ReentrantLock lock1 = locker.acquireLock("l1");
    Assert.assertTrue(lock1.isHeldByCurrentThread());

    ReentrantLock lock2 = locker.acquireLock("l2");
    Assert.assertTrue(lock2.isHeldByCurrentThread());
    Assert.assertTrue(lock1 != lock2);

    // same key = same lock
    ReentrantLock lock20 = locker.acquireLock("l2");
    Assert.assertTrue(lock20 == lock2);
    Assert.assertTrue(lock2.isHeldByCurrentThread());
    Assert.assertTrue(lock20.isHeldByCurrentThread());

    // Locks are still reentrant; so with 2 acquires we want two unlocks
    lock20.unlock();
    Assert.assertTrue(lock20.isHeldByCurrentThread());

    lock2.unlock();
    Assert.assertFalse(lock20.isHeldByCurrentThread());

    // The lock object was freed once useless, so we're recreating a new one
    ReentrantLock lock200 = locker.acquireLock("l2");
    Assert.assertTrue(lock2 != lock200);
    lock200.unlock();
    Assert.assertFalse(lock200.isHeldByCurrentThread());

    // first lock is still there
    Assert.assertTrue(lock1.isHeldByCurrentThread());
    lock1.unlock();
    Assert.assertFalse(lock1.isHeldByCurrentThread());
  }
}
