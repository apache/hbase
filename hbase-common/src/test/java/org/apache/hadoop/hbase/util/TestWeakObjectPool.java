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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, SmallTests.class})
public class TestWeakObjectPool {
  WeakObjectPool<String, Object> pool;

  @Before
  public void setUp() {
    pool = new WeakObjectPool<String, Object>(
        new WeakObjectPool.ObjectFactory<String, Object>() {
          @Override
          public Object createObject(String key) {
            return new Object();
          }
        });
  }

  @Test
  public void testKeys() {
    Object obj1 = pool.get("a");
    Object obj2 = pool.get(new String("a"));

    Assert.assertSame(obj1, obj2);

    Object obj3 = pool.get("b");

    Assert.assertNotSame(obj1, obj3);
  }

  @Test
  public void testWeakReference() throws Exception {
    Object obj1 = pool.get("a");
    int hash1 = System.identityHashCode(obj1);

    System.gc();
    System.gc();
    System.gc();

    Thread.sleep(10);
    // Sleep a while because references newly becoming stale
    // may still remain when calling the {@code purge} method.
    pool.purge();
    Assert.assertEquals(1, pool.size());

    Object obj2 = pool.get("a");
    Assert.assertSame(obj1, obj2);

    obj1 = null;
    obj2 = null;

    System.gc();
    System.gc();
    System.gc();

    Thread.sleep(10);
    pool.purge();
    Assert.assertEquals(0, pool.size());

    Object obj3 = pool.get("a");
    Assert.assertNotEquals(hash1, System.identityHashCode(obj3));
  }

  @Test(timeout=1000)
  public void testCongestion() throws Exception {
    final int THREAD_COUNT = 100;

    final AtomicBoolean assertionFailed = new AtomicBoolean();
    final AtomicReference<Object> expectedObjRef = new AtomicReference<Object>();
    final CountDownLatch prepareLatch = new CountDownLatch(THREAD_COUNT);
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch endLatch = new CountDownLatch(THREAD_COUNT);

    for (int i=0; i<THREAD_COUNT; i++) {
      new Thread() {
        @Override
        public void run() {
          prepareLatch.countDown();
          try {
            startLatch.await();

            Object obj = pool.get("a");
            if (! expectedObjRef.compareAndSet(null, obj)) {
              if (expectedObjRef.get() != obj) {
                assertionFailed.set(true);
              }
            }
          } catch (Exception e) {
            assertionFailed.set(true);

          } finally {
            endLatch.countDown();
          }
        }
      }.start();
    }

    prepareLatch.await();
    startLatch.countDown();
    endLatch.await();

    if (assertionFailed.get()) {
      Assert.fail();
    }
  }
}
