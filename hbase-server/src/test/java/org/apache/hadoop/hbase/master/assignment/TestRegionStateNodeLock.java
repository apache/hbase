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
package org.apache.hadoop.hbase.master.assignment;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.NoopProcedure;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.AtomicUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, SmallTests.class })
public class TestRegionStateNodeLock {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionStateNodeLock.class);

  private final RegionInfo regionInfo =
    RegionInfoBuilder.newBuilder(TableName.valueOf("test")).build();

  private RegionStateNodeLock lock;

  @Before
  public void setUp() {
    lock = new RegionStateNodeLock(regionInfo);
  }

  @Test
  public void testLockByThread() {
    assertFalse(lock.isLocked());
    assertThrows(IllegalMonitorStateException.class, () -> lock.unlock());
    lock.lock();
    assertTrue(lock.isLocked());
    // reentrant
    assertTrue(lock.tryLock());
    lock.unlock();
    assertTrue(lock.isLocked());
    lock.unlock();
    assertFalse(lock.isLocked());
  }

  @Test
  public void testLockByProc() {
    NoopProcedure<?> proc = new NoopProcedure<Void>();
    assertFalse(lock.isLocked());
    assertThrows(IllegalMonitorStateException.class, () -> lock.unlock(proc));
    lock.lock(proc);
    assertTrue(lock.isLocked());
    // reentrant
    assertTrue(lock.tryLock(proc));
    lock.unlock(proc);
    assertTrue(lock.isLocked());
    lock.unlock(proc);
    assertFalse(lock.isLocked());
  }

  @Test
  public void testLockProcThenThread() {
    NoopProcedure<?> proc = new NoopProcedure<Void>();
    assertFalse(lock.isLocked());
    lock.lock(proc);
    assertFalse(lock.tryLock());
    assertThrows(IllegalMonitorStateException.class, () -> lock.unlock());
    long startNs = System.nanoTime();
    new Thread(() -> {
      Threads.sleepWithoutInterrupt(2000);
      lock.unlock(proc);
    }).start();
    lock.lock();
    long costNs = System.nanoTime() - startNs;
    assertThat(TimeUnit.NANOSECONDS.toMillis(costNs), greaterThanOrEqualTo(1800L));
    assertTrue(lock.isLocked());
    lock.unlock();
    assertFalse(lock.isLocked());
  }

  @Test
  public void testLockMultiThread() throws InterruptedException {
    int nThreads = 10;
    AtomicLong concurrency = new AtomicLong(0);
    AtomicLong maxConcurrency = new AtomicLong(0);
    Thread[] threads = new Thread[nThreads];
    for (int i = 0; i < nThreads; i++) {
      threads[i] = new Thread(() -> {
        for (int j = 0; j < 1000; j++) {
          lock.lock();
          try {
            long c = concurrency.incrementAndGet();
            AtomicUtils.updateMax(maxConcurrency, c);
            concurrency.decrementAndGet();
          } finally {
            lock.unlock();
          }
          Threads.sleepWithoutInterrupt(1);
        }
      });
    }
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    assertEquals(0, concurrency.get());
    assertEquals(1, maxConcurrency.get());
  }
}
