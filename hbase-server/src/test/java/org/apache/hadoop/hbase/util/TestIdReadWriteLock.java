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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.IdReadWriteLock.ReferenceType;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
@Category({MiscTests.class, MediumTests.class})
// Medium as it creates 100 threads; seems better to run it isolated
public class TestIdReadWriteLock {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestIdReadWriteLock.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestIdReadWriteLock.class);

  private static final int NUM_IDS = 16;
  private static final int NUM_THREADS = 128;
  private static final int NUM_SECONDS = 15;

  @Parameterized.Parameter
  public IdReadWriteLock<Long> idLock;

  @Parameterized.Parameters
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][] { { new IdReadWriteLock<Long>(ReferenceType.WEAK) },
      { new IdReadWriteLock<Long>(ReferenceType.SOFT) } });
  }

  private Map<Long, String> idOwner = new ConcurrentHashMap<>();

  private class IdLockTestThread implements Callable<Boolean> {

    private String clientId;

    public IdLockTestThread(String clientId) {
      this.clientId = clientId;
    }

    @Override
    public Boolean call() throws Exception {
      Thread.currentThread().setName(clientId);
      Random rand = ThreadLocalRandom.current();
      long endTime = EnvironmentEdgeManager.currentTime() + NUM_SECONDS * 1000;
      while (EnvironmentEdgeManager.currentTime() < endTime) {
        long id = rand.nextInt(NUM_IDS);
        boolean readLock = rand.nextBoolean();

        ReentrantReadWriteLock readWriteLock = idLock.getLock(id);
        Lock lock = readLock ? readWriteLock.readLock() : readWriteLock.writeLock();
        try {
          lock.lock();
          int sleepMs = 1 + rand.nextInt(4);
          String owner = idOwner.get(id);
          if (owner != null && LOG.isDebugEnabled()) {
            LOG.debug((readLock ? "Read" : "Write") + "lock of Id " + id + " already taken by "
                + owner + ", we are " + clientId);
          }

          idOwner.put(id, clientId);
          Thread.sleep(sleepMs);
          idOwner.remove(id);

        } finally {
          lock.unlock();
          if (LOG.isDebugEnabled()) {
            LOG.debug("Release " + (readLock ? "Read" : "Write") + " lock of Id" + id + ", we are "
                + clientId);
          }
        }
      }
      return true;
    }

  }

  @Test
  public void testMultipleClients() throws Exception {
    ExecutorService exec = Executors.newFixedThreadPool(NUM_THREADS);
    try {
      ExecutorCompletionService<Boolean> ecs = new ExecutorCompletionService<>(exec);
      for (int i = 0; i < NUM_THREADS; ++i)
        ecs.submit(new IdLockTestThread("client_" + i));
      for (int i = 0; i < NUM_THREADS; ++i) {
        Future<Boolean> result = ecs.take();
        assertTrue(result.get());
      }
      int entryPoolSize = idLock.purgeAndGetEntryPoolSize();
      LOG.debug("Size of entry pool after gc and purge: " + entryPoolSize);
      ReferenceType refType = idLock.getReferenceType();
      switch (refType) {
      case WEAK:
        // make sure the entry pool will be cleared after GC and purge call
        assertEquals(0, entryPoolSize);
        break;
      case SOFT:
        // make sure the entry pool won't be cleared when JVM memory is enough
        // even after GC and purge call
        assertEquals(NUM_IDS, entryPoolSize);
        break;
      default:
        break;
      }
    } finally {
      exec.shutdown();
      exec.awaitTermination(5000, TimeUnit.MILLISECONDS);
    }
  }


}

