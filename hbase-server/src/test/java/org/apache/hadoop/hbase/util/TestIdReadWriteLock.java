/*
 *
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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, MediumTests.class})
// Medium as it creates 100 threads; seems better to run it isolated
public class TestIdReadWriteLock {

  private static final Log LOG = LogFactory.getLog(TestIdReadWriteLock.class);

  private static final int NUM_IDS = 16;
  private static final int NUM_THREADS = 128;
  private static final int NUM_SECONDS = 15;

  private IdReadWriteLock idLock = new IdReadWriteLock();

  private Map<Long, String> idOwner = new ConcurrentHashMap<Long, String>();

  private class IdLockTestThread implements Callable<Boolean> {

    private String clientId;

    public IdLockTestThread(String clientId) {
      this.clientId = clientId;
    }

    @Override
    public Boolean call() throws Exception {
      Thread.currentThread().setName(clientId);
      Random rand = new Random();
      long endTime = System.currentTimeMillis() + NUM_SECONDS * 1000;
      while (System.currentTimeMillis() < endTime) {
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

  @Test(timeout = 60000)
  public void testMultipleClients() throws Exception {
    ExecutorService exec = Executors.newFixedThreadPool(NUM_THREADS);
    try {
      ExecutorCompletionService<Boolean> ecs =
          new ExecutorCompletionService<Boolean>(exec);
      for (int i = 0; i < NUM_THREADS; ++i)
        ecs.submit(new IdLockTestThread("client_" + i));
      for (int i = 0; i < NUM_THREADS; ++i) {
        Future<Boolean> result = ecs.take();
        assertTrue(result.get());
      }
      // make sure the entry pool will be cleared after GC and purge call
      int entryPoolSize = idLock.purgeAndGetEntryPoolSize();
      LOG.debug("Size of entry pool after gc and purge: " + entryPoolSize);
      assertEquals(0, entryPoolSize);
    } finally {
      exec.shutdown();
      exec.awaitTermination(5000, TimeUnit.MILLISECONDS);
    }
  }


}

