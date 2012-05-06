/*
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.zookeeper;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RuntimeExceptionAbortStrategy;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;


public class TestDistributedLock {

  private static final Log LOG = LogFactory.getLog(TestDistributedLock.class);

  private static final HBaseTestingUtility TEST_UTIL =
    new HBaseTestingUtility();

  private static final int NUM_THREADS = 10;

  private static Configuration conf;

  private final CountDownLatch lockHeldLatch = new CountDownLatch(1);

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    TEST_UTIL.startMiniZKCluster();
    conf.setInt(HConstants.ZOOKEEPER_SESSION_TIMEOUT, 1000);
  }

  @AfterClass
  public static void afterAllTests() throws IOException {
    TEST_UTIL.shutdownMiniZKCluster();
  }

  @Test(timeout = 30000)
  public void testTimeout() throws Exception {
    Callable<Object> shouldHog = new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        ZooKeeperWrapper zkw = ZooKeeperWrapper.createInstance(conf,
          "hogThread", new RuntimeExceptionAbortStrategy());
        DistributedLock lock = new DistributedLock(zkw, "testTimeout",
          Bytes.toBytes("hogThread"), null);
        lock.acquire();
        lockHeldLatch.countDown();
        Thread.sleep(10000);
        lock.release();
        return null;
      }
    };
    Callable<Object> shouldTimeout = new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        ZooKeeperWrapper zkw = ZooKeeperWrapper.createInstance(conf,
          "threadShouldTimeout", new RuntimeExceptionAbortStrategy());
        DistributedLock lock = new DistributedLock(zkw, "testTimeout",
          Bytes.toBytes("threadShouldTimeout"), null);
        lockHeldLatch.await();
        assertFalse(lock.tryAcquire(5000));
        return null;
      }
    };
    Callable<Object> shouldAcquireLock = new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        ZooKeeperWrapper zkw = ZooKeeperWrapper.createInstance(conf,
          "threadShouldAcquireLock", new RuntimeExceptionAbortStrategy());
        DistributedLock lock = new DistributedLock(zkw, "testTimeout",
          Bytes.toBytes("threadShouldAcquireLock"), null);
        lockHeldLatch.await();
        assertTrue(lock.tryAcquire(30000));
        lock.release();
        return null;
      }
    };
    ExecutorService executor = Executors.newFixedThreadPool(3);
    List<Future<Object>> threadResults = new ArrayList<Future<Object>>();
    threadResults.add(executor.submit(shouldHog));
    threadResults.add(executor.submit(shouldAcquireLock));
    threadResults.add(executor.submit(shouldTimeout));
    executor.shutdown();
    assertOnFutures(threadResults);
  }

  @Test(timeout = 30000)
  public void testLockAndRelease() throws Exception {
    final AtomicBoolean isLockHeld = new AtomicBoolean(false);
    ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
    List<Future<Object>> threadResults = new ArrayList<Future<Object>>();
    for (int i = 0; i < NUM_THREADS; ++i) {
      final String threadDesc = "testLockAndRelease" + i;
      Future<Object> threadResult =
        executor.submit(new Callable<Object>() {
          @Override
          public Object call() throws IOException {
            try {
              ZooKeeperWrapper zkw = ZooKeeperWrapper.createInstance(conf,
                threadDesc, new RuntimeExceptionAbortStrategy());
              DistributedLock lock =
                new DistributedLock(zkw, "testLockAndRelease",
                  Bytes.toBytes(threadDesc), null);
              lock.acquire();
              try {
                // No one else should be holding the lock
                assertTrue(isLockHeld.compareAndSet(false, true));
                Thread.sleep(1000);
                // No one else should have released the lock
                assertTrue(isLockHeld.compareAndSet(true, false));
              } finally {
                isLockHeld.set(false);
                lock.release();
              }
            } catch (IOException e) {
              LOG.error("Exception acquiring lock", e);
              throw e;
            } catch (InterruptedException e) {
              LOG.warn(threadDesc + " interrupted", e);
              Thread.currentThread().interrupt();
              throw new InterruptedIOException();
            }
            return null;
          }
        });
      threadResults.add(threadResult);
    }
    assertOnFutures(threadResults);
  }

  private static void assertOnFutures(List<Future<Object>> threadResults)
    throws InterruptedException, ExecutionException {
    for (Future threadResult : threadResults) {
      try {
        threadResult.get();
      } catch (ExecutionException e) {
        if (e.getCause() instanceof AssertionError) {
          throw (AssertionError) e.getCause();
        }
        throw e;
      }
    }
  }
}
