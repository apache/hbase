/**
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
package org.apache.hadoop.hbase.zookeeper.lock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HLock.MetadataHandler;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.DaemonThreadFactory;
import org.apache.hadoop.hbase.util.MultiThreadedTestUtils;
import org.apache.hadoop.hbase.util.RuntimeExceptionAbortStrategy;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Category(MediumTests.class)
public class TestHReadWriteLockImpl {

  private static final Log LOG =
      LogFactory.getLog(TestHReadWriteLockImpl.class);

  private static final HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();

  private static final int NUM_THREADS = 10;

  private static Configuration conf;

  private final AtomicBoolean isLockHeld = new AtomicBoolean(false);
  private final ExecutorService executor =
      Executors.newFixedThreadPool(NUM_THREADS,
          new DaemonThreadFactory("TestDistributedReadWriteLock-"));

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    TEST_UTIL.startMiniZKCluster();
    conf.setInt(HConstants.ZOOKEEPER_SESSION_TIMEOUT, 1000);
  }

  @AfterClass
  public static void afterAllTests() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
  }

  @After
  public void tearDown() {
    executor.shutdown();
  }

  private static ZooKeeperWrapper getZooKeeperWrapper(String desc)
  throws IOException {
    return ZooKeeperWrapper.createInstance(conf, desc,
        new RuntimeExceptionAbortStrategy());
  }


  @Test(timeout = 30000)
  public void testWriteLockExcludesWriters() throws Exception {
    final String testName = "testWriteLockExcludesWriters";
    final HReadWriteLockImpl readWriteLock =
        getReadWriteLock(testName);
    List<Future<Void>> results = Lists.newArrayList();
    for (int i = 0; i < NUM_THREADS; ++i) {
      final String threadDesc = testName + i;
      results.add(executor.submit(new Callable<Void>() {
        @Override
        public Void call() throws IOException {
          HWriteLockImpl writeLock =
              readWriteLock.writeLock(Bytes.toBytes(threadDesc));
          try {
            writeLock.acquire();
            try {
              // No one else should hold the lock
              assertTrue(isLockHeld.compareAndSet(false, true));
              Thread.sleep(1000);
              // No one else should have released the lock
              assertTrue(isLockHeld.compareAndSet(true, false));
            } finally {
              isLockHeld.set(false);
              writeLock.release();
            }
          } catch (InterruptedException e) {
            LOG.warn(threadDesc + " interrupted", e);
            Thread.currentThread().interrupt();
            throw new InterruptedIOException();
          }
          return null;
        }
      }));

    }
    MultiThreadedTestUtils.assertOnFutures(results);
  }

  @Test(timeout = 30000)
  public void testReadLockDoesNotExcludeReaders() throws Exception {
    final String testName = "testReadLockDoesNotExcludeReaders";
    final HReadWriteLockImpl readWriteLock =
        getReadWriteLock(testName);
    final CountDownLatch locksAcquiredLatch = new CountDownLatch(NUM_THREADS);
    final AtomicInteger locksHeld = new AtomicInteger(0);
    List<Future<Void>> results = Lists.newArrayList();
    for (int i = 0; i < NUM_THREADS; ++i) {
      final String threadDesc = testName + i;
      results.add(executor.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          HReadLockImpl readLock =
              readWriteLock.readLock(Bytes.toBytes(threadDesc));
          readLock.acquire();
          try {
            locksHeld.incrementAndGet();
            locksAcquiredLatch.countDown();
            Thread.sleep(1000);
          } finally {
            readLock.release();
            locksHeld.decrementAndGet();
          }
          return null;
        }
      }));
    }
    locksAcquiredLatch.await();
    assertEquals(locksHeld.get(), NUM_THREADS);
    MultiThreadedTestUtils.assertOnFutures(results);
  }

  @Test(timeout = 3000)
  public void testReadLockExcludesWriters() throws Exception {
    // Submit a read lock request first
    // Submit a write lock request second
    final String testName = "testReadLockExcludesWriters";
    List<Future<Void>> results = Lists.newArrayList();
    final CountDownLatch readLockAcquiredLatch = new CountDownLatch(1);
    Callable<Void> acquireReadLock = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        final String threadDesc = testName + "-acquireReadLock";
        HReadLockImpl readLock =
            getReadWriteLock(testName).readLock(Bytes.toBytes(threadDesc));
        readLock.acquire();
        try {
          assertTrue(isLockHeld.compareAndSet(false, true));
          readLockAcquiredLatch.countDown();
          Thread.sleep(1000);
        } finally {
          isLockHeld.set(false);
          readLock.release();
        }
        return null;
      }
    };
    Callable<Void> acquireWriteLock = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        final String threadDesc = testName + "-acquireWriteLock";
        HWriteLockImpl writeLock =
            getReadWriteLock(testName).writeLock(Bytes.toBytes(threadDesc));
        readLockAcquiredLatch.await();
        assertTrue(isLockHeld.get());
        writeLock.acquire();
        try {
          assertFalse(isLockHeld.get());
        } finally {
          writeLock.release();
        }
        return null;
      }
    };
    results.add(executor.submit(acquireReadLock));
    results.add(executor.submit(acquireWriteLock));
    MultiThreadedTestUtils.assertOnFutures(results);
  }

  private static HReadWriteLockImpl getReadWriteLock(String testName)
      throws IOException {
    MetadataHandler handler = new MetadataHandler() {
      @Override
      public void handleMetadata(byte[] ownerMetadata) {
        LOG.info("Lock info: " + Bytes.toString(ownerMetadata));
      }
    };
    return new HReadWriteLockImpl(getZooKeeperWrapper(testName), testName,
        handler);
  }

  @Test(timeout = 30000)
  public void testWriteLockExcludesReaders() throws Exception {
    // Submit a read lock request first
    // Submit a write lock request second
    final String testName = "testReadLockExcludesWriters";
    List<Future<Void>> results = Lists.newArrayList();
    final CountDownLatch writeLockAcquiredLatch = new CountDownLatch(1);
    Callable<Void> acquireWriteLock = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        final String threadDesc = testName + "-acquireWriteLock";
        HWriteLockImpl writeLock =
            getReadWriteLock(testName).writeLock(Bytes.toBytes(threadDesc));
        writeLock.acquire();
        try {
          writeLockAcquiredLatch.countDown();
          assertTrue(isLockHeld.compareAndSet(false, true));
          Thread.sleep(1000);
        } finally {
          isLockHeld.set(false);
          writeLock.release();
        }
        return null;
      }
    };
    Callable<Void> acquireReadLock = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        final String threadDesc = testName + "-acquireReadLock";
        HReadLockImpl readLock =
            getReadWriteLock(testName).readLock(Bytes.toBytes(threadDesc));
        writeLockAcquiredLatch.await();
        readLock.acquire();
        try {
          assertFalse(isLockHeld.get());
        } finally {
          readLock.release();
        }
        return null;
      }
    };
    results.add(executor.submit(acquireWriteLock));
    results.add(executor.submit(acquireReadLock));
    MultiThreadedTestUtils.assertOnFutures(results);
  }

  @Test(timeout = 60000)
  public void testTimeout() throws Exception {
    final String testName = "testTimeout";
    final CountDownLatch lockAcquiredLatch = new CountDownLatch(1);
    Callable<Void> shouldHog = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        final String threadDesc = testName + "-shouldHog";
        HWriteLockImpl lock =
            getReadWriteLock(testName).writeLock(Bytes.toBytes(threadDesc));
        lock.acquire();
        lockAcquiredLatch.countDown();
        Thread.sleep(10000);
        lock.release();
        return null;
      }
    };
    Callable<Void> shouldTimeout = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        final String threadDesc = testName + "-shouldTimeout";
        HWriteLockImpl lock =
            getReadWriteLock(testName).writeLock(Bytes.toBytes(threadDesc));
        lockAcquiredLatch.await();
        assertFalse(lock.tryAcquire(5000));
        return null;
      }
    };
    Callable<Void> shouldAcquireLock = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        final String threadDesc = testName + "-shouldAcquireLock";
        HWriteLockImpl lock =
            getReadWriteLock(testName).writeLock(Bytes.toBytes(threadDesc));
        lockAcquiredLatch.await();
        assertTrue(lock.tryAcquire(30000));
        lock.release();
        return null;
      }
    };
    List<Future<Void>> results = Lists.newArrayList();
    results.add(executor.submit(shouldHog));
    results.add(executor.submit(shouldTimeout));
    results.add(executor.submit(shouldAcquireLock));
    MultiThreadedTestUtils.assertOnFutures(results);
  }
}
