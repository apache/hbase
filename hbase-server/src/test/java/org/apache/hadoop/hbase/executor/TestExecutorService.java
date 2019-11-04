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
package org.apache.hadoop.hbase.executor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyObject;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.StringWriter;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.executor.ExecutorService.Executor;
import org.apache.hadoop.hbase.executor.ExecutorService.ExecutorStatus;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MiscTests.class, SmallTests.class})
public class TestExecutorService {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestExecutorService.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestExecutorService.class);

  @Test
  public void testExecutorService() throws Exception {
    int maxThreads = 5;
    int maxTries = 10;
    int sleepInterval = 10;

    Server mockedServer = mock(Server.class);
    when(mockedServer.getConfiguration()).thenReturn(HBaseConfiguration.create());

    // Start an executor service pool with max 5 threads
    ExecutorService executorService = new ExecutorService("unit_test");
    executorService.startExecutorService(
      ExecutorType.MASTER_SERVER_OPERATIONS, maxThreads);

    Executor executor =
      executorService.getExecutor(ExecutorType.MASTER_SERVER_OPERATIONS);
    ThreadPoolExecutor pool = executor.threadPoolExecutor;

    // Assert no threads yet
    assertEquals(0, pool.getPoolSize());

    AtomicBoolean lock = new AtomicBoolean(true);
    AtomicInteger counter = new AtomicInteger(0);

    // Submit maxThreads executors.
    for (int i = 0; i < maxThreads; i++) {
      executorService.submit(
        new TestEventHandler(mockedServer, EventType.M_SERVER_SHUTDOWN,
            lock, counter));
    }

    // The TestEventHandler will increment counter when it starts.
    int tries = 0;
    while (counter.get() < maxThreads && tries < maxTries) {
      LOG.info("Waiting for all event handlers to start...");
      Thread.sleep(sleepInterval);
      tries++;
    }

    // Assert that pool is at max threads.
    assertEquals(maxThreads, counter.get());
    assertEquals(maxThreads, pool.getPoolSize());

    ExecutorStatus status = executor.getStatus();
    assertTrue(status.queuedEvents.isEmpty());
    assertEquals(5, status.running.size());
    checkStatusDump(status);


    // Now interrupt the running Executor
    synchronized (lock) {
      lock.set(false);
      lock.notifyAll();
    }

    // Executor increments counter again on way out so.... test that happened.
    while (counter.get() < (maxThreads * 2) && tries < maxTries) {
      System.out.println("Waiting for all event handlers to finish...");
      Thread.sleep(sleepInterval);
      tries++;
    }

    assertEquals(maxThreads * 2, counter.get());
    assertEquals(maxThreads, pool.getPoolSize());

    // Add more than the number of threads items.
    // Make sure we don't get RejectedExecutionException.
    for (int i = 0; i < (2 * maxThreads); i++) {
      executorService.submit(
        new TestEventHandler(mockedServer, EventType.M_SERVER_SHUTDOWN,
            lock, counter));
    }
    // Now interrupt the running Executor
    synchronized (lock) {
      lock.set(false);
      lock.notifyAll();
    }

    // Make sure threads are still around even after their timetolive expires.
    Thread.sleep(ExecutorService.Executor.keepAliveTimeInMillis * 2);
    assertEquals(maxThreads, pool.getPoolSize());

    executorService.shutdown();

    assertEquals(0, executorService.getAllExecutorStatuses().size());

    // Test that submit doesn't throw NPEs
    executorService.submit(
      new TestEventHandler(mockedServer, EventType.M_SERVER_SHUTDOWN,
            lock, counter));
  }

  private void checkStatusDump(ExecutorStatus status) throws IOException {
    StringWriter sw = new StringWriter();
    status.dumpTo(sw, "");
    String dump = sw.toString();
    LOG.info("Got status dump:\n" + dump);

    assertTrue(dump.contains("Waiting on java.util.concurrent.atomic.AtomicBoolean"));
  }

  public static class TestEventHandler extends EventHandler {
    private final AtomicBoolean lock;
    private AtomicInteger counter;

    public TestEventHandler(Server server, EventType eventType,
                            AtomicBoolean lock, AtomicInteger counter) {
      super(server, eventType);
      this.lock = lock;
      this.counter = counter;
    }

    @Override
    public void process() throws IOException {
      int num = counter.incrementAndGet();
      LOG.info("Running process #" + num + ", threadName=" +
        Thread.currentThread().getName());
      synchronized (lock) {
        while (lock.get()) {
          try {
            lock.wait();
          } catch (InterruptedException e) {
            // do nothing
          }
        }
      }
      counter.incrementAndGet();
    }
  }

  @Test
  public void testAborting() throws Exception {
    final Configuration conf = HBaseConfiguration.create();
    final Server server = mock(Server.class);
    when(server.getConfiguration()).thenReturn(conf);

    ExecutorService executorService = new ExecutorService("unit_test");
    executorService.startExecutorService(
      ExecutorType.MASTER_SERVER_OPERATIONS, 1);


    executorService.submit(new EventHandler(server, EventType.M_SERVER_SHUTDOWN) {
      @Override
      public void process() throws IOException {
        throw new RuntimeException("Should cause abort");
      }
    });

    Waiter.waitFor(conf, 30000, new Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        try {
          verify(server, times(1)).abort(anyString(), (Throwable) anyObject());
          return true;
        } catch (Throwable t) {
          return false;
        }
      }
    });

    executorService.shutdown();
  }

  @Test
  public void testSnapshotHandlers() throws Exception {
    final Configuration conf = HBaseConfiguration.create();
    final Server server = mock(Server.class);
    when(server.getConfiguration()).thenReturn(conf);

    ExecutorService executorService = new ExecutorService("testSnapshotHandlers");
    executorService.startExecutorService(ExecutorType.MASTER_SNAPSHOT_OPERATIONS, 1);

    CountDownLatch latch = new CountDownLatch(1);
    CountDownLatch waitForEventToStart = new CountDownLatch(1);
    executorService.submit(new EventHandler(server, EventType.C_M_SNAPSHOT_TABLE) {
      @Override
      public void process() throws IOException {
        waitForEventToStart.countDown();
        try {
          latch.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    });

    //Wait EventHandler to start
    waitForEventToStart.await(10, TimeUnit.SECONDS);
    int activeCount = executorService.getExecutor(ExecutorType.MASTER_SNAPSHOT_OPERATIONS)
        .getThreadPoolExecutor().getActiveCount();
    Assert.assertEquals(1, activeCount);
    latch.countDown();
    Waiter.waitFor(conf, 3000, () -> {
      int count = executorService.getExecutor(ExecutorType.MASTER_SNAPSHOT_OPERATIONS)
          .getThreadPoolExecutor().getActiveCount();
      return count == 0;
    });
  }
}

