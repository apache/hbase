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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MultithreadedTestUtil {

  private static final Logger LOG = LoggerFactory.getLogger(MultithreadedTestUtil.class);

  public static class TestContext {
    private final Configuration conf;
    private Throwable err = null;
    private boolean stopped = false;
    private int threadDoneCount = 0;
    private Set<TestThread> testThreads = new HashSet<>();

    public TestContext(Configuration configuration) {
      this.conf = configuration;
    }

    protected Configuration getConf() {
      return conf;
    }

    public synchronized boolean shouldRun() {
      return !stopped && err == null;
    }

    public void addThread(TestThread t) {
      testThreads.add(t);
    }

    public void startThreads() {
      for (TestThread t : testThreads) {
        t.start();
      }
    }

    public void waitFor(long millis) throws Exception {
      long endTime = System.currentTimeMillis() + millis;
      while (!stopped) {
        long left = endTime - System.currentTimeMillis();
        if (left <= 0) break;
        synchronized (this) {
          checkException();
          wait(left);
        }
      }
    }

    private synchronized void checkException() throws Exception {
      if (err != null) {
        throw new RuntimeException("Deferred", err);
      }
    }

    public synchronized void threadFailed(Throwable t) {
      if (err == null) err = t;
      LOG.error("Failed!", err);
      notify();
    }

    public synchronized void threadDone() {
      threadDoneCount++;
    }

    public void setStopFlag(boolean s) throws Exception {
      synchronized (this) {
        stopped = s;
      }
    }

    public void stop() throws Exception {
      synchronized (this) {
        stopped = true;
      }
      for (TestThread t : testThreads) {
        t.join();
      }
      checkException();
    }
  }

  /**
   * A thread that can be added to a test context, and properly passes exceptions through.
   */
  public static abstract class TestThread extends Thread {
    protected final TestContext ctx;
    protected boolean stopped;

    public TestThread(TestContext ctx) {
      this.ctx = ctx;
    }

    @Override
    public void run() {
      try {
        doWork();
      } catch (Throwable t) {
        ctx.threadFailed(t);
      }
      ctx.threadDone();
    }

    public abstract void doWork() throws Exception;

    protected void stopTestThread() {
      this.stopped = true;
    }
  }

  /**
   * A test thread that performs a repeating operation.
   */
  public static abstract class RepeatingTestThread extends TestThread {
    public RepeatingTestThread(TestContext ctx) {
      super(ctx);
    }

    @Override
    public final void doWork() throws Exception {
      try {
        while (ctx.shouldRun() && !stopped) {
          doAnAction();
        }
      } finally {
        workDone();
      }
    }

    public abstract void doAnAction() throws Exception;

    public void workDone() throws IOException {
    }
  }

  /**
   * Verify that no assertions have failed inside a future. Used for unit tests that spawn threads.
   * E.g.,
   * <p>
   *
   * <pre>
   *   List&lt;Future&lt;Void>> results = Lists.newArrayList();
   *   Future&lt;Void> f = executor.submit(new Callable&lt;Void> {
   *     public Void call() {
   *       assertTrue(someMethod());
   *     }
   *   });
   *   results.add(f);
   *   assertOnFutures(results);
   * </pre>
   *
   * @param threadResults A list of futures
   * @throws InterruptedException If interrupted when waiting for a result from one of the futures
   * @throws ExecutionException   If an exception other than AssertionError occurs inside any of the
   *                              futures
   */
  public static void assertOnFutures(List<Future<?>> threadResults)
    throws InterruptedException, ExecutionException {
    for (Future<?> threadResult : threadResults) {
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
