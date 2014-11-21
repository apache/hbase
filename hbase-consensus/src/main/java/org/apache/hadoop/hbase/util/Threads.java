/**
 * Copyright 2010 The Apache Software Foundation
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

import java.io.InterruptedIOException;
import java.io.PrintWriter;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.hbase.util.HasThread;

/**
 * Thread Utility
 */
public class Threads {
  protected static final Log LOG = LogFactory.getLog(Threads.class);

  /**
   * Utility method that sets name, daemon status and starts passed thread.
   * @param t thread to frob
   * @param name new name
   * @return Returns the passed Thread <code>t</code>.
   */
  public static Thread setDaemonThreadRunning(final Thread t,
    final String name) {
    return setDaemonThreadRunning(t, name, null);
  }

  public static HasThread setDaemonThreadRunning(final HasThread t,
      final String name) {
    setDaemonThreadRunning(t.getThread(), name, null);
    return t;
  }

  /**
   * Utility method that sets name, daemon status and starts passed thread.
   * @param t thread to frob
   * @param name new name
   * @param handler A handler to set on the thread.  Pass null if want to
   * use default handler.
   * @return Returns the passed Thread <code>t</code>.
   */
  public static Thread setDaemonThreadRunning(final Thread t,
    final String name, final UncaughtExceptionHandler handler) {
    if (name != null) {
      t.setName(name);
    }
    if (handler != null) {
      t.setUncaughtExceptionHandler(handler);
    }
    t.setDaemon(true);
    t.start();
    return t;
  }

  public static HasThread setDaemonThreadRunning(final HasThread t,
      final String name, final UncaughtExceptionHandler handler) {
    setDaemonThreadRunning(t.getThread(), name, handler);
    return t;
  }

  /**
   * Shutdown passed thread using isAlive and join.
   * @param t Thread to shutdown
   */
  public static void shutdown(final Thread t) {
    shutdown(t, 0);
  }

  public static void shutdown(final HasThread t) {
    shutdown(t.getThread(), 0);
  }

  /**
   * Shutdown passed thread using isAlive and join.
   * @param joinwait Pass 0 if we're to wait forever.
   * @param t Thread to shutdown
   */
  public static void shutdown(final Thread t, final long joinwait) {
    if (t == null) return;
    while (t.isAlive()) {
      try {
        t.join(joinwait);
      } catch (InterruptedException e) {
        LOG.warn(t.getName() + "; joinwait=" + joinwait, e);
      }
    }
  }
  
  public static void shutdown(final HasThread t, final long joinwait) {
    shutdown(t.getThread(), joinwait);
  }

  /**
   * @param t Waits on the passed thread to die dumping a threaddump every
   * minute while its up.
   * @throws InterruptedException
   */
  public static void threadDumpingIsAlive(final Thread t)
  throws InterruptedException {
    if (t == null) {
      return;
    }
    long startTime = System.currentTimeMillis();
    while (t.isAlive()) {
      Thread.sleep(1000);
      if (System.currentTimeMillis() - startTime > 60000) {
        startTime = System.currentTimeMillis();
        ReflectionUtils.printThreadInfo(new PrintWriter(System.out),
            "Automatic Stack Trace every 60 seconds waiting on " +
            t.getName());
      }
    }
  }

  /**
   * @param millis How long to sleep for in milliseconds.
   */
  public static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted when sleeping for " + millis + "ms, ignoring", e);
    }
  }

  /**
   * Sleeps for the given amount of time. Retains the thread's interruption status. 
   * @param millis How long to sleep for in milliseconds.
   */
  public static void sleepRetainInterrupt(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted when sleeping for " + millis + "ms");
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Sleeps for the given amount of time even if interrupted. Preserves
   * the interrupt status.
   * @param msToWait the amount of time to sleep in milliseconds
   */
  public static void sleepWithoutInterrupt(final long msToWait) {
    long timeMillis = System.currentTimeMillis();
    long endTime = timeMillis + msToWait;
    boolean interrupted = false;
    while (timeMillis < endTime) {
      try {
        Thread.sleep(endTime - timeMillis);
      } catch (InterruptedException ex) {
        interrupted = true;
      }
      timeMillis = System.currentTimeMillis();
    }

    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Create a new CachedThreadPool with a bounded number as the maximum 
   * thread size in the pool.
   * 
   * @param maxCachedThread the maximum thread could be created in the pool
   * @param timeout the maximum time to wait
   * @param unit the time unit of the timeout argument
   * @param threadFactory the factory to use when creating new threads
   * @return threadPoolExecutor the cachedThreadPool with a bounded number 
   * as the maximum thread size in the pool. 
   */
  public static ThreadPoolExecutor getBoundedCachedThreadPool(
      int maxCachedThread, long timeout, TimeUnit unit,
      ThreadFactory threadFactory) {
    ThreadPoolExecutor boundedCachedThreadPool =
      new ThreadPoolExecutor(maxCachedThread, maxCachedThread, timeout,
        TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), threadFactory);
    // allow the core pool threads timeout and terminate
    boundedCachedThreadPool.allowCoreThreadTimeOut(true);
    return boundedCachedThreadPool;
  }

  /**
   * Creates a ThreadPoolExecutor which has a bound on the number of tasks that can be 
   * submitted to it, determined by the blockingLimit parameter. Excess tasks 
   * submitted will block on the calling thread till space frees up.
   * 
   * @param blockingLimit max number of tasks that can be submitted
   * @param timeout time value after which unused threads are killed
   * @param unit time unit for killing unused threads
   * @param threadFactory thread factory to use to spawn threads
   * @return the ThreadPoolExecutor
   */
  public static ThreadPoolExecutor getBlockingThreadPool(
      int blockingLimit, long timeout, TimeUnit unit,
      ThreadFactory threadFactory) {
    ThreadPoolExecutor blockingThreadPool = 
      new ThreadPoolExecutor( 
        1, blockingLimit, timeout, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(),
        threadFactory, 
        new RejectedExecutionHandler() {  
          @Override 
          public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {  
            try { 
              // The submitting thread will block until the thread pool frees up. 
              executor.getQueue().put(r); 
            } catch (InterruptedException e) {  
              throw new RejectedExecutionException( 
                  "Failed to requeue the rejected request because of ", e); 
            } 
          } 
        });
    blockingThreadPool.allowCoreThreadTimeOut(true);
    return blockingThreadPool;
  }

  public static void renameThread(Thread t, String newName) {
    String oldName = t.getName();
    if (!oldName.equals(newName)) {
      LOG.info("Thread '" + oldName + "' is now known as '" + newName + "'");
      t.setName(newName);
    }
  }

  /**
   * A helper function to check if the current thread was interrupted.
   *
   * @throws InterruptedIOException if the thread had been interrupted.
   **/
  public static void checkInterrupted() throws InterruptedIOException {
    if (Thread.interrupted()) {
      throw new InterruptedIOException("Thread was interrupted");
    }
  }
}
