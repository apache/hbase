/**
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

import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

/**
 * Thread Utility
 */
@InterfaceAudience.Private
public class Threads {
  private static final Log LOG = LogFactory.getLog(Threads.class);
  private static final AtomicInteger poolNumber = new AtomicInteger(1);

  private static UncaughtExceptionHandler LOGGING_EXCEPTION_HANDLER =
    new UncaughtExceptionHandler() {
    @Override
    public void uncaughtException(Thread t, Throwable e) {
      LOG.warn("Thread:" + t + " exited with Exception:"
          + StringUtils.stringifyException(e));
    }
  };

  /**
   * Utility method that sets name, daemon status and starts passed thread.
   * @param t thread to run
   * @return Returns the passed Thread <code>t</code>.
   */
  public static Thread setDaemonThreadRunning(final Thread t) {
    return setDaemonThreadRunning(t, t.getName());
  }

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
    t.setName(name);
    if (handler != null) {
      t.setUncaughtExceptionHandler(handler);
    }
    t.setDaemon(true);
    t.start();
    return t;
  }

  /**
   * Shutdown passed thread using isAlive and join.
   * @param t Thread to shutdown
   */
  public static void shutdown(final Thread t) {
    shutdown(t, 0);
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

    while (t.isAlive()) {
      t.join(60 * 1000);
      if (t.isAlive()) {
        printThreadInfo(System.out,
            "Automatic Stack Trace every 60 seconds waiting on " +
            t.getName());
      }
    }
  }

  /**
   * If interrupted, just prints out the interrupt on STDOUT, resets interrupt and returns
   * @param millis How long to sleep for in milliseconds.
   */
  public static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      e.printStackTrace();
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
        unit, new LinkedBlockingQueue<Runnable>(), threadFactory);
    // allow the core pool threads timeout and terminate
    boundedCachedThreadPool.allowCoreThreadTimeOut(true);
    return boundedCachedThreadPool;
  }


  /**
   * Returns a {@link java.util.concurrent.ThreadFactory} that names each created thread uniquely,
   * with a common prefix.
   * @param prefix The prefix of every created Thread's name
   * @return a {@link java.util.concurrent.ThreadFactory} that names threads
   */
  public static ThreadFactory getNamedThreadFactory(final String prefix) {
    SecurityManager s = System.getSecurityManager();
    final ThreadGroup threadGroup = (s != null) ? s.getThreadGroup() : Thread.currentThread()
        .getThreadGroup();

    return new ThreadFactory() {
      final AtomicInteger threadNumber = new AtomicInteger(1);
      private final int poolNumber = Threads.poolNumber.getAndIncrement();
      final ThreadGroup group = threadGroup;

      @Override
      public Thread newThread(Runnable r) {
        final String name = prefix + "-pool" + poolNumber + "-t" + threadNumber.getAndIncrement();
        return new Thread(group, r, name);
      }
    };
  }

  /**
   * Same as {#newDaemonThreadFactory(String, UncaughtExceptionHandler)},
   * without setting the exception handler.
   */
  public static ThreadFactory newDaemonThreadFactory(final String prefix) {
    return newDaemonThreadFactory(prefix, null);
  }

  /**
   * Get a named {@link ThreadFactory} that just builds daemon threads.
   * @param prefix name prefix for all threads created from the factory
   * @param handler unhandles exception handler to set for all threads
   * @return a thread factory that creates named, daemon threads with
   *         the supplied exception handler and normal priority
   */
  public static ThreadFactory newDaemonThreadFactory(final String prefix,
      final UncaughtExceptionHandler handler) {
    final ThreadFactory namedFactory = getNamedThreadFactory(prefix);
    return new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        Thread t = namedFactory.newThread(r);
        if (handler != null) {
          t.setUncaughtExceptionHandler(handler);
        } else {
          t.setUncaughtExceptionHandler(LOGGING_EXCEPTION_HANDLER);
        }
        if (!t.isDaemon()) {
          t.setDaemon(true);
        }
        if (t.getPriority() != Thread.NORM_PRIORITY) {
          t.setPriority(Thread.NORM_PRIORITY);
        }
        return t;
      }

    };
  }

  /** Sets an UncaughtExceptionHandler for the thread which logs the
   * Exception stack if the thread dies.
   */
  public static void setLoggingUncaughtExceptionHandler(Thread t) {
    t.setUncaughtExceptionHandler(LOGGING_EXCEPTION_HANDLER);
  }

  private static Method PRINT_THREAD_INFO_METHOD = null;
  private static boolean PRINT_THREAD_INFO_METHOD_WITH_PRINTSTREAM = true;

  /**
   * Print all of the thread's information and stack traces. Wrapper around Hadoop's method.
   *
   * @param stream the stream to
   * @param title a string title for the stack trace
   */
  public static synchronized void printThreadInfo(PrintStream stream, String title) {
    if (PRINT_THREAD_INFO_METHOD == null) {
      try {
        // Hadoop 2.7+ declares printThreadInfo(PrintStream, String)
        PRINT_THREAD_INFO_METHOD = ReflectionUtils.class.getMethod("printThreadInfo",
          PrintStream.class, String.class);
      } catch (NoSuchMethodException e) {
        // Hadoop 2.6 and earlier declares printThreadInfo(PrintWriter, String)
        PRINT_THREAD_INFO_METHOD_WITH_PRINTSTREAM = false;
        try {
          PRINT_THREAD_INFO_METHOD = ReflectionUtils.class.getMethod("printThreadInfo",
            PrintWriter.class, String.class);
        } catch (NoSuchMethodException e1) {
          throw new RuntimeException("Cannot find method. Check hadoop jars linked", e1);
        }
      }
      PRINT_THREAD_INFO_METHOD.setAccessible(true);
    }

    try {
      if (PRINT_THREAD_INFO_METHOD_WITH_PRINTSTREAM) {
        PRINT_THREAD_INFO_METHOD.invoke(null, stream, title);
      } else {
        PRINT_THREAD_INFO_METHOD.invoke(null, new PrintWriter(stream), title);
      }
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new RuntimeException(e.getCause());
    }
  }
}
