/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.client;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/*
 * This class propose a ThreadPoolExecutor which
 * uses hbase.client.max.total.tasks as default pool size (if hbase.htable.threads.max isn't set)
 * computes some metrics on submitted task to allow to the application to know:
 * how many flush have been triggered from mutate()
 * the amount of time spent in those background threads
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "DLS_DEAD_LOCAL_STORE",
    justification = "Class provided as illustration for application"
        + " as an alternative to the default pool")
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class BufferedMutatorThreadPoolExecutor extends ThreadPoolExecutor {

  private final Object _lock = new Object();

  private Map<Thread, Runnable> ssrMap;

  private long ssrCacheMaxStored = 0;
  private AtomicLong ssrCachePutCount = new AtomicLong(0);
  private long ssrCacheRemoveCount = 0;

  private long totalExecutedTaskCount = 0;
  private long totalRejectedTaskCount = 0;
  private long totalExecutedNanoTime = 0;
  private long totalRejectedNanoTime = 0;
  private AtomicLong totalMissingTaskCount = new AtomicLong(0);

  public BufferedMutatorThreadPoolExecutor(final int corePoolSize, final int maximumPoolSize,
      final long keepAliveTime, final TimeUnit unit, final BlockingQueue<Runnable> workQueue,
      final ThreadFactory threadFactory) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    ssrMap = new ConcurrentHashMap<>(this.getMaximumPoolSize());
  }

  public static BufferedMutatorThreadPoolExecutor getPoolExecutor(Configuration conf) {
    int maxThreads = conf.getInt("hbase.htable.threads.max", Integer.MAX_VALUE);
    if (maxThreads <= 0 || maxThreads == Integer.MAX_VALUE) {
      maxThreads = conf.getInt("hbase.client.max.total.tasks", Integer.MAX_VALUE);
    }
    if (maxThreads <= 0 || maxThreads == Integer.MAX_VALUE) {
      throw new IllegalArgumentException("hbase.client.max.total.tasks must be >0");
    }
    long keepAliveTime = conf.getLong("hbase.htable.threads.keepalivetime", 60);

    // Using the "direct handoff" approach, new threads will only be created
    // if it is necessary and will grow unbounded. This could be bad but in HCM
    // we only create as many Runnables as there are region servers. It means
    // it also scales when new region servers are added.
    BufferedMutatorThreadPoolExecutor pool =
        new BufferedMutatorThreadPoolExecutor(1, maxThreads, keepAliveTime, TimeUnit.SECONDS,
            new SynchronousQueue<>(), Threads.newDaemonThreadFactory("htable"));
    pool.allowCoreThreadTimeOut(true);
    return pool;
  }

  @Override
  public Future<?> submit(final Runnable r) {
    if (r instanceof AsyncRequestFutureImpl.SingleServerRequestRunnable) {
      final AsyncRequestFutureImpl.SingleServerRequestRunnable task =
          ((AsyncRequestFutureImpl.SingleServerRequestRunnable) r);
      task.onStart();
    }
    return super.submit(r);
  }

  @Override
  protected void beforeExecute(final Thread t, final Runnable r) {
    // if t is null, this means we will get a RejectedExecutionException, and NEVER be called
    // in afterExecute
    // To avoid a memory leak, add in the map only if t is NOT null!
    if (t != null && r instanceof Future<?>) {
      final Future<?> f = (Future<?>) r;
      final Object o = TaskDiscoverer.findRealTask(r);
      if (o instanceof AsyncRequestFutureImpl.SingleServerRequestRunnable) {
        final AsyncRequestFutureImpl.SingleServerRequestRunnable task =
            ((AsyncRequestFutureImpl.SingleServerRequestRunnable) o);
        ssrMap.put(t, task);
        ssrCachePutCount.incrementAndGet();
      }
    }
    super.beforeExecute(t, r);
  }

  @Override
  protected void afterExecute(final Runnable r, final Throwable t) {
    super.afterExecute(r, t);

    final Object o = ssrMap.remove(Thread.currentThread());
    if (o instanceof AsyncRequestFutureImpl.SingleServerRequestRunnable) {
      final AsyncRequestFutureImpl.SingleServerRequestRunnable task =
          ((AsyncRequestFutureImpl.SingleServerRequestRunnable) o);
      task.onFinish();
      synchronized (_lock) {
        ++ssrCacheRemoveCount;
        ssrCacheMaxStored =
            Math.max(ssrCacheMaxStored, ssrCachePutCount.get() - ssrCacheRemoveCount);
        ++totalExecutedTaskCount;
        totalExecutedNanoTime += task.getElapseNanoTime();
        totalRejectedTaskCount += task.getNumReject();
        totalRejectedNanoTime += task.getRejectedElapseNanoTime();
      }
    } else {
      totalMissingTaskCount.incrementAndGet();
    }

  }

  public long getAndResetSsrCacheMaxStored() {
    long ret = ssrCacheMaxStored;
    ssrCacheMaxStored = 0;
    ssrCachePutCount.set(0);
    ssrCacheRemoveCount = 0;
    return ret;
  }

  public long getAvgRejectedNanoTime() {
    if (totalRejectedTaskCount == 0) {
      return 0;
    }
    return totalRejectedNanoTime / totalRejectedTaskCount;
  }

  public long getTotalMissingTaskCount() {
    return totalMissingTaskCount.get();
  }

  public long getAvgExecutedNanoTime() {
    if (totalExecutedTaskCount == 0) {
      return 0;
    }
    return totalExecutedNanoTime / totalExecutedTaskCount;
  }

  public long getTotalExecutedTaskCount() {
    return totalExecutedTaskCount;
  }

  public long getTotalExecutedNanoTime() {
    return totalExecutedNanoTime;
  }

  public long getTotalRejectedTaskCount() {
    return totalRejectedTaskCount;
  }

  public long getTotalRejectedNanoTime() {
    return totalRejectedNanoTime;
  }

  public static class TaskDiscoverer {

    private final static Field callableInFutureTask;
    private static final Class<? extends Callable> adapterClass;
    private static final Field runnableInAdapter;

    static {
      try {
        callableInFutureTask = FutureTask.class.getDeclaredField("callable");
        callableInFutureTask.setAccessible(true);
        adapterClass = Executors.callable(() -> {
        }).getClass();
        runnableInAdapter = adapterClass.getDeclaredField("task");
        runnableInAdapter.setAccessible(true);
      } catch (NoSuchFieldException e) {
        throw new ExceptionInInitializerError(e);
      }
    }

    public static Object findRealTask(final Runnable task) {
      if (task instanceof FutureTask) {
        try {
          Object callable = callableInFutureTask.get(task);
          if (adapterClass.isInstance(callable)) {
            return runnableInAdapter.get(callable);
          } else {
            return callable;
          }
        } catch (IllegalAccessException e) {
          throw new IllegalStateException(e);
        }
      }
      throw new ClassCastException("Not a FutureTask");
    }
  }

}
