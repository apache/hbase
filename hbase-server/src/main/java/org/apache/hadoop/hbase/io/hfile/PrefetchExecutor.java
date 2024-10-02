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
package org.apache.hadoop.hbase.io.hfile;

import com.google.errorprone.annotations.RestrictedApi;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public final class PrefetchExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(PrefetchExecutor.class);
  /** Wait time in miliseconds before executing prefetch */
  public static final String PREFETCH_DELAY = "hbase.hfile.prefetch.delay";
  public static final String PREFETCH_DELAY_VARIATION = "hbase.hfile.prefetch.delay.variation";
  public static final float PREFETCH_DELAY_VARIATION_DEFAULT_VALUE = 0.2f;

  /** Futures for tracking block prefetch activity */
  private static final Map<Path, Future<?>> prefetchFutures = new ConcurrentSkipListMap<>();
  /** Runnables for resetting the prefetch activity */
  private static final Map<Path, Runnable> prefetchRunnable = new ConcurrentSkipListMap<>();
  /** Executor pool shared among all HFiles for block prefetch */
  private static final ScheduledExecutorService prefetchExecutorPool;
  /** Delay before beginning prefetch */
  private static int prefetchDelayMillis;
  /** Variation in prefetch delay times, to mitigate stampedes */
  private static float prefetchDelayVariation;
  static {
    // Consider doing this on demand with a configuration passed in rather
    // than in a static initializer.
    Configuration conf = HBaseConfiguration.create();
    // 1s here for tests, consider 30s in hbase-default.xml
    // Set to 0 for no delay
    prefetchDelayMillis = conf.getInt(PREFETCH_DELAY, 1000);
    prefetchDelayVariation =
      conf.getFloat(PREFETCH_DELAY_VARIATION, PREFETCH_DELAY_VARIATION_DEFAULT_VALUE);
    int prefetchThreads = conf.getInt("hbase.hfile.thread.prefetch", 4);
    prefetchExecutorPool = new ScheduledThreadPoolExecutor(prefetchThreads, new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        String name = "hfile-prefetch-" + EnvironmentEdgeManager.currentTime();
        Thread t = new Thread(r, name);
        t.setDaemon(true);
        return t;
      }
    });
  }

  // TODO: We want HFile, which is where the blockcache lives, to handle
  // prefetching of file blocks but the Store level is where path convention
  // knowledge should be contained
  private static final Pattern prefetchPathExclude =
    Pattern.compile("(" + Path.SEPARATOR_CHAR + HConstants.HBASE_TEMP_DIRECTORY.replace(".", "\\.")
      + Path.SEPARATOR_CHAR + ")|(" + Path.SEPARATOR_CHAR
      + HConstants.HREGION_COMPACTIONDIR_NAME.replace(".", "\\.") + Path.SEPARATOR_CHAR + ")");

  public static void request(Path path, Runnable runnable) {
    if (!prefetchPathExclude.matcher(path.toString()).find()) {
      long delay;
      if (prefetchDelayMillis > 0) {
        delay = (long) ((prefetchDelayMillis * (1.0f - (prefetchDelayVariation / 2)))
          + (prefetchDelayMillis * (prefetchDelayVariation / 2)
            * ThreadLocalRandom.current().nextFloat()));
      } else {
        delay = 0;
      }
      try {
        LOG.debug("Prefetch requested for {}, delay={} ms", path, delay);
        final Runnable tracedRunnable =
          TraceUtil.tracedRunnable(runnable, "PrefetchExecutor.request");
        final Future<?> future =
          prefetchExecutorPool.schedule(tracedRunnable, delay, TimeUnit.MILLISECONDS);
        prefetchFutures.put(path, future);
        prefetchRunnable.put(path, runnable);
      } catch (RejectedExecutionException e) {
        prefetchFutures.remove(path);
        prefetchRunnable.remove(path);
        LOG.warn("Prefetch request rejected for {}", path);
      }
    }
  }

  public static void complete(Path path) {
    prefetchFutures.remove(path);
    prefetchRunnable.remove(path);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Prefetch completed for {}", path.getName());
    }
  }

  public static void cancel(Path path) {
    Future<?> future = prefetchFutures.get(path);
    if (future != null) {
      // ok to race with other cancellation attempts
      future.cancel(true);
      prefetchFutures.remove(path);
      prefetchRunnable.remove(path);
      LOG.debug("Prefetch cancelled for {}", path);
    }
  }

  private PrefetchExecutor() {
  }

  public static boolean isCompleted(Path path) {
    Future<?> future = prefetchFutures.get(path);
    if (future != null) {
      return future.isDone();
    }
    return true;
  }

  /* Visible for testing only */
  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  static ScheduledExecutorService getExecutorPool() {
    return prefetchExecutorPool;
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  static Map<Path, Future<?>> getPrefetchFutures() {
    return prefetchFutures;
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  static Map<Path, Runnable> getPrefetchRunnable() {
    return prefetchRunnable;
  }

  static boolean isPrefetchStarted() {
    AtomicBoolean prefetchStarted = new AtomicBoolean(false);
    for (Map.Entry<Path, Future<?>> entry : prefetchFutures.entrySet()) {
      Path k = entry.getKey();
      Future<?> v = entry.getValue();
      ScheduledFuture sf = (ScheduledFuture) prefetchFutures.get(k);
      long waitTime = sf.getDelay(TimeUnit.MILLISECONDS);
      if (waitTime < 0) {
        // At this point prefetch is started
        prefetchStarted.set(true);
        break;
      }
    }
    return prefetchStarted.get();
  }

  public static int getPrefetchDelay() {
    return prefetchDelayMillis;
  }

  public static void loadConfiguration(Configuration conf) {
    prefetchDelayMillis = conf.getInt(PREFETCH_DELAY, 1000);
    prefetchDelayVariation =
      conf.getFloat(PREFETCH_DELAY_VARIATION, PREFETCH_DELAY_VARIATION_DEFAULT_VALUE);
    prefetchFutures.forEach((k, v) -> {
      ScheduledFuture sf = (ScheduledFuture) prefetchFutures.get(k);
      if (!(sf.getDelay(TimeUnit.MILLISECONDS) > 0)) {
        Runnable runnable = prefetchRunnable.get(k);
        Future<?> future = prefetchFutures.get(k);
        if (future != null) {
          prefetchFutures.remove(k);
          // ok to race with other cancellation attempts
          boolean canceled = future.cancel(true);
          LOG.debug("Prefetch {} for {}",
            canceled ? "cancelled" : "cancel attempted but it was already finished", k);
        }
        if (runnable != null && future != null && future.isCancelled()) {
          request(k, runnable);
        }
      }
      LOG.debug("Reset called on Prefetch of file {} with delay {}, delay variation {}", k,
        prefetchDelayMillis, prefetchDelayVariation);
    });
  }
}
