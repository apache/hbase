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
package org.apache.hadoop.hbase.io.hfile;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache;
import org.apache.hadoop.hbase.util.DirectMemoryUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;

/**
 * A singleton implementation of {@link L2CacheFactory} that creates
 * {@link L2BucketCache} instances. This is a <b>lazy</b> singleton:
 * first call to getInstance() will initialize and cache an instance,
 * subsequent invocations will return the cached instance. All (static and
 * per-instance) class methods are thread-safe.
 * </p>
 * TODO (avf): remove all singleton functionality once Guice or other DI
 *             mechanism is available
 */
public class L2BucketCacheFactory implements L2CacheFactory {

  private static final Log LOG = LogFactory.getLog(L2BucketCacheFactory.class);

  /** Cached singleton instance or null if not initialized */
  private static L2BucketCacheFactory instance;

  private L2BucketCacheFactory() { } // Private as this is a singleton

  /**
   * Returns a single cached instance of this class, initializing if needed.
   * @return A cached and instantiated instance of this class
   */
  public synchronized static L2BucketCacheFactory getInstance() {
    if (instance == null) {
      instance = new L2BucketCacheFactory();
    }
    return instance;
  }

  /** Cached instance of the L2Cache or null if not initialized */
  private L2BucketCache l2Cache;

  // Allows to short circuit getL2Cache() calls if the cache is disabled
  private boolean l2CacheDisabled;

  private static String bucketSizesToString(int[] bucketSizes) {
    StringBuilder sb = new StringBuilder();
    sb.append("Configured bucket sizes: ");
    for (int bucketSize : bucketSizes) {
      sb.append(StringUtils.humanReadableInt(bucketSize));
      sb.append(" ");
    }
    return sb.toString();
  }
  /**
   * Returns a cached initialized instance of {@link L2BucketCache}. Follows
   * lazy initialization pattern: first invocation creates and initializes the
   * instance, subsequent invocations return the cached instance.
   * @param conf The configuration to pass to {@link L2BucketCache}
   * @return The {@link L2BucketCache} instance
   */
  @Override
  public synchronized L2Cache getL2Cache(Configuration conf) {
    Preconditions.checkNotNull(conf);

    if (l2Cache != null) {
      return l2Cache;
    }
    if (l2CacheDisabled) {
      return null;
    }

    // If no IOEngine is specified for the L2 cache, assume the L2 cache
    // is disabled
    String bucketCacheIOEngineName =
        conf.get(CacheConfig.L2_BUCKET_CACHE_IOENGINE_KEY, null);
    if (bucketCacheIOEngineName == null) {
      l2CacheDisabled = true;
      return null;
    }

    long maxMem = 0;
    if (bucketCacheIOEngineName.equals("offheap")) {
      maxMem = DirectMemoryUtils.getDirectMemorySize();
    } else if (bucketCacheIOEngineName.equals("heap")) {
      MemoryUsage mu = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
      maxMem = mu.getMax();
    }

    // Unless a percentage of absolute size is set, assume the cache is
    // disabled. This is analogous to default behaviour with LruBlockCache.
    float bucketCachePercentage =
        conf.getFloat(CacheConfig.L2_BUCKET_CACHE_SIZE_KEY, 0F);
    long bucketCacheSize = (long) (bucketCachePercentage < 1
                                       ?maxMem * bucketCachePercentage :
                                       bucketCachePercentage * 1024 * 1024);
    if (bucketCacheSize > 0) {
      int writerThreads = conf.getInt(
          CacheConfig.L2_BUCKET_CACHE_WRITER_THREADS_KEY,
          BucketCache.DEFAULT_WRITER_THREADS);
      int writerQueueLen = conf.getInt(CacheConfig.L2_BUCKET_CACHE_QUEUE_KEY,
          BucketCache.DEFAULT_WRITER_QUEUE_ITEMS);
      int ioErrorsTolerationDuration =
          conf.getInt(CacheConfig.L2_BUCKET_CACHE_IOENGINE_ERRORS_TOLERATED_DURATION_KEY,
              BucketCache.DEFAULT_ERROR_TOLERATION_DURATION);
      try {
        int[] bucketSizes = CacheConfig.getL2BucketSizes(conf);
        LOG.info(bucketSizesToString(bucketSizes));
        long bucketCacheInitStartMs = EnvironmentEdgeManager.currentTimeMillis();
        BucketCache bucketCache = new BucketCache(bucketCacheIOEngineName,
            bucketCacheSize, writerThreads, writerQueueLen,
            ioErrorsTolerationDuration, bucketSizes, conf);
        l2Cache = new L2BucketCache(bucketCache);
        long bucketCacheInitElapsedMs =
            EnvironmentEdgeManager.currentTimeMillis() - bucketCacheInitStartMs;
        LOG.info("L2BucketCache instantiated in " + bucketCacheInitElapsedMs +
            " ms.; bucketCacheIOEngine=" + bucketCacheIOEngineName +
            ", bucketCacheSize=" + bucketCacheSize +
            ", writerThreads=" + writerThreads + ", writerQueueLen=" +
            writerQueueLen + ", ioErrorsTolerationDuration=" +
            ioErrorsTolerationDuration);
      } catch (IOException ioe) {
        LOG.error("Can't instantiate L2 cache with bucket cache engine",
            ioe);
        throw new RuntimeException(ioe);
      }
    }
    return l2Cache;
  }
}
