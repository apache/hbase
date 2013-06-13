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
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.util.StringUtils;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;

/**
 * A singleton implementation of {@link BlockCacheFactory} that creates
 * {@link LruBlockCache} instances. This is a <b>lazy</b> singleton:
 * first call to getInstance() will initialize and cache an instance,
 * subsequent invocations will return the cached instances. All (static
 * and per-instance) class methods are thread-safe.
 * </p>
 * TODO (avf): remove all singleton functionality once Guice or other DI
 *             mechanism is available
 */
public class LruBlockCacheFactory implements BlockCacheFactory {

  private static final Log LOG = LogFactory.getLog(LruBlockCache.class);

  /** Cached singleton instance or null if not initialized */
  private static LruBlockCacheFactory instance;

  private LruBlockCacheFactory() { } // Private as this is a singleton

  public synchronized static LruBlockCacheFactory getInstance() {
    if (instance == null) {
      instance = new LruBlockCacheFactory();
    }
    return instance;
  }

  /** Cached instance of the BlockCache or null if not initialized */
  private LruBlockCache blockCache;

  // ALlows to short circuit getBlockCache calls if the cache is disabled
  private boolean blockCacheDisabled;

  /**
   * Returns the current underlying block cache instance or null if
   * block cache is disabled or not initialized. Used by
   * {@link SchemaMetrics}
   * @return The current block cache instance, null if disabled or
   *         or not initialized
   */
  public synchronized LruBlockCache getCurrentBlockCacheInstance() {
    return blockCache;
  }

  /**
   * Returns a cached initialized instance of {@link LruBlockCache}.
   * Follows lazy initialization pattern: first invocation creates and
   * initializes the instance, subsequent invocations return the cached
   * instance. This replaced a static "instantiateBlockCache()" method
   * in CacheConfig.
   * * @param conf The HBase configuration needed to create the instance
   * @return The {@link LruBlockCache} instance.
   */
  @Override
  public synchronized BlockCache getBlockCache(Configuration conf) {
    Preconditions.checkNotNull(conf);

    if (blockCache != null)
      return blockCache;
    if (blockCacheDisabled)
      return null;

    float cachePercentage = conf.getFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY,
        HConstants.HFILE_BLOCK_CACHE_SIZE_DEFAULT);
    if (cachePercentage == 0L) {
      blockCacheDisabled = true;
      return null;
    }
    if (cachePercentage > 1.0) {
      throw new IllegalArgumentException(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY +
          " must be between 0.0 and 1.0, not > 1.0");
    }

    // Calculate the amount of heap to give the heap.
    MemoryUsage mu = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
    long cacheSize = (long)(mu.getMax() * cachePercentage);
    LOG.info("Allocating LruBlockCache with maximum size " +
        StringUtils.humanReadableInt(cacheSize));
    blockCache = new LruBlockCache(cacheSize,
        StoreFile.DEFAULT_BLOCKSIZE_SMALL);
    return blockCache;
  }
}
