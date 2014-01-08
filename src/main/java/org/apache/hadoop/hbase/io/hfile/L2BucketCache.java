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


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache;

/**
 * An implementation of L2 cache based on {@link BucketCache}
 */
public class L2BucketCache implements L2Cache {

  private static final Log LOG = LogFactory.getLog(L2BucketCache.class);

  private final BucketCache bucketCache;

  public L2BucketCache(BucketCache bucketCache) {
    this.bucketCache = bucketCache;
  }

  @Override
  public byte[] getRawBlockBytes(BlockCacheKey key) {
    long startTimeNs = System.nanoTime();
    byte[] fromCache = bucketCache.getBlock(key, true);
    if (LOG.isTraceEnabled()) {
      // Log elapsed time to retrieve a block from the cache
      long elapsedNs = System.nanoTime() - startTimeNs;
      LOG.trace("getRawBlock() " + (fromCache == null ? "MISS" : "HIT") +
          " on hfileName=" + key.getHfileName() +
          ", offset=" + key.getOffset() + " in " +  elapsedNs + " ns.");
    }
    return fromCache;
  }

  @Override
  public boolean cacheRawBlock(BlockCacheKey key, RawHFileBlock block) {
    long startTimeNs = System.nanoTime();
    boolean cached = bucketCache.cacheBlock(key, block);
    if (LOG.isTraceEnabled()) {
      long elapsedNs = System.nanoTime() - startTimeNs;
      LOG.trace("cacheRawBlock() on hfileName=" + key.getHfileName() +
          ", offset=" + key.getOffset() + " in " + elapsedNs + " ns.");
    }
    return cached;
  }

  @Override
  public boolean evictRawBlock(BlockCacheKey cacheKey) {
    long startTimeNs = System.nanoTime();
    boolean evicted = bucketCache.evictBlock(cacheKey);
    if (LOG.isTraceEnabled()) {
      long elapsedNs = System.nanoTime() - startTimeNs;
      LOG.trace("evictBlock() of " + cacheKey + " in " + elapsedNs + " ns.");
    }
    return evicted;
  }

  @Override
  public int evictBlocksByHfileName(String hfileName) {
    long startTimeNs = System.nanoTime(); // Measure eviction perf
    int numEvicted = bucketCache.evictBlocksByHfileName(hfileName);
    if (LOG.isTraceEnabled()) {
      long elapsedNs = System.nanoTime() - startTimeNs;
      LOG.trace("evictBlockByHfileName() on hfileName=" + hfileName + ": " +
          numEvicted + " blocks evicted in " + elapsedNs + " ns.");
    }
    return numEvicted;
  }

  @Override
  public boolean isEnabled() {
    return bucketCache.isEnabled();
  }

  public void shutdown() {
    bucketCache.shutdown();
  }

  public LruBlockCache.CacheStats getStats() {
    return bucketCache.getStats();
  }

  public long size() {
    return bucketCache.getBlockCount();
  }

  public long getFreeSize() {
    return bucketCache.getFreeSize();
  }

  public long getCurrentSize() {
    return bucketCache.size();
  }

  public long getEvictedCount() {
    return bucketCache.getEvictedCount();
  }
}
