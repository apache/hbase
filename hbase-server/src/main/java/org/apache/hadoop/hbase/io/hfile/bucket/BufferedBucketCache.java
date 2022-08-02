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
package org.apache.hadoop.hbase.io.hfile.bucket;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.Cacheable;

/**
 * A {@link BucketCache} with RAMBuffer to reduce GC pressure.
 */
@InterfaceAudience.Private
public class BufferedBucketCache extends BucketCache {
  private static final Log LOG = LogFactory.getLog(BufferedBucketCache.class);

  static final String RAM_BUFFER_SIZE_RATIO = "hbase.bucketcache.rambuffer.ratio";
  static final double RAM_BUFFER_SIZE_RATIO_DEFAULT = 0.1;
  static final String RAM_BUFFER_TIMEOUT = "hbase.bucketcache.rambuffer.timeout"; // in seconds.
  static final int RAM_BUFFER_TIMEOUT_DEFAULT = 60;

  private final Cache<BlockCacheKey, Cacheable> ramBuffer;
  private final long maxBufferSize;

  private final AtomicLong ramBufferEvictCount = new AtomicLong(0);

  private volatile float ramBufferThreshold;

  private transient final ScheduledExecutorService scheduleThreadPool =
    Executors.newScheduledThreadPool(1,
      new ThreadFactoryBuilder().setNameFormat("RAMBufferAdjustExecutor").setDaemon(true).build());

  public BufferedBucketCache(String ioEngineName, long capacity, int blockSize, int[] bucketSizes,
      int writerThreadNum, int writerQLen, String persistencePath, int ioErrorsTolerationDuration,
      Configuration conf) throws IOException {
    super(ioEngineName, capacity, blockSize, bucketSizes, writerThreadNum, writerQLen,
      persistencePath, ioErrorsTolerationDuration, conf);

    maxBufferSize = (long) ((capacity / (double) blockSize) * conf.getDouble(RAM_BUFFER_SIZE_RATIO,
      RAM_BUFFER_SIZE_RATIO_DEFAULT));
    int timeout = conf.getInt(RAM_BUFFER_TIMEOUT, RAM_BUFFER_TIMEOUT_DEFAULT);
    ramBuffer = CacheBuilder.newBuilder().
      expireAfterAccess(timeout, TimeUnit.SECONDS).
      maximumSize(maxBufferSize).
      removalListener(new RemovalListener<BlockCacheKey, Cacheable>() {
        @Override
        public void onRemoval(RemovalNotification<BlockCacheKey, Cacheable> removalNotification) {
          ramBufferEvictCount.incrementAndGet();
        }
      }).build();

    // Adjust the cache threshold every minute.
    scheduleThreadPool.scheduleAtFixedRate(
      new RAMBufferAdjustThread(this), 60, 60,TimeUnit.SECONDS);
  }

  @Override
  public Cacheable getBlock(BlockCacheKey key, boolean caching, boolean repeat,
    boolean updateCacheMetrics) {
    Cacheable block = ramBuffer.getIfPresent(key);
    if (block != null) {
      if (updateCacheMetrics) {
        this.getStats().hit(caching, key.isPrimary(), key.getBlockType());
      }
      return block;
    }
    block = super.getBlock(key, caching, repeat, updateCacheMetrics);
    if (block != null && ramBuffer.size() < maxBufferSize * ramBufferThreshold) {
      ramBuffer.put(key, block);
    }
    return block;
  }

  private void updateRAMBufferThreshold(final float newThreshold) {
    this.ramBufferThreshold = Math.max(Math.min(newThreshold, 1.0f), 0.01f);
  }

  static class RAMBufferAdjustThread extends Thread {
    private final BufferedBucketCache bucketCache;

    RAMBufferAdjustThread(BufferedBucketCache bucketCache) {
      this.bucketCache = bucketCache;
    }

    @Override
    public void run() {
      long currentEvictCount = bucketCache.ramBufferEvictCount.get();
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        LOG.info(e);
        return;
      }
      long delta = (bucketCache.ramBufferEvictCount.get() - currentEvictCount) / 10;
      if (delta > 100) {
        bucketCache.updateRAMBufferThreshold((float) (bucketCache.ramBufferThreshold * 0.9));
      } else if (delta < 10) {
        bucketCache.updateRAMBufferThreshold((float) (bucketCache.ramBufferThreshold * 1.1));
      }
    }
  }
}
