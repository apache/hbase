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
package org.apache.hadoop.hbase.io.hfile;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * In-memory BlockCache that may be backed by secondary layer(s).
 */
@InterfaceAudience.Private
public abstract class FirstLevelBlockCache implements ResizableBlockCache, HeapSize {

  /* Statistics thread */
  protected static String STAT_THREAD_ENABLE_KEY = "hbase.lru.stat.enable";
  protected static boolean STAT_THREAD_ENABLE_DEFAULT = false;
  protected static final int STAT_THREAD_PERIOD = 60 * 5;

  protected transient ScheduledExecutorService statsThreadPool;

  FirstLevelBlockCache(boolean statEnabled) {
    if (statEnabled) {
      this.statsThreadPool = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder()
        .setNameFormat("LruBlockCacheStatsExecutor").setDaemon(true).build());
      this.statsThreadPool.scheduleAtFixedRate(new LruBlockCache.StatisticsThread(this),
        STAT_THREAD_PERIOD, STAT_THREAD_PERIOD, TimeUnit.SECONDS);
    }
  }

  /**
   * Whether the cache contains the block with specified cacheKey
   *
   * @param cacheKey cache key for the block
   * @return true if it contains the block
   */
  abstract boolean containsBlock(BlockCacheKey cacheKey);

  /**
   * Specifies the secondary cache. An entry that is evicted from this cache due to a size
   * constraint will be inserted into the victim cache.
   *
   * @param victimCache the second level cache
   * @throws IllegalArgumentException if the victim cache had already been set
   */
  abstract void setVictimCache(BlockCache victimCache);

  public void shutdown() {
    if (statsThreadPool != null) {
      this.statsThreadPool.shutdown();
      for (int i = 0; i < 10; i++) {
        if (!this.statsThreadPool.isShutdown()) {
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      }
    }
  }

  /*
   * Statistics thread.  Periodically prints the cache statistics to the log.
   */
  static class StatisticsThread extends Thread {

    private final FirstLevelBlockCache l1;

    public StatisticsThread(FirstLevelBlockCache l1) {
      super("LruBlockCacheStats");
      setDaemon(true);
      this.l1 = l1;
    }

    @Override
    public void run() {
      l1.logStats();
    }
  }

  protected abstract void logStats();
}
