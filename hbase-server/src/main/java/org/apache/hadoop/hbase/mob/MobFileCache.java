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
package org.apache.hadoop.hbase.mob;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.IdLock;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * The cache for mob files.
 * This cache doesn't cache the mob file blocks. It only caches the references of mob files.
 * We are doing this to avoid opening and closing mob files all the time. We just keep
 * references open.
 */
@InterfaceAudience.Private
public class MobFileCache {

  private static final Log LOG = LogFactory.getLog(MobFileCache.class);

  /*
   * Eviction and statistics thread. Periodically run to print the statistics and
   * evict the lru cached mob files when the count of the cached files is larger
   * than the threshold.
   */
  static class EvictionThread extends Thread {
    MobFileCache lru;

    public EvictionThread(MobFileCache lru) {
      super("MobFileCache.EvictionThread");
      setDaemon(true);
      this.lru = lru;
    }

    @Override
    public void run() {
      lru.evict();
    }
  }

  // a ConcurrentHashMap, accesses to this map are synchronized.
  private Map<String, CachedMobFile> map = null;
  // caches access count
  private final AtomicLong count = new AtomicLong(0);
  private long lastAccess = 0;
  private final AtomicLong miss = new AtomicLong(0);
  private long lastMiss = 0;
  private final AtomicLong evictedFileCount = new AtomicLong(0);
  private long lastEvictedFileCount = 0;

  // a lock to sync the evict to guarantee the eviction occurs in sequence.
  // the method evictFile is not sync by this lock, the ConcurrentHashMap does the sync there.
  private final ReentrantLock evictionLock = new ReentrantLock(true);

  //stripes lock on each mob file based on its hash. Sync the openFile/closeFile operations.
  private final IdLock keyLock = new IdLock();

  private final ScheduledExecutorService scheduleThreadPool = Executors.newScheduledThreadPool(1,
      new ThreadFactoryBuilder().setNameFormat("MobFileCache #%d").setDaemon(true).build());
  private final Configuration conf;

  // the count of the cached references to mob files
  private final int mobFileMaxCacheSize;
  private final boolean isCacheEnabled;
  private float evictRemainRatio;

  public MobFileCache(Configuration conf) {
    this.conf = conf;
    this.mobFileMaxCacheSize = conf.getInt(MobConstants.MOB_FILE_CACHE_SIZE_KEY,
        MobConstants.DEFAULT_MOB_FILE_CACHE_SIZE);
    isCacheEnabled = (mobFileMaxCacheSize > 0);
    map = new ConcurrentHashMap<String, CachedMobFile>(mobFileMaxCacheSize);
    if (isCacheEnabled) {
      long period = conf.getLong(MobConstants.MOB_CACHE_EVICT_PERIOD,
          MobConstants.DEFAULT_MOB_CACHE_EVICT_PERIOD); // in seconds
      evictRemainRatio = conf.getFloat(MobConstants.MOB_CACHE_EVICT_REMAIN_RATIO,
          MobConstants.DEFAULT_EVICT_REMAIN_RATIO);
      if (evictRemainRatio < 0.0) {
        evictRemainRatio = 0.0f;
        LOG.warn(MobConstants.MOB_CACHE_EVICT_REMAIN_RATIO + " is less than 0.0, 0.0 is used.");
      } else if (evictRemainRatio > 1.0) {
        evictRemainRatio = 1.0f;
        LOG.warn(MobConstants.MOB_CACHE_EVICT_REMAIN_RATIO + " is larger than 1.0, 1.0 is used.");
      }
      this.scheduleThreadPool.scheduleAtFixedRate(new EvictionThread(this), period, period,
          TimeUnit.SECONDS);
    }
    LOG.info("MobFileCache is initialized, and the cache size is " + mobFileMaxCacheSize);
  }

  /**
   * Evicts the lru cached mob files when the count of the cached files is larger
   * than the threshold.
   */
  public void evict() {
    if (isCacheEnabled) {
      // Ensure only one eviction at a time
      if (!evictionLock.tryLock()) {
        return;
      }
      printStatistics();
      List<CachedMobFile> evictedFiles = new ArrayList<CachedMobFile>();
      try {
        if (map.size() <= mobFileMaxCacheSize) {
          return;
        }
        List<CachedMobFile> files = new ArrayList<CachedMobFile>(map.values());
        Collections.sort(files);
        int start = (int) (mobFileMaxCacheSize * evictRemainRatio);
        if (start >= 0) {
          for (int i = start; i < files.size(); i++) {
            String name = files.get(i).getFileName();
            CachedMobFile evictedFile = map.remove(name);
            if (evictedFile != null) {
              evictedFiles.add(evictedFile);
            }
          }
        }
      } finally {
        evictionLock.unlock();
      }
      // EvictionLock is released. Close the evicted files one by one.
      // The closes are sync in the closeFile method.
      for (CachedMobFile evictedFile : evictedFiles) {
        closeFile(evictedFile);
      }
      evictedFileCount.addAndGet(evictedFiles.size());
    }
  }

  /**
   * Evicts the cached file by the name.
   * @param fileName The name of a cached file.
   */
  public void evictFile(String fileName) {
    if (isCacheEnabled) {
      IdLock.Entry lockEntry = null;
      try {
        // obtains the lock to close the cached file.
        lockEntry = keyLock.getLockEntry(fileName.hashCode());
        CachedMobFile evictedFile = map.remove(fileName);
        if (evictedFile != null) {
          evictedFile.close();
          evictedFileCount.incrementAndGet();
        }
      } catch (IOException e) {
        LOG.error("Failed to evict the file " + fileName, e);
      } finally {
        if (lockEntry != null) {
          keyLock.releaseLockEntry(lockEntry);
        }
      }
    }
  }

  /**
   * Opens a mob file.
   * @param fs The current file system.
   * @param path The file path.
   * @param cacheConf The current MobCacheConfig
   * @return A opened mob file.
   * @throws IOException
   */
  public MobFile openFile(FileSystem fs, Path path, MobCacheConfig cacheConf) throws IOException {
    if (!isCacheEnabled) {
      MobFile mobFile = MobFile.create(fs, path, conf, cacheConf);
      mobFile.open();
      return mobFile;
    } else {
      String fileName = path.getName();
      CachedMobFile cached = map.get(fileName);
      IdLock.Entry lockEntry = keyLock.getLockEntry(fileName.hashCode());
      try {
        if (cached == null) {
          cached = map.get(fileName);
          if (cached == null) {
            if (map.size() > mobFileMaxCacheSize) {
              evict();
            }
            cached = CachedMobFile.create(fs, path, conf, cacheConf);
            cached.open();
            map.put(fileName, cached);
            miss.incrementAndGet();
          }
        }
        cached.open();
        cached.access(count.incrementAndGet());
      } finally {
        keyLock.releaseLockEntry(lockEntry);
      }
      return cached;
    }
  }

  /**
   * Closes a mob file.
   * @param file The mob file that needs to be closed.
   */
  public void closeFile(MobFile file) {
    IdLock.Entry lockEntry = null;
    try {
      if (!isCacheEnabled) {
        file.close();
      } else {
        lockEntry = keyLock.getLockEntry(file.getFileName().hashCode());
        file.close();
      }
    } catch (IOException e) {
      LOG.error("MobFileCache, Exception happen during close " + file.getFileName(), e);
    } finally {
      if (lockEntry != null) {
        keyLock.releaseLockEntry(lockEntry);
      }
    }
  }

  public void shutdown() {
    this.scheduleThreadPool.shutdown();
    for (int i = 0; i < 100; i++) {
      if (!this.scheduleThreadPool.isShutdown()) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          LOG.warn("Interrupted while sleeping");
          Thread.currentThread().interrupt();
          break;
        }
      }
    }

    if (!this.scheduleThreadPool.isShutdown()) {
      List<Runnable> runnables = this.scheduleThreadPool.shutdownNow();
      LOG.debug("Still running " + runnables);
    }
  }

  /**
   * Gets the count of cached mob files.
   * @return The count of the cached mob files.
   */
  public int getCacheSize() {
    return map == null ? 0 : map.size();
  }

  /**
   * Gets the count of accesses to the mob file cache.
   * @return The count of accesses to the mob file cache.
   */
  public long getAccessCount() {
    return count.get();
  }

  /**
   * Gets the count of misses to the mob file cache.
   * @return The count of misses to the mob file cache.
   */
  public long getMissCount() {
    return miss.get();
  }

  /**
   * Gets the number of items evicted from the mob file cache.
   * @return The number of items evicted from the mob file cache.
   */
  public long getEvictedFileCount() {
    return evictedFileCount.get();
  }

  /**
   * Gets the hit ratio to the mob file cache.
   * @return The hit ratio to the mob file cache.
   */
  public double getHitRatio() {
    return count.get() == 0 ? 0 : ((float) (count.get() - miss.get())) / (float) count.get();
  }

  /**
   * Prints the statistics.
   */
  public void printStatistics() {
    long access = count.get() - lastAccess;
    long missed = miss.get() - lastMiss;
    long evicted = evictedFileCount.get() - lastEvictedFileCount;
    int hitRatio = access == 0 ? 0 : (int) (((float) (access - missed)) / (float) access * 100);
    LOG.info("MobFileCache Statistics, access: " + access + ", miss: " + missed + ", hit: "
        + (access - missed) + ", hit ratio: " + hitRatio + "%, evicted files: " + evicted);
    lastAccess += access;
    lastMiss += missed;
    lastEvictedFileCount += evicted;
  }

}
