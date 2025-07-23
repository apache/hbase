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
package org.apache.hadoop.hbase.io.util;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.MemStoreLAB;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Util class to calculate memory size for memstore(on heap, off heap), block cache(L1, L2) of RS.
 */
@InterfaceAudience.Private
public class MemorySizeUtil {

  public static final String MEMSTORE_SIZE_KEY = "hbase.regionserver.global.memstore.size";
  public static final String MEMSTORE_SIZE_OLD_KEY =
    "hbase.regionserver.global.memstore.upperLimit";
  public static final String MEMSTORE_SIZE_LOWER_LIMIT_KEY =
    "hbase.regionserver.global.memstore.size.lower.limit";
  public static final String MEMSTORE_SIZE_LOWER_LIMIT_OLD_KEY =
    "hbase.regionserver.global.memstore.lowerLimit";
  // Max global off heap memory that can be used for all memstores
  // This should be an absolute value in MBs and not percent.
  public static final String OFFHEAP_MEMSTORE_SIZE_KEY =
    "hbase.regionserver.offheap.global.memstore.size";

  public static final float DEFAULT_MEMSTORE_SIZE = 0.4f;
  // Default lower water mark limit is 95% size of memstore size.
  public static final float DEFAULT_MEMSTORE_SIZE_LOWER_LIMIT = 0.95f;

  /**
   * Configuration key for the absolute amount of heap memory that must remain free for a
   * RegionServer to start
   */
  public static final String HBASE_REGION_SERVER_FREE_HEAP_MIN_MEMORY_SIZE_KEY =
    "hbase.regionserver.free.heap.min.memory.size";

  private static final Logger LOG = LoggerFactory.getLogger(MemorySizeUtil.class);

  private static final String JVM_HEAP_EXCEPTION = "Got an exception while attempting to read "
    + "information about the JVM heap. Please submit this log information in a bug report and "
    + "include your JVM settings, specifically the GC in use and any -XX options. Consider "
    + "restarting the service.";

  /**
   * Return JVM memory statistics while properly handling runtime exceptions from the JVM.
   * @return a memory usage object, null if there was a runtime exception. (n.b. you could also get
   *         -1 values back from the JVM)
   * @see MemoryUsage
   */
  public static MemoryUsage safeGetHeapMemoryUsage() {
    MemoryUsage usage = null;
    try {
      usage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
    } catch (RuntimeException exception) {
      LOG.warn(JVM_HEAP_EXCEPTION, exception);
    }
    return usage;
  }

  /**
   * Validates that heap allocations for MemStore and block cache do not exceed the allowed limit,
   * ensuring enough free heap remains for other RegionServer tasks.
   * @param conf the configuration to validate
   * @throws RuntimeException if the combined allocation exceeds the threshold
   */
  public static void validateRegionServerHeapMemoryAllocation(Configuration conf) {
    if (conf.get(MEMSTORE_SIZE_OLD_KEY) != null) {
      LOG.warn(MEMSTORE_SIZE_OLD_KEY + " is deprecated by " + MEMSTORE_SIZE_KEY);
    }
    float memStoreFraction = getGlobalMemStoreHeapPercent(conf, false);
    float blockCacheFraction = getBlockCacheHeapPercent(conf);
    float minFreeHeapFraction = getRegionServerMinFreeHeapFraction(conf);

    int memStorePercent = (int) (memStoreFraction * 100);
    int blockCachePercent = (int) (blockCacheFraction * 100);
    int minFreeHeapPercent = (int) (minFreeHeapFraction * 100);
    int usedPercent = memStorePercent + blockCachePercent;
    int maxAllowedUsed = 100 - minFreeHeapPercent;

    if (usedPercent > maxAllowedUsed) {
      throw new RuntimeException(String.format(
        "RegionServer heap memory allocation is invalid: total memory usage exceeds 100%% "
          + "(memStore + blockCache + requiredFreeHeap). "
          + "Check the following configuration values:%n" + "  - %s = %.2f%n" + "  - %s = %s%n"
          + "  - %s = %s%n" + "  - %s = %s",
        MEMSTORE_SIZE_KEY, memStoreFraction, HConstants.HFILE_BLOCK_CACHE_MEMORY_SIZE_KEY,
        conf.get(HConstants.HFILE_BLOCK_CACHE_MEMORY_SIZE_KEY),
        HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, conf.get(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY),
        HBASE_REGION_SERVER_FREE_HEAP_MIN_MEMORY_SIZE_KEY,
        conf.get(HBASE_REGION_SERVER_FREE_HEAP_MIN_MEMORY_SIZE_KEY)));
    }
  }

  /**
   * Retrieve an explicit minimum required free heap size in bytes in the configuration.
   * @param conf used to read configs
   * @return the minimum required free heap size in bytes, or a negative value if not configured.
   * @throws IllegalArgumentException if HBASE_REGION_SERVER_FREE_HEAP_MIN_MEMORY_SIZE_KEY format is
   *                                  invalid
   */
  private static long getRegionServerMinFreeHeapInBytes(Configuration conf) {
    final String key = HBASE_REGION_SERVER_FREE_HEAP_MIN_MEMORY_SIZE_KEY;
    try {
      return Long.parseLong(conf.get(key));
    } catch (NumberFormatException e) {
      return (long) conf.getStorageSize(key, -1, StorageUnit.BYTES);
    }
  }

  /**
   * Returns the minimum required free heap as a fraction of total heap.
   */
  public static float getRegionServerMinFreeHeapFraction(final Configuration conf) {
    final MemoryUsage usage = safeGetHeapMemoryUsage();
    if (usage == null) {
      return 0;
    }

    long minFreeHeapInBytes = getRegionServerMinFreeHeapInBytes(conf);
    if (minFreeHeapInBytes >= 0) {
      return (float) minFreeHeapInBytes / usage.getMax();
    }

    return HConstants.HBASE_CLUSTER_MINIMUM_MEMORY_THRESHOLD;
  }

  /**
   * Retrieve global memstore configured size as percentage of total heap.
   */
  public static float getGlobalMemStoreHeapPercent(final Configuration c,
    final boolean logInvalid) {
    float limit =
      c.getFloat(MEMSTORE_SIZE_KEY, c.getFloat(MEMSTORE_SIZE_OLD_KEY, DEFAULT_MEMSTORE_SIZE));
    if (limit > 0.8f || limit <= 0.0f) {
      if (logInvalid) {
        LOG.warn("Setting global memstore limit to default of " + DEFAULT_MEMSTORE_SIZE
          + " because supplied value outside allowed range of (0 -> 0.8]");
      }
      limit = DEFAULT_MEMSTORE_SIZE;
    }
    return limit;
  }

  /**
   * Retrieve configured size for global memstore lower water mark as fraction of global memstore
   * size.
   */
  public static float getGlobalMemStoreHeapLowerMark(final Configuration conf,
    boolean honorOldConfig) {
    String lowMarkPercentStr = conf.get(MEMSTORE_SIZE_LOWER_LIMIT_KEY);
    if (lowMarkPercentStr != null) {
      float lowMarkPercent = Float.parseFloat(lowMarkPercentStr);
      if (lowMarkPercent > 1.0f) {
        LOG.error("Bad configuration value for " + MEMSTORE_SIZE_LOWER_LIMIT_KEY + ": "
          + lowMarkPercent + ". Using 1.0f instead.");
        lowMarkPercent = 1.0f;
      }
      return lowMarkPercent;
    }
    if (!honorOldConfig) return DEFAULT_MEMSTORE_SIZE_LOWER_LIMIT;
    String lowerWaterMarkOldValStr = conf.get(MEMSTORE_SIZE_LOWER_LIMIT_OLD_KEY);
    if (lowerWaterMarkOldValStr != null) {
      LOG.warn(MEMSTORE_SIZE_LOWER_LIMIT_OLD_KEY + " is deprecated. Instead use "
        + MEMSTORE_SIZE_LOWER_LIMIT_KEY);
      float lowerWaterMarkOldVal = Float.parseFloat(lowerWaterMarkOldValStr);
      float upperMarkPercent = getGlobalMemStoreHeapPercent(conf, false);
      if (lowerWaterMarkOldVal > upperMarkPercent) {
        lowerWaterMarkOldVal = upperMarkPercent;
        LOG.error("Value of " + MEMSTORE_SIZE_LOWER_LIMIT_OLD_KEY + " (" + lowerWaterMarkOldVal
          + ") is greater than global memstore limit (" + upperMarkPercent + ") set by "
          + MEMSTORE_SIZE_KEY + "/" + MEMSTORE_SIZE_OLD_KEY + ". Setting memstore lower limit "
          + "to " + upperMarkPercent);
      }
      return lowerWaterMarkOldVal / upperMarkPercent;
    }
    return DEFAULT_MEMSTORE_SIZE_LOWER_LIMIT;
  }

  /** Returns Pair of global memstore size and memory type(ie. on heap or off heap). */
  public static Pair<Long, MemoryType> getGlobalMemStoreSize(Configuration conf) {
    long offheapMSGlobal = conf.getLong(OFFHEAP_MEMSTORE_SIZE_KEY, 0);// Size in MBs
    if (offheapMSGlobal > 0) {
      // Off heap memstore size has not relevance when MSLAB is turned OFF. We will go with making
      // this entire size split into Chunks and pooling them in MemstoreLABPoool. We dont want to
      // create so many on demand off heap chunks. In fact when this off heap size is configured, we
      // will go with 100% of this size as the pool size
      if (MemStoreLAB.isEnabled(conf)) {
        // We are in offheap Memstore use
        long globalMemStoreLimit = (long) (offheapMSGlobal * 1024 * 1024); // Size in bytes
        return new Pair<>(globalMemStoreLimit, MemoryType.NON_HEAP);
      } else {
        // Off heap max memstore size is configured with turning off MSLAB. It makes no sense. Do a
        // warn log and go with on heap memstore percentage. By default it will be 40% of Xmx
        LOG.warn("There is no relevance of configuring '" + OFFHEAP_MEMSTORE_SIZE_KEY + "' when '"
          + MemStoreLAB.USEMSLAB_KEY + "' is turned off."
          + " Going with on heap global memstore size ('" + MEMSTORE_SIZE_KEY + "')");
      }
    }
    return new Pair<>(getOnheapGlobalMemStoreSize(conf), MemoryType.HEAP);
  }

  /**
   * Returns the onheap global memstore limit based on the config
   * 'hbase.regionserver.global.memstore.size'.
   * @return the onheap global memstore limt
   */
  public static long getOnheapGlobalMemStoreSize(Configuration conf) {
    long max = -1L;
    final MemoryUsage usage = safeGetHeapMemoryUsage();
    if (usage != null) {
      max = usage.getMax();
    }
    float globalMemStorePercent = getGlobalMemStoreHeapPercent(conf, true);
    return ((long) (max * globalMemStorePercent));
  }

  /**
   * Retrieve configured size for on heap block cache as percentage of total heap.
   */
  public static float getBlockCacheHeapPercent(final Configuration conf) {
    // Check if an explicit block cache size is configured.
    long l1CacheSizeInBytes = getBlockCacheSizeInBytes(conf);
    if (l1CacheSizeInBytes > 0) {
      final MemoryUsage usage = safeGetHeapMemoryUsage();
      return usage == null ? 0 : (float) l1CacheSizeInBytes / usage.getMax();
    }

    return conf.getFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY,
      HConstants.HFILE_BLOCK_CACHE_SIZE_DEFAULT);
  }

  /**
   * Retrieve an explicit block cache size in bytes in the configuration.
   * @param conf used to read cache configs
   * @return the number of bytes to use for LRU, negative if disabled.
   * @throws IllegalArgumentException if HFILE_BLOCK_CACHE_MEMORY_SIZE_KEY format is invalid
   */
  public static long getBlockCacheSizeInBytes(Configuration conf) {
    final String key = HConstants.HFILE_BLOCK_CACHE_MEMORY_SIZE_KEY;
    try {
      return Long.parseLong(conf.get(key));
    } catch (NumberFormatException e) {
      return (long) conf.getStorageSize(key, -1, StorageUnit.BYTES);
    }
  }

  /**
   * @param conf used to read cache configs
   * @return the number of bytes to use for LRU, negative if disabled.
   * @throws IllegalArgumentException if HFILE_BLOCK_CACHE_SIZE_KEY is > 1.0
   */
  public static long getOnHeapCacheSize(final Configuration conf) {
    final float cachePercentage = getBlockCacheHeapPercent(conf);
    if (cachePercentage <= 0.0001f) {
      return -1;
    }
    if (cachePercentage > 1.0) {
      throw new IllegalArgumentException(
        HConstants.HFILE_BLOCK_CACHE_SIZE_KEY + " must be between 0.0 and 1.0, and not > 1.0");
    }

    final MemoryUsage usage = safeGetHeapMemoryUsage();
    if (usage == null) {
      return -1;
    }
    final long heapMax = usage.getMax();
    float onHeapCacheFixedSize =
      (float) conf.getLong(HConstants.HFILE_ONHEAP_BLOCK_CACHE_FIXED_SIZE_KEY,
        HConstants.HFILE_ONHEAP_BLOCK_CACHE_FIXED_SIZE_DEFAULT) / heapMax;
    // Calculate the amount of heap to give the heap.
    if (onHeapCacheFixedSize > 0 && onHeapCacheFixedSize < cachePercentage) {
      return (long) (heapMax * onHeapCacheFixedSize);
    } else {
      final long cacheSizeInBytes = getBlockCacheSizeInBytes(conf);
      return cacheSizeInBytes > 0 ? cacheSizeInBytes : (long) (heapMax * cachePercentage);
    }
  }

  /**
   * @param conf used to read config for bucket cache size.
   * @return the number of bytes to use for bucket cache, negative if disabled.
   */
  public static long getBucketCacheSize(final Configuration conf) {
    // Size configured in MBs
    float bucketCacheSize = conf.getFloat(HConstants.BUCKET_CACHE_SIZE_KEY, 0F);
    if (bucketCacheSize < 1) {
      throw new IllegalArgumentException("Bucket Cache should be minimum 1 MB in size."
        + "Configure 'hbase.bucketcache.size' with > 1 value");
    }
    return (long) (bucketCacheSize * 1024 * 1024);
  }
}
