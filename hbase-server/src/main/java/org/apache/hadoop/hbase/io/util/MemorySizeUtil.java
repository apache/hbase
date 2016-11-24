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
package org.apache.hadoop.hbase.io.util;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.MemStoreLAB;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Util class to calculate memory size for memstore, block cache(L1, L2) of RS.
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

  private static final Log LOG = LogFactory.getLog(MemorySizeUtil.class);
  // a constant to convert a fraction to a percentage
  private static final int CONVERT_TO_PERCENTAGE = 100;

  /**
   * Checks whether we have enough heap memory left out after portion for Memstore and Block cache.
   * We need atleast 20% of heap left out for other RS functions.
   * @param conf
   */
  public static void checkForClusterFreeHeapMemoryLimit(Configuration conf) {
    if (conf.get(MEMSTORE_SIZE_OLD_KEY) != null) {
      LOG.warn(MEMSTORE_SIZE_OLD_KEY + " is deprecated by " + MEMSTORE_SIZE_KEY);
    }
    float globalMemstoreSize = getGlobalMemStoreHeapPercent(conf, false);
    int gml = (int)(globalMemstoreSize * CONVERT_TO_PERCENTAGE);
    float blockCacheUpperLimit = getBlockCacheHeapPercent(conf);
    int bcul = (int)(blockCacheUpperLimit * CONVERT_TO_PERCENTAGE);
    if (CONVERT_TO_PERCENTAGE - (gml + bcul)
            < (int)(CONVERT_TO_PERCENTAGE *
                    HConstants.HBASE_CLUSTER_MINIMUM_MEMORY_THRESHOLD)) {
      throw new RuntimeException("Current heap configuration for MemStore and BlockCache exceeds "
          + "the threshold required for successful cluster operation. "
          + "The combined value cannot exceed 0.8. Please check "
          + "the settings for hbase.regionserver.global.memstore.size and "
          + "hfile.block.cache.size in your configuration. "
          + "hbase.regionserver.global.memstore.size is " + globalMemstoreSize
          + " hfile.block.cache.size is " + blockCacheUpperLimit);
    }
  }

  /**
   * Retrieve global memstore configured size as percentage of total heap.
   * @param c
   * @param logInvalid
   */
  public static float getGlobalMemStoreHeapPercent(final Configuration c,
      final boolean logInvalid) {
    float limit = c.getFloat(MEMSTORE_SIZE_KEY,
        c.getFloat(MEMSTORE_SIZE_OLD_KEY, DEFAULT_MEMSTORE_SIZE));
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

  /**
   * @return Pair of global memstore size and memory type(ie. on heap or off heap).
   */
  public static Pair<Long, MemoryType> getGlobalMemstoreSize(Configuration conf) {
    long offheapMSGlobal = conf.getLong(OFFHEAP_MEMSTORE_SIZE_KEY, 0);// Size in MBs
    if (offheapMSGlobal > 0) {
      // Off heap memstore size has not relevance when MSLAB is turned OFF. We will go with making
      // this entire size split into Chunks and pooling them in MemstoreLABPoool. We dont want to
      // create so many on demand off heap chunks. In fact when this off heap size is configured, we
      // will go with 100% of this size as the pool size
      if (MemStoreLAB.isEnabled(conf)) {
        // We are in offheap Memstore use
        long globalMemStoreLimit = (long) (offheapMSGlobal * 1024 * 1024); // Size in bytes
        return new Pair<Long, MemoryType>(globalMemStoreLimit, MemoryType.NON_HEAP);
      } else {
        // Off heap max memstore size is configured with turning off MSLAB. It makes no sense. Do a
        // warn log and go with on heap memstore percentage. By default it will be 40% of Xmx
        LOG.warn("There is no relevance of configuring '" + OFFHEAP_MEMSTORE_SIZE_KEY + "' when '"
            + MemStoreLAB.USEMSLAB_KEY + "' is turned off."
            + " Going with on heap global memstore size ('" + MEMSTORE_SIZE_KEY + "')");
      }
    }
    long max = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
    float globalMemStorePercent = getGlobalMemStoreHeapPercent(conf, true);
    return new Pair<Long, MemoryType>((long) (max * globalMemStorePercent), MemoryType.HEAP);
  }

  /**
   * Retrieve configured size for on heap block cache as percentage of total heap.
   * @param conf
   */
  public static float getBlockCacheHeapPercent(final Configuration conf) {
    // L1 block cache is always on heap
    float l1CachePercent = conf.getFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY,
        HConstants.HFILE_BLOCK_CACHE_SIZE_DEFAULT);
    float l2CachePercent = getL2BlockCacheHeapPercent(conf);
    return l1CachePercent + l2CachePercent;
  }

  /**
   * @param conf
   * @return The on heap size for L2 block cache.
   */
  public static float getL2BlockCacheHeapPercent(Configuration conf) {
    float l2CachePercent = 0.0F;
    String bucketCacheIOEngineName = conf.get(HConstants.BUCKET_CACHE_IOENGINE_KEY, null);
    // L2 block cache can be on heap when IOEngine is "heap"
    if (bucketCacheIOEngineName != null && bucketCacheIOEngineName.startsWith("heap")) {
      float bucketCachePercentage = conf.getFloat(HConstants.BUCKET_CACHE_SIZE_KEY, 0F);
      MemoryUsage mu = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
      l2CachePercent = bucketCachePercentage < 1 ? bucketCachePercentage
          : (bucketCachePercentage * 1024 * 1024) / mu.getMax();
    }
    return l2CachePercent;
  }
}
