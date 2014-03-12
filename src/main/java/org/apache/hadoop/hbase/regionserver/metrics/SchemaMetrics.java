/*
 * Copyright 2011 The Apache Software Foundation
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

package org.apache.hadoop.hbase.regionserver.metrics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.BlockType.BlockCategory;
import org.apache.hadoop.hbase.io.hfile.LruBlockCache;
import org.apache.hadoop.hbase.io.hfile.LruBlockCacheFactory;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.base.Preconditions;

/**
 * A collection of metric names in a given column family or a (table, column
 * family) combination. The following "dimensions" are supported:
 * <ul>
 * <li>Table name (optional; enabled based on configuration)</li>
 * <li>Per-column family vs. aggregated. The aggregated mode is only supported
 * when table name is not included.</li>
 * <li>Block category (data, index, bloom filter, etc.)</li>
 * <li>Whether the request is part of a compaction</li>
 * <li>Metric type (read time, block read count, cache hits/misses, etc.)</li>
 * </ul>
 * <p>
 * An instance of this class does not store any metric values. It just allows
 * to determine the correct metric name for each combination of the above
 * dimensions.
 * <p>
 * <table>
 * <tr>
 * <th rowspan="2">Metric key</th>
 * <th colspan="2">Per-table metrics conf setting</th>
 * <th rowspan="2">Description</th>
 * </tr>
 * <tr>
 * <th>On</th>
 * <th>Off</th>
 * </th>
 * <tr>
 *   <td> tab.T.cf.CF.M </td> <td> Include </td> <td> Skip    </td>
 *   <td> A specific column family of a specific table        </td>
 * </tr>
 * <tr>
 *   <td> tab.T.M       </td> <td> Skip    </td> <td> Skip    </td>
 *   <td> All column families in the given table              </td>
 * </tr>
 * <tr>
 *   <td> cf.CF.M       </td> <td> Skip    </td> <td> Include </td>
 *   <td> A specific column family in all tables              </td>
 * </tr>
 * <tr>
 *   <td> M             </td> <td> Include </td> <td> Include </td>
 *   <td> All column families in all tables                   </td>
 * </tr>
 * </table>
 */
public class SchemaMetrics {

  /**
   * Used in place of the compaction vs. non-compaction flag for metrics that do not distinguish
   * between these two cases.
   */
  private static final boolean DEFAULT_COMPACTION_FLAG = false;

  public interface SchemaAware {
    public String getTableName();
    public String getColumnFamilyName();
    public SchemaMetrics getSchemaMetrics();
  }

  private static final Log LOG = LogFactory.getLog(SchemaMetrics.class);

  private static final int COMPACTION_AWARE_METRIC_FLAG = 0x01;
  private static final int TIME_VARYING_METRIC_FLAG = 0x02;
  private static final int PERSISTENT_METRIC_FLAG = 0x04;

  public static enum BlockMetricType {
    READ_TIME("Read", COMPACTION_AWARE_METRIC_FLAG | TIME_VARYING_METRIC_FLAG),

    READ_COUNT("BlockReadCnt", COMPACTION_AWARE_METRIC_FLAG),
    CACHE_HIT("BlockReadCacheHitCnt", COMPACTION_AWARE_METRIC_FLAG),
    CACHE_MISS("BlockReadCacheMissCnt", COMPACTION_AWARE_METRIC_FLAG),

    CACHE_SIZE("blockCacheSize", PERSISTENT_METRIC_FLAG),
    UNENCODED_CACHE_SIZE("blockCacheUnencodedSize", PERSISTENT_METRIC_FLAG),
    CACHE_NUM_BLOCKS("cacheNumBlocks", PERSISTENT_METRIC_FLAG),
    CACHED("blockCacheNumCached"),
    EVICTED("blockCacheNumEvicted"),

    L2_READ_COUNT("L2ReadCnt", COMPACTION_AWARE_METRIC_FLAG),
    L2_CACHE_HIT("L2CacheHitCnt", COMPACTION_AWARE_METRIC_FLAG),
    L2_CACHE_MISS("L2CacheMissCnt", COMPACTION_AWARE_METRIC_FLAG),

    L2_CACHE_NUM("L2CacheNumBlocks", PERSISTENT_METRIC_FLAG),
    L2_CACHE_SIZE("L2CacheSize", PERSISTENT_METRIC_FLAG),
    L2_CACHED("L2CacheNumCached"),
    L2_EVICTED("L2CacheNumEvicted"),

    PRELOAD_CACHE_HIT("PreloadCacheHitCnt", COMPACTION_AWARE_METRIC_FLAG),
    PRELOAD_CACHE_MISS("PreloadCacheMissCnt", COMPACTION_AWARE_METRIC_FLAG),
    PRELOAD_READ_TIME("PreloadReadTime",
            COMPACTION_AWARE_METRIC_FLAG | TIME_VARYING_METRIC_FLAG);

    private final String metricStr;
    private final int flags;

    BlockMetricType(String metricStr) {
      this(metricStr, 0);
    }

    BlockMetricType(String metricStr, int flags) {
      this.metricStr = metricStr;
      this.flags = flags;
    }

    @Override
    public String toString() {
      return metricStr;
    }

    private static final String BLOCK_METRIC_TYPE_RE;
    static {
      StringBuilder sb = new StringBuilder();
      for (BlockMetricType bmt : values()) {
        if (sb.length() > 0)
          sb.append("|");
        sb.append(bmt);
      }
      BLOCK_METRIC_TYPE_RE = sb.toString();
    }

    final boolean compactionAware() {
      return (flags & COMPACTION_AWARE_METRIC_FLAG) != 0;
    }

    private final boolean timeVarying() {
      return (flags & TIME_VARYING_METRIC_FLAG) != 0;
    }

    final boolean persistent() {
      return (flags & PERSISTENT_METRIC_FLAG) != 0;
    }
  };

  public static enum StoreMetricType {
    STORE_FILE_COUNT("storeFileCount"),
    STORE_FILE_INDEX_SIZE("storeFileIndexSizeMB"),
    STORE_FILE_SIZE_MB("storeFileSizeMB"),
    STATIC_BLOOM_SIZE_KB("staticBloomSizeKB"),
    MEMSTORE_SIZE_MB("memstoreSizeMB"),
    STATIC_INDEX_SIZE_KB("staticIndexSizeKB"),
    FLUSH_SIZE("flushSize", PERSISTENT_METRIC_FLAG),
    COMPACTION_WRITE_SIZE("compactionWriteSize", PERSISTENT_METRIC_FLAG),
    STORE_COMPHOOK_KVS_TRANSFORMED("storeCompHookKVsTransformed", PERSISTENT_METRIC_FLAG),
    STORE_COMPHOOK_BYTES_SAVED("storeCompHookBytesSaved", PERSISTENT_METRIC_FLAG),
    //if exception occurred during compaction hook execution, count the number of errors
    STORE_COMPHOOK_KVS_ERRORS("storeCompKVsErrors", PERSISTENT_METRIC_FLAG);

    private final String metricStr;
    private final int flags;

    private StoreMetricType(String metricStr) {
      this(metricStr, 0);
    }

    private StoreMetricType(String metricStr, int flags) {
      this.metricStr = metricStr;
      this.flags = flags;
    }

    @Override
    public String toString() {
      return metricStr;
    }

    private final boolean persistent() {
      return (flags & PERSISTENT_METRIC_FLAG) != 0;
    }
  };

  // Constants
  /**
   * A string used when column family or table name is unknown, and in some
   * unit tests. This should not normally show up in metric names but if it
   * does it is better than creating a silent discrepancy in total vs.
   * per-CF/table metrics.
   */
  public static final String UNKNOWN = "__unknown";

  public static final String TABLE_PREFIX = "tbl.";
  public static final String CF_PREFIX = "cf.";
  public static final String BLOCK_TYPE_PREFIX = "bt.";
  public static final String CF_UNKNOWN_PREFIX = CF_PREFIX + UNKNOWN + ".";
  public static final String CF_BAD_FAMILY_PREFIX = CF_PREFIX + "__badfamily.";

  /** Use for readability when obtaining non-compaction counters */
  public static final boolean NO_COMPACTION = false;

  /**
   * A special schema metric value that means "all tables aggregated" or
   * "all column families aggregated" when used as a table name or a column
   * family name.
   */
  public static final String TOTAL_KEY = "";

  /**
   * Special handling for meta-block-specific metrics for
   * backwards-compatibility.
   */
  private static final String META_BLOCK_CATEGORY_STR = "Meta";

  private static final int NUM_BLOCK_CATEGORIES =
      BlockCategory.values().length;

  private static final int NUM_METRIC_TYPES =
      BlockMetricType.values().length;

  static final boolean[] BOOL_VALUES = new boolean[] { false, true };

  private static final int NUM_BLOCK_METRICS =
      NUM_BLOCK_CATEGORIES *  // blockCategory
      BOOL_VALUES.length *    // isCompaction
      NUM_METRIC_TYPES;       // metricType

  private static final int NUM_STORE_METRIC_TYPES =
      StoreMetricType.values().length;

  /** Conf key controlling whether we include table name in metric names */
  public static final String SHOW_TABLE_NAME_CONF_KEY =
      "hbase.metrics.showTableName";

  private static final String WORD_BOUNDARY_RE_STR = "\\b";

  private static final String COMPACTION_METRIC_PREFIX = "compaction";
  private static final String NON_COMPACTION_METRIC_PREFIX = "fs";

  // Global variables
  /**
   * Maps a string key consisting of table name and column family name, with
   * table name optionally replaced with {@link #TOTAL_KEY} if per-table
   * metrics are disabled, to an instance of this class.
   */
  private static final ConcurrentMap<String, SchemaMetrics>
      tableAndFamilyToMetrics = new ConcurrentHashMap<String, SchemaMetrics>();

  /** Metrics for all tables and column families. */
  // This has to be initialized after cfToMetrics.
  public static final SchemaMetrics ALL_SCHEMA_METRICS =
    getInstance(TOTAL_KEY, TOTAL_KEY);

  private static final Pattern PERSISTENT_METRIC_RE;
  static {
    StringBuilder sb = new StringBuilder();
    for (BlockMetricType bmt : BlockMetricType.values()) {
      if (bmt.persistent()) {
        sb.append((sb.length() == 0 ? "" : "|") + bmt);
      }
    }
    for (StoreMetricType smt : StoreMetricType.values()) {
      if (smt.persistent()) {
        sb.append((sb.length() == 0 ? "" : "|") + smt);
      }
    }
    PERSISTENT_METRIC_RE = Pattern.compile(".*" + WORD_BOUNDARY_RE_STR +
        "(" + META_BLOCK_CATEGORY_STR + ")?(" + sb + ")$");
  }

  /**
   * Whether to include table name in metric names. If this is null, it has not
   * been initialized. This is a global instance, but we also have a copy of it
   * per a {@link SchemaMetrics} object to avoid synchronization overhead.
   */
  private static volatile Boolean useTableNameGlobally;

  /** Whether we logged a message about configuration inconsistency */
  private static volatile boolean loggedConfInconsistency;

  // Instance variables
  private final String[] blockMetricNames = new String[NUM_BLOCK_METRICS];
  private final boolean[] blockMetricTimeVarying =
      new boolean[NUM_BLOCK_METRICS];

  private final String[] bloomMetricNames = new String[2];
  private final String[] storeMetricNames = new String[NUM_STORE_METRIC_TYPES];
  private final String[] storeMetricNamesMax = new String[NUM_STORE_METRIC_TYPES];
  private final String[] dataBlockEncodingMismatchMetricNames = new String[2];

  private SchemaMetrics(final String tableName, final String cfName) {
    String metricPrefix =
      SchemaMetrics.generateSchemaMetricsPrefix(tableName, cfName);

    for (BlockCategory blockCategory : BlockCategory.values()) {
      for (boolean isCompaction : BOOL_VALUES) {
        for (BlockMetricType metricType : BlockMetricType.values()) {
          if (!metricType.compactionAware() && isCompaction) {
            continue;
          }

          StringBuilder sb = new StringBuilder(metricPrefix);
          if (blockCategory != BlockCategory.ALL_CATEGORIES
              && blockCategory != BlockCategory.META) {
            String categoryStr = blockCategory.toString();
            categoryStr = categoryStr.charAt(0)
                + categoryStr.substring(1).toLowerCase();
            sb.append(BLOCK_TYPE_PREFIX + categoryStr + ".");
          }

          if (metricType.compactionAware()) {
            sb.append(isCompaction ? COMPACTION_METRIC_PREFIX : NON_COMPACTION_METRIC_PREFIX);
          }

          // A special-case for meta blocks for backwards-compatibility.
          if (blockCategory == BlockCategory.META) {
            sb.append(META_BLOCK_CATEGORY_STR);
          }

          sb.append(metricType);

          int i = getBlockMetricIndex(blockCategory, isCompaction, metricType);
          blockMetricNames[i] = sb.toString().intern();
          blockMetricTimeVarying[i] = metricType.timeVarying();
        }
      }
    }

    for (boolean isInBloom : BOOL_VALUES) {
      bloomMetricNames[isInBloom ? 1 : 0] = metricPrefix
          + (isInBloom ? "keyMaybeInBloomCnt" : "keyNotInBloomCnt");
    }

    for (boolean isCompaction : BOOL_VALUES) {
      dataBlockEncodingMismatchMetricNames[isCompaction ? 1 : 0] =
              String.format("%s%sDataBlockEncodingMismatchCnt", metricPrefix,
                      isCompaction ? COMPACTION_METRIC_PREFIX :
                              NON_COMPACTION_METRIC_PREFIX);
    }

    for (StoreMetricType storeMetric : StoreMetricType.values()) {
      String coreName = metricPrefix + storeMetric.toString();
      storeMetricNames[storeMetric.ordinal()] = coreName;
      storeMetricNamesMax[storeMetric.ordinal()] = coreName + ".max";
    }
  }

  /**
   * Returns a {@link SchemaMetrics} object for the given table and column
   * family, instantiating it if necessary.
   *
   * @param tableName table name (null is interpreted as "unknown"). This is
   *          ignored
   * @param cfName column family name (null is interpreted as "unknown")
   */
  public static SchemaMetrics getInstance(String tableName, String cfName) {
    if (tableName == null) {
      tableName = UNKNOWN;
    }

    if (cfName == null) {
      cfName = UNKNOWN;
    }

    tableName = getEffectiveTableName(tableName);

    final String instanceKey = tableName + "\t" + cfName;
    SchemaMetrics schemaMetrics = tableAndFamilyToMetrics.get(instanceKey);
    if (schemaMetrics != null) {
      return schemaMetrics;
    }

    schemaMetrics = new SchemaMetrics(tableName, cfName);
    SchemaMetrics existingMetrics =
        tableAndFamilyToMetrics.putIfAbsent(instanceKey, schemaMetrics);
    return existingMetrics != null ? existingMetrics : schemaMetrics;
  }

  private static final int getBlockMetricIndex(BlockCategory blockCategory,
      boolean isCompaction, BlockMetricType metricType) {
    int i = 0;
    i = i * NUM_BLOCK_CATEGORIES + blockCategory.ordinal();
    i = i * BOOL_VALUES.length + (isCompaction ? 1 : 0);
    i = i * NUM_METRIC_TYPES + metricType.ordinal();
    return i;
  }

  public String getBlockMetric(BlockMetricType type, BlockCategory category,
      boolean isCompaction) {
    return HRegion.getNumericMetric(getBlockMetricName(category, isCompaction,
      type)) + "";
  }

  public String getTimeVaryingBlockMetric(BlockMetricType type,
      BlockCategory category, boolean isCompaction) {
    return HRegion.getTimeVaryingMetric(getBlockMetricName(category,
      isCompaction, type)) + "";
  }

  public String getBlockMetricName(BlockCategory blockCategory,
      boolean isCompaction, BlockMetricType metricType) {
    if (isCompaction && !metricType.compactionAware()) {
      throw new IllegalArgumentException("isCompaction cannot be true for "
          + metricType);
    }
    return blockMetricNames[getBlockMetricIndex(blockCategory, isCompaction,
        metricType)];
  }

  public String getBloomMetricName(boolean isInBloom) {
    return bloomMetricNames[isInBloom ? 1 : 0];
  }

  public String getDataBlockEncodingMismatchMetricNames(boolean isCompaction) {
    return dataBlockEncodingMismatchMetricNames[isCompaction ? 1 : 0];
  }

  /**
   * Increments the given metric, both per-CF and aggregate, for both the given
   * category and all categories in aggregate (four counters total).
   */
  private void incrNumericMetric(BlockCategory blockCategory,
      boolean isCompaction, BlockMetricType metricType) {
    if (blockCategory == null) {
      blockCategory = BlockCategory.UNKNOWN;  // So that we see this in stats.
    }
    HRegion.incrNumericMetric(getBlockMetricName(blockCategory,
        isCompaction, metricType), 1);

    if (blockCategory != BlockCategory.ALL_CATEGORIES) {
      incrNumericMetric(BlockCategory.ALL_CATEGORIES, isCompaction,
          metricType);
    }
  }

  private void addToReadTime(BlockCategory blockCategory,
      boolean isCompaction, long timeMs) {
    HRegion.incrTimeVaryingMetric(getBlockMetricName(blockCategory,
        isCompaction, BlockMetricType.READ_TIME), timeMs);

    // Also update the read time aggregated across all block categories
    if (blockCategory != BlockCategory.ALL_CATEGORIES) {
      addToReadTime(BlockCategory.ALL_CATEGORIES, isCompaction, timeMs);
    }
  }

  /**
   * Used to accumulate store metrics across multiple regions in a region
   * server.  These metrics are not "persistent", i.e. we keep overriding them
   * on every update instead of incrementing, so we need to accumulate them in
   * a temporary map before pushing them to the global metric collection.
   * @param tmpMap a temporary map for accumulating store metrics
   * @param storeMetricType the store metric type to increment
   * @param val the value to add to the metric
   */
  public void accumulateStoreMetric(final Map<String, MutableDouble> tmpMap,
      StoreMetricType storeMetricType, double val) {
    final String key = getStoreMetricName(storeMetricType);
    if (tmpMap.get(key) == null) {
      tmpMap.put(key, new MutableDouble(val));
    } else {
      tmpMap.get(key).add(val);
    }

    if (this == ALL_SCHEMA_METRICS) {
      // also compute the max value across all Stores on this server
      final String maxKey = getStoreMetricNameMax(storeMetricType);
      MutableDouble cur = tmpMap.get(maxKey);
      if (cur == null) {
        tmpMap.put(maxKey, new MutableDouble(val));
      } else if (cur.doubleValue() < val) {
        cur.setValue(val);
      }
    } else {
      ALL_SCHEMA_METRICS.accumulateStoreMetric(tmpMap, storeMetricType, val);
    }
  }

  public String getStoreMetricName(StoreMetricType storeMetricType) {
    return storeMetricNames[storeMetricType.ordinal()];
  }

  public String getStoreMetricNameMax(StoreMetricType storeMetricType) {
    return storeMetricNamesMax[storeMetricType.ordinal()];
  }

  /**
   * Update a metric that does not get reset on every poll.
   * @param storeMetricType the store metric to update
   * @param value the value to update the metric to
   */
  public void updatePersistentStoreMetric(StoreMetricType storeMetricType,
      long value) {
    Preconditions.checkArgument(storeMetricType.persistent());
    HRegion.incrNumericPersistentMetric(
        storeMetricNames[storeMetricType.ordinal()], value);
  }

  /**
   * Updates metrics for cacheHit/cacheMiss when a block is read.
   * @param blockCategory category of the block read
   * @param isCompaction whether this is compaction read or not
   * @param timeMs time taken to read the block
   * @param l1Cached whether this block was read from cache or not
   * @param l2Cached whether this block was read from L2 cache or not
   * @param preload whether this a preloaded block or not
   */
  public void updateOnBlockRead(BlockCategory blockCategory,
      boolean isCompaction, long timeMs, boolean l1Cached, boolean l2Cached,
      boolean preload) {
    addToReadTime(blockCategory, isCompaction, timeMs);
    if (l1Cached || l2Cached) {
      if (l1Cached) {
        updateOnCacheHit(blockCategory, isCompaction);
      }
      if (l2Cached) {
        updateOnL2CacheHit(blockCategory, isCompaction);
      }
      if (preload) {
        updateOnPreloadCacheHit(blockCategory, isCompaction, timeMs);
      }
    } else {
      updateOnCacheMiss(blockCategory, isCompaction);
      updateOnL2CacheMiss(blockCategory, isCompaction);
      if (preload) {
        updateOnPreloadCacheMiss(blockCategory, isCompaction, timeMs);
      }
    }

    if (this != ALL_SCHEMA_METRICS) {
      ALL_SCHEMA_METRICS.updateOnBlockRead(blockCategory, isCompaction, timeMs,
              l1Cached, l2Cached, preload);
    }
  }

  /**
   * Updates the number of hits and the total number of block reads on a block
   * cache hit.
   */
  public void updateOnCacheHit(BlockCategory blockCategory,
      boolean isCompaction) {
    blockCategory.expectSpecific();
    incrNumericMetric(blockCategory, isCompaction, BlockMetricType.CACHE_HIT);
    incrNumericMetric(blockCategory, isCompaction, BlockMetricType.READ_COUNT);
  }

  /**
   * Updates read time and the number of misses on a block cache miss.
   */
  private void updateOnCacheMiss(BlockCategory blockCategory,
                                 boolean isCompaction) {
    blockCategory.expectSpecific();
    incrNumericMetric(blockCategory, isCompaction, BlockMetricType.CACHE_MISS);
    incrNumericMetric(blockCategory, isCompaction, BlockMetricType.READ_COUNT);
  }

  /**
   * Updates the number of hits and the total number of block reads on a L2
   * cache hit.
   */
  private void updateOnL2CacheHit(BlockCategory blockCategory,
                                 boolean isCompaction) {
    blockCategory.expectSpecific();
    incrNumericMetric(blockCategory, isCompaction,
            BlockMetricType.L2_READ_COUNT);
    incrNumericMetric(blockCategory, isCompaction,
            BlockMetricType.L2_CACHE_HIT);
  }

  /**
   * Updates read time and the number of misses on a block cache miss.
   */
  private void updateOnL2CacheMiss(BlockCategory blockCategory,
                                boolean isCompaction) {
    blockCategory.expectSpecific();
    incrNumericMetric(blockCategory, isCompaction,
            BlockMetricType.L2_CACHE_MISS);
    incrNumericMetric(blockCategory, isCompaction,
            BlockMetricType.L2_READ_COUNT);
  }

  private void addToPreloadReadTime(BlockCategory blockCategory,
      boolean isCompaction, long timeMs) {
    HRegion.incrTimeVaryingMetric(getBlockMetricName(blockCategory,
        isCompaction, BlockMetricType.PRELOAD_READ_TIME), timeMs);

    // Also update the read time aggregated across all block categories
    if (blockCategory != BlockCategory.ALL_CATEGORIES) {
      addToPreloadReadTime(BlockCategory.ALL_CATEGORIES, isCompaction, timeMs);
    }
  }

  /**
   * Updates read time, the number of misses, and the total number of block for preloader
   */
  private void updateOnPreloadCacheMiss(BlockCategory blockCategory, boolean isCompaction,
      long timeMs) {
    blockCategory.expectSpecific();
    addToPreloadReadTime(blockCategory, isCompaction, timeMs);
    incrNumericMetric(blockCategory, isCompaction, BlockMetricType.PRELOAD_CACHE_MISS);
  }

  /**
   * Updates read time, the number of hits, and the total number of block for preloader
   */
  private void updateOnPreloadCacheHit(BlockCategory blockCategory,
      boolean isCompaction, long timeMs) {
    blockCategory.expectSpecific();
    addToPreloadReadTime(blockCategory, isCompaction, timeMs);
    incrNumericMetric(blockCategory, isCompaction,
            BlockMetricType.PRELOAD_CACHE_HIT);
  }

  /**
   * Adds the given delta to the cache size for the given block category and
   * the aggregate metric for all block categories. Updates both the per-CF
   * counter and the counter for all CFs (four metrics total). The cache size
   * metric is "persistent", i.e. it does not get reset when metrics are
   * collected.
   */
  private void addToCacheSize(BlockCategory category, long cacheSizeDelta,
      long unencodedCacheSizeDelta) {
    if (category == null) {
      category = BlockCategory.ALL_CATEGORIES;
    }
    HRegion.incrNumericPersistentMetric(getBlockMetricName(category, DEFAULT_COMPACTION_FLAG,
        BlockMetricType.CACHE_SIZE), cacheSizeDelta);
    HRegion.incrNumericPersistentMetric(getBlockMetricName(category, DEFAULT_COMPACTION_FLAG,
        BlockMetricType.UNENCODED_CACHE_SIZE), unencodedCacheSizeDelta);
    HRegion.incrNumericPersistentMetric(getBlockMetricName(category, DEFAULT_COMPACTION_FLAG,
        BlockMetricType.CACHE_NUM_BLOCKS), cacheSizeDelta > 0 ? 1 : -1);

    if (category != BlockCategory.ALL_CATEGORIES) {
      addToCacheSize(BlockCategory.ALL_CATEGORIES, cacheSizeDelta, unencodedCacheSizeDelta);
    }
  }

  private void updateL2CacheSize(BlockCategory category, long delta) {
    if (category == null) {
      category = BlockCategory.ALL_CATEGORIES;
    }
    HRegion.incrNumericPersistentMetric(getBlockMetricName(category,
            DEFAULT_COMPACTION_FLAG, BlockMetricType.L2_CACHE_NUM),
            delta > 0 ? 1 : -1);
    HRegion.incrNumericPersistentMetric(getBlockMetricName(category,
            DEFAULT_COMPACTION_FLAG, BlockMetricType.L2_CACHE_SIZE), delta);

    if (category != BlockCategory.ALL_CATEGORIES) {
      updateL2CacheSize(BlockCategory.ALL_CATEGORIES, delta);
    }
  }

  /**
   * Updates the number and the total size of blocks in cache for both the configured table/CF
   * and all table/CFs (by calling the same method on {@link #ALL_SCHEMA_METRICS}), both the given
   * block category and all block categories aggregated, and the given block size.
   * @param blockCategory block category, e.g. index or data
   * @param cacheSizeDelta the size of the block being cached (positive) or evicted (negative)
   * @param unencodedCacheSizeDelta the amount to add to unencoded cache size. Must have the same
   *                                sign as cacheSizeDelta.
   */
  public void updateOnCachePutOrEvict(BlockCategory blockCategory, long cacheSizeDelta,
      long unencodedCacheSizeDelta) {
    Preconditions.checkState((cacheSizeDelta > 0) == (unencodedCacheSizeDelta > 0));
    addToCacheSize(blockCategory, cacheSizeDelta, unencodedCacheSizeDelta);
    incrNumericMetric(blockCategory, DEFAULT_COMPACTION_FLAG,
        cacheSizeDelta > 0 ? BlockMetricType.CACHED : BlockMetricType.EVICTED);
    if (this != ALL_SCHEMA_METRICS) {
      ALL_SCHEMA_METRICS.updateOnCachePutOrEvict(blockCategory, cacheSizeDelta,
          unencodedCacheSizeDelta);
    }
  }

  public void updateOnL2CachePutOrEvict(BlockCategory blockCategory,
                                        long delta) {
    updateL2CacheSize(blockCategory, delta);
    incrNumericMetric(blockCategory, DEFAULT_COMPACTION_FLAG,
            delta > 0 ? BlockMetricType.L2_CACHED : BlockMetricType.L2_EVICTED);
    if (this != ALL_SCHEMA_METRICS) {
      ALL_SCHEMA_METRICS.updateOnL2CachePutOrEvict(blockCategory, delta);
    }
  }

  /**
   * Increments both the per-CF and the aggregate counter of bloom
   * positives/negatives as specified by the argument.
   */
  public void updateBloomMetrics(boolean isInBloom) {
    HRegion.incrNumericMetric(getBloomMetricName(isInBloom), 1);
    if (this != ALL_SCHEMA_METRICS) {
      ALL_SCHEMA_METRICS.updateBloomMetrics(isInBloom);
    }
  }

  /**
   * Updates the number of times a block was present in the block cache, but
   * could not be used due to a type mismatch. The assumption is this should
   * really only happen upon compactions of CFs which use block encoding.
   */
  public void updateOnDataBlockEncodingMismatch(boolean isCompaction) {
    HRegion.incrNumericMetric(
            getDataBlockEncodingMismatchMetricNames(isCompaction), 1);
    if (this != ALL_SCHEMA_METRICS) {
      ALL_SCHEMA_METRICS.updateOnDataBlockEncodingMismatch(isCompaction);
    }
  }

  /**
   * Sets the flag whether to use table name in metric names according to the
   * given configuration. This must be called at least once before
   * instantiating HFile readers/writers.
   */
  public static void configureGlobally(Configuration conf) {
    if (conf != null) {
      final boolean useTableNameNew =
          conf.getBoolean(SHOW_TABLE_NAME_CONF_KEY, false);
      setUseTableName(useTableNameNew);
    } else {
      setUseTableName(false);
    }
  }

  private static String getEffectiveTableName(String tableName) {
    if (!tableName.equals(TOTAL_KEY)) {
      // We are provided with a non-trivial table name (including "unknown").
      // We need to know whether table name should be included into metrics.
      if (useTableNameGlobally == null) {
        throw new IllegalStateException("The value of the "
            + SHOW_TABLE_NAME_CONF_KEY + " conf option has not been specified "
            + "in SchemaMetrics");
      }
      final boolean useTableName = useTableNameGlobally;
      if (!useTableName) {
        // Don't include table name in metric keys.
        tableName = TOTAL_KEY;
      }
    }
    return tableName;
  }

  /**
   * Method to transform the column family with the table name
   * into a schemaMetrics prefix which is used for printing out in metrics
   *
   * @param tableName
   * @param cfName the column family name
   * @return schemaMetricsPrefix
   */
  public static String generateSchemaMetricsPrefix(String tableName,
      final String cfName){
    tableName = getEffectiveTableName(tableName);
    String schemaMetricPrefix =
      tableName.equals(TOTAL_KEY) ? "" : TABLE_PREFIX + tableName + ".";
    schemaMetricPrefix +=
      cfName.equals(TOTAL_KEY) ? "" : CF_PREFIX + cfName + ".";
    return schemaMetricPrefix;
  }

  /**
   * Method to transform a set of column families in byte[] format with table name
   * into a schemaMetrics prefix which is used for printing out in metrics
   *
   * @param tableName
   * @param families the ordered set of column families
   * @return schemaMetricsPrefix
   */
  public static String generateSchemaMetricsPrefix(String tableName,
      Set<byte[]> families) {
    if (families == null || families.isEmpty() ||
        tableName == null || tableName.length() == 0)
      return "";

    if (families.size() == 1) {
      return SchemaMetrics.generateSchemaMetricsPrefix(tableName,
          Bytes.toString(families.iterator().next()));
    }

    tableName = getEffectiveTableName(tableName);

    StringBuilder sb = new StringBuilder();
    int MAX_SIZE = 256;
    int limit = families.size();
    for (byte[] family : families) {
      if (sb.length() > MAX_SIZE) {
        sb.append("__more");
        break;
      }
      --limit;
      sb.append(Bytes.toString(family));
      if (0 != limit) {
        sb.append("~");
      }
    }

    return SchemaMetrics.generateSchemaMetricsPrefix(tableName, sb.toString());
  }

  /**
   * Sets the flag of whether to use table name in metric names. This flag
   * is specified in configuration and is not expected to change at runtime,
   * so we log an error message when it does change.
   */
  private static void setUseTableName(final boolean useTableNameNew) {
    if (useTableNameGlobally == null) {
      // This configuration option has not yet been set.
      useTableNameGlobally = useTableNameNew;
    } else if (useTableNameGlobally != useTableNameNew
        && !loggedConfInconsistency) {
      // The configuration is inconsistent and we have not reported it
      // previously. Once we report it, just keep ignoring the new setting.
      LOG.error("Inconsistent configuration. Previous configuration "
          + "for using table name in metrics: " + useTableNameGlobally + ", "
          + "new configuration: " + useTableNameNew);
      loggedConfInconsistency = true;
    }
  }

  // Methods used in testing

  private static final String regexEscape(String s) {
    return s.replace(".", "\\.");
  }

  /**
   * Assume that table names used in tests don't contain dots, except for the
   * META table.
   */
  private static final String WORD_AND_DOT_RE_STR = "([^.]+|" +
      regexEscape(Bytes.toString(HConstants.META_TABLE_NAME)) +
      ")\\.";

  /** "tab.<table_name>." */
  private static final String TABLE_NAME_RE_STR =
      WORD_BOUNDARY_RE_STR + regexEscape(TABLE_PREFIX) + WORD_AND_DOT_RE_STR;

  /** "cf.<cf_name>." */
  private static final String CF_NAME_RE_STR =
      WORD_BOUNDARY_RE_STR + regexEscape(CF_PREFIX) + WORD_AND_DOT_RE_STR;
  private static final Pattern CF_NAME_RE = Pattern.compile(CF_NAME_RE_STR);

  /** "tab.<table_name>.cf.<cf_name>." */
  private static final Pattern TABLE_AND_CF_NAME_RE = Pattern.compile(
      TABLE_NAME_RE_STR + CF_NAME_RE_STR);

  private static final String COMPACTION_PREFIX_RE_STR = "(" +
      NON_COMPACTION_METRIC_PREFIX + "|" + COMPACTION_METRIC_PREFIX + ")?";

  static final Pattern BLOCK_CATEGORY_RE = Pattern.compile(
      "(" +
      WORD_BOUNDARY_RE_STR +
      regexEscape(BLOCK_TYPE_PREFIX) + "[^.]+\\." +
      // Also remove the special-case block type marker for meta blocks
      "|" +
      // Note we are not using word boundary here because "fs" or "compaction" may precede "Meta"
      META_BLOCK_CATEGORY_STR +
      ")" +
      // Positive lookahead for block metric types. Needed for both meta and non-meta metrics.
      "(?=" + COMPACTION_PREFIX_RE_STR + "(" + BlockMetricType.BLOCK_METRIC_TYPE_RE + "))");

  /**
   * A suffix for the "number of operations" part of "time-varying metrics". We
   * only use this for metric verification in unit testing. Time-varying
   * metrics are handled by a different code path in production.
   */
  private static String NUM_OPS_SUFFIX = "numops";

  /**
   * A custom suffix that we use for verifying the second component of
   * a "time-varying metric".
   */
  private static String TOTAL_SUFFIX = "_total";
  private static final Pattern TIME_VARYING_SUFFIX_RE = Pattern.compile(
      "(" + NUM_OPS_SUFFIX + "|" + TOTAL_SUFFIX + ")$");

  void printMetricNames() {
    for (BlockCategory blockCategory : BlockCategory.values()) {
      for (boolean isCompaction : BOOL_VALUES) {
        for (BlockMetricType metricType : BlockMetricType.values()) {
          int i = getBlockMetricIndex(blockCategory, isCompaction, metricType);
          LOG.debug("blockCategory=" + blockCategory + ", "
              + "metricType=" + metricType + ", isCompaction=" + isCompaction
              + ", metricName=" + blockMetricNames[i]);
        }
      }
    }
  }

  private Collection<String> getAllMetricNames() {
    List<String> allMetricNames = new ArrayList<String>();
    for (int i = 0; i < blockMetricNames.length; ++i) {
      final String blockMetricName = blockMetricNames[i];
      final boolean timeVarying = blockMetricTimeVarying[i];
      if (blockMetricName != null) {
        if (timeVarying) {
          allMetricNames.add(blockMetricName + NUM_OPS_SUFFIX);
          allMetricNames.add(blockMetricName + TOTAL_SUFFIX);
        } else {
          allMetricNames.add(blockMetricName);
        }
      }
    }
    allMetricNames.addAll(Arrays.asList(bloomMetricNames));
    return allMetricNames;
  }

  private static final boolean isTimeVaryingMetricKey(String metricKey) {
    return metricKey.endsWith(NUM_OPS_SUFFIX)
        || metricKey.endsWith(TOTAL_SUFFIX);
  }

  static final boolean isPersistentMetricKey(String metricKey) {
    return PERSISTENT_METRIC_RE.matcher(metricKey).matches();
  }

  private static final String stripTimeVaryingSuffix(String metricKey) {
    return TIME_VARYING_SUFFIX_RE.matcher(metricKey).replaceAll("");
  }

  public static Map<String, Long> getMetricsSnapshot() {
    Map<String, Long> metricsSnapshot = new TreeMap<String, Long>();
    for (SchemaMetrics cfm : tableAndFamilyToMetrics.values()) {
      for (String metricName : cfm.getAllMetricNames()) {
        long metricValue;
        if (isTimeVaryingMetricKey(metricName)) {
          Pair<Long, Integer> totalAndCount =
              HRegion.getTimeVaryingMetric(stripTimeVaryingSuffix(metricName));
          metricValue = metricName.endsWith(TOTAL_SUFFIX) ?
              totalAndCount.getFirst() : totalAndCount.getSecond();
        } else if (isPersistentMetricKey(metricName)) {
          metricValue = HRegion.getNumericPersistentMetric(metricName);
        } else {
          metricValue = HRegion.getNumericMetric(metricName);
        }

        metricsSnapshot.put(metricName, metricValue);
      }
    }
    return metricsSnapshot;
  }

  public static long getLong(Map<String, Long> m, String k) {
    Long l = m.get(k);
    return l != null ? l : 0;
  }

  private static void incrLong(Map<String, Long> m, String k, long delta) {
    putLong(m, k, getLong(m, k) + delta);
  }

  private static void putLong(Map<String, Long> m, String k, long v) {
    if (v != 0) {
      m.put(k, v);
    } else {
      m.remove(k);
    }
  }

  public static Map<String, Long> diffMetrics(Map<String, Long> a,
      Map<String, Long> b) {
    Set<String> allKeys = new TreeSet<String>(a.keySet());
    allKeys.addAll(b.keySet());
    Map<String, Long> diff = new TreeMap<String, Long>();
    for (String k : allKeys) {
      long aVal = getLong(a, k);
      long bVal = getLong(b, k);
      if (aVal != bVal) {
        diff.put(k, bVal - aVal);
      }
    }
    return diff;
  }

  /**
   * Checks whether metric changes between the given old state and the current state are consistent.
   * If they are not, throws an {@link AssertionError}.
   */
  public static void validateMetricChanges(Map<String, Long> oldMetrics) {
    if (!validateMetricChangesInternal(oldMetrics, true)) {
      // This will output diagnostic info and throw an assertion error.
      validateMetricChangesInternal(oldMetrics, false);
    }
  }

  /**
   * Validates metric changes between the given set of old metric values and the current values.
   * @param oldMetrics old metric value map
   * @param quiet if true, don't output anything and return whether validation is successful;
   *              if false, output diagnostic info and throw an assertion if validation fails
   * @return whether validation is successful
   */
  private static boolean validateMetricChangesInternal(Map<String, Long> oldMetrics,
      boolean quiet) {
    final Map<String, Long> newMetrics = getMetricsSnapshot();
    final Map<String, Long> allCfDeltas = new TreeMap<String, Long>();
    final Map<String, Long> allBlockCategoryDeltas =
        new TreeMap<String, Long>();
    final Map<String, Long> deltas = diffMetrics(oldMetrics, newMetrics);
    final Pattern cfTableMetricRE =
        useTableNameGlobally ? TABLE_AND_CF_NAME_RE : CF_NAME_RE;
    final Set<String> allKeys = new TreeSet<String>(oldMetrics.keySet());
    allKeys.addAll(newMetrics.keySet());

    for (SchemaMetrics cfm : tableAndFamilyToMetrics.values()) {
      for (String metricName : cfm.getAllMetricNames()) {
        if (metricName.startsWith(CF_PREFIX + CF_PREFIX)) {
          if (quiet) {
            return false;
          } else {
            throw new AssertionError("Column family prefix used twice: " +
                metricName);
          }
        }

        final long oldValue = getLong(oldMetrics, metricName);
        final long newValue = getLong(newMetrics, metricName);
        final long delta = newValue - oldValue;

        if (delta == 0) {
          continue;
        }

        // Re-calculate values of metrics with no column family (or CF/table)
        // specified based on all metrics with CF (or CF/table) specified.
        if (cfm != ALL_SCHEMA_METRICS) {
          final String aggregateMetricName =
              cfTableMetricRE.matcher(metricName).replaceAll("");
          if (!aggregateMetricName.equals(metricName)) {
            if (!quiet) {
              LOG.debug("Counting " + delta + " units of " + metricName
                  + " towards " + aggregateMetricName);
            }

            incrLong(allCfDeltas, aggregateMetricName, delta);
          }
        }

        Matcher matcher = BLOCK_CATEGORY_RE.matcher(metricName);
        if (matcher.find()) {
          // Only process per-block-category metrics
          String metricNoBlockCategory = matcher.replaceAll("");
          if (!quiet) {
            LOG.debug("Counting " + delta + " units of " + metricName + " towards " +
                metricNoBlockCategory);
          }

          incrLong(allBlockCategoryDeltas, metricNoBlockCategory, delta);
        }
      }
    }

    StringBuilder errors = new StringBuilder();
    for (String key : ALL_SCHEMA_METRICS.getAllMetricNames()) {
      long actual = getLong(deltas, key);
      long expected = getLong(allCfDeltas, key);
      if (actual != expected) {
        if (errors.length() > 0) {
          errors.append("\n");
        }
        errors.append("The all-CF metric " + key + " changed by "
            + actual + " but the aggregation of per-CF/table metrics "
            + "yields " + expected);
      }
    }

    // Verify metrics computed for all block types based on the aggregation
    // of per-block-type metrics.
    for (String key : allKeys) {
      if (BLOCK_CATEGORY_RE.matcher(key).find() ||
          key.contains(ALL_SCHEMA_METRICS.getBloomMetricName(false)) ||
          key.contains(ALL_SCHEMA_METRICS.getBloomMetricName(true))){
        // Skip per-block-category metrics. Also skip bloom filters, because
        // they are not aggregated per block type.
        continue;
      }
      long actual = getLong(deltas, key);
      long expected = getLong(allBlockCategoryDeltas, key);
      if (actual != expected) {
        if (errors.length() > 0) {
          errors.append("\n");
        }
        errors.append("The all-block-category metric " + key
            + " changed by " + actual + " but the aggregation of "
            + "per-block-category metrics yields " + expected);
      }
    }

    checkNumBlocksInCache();

    if (errors.length() > 0) {
      if (quiet) {
        return false;
      } else {
        throw new AssertionError(errors.toString());
      }
    }
    return true;
  }

  /**
   * Creates an instance pretending both the table and column family are
   * unknown. Used in unit tests.
   */
  public static SchemaMetrics getUnknownInstanceForTest() {
    return getInstance(UNKNOWN, UNKNOWN);
  }

  /**
   * Set the flag to use or not use table name in metric names. Used in unit
   * tests, so the flag can be set arbitrarily.
   */
  public static void setUseTableNameInTest(final boolean useTableNameNew) {
    useTableNameGlobally = useTableNameNew;
  }

  /** Formats the given map of metrics in a human-readable way. */
  public static String formatMetrics(Map<String, Long> metrics) {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, Long> entry : metrics.entrySet()) {
      if (sb.length() > 0) {
        sb.append('\n');
      }
      sb.append(entry.getKey() + " : " + entry.getValue());
    }
    return sb.toString();
  }

  /** Validates metrics that keep track of the number of cached blocks for each category */
  private static void checkNumBlocksInCache() {
    final LruBlockCache cache =
        LruBlockCacheFactory.getInstance().getCurrentBlockCacheInstance();
    if (cache == null) {
      // There is no global block cache instantiated. Most likely there is no mini-cluster running.
      return;
    }
    final Map<BlockType, Integer> blockTypeCounts = cache.getBlockTypeCountsForTest();
    long[] blockCategoryCounts = new long[BlockCategory.values().length];
    for (Map.Entry<BlockType, Integer> entry : blockTypeCounts.entrySet()) {
      blockCategoryCounts[entry.getKey().getCategory().ordinal()] += entry.getValue();
      blockCategoryCounts[BlockCategory.ALL_CATEGORIES.ordinal()] += entry.getValue();
    }
    for (BlockCategory blockCategory : BlockCategory.values()) {
      String metricName = ALL_SCHEMA_METRICS.getBlockMetricName(blockCategory,
          DEFAULT_COMPACTION_FLAG, BlockMetricType.CACHE_NUM_BLOCKS);
      long metricValue = HRegion.getNumericPersistentMetric(metricName);
      long expectedValue = blockCategoryCounts[blockCategory.ordinal()];
      if (metricValue != expectedValue) {
        throw new AssertionError("Expected " + expectedValue + " blocks of category " +
            blockCategory + " in cache, but found " + metricName + "=" + metricValue);
      }
    }
  }

  public static void clearBlockCacheMetrics() {
    for (SchemaMetrics metrics : tableAndFamilyToMetrics.values()) {
      for (BlockCategory blockCategory : BlockCategory.values()) {
        String key = metrics.getBlockMetricName(blockCategory, DEFAULT_COMPACTION_FLAG,
            BlockMetricType.CACHE_NUM_BLOCKS);
        HRegion.clearNumericPersistentMetric(key);
      }
    }
  }

}
