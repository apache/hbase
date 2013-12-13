/*
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hbase.regionserver.metrics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.hfile.BlockType.BlockCategory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

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
 *   <td> tbl.T.cf.CF.M </td> <td> Include </td> <td> Skip    </td>
 *   <td> A specific column family of a specific table        </td>
 * </tr>
 * <tr>
 *   <td> tbl.T.M       </td> <td> Skip    </td> <td> Skip    </td>
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

  public interface SchemaAware {
    public String getTableName();
    public String getColumnFamilyName();
    public SchemaMetrics getSchemaMetrics();
  }

  private static final Log LOG = LogFactory.getLog(SchemaMetrics.class);

  public static enum BlockMetricType {
    // Metric configuration: compactionAware, timeVarying
    READ_TIME("Read",                   true, true),
    READ_COUNT("BlockReadCnt",          true, false),
    CACHE_HIT("BlockReadCacheHitCnt",   true, false),
    CACHE_MISS("BlockReadCacheMissCnt", true, false),

    CACHE_SIZE("blockCacheSize",        false, false),
    CACHED("blockCacheNumCached",       false, false),
    EVICTED("blockCacheNumEvicted",     false, false);

    private final String metricStr;
    private final boolean compactionAware;
    private final boolean timeVarying;

    BlockMetricType(String metricStr, boolean compactionAware,
          boolean timeVarying) {
      this.metricStr = metricStr;
      this.compactionAware = compactionAware;
      this.timeVarying = timeVarying;
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
  };

  public static enum StoreMetricType {
    STORE_FILE_COUNT("storeFileCount"),
    STORE_FILE_INDEX_SIZE("storeFileIndexSizeMB"),
    STORE_FILE_SIZE_MB("storeFileSizeMB"),
    STATIC_BLOOM_SIZE_KB("staticBloomSizeKB"),
    MEMSTORE_SIZE_MB("memstoreSizeMB"),
    STATIC_INDEX_SIZE_KB("staticIndexSizeKB"),
    FLUSH_SIZE("flushSize");

    private final String metricStr;

    StoreMetricType(String metricStr) {
      this.metricStr = metricStr;
    }

    @Override
    public String toString() {
      return metricStr;
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
  public static final String REGION_PREFIX = "region.";

  public static final String CF_UNKNOWN_PREFIX = CF_PREFIX + UNKNOWN + ".";
  public static final String CF_BAD_FAMILY_PREFIX = CF_PREFIX + "__badfamily.";

  /** Use for readability when obtaining non-compaction counters */
  public static final boolean NO_COMPACTION = false;

  public static final String METRIC_GETSIZE = "getsize";
  public static final String METRIC_NEXTSIZE = "nextsize";

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
  private static final String SHOW_TABLE_NAME_CONF_KEY =
      "hbase.metrics.showTableName";

  /** We use this when too many column families are involved in a request. */
  private static final String MORE_CFS_OMITTED_STR = "__more";

  /**
   * Maximum length of a metric name prefix. Used when constructing metric
   * names from a set of column families participating in a request.
   */
  private static final int MAX_METRIC_PREFIX_LENGTH =
      256 - MORE_CFS_OMITTED_STR.length();

  // Global variables
  /**
   * Maps a string key consisting of table name and column family name, with
   * table name optionally replaced with {@link #TOTAL_KEY} if per-table
   * metrics are disabled, to an instance of this class.
   */
  private static final ConcurrentHashMap<String, SchemaMetrics>
      tableAndFamilyToMetrics = new ConcurrentHashMap<String, SchemaMetrics>();

  /** Metrics for all tables and column families. */
  // This has to be initialized after cfToMetrics.
  public static final SchemaMetrics ALL_SCHEMA_METRICS =
    getInstance(TOTAL_KEY, TOTAL_KEY);

  /** Threshold for flush the metrics, currently used only for "on cache hit" */
  private static final long THRESHOLD_METRICS_FLUSH = 100l;

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
  private final AtomicLongArray onHitCacheMetrics= 
      new AtomicLongArray(NUM_BLOCK_CATEGORIES * BOOL_VALUES.length);

  private SchemaMetrics(final String tableName, final String cfName) {
    String metricPrefix = SchemaMetrics.generateSchemaMetricsPrefix(
        tableName, cfName);

    for (BlockCategory blockCategory : BlockCategory.values()) {
      for (boolean isCompaction : BOOL_VALUES) {
        // initialize the cache metrics
        onHitCacheMetrics.set(getCacheHitMetricIndex(blockCategory, isCompaction), 0);
        
        for (BlockMetricType metricType : BlockMetricType.values()) {
          if (!metricType.compactionAware && isCompaction) {
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

          if (metricType.compactionAware) {
            sb.append(isCompaction ? "compaction" : "fs");
          }

          // A special-case for meta blocks for backwards-compatibility.
          if (blockCategory == BlockCategory.META) {
            sb.append(META_BLOCK_CATEGORY_STR);
          }

          sb.append(metricType);

          int i = getBlockMetricIndex(blockCategory, isCompaction, metricType);
          blockMetricNames[i] = sb.toString();
          blockMetricTimeVarying[i] = metricType.timeVarying;
        }
      }
    }

    for (boolean isInBloom : BOOL_VALUES) {
      bloomMetricNames[isInBloom ? 1 : 0] = metricPrefix
          + (isInBloom ? "keyMaybeInBloomCnt" : "keyNotInBloomCnt");
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

  private static final int getCacheHitMetricIndex (BlockCategory blockCategory,
      boolean isCompaction) {
    return blockCategory.ordinal() * BOOL_VALUES.length + (isCompaction ? 1 : 0);
  }
  
  private static final int getBlockMetricIndex(BlockCategory blockCategory,
      boolean isCompaction, BlockMetricType metricType) {
    int i = 0;
    i = i * NUM_BLOCK_CATEGORIES + blockCategory.ordinal();
    i = i * BOOL_VALUES.length + (isCompaction ? 1 : 0);
    i = i * NUM_METRIC_TYPES + metricType.ordinal();
    return i;
  }

  public String getBlockMetricName(BlockCategory blockCategory,
      boolean isCompaction, BlockMetricType metricType) {
    if (isCompaction && !metricType.compactionAware) {
      throw new IllegalArgumentException("isCompaction cannot be true for "
          + metricType);
    }
    return blockMetricNames[getBlockMetricIndex(blockCategory, isCompaction,
        metricType)];
  }

  public String getBloomMetricName(boolean isInBloom) {
    return bloomMetricNames[isInBloom ? 1 : 0];
  }

  /**
   * Increments the given metric, both per-CF and aggregate, for both the given
   * category and all categories in aggregate (four counters total).
   */
  private void incrNumericMetric(BlockCategory blockCategory,
      boolean isCompaction, BlockMetricType metricType) {
    incrNumericMetric (blockCategory, isCompaction, metricType, 1);
  }
  
  /**
   * Increments the given metric, both per-CF and aggregate, for both the given
   * category and all categories in aggregate (four counters total).
   */
  private void incrNumericMetric(BlockCategory blockCategory,
      boolean isCompaction, BlockMetricType metricType, long amount) {
    if (blockCategory == null) {
      blockCategory = BlockCategory.UNKNOWN;  // So that we see this in stats.
    }
    RegionMetricsStorage.incrNumericMetric(getBlockMetricName(blockCategory,
        isCompaction, metricType), amount);

    if (blockCategory != BlockCategory.ALL_CATEGORIES) {
      incrNumericMetric(BlockCategory.ALL_CATEGORIES, isCompaction,
          metricType, amount);
    }
  }

  private void addToReadTime(BlockCategory blockCategory,
      boolean isCompaction, long timeMs) {
    RegionMetricsStorage.incrTimeVaryingMetric(getBlockMetricName(blockCategory,
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
    RegionMetricsStorage.incrNumericPersistentMetric(
        storeMetricNames[storeMetricType.ordinal()], value);
  }

  /**
   * Updates the number of hits and the total number of block reads on a block
   * cache hit.
   */
  public void updateOnCacheHit(BlockCategory blockCategory,
      boolean isCompaction) {
    updateOnCacheHit(blockCategory, isCompaction, 1);
  }
  
  /**
   * Updates the number of hits and the total number of block reads on a block
   * cache hit.
   */
  public void updateOnCacheHit(BlockCategory blockCategory,
      boolean isCompaction, long count) {
    blockCategory.expectSpecific();
    int idx = getCacheHitMetricIndex(blockCategory, isCompaction);
    
    if (this.onHitCacheMetrics.addAndGet(idx, count) > THRESHOLD_METRICS_FLUSH) {
      flushCertainOnCacheHitMetrics(blockCategory, isCompaction);
    }
    
    if (this != ALL_SCHEMA_METRICS) {
      ALL_SCHEMA_METRICS.updateOnCacheHit(blockCategory, isCompaction, count);
    }
  }
  
  private void flushCertainOnCacheHitMetrics(BlockCategory blockCategory, boolean isCompaction) {
    int idx = getCacheHitMetricIndex(blockCategory, isCompaction);
    long tempCount = this.onHitCacheMetrics.getAndSet(idx, 0);
    
    if (tempCount > 0) {
      incrNumericMetric(blockCategory, isCompaction, BlockMetricType.CACHE_HIT, tempCount);
      incrNumericMetric(blockCategory, isCompaction, BlockMetricType.READ_COUNT, tempCount);
    }
  }
  
  /**
   * Flush the on cache hit metrics;
   */
  private void flushOnCacheHitMetrics() {
    for (BlockCategory blockCategory : BlockCategory.values()) {
      for (boolean isCompaction : BOOL_VALUES) {
        flushCertainOnCacheHitMetrics (blockCategory, isCompaction);
      }
    }
    
    if (this != ALL_SCHEMA_METRICS) {
      ALL_SCHEMA_METRICS.flushOnCacheHitMetrics();
    }
  }

  /**
   * Notify the SchemaMetrics to flush all of the the metrics
   */
  public void flushMetrics() {
    // currently only for "on cache hit metrics"
    flushOnCacheHitMetrics();
  }
  
  /**
   * Updates read time, the number of misses, and the total number of block
   * reads on a block cache miss.
   */
  public void updateOnCacheMiss(BlockCategory blockCategory,
      boolean isCompaction, long timeMs) {
    blockCategory.expectSpecific();
    addToReadTime(blockCategory, isCompaction, timeMs);
    incrNumericMetric(blockCategory, isCompaction, BlockMetricType.CACHE_MISS);
    incrNumericMetric(blockCategory, isCompaction, BlockMetricType.READ_COUNT);
    if (this != ALL_SCHEMA_METRICS) {
      ALL_SCHEMA_METRICS.updateOnCacheMiss(blockCategory, isCompaction,
          timeMs);
    }
  }

  /**
   * Adds the given delta to the cache size for the given block category and
   * the aggregate metric for all block categories. Updates both the per-CF
   * counter and the counter for all CFs (four metrics total). The cache size
   * metric is "persistent", i.e. it does not get reset when metrics are
   * collected.
   */
  public void addToCacheSize(BlockCategory category, long cacheSizeDelta) {
    if (category == null) {
      category = BlockCategory.ALL_CATEGORIES;
    }
    RegionMetricsStorage.incrNumericPersistentMetric(getBlockMetricName(category, false,
        BlockMetricType.CACHE_SIZE), cacheSizeDelta);

    if (category != BlockCategory.ALL_CATEGORIES) {
      addToCacheSize(BlockCategory.ALL_CATEGORIES, cacheSizeDelta);
    }
  }

  public void updateOnCachePutOrEvict(BlockCategory blockCategory,
      long cacheSizeDelta, boolean isEviction) {
    addToCacheSize(blockCategory, cacheSizeDelta);
    incrNumericMetric(blockCategory, false,
        isEviction ? BlockMetricType.EVICTED : BlockMetricType.CACHED);
    if (this != ALL_SCHEMA_METRICS) {
      ALL_SCHEMA_METRICS.updateOnCachePutOrEvict(blockCategory, cacheSizeDelta,
          isEviction);
    }
  }

  /**
   * Increments both the per-CF and the aggregate counter of bloom
   * positives/negatives as specified by the argument.
   */
  public void updateBloomMetrics(boolean isInBloom) {
    RegionMetricsStorage.incrNumericMetric(getBloomMetricName(isInBloom), 1);
    if (this != ALL_SCHEMA_METRICS) {
      ALL_SCHEMA_METRICS.updateBloomMetrics(isInBloom);
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

  /**
   * Determine the table name to be included in metric keys. If the global
   * configuration says that we should not use table names in metrics,
   * we always return {@link #TOTAL_KEY} even if nontrivial table name is
   * provided.
   *
   * @param tableName a table name or {@link #TOTAL_KEY} when aggregating
   * across all tables
   * @return the table name to use in metric keys
   */
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
   * Method to transform a combination of a table name and a column family name
   * into a metric key prefix. Tables/column family names equal to
   * {@link #TOTAL_KEY} are omitted from the prefix.
   *
   * @param tableName the table name or {@link #TOTAL_KEY} for all tables
   * @param cfName the column family name or {@link #TOTAL_KEY} for all CFs
   * @return the metric name prefix, ending with a dot.
   */
  public static String generateSchemaMetricsPrefix(String tableName,
      final String cfName) {
    tableName = getEffectiveTableName(tableName);
    String schemaMetricPrefix =
        tableName.equals(TOTAL_KEY) ? "" : TABLE_PREFIX + tableName + ".";
    schemaMetricPrefix +=
        cfName.equals(TOTAL_KEY) ? "" : CF_PREFIX + cfName + ".";
    return schemaMetricPrefix;
  }

  public static String generateSchemaMetricsPrefix(byte[] tableName,
      byte[] cfName) {
    return generateSchemaMetricsPrefix(Bytes.toString(tableName),
        Bytes.toString(cfName));
  }

  /**
   * Method to transform a set of column families in byte[] format with table
   * name into a metric key prefix.
   *
   * @param tableName the table name or {@link #TOTAL_KEY} for all tables
   * @param families the ordered set of column families
   * @return the metric name prefix, ending with a dot, or an empty string in
   *         case of invalid arguments. This is OK since we always expect
   *         some CFs to be included.
   */
  public static String generateSchemaMetricsPrefix(String tableName,
      Set<byte[]> families) {
    if (families == null || families.isEmpty() ||
        tableName == null || tableName.isEmpty()) {
      return "";
    }

    if (families.size() == 1) {
      return generateSchemaMetricsPrefix(tableName,
          Bytes.toString(families.iterator().next()));
    }

    tableName = getEffectiveTableName(tableName);
    List<byte[]> sortedFamilies = new ArrayList<byte[]>(families);
    Collections.sort(sortedFamilies, Bytes.BYTES_COMPARATOR);

    StringBuilder sb = new StringBuilder();

    int numCFsLeft = families.size();
    for (byte[] family : sortedFamilies) {
      if (sb.length() > MAX_METRIC_PREFIX_LENGTH) {
        sb.append(MORE_CFS_OMITTED_STR);
        break;
      }
      --numCFsLeft;
      sb.append(Bytes.toString(family));
      if (numCFsLeft > 0) {
        sb.append("~");
      }
    }

    return SchemaMetrics.generateSchemaMetricsPrefix(tableName, sb.toString());
  }

  /**
   * Get the prefix for metrics generated about a single region.
   * 
   * @param tableName
   *          the table name or {@link #TOTAL_KEY} for all tables
   * @param regionName
   *          regionName
   * @return the prefix for this table/region combination.
   */
  static String generateRegionMetricsPrefix(String tableName, String regionName) {
    tableName = getEffectiveTableName(tableName);
    String schemaMetricPrefix = tableName.equals(TOTAL_KEY) ? "" : TABLE_PREFIX + tableName + ".";
    schemaMetricPrefix += regionName.equals(TOTAL_KEY) ? "" : REGION_PREFIX + regionName + ".";

    return schemaMetricPrefix;
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

  /** "tbl.<table_name>." */
  private static final String TABLE_NAME_RE_STR =
      "\\b" + regexEscape(TABLE_PREFIX) + WORD_AND_DOT_RE_STR;

  /** "cf.<cf_name>." */
  private static final String CF_NAME_RE_STR =
      "\\b" + regexEscape(CF_PREFIX) + WORD_AND_DOT_RE_STR;
  private static final Pattern CF_NAME_RE = Pattern.compile(CF_NAME_RE_STR);

  /** "tbl.<table_name>.cf.<cf_name>." */
  private static final Pattern TABLE_AND_CF_NAME_RE = Pattern.compile(
      TABLE_NAME_RE_STR + CF_NAME_RE_STR);

  private static final Pattern BLOCK_CATEGORY_RE = Pattern.compile(
      "\\b" + regexEscape(BLOCK_TYPE_PREFIX) + "[^.]+\\." +
      // Also remove the special-case block type marker for meta blocks
      "|" + META_BLOCK_CATEGORY_STR + "(?=" +
      BlockMetricType.BLOCK_METRIC_TYPE_RE + ")");

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

  private static final boolean isTimeVaryingKey(String metricKey) {
    return metricKey.endsWith(NUM_OPS_SUFFIX)
        || metricKey.endsWith(TOTAL_SUFFIX);
  }

  private static final String stripTimeVaryingSuffix(String metricKey) {
    return TIME_VARYING_SUFFIX_RE.matcher(metricKey).replaceAll("");
  }

  public static Map<String, Long> getMetricsSnapshot() {
    Map<String, Long> metricsSnapshot = new TreeMap<String, Long>();
    for (SchemaMetrics cfm : tableAndFamilyToMetrics.values()) {
      cfm.flushMetrics();
      for (String metricName : cfm.getAllMetricNames()) {
        long metricValue;
        if (isTimeVaryingKey(metricName)) {
          Pair<Long, Integer> totalAndCount =
              RegionMetricsStorage.getTimeVaryingMetric(stripTimeVaryingSuffix(metricName));
          metricValue = metricName.endsWith(TOTAL_SUFFIX) ?
              totalAndCount.getFirst() : totalAndCount.getSecond();
        } else {
          metricValue = RegionMetricsStorage.getNumericMetric(metricName);
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

  private static void putLong(Map<String, Long> m, String k, long v) {
    if (v != 0) {
      m.put(k, v);
    } else {
      m.remove(k);
    }
  }

  /**
   * @return the difference between two sets of metrics (second minus first).
   *         Only includes keys that have nonzero difference.
   */
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

  public static void validateMetricChanges(Map<String, Long> oldMetrics) {
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
          throw new AssertionError("Column family prefix used twice: " +
              metricName);
        }

        final long oldValue = getLong(oldMetrics, metricName);
        final long newValue = getLong(newMetrics, metricName);
        final long delta = newValue - oldValue;

        // Re-calculate values of metrics with no column family (or CF/table)
        // specified based on all metrics with CF (or CF/table) specified.
        if (delta != 0) {
          if (cfm != ALL_SCHEMA_METRICS) {
            final String aggregateMetricName =
                cfTableMetricRE.matcher(metricName).replaceAll("");
            if (!aggregateMetricName.equals(metricName)) {
              LOG.debug("Counting " + delta + " units of " + metricName
                  + " towards " + aggregateMetricName);

              putLong(allCfDeltas, aggregateMetricName,
                  getLong(allCfDeltas, aggregateMetricName) + delta);
            }
          } else {
            LOG.debug("Metric=" + metricName + ", delta=" + delta);
          }
        }

        Matcher matcher = BLOCK_CATEGORY_RE.matcher(metricName);
        if (matcher.find()) {
           // Only process per-block-category metrics
          String metricNoBlockCategory = matcher.replaceAll("");

          putLong(allBlockCategoryDeltas, metricNoBlockCategory,
              getLong(allBlockCategoryDeltas, metricNoBlockCategory) + delta);
        }
      }
    }

    StringBuilder errors = new StringBuilder();
    for (String key : ALL_SCHEMA_METRICS.getAllMetricNames()) {
      long actual = getLong(deltas, key);
      long expected = getLong(allCfDeltas, key);
      if (actual != expected) {
        if (errors.length() > 0)
          errors.append("\n");
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
        if (errors.length() > 0)
          errors.append("\n");
        errors.append("The all-block-category metric " + key
            + " changed by " + actual + " but the aggregation of "
            + "per-block-category metrics yields " + expected);
      }
    }

    if (errors.length() > 0) {
      throw new AssertionError(errors.toString());
    }
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

}
