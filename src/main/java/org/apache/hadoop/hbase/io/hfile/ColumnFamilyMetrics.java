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

package org.apache.hadoop.hbase.io.hfile;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.BlockType.BlockCategory;
import org.apache.hadoop.hbase.regionserver.HRegion;

/**
 * A collection of metric names in a given column family. The following
 * "dimensions" are supported:
 * <ul>
 * <li>Per-column family vs. aggregated</li>
 * <li>Block category (data, index, bloom filter, etc.)</li>
 * <li>Whether the request is part of a compaction</li>
 * <li>Metric type (read time, block read count, cache hits/misses, etc.)</li>
 * </ul>
 *
 * <p>
 * An instance of this class does not store any metric values. It just allows
 * to determine the correct metric name for each combination of the above
 * dimensions.
 */
public class ColumnFamilyMetrics {
  private static final Log LOG = LogFactory.getLog(ColumnFamilyMetrics.class);

  public interface ColumnFamilyAware {
    /**
     * @return Column family name pertaining to this reader/writer/block
     */
    public String getColumnFamilyName();
  }

  public static class ColumnFamilyConfigured implements ColumnFamilyAware {
    private final String cfName;

    public ColumnFamilyConfigured(Path path) {
      if (path == null) {
        cfName = UNKNOWN_CF;
        return;
      }

      String splits[] = path.toString().split("/");
      if (splits.length < 2) {
        LOG.warn("Could not determine the column family of the HFile path "
            + path);
        cfName = UNKNOWN_CF;
      } else {
        cfName = splits[splits.length - 2];
      }
    }

    @Override
    public String getColumnFamilyName() {
      return cfName;
    }
  }

  private static final String BLOCK_TYPE_PREFIX = "bt.";
  private static final String CF_PREFIX = "cf.";

  /** A special column family value that means "all column families". */
  private static final String CF_ALL = "";

  /**
   * Special handling for meta-block-specific metrics for
   * backwards-compatibility.
   */
  private static final String META_BLOCK_CATEGORY_STR = "Meta";

  static final String UNKNOWN_CF = "unknown";

  public static enum BlockMetricType {
    READ_TIME("Read", true),
    READ_COUNT("BlockReadCnt", true),
    CACHE_HIT("BlockReadCacheHitCnt", true),
    CACHE_MISS("BlockReadCacheMissCnt", true),

    CACHE_SIZE("blockCacheSize", false),
    CACHED("blockCacheNumCached", false),
    EVICTED("blockCacheNumEvicted", false);

    private final String metricStr;
    private final boolean compactionAware;

    BlockMetricType(String metricStr, boolean compactionAware) {
      this.metricStr = metricStr;
      this.compactionAware = compactionAware;
    }

    @Override
    public String toString() {
      return metricStr;
    }

    private static final String BLOCK_METRIC_TYPE_REGEX;
    static {
      StringBuilder sb = new StringBuilder();
      for (BlockMetricType bmt : values()) {
        if (sb.length() > 0)
          sb.append("|");
        sb.append(bmt);
      }
      BLOCK_METRIC_TYPE_REGEX = sb.toString();
    }
  };

  private static final int NUM_BLOCK_CATEGORIES = BlockCategory.values().length;

  private static final int NUM_METRIC_TYPES = BlockMetricType.values().length;

  private static final boolean[] BOOL_VALUES = new boolean[] { false, true };

  private static final int NUM_METRICS =
      NUM_BLOCK_CATEGORIES *  // blockCategory
      BOOL_VALUES.length *    // isCompaction
      NUM_METRIC_TYPES;       // metricType

  /** All instances of this class */
  private static final ConcurrentHashMap<String, ColumnFamilyMetrics>
      cfToMetrics = new ConcurrentHashMap<String, ColumnFamilyMetrics>();

  private final String[] blockMetricNames = new String[NUM_METRICS];
  private final String[] bloomMetricNames = new String[2];

  public static final ColumnFamilyMetrics ALL_CF_METRICS = getInstance(CF_ALL);

  private ColumnFamilyMetrics(final String cf) {
    final String cfPrefix = cf.equals(CF_ALL) ? "" : CF_PREFIX + cf + ".";
    for (BlockCategory blockCategory : BlockCategory.values()) {
      for (boolean isCompaction : BOOL_VALUES) {
        for (BlockMetricType metricType : BlockMetricType.values()) {
          if (!metricType.compactionAware && isCompaction)
            continue;

          StringBuilder sb = new StringBuilder(cfPrefix);
          if (blockCategory != BlockCategory.ALL_CATEGORIES
              && blockCategory != BlockCategory.META) {
            String categoryStr = blockCategory.toString();
            categoryStr = categoryStr.charAt(0)
                + categoryStr.substring(1).toLowerCase();
            sb.append(BLOCK_TYPE_PREFIX + categoryStr + ".");
          }

          if (metricType.compactionAware)
            sb.append(isCompaction ? "compaction" : "fs");

          // A special-case for meta blocks for backwards-compatibility.
          if (blockCategory == BlockCategory.META)
            sb.append(META_BLOCK_CATEGORY_STR);

          sb.append(metricType);

          int i = getBlockMetricIndex(blockCategory, isCompaction, metricType);
          blockMetricNames[i] = sb.toString().intern();
        }
      }
    }

    for (boolean isInBloom : BOOL_VALUES) {
      bloomMetricNames[isInBloom ? 1 : 0] = cfPrefix
          + (isInBloom ? "keyMaybeInBloomCnt" : "keyNotInBloomCnt");
    }
  }

  /**
   * Returns a metrics object for the given column family, instantiating if
   * necessary.
   *
   * @param cf column family name or null in case of unit tests or necessary
   *          fallback
   */
  public static ColumnFamilyMetrics getInstance(String cf) {
    if (cf == null)
      cf = UNKNOWN_CF;
    ColumnFamilyMetrics cfMetrics = cfToMetrics.get(cf);
    if (cfMetrics != null)
      return cfMetrics;
    cfMetrics = new ColumnFamilyMetrics(cf);
    ColumnFamilyMetrics existingMetrics = cfToMetrics.putIfAbsent(cf,
        cfMetrics);
    return existingMetrics != null ? existingMetrics : cfMetrics;
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

  /** Used in testing */
  void printMetricNames() {
    for (BlockCategory blockCategory : BlockCategory.values()) {
      for (boolean isCompaction : BOOL_VALUES) {
        for (BlockMetricType metricType : BlockMetricType.values()) {
          int i = getBlockMetricIndex(blockCategory, isCompaction, metricType);
          System.err.println("blockCategory=" + blockCategory + ", "
              + "metricType=" + metricType + ", isCompaction=" + isCompaction
              + ", metricName=" + blockMetricNames[i]);
        }
      }
    }
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
      incrNumericMetric(BlockCategory.ALL_CATEGORIES, isCompaction, metricType);
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
   * Updates the number of hits and the total number of block reads on a block
   * cache hit.
   */
  public void updateOnCacheHit(BlockCategory blockCategory,
      boolean isCompaction) {
    incrNumericMetric(blockCategory, isCompaction, BlockMetricType.CACHE_HIT);
    incrNumericMetric(blockCategory, isCompaction, BlockMetricType.READ_COUNT);
    if (this != ALL_CF_METRICS) {
      ALL_CF_METRICS.updateOnCacheHit(blockCategory, isCompaction);
    }
  }

  /**
   * Updates read time, the number of misses, and the total number of block
   * reads on a block cache miss.
   */
  public void updateOnCacheMiss(BlockCategory blockCategory,
      boolean isCompaction, long timeMs) {
    addToReadTime(blockCategory, isCompaction, timeMs);
    incrNumericMetric(blockCategory, isCompaction, BlockMetricType.CACHE_MISS);
    incrNumericMetric(blockCategory, isCompaction, BlockMetricType.READ_COUNT);
    if (this != ALL_CF_METRICS) {
      ALL_CF_METRICS.updateOnCacheMiss(blockCategory, isCompaction, timeMs);
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
    HRegion.incrNumericPersistentMetric(getBlockMetricName(category, false,
        BlockMetricType.CACHE_SIZE), cacheSizeDelta);

    if (category != BlockCategory.ALL_CATEGORIES) {
      addToCacheSize(BlockCategory.ALL_CATEGORIES, cacheSizeDelta);
    }
  }

  public void updateBlockCacheMetrics(BlockCategory blockCategory,
      long cacheSizeDelta, boolean isEviction) {
    addToCacheSize(blockCategory, cacheSizeDelta);
    incrNumericMetric(blockCategory, false,
        isEviction ? BlockMetricType.EVICTED : BlockMetricType.CACHED);
    if (this != ALL_CF_METRICS) {
      ALL_CF_METRICS.updateBlockCacheMetrics(blockCategory, cacheSizeDelta,
          isEviction);
    }
  }

  /**
   * Increments both the per-CF and the aggregate counter of bloom
   * positives/negatives as specified by the argument.
   */
  public void updateBloomMetrics(boolean isInBloom) {
    HRegion.incrNumericMetric(getBloomMetricName(isInBloom), 1);
    if (this != ALL_CF_METRICS) {
      ALL_CF_METRICS.updateBloomMetrics(isInBloom);
    }
  }

  // Test code

  private Collection<String> getAllMetricNames() {
    List<String> allMetricNames = new ArrayList<String>();
    for (String blockMetricName : blockMetricNames)
      if (blockMetricName != null)
        allMetricNames.add(blockMetricName);
    allMetricNames.addAll(Arrays.asList(bloomMetricNames));
    return allMetricNames;
  }

  public static Map<String, Long> getMetricsSnapshot() {
    Map<String, Long> metricsSnapshot = new TreeMap<String, Long>();
    for (ColumnFamilyMetrics cfm : cfToMetrics.values()) {
      for (String metricName : cfm.getAllMetricNames()) {
        metricsSnapshot.put(metricName, HRegion.getNumericMetric(metricName));
      }
    }
    return metricsSnapshot;
  }

  private static final Pattern CF_NAME_REGEX = Pattern.compile(
      "\\b" + CF_PREFIX.replace(".", "\\.") + "[^.]+\\.");
  private static final Pattern BLOCK_CATEGORY_REGEX = Pattern.compile(
      "\\b" + BLOCK_TYPE_PREFIX.replace(".", "\\.") + "[^.]+\\." +
      // Also remove the special-case block type marker for meta blocks
      "|" + META_BLOCK_CATEGORY_STR + "(?=" +
      BlockMetricType.BLOCK_METRIC_TYPE_REGEX + ")");

  private static long getLong(Map<String, Long> m, String k) {
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
  private static Map<String, Long> diffMetrics(Map<String, Long> a,
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

  /** Used in unit tests */
  public static void validateMetricChanges(Map<String, Long> oldMetrics) {
    final Map<String, Long> newMetrics = getMetricsSnapshot();
    final Set<String> allKeys = new TreeSet<String>(oldMetrics.keySet());
    allKeys.addAll(newMetrics.keySet());
    final Map<String, Long> allCfDeltas = new TreeMap<String, Long>();
    final Map<String, Long> allBlockCategoryDeltas =
        new TreeMap<String, Long>();
    final Map<String, Long> deltas = diffMetrics(oldMetrics, newMetrics);

    for (ColumnFamilyMetrics cfm : cfToMetrics.values()) {
      for (String metricName : cfm.getAllMetricNames()) {
        if (metricName.startsWith(CF_PREFIX + CF_PREFIX)) {
          throw new AssertionError("Column family prefix used twice: " +
              metricName);
        }

        final long oldValue = getLong(oldMetrics, metricName);
        final long newValue = getLong(newMetrics, metricName);
        final long delta = newValue - oldValue;
        if (oldValue != newValue) {
          // Debug output for the unit test
          System.err.println("key=" + metricName + ", delta=" + delta);
        }

        if (cfm != ALL_CF_METRICS) {
          // Re-calculate values of metrics with no column family specified
          // based on all metrics with cf specified, like this one.
          final String metricNoCF =
              CF_NAME_REGEX.matcher(metricName).replaceAll("");
          putLong(allCfDeltas, metricNoCF,
              getLong(allCfDeltas, metricNoCF) + delta);
        }

        Matcher matcher = BLOCK_CATEGORY_REGEX.matcher(metricName);
        if (matcher.find()) {
           // Only process per-block-category metrics
          String metricNoBlockCategory = matcher.replaceAll("");
          putLong(allBlockCategoryDeltas, metricNoBlockCategory,
              getLong(allBlockCategoryDeltas, metricNoBlockCategory) + delta);
        }
      }
    }

    StringBuilder errors = new StringBuilder();
    for (String key : ALL_CF_METRICS.getAllMetricNames()) {
      long actual = getLong(deltas, key);
      long expected = getLong(allCfDeltas, key);
      if (actual != expected) {
        if (errors.length() > 0)
          errors.append("\n");
        errors.append("The all-CF metric " + key + " changed by "
            + actual + " but the aggregation of per-column-family metrics "
            + "yields " + expected);
      }
    }

    // Verify metrics computed for all block types based on the aggregation
    // of per-block-type metrics.
    for (String key : allKeys) {
      if (BLOCK_CATEGORY_REGEX.matcher(key).find() ||
          key.contains(ALL_CF_METRICS.getBloomMetricName(false)) ||
          key.contains(ALL_CF_METRICS.getBloomMetricName(true))){
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

}
