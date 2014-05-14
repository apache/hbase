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

import static org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics.BOOL_VALUES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.BlockType.BlockCategory;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics.BlockMetricType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@Category(SmallTests.class)
@RunWith(Parameterized.class)
public class TestSchemaMetrics {

  private static final Log LOG = LogFactory.getLog(TestSchemaMetrics.class);

  private final String TABLE_NAME = "myTable";
  private final String CF_NAME = "myColumnFamily";

  private final boolean useTableName;
  private final String metricPrefix;
  private final SchemaMetrics schemaMetrics;

  private Map<String, Long> startingMetrics;

  @Parameters
  public static Collection<Object[]> parameters() {
    return HBaseTestingUtility.BOOLEAN_PARAMETERIZED;
  }

  public TestSchemaMetrics(boolean useTableName) {
    this.useTableName = useTableName;
    SchemaMetrics.setUseTableNameInTest(useTableName);
    metricPrefix = (useTableName ? SchemaMetrics.TABLE_PREFIX +
        TABLE_NAME + "." : "") + SchemaMetrics.CF_PREFIX + CF_NAME + ".";
    schemaMetrics = SchemaMetrics.getInstance(TABLE_NAME,
        CF_NAME);
  }

  @Before
  public void setUp() {
    startingMetrics = SchemaMetrics.getMetricsSnapshot();
  };

  @Test
  public void testPersistentMetric() {
    for (BlockCategory blockCat : BlockCategory.values()) {
      for (boolean isCompaction : HConstants.BOOLEAN_VALUES) {
        for (BlockMetricType bmt : BlockMetricType.values()) {
          if (!bmt.compactionAware() && isCompaction) {
            continue;
          }
          String metricKey = schemaMetrics.getBlockMetricName(blockCat, isCompaction, bmt);
          assertEquals("Incorrectly identified whether metric key is persistent: " + metricKey,
              bmt.persistent(), SchemaMetrics.isPersistentMetricKey(metricKey));
        }
      }
    }
  }

  @Test
  public void testRemoveBlockCategoryFromMetricKey() {
    for (BlockCategory blockCat : BlockCategory.values()) {
      if (blockCat == BlockCategory.ALL_CATEGORIES) {
        continue;
      }
      for (boolean isCompaction : HConstants.BOOLEAN_VALUES) {
        for (BlockMetricType bmt : BlockMetricType.values()) {
          if (!bmt.compactionAware() && isCompaction) {
            continue;
          }
          String metricKey = schemaMetrics.getBlockMetricName(blockCat, isCompaction, bmt);
          String metricKeyNoBlockCat =
              schemaMetrics.getBlockMetricName(BlockCategory.ALL_CATEGORIES, isCompaction, bmt);
          assertEquals(metricKeyNoBlockCat,
              SchemaMetrics.BLOCK_CATEGORY_RE.matcher(metricKey).replaceAll(""));
        }
      }
    }

  }

  @Test
  public void testNaming() {
    SchemaMetrics ALL_CF_METRICS = SchemaMetrics.ALL_SCHEMA_METRICS;

    // fsReadTimeMetric
    assertEquals(metricPrefix + "fsRead", schemaMetrics.getBlockMetricName(
        BlockCategory.ALL_CATEGORIES, false, BlockMetricType.READ_TIME));

    // compactionReadTimeMetric
    assertEquals(metricPrefix + "compactionRead",
        schemaMetrics.getBlockMetricName(BlockCategory.ALL_CATEGORIES, true,
            BlockMetricType.READ_TIME));

    // fsBlockReadCntMetric
    assertEquals(metricPrefix + "fsBlockReadCnt",
        schemaMetrics.getBlockMetricName(BlockCategory.ALL_CATEGORIES, false,
            BlockMetricType.READ_COUNT));

    // fsBlockReadCacheHitCntMetric
    assertEquals(metricPrefix + "fsBlockReadCacheHitCnt",
        schemaMetrics.getBlockMetricName(BlockCategory.ALL_CATEGORIES, false,
            BlockMetricType.CACHE_HIT));

    // fsBlockReadCacheMissCntMetric
    assertEquals(metricPrefix + "fsBlockReadCacheMissCnt",
        schemaMetrics.getBlockMetricName(BlockCategory.ALL_CATEGORIES, false,
            BlockMetricType.CACHE_MISS));

    // compactionBlockReadCntMetric
    assertEquals(metricPrefix + "compactionBlockReadCnt",
        schemaMetrics.getBlockMetricName(BlockCategory.ALL_CATEGORIES, true,
            BlockMetricType.READ_COUNT));

    // compactionBlockReadCacheHitCntMetric
    assertEquals(metricPrefix + "compactionBlockReadCacheHitCnt",
        schemaMetrics.getBlockMetricName(BlockCategory.ALL_CATEGORIES, true,
            BlockMetricType.CACHE_HIT));

    // compactionBlockReadCacheMissCntMetric
    assertEquals(metricPrefix + "compactionBlockReadCacheMissCnt",
        schemaMetrics.getBlockMetricName(BlockCategory.ALL_CATEGORIES, true,
            BlockMetricType.CACHE_MISS));

    // fsMetaBlockReadCntMetric
    assertEquals("fsMetaBlockReadCnt", ALL_CF_METRICS.getBlockMetricName(
        BlockCategory.META, false, BlockMetricType.READ_COUNT));

    // fsMetaBlockReadCacheHitCntMetric
    assertEquals("fsMetaBlockReadCacheHitCnt",
        ALL_CF_METRICS.getBlockMetricName(BlockCategory.META, false,
            BlockMetricType.CACHE_HIT));

    // fsMetaBlockReadCacheMissCntMetric
    assertEquals("fsMetaBlockReadCacheMissCnt",
        ALL_CF_METRICS.getBlockMetricName(BlockCategory.META, false,
            BlockMetricType.CACHE_MISS));

    // Per-(column family, block type) statistics.
    assertEquals(metricPrefix + "bt.Index.fsBlockReadCnt",
        schemaMetrics.getBlockMetricName(BlockCategory.INDEX, false,
            BlockMetricType.READ_COUNT));

    assertEquals(metricPrefix + "bt.Data.compactionBlockReadCacheHitCnt",
        schemaMetrics.getBlockMetricName(BlockCategory.DATA, true,
            BlockMetricType.CACHE_HIT));

    // A special case for Meta blocks
    assertEquals(metricPrefix + "compactionMetaBlockReadCacheHitCnt",
        schemaMetrics.getBlockMetricName(BlockCategory.META, true,
            BlockMetricType.CACHE_HIT));

    // Cache metrics
    assertEquals(metricPrefix + "blockCacheSize",
        schemaMetrics.getBlockMetricName(BlockCategory.ALL_CATEGORIES, false,
            BlockMetricType.CACHE_SIZE));

    assertEquals(metricPrefix + "cacheNumBlocks",
        schemaMetrics.getBlockMetricName(BlockCategory.ALL_CATEGORIES, false,
            BlockMetricType.CACHE_NUM_BLOCKS));

    assertEquals(metricPrefix + "bt.Index.blockCacheNumEvicted",
        schemaMetrics.getBlockMetricName(BlockCategory.INDEX, false,
            BlockMetricType.EVICTED));

    assertEquals("bt.Data.blockCacheNumCached",
        ALL_CF_METRICS.getBlockMetricName(BlockCategory.DATA, false,
            BlockMetricType.CACHED));

    assertEquals("blockCacheNumCached", ALL_CF_METRICS.getBlockMetricName(
        BlockCategory.ALL_CATEGORIES, false, BlockMetricType.CACHED));
    
    // "Non-compaction aware" metrics
    try {
      ALL_CF_METRICS.getBlockMetricName(BlockCategory.ALL_CATEGORIES, true,
          BlockMetricType.CACHE_SIZE);
      fail("Exception expected");
    } catch (IllegalArgumentException ex) {
    }

    // Bloom metrics
    assertEquals("keyMaybeInBloomCnt", ALL_CF_METRICS.getBloomMetricName(true));
    assertEquals(metricPrefix + "keyNotInBloomCnt",
        schemaMetrics.getBloomMetricName(false));

    schemaMetrics.printMetricNames();
  }

  public void checkMetrics() {
    SchemaMetrics.validateMetricChanges(startingMetrics);
  }

  private static final int NUM_TABLES = 3;
  private static final int NUM_FAMILIES = 3;
  private static final int BLOCK_SIZE_RANGE = 1024 * 1024;
  private static final int READ_TIME_MS_RANGE = 1000;

  @Test
  public void testIncrements() {
    Random rand = new Random(23982737L);
    for (int i = 1; i <= NUM_TABLES; ++i) {
      final String tableName = "table" + i;
      for (int j = 1; j <= NUM_FAMILIES; ++j) {
        final String cfName = "cf" + j;
        SchemaMetrics sm = SchemaMetrics.getInstance(tableName, cfName);
        for (boolean isInBloom : BOOL_VALUES) {
          sm.updateBloomMetrics(isInBloom);
          checkMetrics();
        }

        for (BlockCategory blockCat : BlockType.BlockCategory.values()) {
          if (blockCat == BlockCategory.ALL_CATEGORIES) {
            continue;
          }

          for (boolean isCompaction : BOOL_VALUES) {
            sm.updateOnBlockRead(blockCat, isCompaction, 0, true, false, false);
            checkMetrics();
            sm.updateOnBlockRead(blockCat, isCompaction,
                    rand.nextInt(READ_TIME_MS_RANGE), false, false, false);
            checkMetrics();
          }

          for (boolean isEviction : BOOL_VALUES) {
            int encodedDelta = (isEviction ? -1 : 1) * (rand.nextInt(BLOCK_SIZE_RANGE) + 1);
            sm.updateOnCachePutOrEvict(blockCat, encodedDelta, 2 * encodedDelta);
          }
        }
      }
    }
  }

  @Test
  public void testSchemaConfiguredHeapSize() {
    SchemaConfigured sc = new SchemaConfigured(null, TABLE_NAME, CF_NAME);
    assertEquals(ClassSize.estimateBase(SchemaConfigured.class, true),
        sc.heapSize());
  }

  @Test
  public void testGenerateSchemaMetricsPrefix() {
    String tableName = "table1";
    int numCF = 3;

    StringBuilder expected = new StringBuilder();
    if (useTableName) {
      expected.append(SchemaMetrics.TABLE_PREFIX);
      expected.append(tableName);
      expected.append(".");
    }
    expected.append(SchemaMetrics.CF_PREFIX);
    Set<byte[]> families = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    for (int i = 1; i <= numCF; i++) {
      String cf = "cf" + i;
      families.add(Bytes.toBytes(cf));
      expected.append(cf);
      if (i == numCF) {
        expected.append(".");
      } else {
        expected.append("~");
      }
    }

    String result = SchemaMetrics.generateSchemaMetricsPrefix(tableName,
        families);
    assertEquals(expected.toString(), result);
  }

}
