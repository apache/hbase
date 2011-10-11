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

import org.apache.hadoop.hbase.io.hfile.BlockType.BlockCategory;
import static org.apache.hadoop.hbase.io.hfile.ColumnFamilyMetrics.BlockMetricType;
import static org.apache.hadoop.hbase.io.hfile.ColumnFamilyMetrics.ALL_CF_METRICS;
import static org.junit.Assert.*;

import org.junit.Test;

public class TestColumnFamilyMetrics {

  @Test
  public void testNaming() {
    final String cfName = "myColumnFamily";
    final String cfPrefix = "cf." + cfName + ".";
    ColumnFamilyMetrics cfMetrics = ColumnFamilyMetrics.getInstance(cfName);

    // fsReadTimeMetric
    assertEquals(cfPrefix + "fsRead", cfMetrics.getBlockMetricName(
        BlockCategory.ALL_CATEGORIES, false, BlockMetricType.READ_TIME));

    // compactionReadTimeMetric
    assertEquals(cfPrefix + "compactionRead", cfMetrics.getBlockMetricName(
        BlockCategory.ALL_CATEGORIES, true, BlockMetricType.READ_TIME));

    // fsBlockReadCntMetric
    assertEquals(cfPrefix + "fsBlockReadCnt", cfMetrics.getBlockMetricName(
        BlockCategory.ALL_CATEGORIES, false, BlockMetricType.READ_COUNT));

    // fsBlockReadCacheHitCntMetric
    assertEquals(cfPrefix + "fsBlockReadCacheHitCnt",
        cfMetrics.getBlockMetricName(BlockCategory.ALL_CATEGORIES, false,
            BlockMetricType.CACHE_HIT));

    // fsBlockReadCacheMissCntMetric
    assertEquals(cfPrefix + "fsBlockReadCacheMissCnt",
        cfMetrics.getBlockMetricName(BlockCategory.ALL_CATEGORIES, false,
            BlockMetricType.CACHE_MISS));

    // compactionBlockReadCntMetric
    assertEquals(cfPrefix + "compactionBlockReadCnt",
        cfMetrics.getBlockMetricName(BlockCategory.ALL_CATEGORIES, true,
            BlockMetricType.READ_COUNT));

    // compactionBlockReadCacheHitCntMetric
    assertEquals(cfPrefix + "compactionBlockReadCacheHitCnt",
        cfMetrics.getBlockMetricName(BlockCategory.ALL_CATEGORIES, true,
            BlockMetricType.CACHE_HIT));

    // compactionBlockReadCacheMissCntMetric
    assertEquals(cfPrefix + "compactionBlockReadCacheMissCnt",
        cfMetrics.getBlockMetricName(BlockCategory.ALL_CATEGORIES, true,
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
    assertEquals(cfPrefix + "bt.Index.fsBlockReadCnt",
        cfMetrics.getBlockMetricName(BlockCategory.INDEX, false,
            BlockMetricType.READ_COUNT));

    assertEquals(cfPrefix + "bt.Data.compactionBlockReadCacheHitCnt",
        cfMetrics.getBlockMetricName(BlockCategory.DATA, true,
            BlockMetricType.CACHE_HIT));

    // A special case for Meta blocks
    assertEquals(cfPrefix + "compactionMetaBlockReadCacheHitCnt",
        cfMetrics.getBlockMetricName(BlockCategory.META, true,
            BlockMetricType.CACHE_HIT));

    // Cache metrics
    assertEquals(cfPrefix + "blockCacheSize", cfMetrics.getBlockMetricName(
        BlockCategory.ALL_CATEGORIES, false, BlockMetricType.CACHE_SIZE));

    assertEquals(cfPrefix + "bt.Index.blockCacheNumEvicted",
        cfMetrics.getBlockMetricName(BlockCategory.INDEX, false,
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
    assertEquals(cfPrefix + "keyNotInBloomCnt",
        cfMetrics.getBloomMetricName(false));

    cfMetrics.printMetricNames();
  }

}
