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
package org.apache.hadoop.hbase.io.hfile.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link AggregateCacheStats}.
 */
@Tag(IOTests.TAG)
@Tag(SmallTests.TAG)
public class TestAggregateCacheStats {

  /**
   * Verifies that hit, miss, request, and eviction counts are aggregated across all delegates.
   */
  @Test
  void testAggregatesCacheStatistics() {
    CacheStats l1 = new CacheStats("l1");
    CacheStats l2 = new CacheStats("l2");

    l1.hit(true, true, BlockType.LEAF_INDEX);
    l1.miss(true, true, BlockType.DATA);
    l1.evict();
    l1.evicted(1L, true);

    l2.hit(true, true, BlockType.DATA);
    l2.hit(true, false, BlockType.DATA);
    l2.miss(true, false, BlockType.DATA);
    l2.evict();
    l2.evicted(2L, false);

    CacheStats stats = new AggregateCacheStats("aggregate", l1, l2);

    assertEquals(3L, stats.getHitCount());
    assertEquals(2L, stats.getMissCount());
    assertEquals(5L, stats.getRequestCount());

    assertEquals(1L, stats.getLeafIndexHitCount());
    assertEquals(2L, stats.getDataHitCount());
    assertEquals(2L, stats.getDataMissCount());

    assertEquals(2L, stats.getPrimaryHitCount());
    assertEquals(1L, stats.getPrimaryMissCount());

    assertEquals(3L, stats.getHitCachingCount());
    assertEquals(2L, stats.getMissCachingCount());
    assertEquals(5L, stats.getRequestCachingCount());

    assertEquals(2L, stats.getEvictionCount());
    assertEquals(2L, stats.getEvictedCount());
    assertEquals(1L, stats.getPrimaryEvictedCount());
  }

  /**
   * Verifies that failed insertion counts are aggregated across all delegates.
   */
  @Test
  void testAggregatesFailedInserts() {
    CacheStats l1 = new CacheStats("l1");
    CacheStats l2 = new CacheStats("l2");

    l1.failInsert();
    l2.failInsert();
    l2.failInsert();

    CacheStats stats = new AggregateCacheStats("aggregate", l1, l2);

    assertEquals(3L, stats.getFailedInserts());
  }

  /**
   * Verifies that rolling the aggregate statistics rolls all delegate statistics.
   */
  @Test
  void testRollMetricsPeriod() {
    CacheStats l1 = new CacheStats("l1");
    CacheStats l2 = new CacheStats("l2");

    l1.hit(true, true, BlockType.DATA);
    l2.miss(true, true, BlockType.DATA);

    CacheStats stats = new AggregateCacheStats("aggregate", l1, l2);

    stats.rollMetricsPeriod();

    assertEquals(1L, stats.getSumHitCountsPastNPeriods());
    assertEquals(2L, stats.getSumRequestCountsPastNPeriods());
    assertEquals(1L, stats.getSumHitCachingCountsPastNPeriods());
    assertEquals(2L, stats.getSumRequestCachingCountsPastNPeriods());
  }
}
