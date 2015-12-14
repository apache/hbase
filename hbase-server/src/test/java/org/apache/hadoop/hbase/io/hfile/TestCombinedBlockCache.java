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
package org.apache.hadoop.hbase.io.hfile;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.io.hfile.CombinedBlockCache.CombinedCacheStats;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SmallTests.class})
public class TestCombinedBlockCache {
  @Test
  public void testCombinedCacheStats() {
    CacheStats lruCacheStats = new CacheStats("lruCacheStats", 2);
    CacheStats bucketCacheStats = new CacheStats("bucketCacheStats", 2);
    CombinedCacheStats stats =
        new CombinedCacheStats(lruCacheStats, bucketCacheStats);
    
    double delta = 0.01;
    
    // period 1:
    // lru cache: 1 hit caching, 1 miss caching
    // bucket cache: 2 hit non-caching,1 miss non-caching/primary,1 fail insert
    lruCacheStats.hit(true);
    lruCacheStats.miss(true);
    bucketCacheStats.hit(false);
    bucketCacheStats.hit(false);
    bucketCacheStats.miss(false);
    
    assertEquals(5, stats.getRequestCount());
    assertEquals(2, stats.getRequestCachingCount());
    assertEquals(2, stats.getMissCount());
    assertEquals(1, stats.getMissCachingCount());
    assertEquals(3, stats.getHitCount());
    assertEquals(1, stats.getHitCachingCount());
    assertEquals(0.6, stats.getHitRatio(), delta);
    assertEquals(0.5, stats.getHitCachingRatio(), delta);
    assertEquals(0.4, stats.getMissRatio(), delta);
    assertEquals(0.5, stats.getMissCachingRatio(), delta);
    
    
    // lru cache: 2 evicted, 1 evict
    // bucket cache: 1 evict
    lruCacheStats.evicted(1000);
    lruCacheStats.evicted(1000);
    lruCacheStats.evict();
    bucketCacheStats.evict();
    assertEquals(2, stats.getEvictionCount());
    assertEquals(2, stats.getEvictedCount());
    assertEquals(1.0, stats.evictedPerEviction(), delta);
    
    // rollMetricsPeriod
    stats.rollMetricsPeriod();
    assertEquals(3, stats.getSumHitCountsPastNPeriods());
    assertEquals(5, stats.getSumRequestCountsPastNPeriods());
    assertEquals(1, stats.getSumHitCachingCountsPastNPeriods());
    assertEquals(2, stats.getSumRequestCachingCountsPastNPeriods());
    assertEquals(0.6, stats.getHitRatioPastNPeriods(), delta);
    assertEquals(0.5, stats.getHitCachingRatioPastNPeriods(), delta);
    
    // period 2:
    // lru cache: 3 hit caching
    lruCacheStats.hit(true);
    lruCacheStats.hit(true);
    lruCacheStats.hit(true);
    stats.rollMetricsPeriod();
    assertEquals(6, stats.getSumHitCountsPastNPeriods());
    assertEquals(8, stats.getSumRequestCountsPastNPeriods());
    assertEquals(4, stats.getSumHitCachingCountsPastNPeriods());
    assertEquals(5, stats.getSumRequestCachingCountsPastNPeriods());
    assertEquals(0.75, stats.getHitRatioPastNPeriods(), delta);
    assertEquals(0.8, stats.getHitCachingRatioPastNPeriods(), delta);
  }
}
