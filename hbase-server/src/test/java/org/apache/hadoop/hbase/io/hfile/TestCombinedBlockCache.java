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

import static org.apache.hadoop.hbase.HConstants.BUCKET_CACHE_IOENGINE_KEY;
import static org.apache.hadoop.hbase.HConstants.BUCKET_CACHE_SIZE_KEY;
import static org.apache.hadoop.hbase.io.ByteBuffAllocator.HEAP;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.hfile.CombinedBlockCache.CombinedCacheStats;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ SmallTests.class })
public class TestCombinedBlockCache {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCombinedBlockCache.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  @Test
  public void testCombinedCacheStats() {
    CacheStats lruCacheStats = new CacheStats("lruCacheStats", 2);
    CacheStats bucketCacheStats = new CacheStats("bucketCacheStats", 2);
    CombinedCacheStats stats = new CombinedCacheStats(lruCacheStats, bucketCacheStats);

    double delta = 0.01;

    // period 1:
    // lru cache: 1 hit caching, 1 miss caching
    // bucket cache: 2 hit non-caching,1 miss non-caching/primary,1 fail insert
    lruCacheStats.hit(true, true, BlockType.DATA);
    lruCacheStats.miss(true, false, BlockType.DATA);
    bucketCacheStats.hit(false, true, BlockType.DATA);
    bucketCacheStats.hit(false, true, BlockType.DATA);
    bucketCacheStats.miss(false, true, BlockType.DATA);

    assertEquals(5, stats.getRequestCount());
    assertEquals(2, stats.getRequestCachingCount());
    assertEquals(2, stats.getMissCount());
    assertEquals(1, stats.getPrimaryMissCount());
    assertEquals(1, stats.getMissCachingCount());
    assertEquals(3, stats.getHitCount());
    assertEquals(3, stats.getPrimaryHitCount());
    assertEquals(1, stats.getHitCachingCount());
    assertEquals(0.6, stats.getHitRatio(), delta);
    assertEquals(0.5, stats.getHitCachingRatio(), delta);
    assertEquals(0.4, stats.getMissRatio(), delta);
    assertEquals(0.5, stats.getMissCachingRatio(), delta);

    // lru cache: 2 evicted, 1 evict
    // bucket cache: 1 evict
    lruCacheStats.evicted(1000, true);
    lruCacheStats.evicted(1000, false);
    lruCacheStats.evict();
    bucketCacheStats.evict();
    assertEquals(2, stats.getEvictionCount());
    assertEquals(2, stats.getEvictedCount());
    assertEquals(1, stats.getPrimaryEvictedCount());
    assertEquals(1.0, stats.evictedPerEviction(), delta);

    // lru cache: 1 fail insert
    lruCacheStats.failInsert();
    assertEquals(1, stats.getFailedInserts());

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
    lruCacheStats.hit(true, true, BlockType.DATA);
    lruCacheStats.hit(true, true, BlockType.DATA);
    lruCacheStats.hit(true, true, BlockType.DATA);
    stats.rollMetricsPeriod();
    assertEquals(6, stats.getSumHitCountsPastNPeriods());
    assertEquals(8, stats.getSumRequestCountsPastNPeriods());
    assertEquals(4, stats.getSumHitCachingCountsPastNPeriods());
    assertEquals(5, stats.getSumRequestCachingCountsPastNPeriods());
    assertEquals(0.75, stats.getHitRatioPastNPeriods(), delta);
    assertEquals(0.8, stats.getHitCachingRatioPastNPeriods(), delta);
  }

  @Test
  public void testMultiThreadGetAndEvictBlock() throws Exception {
    BlockCache blockCache = createCombinedBlockCache();
    TestLruBlockCache.testMultiThreadGetAndEvictBlockInternal(blockCache);
  }

  @Test
  public void testCombinedBlockCacheStatsWithDataBlockType() throws Exception {
    testCombinedBlockCacheStats(BlockType.DATA, 0, 1);
  }

  @Test
  public void testCombinedBlockCacheStatsWithMetaBlockType() throws Exception {
    testCombinedBlockCacheStats(BlockType.META, 1, 0);
  }

  @Test
  public void testCombinedBlockCacheStatsWithNoBlockType() throws Exception {
    testCombinedBlockCacheStats(null, 0, 1);
  }

  @Test
  public void testCombinedBlockCacheStatsWithRowCellsBlockType() throws Exception {
    // ROW_CELLS type is cached only in the L1 cache, since it is not a DATA block type
    testCombinedBlockCacheStats(BlockType.ROW_CELLS, 1, 0);
  }

  private CombinedBlockCache createCombinedBlockCache() {
    Configuration conf = UTIL.getConfiguration();
    conf.set(BUCKET_CACHE_IOENGINE_KEY, "offheap");
    conf.setInt(BUCKET_CACHE_SIZE_KEY, 32);
    BlockCache blockCache = BlockCacheFactory.createBlockCache(conf);
    Assert.assertTrue(blockCache instanceof CombinedBlockCache);
    return (CombinedBlockCache) blockCache;
  }

  public void testCombinedBlockCacheStats(BlockType type, int expectedL1Miss, int expectedL2Miss)
    throws Exception {
    CombinedBlockCache blockCache = createCombinedBlockCache();
    BlockCacheKey key = new BlockCacheKey("key1", 0, false, type);
    int size = 100;
    int length = HConstants.HFILEBLOCK_HEADER_SIZE + size;
    byte[] byteArr = new byte[length];
    HFileContext meta = new HFileContextBuilder().build();
    HFileBlock blk = new HFileBlock(type != null ? type : BlockType.DATA, size, size, -1,
      ByteBuff.wrap(ByteBuffer.wrap(byteArr, 0, size)), HFileBlock.FILL_HEADER, -1, 52, -1, meta,
      HEAP);
    blockCache.cacheBlock(key, blk);
    blockCache.getBlock(key, true, false, true);
    assertEquals(0, blockCache.getStats().getMissCount());
    blockCache.evictBlock(key);
    blockCache.getBlock(key, true, false, true);
    assertEquals(1, blockCache.getStats().getMissCount());
    assertEquals(expectedL1Miss, blockCache.getFirstLevelCache().getStats().getMissCount());
    assertEquals(expectedL2Miss, blockCache.getSecondLevelCache().getStats().getMissCount());
  }

}
