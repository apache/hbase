/**
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.hfile.LruBlockCache.EvictionThread;
import org.apache.hadoop.hbase.util.ClassSize;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests the concurrent LruBlockCache.<p>
 *
 * Tests will ensure it grows and shrinks in size properly,
 * evictions run when they're supposed to and do what they should,
 * and that cached blocks are accessible when expected to be.
 */
@Category({IOTests.class, SmallTests.class})
public class TestLruBlockCache {


  @Test
  public void testBackgroundEvictionThread() throws Exception {
    long maxSize = 100000;
    int numBlocks = 9;
    long blockSize = calculateBlockSizeDefault(maxSize, numBlocks);
    assertTrue("calculateBlockSize appears broken.", blockSize * numBlocks <= maxSize);

    LruBlockCache cache = new LruBlockCache(maxSize,blockSize);
    EvictionThread evictionThread = cache.getEvictionThread();
    assertTrue(evictionThread != null);

    CachedItem[] blocks = generateFixedBlocks(numBlocks + 1, blockSize, "block");

    // Make sure eviction thread has entered run method
    while (!evictionThread.isEnteringRun()) {
      Thread.sleep(1);
    }

    // Add all the blocks
    for (CachedItem block : blocks) {
      cache.cacheBlock(block.cacheKey, block);
    }

    // wait until at least one eviction has run
    int n = 0;
    while(cache.getStats().getEvictionCount() == 0) {
      Thread.sleep(200);
      assertTrue("Eviction never happened.", n++ < 20);
    }

    // let cache stabilize
    // On some systems, the cache will run multiple evictions before it attains
    // steady-state. For instance, after populating the cache with 10 blocks,
    // the first eviction evicts a single block and then a second eviction
    // evicts another. I think this is due to the delta between minSize and
    // acceptableSize, combined with variance between object overhead on
    // different environments.
    n = 0;
    for (long prevCnt = 0 /* < number of blocks added */,
              curCnt = cache.getBlockCount();
        prevCnt != curCnt; prevCnt = curCnt, curCnt = cache.getBlockCount()) {
      Thread.sleep(200);
      assertTrue("Cache never stabilized.", n++ < 20);
    }

    long evictionCount = cache.getStats().getEvictionCount();
    assertTrue(evictionCount >= 1);
    System.out.println("Background Evictions run: " + evictionCount);
  }

  @Test
  public void testCacheSimple() throws Exception {

    long maxSize = 1000000;
    long blockSize = calculateBlockSizeDefault(maxSize, 101);

    LruBlockCache cache = new LruBlockCache(maxSize, blockSize);

    CachedItem [] blocks = generateRandomBlocks(100, blockSize);

    long expectedCacheSize = cache.heapSize();

    // Confirm empty
    for (CachedItem block : blocks) {
      assertTrue(cache.getBlock(block.cacheKey, true, false, true) == null);
    }

    // Add blocks
    for (CachedItem block : blocks) {
      cache.cacheBlock(block.cacheKey, block);
      expectedCacheSize += block.cacheBlockHeapSize();
    }

    // Verify correctly calculated cache heap size
    assertEquals(expectedCacheSize, cache.heapSize());

    // Check if all blocks are properly cached and retrieved
    for (CachedItem block : blocks) {
      HeapSize buf = cache.getBlock(block.cacheKey, true, false, true);
      assertTrue(buf != null);
      assertEquals(buf.heapSize(), block.heapSize());
    }

    // Re-add same blocks and ensure nothing has changed
    long expectedBlockCount = cache.getBlockCount();
    for (CachedItem block : blocks) {
      cache.cacheBlock(block.cacheKey, block);
    }
    assertEquals(
            "Cache should ignore cache requests for blocks already in cache",
            expectedBlockCount, cache.getBlockCount());
    
    // Verify correctly calculated cache heap size
    assertEquals(expectedCacheSize, cache.heapSize());

    // Check if all blocks are properly cached and retrieved
    for (CachedItem block : blocks) {
      HeapSize buf = cache.getBlock(block.cacheKey, true, false, true);
      assertTrue(buf != null);
      assertEquals(buf.heapSize(), block.heapSize());
    }

    // Expect no evictions
    assertEquals(0, cache.getStats().getEvictionCount());
    Thread t = new LruBlockCache.StatisticsThread(cache);
    t.start();
    t.join();
  }

  @Test
  public void testCacheEvictionSimple() throws Exception {

    long maxSize = 100000;
    long blockSize = calculateBlockSizeDefault(maxSize, 10);

    LruBlockCache cache = new LruBlockCache(maxSize,blockSize,false);

    CachedItem [] blocks = generateFixedBlocks(10, blockSize, "block");

    long expectedCacheSize = cache.heapSize();

    // Add all the blocks
    for (CachedItem block : blocks) {
      cache.cacheBlock(block.cacheKey, block);
      expectedCacheSize += block.cacheBlockHeapSize();
    }

    // A single eviction run should have occurred
    assertEquals(1, cache.getStats().getEvictionCount());

    // Our expected size overruns acceptable limit
    assertTrue(expectedCacheSize >
      (maxSize * LruBlockCache.DEFAULT_ACCEPTABLE_FACTOR));

    // But the cache did not grow beyond max
    assertTrue(cache.heapSize() < maxSize);

    // And is still below the acceptable limit
    assertTrue(cache.heapSize() <
        (maxSize * LruBlockCache.DEFAULT_ACCEPTABLE_FACTOR));

    // All blocks except block 0  should be in the cache
    assertTrue(cache.getBlock(blocks[0].cacheKey, true, false, true) == null);
    for(int i=1;i<blocks.length;i++) {
      assertEquals(cache.getBlock(blocks[i].cacheKey, true, false, true),
          blocks[i]);
    }
  }

  @Test
  public void testCacheEvictionTwoPriorities() throws Exception {

    long maxSize = 100000;
    long blockSize = calculateBlockSizeDefault(maxSize, 10);

    LruBlockCache cache = new LruBlockCache(maxSize,blockSize,false);

    CachedItem [] singleBlocks = generateFixedBlocks(5, 10000, "single");
    CachedItem [] multiBlocks = generateFixedBlocks(5, 10000, "multi");

    long expectedCacheSize = cache.heapSize();

    // Add and get the multi blocks
    for (CachedItem block : multiBlocks) {
      cache.cacheBlock(block.cacheKey, block);
      expectedCacheSize += block.cacheBlockHeapSize();
      assertEquals(cache.getBlock(block.cacheKey, true, false, true), block);
    }

    // Add the single blocks (no get)
    for (CachedItem block : singleBlocks) {
      cache.cacheBlock(block.cacheKey, block);
      expectedCacheSize += block.heapSize();
    }

    // A single eviction run should have occurred
    assertEquals(cache.getStats().getEvictionCount(), 1);

    // We expect two entries evicted
    assertEquals(cache.getStats().getEvictedCount(), 2);

    // Our expected size overruns acceptable limit
    assertTrue(expectedCacheSize >
      (maxSize * LruBlockCache.DEFAULT_ACCEPTABLE_FACTOR));

    // But the cache did not grow beyond max
    assertTrue(cache.heapSize() <= maxSize);

    // And is now below the acceptable limit
    assertTrue(cache.heapSize() <=
        (maxSize * LruBlockCache.DEFAULT_ACCEPTABLE_FACTOR));

    // We expect fairness across the two priorities.
    // This test makes multi go barely over its limit, in-memory
    // empty, and the rest in single.  Two single evictions and
    // one multi eviction expected.
    assertTrue(cache.getBlock(singleBlocks[0].cacheKey, true, false, true) == null);
    assertTrue(cache.getBlock(multiBlocks[0].cacheKey, true, false, true) == null);

    // And all others to be cached
    for(int i=1;i<4;i++) {
      assertEquals(cache.getBlock(singleBlocks[i].cacheKey, true, false, true),
          singleBlocks[i]);
      assertEquals(cache.getBlock(multiBlocks[i].cacheKey, true, false, true),
          multiBlocks[i]);
    }
  }

  @Test
  public void testCacheEvictionThreePriorities() throws Exception {

    long maxSize = 100000;
    long blockSize = calculateBlockSize(maxSize, 10);

    LruBlockCache cache = new LruBlockCache(maxSize, blockSize, false,
        (int)Math.ceil(1.2*maxSize/blockSize),
        LruBlockCache.DEFAULT_LOAD_FACTOR,
        LruBlockCache.DEFAULT_CONCURRENCY_LEVEL,
        0.98f, // min
        0.99f, // acceptable
        0.33f, // single
        0.33f, // multi
        0.34f, // memory
        false);

    CachedItem [] singleBlocks = generateFixedBlocks(5, blockSize, "single");
    CachedItem [] multiBlocks = generateFixedBlocks(5, blockSize, "multi");
    CachedItem [] memoryBlocks = generateFixedBlocks(5, blockSize, "memory");

    long expectedCacheSize = cache.heapSize();

    // Add 3 blocks from each priority
    for(int i=0;i<3;i++) {

      // Just add single blocks
      cache.cacheBlock(singleBlocks[i].cacheKey, singleBlocks[i]);
      expectedCacheSize += singleBlocks[i].cacheBlockHeapSize();

      // Add and get multi blocks
      cache.cacheBlock(multiBlocks[i].cacheKey, multiBlocks[i]);
      expectedCacheSize += multiBlocks[i].cacheBlockHeapSize();
      cache.getBlock(multiBlocks[i].cacheKey, true, false, true);

      // Add memory blocks as such
      cache.cacheBlock(memoryBlocks[i].cacheKey, memoryBlocks[i], true, false);
      expectedCacheSize += memoryBlocks[i].cacheBlockHeapSize();

    }

    // Do not expect any evictions yet
    assertEquals(0, cache.getStats().getEvictionCount());

    // Verify cache size
    assertEquals(expectedCacheSize, cache.heapSize());

    // Insert a single block, oldest single should be evicted
    cache.cacheBlock(singleBlocks[3].cacheKey, singleBlocks[3]);

    // Single eviction, one thing evicted
    assertEquals(1, cache.getStats().getEvictionCount());
    assertEquals(1, cache.getStats().getEvictedCount());

    // Verify oldest single block is the one evicted
    assertEquals(null, cache.getBlock(singleBlocks[0].cacheKey, true, false, true));

    // Change the oldest remaining single block to a multi
    cache.getBlock(singleBlocks[1].cacheKey, true, false, true);

    // Insert another single block
    cache.cacheBlock(singleBlocks[4].cacheKey, singleBlocks[4]);

    // Two evictions, two evicted.
    assertEquals(2, cache.getStats().getEvictionCount());
    assertEquals(2, cache.getStats().getEvictedCount());

    // Oldest multi block should be evicted now
    assertEquals(null, cache.getBlock(multiBlocks[0].cacheKey, true, false, true));

    // Insert another memory block
    cache.cacheBlock(memoryBlocks[3].cacheKey, memoryBlocks[3], true, false);

    // Three evictions, three evicted.
    assertEquals(3, cache.getStats().getEvictionCount());
    assertEquals(3, cache.getStats().getEvictedCount());

    // Oldest memory block should be evicted now
    assertEquals(null, cache.getBlock(memoryBlocks[0].cacheKey, true, false, true));

    // Add a block that is twice as big (should force two evictions)
    CachedItem [] bigBlocks = generateFixedBlocks(3, blockSize*3, "big");
    cache.cacheBlock(bigBlocks[0].cacheKey, bigBlocks[0]);

    // Four evictions, six evicted (inserted block 3X size, expect +3 evicted)
    assertEquals(4, cache.getStats().getEvictionCount());
    assertEquals(6, cache.getStats().getEvictedCount());

    // Expect three remaining singles to be evicted
    assertEquals(null, cache.getBlock(singleBlocks[2].cacheKey, true, false, true));
    assertEquals(null, cache.getBlock(singleBlocks[3].cacheKey, true, false, true));
    assertEquals(null, cache.getBlock(singleBlocks[4].cacheKey, true, false, true));

    // Make the big block a multi block
    cache.getBlock(bigBlocks[0].cacheKey, true, false, true);

    // Cache another single big block
    cache.cacheBlock(bigBlocks[1].cacheKey, bigBlocks[1]);

    // Five evictions, nine evicted (3 new)
    assertEquals(5, cache.getStats().getEvictionCount());
    assertEquals(9, cache.getStats().getEvictedCount());

    // Expect three remaining multis to be evicted
    assertEquals(null, cache.getBlock(singleBlocks[1].cacheKey, true, false, true));
    assertEquals(null, cache.getBlock(multiBlocks[1].cacheKey, true, false, true));
    assertEquals(null, cache.getBlock(multiBlocks[2].cacheKey, true, false, true));

    // Cache a big memory block
    cache.cacheBlock(bigBlocks[2].cacheKey, bigBlocks[2], true, false);

    // Six evictions, twelve evicted (3 new)
    assertEquals(6, cache.getStats().getEvictionCount());
    assertEquals(12, cache.getStats().getEvictedCount());

    // Expect three remaining in-memory to be evicted
    assertEquals(null, cache.getBlock(memoryBlocks[1].cacheKey, true, false, true));
    assertEquals(null, cache.getBlock(memoryBlocks[2].cacheKey, true, false, true));
    assertEquals(null, cache.getBlock(memoryBlocks[3].cacheKey, true, false, true));
  }

  @Test
  public void testCacheEvictionInMemoryForceMode() throws Exception {
    long maxSize = 100000;
    long blockSize = calculateBlockSize(maxSize, 10);

    LruBlockCache cache = new LruBlockCache(maxSize, blockSize, false,
        (int)Math.ceil(1.2*maxSize/blockSize),
        LruBlockCache.DEFAULT_LOAD_FACTOR,
        LruBlockCache.DEFAULT_CONCURRENCY_LEVEL,
        0.98f, // min
        0.99f, // acceptable
        0.2f, // single
        0.3f, // multi
        0.5f, // memory
        true);

    CachedItem [] singleBlocks = generateFixedBlocks(10, blockSize, "single");
    CachedItem [] multiBlocks = generateFixedBlocks(10, blockSize, "multi");
    CachedItem [] memoryBlocks = generateFixedBlocks(10, blockSize, "memory");

    long expectedCacheSize = cache.heapSize();

    // 0. Add 5 single blocks and 4 multi blocks to make cache full, si:mu:me = 5:4:0
    for(int i = 0; i < 4; i++) {
      // Just add single blocks
      cache.cacheBlock(singleBlocks[i].cacheKey, singleBlocks[i]);
      expectedCacheSize += singleBlocks[i].cacheBlockHeapSize();
      // Add and get multi blocks
      cache.cacheBlock(multiBlocks[i].cacheKey, multiBlocks[i]);
      expectedCacheSize += multiBlocks[i].cacheBlockHeapSize();
      cache.getBlock(multiBlocks[i].cacheKey, true, false, true);
    }
    // 5th single block
    cache.cacheBlock(singleBlocks[4].cacheKey, singleBlocks[4]);
    expectedCacheSize += singleBlocks[4].cacheBlockHeapSize();
    // Do not expect any evictions yet
    assertEquals(0, cache.getStats().getEvictionCount());
    // Verify cache size
    assertEquals(expectedCacheSize, cache.heapSize());

    // 1. Insert a memory block, oldest single should be evicted, si:mu:me = 4:4:1
    cache.cacheBlock(memoryBlocks[0].cacheKey, memoryBlocks[0], true, false);
    // Single eviction, one block evicted
    assertEquals(1, cache.getStats().getEvictionCount());
    assertEquals(1, cache.getStats().getEvictedCount());
    // Verify oldest single block (index = 0) is the one evicted
    assertEquals(null, cache.getBlock(singleBlocks[0].cacheKey, true, false, true));

    // 2. Insert another memory block, another single evicted, si:mu:me = 3:4:2
    cache.cacheBlock(memoryBlocks[1].cacheKey, memoryBlocks[1], true, false);
    // Two evictions, two evicted.
    assertEquals(2, cache.getStats().getEvictionCount());
    assertEquals(2, cache.getStats().getEvictedCount());
    // Current oldest single block (index = 1) should be evicted now
    assertEquals(null, cache.getBlock(singleBlocks[1].cacheKey, true, false, true));

    // 3. Insert 4 memory blocks, 2 single and 2 multi evicted, si:mu:me = 1:2:6
    cache.cacheBlock(memoryBlocks[2].cacheKey, memoryBlocks[2], true, false);
    cache.cacheBlock(memoryBlocks[3].cacheKey, memoryBlocks[3], true, false);
    cache.cacheBlock(memoryBlocks[4].cacheKey, memoryBlocks[4], true, false);
    cache.cacheBlock(memoryBlocks[5].cacheKey, memoryBlocks[5], true, false);
    // Three evictions, three evicted.
    assertEquals(6, cache.getStats().getEvictionCount());
    assertEquals(6, cache.getStats().getEvictedCount());
    // two oldest single blocks and two oldest multi blocks evicted
    assertEquals(null, cache.getBlock(singleBlocks[2].cacheKey, true, false, true));
    assertEquals(null, cache.getBlock(singleBlocks[3].cacheKey, true, false, true));
    assertEquals(null, cache.getBlock(multiBlocks[0].cacheKey, true, false, true));
    assertEquals(null, cache.getBlock(multiBlocks[1].cacheKey, true, false, true));

    // 4. Insert 3 memory blocks, the remaining 1 single and 2 multi evicted
    // si:mu:me = 0:0:9
    cache.cacheBlock(memoryBlocks[6].cacheKey, memoryBlocks[6], true, false);
    cache.cacheBlock(memoryBlocks[7].cacheKey, memoryBlocks[7], true, false);
    cache.cacheBlock(memoryBlocks[8].cacheKey, memoryBlocks[8], true, false);
    // Three evictions, three evicted.
    assertEquals(9, cache.getStats().getEvictionCount());
    assertEquals(9, cache.getStats().getEvictedCount());
    // one oldest single block and two oldest multi blocks evicted
    assertEquals(null, cache.getBlock(singleBlocks[4].cacheKey, true, false, true));
    assertEquals(null, cache.getBlock(multiBlocks[2].cacheKey, true, false, true));
    assertEquals(null, cache.getBlock(multiBlocks[3].cacheKey, true, false, true));

    // 5. Insert one memory block, the oldest memory evicted
    // si:mu:me = 0:0:9
    cache.cacheBlock(memoryBlocks[9].cacheKey, memoryBlocks[9], true, false);
    // one eviction, one evicted.
    assertEquals(10, cache.getStats().getEvictionCount());
    assertEquals(10, cache.getStats().getEvictedCount());
    // oldest memory block evicted
    assertEquals(null, cache.getBlock(memoryBlocks[0].cacheKey, true, false, true));

    // 6. Insert one new single block, itself evicted immediately since
    //    all blocks in cache are memory-type which have higher priority
    // si:mu:me = 0:0:9 (no change)
    cache.cacheBlock(singleBlocks[9].cacheKey, singleBlocks[9]);
    // one eviction, one evicted.
    assertEquals(11, cache.getStats().getEvictionCount());
    assertEquals(11, cache.getStats().getEvictedCount());
    // the single block just cached now evicted (can't evict memory)
    assertEquals(null, cache.getBlock(singleBlocks[9].cacheKey, true, false, true));
  }

  // test scan resistance
  @Test
  public void testScanResistance() throws Exception {

    long maxSize = 100000;
    long blockSize = calculateBlockSize(maxSize, 10);

    LruBlockCache cache = new LruBlockCache(maxSize, blockSize, false,
        (int)Math.ceil(1.2*maxSize/blockSize),
        LruBlockCache.DEFAULT_LOAD_FACTOR,
        LruBlockCache.DEFAULT_CONCURRENCY_LEVEL,
        0.66f, // min
        0.99f, // acceptable
        0.33f, // single
        0.33f, // multi
        0.34f, // memory
        false);

    CachedItem [] singleBlocks = generateFixedBlocks(20, blockSize, "single");
    CachedItem [] multiBlocks = generateFixedBlocks(5, blockSize, "multi");

    // Add 5 multi blocks
    for (CachedItem block : multiBlocks) {
      cache.cacheBlock(block.cacheKey, block);
      cache.getBlock(block.cacheKey, true, false, true);
    }

    // Add 5 single blocks
    for(int i=0;i<5;i++) {
      cache.cacheBlock(singleBlocks[i].cacheKey, singleBlocks[i]);
    }

    // An eviction ran
    assertEquals(1, cache.getStats().getEvictionCount());

    // To drop down to 2/3 capacity, we'll need to evict 4 blocks
    assertEquals(4, cache.getStats().getEvictedCount());

    // Should have been taken off equally from single and multi
    assertEquals(null, cache.getBlock(singleBlocks[0].cacheKey, true, false, true));
    assertEquals(null, cache.getBlock(singleBlocks[1].cacheKey, true, false, true));
    assertEquals(null, cache.getBlock(multiBlocks[0].cacheKey, true, false, true));
    assertEquals(null, cache.getBlock(multiBlocks[1].cacheKey, true, false, true));

    // Let's keep "scanning" by adding single blocks.  From here on we only
    // expect evictions from the single bucket.

    // Every time we reach 10 total blocks (every 4 inserts) we get 4 single
    // blocks evicted.  Inserting 13 blocks should yield 3 more evictions and
    // 12 more evicted.

    for(int i=5;i<18;i++) {
      cache.cacheBlock(singleBlocks[i].cacheKey, singleBlocks[i]);
    }

    // 4 total evictions, 16 total evicted
    assertEquals(4, cache.getStats().getEvictionCount());
    assertEquals(16, cache.getStats().getEvictedCount());

    // Should now have 7 total blocks
    assertEquals(7, cache.getBlockCount());

  }

  // test setMaxSize
  @Test
  public void testResizeBlockCache() throws Exception {

    long maxSize = 300000;
    long blockSize = calculateBlockSize(maxSize, 31);

    LruBlockCache cache = new LruBlockCache(maxSize, blockSize, false,
        (int)Math.ceil(1.2*maxSize/blockSize),
        LruBlockCache.DEFAULT_LOAD_FACTOR,
        LruBlockCache.DEFAULT_CONCURRENCY_LEVEL,
        0.98f, // min
        0.99f, // acceptable
        0.33f, // single
        0.33f, // multi
        0.34f, // memory
        false);

    CachedItem [] singleBlocks = generateFixedBlocks(10, blockSize, "single");
    CachedItem [] multiBlocks = generateFixedBlocks(10, blockSize, "multi");
    CachedItem [] memoryBlocks = generateFixedBlocks(10, blockSize, "memory");

    // Add all blocks from all priorities
    for(int i=0;i<10;i++) {

      // Just add single blocks
      cache.cacheBlock(singleBlocks[i].cacheKey, singleBlocks[i]);

      // Add and get multi blocks
      cache.cacheBlock(multiBlocks[i].cacheKey, multiBlocks[i]);
      cache.getBlock(multiBlocks[i].cacheKey, true, false, true);

      // Add memory blocks as such
      cache.cacheBlock(memoryBlocks[i].cacheKey, memoryBlocks[i], true, false);
    }

    // Do not expect any evictions yet
    assertEquals(0, cache.getStats().getEvictionCount());

    // Resize to half capacity plus an extra block (otherwise we evict an extra)
    cache.setMaxSize((long)(maxSize * 0.5f));

    // Should have run a single eviction
    assertEquals(1, cache.getStats().getEvictionCount());

    // And we expect 1/2 of the blocks to be evicted
    assertEquals(15, cache.getStats().getEvictedCount());

    // And the oldest 5 blocks from each category should be gone
    for(int i=0;i<5;i++) {
      assertEquals(null, cache.getBlock(singleBlocks[i].cacheKey, true, false, true));
      assertEquals(null, cache.getBlock(multiBlocks[i].cacheKey, true, false, true));
      assertEquals(null, cache.getBlock(memoryBlocks[i].cacheKey, true, false, true));
    }

    // And the newest 5 blocks should still be accessible
    for(int i=5;i<10;i++) {
      assertEquals(singleBlocks[i], cache.getBlock(singleBlocks[i].cacheKey, true, false, true));
      assertEquals(multiBlocks[i], cache.getBlock(multiBlocks[i].cacheKey, true, false, true));
      assertEquals(memoryBlocks[i], cache.getBlock(memoryBlocks[i].cacheKey, true, false, true));
    }
  }

  // test metricsPastNPeriods
  @Test
  public void testPastNPeriodsMetrics() throws Exception {
   double delta = 0.01;

    // 3 total periods
    CacheStats stats = new CacheStats("test", 3);

    // No accesses, should be 0
    stats.rollMetricsPeriod();
    assertEquals(0.0, stats.getHitRatioPastNPeriods(), delta);
    assertEquals(0.0, stats.getHitCachingRatioPastNPeriods(), delta);

    // period 1, 1 hit caching, 1 hit non-caching, 2 miss non-caching
    // should be (2/4)=0.5 and (1/1)=1
    stats.hit(false);
    stats.hit(true);
    stats.miss(false);
    stats.miss(false);
    stats.rollMetricsPeriod();
    assertEquals(0.5, stats.getHitRatioPastNPeriods(), delta);
    assertEquals(1.0, stats.getHitCachingRatioPastNPeriods(), delta);

    // period 2, 1 miss caching, 3 miss non-caching
    // should be (2/8)=0.25 and (1/2)=0.5
    stats.miss(true);
    stats.miss(false);
    stats.miss(false);
    stats.miss(false);
    stats.rollMetricsPeriod();
    assertEquals(0.25, stats.getHitRatioPastNPeriods(), delta);
    assertEquals(0.5, stats.getHitCachingRatioPastNPeriods(), delta);

    // period 3, 2 hits of each type
    // should be (6/12)=0.5 and (3/4)=0.75
    stats.hit(false);
    stats.hit(true);
    stats.hit(false);
    stats.hit(true);
    stats.rollMetricsPeriod();
    assertEquals(0.5, stats.getHitRatioPastNPeriods(), delta);
    assertEquals(0.75, stats.getHitCachingRatioPastNPeriods(), delta);

    // period 4, evict period 1, two caching misses
    // should be (4/10)=0.4 and (2/5)=0.4
    stats.miss(true);
    stats.miss(true);
    stats.rollMetricsPeriod();
    assertEquals(0.4, stats.getHitRatioPastNPeriods(), delta);
    assertEquals(0.4, stats.getHitCachingRatioPastNPeriods(), delta);

    // period 5, evict period 2, 2 caching misses, 2 non-caching hit
    // should be (6/10)=0.6 and (2/6)=1/3
    stats.miss(true);
    stats.miss(true);
    stats.hit(false);
    stats.hit(false);
    stats.rollMetricsPeriod();
    assertEquals(0.6, stats.getHitRatioPastNPeriods(), delta);
    assertEquals((double)1/3, stats.getHitCachingRatioPastNPeriods(), delta);

    // period 6, evict period 3
    // should be (2/6)=1/3 and (0/4)=0
    stats.rollMetricsPeriod();
    assertEquals((double)1/3, stats.getHitRatioPastNPeriods(), delta);
    assertEquals(0.0, stats.getHitCachingRatioPastNPeriods(), delta);

    // period 7, evict period 4
    // should be (2/4)=0.5 and (0/2)=0
    stats.rollMetricsPeriod();
    assertEquals(0.5, stats.getHitRatioPastNPeriods(), delta);
    assertEquals(0.0, stats.getHitCachingRatioPastNPeriods(), delta);

    // period 8, evict period 5
    // should be 0 and 0
    stats.rollMetricsPeriod();
    assertEquals(0.0, stats.getHitRatioPastNPeriods(), delta);
    assertEquals(0.0, stats.getHitCachingRatioPastNPeriods(), delta);

    // period 9, one of each
    // should be (2/4)=0.5 and (1/2)=0.5
    stats.miss(true);
    stats.miss(false);
    stats.hit(true);
    stats.hit(false);
    stats.rollMetricsPeriod();
    assertEquals(0.5, stats.getHitRatioPastNPeriods(), delta);
    assertEquals(0.5, stats.getHitCachingRatioPastNPeriods(), delta);
  }

  private CachedItem [] generateFixedBlocks(int numBlocks, int size, String pfx) {
    CachedItem [] blocks = new CachedItem[numBlocks];
    for(int i=0;i<numBlocks;i++) {
      blocks[i] = new CachedItem(pfx + i, size);
    }
    return blocks;
  }

  private CachedItem [] generateFixedBlocks(int numBlocks, long size, String pfx) {
    return generateFixedBlocks(numBlocks, (int)size, pfx);
  }

  private CachedItem [] generateRandomBlocks(int numBlocks, long maxSize) {
    CachedItem [] blocks = new CachedItem[numBlocks];
    Random r = new Random();
    for(int i=0;i<numBlocks;i++) {
      blocks[i] = new CachedItem("block" + i, r.nextInt((int)maxSize)+1);
    }
    return blocks;
  }

  private long calculateBlockSize(long maxSize, int numBlocks) {
    long roughBlockSize = maxSize / numBlocks;
    int numEntries = (int)Math.ceil((1.2)*maxSize/roughBlockSize);
    long totalOverhead = LruBlockCache.CACHE_FIXED_OVERHEAD +
        ClassSize.CONCURRENT_HASHMAP +
        (numEntries * ClassSize.CONCURRENT_HASHMAP_ENTRY) +
        (LruBlockCache.DEFAULT_CONCURRENCY_LEVEL * ClassSize.CONCURRENT_HASHMAP_SEGMENT);
    long negateBlockSize = (long)(totalOverhead/numEntries);
    negateBlockSize += LruCachedBlock.PER_BLOCK_OVERHEAD;
    return ClassSize.align((long)Math.floor((roughBlockSize - negateBlockSize)*0.99f));
  }

  private long calculateBlockSizeDefault(long maxSize, int numBlocks) {
    long roughBlockSize = maxSize / numBlocks;
    int numEntries = (int)Math.ceil((1.2)*maxSize/roughBlockSize);
    long totalOverhead = LruBlockCache.CACHE_FIXED_OVERHEAD +
        ClassSize.CONCURRENT_HASHMAP +
        (numEntries * ClassSize.CONCURRENT_HASHMAP_ENTRY) +
        (LruBlockCache.DEFAULT_CONCURRENCY_LEVEL * ClassSize.CONCURRENT_HASHMAP_SEGMENT);
    long negateBlockSize = totalOverhead / numEntries;
    negateBlockSize += LruCachedBlock.PER_BLOCK_OVERHEAD;
    return ClassSize.align((long)Math.floor((roughBlockSize - negateBlockSize)*
        LruBlockCache.DEFAULT_ACCEPTABLE_FACTOR));
  }

  private static class CachedItem implements Cacheable {
    BlockCacheKey cacheKey;
    int size;

    CachedItem(String blockName, int size) {
      this.cacheKey = new BlockCacheKey(blockName, 0);
      this.size = size;
    }

    /** The size of this item reported to the block cache layer */
    @Override
    public long heapSize() {
      return ClassSize.align(size);
    }

    /** Size of the cache block holding this item. Used for verification. */
    public long cacheBlockHeapSize() {
      return LruCachedBlock.PER_BLOCK_OVERHEAD
          + ClassSize.align(cacheKey.heapSize())
          + ClassSize.align(size);
    }

    @Override
    public int getSerializedLength() {
      return 0;
    }

    @Override
    public CacheableDeserializer<Cacheable> getDeserializer() {
      return null;
    }

    @Override
    public void serialize(ByteBuffer destination) {
    }
    
    @Override
    public BlockType getBlockType() {
      return BlockType.DATA;
    }

    @Override
    public MemoryType getMemoryType() {
      return MemoryType.EXCLUSIVE;
    }

  }

}

