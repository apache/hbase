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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.ClassSize;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests the concurrent TinyLfuBlockCache.
 */
@Category({IOTests.class, SmallTests.class})
public class TestTinyLfuBlockCache {

  @Test
  public void testCacheSimple() throws Exception {

    long maxSize = 1000000;
    long blockSize = calculateBlockSizeDefault(maxSize, 101);

    TinyLfuBlockCache cache = new TinyLfuBlockCache(maxSize, blockSize, blockSize, Runnable::run);

    CachedItem [] blocks = generateRandomBlocks(100, blockSize);

    long expectedCacheSize = cache.heapSize();

    // Confirm empty
    for (CachedItem block : blocks) {
      assertTrue(cache.getBlock(block.cacheKey, true, false, true) == null);
    }

    // Add blocks
    for (CachedItem block : blocks) {
      cache.cacheBlock(block.cacheKey, block);
      expectedCacheSize += block.heapSize();
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
  }

  @Test
  public void testCacheEvictionSimple() throws Exception {

    long maxSize = 100000;
    long blockSize = calculateBlockSizeDefault(maxSize, 10);

    TinyLfuBlockCache cache = new TinyLfuBlockCache(maxSize, blockSize, blockSize, Runnable::run);

    CachedItem [] blocks = generateFixedBlocks(11, blockSize, "block");

    // Add all the blocks
    for (CachedItem block : blocks) {
      cache.cacheBlock(block.cacheKey, block);
    }

    // A single eviction run should have occurred
    assertEquals(1, cache.getStats().getEvictionCount());

    // The cache did not grow beyond max
    assertTrue(cache.heapSize() < maxSize);

    // All blocks except one should be in the cache
    assertEquals(10, cache.getBlockCount());
  }

  @Test
  public void testScanResistance() throws Exception {

    long maxSize = 100000;
    long blockSize = calculateBlockSize(maxSize, 10);

    TinyLfuBlockCache cache = new TinyLfuBlockCache(maxSize, blockSize, blockSize, Runnable::run);

    CachedItem [] singleBlocks = generateFixedBlocks(20, blockSize, "single");
    CachedItem [] multiBlocks = generateFixedBlocks(5, blockSize, "multi");

    // Add 5 blocks from each
    for(int i=0; i<5; i++) {
      cache.cacheBlock(singleBlocks[i].cacheKey, singleBlocks[i]);
      cache.cacheBlock(multiBlocks[i].cacheKey, multiBlocks[i]);
    }

    // Add frequency
    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 10; j++) {
        CachedItem block = multiBlocks[i];
        cache.getBlock(block.cacheKey, true, false, true);
      }
    }

    // Let's keep "scanning" by adding single blocks.  From here on we only
    // expect evictions from the single bucket.

    for(int i=5;i<18;i++) {
      cache.cacheBlock(singleBlocks[i].cacheKey, singleBlocks[i]);
    }

    for (CachedItem block : multiBlocks) {
      assertTrue(cache.cache.asMap().containsKey(block.cacheKey));
    }

    assertEquals(10, cache.getBlockCount());
    assertEquals(13, cache.getStats().getEvictionCount());

  }

  @Test
  public void testMaxBlockSize() throws Exception {
    long maxSize = 100000;
    long blockSize = calculateBlockSize(maxSize, 10);

    TinyLfuBlockCache cache = new TinyLfuBlockCache(maxSize, blockSize, blockSize, Runnable::run);
    CachedItem [] tooLong = generateFixedBlocks(10, 2 * blockSize, "long");
    CachedItem [] small = generateFixedBlocks(15, blockSize / 2, "small");

    for (CachedItem i:tooLong) {
      cache.cacheBlock(i.cacheKey, i);
    }
    for (CachedItem i:small) {
      cache.cacheBlock(i.cacheKey, i);
    }
    assertEquals(15,cache.getBlockCount());
    for (CachedItem i:small) {
      assertNotNull(cache.getBlock(i.cacheKey, true, false, false));
    }
    for (CachedItem i:tooLong) {
      assertNull(cache.getBlock(i.cacheKey, true, false, false));
    }

    assertEquals(10, cache.getStats().getFailedInserts());
  }

  @Test
  public void testResizeBlockCache() throws Exception {

    long maxSize = 100000;
    long blockSize = calculateBlockSize(maxSize, 10);

    TinyLfuBlockCache cache = new TinyLfuBlockCache(maxSize, blockSize, blockSize, Runnable::run);

    CachedItem [] blocks = generateFixedBlocks(10, blockSize, "block");

    for(CachedItem block : blocks) {
      cache.cacheBlock(block.cacheKey, block);
    }

    // Do not expect any evictions yet
    assertEquals(10, cache.getBlockCount());
    assertEquals(0, cache.getStats().getEvictionCount());

    // Resize to half capacity plus an extra block (otherwise we evict an extra)
    cache.setMaxSize(maxSize / 2);

    // And we expect 1/2 of the blocks to be evicted
    assertEquals(5, cache.getBlockCount());
    assertEquals(5, cache.getStats().getEvictedCount());
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
    long negateBlockSize = totalOverhead/numEntries;
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

