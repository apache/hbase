/**
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
package org.apache.hadoop.hbase.io.hfile.bucket;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.CacheTestUtils;
import org.apache.hadoop.hbase.io.hfile.CacheTestUtils.HFileBlockPair;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketAllocator.BucketSizeInfo;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketAllocator.IndexStatistics;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.IdLock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Basic test of BucketCache.Puts and gets.
 * <p>
 * Tests will ensure that blocks' data correctness under several threads concurrency
 */
@RunWith(Parameterized.class)
@Category(SmallTests.class)
public class TestBucketCache {

  private static final Random RAND = new Random();

  @Parameterized.Parameters(name = "{index}: blockSize={0}, bucketSizes={1}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][] {
        { 8192, null }, // TODO: why is 8k the default blocksize for these tests?
        {
            16 * 1024,
            new int[] { 2 * 1024 + 1024, 4 * 1024 + 1024, 8 * 1024 + 1024, 16 * 1024 + 1024,
                28 * 1024 + 1024, 32 * 1024 + 1024, 64 * 1024 + 1024, 96 * 1024 + 1024,
                128 * 1024 + 1024 } } });
  }

  @Parameterized.Parameter(0)
  public int constructedBlockSize;

  @Parameterized.Parameter(1)
  public int[] constructedBlockSizes;

  BucketCache cache;
  final int CACHE_SIZE = 1000000;
  final int NUM_BLOCKS = 100;
  final int BLOCK_SIZE = CACHE_SIZE / NUM_BLOCKS;
  final int NUM_THREADS = 100;
  final int NUM_QUERIES = 10000;

  final long capacitySize = 32 * 1024 * 1024;
  final int writeThreads = BucketCache.DEFAULT_WRITER_THREADS;
  final int writerQLen = BucketCache.DEFAULT_WRITER_QUEUE_ITEMS;
  String ioEngineName = "heap";
  String persistencePath = null;

  private class MockedBucketCache extends BucketCache {

    public MockedBucketCache(String ioEngineName, long capacity, int blockSize, int[] bucketSizes,
        int writerThreads, int writerQLen, String persistencePath) throws FileNotFoundException,
        IOException {
      super(ioEngineName, capacity, blockSize, bucketSizes, writerThreads, writerQLen,
          persistencePath);
      super.wait_when_cache = true;
    }

    @Override
    public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf, boolean inMemory,
        boolean cacheDataInL1) {
      if (super.getBlock(cacheKey, true, false, true) != null) {
        throw new RuntimeException("Cached an already cached block");
      }
      super.cacheBlock(cacheKey, buf, inMemory, cacheDataInL1);
    }

    @Override
    public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf) {
      if (super.getBlock(cacheKey, true, false, true) != null) {
        throw new RuntimeException("Cached an already cached block");
      }
      super.cacheBlock(cacheKey, buf);
    }
  }

  @Before
  public void setup() throws FileNotFoundException, IOException {
    cache =
        new MockedBucketCache(ioEngineName, capacitySize, constructedBlockSize,
            constructedBlockSizes, writeThreads, writerQLen, persistencePath);
  }

  @After
  public void tearDown() {
    cache.shutdown();
  }

  /**
   * Return a random element from {@code a}.
   */
  private static <T> T randFrom(List<T> a) {
    return a.get(RAND.nextInt(a.size()));
  }

  @Test
  public void testBucketAllocator() throws BucketAllocatorException {
    BucketAllocator mAllocator = cache.getAllocator();
    /*
     * Test the allocator first
     */
    final List<Integer> BLOCKSIZES = Arrays.asList(4 * 1024, 8 * 1024, 64 * 1024, 96 * 1024);

    boolean full = false;
    ArrayList<Long> allocations = new ArrayList<Long>();
    // Fill the allocated extents by choosing a random blocksize. Continues selecting blocks until
    // the cache is completely filled.
    List<Integer> tmp = new ArrayList<Integer>(BLOCKSIZES);
    while (!full) {
      Integer blockSize = null;
      try {
        blockSize = randFrom(tmp);
        allocations.add(mAllocator.allocateBlock(blockSize));
      } catch (CacheFullException cfe) {
        tmp.remove(blockSize);
        if (tmp.isEmpty()) full = true;
      }
    }

    for (Integer blockSize : BLOCKSIZES) {
      BucketSizeInfo bucketSizeInfo = mAllocator.roundUpToBucketSizeInfo(blockSize);
      IndexStatistics indexStatistics = bucketSizeInfo.statistics();
      assertEquals("unexpected freeCount for " + bucketSizeInfo, 0, indexStatistics.freeCount());
    }

    for (long offset : allocations) {
      assertEquals(mAllocator.sizeOfAllocation(offset), mAllocator.freeBlock(offset));
    }
    assertEquals(0, mAllocator.getUsedSize());
  }

  @Test
  public void testCacheSimple() throws Exception {
    CacheTestUtils.testCacheSimple(cache, BLOCK_SIZE, NUM_QUERIES);
  }

  @Test
  public void testCacheMultiThreadedSingleKey() throws Exception {
    CacheTestUtils.hammerSingleKey(cache, BLOCK_SIZE, NUM_THREADS, NUM_QUERIES);
  }

  @Test
  public void testHeapSizeChanges() throws Exception {
    cache.stopWriterThreads();
    CacheTestUtils.testHeapSizeChanges(cache, BLOCK_SIZE);
  }

  // BucketCache.cacheBlock is async, it first adds block to ramCache and writeQueue, then writer
  // threads will flush it to the bucket and put reference entry in backingMap.
  private void cacheAndWaitUntilFlushedToBucket(BucketCache cache, BlockCacheKey cacheKey,
      Cacheable block) throws InterruptedException {
    cache.cacheBlock(cacheKey, block);
    while (!cache.backingMap.containsKey(cacheKey)) {
      Thread.sleep(100);
    }
  }

  @Test
  public void testMemoryLeak() throws Exception {
    final BlockCacheKey cacheKey = new BlockCacheKey("dummy", 1L);
    cacheAndWaitUntilFlushedToBucket(cache, cacheKey, new CacheTestUtils.ByteArrayCacheable(
        new byte[10]));
    long lockId = cache.backingMap.get(cacheKey).offset();
    IdLock.Entry lockEntry = cache.offsetLock.getLockEntry(lockId);
    Thread evictThread = new Thread("evict-block") {

      @Override
      public void run() {
        cache.evictBlock(cacheKey);
      }

    };
    evictThread.start();
    cache.offsetLock.waitForWaiters(lockId, 1);
    cache.blockEvicted(cacheKey, cache.backingMap.remove(cacheKey), true);
    cacheAndWaitUntilFlushedToBucket(cache, cacheKey, new CacheTestUtils.ByteArrayCacheable(
        new byte[10]));
    cache.offsetLock.releaseLockEntry(lockEntry);
    evictThread.join();
    assertEquals(1L, cache.getBlockCount());
    assertTrue(cache.getCurrentSize() > 0L);
    assertTrue("We should have a block!", cache.iterator().hasNext());
  }

  @Test
  public void testRetrieveFromFile() throws Exception {
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    Path testDir = TEST_UTIL.getDataTestDir();
    TEST_UTIL.getTestFileSystem().mkdirs(testDir);

    BucketCache bucketCache = new BucketCache("file:" + testDir + "/bucket.cache", capacitySize,
        constructedBlockSize, constructedBlockSizes, writeThreads, writerQLen, testDir
            + "/bucket.persistence");
    long usedSize = bucketCache.getAllocator().getUsedSize();
    assertTrue(usedSize == 0);

    HFileBlockPair[] blocks = CacheTestUtils.generateHFileBlocks(constructedBlockSize, 1);
    // Add blocks
    for (HFileBlockPair block : blocks) {
      bucketCache.cacheBlock(block.getBlockName(), block.getBlock());
    }
    for (HFileBlockPair block : blocks) {
      cacheAndWaitUntilFlushedToBucket(bucketCache, block.getBlockName(), block.getBlock());
    }
    usedSize = bucketCache.getAllocator().getUsedSize();
    assertTrue(usedSize != 0);
    // persist cache to file
    bucketCache.shutdown();

    // restore cache from file
    bucketCache = new BucketCache("file:" + testDir + "/bucket.cache", capacitySize,
        constructedBlockSize, constructedBlockSizes, writeThreads, writerQLen, testDir
            + "/bucket.persistence");
    assertEquals(usedSize, bucketCache.getAllocator().getUsedSize());
    // persist cache to file
    bucketCache.shutdown();

    // reconfig buckets sizes, the biggest bucket is small than constructedBlockSize (8k or 16k)
    // so it can't restore cache from file
    int[] smallBucketSizes = new int[] { 2 * 1024 + 1024, 4 * 1024 + 1024 };
    bucketCache = new BucketCache("file:" + testDir + "/bucket.cache", capacitySize,
        constructedBlockSize, smallBucketSizes, writeThreads,
        writerQLen, testDir + "/bucket.persistence");
    assertEquals(0, bucketCache.getAllocator().getUsedSize());
    assertEquals(0, bucketCache.backingMap.size());

    TEST_UTIL.cleanupTestDir();
  }
}
