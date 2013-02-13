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

import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.CacheTestUtils;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketAllocator.BucketSizeInfo;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketAllocator.IndexStatistics;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Basic test of BucketCache.Puts and gets.
 * <p>
 * Tests will ensure that blocks' data correctness under several threads
 * concurrency
 */
@Category(SmallTests.class)
public class TestBucketCache {
  static final Log LOG = LogFactory.getLog(TestBucketCache.class);
  BucketCache cache;
  final int CACHE_SIZE = 1000000;
  final int NUM_BLOCKS = 100;
  final int BLOCK_SIZE = CACHE_SIZE / NUM_BLOCKS;
  final int NUM_THREADS = 1000;
  final int NUM_QUERIES = 10000;
  
  final long capacitySize = 32 * 1024 * 1024;
  final int writeThreads = BucketCache.DEFAULT_WRITER_THREADS;
  final int writerQLen = BucketCache.DEFAULT_WRITER_QUEUE_ITEMS;
  String ioEngineName = "heap";
  String persistencePath = null;

  private class MockedBucketCache extends BucketCache {

    public MockedBucketCache(String ioEngineName, long capacity,
        int writerThreads,
        int writerQLen, String persistencePath) throws FileNotFoundException,
        IOException {
      super(ioEngineName, capacity, writerThreads, writerQLen, persistencePath);
      super.wait_when_cache = true;
    }

    @Override
    public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf,
        boolean inMemory) {
      if (super.getBlock(cacheKey, true, false) != null) {
        throw new RuntimeException("Cached an already cached block");
      }
      super.cacheBlock(cacheKey, buf, inMemory);
    }

    @Override
    public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf) {
      if (super.getBlock(cacheKey, true, false) != null) {
        throw new RuntimeException("Cached an already cached block");
      }
      super.cacheBlock(cacheKey, buf);
    }
  }

  @Before
  public void setup() throws FileNotFoundException, IOException {
    cache = new MockedBucketCache(ioEngineName, capacitySize, writeThreads,
        writerQLen, persistencePath);
  }

  @After
  public void tearDown() {
    cache.shutdown();
  }

  @Test
  public void testBucketAllocator() throws BucketAllocatorException {
    BucketAllocator mAllocator = cache.getAllocator();
    /*
     * Test the allocator first
     */
    int[] blockSizes = new int[2];
    blockSizes[0] = 4 * 1024;
    blockSizes[1] = 8 * 1024;
    boolean full = false;
    int i = 0;
    ArrayList<Long> allocations = new ArrayList<Long>();
    // Fill the allocated extents
    while (!full) {
      try {
        allocations.add(new Long(mAllocator.allocateBlock(blockSizes[i
            % blockSizes.length])));
        ++i;
      } catch (CacheFullException cfe) {
        full = true;
      }
    }

    for (i = 0; i < blockSizes.length; i++) {
      BucketSizeInfo bucketSizeInfo = mAllocator
          .roundUpToBucketSizeInfo(blockSizes[0]);
      IndexStatistics indexStatistics = bucketSizeInfo.statistics();
      assertTrue(indexStatistics.freeCount() == 0);
    }

    for (long offset : allocations) {
      assertTrue(mAllocator.sizeOfAllocation(offset) == mAllocator
          .freeBlock(offset));
    }
    assertTrue(mAllocator.getUsedSize() == 0);
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

}
