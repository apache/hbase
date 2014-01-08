/*
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

package org.apache.hadoop.hbase.io.hfile.bucket;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MultithreadedTestUtil;

import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.RawHFileBlock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class TestBucketCache {

  private static final Log LOG = LogFactory.getLog(TestBucketCache.class);

  private static final long CAPACITY_SIZE = 32 * 1024 * 1024;

  private static final int CACHE_SIZE = 1000000;
  private static final int NUM_BLOCKS = 100;
  private static final int BLOCK_SIZE = CACHE_SIZE / NUM_BLOCKS;
  private static final int NUM_THREADS = 1000;
  private static final int NUM_QUERIES = 10000;

  private final String ioEngineName;

  private BucketCache cache;



  public TestBucketCache(String ioEngineName) {
    this.ioEngineName = ioEngineName;
    LOG.info("Running with ioEngineName = " + ioEngineName);
  }

  @Parameterized.Parameters
  public static Collection<Object[]> getConfigurations() {
    Object[][] data = new Object[][] { {"heap"}, {"offheap"} };
    return Arrays.asList(data);
  }

  @Before
  public void setup() throws IOException {
    cache = new MockedBucketCache(ioEngineName, CAPACITY_SIZE,
        BucketCache.DEFAULT_WRITER_THREADS,
        BucketCache.DEFAULT_WRITER_QUEUE_ITEMS);
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
        allocations.add(mAllocator.allocateBlock(blockSizes[i
            % blockSizes.length]));
        ++i;
      } catch (CacheFullException cfe) {
        full = true;
      }
    }

    for (i = 0; i < blockSizes.length; i++) {
      BucketAllocator.BucketSizeInfo bucketSizeInfo = mAllocator
          .roundUpToBucketSizeInfo(blockSizes[0]);
      BucketAllocator.IndexStatistics indexStatistics = bucketSizeInfo.statistics();
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
    BlockOnDisk[] blocks = generateDiskBlocks(NUM_QUERIES,
        BLOCK_SIZE);
    // Confirm empty
    for (BlockOnDisk block : blocks) {
      assertNull(cache.getBlock(block.blockName, true));
    }

    // Add blocks
    for (BlockOnDisk block : blocks) {
      cache.cacheBlock(block.blockName,
              new RawHFileBlock(BlockType.DATA, block.block));
    }

    // Check if all blocks are properly cached and contain the right
    // information, or the blocks are null.
    // MapMaker makes no guarantees when it will evict, so neither can we.

    for (BlockOnDisk block : blocks) {
      byte[] buf = cache.getBlock(block.blockName, true);
      if (buf != null) {
        assertArrayEquals(block.block, buf);
      }

    }

    // Re-add some duplicate blocks. Hope nothing breaks.

    for (BlockOnDisk block : blocks) {
      try {
        if (cache.getBlock(block.blockName, true) != null) {
          cache.cacheBlock(block.blockName,
                  new RawHFileBlock(BlockType.DATA, block.block));
        }
      } catch (RuntimeException re) {
        // expected
      }
    }
  }

  @Test
  public void testCacheMultiThreadedSingleKey() throws Exception {
    final BlockCacheKey key = new BlockCacheKey("key", 0);
    final byte[] buf = new byte[5 * 1024];
    Arrays.fill(buf, (byte) 5);

    Configuration conf = new Configuration();
    MultithreadedTestUtil.TestContext ctx = new MultithreadedTestUtil.TestContext(
        conf);

    final AtomicInteger totalQueries = new AtomicInteger();
    cache.cacheBlock(key, new RawHFileBlock(BlockType.DATA, buf));

    for (int i = 0; i < NUM_THREADS; i++) {
      MultithreadedTestUtil.TestThread t = new MultithreadedTestUtil.RepeatingTestThread(ctx) {
        @Override
        public void doAnAction() throws Exception {
          byte[] returned = cache.getBlock(key, false);
          assertArrayEquals(buf, returned);
          totalQueries.incrementAndGet();
        }
      };

      t.setDaemon(true);
      ctx.addThread(t);
    }

    ctx.startThreads();
    while (totalQueries.get() < NUM_QUERIES && ctx.shouldRun()) {
      Thread.sleep(10);
    }
    ctx.stop();
  }

  @Test
  public void testHeapSizeChanges() throws Exception {
    cache.stopWriterThreads();
    BlockOnDisk[] blocks = generateDiskBlocks(BLOCK_SIZE, 1);
    long heapSize = cache.heapSize();
    cache.cacheBlock(blocks[0].blockName,
            new RawHFileBlock(BlockType.DATA, blocks[0].block));

    /*When we cache something HeapSize should always increase */
    assertTrue(heapSize < cache.heapSize());

    cache.evictBlock(blocks[0].blockName);

    /*Post eviction, heapsize should be the same */
    assertEquals(heapSize, cache.heapSize());
  }

  private static class BlockOnDisk {
    BlockCacheKey blockName;
    byte[] block;
  }

  private static BlockOnDisk[] generateDiskBlocks(int blockSize,
      int numBlocks) {
    BlockOnDisk[] retVal = new BlockOnDisk[numBlocks];
    Random rand = new Random();
    HashSet<String> usedStrings = new HashSet<String>();
    for (int i = 0; i < numBlocks; i++) {
      byte[] generated = new byte[blockSize];
      rand.nextBytes(generated);
      String strKey;
      strKey = Long.toString(rand.nextLong());
      while (!usedStrings.add(strKey)) {
        strKey = Long.toString(rand.nextLong());
      }
      retVal[i] = new BlockOnDisk();
      retVal[i].blockName = new BlockCacheKey(strKey, 0);
      retVal[i].block = generated;
    }
    return retVal;
  }

  private static class MockedBucketCache extends BucketCache {

    public MockedBucketCache(String ioEngineName, long capacity,
        int writerThreads,
        int writerQLen) throws IOException {
      super(ioEngineName,
          capacity,
          writerThreads,
          writerQLen,
          DEFAULT_ERROR_TOLERATION_DURATION,
          CacheConfig.DEFAULT_L2_BUCKET_CACHE_BUCKET_SIZES,
          null);
      super.wait_when_cache = true;
    }

    @Override
    public boolean cacheBlock(BlockCacheKey cacheKey, RawHFileBlock block,
        boolean inMemory) {
      if (super.getBlock(cacheKey, true, false) != null) {
        throw new RuntimeException("Cached an already cached block");
      }
      return super.cacheBlock(cacheKey, block, inMemory);
    }

    @Override
    public boolean cacheBlock(BlockCacheKey cacheKey, RawHFileBlock block) {
      if (super.getBlock(cacheKey, true, false) != null) {
        throw new RuntimeException("Cached an already cached block");
      }
      return super.cacheBlock(cacheKey, block);
    }
  }
}
