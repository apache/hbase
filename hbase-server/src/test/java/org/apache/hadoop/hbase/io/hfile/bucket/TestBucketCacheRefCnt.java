/**
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache.WriterThread;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ IOTests.class, MediumTests.class })
public class TestBucketCacheRefCnt {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestBucketCacheRefCnt.class);

  private static final String IO_ENGINE = "offheap";
  private static final long CAPACITY_SIZE = 32 * 1024 * 1024;
  private static final int BLOCK_SIZE = 1024;
  private static final int[] BLOCK_SIZE_ARRAY =
      new int[] { 64, 128, 256, 512, 1024, 2048, 4096, 8192 };
  private static final String PERSISTENCE_PATH = null;
  private static final HFileContext CONTEXT = new HFileContextBuilder().build();

  private BucketCache cache;

  private static BucketCache create(int writerSize, int queueSize) throws IOException {
    return new BucketCache(IO_ENGINE, CAPACITY_SIZE, BLOCK_SIZE, BLOCK_SIZE_ARRAY, writerSize,
        queueSize, PERSISTENCE_PATH);
  }

  private static HFileBlock createBlock(int offset, int size) {
    return createBlock(offset, size, ByteBuffAllocator.HEAP);
  }

  private static HFileBlock createBlock(int offset, int size, ByteBuffAllocator alloc) {
    return new HFileBlock(BlockType.DATA, size, size, -1, ByteBuffer.allocate(size),
        HFileBlock.FILL_HEADER, offset, 52, size, CONTEXT, alloc);
  }

  private static BlockCacheKey createKey(String hfileName, long offset) {
    return new BlockCacheKey(hfileName, offset);
  }

  private void disableWriter() {
    if (cache != null) {
      for (WriterThread wt : cache.writerThreads) {
        wt.disableWriter();
        wt.interrupt();
      }
    }
  }

  @Test
  public void testBlockInRAMCache() throws IOException {
    cache = create(1, 1000);
    disableWriter();
    try {
      for (int i = 0; i < 10; i++) {
        HFileBlock blk = createBlock(i, 1020);
        BlockCacheKey key = createKey("testHFile-00", i);
        assertEquals(1, blk.refCnt());
        cache.cacheBlock(key, blk);
        assertEquals(i + 1, cache.getBlockCount());
        assertEquals(2, blk.refCnt());

        Cacheable block = cache.getBlock(key, false, false, false);
        try {
          assertEquals(3, blk.refCnt());
          assertEquals(3, block.refCnt());
          assertEquals(blk, block);
        } finally {
          block.release();
        }
        assertEquals(2, blk.refCnt());
        assertEquals(2, block.refCnt());
      }

      for (int i = 0; i < 10; i++) {
        BlockCacheKey key = createKey("testHFile-00", i);
        Cacheable blk = cache.getBlock(key, false, false, false);
        assertEquals(3, blk.refCnt());
        assertFalse(blk.release());
        assertEquals(2, blk.refCnt());

        assertTrue(cache.evictBlock(key));
        assertEquals(1, blk.refCnt());
        assertTrue(blk.release());
        assertEquals(0, blk.refCnt());
      }
    } finally {
      cache.shutdown();
    }
  }

  private void waitUntilFlushedToCache(BlockCacheKey key) throws InterruptedException {
    while (!cache.backingMap.containsKey(key) || cache.ramCache.containsKey(key)) {
      Thread.sleep(100);
    }
    Thread.sleep(1000);
  }

  @Test
  public void testBlockInBackingMap() throws Exception {
    ByteBuffAllocator alloc = ByteBuffAllocator.create(HBaseConfiguration.create(), true);
    cache = create(1, 1000);
    try {
      HFileBlock blk = createBlock(200, 1020, alloc);
      BlockCacheKey key = createKey("testHFile-00", 200);
      cache.cacheBlock(key, blk);
      waitUntilFlushedToCache(key);
      assertEquals(1, blk.refCnt());

      Cacheable block = cache.getBlock(key, false, false, false);
      assertTrue(block instanceof HFileBlock);
      assertTrue(((HFileBlock) block).getByteBuffAllocator() == alloc);
      assertEquals(2, block.refCnt());

      block.retain();
      assertEquals(3, block.refCnt());

      Cacheable newBlock = cache.getBlock(key, false, false, false);
      assertTrue(newBlock instanceof HFileBlock);
      assertTrue(((HFileBlock) newBlock).getByteBuffAllocator() == alloc);
      assertEquals(4, newBlock.refCnt());

      // release the newBlock
      assertFalse(newBlock.release());
      assertEquals(3, newBlock.refCnt());
      assertEquals(3, block.refCnt());

      // Evict the key
      cache.evictBlock(key);
      assertEquals(2, block.refCnt());

      // Evict again, shouldn't change the refCnt.
      cache.evictBlock(key);
      assertEquals(2, block.refCnt());

      assertFalse(block.release());
      assertEquals(1, block.refCnt());

      newBlock = cache.getBlock(key, false, false, false);
      assertEquals(2, block.refCnt());
      assertEquals(2, newBlock.refCnt());
      assertTrue(((HFileBlock) newBlock).getByteBuffAllocator() == alloc);

      // Release the block
      assertFalse(block.release());
      assertEquals(1, block.refCnt());

      // Release the newBlock;
      assertTrue(newBlock.release());
      assertEquals(0, newBlock.refCnt());
    } finally {
      cache.shutdown();
    }
  }

  @Test
  public void testInBucketCache() throws IOException {
    ByteBuffAllocator alloc = ByteBuffAllocator.create(HBaseConfiguration.create(), true);
    cache = create(1, 1000);
    try {
      HFileBlock blk = createBlock(200, 1020, alloc);
      BlockCacheKey key = createKey("testHFile-00", 200);
      cache.cacheBlock(key, blk);
      assertTrue(blk.refCnt() == 1 || blk.refCnt() == 2);

      Cacheable block1 = cache.getBlock(key, false, false, false);
      assertTrue(block1.refCnt() >= 2);
      assertTrue(((HFileBlock) block1).getByteBuffAllocator() == alloc);

      Cacheable block2 = cache.getBlock(key, false, false, false);
      assertTrue(((HFileBlock) block2).getByteBuffAllocator() == alloc);
      assertTrue(block2.refCnt() >= 3);

      cache.evictBlock(key);
      assertTrue(blk.refCnt() >= 1);
      assertTrue(block1.refCnt() >= 2);
      assertTrue(block2.refCnt() >= 2);

      // Get key again
      Cacheable block3 = cache.getBlock(key, false, false, false);
      if (block3 != null) {
        assertTrue(((HFileBlock) block3).getByteBuffAllocator() == alloc);
        assertTrue(block3.refCnt() >= 3);
        assertFalse(block3.release());
      }

      blk.release();
      boolean ret1 = block1.release();
      boolean ret2 = block2.release();
      assertTrue(ret1 || ret2);
      assertEquals(0, blk.refCnt());
      assertEquals(0, block1.refCnt());
      assertEquals(0, block2.refCnt());
    } finally {
      cache.shutdown();
    }
  }

  @Test
  public void testMarkStaleAsEvicted() throws Exception {
    cache = create(1, 1000);
    try {
      HFileBlock blk = createBlock(200, 1020);
      BlockCacheKey key = createKey("testMarkStaleAsEvicted", 200);
      cache.cacheBlock(key, blk);
      waitUntilFlushedToCache(key);
      assertEquals(1, blk.refCnt());
      assertNotNull(cache.backingMap.get(key));
      assertEquals(1, cache.backingMap.get(key).refCnt());

      // RPC reference this cache.
      Cacheable block1 = cache.getBlock(key, false, false, false);
      assertEquals(2, block1.refCnt());
      BucketEntry be1 = cache.backingMap.get(key);
      assertNotNull(be1);
      assertEquals(2, be1.refCnt());

      // We've some RPC reference, so it won't have any effect.
      assertFalse(be1.markStaleAsEvicted());
      assertEquals(2, block1.refCnt());
      assertEquals(2, cache.backingMap.get(key).refCnt());

      // Release the RPC reference.
      block1.release();
      assertEquals(1, block1.refCnt());
      assertEquals(1, cache.backingMap.get(key).refCnt());

      // Mark the stale as evicted again, it'll do the de-allocation.
      assertTrue(be1.markStaleAsEvicted());
      assertEquals(0, block1.refCnt());
      assertNull(cache.backingMap.get(key));
      assertEquals(0, cache.size());
    } finally {
      cache.shutdown();
    }
  }
}
