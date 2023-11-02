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
package org.apache.hadoop.hbase.io.hfile.bucket;

import static org.apache.hadoop.hbase.io.hfile.CacheConfig.BUCKETCACHE_PERSIST_INTERVAL_KEY;
import static org.apache.hadoop.hbase.io.hfile.bucket.BucketCache.DEFAULT_ERROR_TOLERATION_DURATION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.FileInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.CacheTestUtils;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.protobuf.ProtobufMagic;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.BucketCacheProtos;

/**
 * Basic test for check file's integrity before start BucketCache in fileIOEngine
 */
@Category(SmallTests.class)
public class TestRecoveryPersistentBucketCache {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRecoveryPersistentBucketCache.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestRecoveryPersistentBucketCache.class);

  final long capacitySize = 32 * 1024 * 1024;
  final int writeThreads = BucketCache.DEFAULT_WRITER_THREADS;
  final int writerQLen = BucketCache.DEFAULT_WRITER_QUEUE_ITEMS;

  @Test
  public void testBucketCacheRecoveryFailReplayTXs() throws Exception {
    HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
    Path testDir = TEST_UTIL.getDataTestDir();
    TEST_UTIL.getTestFileSystem().mkdirs(testDir);
    Configuration conf = HBaseConfiguration.create();
    // Disables the persister thread by setting its interval to MAX_VALUE
    conf.setLong(BUCKETCACHE_PERSIST_INTERVAL_KEY, Long.MAX_VALUE);
    int[] bucketSizes = new int[] { 8 * 1024 + 1024 };
    BucketCache bucketCache = new BucketCache("file:" + testDir + "/bucket.cache", capacitySize,
      8192, bucketSizes, writeThreads, writerQLen, testDir + "/bucket.persistence",
      DEFAULT_ERROR_TOLERATION_DURATION, conf);

    CacheTestUtils.HFileBlockPair[] blocks = CacheTestUtils.generateHFileBlocks(8192, 4);

    CacheTestUtils.HFileBlockPair[] smallerBlocks = CacheTestUtils.generateHFileBlocks(4096, 1);
    // Add three blocks
    cacheAndWaitUntilFlushedToBucket(bucketCache, blocks[0].getBlockName(), blocks[0].getBlock());
    cacheAndWaitUntilFlushedToBucket(bucketCache, blocks[1].getBlockName(), blocks[1].getBlock());
    cacheAndWaitUntilFlushedToBucket(bucketCache, blocks[2].getBlockName(), blocks[2].getBlock());
    // saves the current state of the cache
    bucketCache.persistToFile();
    bucketCache.cleanOldTransactions();
    bucketCache.resetTXsCount();

    assertEquals(blocks[0].getBlock(),
      bucketCache.getBlock(blocks[0].getBlockName(), false, false, false));
    assertEquals(blocks[1].getBlock(),
      bucketCache.getBlock(blocks[1].getBlockName(), false, false, false));
    assertEquals(blocks[2].getBlock(),
      bucketCache.getBlock(blocks[2].getBlockName(), false, false, false));

    // now adds a 4th block to bucket cache
    cacheAndWaitUntilFlushedToBucket(bucketCache, blocks[3].getBlockName(), blocks[3].getBlock());
    // evicts the 4th block
    bucketCache.evictBlock(blocks[3].getBlockName());
    // now adds a 5th block to bucket cache. This block is half the size of the previous
    // blocks, and it will be added in the same offset of the previous evicted block. When
    // recovering the cache, we should be able to recover this block, even if eviction txs is lost.
    cacheAndWaitUntilFlushedToBucket(bucketCache, smallerBlocks[0].getBlockName(),
      smallerBlocks[0].getBlock());

    // Finds the tx file for the eviction of 4th block and delete it
    bucketCache.listTransactions(false).stream().filter(f -> {
      try (FileInputStream is = new FileInputStream(f)) {
        int pblen = ProtobufMagic.lengthOfPBMagic();
        byte[] pbuf = new byte[pblen];
        IOUtils.readFully(is, pbuf, 0, pblen);
        BucketCacheProtos.BucketCacheTransaction tx =
          BucketCacheProtos.BucketCacheTransaction.parseDelimitedFrom(is);
        return (tx.getType() == BucketCacheProtos.TransactionType.eviction);
      } catch (Exception e) {
        return false;
      }
    }).findFirst().get().delete();
    // Creates new bucket cache instance without persisting to file after evicting 4th block
    // and caching 5th block. Here the cache file has the first three blocks, followed by the
    // 5th block and the second half of 4th block (we evicted 4th block, freeing up its
    // offset in the cache, then added 5th block which is half the size of other blocks, so it's
    // going to override the first half of the 4th block in the cache). That's fine because
    // the in-memory backing map has the right blocks and related offsets. However, the
    // persistent map file only has information about the first three blocks, and we only have
    // valid txs files for the insertion of 4th and 5th blocks, but not the eviction of 4th block.
    // When recovering the state, we'll read the map file with the first 3 blocks, then replay
    // the txs files will give us a backing map with all the 5 blocks. But we also validate the
    // cache time recorded in the back map against the block data in the cache. This is recorded
    // in the cache as the first 8 bytes of a block, so the 4th block had its first 8 blocks
    // now overridden by the 5th block, causing this check to fail and removal of
    // the 4th block from the backing map.
    BucketCache newBucketCache = new BucketCache("file:" + testDir + "/bucket.cache", capacitySize,
      8192, bucketSizes, writeThreads, writerQLen, testDir + "/bucket.persistence",
      DEFAULT_ERROR_TOLERATION_DURATION, conf);

    assertNull(newBucketCache.getBlock(blocks[3].getBlockName(), false, false, false));
    assertEquals(blocks[0].getBlock(),
      newBucketCache.getBlock(blocks[0].getBlockName(), false, false, false));
    assertEquals(blocks[1].getBlock(),
      newBucketCache.getBlock(blocks[1].getBlockName(), false, false, false));
    assertEquals(blocks[2].getBlock(),
      newBucketCache.getBlock(blocks[2].getBlockName(), false, false, false));
    assertEquals(smallerBlocks[0].getBlock(),
      newBucketCache.getBlock(smallerBlocks[0].getBlockName(), false, false, false));
    assertEquals(4, newBucketCache.backingMap.size());
    TEST_UTIL.cleanupTestDir();
  }

  private void waitUntilFlushedToBucket(BucketCache cache, BlockCacheKey cacheKey)
    throws InterruptedException {
    while (!cache.backingMap.containsKey(cacheKey) || cache.ramCache.containsKey(cacheKey)) {
      Thread.sleep(100);
    }
  }

  // BucketCache.cacheBlock is async, it first adds block to ramCache and writeQueue, then writer
  // threads will flush it to the bucket and put reference entry in backingMap.
  private void cacheAndWaitUntilFlushedToBucket(BucketCache cache, BlockCacheKey cacheKey,
    Cacheable block) throws InterruptedException {
    cache.cacheBlock(cacheKey, block);
    waitUntilFlushedToBucket(cache, cacheKey);
  }

}
