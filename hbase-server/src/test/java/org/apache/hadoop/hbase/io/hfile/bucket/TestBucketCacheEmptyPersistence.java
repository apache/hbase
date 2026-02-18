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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.io.hfile.CacheTestUtils;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.protobuf.ProtobufMagic;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests for HBASE-29857: BucketCache recovery should gracefully handle empty or truncated
 * persistence files instead of throwing NullPointerException.
 * <p>
 * These tests verify that the cache recovers gracefully when the persistence file contains only
 * magic bytes without actual cache data. The fix adds null checks that throw IOException instead of
 * allowing NullPointerException to propagate.
 */
@Category(SmallTests.class)
public class TestBucketCacheEmptyPersistence {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBucketCacheEmptyPersistence.class);

  private static final long CAPACITY_SIZE = 32 * 1024 * 1024;
  private static final int WRITE_THREADS = BucketCache.DEFAULT_WRITER_THREADS;
  private static final int WRITER_QUEUE_LEN = BucketCache.DEFAULT_WRITER_QUEUE_ITEMS;

  /**
   * Test that BucketCache recovers gracefully when the persistence file contains only non-chunked
   * format (PBUF) magic bytes without any cache data. This tests the null check fix in
   * retrieveFromFile() for the non-chunked format path.
   */
  @Test
  public void testEmptyPersistenceFileNonChunkedFormat() throws Exception {
    HBaseTestingUtil testUtil = new HBaseTestingUtil();
    Path testDir = testUtil.getDataTestDir();
    testUtil.getTestFileSystem().mkdirs(testDir);

    Configuration conf = HBaseConfiguration.create();
    conf.setLong(BUCKETCACHE_PERSIST_INTERVAL_KEY, Long.MAX_VALUE);

    String persistencePath = testDir + "/bucket.persistence";
    File persistenceFile = new File(persistencePath);

    // Create a persistence file with only PBUF magic bytes - no actual cache data
    try (FileOutputStream fos = new FileOutputStream(persistenceFile)) {
      fos.write(ProtobufMagic.PB_MAGIC);
    }
    assertTrue("Persistence file should exist", persistenceFile.exists());

    int[] bucketSizes = new int[] { 8 * 1024 + 1024 };

    // BucketCache should recover gracefully from the empty file
    BucketCache bucketCache =
      new BucketCache("file:" + testDir + "/bucket.cache", CAPACITY_SIZE, 8192, bucketSizes,
        WRITE_THREADS, WRITER_QUEUE_LEN, persistencePath, DEFAULT_ERROR_TOLERATION_DURATION, conf);

    // Cache should initialize successfully (not hang or throw)
    assertTrue("Cache should initialize successfully after recovering from empty file",
      bucketCache.waitForCacheInitialization(10000));

    // Verify the cache was reset (backing map should be empty since file had no valid data)
    assertEquals("Backing map should be empty after recovering from empty persistence file", 0,
      bucketCache.backingMap.size());

    // Verify the cache is usable - we can add and retrieve blocks
    CacheTestUtils.HFileBlockPair[] blocks = CacheTestUtils.generateHFileBlocks(8192, 1);
    bucketCache.cacheBlock(blocks[0].getBlockName(), blocks[0].getBlock());

    // Wait for block to be written to cache
    Thread.sleep(100);

    // Verify block can be retrieved
    Cacheable retrieved = bucketCache.getBlock(blocks[0].getBlockName(), false, false, false);
    assertNotNull("Should be able to retrieve cached block", retrieved);

    bucketCache.shutdown();
    testUtil.cleanupTestDir();
  }

  /**
   * Test that BucketCache recovers gracefully when the persistence file contains only chunked
   * format (V2UF) magic bytes without any cache data. This tests the null check fix in
   * retrieveChunkedBackingMap().
   */
  @Test
  public void testEmptyPersistenceFileChunkedFormat() throws Exception {
    HBaseTestingUtil testUtil = new HBaseTestingUtil();
    Path testDir = testUtil.getDataTestDir();
    testUtil.getTestFileSystem().mkdirs(testDir);

    Configuration conf = HBaseConfiguration.create();
    conf.setLong(BUCKETCACHE_PERSIST_INTERVAL_KEY, Long.MAX_VALUE);

    String persistencePath = testDir + "/bucket.persistence";
    File persistenceFile = new File(persistencePath);

    // Create a persistence file with only chunked format (V2UF) magic bytes - no actual cache data
    try (FileOutputStream fos = new FileOutputStream(persistenceFile)) {
      fos.write(BucketProtoUtils.PB_MAGIC_V2);
    }
    assertTrue("Persistence file should exist", persistenceFile.exists());

    int[] bucketSizes = new int[] { 8 * 1024 + 1024 };

    // BucketCache should recover gracefully from the empty file
    BucketCache bucketCache =
      new BucketCache("file:" + testDir + "/bucket.cache", CAPACITY_SIZE, 8192, bucketSizes,
        WRITE_THREADS, WRITER_QUEUE_LEN, persistencePath, DEFAULT_ERROR_TOLERATION_DURATION, conf);

    // Cache should initialize successfully (not hang or throw)
    assertTrue("Cache should initialize successfully after recovering from empty file",
      bucketCache.waitForCacheInitialization(10000));

    // Verify the cache was reset (backing map should be empty since file had no valid data)
    assertEquals("Backing map should be empty after recovering from empty persistence file", 0,
      bucketCache.backingMap.size());

    // Verify the cache is usable - we can add and retrieve blocks
    CacheTestUtils.HFileBlockPair[] blocks = CacheTestUtils.generateHFileBlocks(8192, 1);
    bucketCache.cacheBlock(blocks[0].getBlockName(), blocks[0].getBlock());

    // Wait for block to be written to cache
    Thread.sleep(100);

    // Verify block can be retrieved
    Cacheable retrieved = bucketCache.getBlock(blocks[0].getBlockName(), false, false, false);
    assertNotNull("Should be able to retrieve cached block", retrieved);

    bucketCache.shutdown();
    testUtil.cleanupTestDir();
  }
}
