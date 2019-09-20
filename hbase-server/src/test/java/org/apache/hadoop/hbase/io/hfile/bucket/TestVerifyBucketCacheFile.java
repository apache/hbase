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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.CacheTestUtils;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Basic test for check file's integrity when BucketCache retrieve from file
 */
@Category(SmallTests.class)
public class TestVerifyBucketCacheFile {
  final int constructedBlockSize = 8 * 1024;
  final long capacitySize = 32 * 1024 * 1024;
  final int writeThreads = BucketCache.DEFAULT_WRITER_THREADS;
  final int writerQLen = BucketCache.DEFAULT_WRITER_QUEUE_ITEMS;

  /**
   * Test cache file or persistence file does not exist whether BucketCache starts normally
   * (1) Start BucketCache and add some blocks, then shutdown BucketCache and persist cache
   * to file. Restart BucketCache and it can restore cache from file.
   * (2) Delete bucket cache file after shutdown BucketCache. Restart BucketCache and it can't
   * restore cache from file, the cache file and persistence file would be deleted before
   * BucketCache start normally.
   * (3) Delete persistence file after shutdown BucketCache. Restart BucketCache and it can't
   * restore cache from file, the cache file and persistence file would be deleted before
   * BucketCache start normally.
   * @throws Exception the exception
   */
  @Test
  public void testRetrieveFromFile() throws Exception {
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    Path testDir = TEST_UTIL.getDataTestDir();
    TEST_UTIL.getTestFileSystem().mkdirs(testDir);

    BucketCache bucketCache =
      new BucketCache("file:" + testDir + "/bucket.cache", capacitySize, constructedBlockSize,
        null, writeThreads, writerQLen, testDir + "/bucket.persistence");
    long usedSize = bucketCache.getAllocator().getUsedSize();
    assertTrue(usedSize == 0);
    CacheTestUtils.HFileBlockPair[] blocks =
      CacheTestUtils.generateHFileBlocks(constructedBlockSize, 1);
    // Add blocks
    for (CacheTestUtils.HFileBlockPair block : blocks) {
      cacheAndWaitUntilFlushedToBucket(bucketCache, block.getBlockName(), block.getBlock());
    }
    usedSize = bucketCache.getAllocator().getUsedSize();
    assertTrue(usedSize != 0);
    // 1.persist cache to file
    bucketCache.shutdown();
    // restore cache from file
    bucketCache =
      new BucketCache("file:" + testDir + "/bucket.cache", capacitySize, constructedBlockSize,
        null, writeThreads, writerQLen, testDir + "/bucket.persistence");
    assertEquals(usedSize, bucketCache.getAllocator().getUsedSize());
    // persist cache to file
    bucketCache.shutdown();

    // 2.delete bucket cache file
    File cacheFile = new File(testDir + "/bucket.cache");
    assertTrue(cacheFile.delete());
    // can't restore cache from file
    bucketCache =
      new BucketCache("file:" + testDir + "/bucket.cache", capacitySize, constructedBlockSize,
        null, writeThreads, writerQLen, testDir + "/bucket.persistence");
    assertEquals(0, bucketCache.getAllocator().getUsedSize());
    assertEquals(0, bucketCache.backingMap.size());
    // Add blocks
    for (CacheTestUtils.HFileBlockPair block : blocks) {
      cacheAndWaitUntilFlushedToBucket(bucketCache, block.getBlockName(), block.getBlock());
    }
    usedSize = bucketCache.getAllocator().getUsedSize();
    assertTrue(usedSize != 0);
    // persist cache to file
    bucketCache.shutdown();

    // 3.delete backingMap persistence file
    File mapFile = new File(testDir + "/bucket.persistence");
    assertTrue(mapFile.delete());
    // can't restore cache from file
    bucketCache =
      new BucketCache("file:" + testDir + "/bucket.cache", capacitySize, constructedBlockSize,
        null, writeThreads, writerQLen, testDir + "/bucket.persistence");
    assertEquals(0, bucketCache.getAllocator().getUsedSize());
    assertEquals(0, bucketCache.backingMap.size());

    TEST_UTIL.cleanupTestDir();
  }

  /**
   * Test whether BucketCache is started normally after modifying the cache file.
   * Start BucketCache and add some blocks, then shutdown BucketCache and persist cache to file.
   * Restart BucketCache after modify cache file's data, and it can't restore cache from file,
   * the cache file and persistence file would be deleted before BucketCache start normally.
   * @throws Exception the exception
   */
  @Test
  public void testModifiedBucketCacheFileData() throws Exception {
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    Path testDir = TEST_UTIL.getDataTestDir();
    TEST_UTIL.getTestFileSystem().mkdirs(testDir);

    BucketCache bucketCache =
      new BucketCache("file:" + testDir + "/bucket.cache", capacitySize, constructedBlockSize,
        null, writeThreads, writerQLen, testDir + "/bucket.persistence");
    long usedSize = bucketCache.getAllocator().getUsedSize();
    assertTrue(usedSize == 0);

    CacheTestUtils.HFileBlockPair[] blocks =
      CacheTestUtils.generateHFileBlocks(constructedBlockSize, 1);
    // Add blocks
    for (CacheTestUtils.HFileBlockPair block : blocks) {
      cacheAndWaitUntilFlushedToBucket(bucketCache, block.getBlockName(), block.getBlock());
    }
    usedSize = bucketCache.getAllocator().getUsedSize();
    assertTrue(usedSize != 0);
    // persist cache to file
    bucketCache.shutdown();

    // modified bucket cache file
    String file = testDir + "/bucket.cache";
    try(BufferedWriter out = new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream(file, false)))) {
      out.write("test bucket cache");
    }

    // can't restore cache from file
    bucketCache =
      new BucketCache("file:" + testDir + "/bucket.cache", capacitySize, constructedBlockSize,
        null, writeThreads, writerQLen, testDir + "/bucket.persistence");
    assertEquals(0, bucketCache.getAllocator().getUsedSize());
    assertEquals(0, bucketCache.backingMap.size());

    TEST_UTIL.cleanupTestDir();
  }

  /**
   * Test whether BucketCache is started normally after modifying the cache file's last modified
   * time. First Start BucketCache and add some blocks, then shutdown BucketCache and persist
   * cache to file. Then Restart BucketCache after modify cache file's last modified time, and
   * it can't restore cache from file, the cache file and persistence file would be deleted
   * before BucketCache start normally.
   * @throws Exception the exception
   */
  @Test
  public void testModifiedBucketCacheFileTime() throws Exception {
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    Path testDir = TEST_UTIL.getDataTestDir();
    TEST_UTIL.getTestFileSystem().mkdirs(testDir);

    BucketCache bucketCache =
      new BucketCache("file:" + testDir + "/bucket.cache", capacitySize, constructedBlockSize,
        null, writeThreads, writerQLen, testDir + "/bucket.persistence");
    long usedSize = bucketCache.getAllocator().getUsedSize();
    assertTrue(usedSize == 0);

    CacheTestUtils.HFileBlockPair[] blocks =
      CacheTestUtils.generateHFileBlocks(constructedBlockSize, 1);
    // Add blocks
    for (CacheTestUtils.HFileBlockPair block : blocks) {
      cacheAndWaitUntilFlushedToBucket(bucketCache, block.getBlockName(), block.getBlock());
    }
    usedSize = bucketCache.getAllocator().getUsedSize();
    assertTrue(usedSize != 0);
    // persist cache to file
    bucketCache.shutdown();

    // modified bucket cache file LastModifiedTime
    File file = new File(testDir + "/bucket.cache");
    assertTrue(file.setLastModified(System.currentTimeMillis() + 1000));

    // can't restore cache from file
    bucketCache =
      new BucketCache("file:" + testDir + "/bucket.cache", capacitySize, constructedBlockSize,
        null, writeThreads, writerQLen, testDir + "/bucket.persistence");
    assertEquals(0, bucketCache.getAllocator().getUsedSize());
    assertEquals(0, bucketCache.backingMap.size());

    TEST_UTIL.cleanupTestDir();
  }

  /**
   * Test whether it can read the old version's persistence file, it's for backward compatibility.
   * Start BucketCache and add some blocks, then persist cache to file in old way and shutdown
   * BucketCache. Restart BucketCache, and it can normally restore from old version persistence
   * file.
   * @throws Exception the exception
   */
  @Test
  public void compatibilityTest() throws Exception {
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    Path testDir = TEST_UTIL.getDataTestDir();
    TEST_UTIL.getTestFileSystem().mkdirs(testDir);
    String persistencePath = testDir + "/bucket.persistence";
    BucketCache bucketCache =
      new BucketCache("file:" + testDir + "/bucket.cache", capacitySize, constructedBlockSize,
        null, writeThreads, writerQLen, persistencePath);
    long usedSize = bucketCache.getAllocator().getUsedSize();
    assertTrue(usedSize == 0);

    CacheTestUtils.HFileBlockPair[] blocks =
      CacheTestUtils.generateHFileBlocks(constructedBlockSize, 1);
    // Add blocks
    for (CacheTestUtils.HFileBlockPair block : blocks) {
      cacheAndWaitUntilFlushedToBucket(bucketCache, block.getBlockName(), block.getBlock());
    }
    usedSize = bucketCache.getAllocator().getUsedSize();
    assertTrue(usedSize != 0);
    // persistence backingMap using old way
    persistToFileInOldWay(persistencePath + ".old", bucketCache.getMaxSize(),
      bucketCache.backingMap, bucketCache.getDeserialiserMap());
    bucketCache.shutdown();

    // restore cache from file which skip check checksum
    bucketCache =
      new BucketCache("file:" + testDir + "/bucket.cache", capacitySize, constructedBlockSize,
        null, writeThreads, writerQLen, persistencePath + ".old");
    assertEquals(usedSize, bucketCache.getAllocator().getUsedSize());
    assertEquals(blocks.length, bucketCache.backingMap.size());
  }

  private void persistToFileInOldWay(String persistencePath, long cacheCapacity,
    ConcurrentMap backingMap, UniqueIndexMap deserialiserMap)
    throws IOException {
    try(ObjectOutputStream oos = new ObjectOutputStream(
      new FileOutputStream(persistencePath, false))) {
      oos.writeLong(cacheCapacity);
      oos.writeUTF(FileIOEngine.class.getName());
      oos.writeUTF(backingMap.getClass().getName());
      oos.writeObject(deserialiserMap);
      oos.writeObject(backingMap);
    }
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
