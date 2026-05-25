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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.stream.Stream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.CacheTestUtils;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.params.provider.Arguments;

/**
 * Basic test for check file's integrity before start BucketCache in fileIOEngine
 */
@Tag(IOTests.TAG)
@Tag(LargeTests.TAG)
@HBaseParameterizedTestTemplate(name = "{index}: blockSize={0}, bucketSizes={1}")
public class TestVerifyBucketCacheFile {

  public static Stream<Arguments> parameter() {
    return Stream.of(Arguments.of(8192, null),
      Arguments.of(16 * 1024,
        new int[] { 2 * 1024 + 1024, 4 * 1024 + 1024, 8 * 1024 + 1024, 16 * 1024 + 1024,
          28 * 1024 + 1024, 32 * 1024 + 1024, 64 * 1024 + 1024, 96 * 1024 + 1024,
          128 * 1024 + 1024 }));
  }

  private int constructedBlockSize;

  private int[] constructedBlockSizes;

  final long capacitySize = 32 * 1024 * 1024;
  final int writeThreads = BucketCache.DEFAULT_WRITER_THREADS;
  final int writerQLen = BucketCache.DEFAULT_WRITER_QUEUE_ITEMS;

  public TestVerifyBucketCacheFile(int constructedBlockSize, int[] constructedBlockSizes) {
    this.constructedBlockSize = constructedBlockSize;
    this.constructedBlockSizes = constructedBlockSizes;
  }

  /**
   * Test cache file or persistence file does not exist whether BucketCache starts normally (1)
   * Start BucketCache and add some blocks, then shutdown BucketCache and persist cache to file.
   * Restart BucketCache and it can restore cache from file. (2) Delete bucket cache file after
   * shutdown BucketCache. Restart BucketCache and it can't restore cache from file, the cache file
   * and persistence file would be deleted before BucketCache start normally. (3) Delete persistence
   * file after shutdown BucketCache. Restart BucketCache and it can't restore cache from file, the
   * cache file and persistence file would be deleted before BucketCache start normally.
   * @throws Exception the exception
   */
  @TestTemplate
  public void testRetrieveFromFile() throws Exception {
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    Path testDir = TEST_UTIL.getDataTestDir();
    TEST_UTIL.getTestFileSystem().mkdirs(testDir);

    BucketCache bucketCache =
      new BucketCache("file:" + testDir + "/bucket.cache", capacitySize, constructedBlockSize,
        constructedBlockSizes, writeThreads, writerQLen, testDir + "/bucket.persistence");
    long usedSize = bucketCache.getAllocator().getUsedSize();
    assertEquals(0, usedSize);
    CacheTestUtils.HFileBlockPair[] blocks =
      CacheTestUtils.generateHFileBlocks(constructedBlockSize, 1);
    // Add blocks
    for (CacheTestUtils.HFileBlockPair block : blocks) {
      cacheAndWaitUntilFlushedToBucket(bucketCache, block.getBlockName(), block.getBlock());
    }
    usedSize = bucketCache.getAllocator().getUsedSize();
    assertNotEquals(0, usedSize);
    // 1.persist cache to file
    bucketCache.shutdown();
    // restore cache from file
    bucketCache =
      new BucketCache("file:" + testDir + "/bucket.cache", capacitySize, constructedBlockSize,
        constructedBlockSizes, writeThreads, writerQLen, testDir + "/bucket.persistence");
    assertEquals(usedSize, bucketCache.getAllocator().getUsedSize());
    // persist cache to file
    bucketCache.shutdown();

    // 2.delete bucket cache file
    final java.nio.file.Path cacheFile =
      FileSystems.getDefault().getPath(testDir.toString(), "bucket.cache");
    assertTrue(Files.deleteIfExists(cacheFile));
    // can't restore cache from file
    bucketCache =
      new BucketCache("file:" + testDir + "/bucket.cache", capacitySize, constructedBlockSize,
        constructedBlockSizes, writeThreads, writerQLen, testDir + "/bucket.persistence");
    assertEquals(0, bucketCache.getAllocator().getUsedSize());
    assertEquals(0, bucketCache.backingMap.size());
    // Add blocks
    for (CacheTestUtils.HFileBlockPair block : blocks) {
      cacheAndWaitUntilFlushedToBucket(bucketCache, block.getBlockName(), block.getBlock());
    }
    usedSize = bucketCache.getAllocator().getUsedSize();
    assertNotEquals(0, usedSize);
    // persist cache to file
    bucketCache.shutdown();

    // 3.delete backingMap persistence file
    final java.nio.file.Path mapFile =
      FileSystems.getDefault().getPath(testDir.toString(), "bucket.persistence");
    assertTrue(Files.deleteIfExists(mapFile));
    // can't restore cache from file
    bucketCache =
      new BucketCache("file:" + testDir + "/bucket.cache", capacitySize, constructedBlockSize,
        constructedBlockSizes, writeThreads, writerQLen, testDir + "/bucket.persistence");
    assertEquals(0, bucketCache.getAllocator().getUsedSize());
    assertEquals(0, bucketCache.backingMap.size());

    TEST_UTIL.cleanupTestDir();
  }

  /**
   * Test whether BucketCache is started normally after modifying the cache file. Start BucketCache
   * and add some blocks, then shutdown BucketCache and persist cache to file. Restart BucketCache
   * after modify cache file's data, and it can't restore cache from file, the cache file and
   * persistence file would be deleted before BucketCache start normally.
   * @throws Exception the exception
   */
  @TestTemplate
  public void testModifiedBucketCacheFileData() throws Exception {
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    Path testDir = TEST_UTIL.getDataTestDir();
    TEST_UTIL.getTestFileSystem().mkdirs(testDir);

    BucketCache bucketCache =
      new BucketCache("file:" + testDir + "/bucket.cache", capacitySize, constructedBlockSize,
        constructedBlockSizes, writeThreads, writerQLen, testDir + "/bucket.persistence");
    long usedSize = bucketCache.getAllocator().getUsedSize();
    assertEquals(0, usedSize);

    CacheTestUtils.HFileBlockPair[] blocks =
      CacheTestUtils.generateHFileBlocks(constructedBlockSize, 1);
    // Add blocks
    for (CacheTestUtils.HFileBlockPair block : blocks) {
      cacheAndWaitUntilFlushedToBucket(bucketCache, block.getBlockName(), block.getBlock());
    }
    usedSize = bucketCache.getAllocator().getUsedSize();
    assertNotEquals(0, usedSize);
    // persist cache to file
    bucketCache.shutdown();

    // modified bucket cache file
    String file = testDir + "/bucket.cache";
    try (BufferedWriter out =
      new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, false)))) {
      out.write("test bucket cache");
    }
    // can't restore cache from file
    bucketCache =
      new BucketCache("file:" + testDir + "/bucket.cache", capacitySize, constructedBlockSize,
        constructedBlockSizes, writeThreads, writerQLen, testDir + "/bucket.persistence");
    assertEquals(0, bucketCache.getAllocator().getUsedSize());
    assertEquals(0, bucketCache.backingMap.size());

    TEST_UTIL.cleanupTestDir();
  }

  /**
   * Test whether BucketCache is started normally after modifying the cache file's last modified
   * time. First Start BucketCache and add some blocks, then shutdown BucketCache and persist cache
   * to file. Then Restart BucketCache after modify cache file's last modified time, and it can't
   * restore cache from file, the cache file and persistence file would be deleted before
   * BucketCache start normally.
   * @throws Exception the exception
   */
  @TestTemplate
  public void testModifiedBucketCacheFileTime() throws Exception {
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    Path testDir = TEST_UTIL.getDataTestDir();
    TEST_UTIL.getTestFileSystem().mkdirs(testDir);

    BucketCache bucketCache =
      new BucketCache("file:" + testDir + "/bucket.cache", capacitySize, constructedBlockSize,
        constructedBlockSizes, writeThreads, writerQLen, testDir + "/bucket.persistence");
    long usedSize = bucketCache.getAllocator().getUsedSize();
    assertEquals(0, usedSize);

    CacheTestUtils.HFileBlockPair[] blocks =
      CacheTestUtils.generateHFileBlocks(constructedBlockSize, 1);
    // Add blocks
    for (CacheTestUtils.HFileBlockPair block : blocks) {
      cacheAndWaitUntilFlushedToBucket(bucketCache, block.getBlockName(), block.getBlock());
    }
    usedSize = bucketCache.getAllocator().getUsedSize();
    assertNotEquals(0, usedSize);
    // persist cache to file
    bucketCache.shutdown();

    // modified bucket cache file LastModifiedTime
    final java.nio.file.Path file =
      FileSystems.getDefault().getPath(testDir.toString(), "bucket.cache");
    Files.setLastModifiedTime(file, FileTime.from(Instant.now().plusMillis(1_000)));
    // can't restore cache from file
    bucketCache =
      new BucketCache("file:" + testDir + "/bucket.cache", capacitySize, constructedBlockSize,
        constructedBlockSizes, writeThreads, writerQLen, testDir + "/bucket.persistence");
    assertEquals(0, bucketCache.getAllocator().getUsedSize());
    assertEquals(0, bucketCache.backingMap.size());

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
