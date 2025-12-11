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
import static org.apache.hadoop.hbase.io.hfile.bucket.BucketCache.BACKING_MAP_PERSISTENCE_CHUNK_SIZE;
import static org.apache.hadoop.hbase.io.hfile.bucket.BucketCache.DEFAULT_ERROR_TOLERATION_DURATION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.CacheTestUtils;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Basic test for check file's integrity before start BucketCache in fileIOEngine
 */
@RunWith(Parameterized.class)
@Category(SmallTests.class)
public class TestVerifyBucketCacheFile {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestVerifyBucketCacheFile.class);

  @Parameterized.Parameters(name = "{index}: blockSize={0}, bucketSizes={1}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][] { { 8192, null },
      { 16 * 1024,
        new int[] { 2 * 1024 + 1024, 4 * 1024 + 1024, 8 * 1024 + 1024, 16 * 1024 + 1024,
          28 * 1024 + 1024, 32 * 1024 + 1024, 64 * 1024 + 1024, 96 * 1024 + 1024,
          128 * 1024 + 1024 } } });
  }

  @Rule
  public TestName name = new TestName();

  @Parameterized.Parameter(0)
  public int constructedBlockSize;

  @Parameterized.Parameter(1)
  public int[] constructedBlockSizes;

  final long capacitySize = 32 * 1024 * 1024;
  final int writeThreads = BucketCache.DEFAULT_WRITER_THREADS;
  final int writerQLen = BucketCache.DEFAULT_WRITER_QUEUE_ITEMS;

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
  @Test
  public void testRetrieveFromFile() throws Exception {
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    Path testDir = TEST_UTIL.getDataTestDir();
    TEST_UTIL.getTestFileSystem().mkdirs(testDir);

    Configuration conf = HBaseConfiguration.create();
    // Disables the persister thread by setting its interval to MAX_VALUE
    conf.setLong(BUCKETCACHE_PERSIST_INTERVAL_KEY, Long.MAX_VALUE);
    BucketCache bucketCache = null;
    BucketCache recoveredBucketCache = null;
    try {
      bucketCache =
        new BucketCache("file:" + testDir + "/bucket.cache", capacitySize, constructedBlockSize,
          constructedBlockSizes, writeThreads, writerQLen, testDir + "/bucket.persistence"
          + name.getMethodName());
      assertTrue(bucketCache.waitForCacheInitialization(10000));
      long usedSize = bucketCache.getAllocator().getUsedSize();
      assertEquals(0, usedSize);
      CacheTestUtils.HFileBlockPair[] blocks =
        CacheTestUtils.generateHFileBlocks(constructedBlockSize, 1);
      String[] names = CacheTestUtils.getHFileNames(blocks);
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
          constructedBlockSizes, writeThreads, writerQLen, testDir
          + "/bucket.persistence" + name.getMethodName());
      assertTrue(bucketCache.waitForCacheInitialization(10000));
      assertEquals(usedSize, bucketCache.getAllocator().getUsedSize());
      // persist cache to file
      bucketCache.shutdown();

      // 2.delete bucket cache file
      final java.nio.file.Path cacheFile =
        FileSystems.getDefault().getPath(testDir.toString(), "bucket.cache");
      assertTrue(Files.deleteIfExists(cacheFile));
      // can't restore cache from file
      recoveredBucketCache =
        new BucketCache("file:" + testDir + "/bucket.cache", capacitySize, constructedBlockSize,
          constructedBlockSizes, writeThreads, writerQLen, testDir
          + "/bucket.persistence" + name.getMethodName());
      assertTrue(recoveredBucketCache.waitForCacheInitialization(10000));
      assertEquals(0, recoveredBucketCache.getAllocator().getUsedSize());
      assertEquals(0, recoveredBucketCache.backingMap.size());
      BlockCacheKey[] newKeys = CacheTestUtils.regenerateKeys(blocks, names);
      // Add blocks
      for (int i = 0; i < blocks.length; i++) {
        cacheAndWaitUntilFlushedToBucket(recoveredBucketCache, newKeys[i], blocks[i].getBlock());
      }
      usedSize = recoveredBucketCache.getAllocator().getUsedSize();
      assertNotEquals(0, usedSize);
      // persist cache to file
      recoveredBucketCache.shutdown();

      // 3.delete backingMap persistence file
      final java.nio.file.Path mapFile =
        FileSystems.getDefault().getPath(testDir.toString(), "bucket.persistence" + name.getMethodName());
      assertTrue(Files.deleteIfExists(mapFile));
      // can't restore cache from file
      bucketCache = new BucketCache("file:" + testDir + "/bucket.cache", capacitySize,
        constructedBlockSize, constructedBlockSizes, writeThreads, writerQLen,
        testDir + "/bucket.persistence" + name.getMethodName(),
        DEFAULT_ERROR_TOLERATION_DURATION, conf);
      assertTrue(bucketCache.waitForCacheInitialization(10000));
      waitPersistentCacheValidation(conf, bucketCache);
      assertEquals(0, bucketCache.getAllocator().getUsedSize());
      assertEquals(0, bucketCache.backingMap.size());
    } finally {
      if (bucketCache != null) {
        bucketCache.shutdown();
      }
      if (recoveredBucketCache != null) {
        recoveredBucketCache.shutdown();
      }
    }
    TEST_UTIL.cleanupTestDir();
  }

  @Test
  public void testRetrieveFromFileAfterDelete() throws Exception {
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    Path testDir = TEST_UTIL.getDataTestDir();
    TEST_UTIL.getTestFileSystem().mkdirs(testDir);
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setLong(CacheConfig.BUCKETCACHE_PERSIST_INTERVAL_KEY, 300);
    String mapFileName = testDir + "/bucket.persistence"
      + name.getMethodName() + EnvironmentEdgeManager.currentTime();
    BucketCache bucketCache = null;
    try {
      bucketCache = new BucketCache("file:" + testDir + "/bucket.cache" , capacitySize,
        constructedBlockSize, constructedBlockSizes, writeThreads, writerQLen, mapFileName,
        DEFAULT_ERROR_TOLERATION_DURATION, conf);
      assertTrue(bucketCache.waitForCacheInitialization(10000));
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
      // Shutdown BucketCache
      bucketCache.shutdown();
      // Delete the persistence file
      File mapFile = new File(mapFileName);
      assertTrue(mapFile.delete());
      Thread.sleep(350);
      // Create BucketCache
      bucketCache = new BucketCache("file:" + testDir + "/bucket.cache", capacitySize,
        constructedBlockSize, constructedBlockSizes, writeThreads, writerQLen, mapFileName,
        DEFAULT_ERROR_TOLERATION_DURATION, conf);
      assertTrue(bucketCache.waitForCacheInitialization(10000));
      waitPersistentCacheValidation(conf, bucketCache);
      assertEquals(0, bucketCache.getAllocator().getUsedSize());
      assertEquals(0, bucketCache.backingMap.size());
    } finally {
      if (bucketCache != null) {
        bucketCache.shutdown();
      }
    }
  }

  /**
   * Test whether BucketCache is started normally after modifying the cache file. Start BucketCache
   * and add some blocks, then shutdown BucketCache and persist cache to file. Restart BucketCache
   * after modify cache file's data, and it can't restore cache from file, the cache file and
   * persistence file would be deleted before BucketCache start normally.
   * @throws Exception the exception
   */
  @Test
  public void testModifiedBucketCacheFileData() throws Exception {
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    Path testDir = TEST_UTIL.getDataTestDir();
    TEST_UTIL.getTestFileSystem().mkdirs(testDir);

    Configuration conf = HBaseConfiguration.create();
    // Disables the persister thread by setting its interval to MAX_VALUE
    conf.setLong(BUCKETCACHE_PERSIST_INTERVAL_KEY, Long.MAX_VALUE);
    BucketCache bucketCache = null;
    try {
      bucketCache = new BucketCache("file:" + testDir + "/bucket.cache", capacitySize,
        constructedBlockSize, constructedBlockSizes, writeThreads, writerQLen,
        testDir + "/bucket.persistence" + name.getMethodName(),
        DEFAULT_ERROR_TOLERATION_DURATION, conf);
      assertTrue(bucketCache.waitForCacheInitialization(10000));
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
      bucketCache = new BucketCache("file:" + testDir + "/bucket.cache", capacitySize,
        constructedBlockSize, constructedBlockSizes, writeThreads, writerQLen,
        testDir + "/bucket.persistence" + name.getMethodName(),
        DEFAULT_ERROR_TOLERATION_DURATION, conf);
      assertTrue(bucketCache.waitForCacheInitialization(10000));
      waitPersistentCacheValidation(conf, bucketCache);
      assertEquals(0, bucketCache.getAllocator().getUsedSize());
      assertEquals(0, bucketCache.backingMap.size());
    } finally {
      if (bucketCache != null) {
        bucketCache.shutdown();
      }
    }
    TEST_UTIL.cleanupTestDir();
  }

  /**
   * Test whether BucketCache is started normally after modifying the cache file's last modified
   * time. First Start BucketCache and add some blocks, then shutdown BucketCache and persist cache
   * to file. Then Restart BucketCache after modify cache file's last modified time. HBASE-XXXX has
   * modified persistence cache such that now we store extra 8 bytes at the end of each block in the
   * cache, representing the nanosecond time the block has been cached. So in the event the cache
   * file has failed checksum verification during loading time, we go through all the cached blocks
   * in the cache map and validate the cached time long between what is in the map and the cache
   * file. If that check fails, we pull the cache key entry out of the map. Since in this test we
   * are only modifying the access time to induce a checksum error, the cache file content is still
   * valid and the extra verification should validate that all cache keys in the map are still
   * recoverable from the cache.
   * @throws Exception the exception
   */
  @Test
  public void testModifiedBucketCacheFileTime() throws Exception {
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    Path testDir = TEST_UTIL.getDataTestDir();
    TEST_UTIL.getTestFileSystem().mkdirs(testDir);
    Configuration conf = HBaseConfiguration.create();
    // Disables the persister thread by setting its interval to MAX_VALUE
    conf.setLong(BUCKETCACHE_PERSIST_INTERVAL_KEY, Long.MAX_VALUE);
    BucketCache bucketCache = null;
    try {
      bucketCache = new BucketCache("file:" + testDir + "/bucket.cache", capacitySize,
        constructedBlockSize, constructedBlockSizes, writeThreads, writerQLen,
        testDir + "/bucket.persistence" + name.getMethodName(),
        DEFAULT_ERROR_TOLERATION_DURATION, conf);
      assertTrue(bucketCache.waitForCacheInitialization(10000));
      long usedSize = bucketCache.getAllocator().getUsedSize();
      assertEquals(0, usedSize);

      Pair<String, Long> myPair = new Pair<>();

      CacheTestUtils.HFileBlockPair[] blocks =
        CacheTestUtils.generateHFileBlocks(constructedBlockSize, 1);
      // Add blocks
      for (CacheTestUtils.HFileBlockPair block : blocks) {
        cacheAndWaitUntilFlushedToBucket(bucketCache, block.getBlockName(), block.getBlock());
      }
      usedSize = bucketCache.getAllocator().getUsedSize();
      assertNotEquals(0, usedSize);
      long blockCount = bucketCache.backingMap.size();
      assertNotEquals(0, blockCount);
      // persist cache to file
      bucketCache.shutdown();

      // modified bucket cache file LastModifiedTime
      final java.nio.file.Path file =
        FileSystems.getDefault().getPath(testDir.toString(), "bucket.cache");
      Files.setLastModifiedTime(file, FileTime.from(Instant.now().plusMillis(1_000)));
      // can't restore cache from file
      bucketCache = new BucketCache("file:" + testDir + "/bucket.cache", capacitySize,
        constructedBlockSize, constructedBlockSizes, writeThreads, writerQLen,
        testDir + "/bucket.persistence" + name.getMethodName(),
        DEFAULT_ERROR_TOLERATION_DURATION, conf);
      assertTrue(bucketCache.waitForCacheInitialization(10000));
      waitPersistentCacheValidation(conf, bucketCache);
      assertEquals(usedSize, bucketCache.getAllocator().getUsedSize());
      assertEquals(blockCount, bucketCache.backingMap.size());
    } finally {
      if (bucketCache != null) {
        bucketCache.shutdown();
      }
    }
    TEST_UTIL.cleanupTestDir();
  }

  /**
   * When using persistent bucket cache, there may be crashes between persisting the backing map and
   * syncing new blocks to the cache file itself, leading to an inconsistent state between the cache
   * keys and the cached data. This is to make sure the cache keys are updated accordingly, and the
   * keys that are still valid do succeed in retrieve related block data from the cache without any
   * corruption.
   * @throws Exception the exception
   */
  @Test
  public void testBucketCacheRecovery() throws Exception {
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    Path testDir = TEST_UTIL.getDataTestDir();
    TEST_UTIL.getTestFileSystem().mkdirs(testDir);
    Configuration conf = HBaseConfiguration.create();
    // Disables the persister thread by setting its interval to MAX_VALUE
    conf.setLong(BUCKETCACHE_PERSIST_INTERVAL_KEY, Long.MAX_VALUE);
    String mapFileName = testDir + "/bucket.persistence"
      + EnvironmentEdgeManager.currentTime() + name.getMethodName();
    BucketCache bucketCache = null;
    BucketCache newBucketCache = null;
    try {
      bucketCache = new BucketCache("file:" + testDir + "/bucket.cache", capacitySize,
        constructedBlockSize, constructedBlockSizes, writeThreads, writerQLen, mapFileName,
        DEFAULT_ERROR_TOLERATION_DURATION, conf);
      assertTrue(bucketCache.waitForCacheInitialization(10000));

      CacheTestUtils.HFileBlockPair[] blocks =
        CacheTestUtils.generateHFileBlocks(constructedBlockSize, 4);
      String[] names = CacheTestUtils.getHFileNames(blocks);
      // Add three blocks
      cacheAndWaitUntilFlushedToBucket(bucketCache, blocks[0].getBlockName(), blocks[0].getBlock());
      cacheAndWaitUntilFlushedToBucket(bucketCache, blocks[1].getBlockName(), blocks[1].getBlock());
      cacheAndWaitUntilFlushedToBucket(bucketCache, blocks[2].getBlockName(), blocks[2].getBlock());
      // saves the current state
      bucketCache.persistToFile();
      // evicts first block
      bucketCache.evictBlock(blocks[0].getBlockName());

      // now adds a fourth block to bucket cache
      cacheAndWaitUntilFlushedToBucket(bucketCache, blocks[3].getBlockName(), blocks[3].getBlock());
      // Creates new bucket cache instance without persisting to file after evicting first block
      // and caching fourth block. So the bucket cache file has only the last three blocks,
      // but backing map (containing cache keys) was persisted when first three blocks
      // were in the cache. So the state on this recovery is:
      // - Backing map: [block0, block1, block2]
      // - Cache: [block1, block2, block3]
      // Therefore, this bucket cache would be able to recover only block1 and block2.
      newBucketCache = new BucketCache("file:" + testDir + "/bucket.cache", capacitySize,
        constructedBlockSize, constructedBlockSizes, writeThreads, writerQLen, mapFileName,
        DEFAULT_ERROR_TOLERATION_DURATION, conf);
      assertTrue(newBucketCache.waitForCacheInitialization(10000));
      BlockCacheKey[] newKeys = CacheTestUtils.regenerateKeys(blocks, names);
      assertNull(newBucketCache.getBlock(newKeys[0], false, false, false));
      assertEquals(blocks[1].getBlock(), newBucketCache.getBlock(newKeys[1], false, false, false));
      assertEquals(blocks[2].getBlock(), newBucketCache.getBlock(newKeys[2], false, false, false));
      assertNull(newBucketCache.getBlock(newKeys[3], false, false, false));
      assertEquals(2, newBucketCache.backingMap.size());
    } finally {
      if (newBucketCache == null && bucketCache != null) {
        bucketCache.shutdown();
      }
      if (newBucketCache != null) {
        newBucketCache.shutdown();
      }
      TEST_UTIL.cleanupTestDir();
    }
  }

  @Test
  public void testSingleChunk() throws Exception {
    testChunkedBackingMapRecovery(5, 5);
  }

  @Test
  public void testCompletelyFilledChunks() throws Exception {
    // Test where the all the chunks are complete with chunkSize entries
    testChunkedBackingMapRecovery(5, 10);
  }

  @Test
  public void testPartiallyFilledChunks() throws Exception {
    // Test where the last chunk is not completely filled.
    testChunkedBackingMapRecovery(5, 13);
  }

  private void testChunkedBackingMapRecovery(int chunkSize, int numBlocks) throws Exception {
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    Path testDir = TEST_UTIL.getDataTestDir();
    TEST_UTIL.getTestFileSystem().mkdirs(testDir);
    Configuration conf = HBaseConfiguration.create();
    conf.setLong(BACKING_MAP_PERSISTENCE_CHUNK_SIZE, chunkSize);

    String mapFileName = testDir + "/bucket.persistence"
      + EnvironmentEdgeManager.currentTime() + name.getMethodName();
    BucketCache bucketCache = null;
    BucketCache newBucketCache = null;
    try {
      bucketCache = new BucketCache("file:" + testDir + "/bucket.cache", capacitySize,
        constructedBlockSize, constructedBlockSizes, writeThreads, writerQLen, mapFileName,
        DEFAULT_ERROR_TOLERATION_DURATION, conf);
      assertTrue(bucketCache.waitForCacheInitialization(10000));

      CacheTestUtils.HFileBlockPair[] blocks =
        CacheTestUtils.generateHFileBlocks(constructedBlockSize, numBlocks);
      String[] names = CacheTestUtils.getHFileNames(blocks);

      for (int i = 0; i < numBlocks; i++) {
        cacheAndWaitUntilFlushedToBucket(bucketCache, blocks[i].getBlockName(),
          blocks[i].getBlock());
      }
      // saves the current state
      bucketCache.persistToFile();
      // Create a new bucket which reads from persistence file.
      newBucketCache = new BucketCache("file:" + testDir + "/bucket.cache", capacitySize,
        constructedBlockSize, constructedBlockSizes, writeThreads, writerQLen, mapFileName,
        DEFAULT_ERROR_TOLERATION_DURATION, conf);
      assertTrue(newBucketCache.waitForCacheInitialization(10000));
      assertEquals(numBlocks, newBucketCache.backingMap.size());
      BlockCacheKey[] newKeys = CacheTestUtils.regenerateKeys(blocks, names);
      for (int i = 0; i < numBlocks; i++) {
        assertEquals(blocks[i].getBlock(),
          newBucketCache.getBlock(newKeys[i], false, false, false));
      }
    } finally {
      if (bucketCache != null) {
        bucketCache.shutdown();
      }
      if (newBucketCache != null) {
        try {
          newBucketCache.shutdown();
        } catch (NullPointerException e) {
          // We need to enforce these two shutdown to make sure we don't leave "orphan" persister
          // threads running while the unit test JVM instance is up.
          // This would lead to a NPE because of the StringPoolCleanup in bucketCache.shutdown
          //  but that's fine because we don't have more than one bucket cache instance in real life
          //  and here we passed the point where we stop background threads inside shutdown.
        }
      }
      TEST_UTIL.cleanupTestDir();
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

  private void waitPersistentCacheValidation(Configuration config, final BucketCache bucketCache) {
    Waiter.waitFor(config, 5000, () -> bucketCache.getBackingMapValidated().get());
  }
}
