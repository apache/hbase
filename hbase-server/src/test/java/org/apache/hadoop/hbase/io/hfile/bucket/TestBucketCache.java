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
import static org.apache.hadoop.hbase.io.hfile.bucket.BucketCache.ACCEPT_FACTOR_CONFIG_NAME;
import static org.apache.hadoop.hbase.io.hfile.bucket.BucketCache.BACKING_MAP_PERSISTENCE_CHUNK_SIZE;
import static org.apache.hadoop.hbase.io.hfile.bucket.BucketCache.BLOCK_ORPHAN_GRACE_PERIOD;
import static org.apache.hadoop.hbase.io.hfile.bucket.BucketCache.DEFAULT_ERROR_TOLERATION_DURATION;
import static org.apache.hadoop.hbase.io.hfile.bucket.BucketCache.DEFAULT_MIN_FACTOR;
import static org.apache.hadoop.hbase.io.hfile.bucket.BucketCache.DEFAULT_SINGLE_FACTOR;
import static org.apache.hadoop.hbase.io.hfile.bucket.BucketCache.EXTRA_FREE_FACTOR_CONFIG_NAME;
import static org.apache.hadoop.hbase.io.hfile.bucket.BucketCache.MEMORY_FACTOR_CONFIG_NAME;
import static org.apache.hadoop.hbase.io.hfile.bucket.BucketCache.MIN_FACTOR_CONFIG_NAME;
import static org.apache.hadoop.hbase.io.hfile.bucket.BucketCache.MULTI_FACTOR_CONFIG_NAME;
import static org.apache.hadoop.hbase.io.hfile.bucket.BucketCache.QUEUE_ADDITION_WAIT_TIME;
import static org.apache.hadoop.hbase.io.hfile.bucket.BucketCache.SINGLE_FACTOR_CONFIG_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.CacheTestUtils;
import org.apache.hadoop.hbase.io.hfile.CacheTestUtils.HFileBlockPair;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketAllocator.BucketSizeInfo;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketAllocator.IndexStatistics;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache.RAMCache;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache.RAMQueueEntry;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

/**
 * Basic test of BucketCache.Puts and gets.
 * <p>
 * Tests will ensure that blocks' data correctness under several threads concurrency
 */
@RunWith(Parameterized.class)
@Category({ IOTests.class, LargeTests.class })
public class TestBucketCache {

  private static final Logger LOG = LoggerFactory.getLogger(TestBucketCache.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBucketCache.class);

  @Parameterized.Parameters(name = "{index}: blockSize={0}, bucketSizes={1}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][] { { 8192, null }, // TODO: why is 8k the default blocksize
                                                          // for these tests?
      { 16 * 1024,
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
  private String ioEngineName = "offheap";

  private static final HBaseTestingUtil HBASE_TESTING_UTILITY = new HBaseTestingUtil();

  private static class MockedBucketCache extends BucketCache {

    public MockedBucketCache(String ioEngineName, long capacity, int blockSize, int[] bucketSizes,
      int writerThreads, int writerQLen, String persistencePath) throws IOException {
      super(ioEngineName, capacity, blockSize, bucketSizes, writerThreads, writerQLen,
        persistencePath);
    }

    @Override
    public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf, boolean inMemory) {
      super.cacheBlock(cacheKey, buf, inMemory);
    }

    @Override
    public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf) {
      super.cacheBlock(cacheKey, buf);
    }
  }

  @Before
  public void setup() throws IOException {
    cache = new MockedBucketCache(ioEngineName, capacitySize, constructedBlockSize,
      constructedBlockSizes, writeThreads, writerQLen, null);
  }

  @After
  public void tearDown() {
    cache.shutdown();
  }

  /**
   * Test Utility to create test dir and return name
   * @return return name of created dir
   * @throws IOException throws IOException
   */
  private Path createAndGetTestDir() throws IOException {
    final Path testDir = HBASE_TESTING_UTILITY.getDataTestDir();
    HBASE_TESTING_UTILITY.getTestFileSystem().mkdirs(testDir);
    return testDir;
  }

  /**
   * Return a random element from {@code a}.
   */
  private static <T> T randFrom(List<T> a) {
    return a.get(ThreadLocalRandom.current().nextInt(a.size()));
  }

  @Test
  public void testBucketAllocator() throws BucketAllocatorException {
    BucketAllocator mAllocator = cache.getAllocator();
    /*
     * Test the allocator first
     */
    final List<Integer> BLOCKSIZES = Arrays.asList(4 * 1024, 8 * 1024, 64 * 1024, 96 * 1024);

    boolean full = false;
    ArrayList<Pair<Long, Integer>> allocations = new ArrayList<>();
    // Fill the allocated extents by choosing a random blocksize. Continues selecting blocks until
    // the cache is completely filled.
    List<Integer> tmp = new ArrayList<>(BLOCKSIZES);
    while (!full) {
      Integer blockSize = null;
      try {
        blockSize = randFrom(tmp);
        allocations.add(new Pair<>(mAllocator.allocateBlock(blockSize), blockSize));
      } catch (CacheFullException cfe) {
        tmp.remove(blockSize);
        if (tmp.isEmpty()) full = true;
      }
    }

    for (Integer blockSize : BLOCKSIZES) {
      BucketSizeInfo bucketSizeInfo = mAllocator.roundUpToBucketSizeInfo(blockSize);
      IndexStatistics indexStatistics = bucketSizeInfo.statistics();
      assertEquals("unexpected freeCount for " + bucketSizeInfo, 0, indexStatistics.freeCount());

      // we know the block sizes above are multiples of 1024, but default bucket sizes give an
      // additional 1024 on top of that so this counts towards fragmentation in our test
      // real life may have worse fragmentation because blocks may not be perfectly sized to block
      // size, given encoding/compression and large rows
      assertEquals(1024 * indexStatistics.totalCount(), indexStatistics.fragmentationBytes());
    }

    mAllocator.logDebugStatistics();

    for (Pair<Long, Integer> allocation : allocations) {
      assertEquals(mAllocator.sizeOfAllocation(allocation.getFirst()),
        mAllocator.freeBlock(allocation.getFirst(), allocation.getSecond()));
    }
    assertEquals(0, mAllocator.getUsedSize());
  }

  @Test
  public void testCacheSimple() throws Exception {
    CacheTestUtils.testCacheSimple(cache, BLOCK_SIZE, NUM_QUERIES);
  }

  @Test
  public void testCacheMultiThreadedSingleKey() throws Exception {
    CacheTestUtils.hammerSingleKey(cache, 2 * NUM_THREADS, 2 * NUM_QUERIES);
  }

  @Test
  public void testHeapSizeChanges() throws Exception {
    cache.stopWriterThreads();
    CacheTestUtils.testHeapSizeChanges(cache, BLOCK_SIZE);
  }

  public static void waitUntilFlushedToBucket(BucketCache cache, BlockCacheKey cacheKey)
    throws InterruptedException {
    Waiter.waitFor(HBaseConfiguration.create(), 10000,
      () -> (cache.backingMap.containsKey(cacheKey) && !cache.ramCache.containsKey(cacheKey)));
  }

  public static void waitUntilAllFlushedToBucket(BucketCache cache) throws InterruptedException {
    while (!cache.ramCache.isEmpty()) {
      Thread.sleep(100);
    }
    Thread.sleep(1000);
  }

  // BucketCache.cacheBlock is async, it first adds block to ramCache and writeQueue, then writer
  // threads will flush it to the bucket and put reference entry in backingMap.
  private void cacheAndWaitUntilFlushedToBucket(BucketCache cache, BlockCacheKey cacheKey,
    Cacheable block, boolean waitWhenCache) throws InterruptedException {
    cache.cacheBlock(cacheKey, block, false, waitWhenCache);
    waitUntilFlushedToBucket(cache, cacheKey);
  }

  @Test
  public void testMemoryLeak() throws Exception {
    final BlockCacheKey cacheKey = new BlockCacheKey("dummy", 1L);
    cacheAndWaitUntilFlushedToBucket(cache, cacheKey,
      new CacheTestUtils.ByteArrayCacheable(new byte[10]), true);
    long lockId = cache.backingMap.get(cacheKey).offset();
    ReentrantReadWriteLock lock = cache.offsetLock.getLock(lockId);
    lock.writeLock().lock();
    Thread evictThread = new Thread("evict-block") {
      @Override
      public void run() {
        cache.evictBlock(cacheKey);
      }
    };
    evictThread.start();
    cache.offsetLock.waitForWaiters(lockId, 1);
    cache.blockEvicted(cacheKey, cache.backingMap.remove(cacheKey), true, true);
    assertEquals(0, cache.getBlockCount());
    cacheAndWaitUntilFlushedToBucket(cache, cacheKey,
      new CacheTestUtils.ByteArrayCacheable(new byte[10]), true);
    assertEquals(1, cache.getBlockCount());
    lock.writeLock().unlock();
    evictThread.join();
    /**
     * <pre>
     * The asserts here before HBASE-21957 are:
     * assertEquals(1L, cache.getBlockCount());
     * assertTrue(cache.getCurrentSize() > 0L);
     * assertTrue("We should have a block!", cache.iterator().hasNext());
     *
     * The asserts here after HBASE-21957 are:
     * assertEquals(0, cache.getBlockCount());
     * assertEquals(cache.getCurrentSize(), 0L);
     *
     * I think the asserts before HBASE-21957 is more reasonable,because
     * {@link BucketCache#evictBlock} should only evict the {@link BucketEntry}
     * it had seen, and newly added Block after the {@link BucketEntry}
     * it had seen should not be evicted.
     * </pre>
     */
    assertEquals(1L, cache.getBlockCount());
    assertTrue(cache.getCurrentSize() > 0L);
    assertTrue("We should have a block!", cache.iterator().hasNext());
  }

  @Test
  public void testRetrieveFromFile() throws Exception {
    Path testDir = createAndGetTestDir();
    String ioEngineName = "file:" + testDir + "/bucket.cache";
    testRetrievalUtils(testDir, ioEngineName);
    int[] smallBucketSizes = new int[] { 3 * 1024, 5 * 1024 };
    String persistencePath = testDir + "/bucket.persistence";
    BucketCache bucketCache = null;
    try {
      bucketCache = new BucketCache(ioEngineName, capacitySize, constructedBlockSize,
        smallBucketSizes, writeThreads, writerQLen, persistencePath);
      assertTrue(bucketCache.waitForCacheInitialization(10000));
      assertFalse(new File(persistencePath).exists());
      assertEquals(0, bucketCache.getAllocator().getUsedSize());
      assertEquals(0, bucketCache.backingMap.size());
    } finally {
      bucketCache.shutdown();
      HBASE_TESTING_UTILITY.cleanupTestDir();
    }
  }

  @Test
  public void testRetrieveFromMMap() throws Exception {
    final Path testDir = createAndGetTestDir();
    final String ioEngineName = "mmap:" + testDir + "/bucket.cache";
    testRetrievalUtils(testDir, ioEngineName);
  }

  @Test
  public void testRetrieveFromPMem() throws Exception {
    final Path testDir = createAndGetTestDir();
    final String ioEngineName = "pmem:" + testDir + "/bucket.cache";
    testRetrievalUtils(testDir, ioEngineName);
    int[] smallBucketSizes = new int[] { 3 * 1024, 5 * 1024 };
    String persistencePath = testDir + "/bucket.persistence" + EnvironmentEdgeManager.currentTime();
    BucketCache bucketCache = null;
    try {
      bucketCache = new BucketCache(ioEngineName, capacitySize, constructedBlockSize,
        smallBucketSizes, writeThreads, writerQLen, persistencePath);
      assertTrue(bucketCache.waitForCacheInitialization(10000));
      assertFalse(new File(persistencePath).exists());
      assertEquals(0, bucketCache.getAllocator().getUsedSize());
      assertEquals(0, bucketCache.backingMap.size());
    } finally {
      bucketCache.shutdown();
      HBASE_TESTING_UTILITY.cleanupTestDir();
    }
  }

  private void testRetrievalUtils(Path testDir, String ioEngineName)
    throws IOException, InterruptedException {
    final String persistencePath =
      testDir + "/bucket.persistence" + EnvironmentEdgeManager.currentTime();
    BucketCache bucketCache = null;
    try {
      bucketCache = new BucketCache(ioEngineName, capacitySize, constructedBlockSize,
        constructedBlockSizes, writeThreads, writerQLen, persistencePath);
      assertTrue(bucketCache.waitForCacheInitialization(10000));
      long usedSize = bucketCache.getAllocator().getUsedSize();
      assertEquals(0, usedSize);
      HFileBlockPair[] blocks = CacheTestUtils.generateHFileBlocks(constructedBlockSize, 1);
      for (HFileBlockPair block : blocks) {
        bucketCache.cacheBlock(block.getBlockName(), block.getBlock());
      }
      for (HFileBlockPair block : blocks) {
        cacheAndWaitUntilFlushedToBucket(bucketCache, block.getBlockName(), block.getBlock(),
          false);
      }
      usedSize = bucketCache.getAllocator().getUsedSize();
      assertNotEquals(0, usedSize);
      bucketCache.shutdown();
      assertTrue(new File(persistencePath).exists());
      bucketCache = new BucketCache(ioEngineName, capacitySize, constructedBlockSize,
        constructedBlockSizes, writeThreads, writerQLen, persistencePath);
      assertTrue(bucketCache.waitForCacheInitialization(10000));

      assertEquals(usedSize, bucketCache.getAllocator().getUsedSize());
    } finally {
      if (bucketCache != null) {
        bucketCache.shutdown();
      }
    }
    assertTrue(new File(persistencePath).exists());
  }

  @Test
  public void testRetrieveUnsupportedIOE() throws Exception {
    try {
      final Path testDir = createAndGetTestDir();
      final String ioEngineName = testDir + "/bucket.cache";
      testRetrievalUtils(testDir, ioEngineName);
      Assert.fail("Should have thrown IllegalArgumentException because of unsupported IOEngine!!");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Don't understand io engine name for cache- prefix with file:, "
        + "files:, mmap: or offheap", e.getMessage());
    }
  }

  @Test
  public void testRetrieveFromMultipleFiles() throws Exception {
    final Path testDirInitial = createAndGetTestDir();
    final Path newTestDir = new HBaseTestingUtil().getDataTestDir();
    HBASE_TESTING_UTILITY.getTestFileSystem().mkdirs(newTestDir);
    String ioEngineName =
      new StringBuilder("files:").append(testDirInitial).append("/bucket1.cache")
        .append(FileIOEngine.FILE_DELIMITER).append(newTestDir).append("/bucket2.cache").toString();
    testRetrievalUtils(testDirInitial, ioEngineName);
    int[] smallBucketSizes = new int[] { 3 * 1024, 5 * 1024 };
    String persistencePath = testDirInitial + "/bucket.persistence";
    BucketCache bucketCache = null;
    try {
      bucketCache = new BucketCache(ioEngineName, capacitySize, constructedBlockSize,
        smallBucketSizes, writeThreads, writerQLen, persistencePath);
      assertTrue(bucketCache.waitForCacheInitialization(10000));
      assertFalse(new File(persistencePath).exists());
      assertEquals(0, bucketCache.getAllocator().getUsedSize());
      assertEquals(0, bucketCache.backingMap.size());
    } finally {
      bucketCache.shutdown();
      HBASE_TESTING_UTILITY.cleanupTestDir();
    }
  }

  @Test
  public void testRetrieveFromFileWithoutPersistence() throws Exception {
    BucketCache bucketCache = new BucketCache(ioEngineName, capacitySize, constructedBlockSize,
      constructedBlockSizes, writeThreads, writerQLen, null);
    assertTrue(bucketCache.waitForCacheInitialization(10000));
    try {
      final Path testDir = createAndGetTestDir();
      String ioEngineName = "file:" + testDir + "/bucket.cache";
      long usedSize = bucketCache.getAllocator().getUsedSize();
      assertEquals(0, usedSize);
      HFileBlockPair[] blocks = CacheTestUtils.generateHFileBlocks(constructedBlockSize, 1);
      for (HFileBlockPair block : blocks) {
        bucketCache.cacheBlock(block.getBlockName(), block.getBlock());
      }
      for (HFileBlockPair block : blocks) {
        cacheAndWaitUntilFlushedToBucket(bucketCache, block.getBlockName(), block.getBlock(),
          false);
      }
      usedSize = bucketCache.getAllocator().getUsedSize();
      assertNotEquals(0, usedSize);
      bucketCache.shutdown();
      bucketCache = new BucketCache(ioEngineName, capacitySize, constructedBlockSize,
        constructedBlockSizes, writeThreads, writerQLen, null);
      assertTrue(bucketCache.waitForCacheInitialization(10000));
      assertEquals(0, bucketCache.getAllocator().getUsedSize());
    } finally {
      bucketCache.shutdown();
      HBASE_TESTING_UTILITY.cleanupTestDir();
    }
  }

  @Test
  public void testBucketAllocatorLargeBuckets() throws BucketAllocatorException {
    long availableSpace = 20 * 1024L * 1024 * 1024;
    int[] bucketSizes = new int[] { 1024, 1024 * 1024, 1024 * 1024 * 1024 };
    BucketAllocator allocator = new BucketAllocator(availableSpace, bucketSizes);
    assertTrue(allocator.getBuckets().length > 0);
  }

  @Test
  public void testGetPartitionSize() throws IOException {
    // Test default values
    validateGetPartitionSize(cache, DEFAULT_SINGLE_FACTOR, DEFAULT_MIN_FACTOR);

    Configuration conf = HBaseConfiguration.create();
    conf.setFloat(MIN_FACTOR_CONFIG_NAME, 0.5f);
    conf.setFloat(SINGLE_FACTOR_CONFIG_NAME, 0.1f);
    conf.setFloat(MULTI_FACTOR_CONFIG_NAME, 0.7f);
    conf.setFloat(MEMORY_FACTOR_CONFIG_NAME, 0.2f);

    BucketCache cache = new BucketCache(ioEngineName, capacitySize, constructedBlockSize,
      constructedBlockSizes, writeThreads, writerQLen, null, 100, conf);
    assertTrue(cache.waitForCacheInitialization(10000));

    validateGetPartitionSize(cache, 0.1f, 0.5f);
    validateGetPartitionSize(cache, 0.7f, 0.5f);
    validateGetPartitionSize(cache, 0.2f, 0.5f);
  }

  @Test
  public void testCacheSizeCapacity() throws IOException {
    // Test cache capacity (capacity / blockSize) < Integer.MAX_VALUE
    validateGetPartitionSize(cache, DEFAULT_SINGLE_FACTOR, DEFAULT_MIN_FACTOR);
    Configuration conf = HBaseConfiguration.create();
    conf.setFloat(BucketCache.MIN_FACTOR_CONFIG_NAME, 0.5f);
    conf.setFloat(SINGLE_FACTOR_CONFIG_NAME, 0.1f);
    conf.setFloat(MULTI_FACTOR_CONFIG_NAME, 0.7f);
    conf.setFloat(MEMORY_FACTOR_CONFIG_NAME, 0.2f);
    try {
      new BucketCache(ioEngineName, Long.MAX_VALUE, 1, constructedBlockSizes, writeThreads,
        writerQLen, null, 100, conf);
      Assert.fail("Should have thrown IllegalArgumentException because of large cache capacity!");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Cache capacity is too large, only support 32TB now", e.getMessage());
    }
  }

  @Test
  public void testValidBucketCacheConfigs() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.setFloat(ACCEPT_FACTOR_CONFIG_NAME, 0.9f);
    conf.setFloat(MIN_FACTOR_CONFIG_NAME, 0.5f);
    conf.setFloat(EXTRA_FREE_FACTOR_CONFIG_NAME, 0.5f);
    conf.setFloat(SINGLE_FACTOR_CONFIG_NAME, 0.1f);
    conf.setFloat(MULTI_FACTOR_CONFIG_NAME, 0.7f);
    conf.setFloat(MEMORY_FACTOR_CONFIG_NAME, 0.2f);

    BucketCache cache = new BucketCache(ioEngineName, capacitySize, constructedBlockSize,
      constructedBlockSizes, writeThreads, writerQLen, null, 100, conf);
    assertTrue(cache.waitForCacheInitialization(10000));

    assertEquals(ACCEPT_FACTOR_CONFIG_NAME + " failed to propagate.", 0.9f,
      cache.getAcceptableFactor(), 0);
    assertEquals(MIN_FACTOR_CONFIG_NAME + " failed to propagate.", 0.5f, cache.getMinFactor(), 0);
    assertEquals(EXTRA_FREE_FACTOR_CONFIG_NAME + " failed to propagate.", 0.5f,
      cache.getExtraFreeFactor(), 0);
    assertEquals(SINGLE_FACTOR_CONFIG_NAME + " failed to propagate.", 0.1f, cache.getSingleFactor(),
      0);
    assertEquals(MULTI_FACTOR_CONFIG_NAME + " failed to propagate.", 0.7f, cache.getMultiFactor(),
      0);
    assertEquals(MEMORY_FACTOR_CONFIG_NAME + " failed to propagate.", 0.2f, cache.getMemoryFactor(),
      0);
  }

  @Test
  public void testInvalidAcceptFactorConfig() throws IOException {
    float[] configValues = { -1f, 0.2f, 0.86f, 1.05f };
    boolean[] expectedOutcomes = { false, false, true, false };
    Map<String, float[]> configMappings = ImmutableMap.of(ACCEPT_FACTOR_CONFIG_NAME, configValues);
    Configuration conf = HBaseConfiguration.create();
    checkConfigValues(conf, configMappings, expectedOutcomes);
  }

  @Test
  public void testInvalidMinFactorConfig() throws IOException {
    float[] configValues = { -1f, 0f, 0.96f, 1.05f };
    // throws due to <0, in expected range, minFactor > acceptableFactor, > 1.0
    boolean[] expectedOutcomes = { false, true, false, false };
    Map<String, float[]> configMappings = ImmutableMap.of(MIN_FACTOR_CONFIG_NAME, configValues);
    Configuration conf = HBaseConfiguration.create();
    checkConfigValues(conf, configMappings, expectedOutcomes);
  }

  @Test
  public void testInvalidExtraFreeFactorConfig() throws IOException {
    float[] configValues = { -1f, 0f, 0.2f, 1.05f };
    // throws due to <0, in expected range, in expected range, config can be > 1.0
    boolean[] expectedOutcomes = { false, true, true, true };
    Map<String, float[]> configMappings =
      ImmutableMap.of(EXTRA_FREE_FACTOR_CONFIG_NAME, configValues);
    Configuration conf = HBaseConfiguration.create();
    checkConfigValues(conf, configMappings, expectedOutcomes);
  }

  @Test
  public void testInvalidCacheSplitFactorConfig() throws IOException {
    float[] singleFactorConfigValues = { 0.2f, 0f, -0.2f, 1f };
    float[] multiFactorConfigValues = { 0.4f, 0f, 1f, .05f };
    float[] memoryFactorConfigValues = { 0.4f, 0f, 0.2f, .5f };
    // All configs add up to 1.0 and are between 0 and 1.0, configs don't add to 1.0, configs can't
    // be negative, configs don't add to 1.0
    boolean[] expectedOutcomes = { true, false, false, false };
    Map<String,
      float[]> configMappings = ImmutableMap.of(SINGLE_FACTOR_CONFIG_NAME, singleFactorConfigValues,
        MULTI_FACTOR_CONFIG_NAME, multiFactorConfigValues, MEMORY_FACTOR_CONFIG_NAME,
        memoryFactorConfigValues);
    Configuration conf = HBaseConfiguration.create();
    checkConfigValues(conf, configMappings, expectedOutcomes);
  }

  private void checkConfigValues(Configuration conf, Map<String, float[]> configMap,
    boolean[] expectSuccess) throws IOException {
    Set<String> configNames = configMap.keySet();
    for (int i = 0; i < expectSuccess.length; i++) {
      try {
        for (String configName : configNames) {
          conf.setFloat(configName, configMap.get(configName)[i]);
        }
        BucketCache cache = new BucketCache(ioEngineName, capacitySize, constructedBlockSize,
          constructedBlockSizes, writeThreads, writerQLen, null, 100, conf);
        assertTrue(cache.waitForCacheInitialization(10000));
        assertTrue("Created BucketCache and expected it to succeed: " + expectSuccess[i]
          + ", but it actually was: " + !expectSuccess[i], expectSuccess[i]);
      } catch (IllegalArgumentException e) {
        assertFalse("Created BucketCache and expected it to succeed: " + expectSuccess[i]
          + ", but it actually was: " + !expectSuccess[i], expectSuccess[i]);
      }
    }
  }

  private void validateGetPartitionSize(BucketCache bucketCache, float partitionFactor,
    float minFactor) {
    long expectedOutput =
      (long) Math.floor(bucketCache.getAllocator().getTotalSize() * partitionFactor * minFactor);
    assertEquals(expectedOutput, bucketCache.getPartitionSize(partitionFactor));
  }

  @Test
  public void testOffsetProducesPositiveOutput() {
    // This number is picked because it produces negative output if the values isn't ensured to be
    // positive. See HBASE-18757 for more information.
    long testValue = 549888460800L;
    BucketEntry bucketEntry = new BucketEntry(testValue, 10, 10, 10L, true, (entry) -> {
      return ByteBuffAllocator.NONE;
    }, ByteBuffAllocator.HEAP);
    assertEquals(testValue, bucketEntry.offset());
  }

  @Test
  public void testEvictionCount() throws InterruptedException {
    int size = 100;
    int length = HConstants.HFILEBLOCK_HEADER_SIZE + size;
    ByteBuffer buf1 = ByteBuffer.allocate(size), buf2 = ByteBuffer.allocate(size);
    HFileContext meta = new HFileContextBuilder().build();
    ByteBuffAllocator allocator = ByteBuffAllocator.HEAP;
    HFileBlock blockWithNextBlockMetadata = new HFileBlock(BlockType.DATA, size, size, -1,
      ByteBuff.wrap(buf1), HFileBlock.FILL_HEADER, -1, 52, -1, meta, allocator);
    HFileBlock blockWithoutNextBlockMetadata = new HFileBlock(BlockType.DATA, size, size, -1,
      ByteBuff.wrap(buf2), HFileBlock.FILL_HEADER, -1, -1, -1, meta, allocator);

    BlockCacheKey key = new BlockCacheKey("testEvictionCount", 0);
    ByteBuffer actualBuffer = ByteBuffer.allocate(length);
    ByteBuffer block1Buffer = ByteBuffer.allocate(length);
    ByteBuffer block2Buffer = ByteBuffer.allocate(length);
    blockWithNextBlockMetadata.serialize(block1Buffer, true);
    blockWithoutNextBlockMetadata.serialize(block2Buffer, true);

    // Add blockWithNextBlockMetadata, expect blockWithNextBlockMetadata back.
    CacheTestUtils.getBlockAndAssertEquals(cache, key, blockWithNextBlockMetadata, actualBuffer,
      block1Buffer);

    waitUntilFlushedToBucket(cache, key);

    assertEquals(0, cache.getStats().getEvictionCount());

    // evict call should return 1, but then eviction count be 0
    assertEquals(1, cache.evictBlocksByHfileName("testEvictionCount"));
    assertEquals(0, cache.getStats().getEvictionCount());

    // add back
    CacheTestUtils.getBlockAndAssertEquals(cache, key, blockWithNextBlockMetadata, actualBuffer,
      block1Buffer);
    waitUntilFlushedToBucket(cache, key);

    // should not increment
    assertTrue(cache.evictBlock(key));
    assertEquals(0, cache.getStats().getEvictionCount());

    // add back
    CacheTestUtils.getBlockAndAssertEquals(cache, key, blockWithNextBlockMetadata, actualBuffer,
      block1Buffer);
    waitUntilFlushedToBucket(cache, key);

    // should finally increment eviction count
    cache.freeSpace("testing");
    assertEquals(1, cache.getStats().getEvictionCount());
  }

  @Test
  public void testCacheBlockNextBlockMetadataMissing() throws Exception {
    int size = 100;
    int length = HConstants.HFILEBLOCK_HEADER_SIZE + size;
    ByteBuffer buf1 = ByteBuffer.allocate(size), buf2 = ByteBuffer.allocate(size);
    HFileContext meta = new HFileContextBuilder().build();
    ByteBuffAllocator allocator = ByteBuffAllocator.HEAP;
    HFileBlock blockWithNextBlockMetadata = new HFileBlock(BlockType.DATA, size, size, -1,
      ByteBuff.wrap(buf1), HFileBlock.FILL_HEADER, -1, 52, -1, meta, allocator);
    HFileBlock blockWithoutNextBlockMetadata = new HFileBlock(BlockType.DATA, size, size, -1,
      ByteBuff.wrap(buf2), HFileBlock.FILL_HEADER, -1, -1, -1, meta, allocator);

    BlockCacheKey key = new BlockCacheKey("testCacheBlockNextBlockMetadataMissing", 0);
    ByteBuffer actualBuffer = ByteBuffer.allocate(length);
    ByteBuffer block1Buffer = ByteBuffer.allocate(length);
    ByteBuffer block2Buffer = ByteBuffer.allocate(length);
    blockWithNextBlockMetadata.serialize(block1Buffer, true);
    blockWithoutNextBlockMetadata.serialize(block2Buffer, true);

    // Add blockWithNextBlockMetadata, expect blockWithNextBlockMetadata back.
    CacheTestUtils.getBlockAndAssertEquals(cache, key, blockWithNextBlockMetadata, actualBuffer,
      block1Buffer);

    waitUntilFlushedToBucket(cache, key);
    assertNotNull(cache.backingMap.get(key));
    assertEquals(1, cache.backingMap.get(key).refCnt());
    assertEquals(1, blockWithNextBlockMetadata.getBufferReadOnly().refCnt());
    assertEquals(1, blockWithoutNextBlockMetadata.getBufferReadOnly().refCnt());

    // Add blockWithoutNextBlockMetada, expect blockWithNextBlockMetadata back.
    CacheTestUtils.getBlockAndAssertEquals(cache, key, blockWithoutNextBlockMetadata, actualBuffer,
      block1Buffer);
    assertEquals(1, blockWithNextBlockMetadata.getBufferReadOnly().refCnt());
    assertEquals(1, blockWithoutNextBlockMetadata.getBufferReadOnly().refCnt());
    assertEquals(1, cache.backingMap.get(key).refCnt());

    // Clear and add blockWithoutNextBlockMetadata
    assertTrue(cache.evictBlock(key));
    assertEquals(1, blockWithNextBlockMetadata.getBufferReadOnly().refCnt());
    assertEquals(1, blockWithoutNextBlockMetadata.getBufferReadOnly().refCnt());

    assertNull(cache.getBlock(key, false, false, false));
    CacheTestUtils.getBlockAndAssertEquals(cache, key, blockWithoutNextBlockMetadata, actualBuffer,
      block2Buffer);

    waitUntilFlushedToBucket(cache, key);
    assertEquals(1, blockWithNextBlockMetadata.getBufferReadOnly().refCnt());
    assertEquals(1, blockWithoutNextBlockMetadata.getBufferReadOnly().refCnt());

    // Add blockWithNextBlockMetadata, expect blockWithNextBlockMetadata to replace.
    CacheTestUtils.getBlockAndAssertEquals(cache, key, blockWithNextBlockMetadata, actualBuffer,
      block1Buffer);

    waitUntilFlushedToBucket(cache, key);
    assertEquals(1, blockWithNextBlockMetadata.getBufferReadOnly().refCnt());
    assertEquals(1, blockWithoutNextBlockMetadata.getBufferReadOnly().refCnt());
  }

  @Test
  public void testRAMCache() {
    int size = 100;
    int length = HConstants.HFILEBLOCK_HEADER_SIZE + size;
    byte[] byteArr = new byte[length];
    ByteBuffer buf = ByteBuffer.wrap(byteArr, 0, size);
    HFileContext meta = new HFileContextBuilder().build();

    RAMCache cache = new RAMCache();
    BlockCacheKey key1 = new BlockCacheKey("file-1", 1);
    BlockCacheKey key2 = new BlockCacheKey("file-2", 2);
    HFileBlock blk1 = new HFileBlock(BlockType.DATA, size, size, -1, ByteBuff.wrap(buf),
      HFileBlock.FILL_HEADER, -1, 52, -1, meta, ByteBuffAllocator.HEAP);
    HFileBlock blk2 = new HFileBlock(BlockType.DATA, size, size, -1, ByteBuff.wrap(buf),
      HFileBlock.FILL_HEADER, -1, -1, -1, meta, ByteBuffAllocator.HEAP);
    RAMQueueEntry re1 = new RAMQueueEntry(key1, blk1, 1, false, false, false);
    RAMQueueEntry re2 = new RAMQueueEntry(key1, blk2, 1, false, false, false);

    assertFalse(cache.containsKey(key1));
    assertNull(cache.putIfAbsent(key1, re1));
    assertEquals(2, ((HFileBlock) re1.getData()).getBufferReadOnly().refCnt());

    assertNotNull(cache.putIfAbsent(key1, re2));
    assertEquals(2, ((HFileBlock) re1.getData()).getBufferReadOnly().refCnt());
    assertEquals(1, ((HFileBlock) re2.getData()).getBufferReadOnly().refCnt());

    assertNull(cache.putIfAbsent(key2, re2));
    assertEquals(2, ((HFileBlock) re1.getData()).getBufferReadOnly().refCnt());
    assertEquals(2, ((HFileBlock) re2.getData()).getBufferReadOnly().refCnt());

    cache.remove(key1);
    assertEquals(1, ((HFileBlock) re1.getData()).getBufferReadOnly().refCnt());
    assertEquals(2, ((HFileBlock) re2.getData()).getBufferReadOnly().refCnt());

    cache.clear();
    assertEquals(1, ((HFileBlock) re1.getData()).getBufferReadOnly().refCnt());
    assertEquals(1, ((HFileBlock) re2.getData()).getBufferReadOnly().refCnt());
  }

  @Test
  public void testFreeBlockWhenIOEngineWriteFailure() throws IOException {
    // initialize an block.
    int size = 100, offset = 20;
    int length = HConstants.HFILEBLOCK_HEADER_SIZE + size;
    ByteBuffer buf = ByteBuffer.allocate(length);
    HFileContext meta = new HFileContextBuilder().build();
    HFileBlock block = new HFileBlock(BlockType.DATA, size, size, -1, ByteBuff.wrap(buf),
      HFileBlock.FILL_HEADER, offset, 52, -1, meta, ByteBuffAllocator.HEAP);

    // initialize an mocked ioengine.
    IOEngine ioEngine = Mockito.mock(IOEngine.class);
    when(ioEngine.usesSharedMemory()).thenReturn(false);
    // Mockito.doNothing().when(ioEngine).write(Mockito.any(ByteBuffer.class), Mockito.anyLong());
    Mockito.doThrow(RuntimeException.class).when(ioEngine).write(Mockito.any(ByteBuffer.class),
      Mockito.anyLong());
    Mockito.doThrow(RuntimeException.class).when(ioEngine).write(Mockito.any(ByteBuff.class),
      Mockito.anyLong());

    // create an bucket allocator.
    long availableSpace = 1024 * 1024 * 1024L;
    BucketAllocator allocator = new BucketAllocator(availableSpace, null);

    BlockCacheKey key = new BlockCacheKey("dummy", 1L);
    RAMQueueEntry re = new RAMQueueEntry(key, block, 1, true, false, false);

    Assert.assertEquals(0, allocator.getUsedSize());
    try {
      re.writeToCache(ioEngine, allocator, null, null,
        ByteBuffer.allocate(HFileBlock.BLOCK_METADATA_SPACE), Long.MAX_VALUE);
      Assert.fail();
    } catch (Exception e) {
    }
    Assert.assertEquals(0, allocator.getUsedSize());
  }

  /**
   * This test is for HBASE-26295, {@link BucketEntry} which is restored from a persistence file
   * could not be freed even if corresponding {@link HFileBlock} is evicted from
   * {@link BucketCache}.
   */
  @Test
  public void testFreeBucketEntryRestoredFromFile() throws Exception {
    BucketCache bucketCache = null;
    try {
      final Path dataTestDir = createAndGetTestDir();

      String ioEngineName = "file:" + dataTestDir + "/bucketNoRecycler.cache";
      String persistencePath = dataTestDir + "/bucketNoRecycler.persistence";

      bucketCache = new BucketCache(ioEngineName, capacitySize, constructedBlockSize,
        constructedBlockSizes, writeThreads, writerQLen, persistencePath);
      assertTrue(bucketCache.waitForCacheInitialization(10000));
      long usedByteSize = bucketCache.getAllocator().getUsedSize();
      assertEquals(0, usedByteSize);

      HFileBlockPair[] hfileBlockPairs =
        CacheTestUtils.generateHFileBlocks(constructedBlockSize, 1);
      // Add blocks
      for (HFileBlockPair hfileBlockPair : hfileBlockPairs) {
        bucketCache.cacheBlock(hfileBlockPair.getBlockName(), hfileBlockPair.getBlock());
      }

      for (HFileBlockPair hfileBlockPair : hfileBlockPairs) {
        cacheAndWaitUntilFlushedToBucket(bucketCache, hfileBlockPair.getBlockName(),
          hfileBlockPair.getBlock(), false);
      }
      usedByteSize = bucketCache.getAllocator().getUsedSize();
      assertNotEquals(0, usedByteSize);
      // persist cache to file
      bucketCache.shutdown();
      assertTrue(new File(persistencePath).exists());

      // restore cache from file
      bucketCache = new BucketCache(ioEngineName, capacitySize, constructedBlockSize,
        constructedBlockSizes, writeThreads, writerQLen, persistencePath);
      assertTrue(bucketCache.waitForCacheInitialization(10000));
      assertEquals(usedByteSize, bucketCache.getAllocator().getUsedSize());

      for (HFileBlockPair hfileBlockPair : hfileBlockPairs) {
        BlockCacheKey blockCacheKey = hfileBlockPair.getBlockName();
        bucketCache.evictBlock(blockCacheKey);
      }
      assertEquals(0, bucketCache.getAllocator().getUsedSize());
      assertEquals(0, bucketCache.backingMap.size());
    } finally {
      bucketCache.shutdown();
      HBASE_TESTING_UTILITY.cleanupTestDir();
    }
  }

  @Test
  public void testBlockAdditionWaitWhenCache() throws Exception {
    BucketCache bucketCache = null;
    try {
      final Path dataTestDir = createAndGetTestDir();

      String ioEngineName = "file:" + dataTestDir + "/bucketNoRecycler.cache";
      String persistencePath = dataTestDir + "/bucketNoRecycler.persistence";

      Configuration config = HBASE_TESTING_UTILITY.getConfiguration();
      config.setLong(QUEUE_ADDITION_WAIT_TIME, 1000);

      bucketCache = new BucketCache(ioEngineName, capacitySize, constructedBlockSize,
        constructedBlockSizes, 1, 1, persistencePath, DEFAULT_ERROR_TOLERATION_DURATION, config);
      assertTrue(bucketCache.waitForCacheInitialization(10000));
      long usedByteSize = bucketCache.getAllocator().getUsedSize();
      assertEquals(0, usedByteSize);

      HFileBlockPair[] hfileBlockPairs =
        CacheTestUtils.generateHFileBlocks(constructedBlockSize, 10);
      // Add blocks
      for (HFileBlockPair hfileBlockPair : hfileBlockPairs) {
        bucketCache.cacheBlock(hfileBlockPair.getBlockName(), hfileBlockPair.getBlock(), false,
          true);
      }

      // Max wait for 10 seconds.
      long timeout = 10000;
      // Wait for blocks size to match the number of blocks.
      while (bucketCache.backingMap.size() != 10) {
        if (timeout <= 0) break;
        Threads.sleep(100);
        timeout -= 100;
      }
      for (HFileBlockPair hfileBlockPair : hfileBlockPairs) {
        assertTrue(bucketCache.backingMap.containsKey(hfileBlockPair.getBlockName()));
      }
      usedByteSize = bucketCache.getAllocator().getUsedSize();
      assertNotEquals(0, usedByteSize);
      // persist cache to file
      bucketCache.shutdown();
      assertTrue(new File(persistencePath).exists());

      // restore cache from file
      bucketCache = new BucketCache(ioEngineName, capacitySize, constructedBlockSize,
        constructedBlockSizes, writeThreads, writerQLen, persistencePath);
      assertTrue(bucketCache.waitForCacheInitialization(10000));
      assertEquals(usedByteSize, bucketCache.getAllocator().getUsedSize());

      for (HFileBlockPair hfileBlockPair : hfileBlockPairs) {
        BlockCacheKey blockCacheKey = hfileBlockPair.getBlockName();
        bucketCache.evictBlock(blockCacheKey);
      }
      assertEquals(0, bucketCache.getAllocator().getUsedSize());
      assertEquals(0, bucketCache.backingMap.size());
    } finally {
      if (bucketCache != null) {
        bucketCache.shutdown();
      }
      HBASE_TESTING_UTILITY.cleanupTestDir();
    }
  }

  @Test
  public void testOnConfigurationChange() throws Exception {
    BucketCache bucketCache = null;
    try {
      final Path dataTestDir = createAndGetTestDir();

      String ioEngineName = "file:" + dataTestDir + "/bucketNoRecycler.cache";

      Configuration config = HBASE_TESTING_UTILITY.getConfiguration();

      bucketCache = new BucketCache(ioEngineName, capacitySize, constructedBlockSize,
        constructedBlockSizes, 1, 1, null, DEFAULT_ERROR_TOLERATION_DURATION, config);

      assertTrue(bucketCache.waitForCacheInitialization(10000));

      config.setFloat(ACCEPT_FACTOR_CONFIG_NAME, 0.9f);
      config.setFloat(MIN_FACTOR_CONFIG_NAME, 0.8f);
      config.setFloat(EXTRA_FREE_FACTOR_CONFIG_NAME, 0.15f);
      config.setFloat(SINGLE_FACTOR_CONFIG_NAME, 0.2f);
      config.setFloat(MULTI_FACTOR_CONFIG_NAME, 0.6f);
      config.setFloat(MEMORY_FACTOR_CONFIG_NAME, 0.2f);
      config.setLong(QUEUE_ADDITION_WAIT_TIME, 100);
      config.setLong(BUCKETCACHE_PERSIST_INTERVAL_KEY, 500);
      config.setLong(BACKING_MAP_PERSISTENCE_CHUNK_SIZE, 1000);

      bucketCache.onConfigurationChange(config);

      assertEquals(0.9f, bucketCache.getAcceptableFactor(), 0.01);
      assertEquals(0.8f, bucketCache.getMinFactor(), 0.01);
      assertEquals(0.15f, bucketCache.getExtraFreeFactor(), 0.01);
      assertEquals(0.2f, bucketCache.getSingleFactor(), 0.01);
      assertEquals(0.6f, bucketCache.getMultiFactor(), 0.01);
      assertEquals(0.2f, bucketCache.getMemoryFactor(), 0.01);
      assertEquals(100L, bucketCache.getQueueAdditionWaitTime());
      assertEquals(500L, bucketCache.getBucketcachePersistInterval());
      assertEquals(1000L, bucketCache.getPersistenceChunkSize());

    } finally {
      if (bucketCache != null) {
        bucketCache.shutdown();
      }
      HBASE_TESTING_UTILITY.cleanupTestDir();
    }
  }

  @Test
  public void testNotifyFileCachingCompletedSuccess() throws Exception {
    BucketCache bucketCache = null;
    try {
      Path filePath =
        new Path(HBASE_TESTING_UTILITY.getDataTestDir(), "testNotifyFileCachingCompletedSuccess");
      bucketCache = testNotifyFileCachingCompletedForTenBlocks(filePath, 10, false);
      if (bucketCache.getStats().getFailedInserts() > 0) {
        LOG.info("There were {} fail inserts, "
          + "will assert if total blocks in backingMap equals (10 - failInserts) "
          + "and file isn't listed as fully cached.", bucketCache.getStats().getFailedInserts());
        assertEquals(10 - bucketCache.getStats().getFailedInserts(), bucketCache.backingMap.size());
        assertFalse(bucketCache.fullyCachedFiles.containsKey(filePath.getName()));
      } else {
        assertTrue(bucketCache.fullyCachedFiles.containsKey(filePath.getName()));
      }
    } finally {
      if (bucketCache != null) {
        bucketCache.shutdown();
      }
      HBASE_TESTING_UTILITY.cleanupTestDir();
    }
  }

  @Test
  public void testNotifyFileCachingCompletedForEncodedDataSuccess() throws Exception {
    BucketCache bucketCache = null;
    try {
      Path filePath = new Path(HBASE_TESTING_UTILITY.getDataTestDir(),
        "testNotifyFileCachingCompletedForEncodedDataSuccess");
      bucketCache = testNotifyFileCachingCompletedForTenBlocks(filePath, 10, true);
      if (bucketCache.getStats().getFailedInserts() > 0) {
        LOG.info("There were {} fail inserts, "
          + "will assert if total blocks in backingMap equals (10 - failInserts) "
          + "and file isn't listed as fully cached.", bucketCache.getStats().getFailedInserts());
        assertEquals(10 - bucketCache.getStats().getFailedInserts(), bucketCache.backingMap.size());
        assertFalse(bucketCache.fullyCachedFiles.containsKey(filePath.getName()));
      } else {
        assertTrue(bucketCache.fullyCachedFiles.containsKey(filePath.getName()));
      }
    } finally {
      if (bucketCache != null) {
        bucketCache.shutdown();
      }
      HBASE_TESTING_UTILITY.cleanupTestDir();
    }
  }

  @Test
  public void testNotifyFileCachingCompletedNotAllCached() throws Exception {
    BucketCache bucketCache = null;
    try {
      Path filePath = new Path(HBASE_TESTING_UTILITY.getDataTestDir(),
        "testNotifyFileCachingCompletedNotAllCached");
      // Deliberately passing more blocks than we have created to test that
      // notifyFileCachingCompleted will not consider the file fully cached
      bucketCache = testNotifyFileCachingCompletedForTenBlocks(filePath, 12, false);
      assertFalse(bucketCache.fullyCachedFiles.containsKey(filePath.getName()));
    } finally {
      if (bucketCache != null) {
        bucketCache.shutdown();
      }
      HBASE_TESTING_UTILITY.cleanupTestDir();
    }
  }

  private BucketCache testNotifyFileCachingCompletedForTenBlocks(Path filePath,
    int totalBlocksToCheck, boolean encoded) throws Exception {
    final Path dataTestDir = createAndGetTestDir();
    String ioEngineName = "file:" + dataTestDir + "/bucketNoRecycler.cache";
    BucketCache bucketCache = new BucketCache(ioEngineName, capacitySize, constructedBlockSize,
      constructedBlockSizes, 1, 1, null);
    assertTrue(bucketCache.waitForCacheInitialization(10000));
    long usedByteSize = bucketCache.getAllocator().getUsedSize();
    assertEquals(0, usedByteSize);
    HFileBlockPair[] hfileBlockPairs =
      CacheTestUtils.generateBlocksForPath(constructedBlockSize, 10, filePath, encoded);
    // Add blocks
    for (HFileBlockPair hfileBlockPair : hfileBlockPairs) {
      bucketCache.cacheBlock(hfileBlockPair.getBlockName(), hfileBlockPair.getBlock(), false, true);
    }
    bucketCache.notifyFileCachingCompleted(filePath, totalBlocksToCheck, totalBlocksToCheck,
      totalBlocksToCheck * constructedBlockSize);
    return bucketCache;
  }

  @Test
  public void testEvictOrphansOutOfGracePeriod() throws Exception {
    BucketCache bucketCache = testEvictOrphans(0);
    assertEquals(10, bucketCache.getBackingMap().size());
    assertEquals(0, bucketCache.blocksByHFile.stream()
      .filter(key -> key.getHfileName().equals("testEvictOrphans-orphan")).count());
  }

  @Test
  public void testEvictOrphansWithinGracePeriod() throws Exception {
    BucketCache bucketCache = testEvictOrphans(60 * 60 * 1000L);
    assertEquals(18, bucketCache.getBackingMap().size());
    assertTrue(bucketCache.blocksByHFile.stream()
      .filter(key -> key.getHfileName().equals("testEvictOrphans-orphan")).count() > 0);
  }

  private BucketCache testEvictOrphans(long orphanEvictionGracePeriod) throws Exception {
    Path validFile = new Path(HBASE_TESTING_UTILITY.getDataTestDir(), "testEvictOrphans-valid");
    Path orphanFile = new Path(HBASE_TESTING_UTILITY.getDataTestDir(), "testEvictOrphans-orphan");
    Map<String, HRegion> onlineRegions = new HashMap<>();
    List<HStore> stores = new ArrayList<>();
    Collection<HStoreFile> storeFiles = new ArrayList<>();
    HRegion mockedRegion = mock(HRegion.class);
    HStore mockedStore = mock(HStore.class);
    HStoreFile mockedStoreFile = mock(HStoreFile.class);
    when(mockedStoreFile.getPath()).thenReturn(validFile);
    storeFiles.add(mockedStoreFile);
    when(mockedStore.getStorefiles()).thenReturn(storeFiles);
    stores.add(mockedStore);
    when(mockedRegion.getStores()).thenReturn(stores);
    onlineRegions.put("mocked_region", mockedRegion);
    HBASE_TESTING_UTILITY.getConfiguration().setDouble(MIN_FACTOR_CONFIG_NAME, 0.99);
    HBASE_TESTING_UTILITY.getConfiguration().setDouble(ACCEPT_FACTOR_CONFIG_NAME, 1);
    HBASE_TESTING_UTILITY.getConfiguration().setDouble(EXTRA_FREE_FACTOR_CONFIG_NAME, 0.01);
    HBASE_TESTING_UTILITY.getConfiguration().setLong(BLOCK_ORPHAN_GRACE_PERIOD,
      orphanEvictionGracePeriod);
    BucketCache bucketCache = new BucketCache(ioEngineName, (constructedBlockSize + 1024) * 21,
      constructedBlockSize, new int[] { constructedBlockSize + 1024 }, 1, 1, null, 60 * 1000,
      HBASE_TESTING_UTILITY.getConfiguration(), onlineRegions);
    HFileBlockPair[] validBlockPairs =
      CacheTestUtils.generateBlocksForPath(constructedBlockSize, 10, validFile);
    HFileBlockPair[] orphanBlockPairs =
      CacheTestUtils.generateBlocksForPath(constructedBlockSize, 10, orphanFile);
    for (HFileBlockPair pair : validBlockPairs) {
      bucketCache.cacheBlockWithWait(pair.getBlockName(), pair.getBlock(), false, true);
    }
    waitUntilAllFlushedToBucket(bucketCache);
    assertEquals(10, bucketCache.getBackingMap().size());
    bucketCache.freeSpace("test");
    assertEquals(10, bucketCache.getBackingMap().size());
    for (HFileBlockPair pair : orphanBlockPairs) {
      bucketCache.cacheBlockWithWait(pair.getBlockName(), pair.getBlock(), false, true);
    }
    waitUntilAllFlushedToBucket(bucketCache);
    assertEquals(20, bucketCache.getBackingMap().size());
    bucketCache.freeSpace("test");
    return bucketCache;
  }
}
