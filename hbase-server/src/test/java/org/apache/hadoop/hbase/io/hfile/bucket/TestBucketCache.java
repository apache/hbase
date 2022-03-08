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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
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
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

/**
 * Basic test of BucketCache.Puts and gets.
 * <p>
 * Tests will ensure that blocks' data correctness under several threads concurrency
 */
@RunWith(Parameterized.class)
@Category({ IOTests.class, LargeTests.class })
public class TestBucketCache {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestBucketCache.class);

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
  String ioEngineName = "offheap";
  String persistencePath = null;

  private static final HBaseTestingUtility HBASE_TESTING_UTILITY = new HBaseTestingUtility();

  private static class MockedBucketCache extends BucketCache {

    public MockedBucketCache(String ioEngineName, long capacity, int blockSize, int[] bucketSizes,
        int writerThreads, int writerQLen, String persistencePath) throws IOException {
      super(ioEngineName, capacity, blockSize, bucketSizes, writerThreads, writerQLen,
          persistencePath);
      super.wait_when_cache = true;
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
        constructedBlockSizes, writeThreads, writerQLen, persistencePath);
  }

  @After
  public void tearDown() {
    cache.shutdown();
  }

  /**
   * Test Utility to create test dir and return name
   *
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
    ArrayList<Long> allocations = new ArrayList<>();
    // Fill the allocated extents by choosing a random blocksize. Continues selecting blocks until
    // the cache is completely filled.
    List<Integer> tmp = new ArrayList<>(BLOCKSIZES);
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
    CacheTestUtils.hammerSingleKey(cache, 2 * NUM_THREADS, 2 * NUM_QUERIES);
  }

  @Test
  public void testHeapSizeChanges() throws Exception {
    cache.stopWriterThreads();
    CacheTestUtils.testHeapSizeChanges(cache, BLOCK_SIZE);
  }

  public static void waitUntilFlushedToBucket(BucketCache cache, BlockCacheKey cacheKey)
      throws InterruptedException {
    while (!cache.backingMap.containsKey(cacheKey) || cache.ramCache.containsKey(cacheKey)) {
      Thread.sleep(100);
    }
    Thread.sleep(1000);
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
      Cacheable block) throws InterruptedException {
    cache.cacheBlock(cacheKey, block);
    waitUntilFlushedToBucket(cache, cacheKey);
  }

  @Test
  public void testMemoryLeak() throws Exception {
    final BlockCacheKey cacheKey = new BlockCacheKey("dummy", 1L);
    cacheAndWaitUntilFlushedToBucket(cache, cacheKey,
      new CacheTestUtils.ByteArrayCacheable(new byte[10]));
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
    cache.blockEvicted(cacheKey, cache.backingMap.remove(cacheKey), true);
    assertEquals(0, cache.getBlockCount());
    cacheAndWaitUntilFlushedToBucket(cache, cacheKey,
      new CacheTestUtils.ByteArrayCacheable(new byte[10]));
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
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    Path testDir = TEST_UTIL.getDataTestDir();
    TEST_UTIL.getTestFileSystem().mkdirs(testDir);

    String ioEngineName = "file:" + testDir + "/bucket.cache";
    String persistencePath = testDir + "/bucket.persistence";

    BucketCache bucketCache = new BucketCache(ioEngineName, capacitySize, constructedBlockSize,
        constructedBlockSizes, writeThreads, writerQLen, persistencePath);
    long usedSize = bucketCache.getAllocator().getUsedSize();
    assertEquals(0, usedSize);

    HFileBlockPair[] blocks = CacheTestUtils.generateHFileBlocks(constructedBlockSize, 1);
    // Add blocks
    for (HFileBlockPair block : blocks) {
      bucketCache.cacheBlock(block.getBlockName(), block.getBlock());
    }
    for (HFileBlockPair block : blocks) {
      cacheAndWaitUntilFlushedToBucket(bucketCache, block.getBlockName(), block.getBlock());
    }
    usedSize = bucketCache.getAllocator().getUsedSize();
    assertNotEquals(0, usedSize);
    // persist cache to file
    bucketCache.shutdown();
    assertTrue(new File(persistencePath).exists());

    // restore cache from file
    bucketCache = new BucketCache(ioEngineName, capacitySize, constructedBlockSize,
        constructedBlockSizes, writeThreads, writerQLen, persistencePath);
    assertFalse(new File(persistencePath).exists());
    assertEquals(usedSize, bucketCache.getAllocator().getUsedSize());
    // persist cache to file
    bucketCache.shutdown();
    assertTrue(new File(persistencePath).exists());

    // reconfig buckets sizes, the biggest bucket is small than constructedBlockSize (8k or 16k)
    // so it can't restore cache from file
    int[] smallBucketSizes = new int[] { 2 * 1024 + 1024, 4 * 1024 + 1024 };
    bucketCache = new BucketCache(ioEngineName, capacitySize, constructedBlockSize,
        smallBucketSizes, writeThreads, writerQLen, persistencePath);
    assertFalse(new File(persistencePath).exists());
    assertEquals(0, bucketCache.getAllocator().getUsedSize());
    assertEquals(0, bucketCache.backingMap.size());

    TEST_UTIL.cleanupTestDir();
  }

  @Test
  public void testBucketAllocatorLargeBuckets() throws BucketAllocatorException {
    long availableSpace = 20 * 1024L * 1024 * 1024;
    int[] bucketSizes = new int[]{1024, 1024 * 1024, 1024 * 1024 * 1024};
    BucketAllocator allocator = new BucketAllocator(availableSpace, bucketSizes);
    assertTrue(allocator.getBuckets().length > 0);
  }

  @Test
  public void testGetPartitionSize() throws IOException {
    //Test default values
    validateGetPartitionSize(cache, BucketCache.DEFAULT_SINGLE_FACTOR, BucketCache.DEFAULT_MIN_FACTOR);

    Configuration conf = HBaseConfiguration.create();
    conf.setFloat(BucketCache.MIN_FACTOR_CONFIG_NAME, 0.5f);
    conf.setFloat(BucketCache.SINGLE_FACTOR_CONFIG_NAME, 0.1f);
    conf.setFloat(BucketCache.MULTI_FACTOR_CONFIG_NAME, 0.7f);
    conf.setFloat(BucketCache.MEMORY_FACTOR_CONFIG_NAME, 0.2f);

    BucketCache cache = new BucketCache(ioEngineName, capacitySize, constructedBlockSize,
        constructedBlockSizes, writeThreads, writerQLen, persistencePath, 100, conf);

    validateGetPartitionSize(cache, 0.1f, 0.5f);
    validateGetPartitionSize(cache, 0.7f, 0.5f);
    validateGetPartitionSize(cache, 0.2f, 0.5f);
  }

  @Test
  public void testValidBucketCacheConfigs() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.setFloat(BucketCache.ACCEPT_FACTOR_CONFIG_NAME, 0.9f);
    conf.setFloat(BucketCache.MIN_FACTOR_CONFIG_NAME, 0.5f);
    conf.setFloat(BucketCache.EXTRA_FREE_FACTOR_CONFIG_NAME, 0.5f);
    conf.setFloat(BucketCache.SINGLE_FACTOR_CONFIG_NAME, 0.1f);
    conf.setFloat(BucketCache.MULTI_FACTOR_CONFIG_NAME, 0.7f);
    conf.setFloat(BucketCache.MEMORY_FACTOR_CONFIG_NAME, 0.2f);

    BucketCache cache = new BucketCache(ioEngineName, capacitySize, constructedBlockSize,
        constructedBlockSizes, writeThreads, writerQLen, persistencePath, 100, conf);

    assertEquals(BucketCache.ACCEPT_FACTOR_CONFIG_NAME + " failed to propagate.", 0.9f,
        cache.getAcceptableFactor(), 0);
    assertEquals(BucketCache.MIN_FACTOR_CONFIG_NAME + " failed to propagate.", 0.5f,
        cache.getMinFactor(), 0);
    assertEquals(BucketCache.EXTRA_FREE_FACTOR_CONFIG_NAME + " failed to propagate.", 0.5f,
        cache.getExtraFreeFactor(), 0);
    assertEquals(BucketCache.SINGLE_FACTOR_CONFIG_NAME + " failed to propagate.", 0.1f,
        cache.getSingleFactor(), 0);
    assertEquals(BucketCache.MULTI_FACTOR_CONFIG_NAME + " failed to propagate.", 0.7f,
        cache.getMultiFactor(), 0);
    assertEquals(BucketCache.MEMORY_FACTOR_CONFIG_NAME + " failed to propagate.", 0.2f,
        cache.getMemoryFactor(), 0);
  }

  @Test
  public void testInvalidAcceptFactorConfig() throws IOException {
    float[] configValues = {-1f, 0.2f, 0.86f, 1.05f};
    boolean[] expectedOutcomes = {false, false, true, false};
    Map<String, float[]> configMappings = ImmutableMap.of(BucketCache.ACCEPT_FACTOR_CONFIG_NAME, configValues);
    Configuration conf = HBaseConfiguration.create();
    checkConfigValues(conf, configMappings, expectedOutcomes);
  }

  @Test
  public void testInvalidMinFactorConfig() throws IOException {
    float[] configValues = {-1f, 0f, 0.96f, 1.05f};
    //throws due to <0, in expected range, minFactor > acceptableFactor, > 1.0
    boolean[] expectedOutcomes = {false, true, false, false};
    Map<String, float[]> configMappings = ImmutableMap
      .of(BucketCache.MIN_FACTOR_CONFIG_NAME, configValues);
    Configuration conf = HBaseConfiguration.create();
    checkConfigValues(conf, configMappings, expectedOutcomes);
  }

  @Test
  public void testInvalidExtraFreeFactorConfig() throws IOException {
    float[] configValues = {-1f, 0f, 0.2f, 1.05f};
    //throws due to <0, in expected range, in expected range, config can be > 1.0
    boolean[] expectedOutcomes = {false, true, true, true};
    Map<String, float[]> configMappings = ImmutableMap.of(BucketCache.EXTRA_FREE_FACTOR_CONFIG_NAME, configValues);
    Configuration conf = HBaseConfiguration.create();
    checkConfigValues(conf, configMappings, expectedOutcomes);
  }

  @Test
  public void testInvalidCacheSplitFactorConfig() throws IOException {
    float[] singleFactorConfigValues = {0.2f, 0f, -0.2f, 1f};
    float[] multiFactorConfigValues = {0.4f, 0f, 1f, .05f};
    float[] memoryFactorConfigValues = {0.4f, 0f, 0.2f, .5f};
    // All configs add up to 1.0 and are between 0 and 1.0, configs don't add to 1.0, configs can't be negative, configs don't add to 1.0
    boolean[] expectedOutcomes = {true, false, false, false};
    Map<String, float[]> configMappings = ImmutableMap.of(BucketCache.SINGLE_FACTOR_CONFIG_NAME,
        singleFactorConfigValues, BucketCache.MULTI_FACTOR_CONFIG_NAME, multiFactorConfigValues,
        BucketCache.MEMORY_FACTOR_CONFIG_NAME, memoryFactorConfigValues);
    Configuration conf = HBaseConfiguration.create();
    checkConfigValues(conf, configMappings, expectedOutcomes);
  }

  private void checkConfigValues(Configuration conf, Map<String, float[]> configMap, boolean[] expectSuccess) throws IOException {
    Set<String> configNames = configMap.keySet();
    for (int i = 0; i < expectSuccess.length; i++) {
      try {
        for (String configName : configNames) {
          conf.setFloat(configName, configMap.get(configName)[i]);
        }
        BucketCache cache = new BucketCache(ioEngineName, capacitySize, constructedBlockSize,
            constructedBlockSizes, writeThreads, writerQLen, persistencePath, 100, conf);
        assertTrue("Created BucketCache and expected it to succeed: " + expectSuccess[i] + ", but it actually was: " + !expectSuccess[i], expectSuccess[i]);
      } catch (IllegalArgumentException e) {
        assertFalse("Created BucketCache and expected it to succeed: " + expectSuccess[i] + ", but it actually was: " + !expectSuccess[i], expectSuccess[i]);
      }
    }
  }

  private void validateGetPartitionSize(BucketCache bucketCache, float partitionFactor, float minFactor) {
    long expectedOutput = (long) Math.floor(bucketCache.getAllocator().getTotalSize() * partitionFactor * minFactor);
    assertEquals(expectedOutput, bucketCache.getPartitionSize(partitionFactor));
  }

  @Test
  public void testOffsetProducesPositiveOutput() {
    // This number is picked because it produces negative output if the values isn't ensured to be
    // positive. See HBASE-18757 for more information.
    long testValue = 549888460800L;
    BucketEntry bucketEntry =
        new BucketEntry(testValue, 10, 10L, true, (entry) -> {
          return ByteBuffAllocator.NONE;
        }, ByteBuffAllocator.HEAP);
    assertEquals(testValue, bucketEntry.offset());
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
    RAMQueueEntry re1 = new RAMQueueEntry(key1, blk1, 1, false);
    RAMQueueEntry re2 = new RAMQueueEntry(key1, blk2, 1, false);

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
    Mockito.when(ioEngine.usesSharedMemory()).thenReturn(false);
    // Mockito.doNothing().when(ioEngine).write(Mockito.any(ByteBuffer.class), Mockito.anyLong());
    Mockito.doThrow(RuntimeException.class).when(ioEngine).write(Mockito.any(ByteBuffer.class),
      Mockito.anyLong());
    Mockito.doThrow(RuntimeException.class).when(ioEngine).write(Mockito.any(ByteBuff.class),
      Mockito.anyLong());

    // create an bucket allocator.
    long availableSpace = 1024 * 1024 * 1024L;
    BucketAllocator allocator = new BucketAllocator(availableSpace, null);

    BlockCacheKey key = new BlockCacheKey("dummy", 1L);
    RAMQueueEntry re = new RAMQueueEntry(key, block, 1, true);

    Assert.assertEquals(0, allocator.getUsedSize());
    try {
      re.writeToCache(ioEngine, allocator, null, null);
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
    try {
      final Path dataTestDir = createAndGetTestDir();

      String ioEngineName = "file:" + dataTestDir + "/bucketNoRecycler.cache";
      String persistencePath = dataTestDir + "/bucketNoRecycler.persistence";

      BucketCache bucketCache = new BucketCache(ioEngineName, capacitySize, constructedBlockSize,
          constructedBlockSizes, writeThreads, writerQLen, persistencePath);
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
          hfileBlockPair.getBlock());
      }
      usedByteSize = bucketCache.getAllocator().getUsedSize();
      assertNotEquals(0, usedByteSize);
      // persist cache to file
      bucketCache.shutdown();
      assertTrue(new File(persistencePath).exists());

      // restore cache from file
      bucketCache = new BucketCache(ioEngineName, capacitySize, constructedBlockSize,
          constructedBlockSizes, writeThreads, writerQLen, persistencePath);
      assertFalse(new File(persistencePath).exists());
      assertEquals(usedByteSize, bucketCache.getAllocator().getUsedSize());

      for (HFileBlockPair hfileBlockPair : hfileBlockPairs) {
        BlockCacheKey blockCacheKey = hfileBlockPair.getBlockName();
        bucketCache.evictBlock(blockCacheKey);
      }
      assertEquals(0, bucketCache.getAllocator().getUsedSize());
      assertEquals(0, bucketCache.backingMap.size());
    } finally {
      HBASE_TESTING_UTILITY.cleanupTestDir();
    }
  }

}
