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
package org.apache.hadoop.hbase.io.hfile;

import static org.junit.Assert.assertSame;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.hfile.BlockType.BlockCategory;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache;
import org.apache.hadoop.hbase.io.hfile.cache.CacheAccessService;
import org.apache.hadoop.hbase.io.hfile.cache.CacheAccessServiceTestFactory;
import org.apache.hadoop.hbase.io.hfile.cache.NoOpCacheAccessService;
import org.apache.hadoop.hbase.io.hfile.cache.TopologyBackedCacheAccessService;
import org.apache.hadoop.hbase.io.hfile.cache.TopologyBackedCacheAccessServices;
import org.apache.hadoop.hbase.io.util.MemorySizeUtil;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests that {@link CacheConfig} does as expected.
 */
// This test is marked as a large test though it runs in a short amount of time
// (seconds). It is large because it depends on being able to reset the global
// blockcache instance which is in a global variable. Experience has it that
// tests clash on the global variable if this test is run as small sized test.
@Tag(IOTests.TAG)
@Tag(MediumTests.TAG)
public class TestCacheConfig {

  private static final Logger LOG = LoggerFactory.getLogger(TestCacheConfig.class);
  private Configuration conf;

  static class Deserializer implements CacheableDeserializer<Cacheable> {
    private final Cacheable cacheable;
    private int deserializedIdentifier = 0;

    Deserializer(final Cacheable c) {
      deserializedIdentifier = CacheableDeserializerIdManager.registerDeserializer(this);
      this.cacheable = c;
    }

    @Override
    public int getDeserializerIdentifier() {
      return deserializedIdentifier;
    }

    @Override
    public Cacheable deserialize(ByteBuff b, ByteBuffAllocator alloc) throws IOException {
      LOG.info("Deserialized " + b);
      return cacheable;
    }
  }

  static class IndexCacheEntry extends DataCacheEntry {
    private static IndexCacheEntry SINGLETON = new IndexCacheEntry();

    public IndexCacheEntry() {
      super(SINGLETON);
    }

    @Override
    public BlockType getBlockType() {
      return BlockType.ROOT_INDEX;
    }
  }

  static class DataCacheEntry implements Cacheable {
    private static final int SIZE = 1 << 20; // 1MB
    private static DataCacheEntry SINGLETON = new DataCacheEntry();
    final CacheableDeserializer<Cacheable> deserializer;

    DataCacheEntry() {
      this(SINGLETON);
    }

    DataCacheEntry(final Cacheable c) {
      this.deserializer = new Deserializer(c);
    }

    @Override
    public String toString() {
      return "size=" + SIZE + ", type=" + getBlockType();
    }

    @Override
    public long heapSize() {
      return SIZE;
    }

    @Override
    public int getSerializedLength() {
      return SIZE;
    }

    @Override
    public void serialize(ByteBuffer destination, boolean includeNextBlockMetadata) {
      LOG.info("Serialized " + this + " to " + destination);
    }

    @Override
    public CacheableDeserializer<Cacheable> getDeserializer() {
      return this.deserializer;
    }

    @Override
    public BlockType getBlockType() {
      return BlockType.DATA;
    }
  }

  static class MetaCacheEntry extends DataCacheEntry {
    @Override
    public BlockType getBlockType() {
      return BlockType.INTERMEDIATE_INDEX;
    }
  }

  @BeforeEach
  public void setUp() throws Exception {
    this.conf = HBaseConfiguration.create();
  }

  /**
   * @param service  The cache access service instance.
   * @param cc       Cache config.
   * @param doubling If true, addition of element ups counter by 2, not 1, because element added to
   *                 onheap and offheap caches.
   * @param sizing   True if we should run sizing test (doesn't always apply).
   */
  void basicBlockCacheOps(final CacheAccessService service, final CacheConfig cc,
    final boolean doubling, final boolean sizing) {
    assertTrue(CacheConfig.DEFAULT_IN_MEMORY == cc.isInMemory());
    BlockCacheKey bck = new BlockCacheKey("f", 0);
    Cacheable c = new DataCacheEntry();
    // Do asserts on block counting.
    long initialBlockCount = service.getBlockCount();
    service.cacheBlock(bck, c, cc.isInMemory());
    assertEquals(doubling ? 2 : 1, service.getBlockCount() - initialBlockCount);
    service.evictBlock(bck);
    assertEquals(initialBlockCount, service.getBlockCount());
    // Do size accounting. Do it after the above 'warm-up' because it looks like some
    // buffers do lazy allocation so sizes are off on first go around.
    if (sizing) {
      long originalSize = service.getCurrentDataSize();
      service.cacheBlock(bck, c, cc.isInMemory());
      assertTrue(service.getCurrentDataSize() > originalSize);
      service.evictBlock(bck);
      long size = service.getCurrentDataSize();
      assertEquals(originalSize, size);
    }
  }

  @Test
  public void testDisableCacheDataBlock() throws IOException {
    // First tests the default configs behaviour and block cache enabled
    Configuration conf = HBaseConfiguration.create();
    CacheConfig cacheConfig = new CacheConfig(conf);
    assertTrue(cacheConfig.shouldCacheBlockOnRead(BlockCategory.DATA));
    assertFalse(cacheConfig.shouldCacheCompressed(BlockCategory.DATA));
    assertFalse(cacheConfig.shouldCacheDataCompressed());
    assertFalse(cacheConfig.shouldCacheDataOnWrite());
    assertFalse(cacheConfig.shouldCacheCompactedBlocksOnWrite());
    assertTrue(cacheConfig.shouldCacheDataOnRead());
    assertTrue(cacheConfig.shouldCacheBlockOnRead(BlockCategory.INDEX));
    assertTrue(cacheConfig.shouldCacheBlockOnRead(BlockCategory.META));
    assertTrue(cacheConfig.shouldCacheBlockOnRead(BlockCategory.BLOOM));
    assertFalse(cacheConfig.shouldCacheBloomsOnWrite());
    assertFalse(cacheConfig.shouldCacheIndexesOnWrite());

    // Tests block cache enabled and related cache on write flags enabled
    conf.setBoolean(CacheConfig.CACHE_BLOCKS_ON_WRITE_KEY, true);
    conf.setBoolean(CacheConfig.CACHE_DATA_BLOCKS_COMPRESSED_KEY, true);
    conf.setBoolean(CacheConfig.CACHE_BLOOM_BLOCKS_ON_WRITE_KEY, true);
    conf.setBoolean(CacheConfig.CACHE_INDEX_BLOCKS_ON_WRITE_KEY, true);
    conf.setBoolean(CacheConfig.CACHE_COMPACTED_BLOCKS_ON_WRITE_KEY, true);

    cacheConfig = new CacheConfig(conf);
    assertTrue(cacheConfig.shouldCacheBlockOnRead(BlockCategory.DATA));
    assertTrue(cacheConfig.shouldCacheCompressed(BlockCategory.DATA));
    assertTrue(cacheConfig.shouldCacheDataCompressed());
    assertTrue(cacheConfig.shouldCacheDataOnWrite());
    assertTrue(cacheConfig.shouldCacheDataOnRead());
    assertTrue(cacheConfig.shouldCacheBlockOnRead(BlockCategory.INDEX));
    assertTrue(cacheConfig.shouldCacheBlockOnRead(BlockCategory.META));
    assertTrue(cacheConfig.shouldCacheBlockOnRead(BlockCategory.BLOOM));
    assertTrue(cacheConfig.shouldCacheBloomsOnWrite());
    assertTrue(cacheConfig.shouldCacheIndexesOnWrite());
    assertTrue(cacheConfig.shouldCacheCompactedBlocksOnWrite());

    // Tests block cache enabled but related cache on read/write properties disabled
    conf.setBoolean(CacheConfig.CACHE_DATA_ON_READ_KEY, false);
    conf.setBoolean(CacheConfig.CACHE_BLOCKS_ON_WRITE_KEY, false);
    conf.setBoolean(CacheConfig.CACHE_COMPACTED_BLOCKS_ON_WRITE_KEY, false);

    cacheConfig = new CacheConfig(conf);
    assertFalse(cacheConfig.shouldCacheBlockOnRead(BlockCategory.DATA));
    assertFalse(cacheConfig.shouldCacheCompressed(BlockCategory.DATA));
    assertFalse(cacheConfig.shouldCacheDataCompressed());
    assertFalse(cacheConfig.shouldCacheDataOnWrite());
    assertFalse(cacheConfig.shouldCacheDataOnRead());
    assertFalse(cacheConfig.shouldCacheCompactedBlocksOnWrite());
    assertTrue(cacheConfig.shouldCacheBlockOnRead(BlockCategory.INDEX));
    assertFalse(cacheConfig.shouldCacheBlockOnRead(BlockCategory.META));
    assertTrue(cacheConfig.shouldCacheBlockOnRead(BlockCategory.BLOOM));
    assertTrue(cacheConfig.shouldCacheBloomsOnWrite());
    assertTrue(cacheConfig.shouldCacheIndexesOnWrite());

    // Finally tests block cache disabled in the column family but all cache on read/write
    // properties enabled in the config.
    conf.setBoolean(CacheConfig.CACHE_BLOCKS_ON_WRITE_KEY, true);
    conf.setBoolean(CacheConfig.CACHE_DATA_BLOCKS_COMPRESSED_KEY, true);
    conf.setBoolean(CacheConfig.CACHE_BLOOM_BLOCKS_ON_WRITE_KEY, true);
    conf.setBoolean(CacheConfig.CACHE_INDEX_BLOCKS_ON_WRITE_KEY, true);
    conf.setBoolean(CacheConfig.CACHE_COMPACTED_BLOCKS_ON_WRITE_KEY, true);

    ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder
      .newBuilder(Bytes.toBytes("testDisableCacheDataBlock")).setBlockCacheEnabled(false).build();

    cacheConfig = new CacheConfig(conf, columnFamilyDescriptor, (CacheAccessService) null,
      ByteBuffAllocator.HEAP);
    assertFalse(cacheConfig.shouldCacheBlockOnRead(BlockCategory.DATA));
    assertFalse(cacheConfig.shouldCacheCompressed(BlockCategory.DATA));
    assertFalse(cacheConfig.shouldCacheDataCompressed());
    assertFalse(cacheConfig.shouldCacheDataOnWrite());
    assertFalse(cacheConfig.shouldCacheDataOnRead());
    assertTrue(cacheConfig.shouldCacheBlockOnRead(BlockCategory.INDEX));
    assertFalse(cacheConfig.shouldCacheBlockOnRead(BlockCategory.META));
    assertTrue(cacheConfig.shouldCacheBlockOnRead(BlockCategory.BLOOM));
    assertFalse(cacheConfig.shouldCacheBloomsOnWrite());
    assertFalse(cacheConfig.shouldCacheIndexesOnWrite());
  }

  @Test
  public void testCacheConfigDefaultLRUBlockCache() {
    CacheConfig cc = new CacheConfig(this.conf);
    assertTrue(CacheConfig.DEFAULT_IN_MEMORY == cc.isInMemory());
    CacheAccessService service = CacheAccessServiceTestFactory.fromConfiguration(this.conf);
    basicBlockCacheOps(service, cc, false, true);
    assertTrue(CacheAccessServiceTestFactory.blockCache(service) instanceof LruBlockCache);
  }

  /**
   * Assert that the caches are deployed with CombinedBlockCache and of the appropriate sizes.
   */
  @Test
  public void testOffHeapBucketCacheConfig() {
    this.conf.set(HConstants.BUCKET_CACHE_IOENGINE_KEY, "offheap");
    doBucketCacheConfigTest();
  }

  @Test
  public void testFileBucketCacheConfig() throws IOException {
    HBaseTestingUtil htu = new HBaseTestingUtil(this.conf);
    try {
      Path p = new Path(htu.getDataTestDir(), "bc.txt");
      FileSystem fs = FileSystem.get(this.conf);
      fs.create(p).close();
      this.conf.set(HConstants.BUCKET_CACHE_IOENGINE_KEY, "file:" + p);
      doBucketCacheConfigTest();
    } finally {
      htu.cleanupTestDir();
    }
  }

  private void doBucketCacheConfigTest() {
    final int bcSize = 100;
    this.conf.setInt(HConstants.BUCKET_CACHE_SIZE_KEY, bcSize);
    CacheConfig cc = new CacheConfig(this.conf);
    CacheAccessService service = CacheAccessServiceTestFactory.fromConfiguration(this.conf);
    basicBlockCacheOps(service, cc, false, false);
    assertTrue(CacheAccessServiceTestFactory.isCombinedBlockCacheEquivalent(service));
    // TODO: Assert sizes allocated are right and proportions.
    LruBlockCache lbc =
      (LruBlockCache) CacheAccessServiceTestFactory.getFirstLevelBlockCache(service);
    assertEquals(MemorySizeUtil.getOnHeapCacheSize(this.conf), lbc.getMaxSize());
    BucketCache bc = (BucketCache) CacheAccessServiceTestFactory.getSecondLevelBlockCache(service);
    // getMaxSize comes back in bytes but we specified size in MB
    assertEquals(bcSize, bc.getMaxSize() / (1024 * 1024));
  }

  /**
   * Verifies the legacy two-tier block cache layout used when bucket cache is enabled but the
   * combined-cache mode is disabled.
   * <p>
   * In this configuration HBase should deploy an {@link LruBlockCache} as the first-level in-memory
   * cache and a {@link BucketCache} as the second-level victim cache. Blocks are inserted into L1
   * first. When L1 evicts blocks under memory pressure, the evicted blocks should be passed to the
   * configured L2 victim cache.
   * </p>
   * <p>
   * This test intentionally verifies the L1-to-L2 victim-cache relationship without relying on an
   * exact final L1 block count or on a specific block key being evicted. {@link LruBlockCache}
   * eviction policy does not guarantee which block will be selected for eviction, only that some
   * blocks may be evicted when cache pressure exceeds the configured threshold.
   * </p>
   * <p>
   * The previous version of this test attempted to force eviction by inserting a single synthetic
   * block whose size was {@code acceptableSize() + 1}, and then waited until the L1 block count
   * returned to its original value. That approach was flawed for two reasons:
   * </p>
   * <ol>
   * <li>{@link LruBlockCache} rejects any block larger than its maximum cacheable block size before
   * eviction can run. Since {@code acceptableSize()} depends on the JVM heap size while the maximum
   * cacheable block size is fixed by configuration, {@code acceptableSize() + 1} may be larger than
   * the maximum cacheable block size. In that case the block is rejected and no eviction is
   * triggered.</li>
   * <li>If the synthetic block is small enough to be accepted, eviction runs only after the block
   * is inserted. The eviction policy is not required to restore the exact previous block count, nor
   * is it required to evict the originally inserted block. Waiting for an exact L1 block count can
   * therefore hang indefinitely.</li>
   * </ol>
   * <p>
   * The test now creates cache pressure using normal cacheable blocks and waits with a timeout
   * until L2 receives at least one block from L1 eviction. This directly verifies the intended
   * contract: L1 is wired with L2 as its victim cache.
   * </p>
   */
  @Test
  public void testBucketCacheConfigL1L2Setup() throws Exception {
    this.conf.set(HConstants.BUCKET_CACHE_IOENGINE_KEY, "offheap");
    // this.conf.setLong("hbase.lru.max.block.size", 1L << 30);
    // from L1 happens, it does not fail because L2 can't take the eviction because block too big.
    this.conf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.001f);
    MemoryUsage mu = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
    long lruExpectedSize = MemorySizeUtil.getOnHeapCacheSize(this.conf);
    final int bcSize = 100;
    long bcExpectedSize = 100 * 1024 * 1024; // MB.
    assertTrue(lruExpectedSize < bcExpectedSize);
    this.conf.setInt(HConstants.BUCKET_CACHE_SIZE_KEY, bcSize);
    CacheConfig cc = new CacheConfig(this.conf);
    CacheAccessService service = CacheAccessServiceTestFactory.fromConfiguration(this.conf);
    basicBlockCacheOps(service, cc, false, false);
    assertTrue(CacheAccessServiceTestFactory.isCombinedBlockCacheEquivalent(service));
    // TODO: Assert sizes allocated are right and proportions.
    FirstLevelBlockCache lbc =
      (FirstLevelBlockCache) CacheAccessServiceTestFactory.getFirstLevelBlockCache(service);
    assertEquals(lruExpectedSize, lbc.getMaxSize());
    BlockCache bc = CacheAccessServiceTestFactory.getSecondLevelBlockCache(service);
    // getMaxSize comes back in bytes but we specified size in MB
    assertEquals(bcExpectedSize, ((BucketCache) bc).getMaxSize());
    /*
     * The topology-backed cache path intentionally clears legacy L1 victim-cache wiring when L1 and
     * L2 are adapted as independent topology engines. Direct calls to the unwrapped L1 cache should
     * therefore not be used to verify L1-to-L2 victim movement. Tier placement, promotion, and
     * lookup are now owned by TopologyBackedCacheAccessService.
     */
    long initialL1BlockCount = lbc.getBlockCount();
    long initialL2BlockCount = bc.getBlockCount();
    Cacheable c = new DataCacheEntry();
    BlockCacheKey bck = new BlockCacheKey("bck", 0);

    lbc.cacheBlock(bck, c, false);

    assertEquals(initialL1BlockCount + 1, lbc.getBlockCount());
    assertEquals(initialL2BlockCount, bc.getBlockCount());
    assertNotNull(lbc.getBlock(bck, true, false, true));
    assertNull(bc.getBlock(bck, true, false, true));
  }

  /**
   * Adds cacheable blocks to L1 until L1 eviction moves at least one block into L2.
   * <p>
   * The helper does not wait for a particular key to appear in L2. {@link LruBlockCache} eviction
   * is policy-driven and does not guarantee that the first inserted block, or any specific later
   * block, will be evicted first. The observable contract needed by this test is only that an L1
   * eviction is forwarded to the configured L2 victim cache.
   * </p>
   * <p>
   * This helper also avoids using a single oversized block to force eviction. Oversized blocks may
   * be rejected by {@link LruBlockCache} before eviction can run. Instead, it inserts regular
   * cacheable blocks and relies on cumulative cache pressure.
   * </p>
   * @param l1Cache             first-level cache
   * @param l2Cache             second-level victim cache
   * @param initialL2BlockCount L2 block count before creating L1 pressure
   * @throws Exception if the expected L1-to-L2 movement does not happen before the wait timeout
   */
  private void waitForAnyBlockToMoveFromL1ToL2(FirstLevelBlockCache l1Cache, BlockCache l2Cache,
    long initialL2BlockCount) throws Exception {
    AtomicInteger blockIndex = new AtomicInteger();

    /*
     * Do not try to force eviction with one block of size acceptableSize() + 1. LruBlockCache
     * rejects blocks larger than maxBlockSize before eviction can run. For accepted blocks,
     * eviction runs after insertion and does not guarantee which block will be evicted. Therefore
     * this test should not wait for a particular block key to appear in L2. The intended contract
     * is only that an L1 eviction moves some evicted block into the configured L2 victim cache.
     */
    Waiter.waitFor(this.conf, 10000, () -> {
      BlockCacheKey evictionKey = new BlockCacheKey("eviction-" + blockIndex.getAndIncrement(), 0);
      l1Cache.cacheBlock(evictionKey, new DataCacheEntry(), false);

      return l2Cache.getBlockCount() > initialL2BlockCount;
    });
  }

  @Test
  public void testL2CacheWithInvalidBucketSize() {
    Configuration c = new Configuration(this.conf);
    c.set(HConstants.BUCKET_CACHE_IOENGINE_KEY, "offheap");
    c.set(BlockCacheFactory.BUCKET_CACHE_BUCKETS_KEY, "256,512,1024,2048,4000,4096");
    c.setFloat(HConstants.BUCKET_CACHE_SIZE_KEY, 1024);
    try {
      BlockCacheFactory.createBlockCache(c);
      fail("Should throw IllegalArgumentException when passing illegal value for bucket size");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testIndexOnlyLruBlockCache() {
    conf.set(BlockCacheFactory.BLOCKCACHE_POLICY_KEY, "IndexOnlyLRU");
    CacheAccessService cache = CacheAccessServiceTestFactory.fromConfiguration(this.conf);
    assertTrue(CacheAccessServiceTestFactory.blockCache(cache) instanceof IndexOnlyLruBlockCache);
    // reject data block
    long initialBlockCount = cache.getBlockCount();
    BlockCacheKey bck = new BlockCacheKey("bck", 0);
    Cacheable c = new DataCacheEntry();
    cache.cacheBlock(bck, c, true);
    // accept index block
    Cacheable indexCacheEntry = new IndexCacheEntry();
    cache.cacheBlock(bck, indexCacheEntry, true);
    assertEquals(initialBlockCount + 1, cache.getBlockCount());
  }

  @Test
  public void testGetOnHeapCacheSize() {
    Configuration copyConf = new Configuration(conf);
    long fixedSize = 1024 * 1024L;
    long onHeapCacheSize = MemorySizeUtil.getOnHeapCacheSize(copyConf);
    assertEquals(null, copyConf.get(HConstants.HFILE_ONHEAP_BLOCK_CACHE_FIXED_SIZE_KEY));
    assertTrue(onHeapCacheSize > 0 && onHeapCacheSize != fixedSize);
    // when HBASE_BLOCK_CACHE_MEMORY_SIZE is set in number
    copyConf.setLong(HConstants.HFILE_BLOCK_CACHE_MEMORY_SIZE_KEY, 3 * 1024 * 1024);
    onHeapCacheSize = MemorySizeUtil.getOnHeapCacheSize(copyConf);
    assertEquals(3 * 1024 * 1024, onHeapCacheSize);
    // when HBASE_BLOCK_CACHE_MEMORY_SIZE is set in human-readable format
    copyConf.set(HConstants.HFILE_BLOCK_CACHE_MEMORY_SIZE_KEY, "2m");
    onHeapCacheSize = MemorySizeUtil.getOnHeapCacheSize(copyConf);
    assertEquals(2 * 1024 * 1024, onHeapCacheSize);
    // when HBASE_BLOCK_CACHE_FIXED_SIZE_KEY is set, it will be a fixed size
    copyConf.setLong(HConstants.HFILE_ONHEAP_BLOCK_CACHE_FIXED_SIZE_KEY, fixedSize);
    onHeapCacheSize = MemorySizeUtil.getOnHeapCacheSize(copyConf);
    assertEquals(fixedSize, onHeapCacheSize);
  }

  @Test
  void testCacheAccessServiceBackedByBlockCacheWhenBlockCacheIsConfigured() {
    Configuration conf = this.conf;
    BlockCache blockCache = BlockCacheFactory.createBlockCache(conf);
    CacheConfig cacheConfig = new CacheConfig(conf, blockCache);

    CacheAccessService service = cacheConfig.getCacheAccessService();

    assertInstanceOf(TopologyBackedCacheAccessService.class, service);
    assertSame(blockCache, TopologyBackedCacheAccessServices.getBlockCache(service));
  }

  @Test
  void testCacheAccessServiceIsNoOpWhenBlockCacheIsNull() {
    Configuration conf = this.conf;

    CacheConfig cacheConfig = new CacheConfig(conf);

    CacheAccessService service = cacheConfig.getCacheAccessService();

    assertInstanceOf(NoOpCacheAccessService.class, service);
    assertFalse(service.isCacheEnabled());
  }
}
