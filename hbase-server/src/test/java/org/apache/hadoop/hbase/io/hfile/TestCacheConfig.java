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
package org.apache.hadoop.hbase.io.hfile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.nio.ByteBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.hfile.BlockType.BlockCategory;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache;
import org.apache.hadoop.hbase.io.util.MemorySizeUtil;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests that {@link CacheConfig} does as expected.
 */
// This test is marked as a large test though it runs in a short amount of time
// (seconds).  It is large because it depends on being able to reset the global
// blockcache instance which is in a global variable.  Experience has it that
// tests clash on the global variable if this test is run as small sized test.
@Category({IOTests.class, MediumTests.class})
public class TestCacheConfig {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCacheConfig.class);

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
    public Cacheable deserialize(ByteBuff b, ByteBuffAllocator alloc)
        throws IOException {
      LOG.info("Deserialized " + b);
      return cacheable;
    }
  };

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
    private static final int SIZE = 1;
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
    };

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

  @Before
  public void setUp() throws Exception {
    this.conf = HBaseConfiguration.create();
  }

  /**
   * @param bc The block cache instance.
   * @param cc Cache config.
   * @param doubling If true, addition of element ups counter by 2, not 1, because element added
   * to onheap and offheap caches.
   * @param sizing True if we should run sizing test (doesn't always apply).
   */
  void basicBlockCacheOps(final BlockCache bc, final CacheConfig cc, final boolean doubling,
      final boolean sizing) {
    assertTrue(CacheConfig.DEFAULT_IN_MEMORY == cc.isInMemory());
    BlockCacheKey bck = new BlockCacheKey("f", 0);
    Cacheable c = new DataCacheEntry();
    // Do asserts on block counting.
    long initialBlockCount = bc.getBlockCount();
    bc.cacheBlock(bck, c, cc.isInMemory());
    assertEquals(doubling ? 2 : 1, bc.getBlockCount() - initialBlockCount);
    bc.evictBlock(bck);
    assertEquals(initialBlockCount, bc.getBlockCount());
    // Do size accounting.  Do it after the above 'warm-up' because it looks like some
    // buffers do lazy allocation so sizes are off on first go around.
    if (sizing) {
      long originalSize = bc.getCurrentSize();
      bc.cacheBlock(bck, c, cc.isInMemory());
      assertTrue(bc.getCurrentSize() > originalSize);
      bc.evictBlock(bck);
      long size = bc.getCurrentSize();
      assertEquals(originalSize, size);
    }
  }

  @Test
  public void testDisableCacheDataBlock() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    CacheConfig cacheConfig = new CacheConfig(conf);
    assertTrue(cacheConfig.shouldCacheBlockOnRead(BlockCategory.DATA));
    assertFalse(cacheConfig.shouldCacheCompressed(BlockCategory.DATA));
    assertFalse(cacheConfig.shouldCacheDataCompressed());
    assertFalse(cacheConfig.shouldCacheDataOnWrite());
    assertTrue(cacheConfig.shouldCacheDataOnRead());
    assertTrue(cacheConfig.shouldCacheBlockOnRead(BlockCategory.INDEX));
    assertTrue(cacheConfig.shouldCacheBlockOnRead(BlockCategory.META));
    assertTrue(cacheConfig.shouldCacheBlockOnRead(BlockCategory.BLOOM));
    assertFalse(cacheConfig.shouldCacheBloomsOnWrite());
    assertFalse(cacheConfig.shouldCacheIndexesOnWrite());

    conf.setBoolean(CacheConfig.CACHE_BLOCKS_ON_WRITE_KEY, true);
    conf.setBoolean(CacheConfig.CACHE_DATA_BLOCKS_COMPRESSED_KEY, true);
    conf.setBoolean(CacheConfig.CACHE_BLOOM_BLOCKS_ON_WRITE_KEY, true);
    conf.setBoolean(CacheConfig.CACHE_INDEX_BLOCKS_ON_WRITE_KEY, true);

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

    conf.setBoolean(CacheConfig.CACHE_DATA_ON_READ_KEY, false);
    conf.setBoolean(CacheConfig.CACHE_BLOCKS_ON_WRITE_KEY, false);

    cacheConfig = new CacheConfig(conf);
    assertFalse(cacheConfig.shouldCacheBlockOnRead(BlockCategory.DATA));
    assertFalse(cacheConfig.shouldCacheCompressed(BlockCategory.DATA));
    assertFalse(cacheConfig.shouldCacheDataCompressed());
    assertFalse(cacheConfig.shouldCacheDataOnWrite());
    assertFalse(cacheConfig.shouldCacheDataOnRead());
    assertTrue(cacheConfig.shouldCacheBlockOnRead(BlockCategory.INDEX));
    assertFalse(cacheConfig.shouldCacheBlockOnRead(BlockCategory.META));
    assertTrue(cacheConfig.shouldCacheBlockOnRead(BlockCategory.BLOOM));
    assertTrue(cacheConfig.shouldCacheBloomsOnWrite());
    assertTrue(cacheConfig.shouldCacheIndexesOnWrite());

    conf.setBoolean(CacheConfig.CACHE_DATA_ON_READ_KEY, true);
    conf.setBoolean(CacheConfig.CACHE_BLOCKS_ON_WRITE_KEY, false);

    HColumnDescriptor family = new HColumnDescriptor("testDisableCacheDataBlock");
    family.setBlockCacheEnabled(false);

    cacheConfig = new CacheConfig(conf, family, null, ByteBuffAllocator.HEAP);
    assertFalse(cacheConfig.shouldCacheBlockOnRead(BlockCategory.DATA));
    assertFalse(cacheConfig.shouldCacheCompressed(BlockCategory.DATA));
    assertFalse(cacheConfig.shouldCacheDataCompressed());
    assertFalse(cacheConfig.shouldCacheDataOnWrite());
    assertFalse(cacheConfig.shouldCacheDataOnRead());
    assertTrue(cacheConfig.shouldCacheBlockOnRead(BlockCategory.INDEX));
    assertFalse(cacheConfig.shouldCacheBlockOnRead(BlockCategory.META));
    assertTrue(cacheConfig.shouldCacheBlockOnRead(BlockCategory.BLOOM));
    assertTrue(cacheConfig.shouldCacheBloomsOnWrite());
    assertTrue(cacheConfig.shouldCacheIndexesOnWrite());
  }

  @Test
  public void testCacheConfigDefaultLRUBlockCache() {
    CacheConfig cc = new CacheConfig(this.conf);
    assertTrue(CacheConfig.DEFAULT_IN_MEMORY == cc.isInMemory());
    BlockCache blockCache = BlockCacheFactory.createBlockCache(this.conf);
    basicBlockCacheOps(blockCache, cc, false, true);
    assertTrue(blockCache instanceof LruBlockCache);
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
    HBaseTestingUtility htu = new HBaseTestingUtility(this.conf);
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
    BlockCache blockCache = BlockCacheFactory.createBlockCache(this.conf);
    basicBlockCacheOps(blockCache, cc, false, false);
    assertTrue(blockCache instanceof CombinedBlockCache);
    // TODO: Assert sizes allocated are right and proportions.
    CombinedBlockCache cbc = (CombinedBlockCache) blockCache;
    BlockCache[] bcs = cbc.getBlockCaches();
    assertTrue(bcs[0] instanceof LruBlockCache);
    LruBlockCache lbc = (LruBlockCache) bcs[0];
    assertEquals(MemorySizeUtil.getOnHeapCacheSize(this.conf), lbc.getMaxSize());
    assertTrue(bcs[1] instanceof BucketCache);
    BucketCache bc = (BucketCache) bcs[1];
    // getMaxSize comes back in bytes but we specified size in MB
    assertEquals(bcSize, bc.getMaxSize() / (1024 * 1024));
  }

  /**
   * Assert that when BUCKET_CACHE_COMBINED_KEY is false, the non-default, that we deploy
   * LruBlockCache as L1 with a BucketCache for L2.
   */
  @Test
  public void testBucketCacheConfigL1L2Setup() {
    this.conf.set(HConstants.BUCKET_CACHE_IOENGINE_KEY, "offheap");
    // Make lru size is smaller than bcSize for sure.  Need this to be true so when eviction
    // from L1 happens, it does not fail because L2 can't take the eviction because block too big.
    this.conf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.001f);
    MemoryUsage mu = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
    long lruExpectedSize = MemorySizeUtil.getOnHeapCacheSize(this.conf);
    final int bcSize = 100;
    long bcExpectedSize = 100 * 1024 * 1024; // MB.
    assertTrue(lruExpectedSize < bcExpectedSize);
    this.conf.setInt(HConstants.BUCKET_CACHE_SIZE_KEY, bcSize);
    CacheConfig cc = new CacheConfig(this.conf);
    BlockCache blockCache = BlockCacheFactory.createBlockCache(this.conf);
    basicBlockCacheOps(blockCache, cc, false, false);
    assertTrue(blockCache instanceof CombinedBlockCache);
    // TODO: Assert sizes allocated are right and proportions.
    CombinedBlockCache cbc = (CombinedBlockCache) blockCache;
    FirstLevelBlockCache lbc = cbc.l1Cache;
    assertEquals(lruExpectedSize, lbc.getMaxSize());
    BlockCache bc = cbc.l2Cache;
    // getMaxSize comes back in bytes but we specified size in MB
    assertEquals(bcExpectedSize, ((BucketCache) bc).getMaxSize());
    // Test the L1+L2 deploy works as we'd expect with blocks evicted from L1 going to L2.
    long initialL1BlockCount = lbc.getBlockCount();
    long initialL2BlockCount = bc.getBlockCount();
    Cacheable c = new DataCacheEntry();
    BlockCacheKey bck = new BlockCacheKey("bck", 0);
    lbc.cacheBlock(bck, c, false);
    assertEquals(initialL1BlockCount + 1, lbc.getBlockCount());
    assertEquals(initialL2BlockCount, bc.getBlockCount());
    // Force evictions by putting in a block too big.
    final long justTooBigSize = ((LruBlockCache)lbc).acceptableSize() + 1;
    lbc.cacheBlock(new BlockCacheKey("bck2", 0), new DataCacheEntry() {
      @Override
      public long heapSize() {
        return justTooBigSize;
      }

      @Override
      public int getSerializedLength() {
        return (int)heapSize();
      }
    });
    // The eviction thread in lrublockcache needs to run.
    while (initialL1BlockCount != lbc.getBlockCount()) Threads.sleep(10);
    assertEquals(initialL1BlockCount, lbc.getBlockCount());
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
    CacheConfig cc = new CacheConfig(this.conf);
    conf.set(BlockCacheFactory.BLOCKCACHE_POLICY_KEY, "IndexOnlyLRU");
    BlockCache blockCache = BlockCacheFactory.createBlockCache(this.conf);
    assertTrue(blockCache instanceof IndexOnlyLruBlockCache);
    // reject data block
    long initialBlockCount = blockCache.getBlockCount();
    BlockCacheKey bck = new BlockCacheKey("bck", 0);
    Cacheable c = new DataCacheEntry();
    blockCache.cacheBlock(bck, c, true);
    // accept index block
    Cacheable indexCacheEntry = new IndexCacheEntry();
    blockCache.cacheBlock(bck, indexCacheEntry, true);
    assertEquals(initialBlockCount + 1, blockCache.getBlockCount());
  }

  @Test
  public void testGetOnHeapCacheSize() {
    Configuration copyConf = new Configuration(conf);
    long fixedSize = 1024 * 1024L;
    long onHeapCacheSize = MemorySizeUtil.getOnHeapCacheSize(copyConf);
    assertEquals(null, copyConf.get(HConstants.HFILE_ONHEAP_BLOCK_CACHE_FIXED_SIZE_KEY));
    assertTrue(onHeapCacheSize > 0 && onHeapCacheSize != fixedSize);
    // when HBASE_BLOCK_CACHE_FIXED_SIZE_KEY is set, it will be a fixed size
    copyConf.setLong(HConstants.HFILE_ONHEAP_BLOCK_CACHE_FIXED_SIZE_KEY, fixedSize);
    onHeapCacheSize = MemorySizeUtil.getOnHeapCacheSize(copyConf);
    assertEquals(fixedSize, onHeapCacheSize);
  }
}
