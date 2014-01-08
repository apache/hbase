/*
 * Copyright The Apache Software Foundation
 *
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache;
import org.apache.hadoop.hbase.regionserver.CreateRandomStoreFile;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaConfigured;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

/**
 * Tests L2 bucket cache for correctness
 */
@RunWith(Parameterized.class)
public class TestL2BucketCache {

  private static final Log LOG = LogFactory.getLog(TestL2BucketCache.class);

  private static final int DATA_BLOCK_SIZE = 2048;
  private static final int NUM_KV = 25000;
  private static final int INDEX_BLOCK_SIZE = 512;
  private static final int BLOOM_BLOCK_SIZE = 4096;
  private static final StoreFile.BloomType BLOOM_TYPE =
      StoreFile.BloomType.ROWCOL;

  private static final HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();
  private static final HFileDataBlockEncoderImpl ENCODER =
      new HFileDataBlockEncoderImpl(DataBlockEncoding.PREFIX);

  private BucketCache underlyingCache;
  private MockedL2Cache mockedL2Cache;

  private Configuration conf;
  private CacheConfig cacheConf;
  private HColumnDescriptor family;
  private HRegion region;
  private FileSystem fs;
  private Path storeFilePath;

  private final Random rand = new Random(12983177L);
  private final String ioEngineName;

  public TestL2BucketCache(String ioEngineName) {
    this.ioEngineName = ioEngineName;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> getConfiguration() {
    Object[][] data = new Object[][] { {"heap"}, {"offheap"}};
    return Arrays.asList(data);
  }

  @Before
  public void setUp() throws IOException {
    conf = TEST_UTIL.getConfiguration();
    conf.setInt(HFile.FORMAT_VERSION_KEY, HFile.MAX_FORMAT_VERSION);
    conf.setInt(HFileBlockIndex.MAX_CHUNK_SIZE_KEY, INDEX_BLOCK_SIZE);
    conf.setInt(BloomFilterFactory.IO_STOREFILE_BLOOM_BLOCK_SIZE,
        BLOOM_BLOCK_SIZE);
    conf.setBoolean(CacheConfig.CACHE_BLOCKS_ON_FLUSH_KEY, true);
    conf.setBoolean(CacheConfig.CACHE_INDEX_BLOCKS_ON_WRITE_KEY, true);
    conf.setBoolean(CacheConfig.CACHE_BLOOM_BLOCKS_ON_WRITE_KEY, true);
    conf.setBoolean(CacheConfig.L2_CACHE_BLOCKS_ON_FLUSH_KEY, true);
    underlyingCache = new BucketCache(ioEngineName,
        32 * DATA_BLOCK_SIZE * 1024,
        BucketCache.DEFAULT_WRITER_QUEUE_ITEMS,
        BucketCache.DEFAULT_WRITER_QUEUE_ITEMS,
        BucketCache.DEFAULT_ERROR_TOLERATION_DURATION,
        CacheConfig.DEFAULT_L2_BUCKET_CACHE_BUCKET_SIZES,
        conf);
    mockedL2Cache = new MockedL2Cache(underlyingCache);

    fs = FileSystem.get(conf);
    cacheConf = new CacheConfig.CacheConfigBuilder(conf)
        .withL2Cache(mockedL2Cache)
        .build();
  }

  @After
  public void tearDown() throws IOException {
    underlyingCache.shutdown();
  }

  // Tests cache on write: when writing to an HFile, the data being written
  // should also be placed in the L2 cache.
  @Test
  public void testCacheOnWrite() throws Exception {
    writeStoreFile();
    DataBlockEncoding encodingInCache = ENCODER.getEncodingInCache();
    HFileReaderV2 reader = (HFileReaderV2) HFile.createReaderWithEncoding(fs,
        storeFilePath, cacheConf, encodingInCache);
    HFileScanner scanner = reader.getScanner(false, false, false);
    assertTrue(scanner.seekTo());
    long offset = 0;
    long cachedCount = 0;
    while (offset < reader.getTrailer().getLoadOnOpenDataOffset()) {
      mockedL2Cache.enableReads.set(false);
      HFileBlock blockFromDisk;
      try {
        blockFromDisk =
            reader.readBlock(offset, -1, false, false, false, null,
                    encodingInCache, null);
      } finally {
        mockedL2Cache.enableReads.set(true);
      }
      boolean isInL1Lcache = cacheConf.getBlockCache().getBlock(
          new BlockCacheKey(reader.getName(), offset), true) != null;
      if (isInL1Lcache) {
        cachedCount++;
        BlockCacheKey key = new BlockCacheKey(reader.getName(), offset);
        byte[] blockFromCacheRaw = mockedL2Cache.getRawBlockBytes(key);
        assertNotNull("All blocks in l1 cache, should also be in l2 cache: "
            + blockFromDisk.toString(), blockFromCacheRaw);
        HFileBlock blockFromL2Cache = HFileBlock.fromBytes(blockFromCacheRaw,
            Compression.Algorithm.GZ, true, offset);
        assertEquals("Data in block from disk (" + blockFromDisk +
            ") should match data in block from cache (" + blockFromL2Cache +
            ").", blockFromL2Cache.getBufferWithHeader(),
            blockFromDisk.getBufferWithHeader());
        assertEquals(blockFromDisk, blockFromL2Cache);
      }
      offset += blockFromDisk.getOnDiskSizeWithHeader();
    }
    assertTrue("> 0 blocks must be cached in L2Cache", cachedCount > 0);
  }

  // Tests cache on read: when blocks are read from an HFile they should
  // be cached in the L2 cache.
  @Test
  public void testCacheOnRead() throws Exception {
    writeStoreFile();
    DataBlockEncoding encodingInCache = ENCODER.getEncodingInCache();
    HFileReaderV2 reader = (HFileReaderV2) HFile.createReaderWithEncoding(fs,
        storeFilePath, cacheConf, encodingInCache);
    long offset = 0;
    cacheConf.getBlockCache().clearCache();
    underlyingCache.clearCache();
    while (offset < reader.getTrailer().getLoadOnOpenDataOffset()) {
      HFileBlock blockFromDisk = reader.readBlock(offset, -1, true, false,
              false, null, encodingInCache, null);
      BlockCacheKey key = new BlockCacheKey(reader.getName(), offset);
      assertNotNull(mockedL2Cache.getRawBlockBytes(key));
      cacheConf.getBlockCache().evictBlock(new BlockCacheKey(reader.getName(),
              offset));
      HFileBlock blockFromL2Cache = reader.readBlock(offset, -1, true, false,
              false, null, encodingInCache, null);
      assertEquals("Data in block from disk (" + blockFromDisk +
          ") should match data in block from cache (" + blockFromL2Cache +
          ").", blockFromL2Cache.getBufferWithHeader(),
          blockFromDisk.getBufferWithHeader());
      assertEquals(blockFromDisk, blockFromL2Cache);
      offset += blockFromDisk.getOnDiskSizeWithHeader();
    }
    assertTrue("This test must have read > 0 blocks", offset > 0);
  }

  @Test
  public void testOnlinePolicyChanges() {
    boolean oldL2CacheDataOnWrite = cacheConf.shouldL2CacheDataOnWrite();
    boolean oldL2EvictOnPromotion = cacheConf.shouldL2EvictOnPromotion();
    boolean oldL2EvictOnClose = cacheConf.shouldL2EvictOnClose();

    Configuration newConf = new Configuration(conf);
    newConf.setBoolean(CacheConfig.L2_CACHE_BLOCKS_ON_FLUSH_KEY,
            !oldL2CacheDataOnWrite);
    newConf.setBoolean(CacheConfig.L2_EVICT_ON_PROMOTION_KEY,
            !oldL2EvictOnPromotion);
    newConf.setBoolean(CacheConfig.L2_EVICT_ON_CLOSE_KEY,
            !oldL2EvictOnClose);
    cacheConf.notifyOnChange(newConf);

    assertNotSame("L2 caching on flush should be negated",
            oldL2CacheDataOnWrite,
            cacheConf.shouldL2CacheDataOnWrite());
    assertNotSame("L2 eviction on promotion should be negated",
            oldL2EvictOnPromotion,
            cacheConf.shouldL2EvictOnPromotion());
    assertNotSame("L2 eviction on close should be negated",
            oldL2EvictOnClose,
            cacheConf.shouldL2EvictOnClose());
  }

  @Test
  public void testOnlineCacheDisable() {
    Configuration newConf = new Configuration(conf);
    newConf.setFloat(CacheConfig.L2_BUCKET_CACHE_SIZE_KEY, 0F);

    assertTrue(cacheConf.isL2CacheEnabled());
    cacheConf.notifyOnChange(newConf);
    assertFalse(cacheConf.isL2CacheEnabled());
  }

  @Test
  public void shouldUpdatePerBlockCategoryMetrics() throws Exception {
    Map<String, Long> snapshot = SchemaMetrics.getMetricsSnapshot();
    writeStoreFile();

    // Get an unknown table and CF.
    SchemaConfigured unknownSchema = new SchemaConfigured(conf, null);
    // Get the test table and CF.
    SchemaConfigured testSchema = new SchemaConfigured(conf, storeFilePath);

    assertEquals(
            "Unknown schema DATA category size in L2 cache should be zero",
            0, getL2CacheSize(unknownSchema, BlockType.BlockCategory.DATA));
    assertTrue(
            "Test schema DATA category size in L2 cache should not be zero",
            getL2CacheSize(testSchema, BlockType.BlockCategory.DATA) > 0);
    // Validate that the per-cf-category metrics add up with the all-cf-category
    // metrics.
    SchemaMetrics.validateMetricChanges(snapshot);

    readAndEvictBlocksFromL2Cache();

    assertTrue("Test schema should have L2 cache hits",
            getL2CacheHitCount(testSchema, BlockType.BlockCategory.DATA) > 0);
    assertEquals("L2 cache should be empty", 0,
            underlyingCache.getBlockCount());
    assertEquals("DATA category size in L2 cache should be zero", 0,
            getL2CacheSize(testSchema, BlockType.BlockCategory.DATA));
    SchemaMetrics.validateMetricChanges(snapshot);
  }

  private void readAndEvictBlocksFromL2Cache() throws IOException {
    long offset = 0;
    HFileReaderV2 reader = (HFileReaderV2) HFile.createReaderWithEncoding(fs,
            storeFilePath, cacheConf, ENCODER.getEncodingInCache());
    // Clear the BlockCache to make sure reads are satisfied from L2 cache or
    // disk.
    cacheConf.getBlockCache().clearCache();
    while (offset < reader.getTrailer().getLoadOnOpenDataOffset()) {
      HFileBlock block = reader.readBlock(offset, -1, true, false,
              false, null, reader.getEffectiveEncodingInCache(false), null);
      mockedL2Cache.evictRawBlock(new BlockCacheKey(reader.getName(), offset));
      offset += block.getOnDiskSizeWithHeader();
    }
  }

  private long getL2CacheSize(SchemaConfigured schema,
                              BlockType.BlockCategory category) {
    return HRegion.getNumericPersistentMetric(
            schema.getSchemaMetrics().getBlockMetricName(category, false,
                    SchemaMetrics.BlockMetricType.L2_CACHE_SIZE));
  }

  private long getL2CacheHitCount(SchemaConfigured schema,
                                  BlockType.BlockCategory category) {
    return HRegion.getNumericMetric(
            schema.getSchemaMetrics().getBlockMetricName(category, false,
                    SchemaMetrics.BlockMetricType.L2_CACHE_HIT));
  }

  private void writeStoreFile() throws IOException {
    // Generate a random family name in order to keep different tests apart.
    // This is important for the metrics test.
    family = new HColumnDescriptor(
            Integer.toHexString(new Random().nextInt(1023)));
    region = TEST_UTIL.createTestRegion(TestL2BucketCache.class.getSimpleName(),
            family);
    Path storeHomeDir = Store.getStoreHomedir(region.getRegionDir(),
            region.getRegionInfo().getEncodedName(), family.getName());
    StoreFile.Writer sfw = new StoreFile.WriterBuilder(conf, cacheConf, fs,
        DATA_BLOCK_SIZE)
        .withOutputDir(storeHomeDir)
        .withCompression(Compression.Algorithm.GZ)
        .withDataBlockEncoder(ENCODER)
        .withComparator(KeyValue.COMPARATOR)
        .withBloomType(BLOOM_TYPE)
        .withMaxKeyCount(NUM_KV)
        .build();

    final int rowLen = 32;
    for (int i = 0; i < NUM_KV; ++i) {
      byte[] k = TestHFileWriterV2.randomOrderedKey(rand, i);
      byte[] v = TestHFileWriterV2.randomValue(rand);
      int cfLen = rand.nextInt(k.length - rowLen + 1);
      KeyValue kv = new KeyValue(
          k, 0, rowLen,
          k, rowLen, cfLen,
          k, rowLen + cfLen, k.length - rowLen - cfLen,
          rand.nextLong(),
          CreateRandomStoreFile.generateKeyType(rand),
          v, 0, v.length);
      sfw.append(kv);
    }
    sfw.close();
    storeFilePath = sfw.getPath();
  }

  // Mocked implementation which allows reads to be enabled and disabled
  // at run time during the tests. Adds additional trace logging that can
  // enabled during unit tests for further debugging.
  private static class MockedL2Cache implements L2Cache {

    final L2BucketCache underlying;
    final AtomicBoolean enableReads = new AtomicBoolean(true);

    MockedL2Cache(BucketCache underlying) throws IOException {
      this.underlying = new L2BucketCache(underlying);
    }

    @Override
    public byte[] getRawBlockBytes(BlockCacheKey key) {
      byte[] ret = null;
      if (enableReads.get()) {
        ret = underlying.getRawBlockBytes(key);
        if (LOG.isTraceEnabled()) {
          LOG.trace("Cache " + (ret == null ?"miss":"hit")  +
              " for hfileName=" + key.getHfileName() +
              ", offset=" + key.getOffset());
        }
      }
      return ret;
    }

    @Override
    public boolean cacheRawBlock(BlockCacheKey cacheKey,
                                 RawHFileBlock rawBlock) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Caching " + rawBlock.getData().length + " bytes, hfileName=" +
            cacheKey.getHfileName() + ", offset=" + cacheKey.getOffset());
      }
      return underlying.cacheRawBlock(cacheKey, rawBlock);
    }

    @Override
    public boolean evictRawBlock(BlockCacheKey cacheKey) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Evicting " + cacheKey);
      }
      return underlying.evictRawBlock(cacheKey);
    }

    @Override
    public int evictBlocksByHfileName(String hfileName) {
      return underlying.evictBlocksByHfileName(hfileName);
    }

    @Override
    public boolean isEnabled() {
      return underlying.isEnabled();
    }

    @Override
    public void shutdown() {
      underlying.shutdown();
    }
  }
}
