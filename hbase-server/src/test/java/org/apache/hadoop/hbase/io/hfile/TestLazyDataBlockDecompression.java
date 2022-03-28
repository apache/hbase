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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;

/**
 * A kind of integration test at the intersection of {@link HFileBlock}, {@link CacheConfig},
 * and {@link LruBlockCache}.
 */
@Category({IOTests.class, SmallTests.class})
@RunWith(Parameterized.class)
public class TestLazyDataBlockDecompression {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestLazyDataBlockDecompression.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestLazyDataBlockDecompression.class);
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final Random RNG = new Random(9713312); // Just a fixed seed.

  private FileSystem fs;

  @Parameterized.Parameter(0)
  public boolean cacheOnWrite;

  @Parameterized.Parameters
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][] {
      { false },
      { true }
    });
  }

  @Before
  public void setUp() throws IOException {
    fs = FileSystem.get(TEST_UTIL.getConfiguration());
  }

  @After
  public void tearDown() {
    fs = null;
  }

  /**
   * Write {@code entryCount} random keyvalues to a new HFile at {@code path}. Returns the row
   * bytes of the KeyValues written, in the order they were written.
   */
  private static void writeHFile(Configuration conf, CacheConfig cc, FileSystem fs, Path path,
      HFileContext cxt, int entryCount) throws IOException {
    HFile.Writer writer = new HFile.WriterFactory(conf, cc)
        .withPath(fs, path)
        .withFileContext(cxt)
        .create();

    // write a bunch of random kvs
    final byte[] family = Bytes.toBytes("f");
    final byte[] qualifier = Bytes.toBytes("q");
    for (int i = 0; i < entryCount; i++) {
      byte[] keyBytes = RandomKeyValueUtil.randomOrderedKey(RNG, i);
      byte[] valueBytes = RandomKeyValueUtil.randomValue(RNG);
      // make a real keyvalue so that hfile tool can examine it
      writer.append(new KeyValue(keyBytes, family, qualifier, valueBytes));
    }
    writer.close();
  }

  /**
   * Read all blocks from {@code path} to populate {@code blockCache}.
   */
  private static void cacheBlocks(Configuration conf, CacheConfig cacheConfig, FileSystem fs,
      Path path, HFileContext cxt) throws IOException {
    FSDataInputStreamWrapper fsdis = new FSDataInputStreamWrapper(fs, path);
    long fileSize = fs.getFileStatus(path).getLen();
    FixedFileTrailer trailer =
      FixedFileTrailer.readFromStream(fsdis.getStream(false), fileSize);
    ReaderContext context = new ReaderContextBuilder()
        .withFilePath(path)
        .withFileSize(fileSize)
        .withFileSystem(fsdis.getHfs())
        .withInputStreamWrapper(fsdis)
        .build();
    HFileInfo fileInfo = new HFileInfo(context, conf);
    HFile.Reader reader = new HFilePreadReader(context, fileInfo, cacheConfig, conf);
    fileInfo.initMetaAndIndex(reader);
    long offset = trailer.getFirstDataBlockOffset(),
      max = trailer.getLastDataBlockOffset();
    List<HFileBlock> blocks = new ArrayList<>(4);
    HFileBlock block;
    while (offset <= max) {
      block = reader.readBlock(offset, -1, /* cacheBlock */ true, /* pread */ false,
      /* isCompaction */ false, /* updateCacheMetrics */ true, null, null);
      offset += block.getOnDiskSizeWithHeader();
      blocks.add(block);
    }
    LOG.info("read " + Iterables.toString(blocks));
    reader.close();
  }

  @Test
  public void testCompressionIncreasesEffectiveBlockCacheSize() throws Exception {
    // enough room for 2 uncompressed block
    int maxSize = (int) (HConstants.DEFAULT_BLOCKSIZE * 2.1);
    Path hfilePath = new Path(TEST_UTIL.getDataTestDir(),
      "testCompressionIncreasesEffectiveBlockcacheSize");
    HFileContext context = new HFileContextBuilder()
      .withCompression(Compression.Algorithm.GZ)
      .build();
    LOG.info("context=" + context);

    // setup cache with lazy-decompression disabled.
    Configuration lazyCompressDisabled = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
    lazyCompressDisabled.setBoolean(CacheConfig.CACHE_BLOCKS_ON_WRITE_KEY, cacheOnWrite);
    lazyCompressDisabled.setBoolean(CacheConfig.CACHE_BLOOM_BLOCKS_ON_WRITE_KEY, cacheOnWrite);
    lazyCompressDisabled.setBoolean(CacheConfig.CACHE_INDEX_BLOCKS_ON_WRITE_KEY, cacheOnWrite);
    lazyCompressDisabled.setBoolean(CacheConfig.CACHE_DATA_BLOCKS_COMPRESSED_KEY, false);
    CacheConfig cc = new CacheConfig(lazyCompressDisabled,
        new LruBlockCache(maxSize, HConstants.DEFAULT_BLOCKSIZE, false, lazyCompressDisabled));
    assertFalse(cc.shouldCacheDataCompressed());
    assertFalse(cc.isCombinedBlockCache());
    LruBlockCache disabledBlockCache = (LruBlockCache) cc.getBlockCache().get();
    LOG.info("disabledBlockCache=" + disabledBlockCache);
    assertEquals("test inconsistency detected.", maxSize, disabledBlockCache.getMaxSize());
    assertTrue("eviction thread spawned unintentionally.",
      disabledBlockCache.getEvictionThread() == null);
    assertEquals("freshly created blockcache contains blocks.",
      0, disabledBlockCache.getBlockCount());

    // 2000 kv's is ~3.6 full unencoded data blocks.
    // Requires a conf and CacheConfig but should not be specific to this instance's cache settings
    writeHFile(lazyCompressDisabled, cc, fs, hfilePath, context, 2000);

    // populate the cache
    cacheBlocks(lazyCompressDisabled, cc, fs, hfilePath, context);
    long disabledBlockCount = disabledBlockCache.getBlockCount();
    assertTrue("blockcache should contain blocks. disabledBlockCount=" + disabledBlockCount,
      disabledBlockCount > 0);
    long disabledEvictedCount = disabledBlockCache.getStats().getEvictedCount();
    for (Map.Entry<BlockCacheKey, LruCachedBlock> e :
      disabledBlockCache.getMapForTests().entrySet()) {
      HFileBlock block = (HFileBlock) e.getValue().getBuffer();
      assertTrue("found a packed block, block=" + block, block.isUnpacked());
    }

    // count blocks with lazy decompression
    Configuration lazyCompressEnabled = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
    lazyCompressEnabled.setBoolean(CacheConfig.CACHE_BLOCKS_ON_WRITE_KEY, cacheOnWrite);
    lazyCompressEnabled.setBoolean(CacheConfig.CACHE_BLOOM_BLOCKS_ON_WRITE_KEY, cacheOnWrite);
    lazyCompressEnabled.setBoolean(CacheConfig.CACHE_INDEX_BLOCKS_ON_WRITE_KEY, cacheOnWrite);
    lazyCompressEnabled.setBoolean(CacheConfig.CACHE_DATA_BLOCKS_COMPRESSED_KEY, true);
    cc = new CacheConfig(lazyCompressEnabled,
        new LruBlockCache(maxSize, HConstants.DEFAULT_BLOCKSIZE, false, lazyCompressEnabled));
    assertTrue("test improperly configured.", cc.shouldCacheDataCompressed());
    assertTrue(cc.getBlockCache().get() instanceof LruBlockCache);
    LruBlockCache enabledBlockCache = (LruBlockCache) cc.getBlockCache().get();
    LOG.info("enabledBlockCache=" + enabledBlockCache);
    assertEquals("test inconsistency detected", maxSize, enabledBlockCache.getMaxSize());
    assertTrue("eviction thread spawned unintentionally.",
      enabledBlockCache.getEvictionThread() == null);
    assertEquals("freshly created blockcache contains blocks.",
      0, enabledBlockCache.getBlockCount());

    cacheBlocks(lazyCompressEnabled, cc, fs, hfilePath, context);
    long enabledBlockCount = enabledBlockCache.getBlockCount();
    assertTrue("blockcache should contain blocks. enabledBlockCount=" + enabledBlockCount,
      enabledBlockCount > 0);
    long enabledEvictedCount = enabledBlockCache.getStats().getEvictedCount();
    int candidatesFound = 0;
    for (Map.Entry<BlockCacheKey, LruCachedBlock> e :
        enabledBlockCache.getMapForTests().entrySet()) {
      candidatesFound++;
      HFileBlock block = (HFileBlock) e.getValue().getBuffer();
      if (cc.shouldCacheCompressed(block.getBlockType().getCategory())) {
        assertFalse("found an unpacked block, block=" + block + ", block buffer capacity=" +
          block.getBufferWithoutHeader().capacity(), block.isUnpacked());
      }
    }
    assertTrue("did not find any candidates for compressed caching. Invalid test.",
      candidatesFound > 0);

    LOG.info("disabledBlockCount=" + disabledBlockCount + ", enabledBlockCount=" +
      enabledBlockCount);
    assertTrue("enabling compressed data blocks should increase the effective cache size. " +
      "disabledBlockCount=" + disabledBlockCount + ", enabledBlockCount=" +
      enabledBlockCount, disabledBlockCount < enabledBlockCount);

    LOG.info("disabledEvictedCount=" + disabledEvictedCount + ", enabledEvictedCount=" +
      enabledEvictedCount);
    assertTrue("enabling compressed data blocks should reduce the number of evictions. " +
      "disabledEvictedCount=" + disabledEvictedCount + ", enabledEvictedCount=" +
      enabledEvictedCount, enabledEvictedCount < disabledEvictedCount);
  }
}
