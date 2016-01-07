/*
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests {@link HFile} cache-on-write functionality for the following block
 * types: data blocks, non-root index blocks, and Bloom filter blocks.
 */
@RunWith(Parameterized.class)
@Category(MediumTests.class)
public class TestCacheOnWrite {

  private static final Log LOG = LogFactory.getLog(TestCacheOnWrite.class);

  private static final HBaseTestingUtility TEST_UTIL =
    new HBaseTestingUtility();
  private Configuration conf;
  private CacheConfig cacheConf;
  private FileSystem fs;
  private Random rand = new Random(12983177L);
  private Path storeFilePath;
  private BlockCache blockCache;
  private String testDescription;

  private final CacheOnWriteType cowType;
  private final Compression.Algorithm compress;
  private final BlockEncoderTestType encoderType;
  private final HFileDataBlockEncoder encoder;

  private static final int DATA_BLOCK_SIZE = 2048;
  private static final int NUM_KV = 25000;
  private static final int INDEX_BLOCK_SIZE = 512;
  private static final int BLOOM_BLOCK_SIZE = 4096;
  private static final BloomType BLOOM_TYPE = StoreFile.BloomType.ROWCOL;
  private static final ChecksumType CKTYPE = ChecksumType.CRC32;
  private static final int CKBYTES = 512;

  /** The number of valid key types possible in a store file */
  private static final int NUM_VALID_KEY_TYPES =
      KeyValue.Type.values().length - 2;

  private static enum CacheOnWriteType {
    DATA_BLOCKS(CacheConfig.CACHE_BLOCKS_ON_WRITE_KEY,
        BlockType.DATA, BlockType.ENCODED_DATA),
    BLOOM_BLOCKS(CacheConfig.CACHE_BLOOM_BLOCKS_ON_WRITE_KEY,
        BlockType.BLOOM_CHUNK),
    INDEX_BLOCKS(CacheConfig.CACHE_INDEX_BLOCKS_ON_WRITE_KEY,
        BlockType.LEAF_INDEX, BlockType.INTERMEDIATE_INDEX);

    private final String confKey;
    private final BlockType blockType1;
    private final BlockType blockType2;

    private CacheOnWriteType(String confKey, BlockType blockType) {
      this(confKey, blockType, blockType);
    }

    private CacheOnWriteType(String confKey, BlockType blockType1,
        BlockType blockType2) {
      this.blockType1 = blockType1;
      this.blockType2 = blockType2;
      this.confKey = confKey;
    }

    public boolean shouldBeCached(BlockType blockType) {
      return blockType == blockType1 || blockType == blockType2;
    }

    public void modifyConf(Configuration conf) {
      for (CacheOnWriteType cowType : CacheOnWriteType.values()) {
        conf.setBoolean(cowType.confKey, cowType == this);
      }
    }

  }

  private static final DataBlockEncoding ENCODING_ALGO =
      DataBlockEncoding.PREFIX;

  /** Provides fancy names for three combinations of two booleans */
  private static enum BlockEncoderTestType {
    NO_BLOCK_ENCODING(false, false),
    BLOCK_ENCODING_IN_CACHE_ONLY(false, true),
    BLOCK_ENCODING_EVERYWHERE(true, true);

    private final boolean encodeOnDisk;
    private final boolean encodeInCache;

    BlockEncoderTestType(boolean encodeOnDisk, boolean encodeInCache) {
      this.encodeOnDisk = encodeOnDisk;
      this.encodeInCache = encodeInCache;
    }

    public HFileDataBlockEncoder getEncoder() {
      return new HFileDataBlockEncoderImpl(
          encodeOnDisk ? ENCODING_ALGO : DataBlockEncoding.NONE,
          encodeInCache ? ENCODING_ALGO : DataBlockEncoding.NONE);
    }
  }

  public TestCacheOnWrite(CacheOnWriteType cowType,
      Compression.Algorithm compress, BlockEncoderTestType encoderType) {
    this.cowType = cowType;
    this.compress = compress;
    this.encoderType = encoderType;
    this.encoder = encoderType.getEncoder();
    testDescription = "[cacheOnWrite=" + cowType + ", compress=" + compress + 
        ", encoderType=" + encoderType + "]";
    System.out.println(testDescription);
  }

  @Parameters
  public static Collection<Object[]> getParameters() {
    List<Object[]> cowTypes = new ArrayList<Object[]>();
    for (CacheOnWriteType cowType : CacheOnWriteType.values()) {
      for (Compression.Algorithm compress :
           HBaseTestingUtility.COMPRESSION_ALGORITHMS) {
        for (BlockEncoderTestType encoderType :
             BlockEncoderTestType.values()) {
          cowTypes.add(new Object[] { cowType, compress, encoderType });
        }
      }
    }
    return cowTypes;
  }

  @Before
  public void setUp() throws IOException {
    conf = TEST_UTIL.getConfiguration();
    conf.setInt(HFile.FORMAT_VERSION_KEY, HFile.MAX_FORMAT_VERSION);
    conf.setInt(HFileBlockIndex.MAX_CHUNK_SIZE_KEY, INDEX_BLOCK_SIZE);
    conf.setInt(BloomFilterFactory.IO_STOREFILE_BLOOM_BLOCK_SIZE,
        BLOOM_BLOCK_SIZE);
    conf.setBoolean(CacheConfig.CACHE_BLOCKS_ON_WRITE_KEY,
        cowType.shouldBeCached(BlockType.DATA));
    conf.setBoolean(CacheConfig.CACHE_INDEX_BLOCKS_ON_WRITE_KEY,
        cowType.shouldBeCached(BlockType.LEAF_INDEX));
    conf.setBoolean(CacheConfig.CACHE_BLOOM_BLOCKS_ON_WRITE_KEY,
        cowType.shouldBeCached(BlockType.BLOOM_CHUNK));
    cowType.modifyConf(conf);
    fs = HFileSystem.get(conf);
    cacheConf = new CacheConfig(conf);
    blockCache = cacheConf.getBlockCache();
  }

  @After
  public void tearDown() {
    cacheConf = new CacheConfig(conf);
    blockCache = cacheConf.getBlockCache();
  }

  @Test
  public void testStoreFileCacheOnWrite() throws IOException {
    writeStoreFile();
    readStoreFile();
  }

  private void readStoreFile() throws IOException {
    HFileReaderV2 reader = (HFileReaderV2) HFile.createReaderWithEncoding(fs,
        storeFilePath, cacheConf, encoder.getEncodingInCache());
    LOG.info("HFile information: " + reader);
    final boolean cacheBlocks = false;
    final boolean pread = false;
    HFileScanner scanner = reader.getScanner(cacheBlocks, pread);
    assertTrue(testDescription, scanner.seekTo());

    long offset = 0;
    HFileBlock prevBlock = null;
    EnumMap<BlockType, Integer> blockCountByType =
        new EnumMap<BlockType, Integer>(BlockType.class);

    DataBlockEncoding encodingInCache =
        encoderType.getEncoder().getEncodingInCache();
    while (offset < reader.getTrailer().getLoadOnOpenDataOffset()) {
      long onDiskSize = -1;
      if (prevBlock != null) {
         onDiskSize = prevBlock.getNextBlockOnDiskSizeWithHeader();
      }
      // Flags: don't cache the block, use pread, this is not a compaction.
      // Also, pass null for expected block type to avoid checking it.
      HFileBlock block = reader.readBlock(offset, onDiskSize, false, true,
          false, null);
      BlockCacheKey blockCacheKey = new BlockCacheKey(reader.getName(),
          offset, encodingInCache, block.getBlockType());
      boolean isCached = blockCache.getBlock(blockCacheKey, true, false) != null;
      boolean shouldBeCached = cowType.shouldBeCached(block.getBlockType());
      if (shouldBeCached != isCached) {
        throw new AssertionError(
            "shouldBeCached: " + shouldBeCached+ "\n" +
            "isCached: " + isCached + "\n" +
            "Test description: " + testDescription + "\n" +
            "block: " + block + "\n" +
            "encodingInCache: " + encodingInCache + "\n" +
            "blockCacheKey: " + blockCacheKey);
      }
      prevBlock = block;
      offset += block.getOnDiskSizeWithHeader();
      BlockType bt = block.getBlockType();
      Integer count = blockCountByType.get(bt);
      blockCountByType.put(bt, (count == null ? 0 : count) + 1);
    }

    LOG.info("Block count by type: " + blockCountByType);
    String countByType = blockCountByType.toString();
    BlockType cachedDataBlockType =
        encoderType.encodeInCache ? BlockType.ENCODED_DATA : BlockType.DATA;
    assertEquals("{" + cachedDataBlockType
        + "=1379, LEAF_INDEX=173, BLOOM_CHUNK=9, INTERMEDIATE_INDEX=24}",
        countByType);

    reader.close();
  }

  public static KeyValue.Type generateKeyType(Random rand) {
    if (rand.nextBoolean()) {
      // Let's make half of KVs puts.
      return KeyValue.Type.Put;
    } else {
      KeyValue.Type keyType =
          KeyValue.Type.values()[1 + rand.nextInt(NUM_VALID_KEY_TYPES)];
      if (keyType == KeyValue.Type.Minimum || keyType == KeyValue.Type.Maximum)
      {
        throw new RuntimeException("Generated an invalid key type: " + keyType
            + ". " + "Probably the layout of KeyValue.Type has changed.");
      }
      return keyType;
    }
  }

  public void writeStoreFile() throws IOException {
    Path storeFileParentDir = new Path(TEST_UTIL.getDataTestDir(),
        "test_cache_on_write");
    StoreFile.Writer sfw = new StoreFile.WriterBuilder(conf, cacheConf, fs,
        DATA_BLOCK_SIZE)
            .withOutputDir(storeFileParentDir)
            .withCompression(compress)
            .withDataBlockEncoder(encoder)
            .withComparator(KeyValue.COMPARATOR)
            .withBloomType(BLOOM_TYPE)
            .withMaxKeyCount(NUM_KV)
            .withChecksumType(CKTYPE)
            .withBytesPerChecksum(CKBYTES)
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
          generateKeyType(rand),
          v, 0, v.length);
      sfw.append(kv);
    }

    sfw.close();
    storeFilePath = sfw.getPath();
  }

  @Test
  public void testNotCachingDataBlocksDuringCompaction() throws IOException {
    // TODO: need to change this test if we add a cache size threshold for
    // compactions, or if we implement some other kind of intelligent logic for
    // deciding what blocks to cache-on-write on compaction.
    final String table = "CompactionCacheOnWrite";
    final String cf = "myCF";
    final byte[] cfBytes = Bytes.toBytes(cf);
    final int maxVersions = 3;
    HRegion region = TEST_UTIL.createTestRegion(table, 
        new HColumnDescriptor(cf)
            .setCompressionType(compress)
            .setBloomFilterType(BLOOM_TYPE)
            .setMaxVersions(maxVersions)
            .setDataBlockEncoding(encoder.getEncodingInCache())
            .setEncodeOnDisk(encoder.getEncodingOnDisk() !=
                DataBlockEncoding.NONE)
    );
    int rowIdx = 0;
    long ts = EnvironmentEdgeManager.currentTimeMillis();
    for (int iFile = 0; iFile < 5; ++iFile) {
      for (int iRow = 0; iRow < 500; ++iRow) {
        String rowStr = "" + (rowIdx * rowIdx * rowIdx) + "row" + iFile + "_" + 
            iRow;
        Put p = new Put(Bytes.toBytes(rowStr));
        ++rowIdx;
        for (int iCol = 0; iCol < 10; ++iCol) {
          String qualStr = "col" + iCol;
          String valueStr = "value_" + rowStr + "_" + qualStr;
          for (int iTS = 0; iTS < 5; ++iTS) {
            p.add(cfBytes, Bytes.toBytes(qualStr), ts++,
                Bytes.toBytes(valueStr));
          }
        }
        region.put(p);
      }
      region.flushcache();
    }
    LruBlockCache blockCache =
        (LruBlockCache) new CacheConfig(conf).getBlockCache();
    blockCache.clearCache();
    assertEquals(0, blockCache.getBlockTypeCountsForTest().size());
    Map<String, Long> metricsBefore = SchemaMetrics.getMetricsSnapshot();
    region.compactStores();
    LOG.debug("compactStores() returned");
    SchemaMetrics.validateMetricChanges(metricsBefore);
    Map<String, Long> compactionMetrics = SchemaMetrics.diffMetrics(
        metricsBefore, SchemaMetrics.getMetricsSnapshot());
    LOG.debug(SchemaMetrics.formatMetrics(compactionMetrics));
    Map<BlockType, Integer> blockTypesInCache =
        blockCache.getBlockTypeCountsForTest();
    LOG.debug("Block types in cache: " + blockTypesInCache);
    assertNull(blockTypesInCache.get(BlockType.DATA));
    region.close();
    blockCache.shutdown();
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

