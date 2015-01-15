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
import static org.junit.Assert.assertFalse;
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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.StoreFile;
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

  private static final HBaseTestingUtility TEST_UTIL = HBaseTestingUtility.createLocalHTU();
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
  private final boolean cacheCompressedData;

  private static final int DATA_BLOCK_SIZE = 2048;
  private static final int NUM_KV = 25000;
  private static final int INDEX_BLOCK_SIZE = 512;
  private static final int BLOOM_BLOCK_SIZE = 4096;
  private static final BloomType BLOOM_TYPE = BloomType.ROWCOL;
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
    NO_BLOCK_ENCODING_NOOP(true, false),
    NO_BLOCK_ENCODING(false, false),
    BLOCK_ENCODING_EVERYWHERE(false, true);

    private final boolean noop;
    private final boolean encode;

    BlockEncoderTestType(boolean noop, boolean encode) {
      this.encode = encode;
      this.noop = noop;
    }

    public HFileDataBlockEncoder getEncoder() {
      return noop ? NoOpDataBlockEncoder.INSTANCE : new HFileDataBlockEncoderImpl(
        encode ? ENCODING_ALGO : DataBlockEncoding.NONE);
    }
  }

  public TestCacheOnWrite(CacheOnWriteType cowType, Compression.Algorithm compress,
      BlockEncoderTestType encoderType, boolean cacheCompressedData, BlockCache blockCache) {
    this.cowType = cowType;
    this.compress = compress;
    this.encoderType = encoderType;
    this.encoder = encoderType.getEncoder();
    this.cacheCompressedData = cacheCompressedData;
    this.blockCache = blockCache;
    testDescription = "[cacheOnWrite=" + cowType + ", compress=" + compress +
        ", encoderType=" + encoderType + ", cacheCompressedData=" + cacheCompressedData +
        ", blockCache=" + blockCache.getClass().getSimpleName() + "]";
    System.out.println(testDescription);
  }

  private static List<BlockCache> getBlockCaches() throws IOException {
    Configuration conf = TEST_UTIL.getConfiguration();
    List<BlockCache> blockcaches = new ArrayList<BlockCache>();
    // default
    blockcaches.add(new CacheConfig(conf).getBlockCache());

    // memory
    BlockCache lru = new LruBlockCache(128 * 1024 * 1024, 64 * 1024, TEST_UTIL.getConfiguration());
    blockcaches.add(lru);

    // bucket cache
    FileSystem.get(conf).mkdirs(TEST_UTIL.getDataTestDir());
    int[] bucketSizes = {INDEX_BLOCK_SIZE, DATA_BLOCK_SIZE, BLOOM_BLOCK_SIZE, 64 * 1024 };
    BlockCache bucketcache =
        new BucketCache("file:" + TEST_UTIL.getDataTestDir() + "/bucket.data",
            256 * 1024 * 1024, 64 * 1024, bucketSizes, 3, 256 * 1024, null);
    blockcaches.add(bucketcache);
    return blockcaches;
  }

  @Parameters
  public static Collection<Object[]> getParameters() throws IOException {
    List<Object[]> cowTypes = new ArrayList<Object[]>();
    for (BlockCache blockache : getBlockCaches()) {
      for (CacheOnWriteType cowType : CacheOnWriteType.values()) {
        for (Compression.Algorithm compress : HBaseTestingUtility.COMPRESSION_ALGORITHMS) {
          for (BlockEncoderTestType encoderType : BlockEncoderTestType.values()) {
            for (boolean cacheCompressedData : new boolean[] { false, true }) {
              cowTypes.add(new Object[] { cowType, compress, encoderType, cacheCompressedData, blockache});
            }
          }
        }
      }
    }
    return cowTypes;
  }

  @Before
  public void setUp() throws IOException {
    conf = TEST_UTIL.getConfiguration();
    this.conf.set("dfs.datanode.data.dir.perm", "700");
    conf.setInt(HFile.FORMAT_VERSION_KEY, HFile.MAX_FORMAT_VERSION);
    conf.setInt(HFileBlockIndex.MAX_CHUNK_SIZE_KEY, INDEX_BLOCK_SIZE);
    conf.setInt(BloomFilterFactory.IO_STOREFILE_BLOOM_BLOCK_SIZE,
        BLOOM_BLOCK_SIZE);
    conf.setBoolean(CacheConfig.CACHE_DATA_BLOCKS_COMPRESSED_KEY, cacheCompressedData);
    cowType.modifyConf(conf);
    fs = HFileSystem.get(conf);
    cacheConf =
        new CacheConfig(blockCache, true, true, cowType.shouldBeCached(BlockType.DATA),
        cowType.shouldBeCached(BlockType.LEAF_INDEX),
        cowType.shouldBeCached(BlockType.BLOOM_CHUNK), false, cacheCompressedData, true);
  }

  @After
  public void tearDown() {
  }

  @Test
  public void testStoreFileCacheOnWrite() throws IOException {
    testStoreFileCacheOnWriteInternals(false);
    testStoreFileCacheOnWriteInternals(true);
  }

  protected void testStoreFileCacheOnWriteInternals(boolean useTags) throws IOException {
    writeStoreFile(useTags);
    readStoreFile(useTags);
  }

  private void readStoreFile(boolean useTags) throws IOException {
    AbstractHFileReader reader;
    if (useTags) {
        reader = (HFileReaderV3) HFile.createReader(fs, storeFilePath, cacheConf, conf);
    } else {
        reader = (HFileReaderV2) HFile.createReader(fs, storeFilePath, cacheConf, conf);
    }
    LOG.info("HFile information: " + reader);
    HFileContext meta = new HFileContextBuilder().withCompression(compress)
      .withBytesPerCheckSum(CKBYTES).withChecksumType(ChecksumType.NULL)
      .withBlockSize(DATA_BLOCK_SIZE).withDataBlockEncoding(encoder.getDataBlockEncoding())
      .withIncludesTags(useTags).build();
    final boolean cacheBlocks = false;
    final boolean pread = false;
    HFileScanner scanner = reader.getScanner(cacheBlocks, pread);
    assertTrue(testDescription, scanner.seekTo());

    long offset = 0;
    HFileBlock prevBlock = null;
    EnumMap<BlockType, Integer> blockCountByType =
        new EnumMap<BlockType, Integer>(BlockType.class);

    DataBlockEncoding encodingInCache =
        encoderType.getEncoder().getDataBlockEncoding();
    while (offset < reader.getTrailer().getLoadOnOpenDataOffset()) {
      long onDiskSize = -1;
      if (prevBlock != null) {
         onDiskSize = prevBlock.getNextBlockOnDiskSizeWithHeader();
      }
      // Flags: don't cache the block, use pread, this is not a compaction.
      // Also, pass null for expected block type to avoid checking it.
      HFileBlock block = reader.readBlock(offset, onDiskSize, false, true,
          false, true, null);
      BlockCacheKey blockCacheKey = new BlockCacheKey(reader.getName(),
      offset, encodingInCache, block.getBlockType());
      HFileBlock fromCache = (HFileBlock) blockCache.getBlock(blockCacheKey, true, false, true);
      boolean isCached = fromCache != null;
      boolean shouldBeCached = cowType.shouldBeCached(block.getBlockType());
      assertTrue("shouldBeCached: " + shouldBeCached+ "\n" +
          "isCached: " + isCached + "\n" +
          "Test description: " + testDescription + "\n" +
          "block: " + block + "\n" +
          "encodingInCache: " + encodingInCache + "\n" +
          "blockCacheKey: " + blockCacheKey,
        shouldBeCached == isCached);
      if (isCached) {
        if (cacheConf.shouldCacheCompressed(fromCache.getBlockType().getCategory())) {
          if (compress != Compression.Algorithm.NONE) {
            assertFalse(fromCache.isUnpacked());
          }
          fromCache = fromCache.unpack(meta, reader.getUncachedBlockReader());
        } else {
          assertTrue(fromCache.isUnpacked());
        }
        // block we cached at write-time and block read from file should be identical
        assertEquals(block.getChecksumType(), fromCache.getChecksumType());
        assertEquals(block.getBlockType(), fromCache.getBlockType());
        if (block.getBlockType() == BlockType.ENCODED_DATA) {
          assertEquals(block.getDataBlockEncodingId(), fromCache.getDataBlockEncodingId());
          assertEquals(block.getDataBlockEncoding(), fromCache.getDataBlockEncoding());
        }
        assertEquals(block.getOnDiskSizeWithHeader(), fromCache.getOnDiskSizeWithHeader());
        assertEquals(block.getOnDiskSizeWithoutHeader(), fromCache.getOnDiskSizeWithoutHeader());
        assertEquals(
          block.getUncompressedSizeWithoutHeader(), fromCache.getUncompressedSizeWithoutHeader());
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
        encoderType.encode ? BlockType.ENCODED_DATA : BlockType.DATA;
    if (useTags) {
      assertEquals("{" + cachedDataBlockType
          + "=1550, LEAF_INDEX=173, BLOOM_CHUNK=9, INTERMEDIATE_INDEX=20}", countByType);
    } else {
      assertEquals("{" + cachedDataBlockType
          + "=1379, LEAF_INDEX=154, BLOOM_CHUNK=9, INTERMEDIATE_INDEX=18}", countByType);
    }

    // iterate all the keyvalue from hfile
    while (scanner.next()) {
      Cell cell = scanner.getKeyValue();
    }
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

  public void writeStoreFile(boolean useTags) throws IOException {
    if(useTags) {
      TEST_UTIL.getConfiguration().setInt("hfile.format.version", 3);
    } else {
      TEST_UTIL.getConfiguration().setInt("hfile.format.version", 2);
    }
    Path storeFileParentDir = new Path(TEST_UTIL.getDataTestDir(),
        "test_cache_on_write");
    HFileContext meta = new HFileContextBuilder().withCompression(compress)
        .withBytesPerCheckSum(CKBYTES).withChecksumType(ChecksumType.NULL)
        .withBlockSize(DATA_BLOCK_SIZE).withDataBlockEncoding(encoder.getDataBlockEncoding())
        .withIncludesTags(useTags).build();
    StoreFile.Writer sfw = new StoreFile.WriterBuilder(conf, cacheConf, fs)
        .withOutputDir(storeFileParentDir).withComparator(KeyValue.COMPARATOR)
        .withFileContext(meta)
        .withBloomType(BLOOM_TYPE).withMaxKeyCount(NUM_KV).build();

    final int rowLen = 32;
    for (int i = 0; i < NUM_KV; ++i) {
      byte[] k = TestHFileWriterV2.randomOrderedKey(rand, i);
      byte[] v = TestHFileWriterV2.randomValue(rand);
      int cfLen = rand.nextInt(k.length - rowLen + 1);
      KeyValue kv;
      if(useTags) {
        Tag t = new Tag((byte) 1, "visibility");
        List<Tag> tagList = new ArrayList<Tag>();
        tagList.add(t);
        Tag[] tags = new Tag[1];
        tags[0] = t;
        kv = new KeyValue(
            k, 0, rowLen,
            k, rowLen, cfLen,
            k, rowLen + cfLen, k.length - rowLen - cfLen,
            rand.nextLong(),
            generateKeyType(rand),
            v, 0, v.length, tagList);
      } else {
        kv = new KeyValue(
          k, 0, rowLen,
          k, rowLen, cfLen,
          k, rowLen + cfLen, k.length - rowLen - cfLen,
          rand.nextLong(),
          generateKeyType(rand),
          v, 0, v.length);
      }
      sfw.append(kv);
    }

    sfw.close();
    storeFilePath = sfw.getPath();
  }

  @Test
  public void testNotCachingDataBlocksDuringCompaction() throws IOException {
    testNotCachingDataBlocksDuringCompactionInternals(false);
    testNotCachingDataBlocksDuringCompactionInternals(true);
  }

  protected void testNotCachingDataBlocksDuringCompactionInternals(boolean useTags) throws IOException {
    if (useTags) {
      TEST_UTIL.getConfiguration().setInt("hfile.format.version", 3);
    } else {
      TEST_UTIL.getConfiguration().setInt("hfile.format.version", 2);
    }
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
            .setDataBlockEncoding(encoder.getDataBlockEncoding())
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
            if (useTags) {
              Tag t = new Tag((byte) 1, "visibility");
              Tag[] tags = new Tag[1];
              tags[0] = t;
              KeyValue kv = new KeyValue(Bytes.toBytes(rowStr), cfBytes, Bytes.toBytes(qualStr),
                  HConstants.LATEST_TIMESTAMP, Bytes.toBytes(valueStr), tags);
              p.add(kv);
            } else {
              p.add(cfBytes, Bytes.toBytes(qualStr), ts++, Bytes.toBytes(valueStr));
            }
          }
        }
        p.setDurability(Durability.ASYNC_WAL);
        region.put(p);
      }
      region.flushcache();
    }
    LruBlockCache blockCache =
        (LruBlockCache) new CacheConfig(conf).getBlockCache();
    blockCache.clearCache();
    assertEquals(0, blockCache.getBlockTypeCountsForTest().size());
    region.compactStores();
    LOG.debug("compactStores() returned");

    Map<BlockType, Integer> blockTypesInCache =
        blockCache.getBlockTypeCountsForTest();
    LOG.debug("Block types in cache: " + blockTypesInCache);
    assertNull(blockTypesInCache.get(BlockType.ENCODED_DATA));
    assertNull(blockTypesInCache.get(BlockType.DATA));
    region.close();
    blockCache.shutdown();
  }
}

