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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Tests {@link HFile} cache-on-write functionality for the following block
 * types: data blocks, non-root index blocks, and Bloom filter blocks.
 */
@RunWith(Parameterized.class)
@Category({IOTests.class, LargeTests.class})
public class TestCacheOnWrite {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCacheOnWrite.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestCacheOnWrite.class);

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
  private final boolean cacheCompressedData;

  private static final int DATA_BLOCK_SIZE = 2048;
  private static final int NUM_KV = 25000;
  private static final int INDEX_BLOCK_SIZE = 512;
  private static final int BLOOM_BLOCK_SIZE = 4096;
  private static final BloomType BLOOM_TYPE = BloomType.ROWCOL;
  private static final int CKBYTES = 512;


  private static final Set<BlockType> INDEX_BLOCK_TYPES = ImmutableSet.of(
    BlockType.INDEX_V1,
    BlockType.INTERMEDIATE_INDEX,
    BlockType.ROOT_INDEX,
    BlockType.LEAF_INDEX
  );
  private static final Set<BlockType> BLOOM_BLOCK_TYPES = ImmutableSet.of(
    BlockType.BLOOM_CHUNK,
    BlockType.GENERAL_BLOOM_META,
    BlockType.DELETE_FAMILY_BLOOM_META
  );
  private static final Set<BlockType> DATA_BLOCK_TYPES = ImmutableSet.of(
    BlockType.ENCODED_DATA,
    BlockType.DATA
  );

  // All test cases are supposed to generate files for compaction within this range
  private static final long CACHE_COMPACTION_LOW_THRESHOLD = 10L;
  private static final long CACHE_COMPACTION_HIGH_THRESHOLD = 1 * 1024 * 1024 * 1024L;

  /** The number of valid key types possible in a store file */
  private static final int NUM_VALID_KEY_TYPES =
      KeyValue.Type.values().length - 2;

  private enum CacheOnWriteType {
    DATA_BLOCKS(CacheConfig.CACHE_BLOCKS_ON_WRITE_KEY,
        BlockType.DATA, BlockType.ENCODED_DATA),
    BLOOM_BLOCKS(CacheConfig.CACHE_BLOOM_BLOCKS_ON_WRITE_KEY,
        BlockType.BLOOM_CHUNK),
    INDEX_BLOCKS(CacheConfig.CACHE_INDEX_BLOCKS_ON_WRITE_KEY,
        BlockType.LEAF_INDEX, BlockType.INTERMEDIATE_INDEX);

    private final String confKey;
    private final BlockType blockType1;
    private final BlockType blockType2;

    CacheOnWriteType(String confKey, BlockType blockType) {
      this(confKey, blockType, blockType);
    }

    CacheOnWriteType(String confKey, BlockType blockType1, BlockType blockType2) {
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

  public TestCacheOnWrite(CacheOnWriteType cowType, Compression.Algorithm compress,
      boolean cacheCompressedData, BlockCache blockCache) {
    this.cowType = cowType;
    this.compress = compress;
    this.cacheCompressedData = cacheCompressedData;
    this.blockCache = blockCache;
    testDescription = "[cacheOnWrite=" + cowType + ", compress=" + compress +
        ", cacheCompressedData=" + cacheCompressedData + "]";
    LOG.info(testDescription);
  }

  private static List<BlockCache> getBlockCaches() throws IOException {
    Configuration conf = TEST_UTIL.getConfiguration();
    List<BlockCache> blockcaches = new ArrayList<>();
    // default
    blockcaches.add(BlockCacheFactory.createBlockCache(conf));

    //set LruBlockCache.LRU_HARD_CAPACITY_LIMIT_FACTOR_CONFIG_NAME to 2.0f due to HBASE-16287
    TEST_UTIL.getConfiguration().setFloat(LruBlockCache.LRU_HARD_CAPACITY_LIMIT_FACTOR_CONFIG_NAME, 2.0f);
    // memory
    BlockCache lru = new LruBlockCache(128 * 1024 * 1024, 64 * 1024, TEST_UTIL.getConfiguration());
    blockcaches.add(lru);

    // bucket cache
    FileSystem.get(conf).mkdirs(TEST_UTIL.getDataTestDir());
    int[] bucketSizes =
        { INDEX_BLOCK_SIZE, DATA_BLOCK_SIZE, BLOOM_BLOCK_SIZE, 64 * 1024, 128 * 1024 };
    BlockCache bucketcache =
        new BucketCache("offheap", 128 * 1024 * 1024, 64 * 1024, bucketSizes, 5, 64 * 100, null);
    blockcaches.add(bucketcache);
    return blockcaches;
  }

  @Parameters
  public static Collection<Object[]> getParameters() throws IOException {
    List<Object[]> params = new ArrayList<>();
    for (BlockCache blockCache : getBlockCaches()) {
      for (CacheOnWriteType cowType : CacheOnWriteType.values()) {
        for (Compression.Algorithm compress : HBaseCommonTestingUtility.COMPRESSION_ALGORITHMS) {
          for (boolean cacheCompressedData : new boolean[] { false, true }) {
            params.add(new Object[] { cowType, compress, cacheCompressedData, blockCache });
          }
        }
      }
    }
    return params;
  }

  private void clearBlockCache(BlockCache blockCache) throws InterruptedException {
    if (blockCache instanceof LruBlockCache) {
      ((LruBlockCache) blockCache).clearCache();
    } else {
      // BucketCache may not return all cached blocks(blocks in write queue), so check it here.
      for (int clearCount = 0; blockCache.getBlockCount() > 0; clearCount++) {
        if (clearCount > 0) {
          LOG.warn("clear block cache " + blockCache + " " + clearCount + " times, "
              + blockCache.getBlockCount() + " blocks remaining");
          Thread.sleep(10);
        }
        for (CachedBlock block : Lists.newArrayList(blockCache)) {
          BlockCacheKey key = new BlockCacheKey(block.getFilename(), block.getOffset());
          // CombinedBucketCache may need evict two times.
          for (int evictCount = 0; blockCache.evictBlock(key); evictCount++) {
            if (evictCount > 1) {
              LOG.warn("evict block " + block + " in " + blockCache + " " + evictCount
                  + " times, maybe a bug here");
            }
          }
        }
      }
    }
  }

  @Before
  public void setUp() throws IOException {
    conf = TEST_UTIL.getConfiguration();
    this.conf.set("dfs.datanode.data.dir.perm", "700");
    conf.setInt(HFileBlockIndex.MAX_CHUNK_SIZE_KEY, INDEX_BLOCK_SIZE);
    conf.setInt(BloomFilterFactory.IO_STOREFILE_BLOOM_BLOCK_SIZE, BLOOM_BLOCK_SIZE);
    conf.setBoolean(CacheConfig.CACHE_DATA_BLOCKS_COMPRESSED_KEY, cacheCompressedData);
    cowType.modifyConf(conf);
    conf.setBoolean(CacheConfig.CACHE_BLOCKS_ON_WRITE_KEY, cowType.shouldBeCached(BlockType.DATA));
    conf.setBoolean(CacheConfig.CACHE_INDEX_BLOCKS_ON_WRITE_KEY,
        cowType.shouldBeCached(BlockType.LEAF_INDEX));
    conf.setBoolean(CacheConfig.CACHE_BLOOM_BLOCKS_ON_WRITE_KEY,
        cowType.shouldBeCached(BlockType.BLOOM_CHUNK));
    cacheConf = new CacheConfig(conf, blockCache);
    fs = HFileSystem.get(conf);
  }

  @After
  public void tearDown() throws IOException, InterruptedException {
    clearBlockCache(blockCache);
  }

  @AfterClass
  public static void afterClass() throws IOException {
    TEST_UTIL.cleanupTestDir();
  }

  private void testStoreFileCacheOnWriteInternals(boolean useTags) throws IOException {
    writeStoreFile(useTags);
    readStoreFile(useTags);
  }

  private void readStoreFile(boolean useTags) throws IOException {
    HFile.Reader reader = HFile.createReader(fs, storeFilePath, cacheConf, true, conf);
    LOG.info("HFile information: " + reader);
    HFileContext meta = new HFileContextBuilder().withCompression(compress)
      .withBytesPerCheckSum(CKBYTES).withChecksumType(ChecksumType.NULL)
      .withBlockSize(DATA_BLOCK_SIZE)
      .withDataBlockEncoding(NoOpDataBlockEncoder.INSTANCE.getDataBlockEncoding())
      .withIncludesTags(useTags).build();
    final boolean cacheBlocks = false;
    final boolean pread = false;
    HFileScanner scanner = reader.getScanner(cacheBlocks, pread);
    assertTrue(testDescription, scanner.seekTo());

    long offset = 0;
    EnumMap<BlockType, Integer> blockCountByType = new EnumMap<>(BlockType.class);

    DataBlockEncoding encodingInCache = NoOpDataBlockEncoder.INSTANCE.getDataBlockEncoding();
    List<Long> cachedBlocksOffset = new ArrayList<>();
    Map<Long, Pair<HFileBlock, HFileBlock>> cachedBlocks = new HashMap<>();
    while (offset < reader.getTrailer().getLoadOnOpenDataOffset()) {
      // Flags: don't cache the block, use pread, this is not a compaction.
      // Also, pass null for expected block type to avoid checking it.
      HFileBlock block = reader.readBlock(offset, -1, false, true, false, true, null,
          encodingInCache);
      BlockCacheKey blockCacheKey = new BlockCacheKey(reader.getName(), offset);
      HFileBlock fromCache = (HFileBlock) blockCache.getBlock(blockCacheKey, true, false, true);
      boolean isCached = fromCache != null;
      cachedBlocksOffset.add(offset);
      cachedBlocks.put(offset, fromCache == null ? null : Pair.newPair(block, fromCache));
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
        assertNotEquals(BlockType.ENCODED_DATA, block.getBlockType());
        assertEquals(block.getOnDiskSizeWithHeader(), fromCache.getOnDiskSizeWithHeader());
        assertEquals(block.getOnDiskSizeWithoutHeader(), fromCache.getOnDiskSizeWithoutHeader());
        assertEquals(
          block.getUncompressedSizeWithoutHeader(), fromCache.getUncompressedSizeWithoutHeader());
      }
      offset += block.getOnDiskSizeWithHeader();
      BlockType bt = block.getBlockType();
      Integer count = blockCountByType.get(bt);
      blockCountByType.put(bt, (count == null ? 0 : count) + 1);
    }

    LOG.info("Block count by type: " + blockCountByType);
    String countByType = blockCountByType.toString();
    if (useTags) {
      assertEquals("{" + BlockType.DATA
          + "=2663, LEAF_INDEX=297, BLOOM_CHUNK=9, INTERMEDIATE_INDEX=32}", countByType);
    } else {
      assertEquals("{" + BlockType.DATA
          + "=2498, LEAF_INDEX=278, BLOOM_CHUNK=9, INTERMEDIATE_INDEX=31}", countByType);
    }

    // iterate all the keyvalue from hfile
    while (scanner.next()) {
      scanner.getCell();
    }
    Iterator<Long> iterator = cachedBlocksOffset.iterator();
    while(iterator.hasNext()) {
      Long entry = iterator.next();
      BlockCacheKey blockCacheKey = new BlockCacheKey(reader.getName(),
          entry);
      Pair<HFileBlock, HFileBlock> blockPair = cachedBlocks.get(entry);
      if (blockPair != null) {
        // Call return twice because for the isCache cased the counter would have got incremented
        // twice. Notice that here we need to returnBlock with different blocks. see comments in
        // BucketCache#returnBlock.
        blockPair.getSecond().release();
        if (cacheCompressedData) {
          if (this.compress == Compression.Algorithm.NONE
              || cowType == CacheOnWriteType.INDEX_BLOCKS
              || cowType == CacheOnWriteType.BLOOM_BLOCKS) {
            blockPair.getFirst().release();
          }
        } else {
          blockPair.getFirst().release();
        }
      }
    }
    scanner.shipped();
    reader.close();
  }

  public static KeyValue.Type generateKeyType(Random rand) {
    if (rand.nextBoolean()) {
      // Let's make half of KVs puts.
      return KeyValue.Type.Put;
    } else {
      KeyValue.Type keyType = KeyValue.Type.values()[1 + rand.nextInt(NUM_VALID_KEY_TYPES)];
      if (keyType == KeyValue.Type.Minimum || keyType == KeyValue.Type.Maximum) {
        throw new RuntimeException("Generated an invalid key type: " + keyType + ". "
            + "Probably the layout of KeyValue.Type has changed.");
      }
      return keyType;
    }
  }

  private void writeStoreFile(boolean useTags) throws IOException {
    Path storeFileParentDir = new Path(TEST_UTIL.getDataTestDir(),
        "test_cache_on_write");
    HFileContext meta = new HFileContextBuilder().withCompression(compress)
        .withBytesPerCheckSum(CKBYTES).withChecksumType(ChecksumType.NULL)
        .withBlockSize(DATA_BLOCK_SIZE)
        .withDataBlockEncoding(NoOpDataBlockEncoder.INSTANCE.getDataBlockEncoding())
        .withIncludesTags(useTags).build();
    StoreFileWriter sfw = new StoreFileWriter.Builder(conf, cacheConf, fs)
        .withOutputDir(storeFileParentDir)
        .withFileContext(meta)
        .withBloomType(BLOOM_TYPE).withMaxKeyCount(NUM_KV).build();
    byte[] cf = Bytes.toBytes("fam");
    for (int i = 0; i < NUM_KV; ++i) {
      byte[] row = RandomKeyValueUtil.randomOrderedKey(rand, i);
      byte[] qualifier = RandomKeyValueUtil.randomRowOrQualifier(rand);
      byte[] value = RandomKeyValueUtil.randomValue(rand);
      KeyValue kv;
      if(useTags) {
        Tag t = new ArrayBackedTag((byte) 1, "visibility");
        List<Tag> tagList = new ArrayList<>();
        tagList.add(t);
        Tag[] tags = new Tag[1];
        tags[0] = t;
        kv =
            new KeyValue(row, 0, row.length, cf, 0, cf.length, qualifier, 0, qualifier.length,
                Math.abs(rand.nextLong()), generateKeyType(rand), value, 0, value.length, tagList);
      } else {
        kv =
            new KeyValue(row, 0, row.length, cf, 0, cf.length, qualifier, 0, qualifier.length,
                Math.abs(rand.nextLong()), generateKeyType(rand), value, 0, value.length);
      }
      sfw.append(kv);
    }

    sfw.close();
    storeFilePath = sfw.getPath();
  }

  private void testCachingDataBlocksDuringCompactionInternals(boolean useTags,
    boolean cacheBlocksOnCompaction, long cacheBlocksOnCompactionThreshold)
    throws IOException, InterruptedException {
    // create a localConf
    boolean localValue = conf.getBoolean(CacheConfig.CACHE_COMPACTED_BLOCKS_ON_WRITE_KEY, false);
    long localCacheCompactedBlocksThreshold = conf
      .getLong(CacheConfig.CACHE_COMPACTED_BLOCKS_ON_WRITE_THRESHOLD_KEY,
        CacheConfig.DEFAULT_CACHE_COMPACTED_BLOCKS_ON_WRITE_THRESHOLD);
    boolean localCacheBloomBlocksValue = conf
      .getBoolean(CacheConfig.CACHE_BLOOM_BLOCKS_ON_WRITE_KEY,
        CacheConfig.DEFAULT_CACHE_BLOOMS_ON_WRITE);
    boolean localCacheIndexBlocksValue = conf
      .getBoolean(CacheConfig.CACHE_INDEX_BLOCKS_ON_WRITE_KEY,
        CacheConfig.DEFAULT_CACHE_INDEXES_ON_WRITE);

    try {
      // Set the conf if testing caching compacted blocks on write
      conf.setBoolean(CacheConfig.CACHE_COMPACTED_BLOCKS_ON_WRITE_KEY,
        cacheBlocksOnCompaction);

      // set size threshold if testing compaction size threshold
      if (cacheBlocksOnCompactionThreshold > 0) {
        conf.setLong(CacheConfig.CACHE_COMPACTED_BLOCKS_ON_WRITE_THRESHOLD_KEY,
          cacheBlocksOnCompactionThreshold);
      }

      // TODO: need to change this test if we add a cache size threshold for
      // compactions, or if we implement some other kind of intelligent logic for
      // deciding what blocks to cache-on-write on compaction.
      final String table = "CompactionCacheOnWrite";
      final String cf = "myCF";
      final byte[] cfBytes = Bytes.toBytes(cf);
      final int maxVersions = 3;
      ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder
        .newBuilder(cfBytes)
        .setCompressionType(compress)
        .setBloomFilterType(BLOOM_TYPE)
        .setMaxVersions(maxVersions)
        .setDataBlockEncoding(NoOpDataBlockEncoder.INSTANCE.getDataBlockEncoding())
        .build();
      HRegion region = TEST_UTIL.createTestRegion(table, cfd, blockCache);
      int rowIdx = 0;
      long ts = EnvironmentEdgeManager.currentTime();
      for (int iFile = 0; iFile < 5; ++iFile) {
        for (int iRow = 0; iRow < 500; ++iRow) {
          String rowStr = "" + (rowIdx * rowIdx * rowIdx) + "row" + iFile + "_" + iRow;
          Put p = new Put(Bytes.toBytes(rowStr));
          ++rowIdx;
          for (int iCol = 0; iCol < 10; ++iCol) {
            String qualStr = "col" + iCol;
            String valueStr = "value_" + rowStr + "_" + qualStr;
            for (int iTS = 0; iTS < 5; ++iTS) {
              if (useTags) {
                Tag t = new ArrayBackedTag((byte) 1, "visibility");
                Tag[] tags = new Tag[1];
                tags[0] = t;
                KeyValue kv = new KeyValue(Bytes.toBytes(rowStr), cfBytes, Bytes.toBytes(qualStr),
                    HConstants.LATEST_TIMESTAMP, Bytes.toBytes(valueStr), tags);
                p.add(kv);
              } else {
                KeyValue kv = new KeyValue(Bytes.toBytes(rowStr), cfBytes, Bytes.toBytes(qualStr),
                  ts++, Bytes.toBytes(valueStr));
                p.add(kv);
              }
            }
          }
          p.setDurability(Durability.ASYNC_WAL);
          region.put(p);
        }
        region.flush(true);
      }

      clearBlockCache(blockCache);
      assertEquals(0, blockCache.getBlockCount());

      region.compact(false);
      LOG.debug("compactStores() returned");

      boolean dataBlockCached = false;
      boolean bloomBlockCached = false;
      boolean indexBlockCached = false;

      for (CachedBlock block : blockCache) {
        if (DATA_BLOCK_TYPES.contains(block.getBlockType())) {
          dataBlockCached = true;
        } else if (BLOOM_BLOCK_TYPES.contains(block.getBlockType())) {
          bloomBlockCached = true;
        } else if (INDEX_BLOCK_TYPES.contains(block.getBlockType())) {
          indexBlockCached = true;
        }
      }

      // Data blocks should be cached in instances where we are caching blocks on write. In the case
      // of testing
      // BucketCache, we cannot verify block type as it is not stored in the cache.
      boolean cacheOnCompactAndNonBucketCache = cacheBlocksOnCompaction
        && !(blockCache instanceof BucketCache);

      String assertErrorMessage = "\nTest description: " + testDescription +
        "\ncacheBlocksOnCompaction: "
        + cacheBlocksOnCompaction + "\n";

      if (cacheOnCompactAndNonBucketCache && cacheBlocksOnCompactionThreshold > 0) {
        if (cacheBlocksOnCompactionThreshold == CACHE_COMPACTION_HIGH_THRESHOLD) {
          assertTrue(assertErrorMessage, dataBlockCached);
          assertTrue(assertErrorMessage, bloomBlockCached);
          assertTrue(assertErrorMessage, indexBlockCached);
        } else {
          assertFalse(assertErrorMessage, dataBlockCached);

          if (localCacheBloomBlocksValue) {
            assertTrue(assertErrorMessage, bloomBlockCached);
          } else {
            assertFalse(assertErrorMessage, bloomBlockCached);
          }

          if (localCacheIndexBlocksValue) {
            assertTrue(assertErrorMessage, indexBlockCached);
          } else {
            assertFalse(assertErrorMessage, indexBlockCached);
          }
        }
      } else {
        assertEquals(assertErrorMessage, cacheOnCompactAndNonBucketCache, dataBlockCached);

        if (cacheOnCompactAndNonBucketCache) {
          assertTrue(assertErrorMessage, bloomBlockCached);
          assertTrue(assertErrorMessage, indexBlockCached);
        }
      }


      region.close();
    } finally {
      // reset back
      conf.setBoolean(CacheConfig.CACHE_COMPACTED_BLOCKS_ON_WRITE_KEY, localValue);
      conf.setLong(CacheConfig.CACHE_COMPACTED_BLOCKS_ON_WRITE_THRESHOLD_KEY,
        localCacheCompactedBlocksThreshold);
      conf.setBoolean(CacheConfig.CACHE_BLOOM_BLOCKS_ON_WRITE_KEY, localCacheBloomBlocksValue);
      conf.setBoolean(CacheConfig.CACHE_INDEX_BLOCKS_ON_WRITE_KEY, localCacheIndexBlocksValue);
    }
  }

  @Test
  public void testStoreFileCacheOnWrite() throws IOException {
    testStoreFileCacheOnWriteInternals(false);
    testStoreFileCacheOnWriteInternals(true);
  }

  @Test
  public void testCachingDataBlocksDuringCompaction() throws IOException, InterruptedException {
    testCachingDataBlocksDuringCompactionInternals(false, false, -1);
    testCachingDataBlocksDuringCompactionInternals(true, true, -1);
  }

  @Test
  public void testCachingDataBlocksThresholdDuringCompaction()
    throws IOException, InterruptedException {
    testCachingDataBlocksDuringCompactionInternals(false, true, CACHE_COMPACTION_HIGH_THRESHOLD);
    testCachingDataBlocksDuringCompactionInternals(false, true, CACHE_COMPACTION_LOW_THRESHOLD);
  }

}
