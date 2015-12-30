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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;

/**
 * Tests {@link HFile} cache-on-write functionality for the following block
 * types: data blocks, non-root index blocks, and Bloom filter blocks.
 */
@RunWith(Parameterized.class)
@Category({IOTests.class, MediumTests.class})
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
    List<BlockCache> blockcaches = new ArrayList<BlockCache>();
    // default
    blockcaches.add(new CacheConfig(conf).getBlockCache());

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
    List<Object[]> params = new ArrayList<Object[]>();
    for (BlockCache blockCache : getBlockCaches()) {
      for (CacheOnWriteType cowType : CacheOnWriteType.values()) {
        for (Compression.Algorithm compress : HBaseTestingUtility.COMPRESSION_ALGORITHMS) {
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
    conf.setInt(BloomFilterFactory.IO_STOREFILE_BLOOM_BLOCK_SIZE,
        BLOOM_BLOCK_SIZE);
    conf.setBoolean(CacheConfig.CACHE_DATA_BLOCKS_COMPRESSED_KEY, cacheCompressedData);
    cowType.modifyConf(conf);
    fs = HFileSystem.get(conf);
    CacheConfig.GLOBAL_BLOCK_CACHE_INSTANCE = blockCache;
    cacheConf =
        new CacheConfig(blockCache, true, true, cowType.shouldBeCached(BlockType.DATA),
        cowType.shouldBeCached(BlockType.LEAF_INDEX),
        cowType.shouldBeCached(BlockType.BLOOM_CHUNK), false, cacheCompressedData,
            false, false, false);
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
    HFile.Reader reader = HFile.createReader(fs, storeFilePath, cacheConf, conf);
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
    HFileBlock prevBlock = null;
    EnumMap<BlockType, Integer> blockCountByType =
        new EnumMap<BlockType, Integer>(BlockType.class);

    DataBlockEncoding encodingInCache = NoOpDataBlockEncoder.INSTANCE.getDataBlockEncoding();
    List<Long> cachedBlocksOffset = new ArrayList<Long>();
    Map<Long, HFileBlock> cachedBlocks = new HashMap<Long, HFileBlock>();
    while (offset < reader.getTrailer().getLoadOnOpenDataOffset()) {
      long onDiskSize = -1;
      if (prevBlock != null) {
         onDiskSize = prevBlock.getNextBlockOnDiskSizeWithHeader();
      }
      // Flags: don't cache the block, use pread, this is not a compaction.
      // Also, pass null for expected block type to avoid checking it.
      HFileBlock block = reader.readBlock(offset, onDiskSize, false, true,
        false, true, null, encodingInCache);
      BlockCacheKey blockCacheKey = new BlockCacheKey(reader.getName(),
          offset);
      HFileBlock fromCache = (HFileBlock) blockCache.getBlock(blockCacheKey, true, false, true);
      boolean isCached = fromCache != null;
      cachedBlocksOffset.add(offset);
      cachedBlocks.put(offset, fromCache);
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
        assertNotEquals(block.getBlockType(), BlockType.ENCODED_DATA);
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
    if (useTags) {
      assertEquals("{" + BlockType.DATA
          + "=2663, LEAF_INDEX=297, BLOOM_CHUNK=9, INTERMEDIATE_INDEX=34}", countByType);
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
      HFileBlock hFileBlock = cachedBlocks.get(entry);
      if (hFileBlock != null) {
        // call return twice because for the isCache cased the counter would have got incremented
        // twice
        blockCache.returnBlock(blockCacheKey, hFileBlock);
        if(cacheCompressedData) {
          if (this.compress == Compression.Algorithm.NONE
              || cowType == CacheOnWriteType.INDEX_BLOCKS
              || cowType == CacheOnWriteType.BLOOM_BLOCKS) {
            blockCache.returnBlock(blockCacheKey, hFileBlock);
          }
        } else {
          blockCache.returnBlock(blockCacheKey, hFileBlock);
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
    StoreFile.Writer sfw = new StoreFile.WriterBuilder(conf, cacheConf, fs)
        .withOutputDir(storeFileParentDir).withComparator(CellComparator.COMPARATOR)
        .withFileContext(meta)
        .withBloomType(BLOOM_TYPE).withMaxKeyCount(NUM_KV).build();
    byte[] cf = Bytes.toBytes("fam");
    for (int i = 0; i < NUM_KV; ++i) {
      byte[] row = RandomKeyValueUtil.randomOrderedKey(rand, i);
      byte[] qualifier = RandomKeyValueUtil.randomRowOrQualifier(rand);
      byte[] value = RandomKeyValueUtil.randomValue(rand);
      KeyValue kv;
      if(useTags) {
        Tag t = new Tag((byte) 1, "visibility");
        List<Tag> tagList = new ArrayList<Tag>();
        tagList.add(t);
        Tag[] tags = new Tag[1];
        tags[0] = t;
        kv =
            new KeyValue(row, 0, row.length, cf, 0, cf.length, qualifier, 0, qualifier.length,
                rand.nextLong(), generateKeyType(rand), value, 0, value.length, tagList);
      } else {
        kv =
            new KeyValue(row, 0, row.length, cf, 0, cf.length, qualifier, 0, qualifier.length,
                rand.nextLong(), generateKeyType(rand), value, 0, value.length);
      }
      sfw.append(kv);
    }

    sfw.close();
    storeFilePath = sfw.getPath();
  }

  private void testNotCachingDataBlocksDuringCompactionInternals(boolean useTags)
      throws IOException, InterruptedException {
    // TODO: need to change this test if we add a cache size threshold for
    // compactions, or if we implement some other kind of intelligent logic for
    // deciding what blocks to cache-on-write on compaction.
    final String table = "CompactionCacheOnWrite";
    final String cf = "myCF";
    final byte[] cfBytes = Bytes.toBytes(cf);
    final int maxVersions = 3;
    Region region = TEST_UTIL.createTestRegion(table, 
        new HColumnDescriptor(cf)
            .setCompressionType(compress)
            .setBloomFilterType(BLOOM_TYPE)
            .setMaxVersions(maxVersions)
            .setDataBlockEncoding(NoOpDataBlockEncoder.INSTANCE.getDataBlockEncoding())
    );
    int rowIdx = 0;
    long ts = EnvironmentEdgeManager.currentTime();
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
              p.addColumn(cfBytes, Bytes.toBytes(qualStr), ts++, Bytes.toBytes(valueStr));
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

    for (CachedBlock block: blockCache) {
      assertNotEquals(BlockType.ENCODED_DATA, block.getBlockType());
      assertNotEquals(BlockType.DATA, block.getBlockType());
    }
    ((HRegion)region).close();
  }

  @Test
  public void testStoreFileCacheOnWrite() throws IOException {
    testStoreFileCacheOnWriteInternals(false);
    testStoreFileCacheOnWriteInternals(true);
  }

  @Test
  public void testNotCachingDataBlocksDuringCompaction() throws IOException, InterruptedException {
    testNotCachingDataBlocksDuringCompactionInternals(false);
    testNotCachingDataBlocksDuringCompactionInternals(true);
  }
}
