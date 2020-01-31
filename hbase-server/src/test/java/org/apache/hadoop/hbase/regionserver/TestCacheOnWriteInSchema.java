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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.BlockCacheFactory;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.io.hfile.RandomKeyValueUtil;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests {@link HFile} cache-on-write functionality for data blocks, non-root
 * index blocks, and Bloom filter blocks, as specified by the column family.
 */
@RunWith(Parameterized.class)
@Category({RegionServerTests.class, SmallTests.class})
public class TestCacheOnWriteInSchema {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCacheOnWriteInSchema.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestCacheOnWriteInSchema.class);
  @Rule public TestName name = new TestName();

  private static final HBaseTestingUtility TEST_UTIL = HBaseTestingUtility.createLocalHTU();
  private static final String DIR = TEST_UTIL.getDataTestDir("TestCacheOnWriteInSchema").toString();
  private static byte [] table;
  private static byte [] family = Bytes.toBytes("family");
  private static final int NUM_KV = 25000;
  private static final Random rand = new Random(12983177L);
  /** The number of valid key types possible in a store file */
  private static final int NUM_VALID_KEY_TYPES =
      KeyValue.Type.values().length - 2;

  private static enum CacheOnWriteType {
    DATA_BLOCKS(BlockType.DATA, BlockType.ENCODED_DATA),
    BLOOM_BLOCKS(BlockType.BLOOM_CHUNK),
    INDEX_BLOCKS(BlockType.LEAF_INDEX, BlockType.INTERMEDIATE_INDEX);

    private final BlockType blockType1;
    private final BlockType blockType2;

    private CacheOnWriteType(BlockType blockType) {
      this(blockType, blockType);
    }

    private CacheOnWriteType(BlockType blockType1, BlockType blockType2) {
      this.blockType1 = blockType1;
      this.blockType2 = blockType2;
    }

    public boolean shouldBeCached(BlockType blockType) {
      return blockType == blockType1 || blockType == blockType2;
    }

    public ColumnFamilyDescriptorBuilder modifyFamilySchema(ColumnFamilyDescriptorBuilder builder) {
      switch (this) {
        case DATA_BLOCKS:
          builder.setCacheDataOnWrite(true);
          break;
        case BLOOM_BLOCKS:
          builder.setCacheBloomsOnWrite(true);
          break;
        case INDEX_BLOCKS:
          builder.setCacheIndexesOnWrite(true);
          break;
      }
      return builder;
    }
  }

  private final CacheOnWriteType cowType;
  private Configuration conf;
  private final String testDescription;
  private HRegion region;
  private HStore store;
  private WALFactory walFactory;
  private FileSystem fs;

  public TestCacheOnWriteInSchema(CacheOnWriteType cowType) {
    this.cowType = cowType;
    testDescription = "[cacheOnWrite=" + cowType + "]";
    System.out.println(testDescription);
  }

  @Parameters
  public static Collection<Object[]> getParameters() {
    List<Object[]> cowTypes = new ArrayList<>();
    for (CacheOnWriteType cowType : CacheOnWriteType.values()) {
      cowTypes.add(new Object[] { cowType });
    }
    return cowTypes;
  }

  @Before
  public void setUp() throws IOException {
    // parameterized tests add [#] suffix get rid of [ and ].
    table = Bytes.toBytes(name.getMethodName().replaceAll("[\\[\\]]", "_"));

    conf = TEST_UTIL.getConfiguration();
    conf.setInt(HFile.FORMAT_VERSION_KEY, HFile.MAX_FORMAT_VERSION);
    conf.setBoolean(CacheConfig.CACHE_BLOCKS_ON_WRITE_KEY, false);
    conf.setBoolean(CacheConfig.CACHE_INDEX_BLOCKS_ON_WRITE_KEY, false);
    conf.setBoolean(CacheConfig.CACHE_BLOOM_BLOCKS_ON_WRITE_KEY, false);
    fs = HFileSystem.get(conf);

    // Create the schema
    ColumnFamilyDescriptor hcd = cowType
        .modifyFamilySchema(
          ColumnFamilyDescriptorBuilder.newBuilder(family).setBloomFilterType(BloomType.ROWCOL))
        .build();
    TableDescriptor htd =
        TableDescriptorBuilder.newBuilder(TableName.valueOf(table)).setColumnFamily(hcd).build();

    // Create a store based on the schema
    String id = TestCacheOnWriteInSchema.class.getName();
    Path logdir = new Path(FSUtils.getRootDir(conf), AbstractFSWALProvider.getWALDirectoryName(id));
    fs.delete(logdir, true);

    RegionInfo info = RegionInfoBuilder.newBuilder(htd.getTableName()).build();
    walFactory = new WALFactory(conf, id);

    region = TEST_UTIL.createLocalHRegion(info, htd, walFactory.getWAL(info));
    region.setBlockCache(BlockCacheFactory.createBlockCache(conf));
    store = new HStore(region, hcd, conf, false);
  }

  @After
  public void tearDown() throws IOException {
    IOException ex = null;
    try {
      region.close();
    } catch (IOException e) {
      LOG.warn("Caught Exception", e);
      ex = e;
    }
    try {
      walFactory.close();
    } catch (IOException e) {
      LOG.warn("Caught Exception", e);
      ex = e;
    }
    try {
      fs.delete(new Path(DIR), true);
    } catch (IOException e) {
      LOG.error("Could not delete " + DIR, e);
      ex = e;
    }
    if (ex != null) {
      throw ex;
    }
  }

  @Test
  public void testCacheOnWriteInSchema() throws IOException {
    // Write some random data into the store
    StoreFileWriter writer = store.createWriterInTmp(Integer.MAX_VALUE,
        HFile.DEFAULT_COMPRESSION_ALGORITHM, false, true, false, false);
    writeStoreFile(writer);
    writer.close();
    // Verify the block types of interest were cached on write
    readStoreFile(writer.getPath());
  }

  private void readStoreFile(Path path) throws IOException {
    CacheConfig cacheConf = store.getCacheConfig();
    BlockCache cache = cacheConf.getBlockCache().get();
    HStoreFile sf = new HStoreFile(fs, path, conf, cacheConf, BloomType.ROWCOL, true);
    sf.initReader();
    HFile.Reader reader = sf.getReader().getHFileReader();
    try {
      // Open a scanner with (on read) caching disabled
      HFileScanner scanner = reader.getScanner(false, false);
      assertTrue(testDescription, scanner.seekTo());
      // Cribbed from io.hfile.TestCacheOnWrite
      long offset = 0;
      while (offset < reader.getTrailer().getLoadOnOpenDataOffset()) {
        // Flags: don't cache the block, use pread, this is not a compaction.
        // Also, pass null for expected block type to avoid checking it.
        HFileBlock block = reader.readBlock(offset, -1, false, true,
          false, true, null, DataBlockEncoding.NONE);
        BlockCacheKey blockCacheKey = new BlockCacheKey(reader.getName(),
          offset);
        boolean isCached = cache.getBlock(blockCacheKey, true, false, true) != null;
        boolean shouldBeCached = cowType.shouldBeCached(block.getBlockType());
        final BlockType blockType = block.getBlockType();

        if (shouldBeCached != isCached &&
            (cowType.blockType1.equals(blockType) || cowType.blockType2.equals(blockType))) {
          throw new AssertionError(
            "shouldBeCached: " + shouldBeCached+ "\n" +
            "isCached: " + isCached + "\n" +
            "Test description: " + testDescription + "\n" +
            "block: " + block + "\n" +
            "blockCacheKey: " + blockCacheKey);
        }
        offset += block.getOnDiskSizeWithHeader();
      }
    } finally {
      reader.close();
    }
  }

  private static KeyValue.Type generateKeyType(Random rand) {
    if (rand.nextBoolean()) {
      // Let's make half of KVs puts.
      return KeyValue.Type.Put;
    } else {
      KeyValue.Type keyType =
          KeyValue.Type.values()[1 + rand.nextInt(NUM_VALID_KEY_TYPES)];
      if (keyType == KeyValue.Type.Minimum || keyType == KeyValue.Type.Maximum) {
        throw new RuntimeException("Generated an invalid key type: " + keyType
            + ". " + "Probably the layout of KeyValue.Type has changed.");
      }
      return keyType;
    }
  }

  private void writeStoreFile(StoreFileWriter writer) throws IOException {
    final int rowLen = 32;
    for (int i = 0; i < NUM_KV; ++i) {
      byte[] k = RandomKeyValueUtil.randomOrderedKey(rand, i);
      byte[] v = RandomKeyValueUtil.randomValue(rand);
      int cfLen = rand.nextInt(k.length - rowLen + 1);
      KeyValue kv = new KeyValue(
          k, 0, rowLen,
          k, rowLen, cfLen,
          k, rowLen + cfLen, k.length - rowLen - cfLen,
          rand.nextLong(),
          generateKeyType(rand),
          v, 0, v.length);
      writer.append(kv);
    }
  }

}

