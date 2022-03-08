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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({IOTests.class, MediumTests.class})
public class TestPrefetch {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestPrefetch.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final int NUM_VALID_KEY_TYPES = KeyValue.Type.values().length - 2;
  private static final int DATA_BLOCK_SIZE = 2048;
  private static final int NUM_KV = 1000;

  private Configuration conf;
  private CacheConfig cacheConf;
  private FileSystem fs;
  private BlockCache blockCache;

  @Before
  public void setUp() throws IOException {
    conf = TEST_UTIL.getConfiguration();
    conf.setBoolean(CacheConfig.PREFETCH_BLOCKS_ON_OPEN_KEY, true);
    fs = HFileSystem.get(conf);
    blockCache = BlockCacheFactory.createBlockCache(conf);
    cacheConf = new CacheConfig(conf, blockCache);
  }

  @Test
  public void testPrefetchSetInHCDWorks() {
    ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder
        .newBuilder(Bytes.toBytes("f")).setPrefetchBlocksOnOpen(true).build();
    Configuration c = HBaseConfiguration.create();
    assertFalse(c.getBoolean(CacheConfig.PREFETCH_BLOCKS_ON_OPEN_KEY, false));
    CacheConfig cc =
        new CacheConfig(c, columnFamilyDescriptor, blockCache, ByteBuffAllocator.HEAP);
    assertTrue(cc.shouldPrefetchOnOpen());
  }

  @Test
  public void testPrefetch() throws Exception {
    Path storeFile = writeStoreFile("TestPrefetch");
    readStoreFile(storeFile);
  }

  @Test
  public void testPrefetchRace() throws Exception {
    for (int i = 0; i < 10; i++) {
      Path storeFile = writeStoreFile("TestPrefetchRace-" + i);
      readStoreFileLikeScanner(storeFile);
    }
  }

  /**
   * Read a storefile in the same manner as a scanner -- using non-positional reads and
   * without waiting for prefetch to complete.
   */
  private void readStoreFileLikeScanner(Path storeFilePath) throws Exception {
    // Open the file
    HFile.Reader reader = HFile.createReader(fs, storeFilePath, cacheConf, true, conf);
    do {
      long offset = 0;
      while (offset < reader.getTrailer().getLoadOnOpenDataOffset()) {
        HFileBlock block = reader.readBlock(offset, -1, false, /*pread=*/false,
            false, true, null, null);
        offset += block.getOnDiskSizeWithHeader();
      }
    } while (!reader.prefetchComplete());
  }

  private void readStoreFile(Path storeFilePath) throws Exception {
    // Open the file
    HFile.Reader reader = HFile.createReader(fs, storeFilePath, cacheConf, true, conf);

    while (!reader.prefetchComplete()) {
      // Sleep for a bit
      Thread.sleep(1000);
    }

    // Check that all of the data blocks were preloaded
    BlockCache blockCache = cacheConf.getBlockCache().get();
    long offset = 0;
    while (offset < reader.getTrailer().getLoadOnOpenDataOffset()) {
      HFileBlock block = reader.readBlock(offset, -1, false, true, false, true, null, null);
      BlockCacheKey blockCacheKey = new BlockCacheKey(reader.getName(), offset);
      boolean isCached = blockCache.getBlock(blockCacheKey, true, false, true) != null;
      if (block.getBlockType() == BlockType.DATA || block.getBlockType() == BlockType.ROOT_INDEX
          || block.getBlockType() == BlockType.INTERMEDIATE_INDEX) {
        assertTrue(isCached);
      }
      offset += block.getOnDiskSizeWithHeader();
    }
  }

  private Path writeStoreFile(String fname) throws IOException {
    Path storeFileParentDir = new Path(TEST_UTIL.getDataTestDir(), fname);
    HFileContext meta = new HFileContextBuilder()
      .withBlockSize(DATA_BLOCK_SIZE)
      .build();
    StoreFileWriter sfw = new StoreFileWriter.Builder(conf, cacheConf, fs)
      .withOutputDir(storeFileParentDir)
      .withFileContext(meta)
      .build();
    Random rand = ThreadLocalRandom.current();
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
      sfw.append(kv);
    }

    sfw.close();
    return sfw.getPath();
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

}
