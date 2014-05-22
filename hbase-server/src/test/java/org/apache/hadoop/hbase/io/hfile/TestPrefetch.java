/*
 * Copyright 2011 The Apache Software Foundation
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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.regionserver.StoreFile;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestPrefetch {

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final int NUM_VALID_KEY_TYPES = KeyValue.Type.values().length - 2;
  private static final int DATA_BLOCK_SIZE = 2048;
  private static final int NUM_KV = 1000;
  private static final Random RNG = new Random();

  private Configuration conf;
  private CacheConfig cacheConf;
  private FileSystem fs;

  @Before
  public void setUp() throws IOException {
    conf = TEST_UTIL.getConfiguration();
    conf.setInt(HFile.FORMAT_VERSION_KEY, 3);
    conf.setBoolean(CacheConfig.PREFETCH_BLOCKS_ON_OPEN_KEY, true);
    fs = HFileSystem.get(conf);
    cacheConf = new CacheConfig(conf);
  }

  @Test(timeout=60000)
  public void testPrefetch() throws Exception {
    Path storeFile = writeStoreFile();
    readStoreFile(storeFile);
  }

  private void readStoreFile(Path storeFilePath) throws Exception {
    // Open the file
    HFileReaderV2 reader = (HFileReaderV2) HFile.createReader(fs,
      storeFilePath, cacheConf, conf);

    while (!((HFileReaderV3)reader).prefetchComplete()) {
      // Sleep for a bit
      Thread.sleep(1000);
    }

    // Check that all of the data blocks were preloaded
    BlockCache blockCache = cacheConf.getBlockCache();
    long offset = 0;
    HFileBlock prevBlock = null;
    while (offset < reader.getTrailer().getLoadOnOpenDataOffset()) {
      long onDiskSize = -1;
      if (prevBlock != null) {
         onDiskSize = prevBlock.getNextBlockOnDiskSizeWithHeader();
      }
      HFileBlock block = reader.readBlock(offset, onDiskSize, false, true, false, true, null);
      BlockCacheKey blockCacheKey = new BlockCacheKey(reader.getName(), offset);
      boolean isCached = blockCache.getBlock(blockCacheKey, true, false, true) != null;
      if (block.getBlockType() == BlockType.DATA ||
          block.getBlockType() == BlockType.ROOT_INDEX ||
          block.getBlockType() == BlockType.INTERMEDIATE_INDEX) {
        assertTrue(isCached);
      }
      prevBlock = block;
      offset += block.getOnDiskSizeWithHeader();
    }
  }

  private Path writeStoreFile() throws IOException {
    Path storeFileParentDir = new Path(TEST_UTIL.getDataTestDir(), "TestPrefetch");
    HFileContext meta = new HFileContextBuilder()
      .withBlockSize(DATA_BLOCK_SIZE)
      .build();
    StoreFile.Writer sfw = new StoreFile.WriterBuilder(conf, cacheConf, fs)
      .withOutputDir(storeFileParentDir)
      .withComparator(KeyValue.COMPARATOR)
      .withFileContext(meta)
      .build();

    final int rowLen = 32;
    for (int i = 0; i < NUM_KV; ++i) {
      byte[] k = TestHFileWriterV2.randomOrderedKey(RNG, i);
      byte[] v = TestHFileWriterV2.randomValue(RNG);
      int cfLen = RNG.nextInt(k.length - rowLen + 1);
      KeyValue kv = new KeyValue(
          k, 0, rowLen,
          k, rowLen, cfLen,
          k, rowLen + cfLen, k.length - rowLen - cfLen,
          RNG.nextLong(),
          generateKeyType(RNG),
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
