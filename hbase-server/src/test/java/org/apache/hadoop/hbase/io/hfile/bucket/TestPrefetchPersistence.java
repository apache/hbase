/*
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
package org.apache.hadoop.hbase.io.hfile.bucket;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.RandomKeyValueUtil;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
@Category({ IOTests.class, LargeTests.class })
public class TestPrefetchPersistence {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestPrefetchPersistence.class);

  public TestName name = new TestName();

  @Parameterized.Parameters(name = "{index}: blockSize={0}, bucketSizes={1}")
  @SuppressWarnings("checkstyle:Indentation")
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][] { { 16 * 1024,
      new int[] { 2 * 1024 + 1024, 4 * 1024 + 1024, 8 * 1024 + 1024, 16 * 1024 + 1024,
        28 * 1024 + 1024, 32 * 1024 + 1024, 64 * 1024 + 1024, 96 * 1024 + 1024,
        128 * 1024 + 1024 } } });
  }

  @Parameterized.Parameter(0)
  public int constructedBlockSize;

  @Parameterized.Parameter(1)
  public int[] constructedBlockSizes;

  private static final Logger LOG = LoggerFactory.getLogger(TestPrefetchPersistence.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private static final int NUM_VALID_KEY_TYPES = KeyValue.Type.values().length - 2;
  private static final int DATA_BLOCK_SIZE = 2048;
  private static final int NUM_KV = 1000;

  private Configuration conf;
  private CacheConfig cacheConf;
  private FileSystem fs;
  String prefetchPersistencePath;
  Path testDir;

  BucketCache bucketCache;

  final long capacitySize = 32 * 1024 * 1024;
  final int writeThreads = BucketCache.DEFAULT_WRITER_THREADS;
  final int writerQLen = BucketCache.DEFAULT_WRITER_QUEUE_ITEMS;

  @Before
  public void setup() throws IOException {
    conf = TEST_UTIL.getConfiguration();
    conf.setBoolean(CacheConfig.PREFETCH_BLOCKS_ON_OPEN_KEY, true);
    testDir = TEST_UTIL.getDataTestDir();
    TEST_UTIL.getTestFileSystem().mkdirs(testDir);
    fs = HFileSystem.get(conf);
  }

  @Test
  public void testPrefetchPersistence() throws Exception {
    bucketCache = new BucketCache("file:" + testDir + "/bucket.cache", capacitySize,
      constructedBlockSize, constructedBlockSizes, writeThreads, writerQLen,
      testDir + "/bucket.persistence", 60 * 1000, conf);
    bucketCache.waitForCacheInitialization(10000);
    cacheConf = new CacheConfig(conf, bucketCache);

    long usedSize = bucketCache.getAllocator().getUsedSize();
    assertEquals(0, usedSize);
    assertTrue(new File(testDir + "/bucket.cache").exists());
    // Load Cache
    Path storeFile = writeStoreFile("TestPrefetch0");
    Path storeFile2 = writeStoreFile("TestPrefetch1");
    readStoreFile(storeFile);
    readStoreFile(storeFile2);
    usedSize = bucketCache.getAllocator().getUsedSize();
    assertNotEquals(0, usedSize);

    bucketCache.shutdown();
    assertTrue(new File(testDir + "/bucket.persistence").exists());
    bucketCache = new BucketCache("file:" + testDir + "/bucket.cache", capacitySize,
      constructedBlockSize, constructedBlockSizes, writeThreads, writerQLen,
      testDir + "/bucket.persistence", 60 * 1000, conf);
    bucketCache.waitForCacheInitialization(10000);
    cacheConf = new CacheConfig(conf, bucketCache);
    assertTrue(usedSize != 0);
    assertTrue(bucketCache.fullyCachedFiles.containsKey(storeFile.getName()));
    assertTrue(bucketCache.fullyCachedFiles.containsKey(storeFile2.getName()));
    TEST_UTIL.cleanupTestDir();
  }

  public void readStoreFile(Path storeFilePath) throws Exception {
    // Open the file
    HFile.Reader reader = HFile.createReader(fs, storeFilePath, cacheConf, true, conf);
    while (!reader.prefetchComplete()) {
      // Sleep for a bit
      Thread.sleep(1000);
    }
  }

  public Path writeStoreFile(String fname) throws IOException {
    Path storeFileParentDir = new Path(TEST_UTIL.getDataTestDir(), fname);
    HFileContext meta = new HFileContextBuilder().withBlockSize(DATA_BLOCK_SIZE).build();
    StoreFileWriter sfw = new StoreFileWriter.Builder(conf, cacheConf, fs)
      .withOutputDir(storeFileParentDir).withFileContext(meta).build();
    Random rand = ThreadLocalRandom.current();
    final int rowLen = 32;
    for (int i = 0; i < NUM_KV; ++i) {
      byte[] k = RandomKeyValueUtil.randomOrderedKey(rand, i);
      byte[] v = RandomKeyValueUtil.randomValue(rand);
      int cfLen = rand.nextInt(k.length - rowLen + 1);
      KeyValue kv = new KeyValue(k, 0, rowLen, k, rowLen, cfLen, k, rowLen + cfLen,
        k.length - rowLen - cfLen, rand.nextLong(), generateKeyType(rand), v, 0, v.length);
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
      KeyValue.Type keyType = KeyValue.Type.values()[1 + rand.nextInt(NUM_VALID_KEY_TYPES)];
      if (keyType == KeyValue.Type.Minimum || keyType == KeyValue.Type.Maximum) {
        throw new RuntimeException("Generated an invalid key type: " + keyType + ". "
          + "Probably the layout of KeyValue.Type has changed.");
      }
      return keyType;
    }
  }

}
