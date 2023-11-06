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

import static org.apache.hadoop.hbase.regionserver.HRegionFileSystem.REGION_INFO_FILE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.PrefetchExecutor;
import org.apache.hadoop.hbase.io.hfile.RandomKeyValueUtil;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ IOTests.class, MediumTests.class })
public class TestBucketCachePersister {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBucketCachePersister.class);

  public TestName name = new TestName();

  public int constructedBlockSize = 16 * 1024;

  public int[] constructedBlockSizes =
    new int[] { 2 * 1024 + 1024, 4 * 1024 + 1024, 8 * 1024 + 1024, 16 * 1024 + 1024,
      28 * 1024 + 1024, 32 * 1024 + 1024, 64 * 1024 + 1024, 96 * 1024 + 1024, 128 * 1024 + 1024 };

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final int NUM_VALID_KEY_TYPES = KeyValue.Type.values().length - 2;
  private static final int DATA_BLOCK_SIZE = 2048;
  private static final int NUM_KV = 1000;

  final long capacitySize = 32 * 1024 * 1024;
  final int writeThreads = BucketCache.DEFAULT_WRITER_THREADS;
  final int writerQLen = BucketCache.DEFAULT_WRITER_QUEUE_ITEMS;
  Path testDir;

  public Configuration setupBucketCacheConfig(long bucketCachePersistInterval) throws IOException {
    Configuration conf;
    conf = TEST_UTIL.getConfiguration();
    conf.setBoolean(CacheConfig.PREFETCH_BLOCKS_ON_OPEN_KEY, true);
    testDir = TEST_UTIL.getDataTestDir();
    TEST_UTIL.getTestFileSystem().mkdirs(testDir);
    conf.setLong(CacheConfig.BUCKETCACHE_PERSIST_INTERVAL_KEY, bucketCachePersistInterval);
    return conf;
  }

  public BucketCache setupBucketCache(Configuration conf) throws IOException {
    BucketCache bucketCache = new BucketCache("file:" + testDir + "/bucket.cache", capacitySize,
      constructedBlockSize, constructedBlockSizes, writeThreads, writerQLen,
      testDir + "/bucket.persistence", 60 * 1000, conf);
    return bucketCache;
  }

  public void cleanupBucketCache(BucketCache bucketCache) throws IOException {
    bucketCache.shutdown();
    TEST_UTIL.cleanupDataTestDirOnTestFS(String.valueOf(testDir));
    assertFalse(TEST_UTIL.getTestFileSystem().exists(testDir));
  }

  @Test
  public void testPrefetchPersistenceCrash() throws Exception {
    long bucketCachePersistInterval = 3000;
    Configuration conf = setupBucketCacheConfig(bucketCachePersistInterval);
    BucketCache bucketCache = setupBucketCache(conf);
    CacheConfig cacheConf = new CacheConfig(conf, bucketCache);
    FileSystem fs = HFileSystem.get(conf);
    // Load Cache
    Path storeFile = writeStoreFile("Region0", "TestPrefetch0", conf, cacheConf, fs);
    Path storeFile2 = writeStoreFile("Region1", "TestPrefetch1", conf, cacheConf, fs);
    readStoreFile(storeFile, 0, fs, cacheConf, conf, bucketCache);
    readStoreFile(storeFile2, 0, fs, cacheConf, conf, bucketCache);
    Thread.sleep(bucketCachePersistInterval);
    assertTrue(new File(testDir + "/bucket.persistence").exists());
    assertTrue(new File(testDir + "/bucket.persistence").delete());
    cleanupBucketCache(bucketCache);
  }

  @Test
  public void testPrefetchPersistenceCrashNegative() throws Exception {
    long bucketCachePersistInterval = Long.MAX_VALUE;
    Configuration conf = setupBucketCacheConfig(bucketCachePersistInterval);
    BucketCache bucketCache = setupBucketCache(conf);
    CacheConfig cacheConf = new CacheConfig(conf, bucketCache);
    FileSystem fs = HFileSystem.get(conf);
    // Load Cache
    Path storeFile = writeStoreFile("Region2", "TestPrefetch2", conf, cacheConf, fs);
    readStoreFile(storeFile, 0, fs, cacheConf, conf, bucketCache);
    assertFalse(new File(testDir + "/bucket.persistence").exists());
    cleanupBucketCache(bucketCache);
  }

  @Test
  public void testPrefetchListUponBlockEviction() throws Exception {
    Configuration conf = setupBucketCacheConfig(200);
    BucketCache bucketCache1 = setupBucketCache(conf);
    CacheConfig cacheConf = new CacheConfig(conf, bucketCache1);
    FileSystem fs = HFileSystem.get(conf);
    // Load Blocks in cache
    Path storeFile = writeStoreFile("Region3", "TestPrefetch3", conf, cacheConf, fs);
    readStoreFile(storeFile, 0, fs, cacheConf, conf, bucketCache1);
    Thread.sleep(500);
    // Evict Blocks from cache
    BlockCacheKey bucketCacheKey = bucketCache1.backingMap.entrySet().iterator().next().getKey();
    assertTrue(bucketCache1.fullyCachedFiles.containsKey(storeFile.getName()));
    int initialRegionPrefetchInfoSize = bucketCache1.getRegionPrefetchInfo().size();
    assertTrue(initialRegionPrefetchInfoSize > 0);
    bucketCache1.evictBlock(bucketCacheKey);
    assertFalse(bucketCache1.fullyCachedFiles.containsKey(storeFile.getName()));
    int newRegionPrefetchInfoSize = bucketCache1.getRegionPrefetchInfo().size();
    assertTrue(initialRegionPrefetchInfoSize - newRegionPrefetchInfoSize == 1);
  }

  public void readStoreFile(Path storeFilePath, long offset, FileSystem fs, CacheConfig cacheConf,
    Configuration conf, BucketCache bucketCache) throws Exception {
    // Open the file
    HFile.Reader reader = HFile.createReader(fs, storeFilePath, cacheConf, true, conf);

    while (!reader.prefetchComplete()) {
      // Sleep for a bit
      Thread.sleep(1000);
    }
    HFileBlock block = reader.readBlock(offset, -1, false, true, false, true, null, null);
    BlockCacheKey blockCacheKey = new BlockCacheKey(reader.getName(), offset);
    BucketEntry be = bucketCache.backingMap.get(blockCacheKey);
    boolean isCached = bucketCache.getBlock(blockCacheKey, true, false, true) != null;

    if (
      block.getBlockType() == BlockType.DATA || block.getBlockType() == BlockType.ROOT_INDEX
        || block.getBlockType() == BlockType.INTERMEDIATE_INDEX
    ) {
      assertTrue(isCached);
    }
  }

  public Path writeStoreFile(String regionName, String fname, Configuration conf,
    CacheConfig cacheConf, FileSystem fs) throws IOException {
    // Create store files as per the following directory structure
    // <region name>/<column family>/<hFile>
    Path regionDir = new Path(TEST_UTIL.getDataTestDir(), regionName);
    Path storeFileParentDir = new Path(regionDir, fname);
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

    // Create a dummy .regioninfo file as the PrefetchExecutor needs it to figure out the region
    // name to be added to the prefetch file list
    Path regionInfoFilePath = new Path(storeFileParentDir, REGION_INFO_FILE);
    File regionInfoFile = new File(regionInfoFilePath.toString());
    try {
      if (!regionInfoFile.createNewFile()) {
        assertFalse("Unable to create .regioninfo file", true);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
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
