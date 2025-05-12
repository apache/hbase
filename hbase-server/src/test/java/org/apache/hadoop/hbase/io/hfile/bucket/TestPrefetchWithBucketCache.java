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

import static org.apache.hadoop.hbase.HConstants.BUCKET_CACHE_IOENGINE_KEY;
import static org.apache.hadoop.hbase.HConstants.BUCKET_CACHE_SIZE_KEY;
import static org.apache.hadoop.hbase.io.hfile.BlockCacheFactory.BUCKET_CACHE_BUCKETS_KEY;
import static org.apache.hadoop.hbase.io.hfile.bucket.BucketCache.QUEUE_ADDITION_WAIT_TIME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.BlockCacheFactory;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileBlock;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.io.hfile.PrefetchExecutor;
import org.apache.hadoop.hbase.io.hfile.RandomKeyValueUtil;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreContext;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTracker;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

@Category({ IOTests.class, MediumTests.class })
public class TestPrefetchWithBucketCache {

  private static final Logger LOG = LoggerFactory.getLogger(TestPrefetchWithBucketCache.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestPrefetchWithBucketCache.class);

  @Rule
  public TestName name = new TestName();

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private static final int NUM_VALID_KEY_TYPES = KeyValue.Type.values().length - 2;
  private static final int DATA_BLOCK_SIZE = 2048;
  private Configuration conf;
  private CacheConfig cacheConf;
  private FileSystem fs;
  private BlockCache blockCache;

  @Before
  public void setUp() throws IOException {
    conf = TEST_UTIL.getConfiguration();
    conf.setBoolean(CacheConfig.PREFETCH_BLOCKS_ON_OPEN_KEY, true);
    fs = HFileSystem.get(conf);
    File testDir = new File(name.getMethodName());
    testDir.mkdir();
    conf.set(BUCKET_CACHE_IOENGINE_KEY, "file:/" + testDir.getAbsolutePath() + "/bucket.cache");
  }

  @After
  public void tearDown() {
    File cacheFile = new File(name.getMethodName() + "/bucket.cache");
    File dir = new File(name.getMethodName());
    cacheFile.delete();
    dir.delete();
  }

  @Test
  public void testPrefetchDoesntOverwork() throws Exception {
    conf.setLong(BUCKET_CACHE_SIZE_KEY, 200);
    blockCache = BlockCacheFactory.createBlockCache(conf);
    cacheConf = new CacheConfig(conf, blockCache);
    Path storeFile = writeStoreFile("TestPrefetchDoesntOverwork", 100);
    // Prefetches the file blocks
    LOG.debug("First read should prefetch the blocks.");
    readStoreFile(storeFile);
    BucketCache bc = BucketCache.getBucketCacheFromCacheConfig(cacheConf).get();
    // Our file should have 6 DATA blocks. We should wait for all of them to be cached
    Waiter.waitFor(conf, 300, () -> bc.getBackingMap().size() == 6);
    Map<BlockCacheKey, BucketEntry> snapshot = ImmutableMap.copyOf(bc.getBackingMap());
    // Reads file again and check we are not prefetching it again
    LOG.debug("Second read, no prefetch should happen here.");
    readStoreFile(storeFile);
    // Makes sure the cache hasn't changed
    snapshot.entrySet().forEach(e -> {
      BucketEntry entry = bc.getBackingMap().get(e.getKey());
      assertNotNull(entry);
      assertEquals(e.getValue().getCachedTime(), entry.getCachedTime());
    });
    // forcibly removes first block from the bc backing map, in order to cause it to be cached again
    BlockCacheKey key = snapshot.keySet().stream().findFirst().get();
    LOG.debug("removing block {}", key);
    bc.getBackingMap().remove(key);
    bc.getFullyCachedFiles().get().remove(storeFile.getName());
    assertTrue(snapshot.size() > bc.getBackingMap().size());
    LOG.debug("Third read should prefetch again, as we removed one block for the file.");
    readStoreFile(storeFile);
    Waiter.waitFor(conf, 300, () -> snapshot.size() == bc.getBackingMap().size());
    assertTrue(snapshot.get(key).getCachedTime() < bc.getBackingMap().get(key).getCachedTime());
  }

  @Test
  public void testPrefetchRefsAfterSplit() throws Exception {
    conf.setLong(BUCKET_CACHE_SIZE_KEY, 200);
    blockCache = BlockCacheFactory.createBlockCache(conf);
    cacheConf = new CacheConfig(conf, blockCache);

    Path tableDir = new Path(TEST_UTIL.getDataTestDir(), "testPrefetchRefsAfterSplit");
    RegionInfo region = RegionInfoBuilder.newBuilder(TableName.valueOf(tableDir.getName())).build();
    Path regionDir = new Path(tableDir, region.getEncodedName());
    Path cfDir = new Path(regionDir, "cf");
    HRegionFileSystem regionFS =
      HRegionFileSystem.createRegionOnFileSystem(conf, fs, tableDir, region);
    Path storeFile = writeStoreFile(100, cfDir);
    StoreFileTracker sft = StoreFileTrackerFactory.create(conf, true,
      StoreContext.getBuilder().withRegionFileSystem(regionFS).withFamilyStoreDirectoryPath(cfDir)
        .withCacheConfig(cacheConf).build());
    // Prefetches the file blocks
    LOG.debug("First read should prefetch the blocks.");
    readStoreFile(storeFile);
    BucketCache bc = BucketCache.getBucketCacheFromCacheConfig(cacheConf).get();
    // Our file should have 6 DATA blocks. We should wait for all of them to be cached
    Waiter.waitFor(conf, 300, () -> bc.getBackingMap().size() == 6);

    // split the file and return references to the original file
    Random rand = ThreadLocalRandom.current();
    byte[] splitPoint = RandomKeyValueUtil.randomOrderedKey(rand, 50);
    HStoreFile file = new HStoreFile(fs, storeFile, conf, cacheConf, BloomType.NONE, true, sft);
    Path ref = regionFS.splitStoreFile(region, "cf", file, splitPoint, false,
      new ConstantSizeRegionSplitPolicy(), sft);
    HStoreFile refHsf = new HStoreFile(this.fs, ref, conf, cacheConf, BloomType.NONE, true, sft);
    // starts reader for the ref. The ref should resolve to the original file blocks
    // and not duplicate blocks in the cache.
    refHsf.initReader();
    HFile.Reader reader = refHsf.getReader().getHFileReader();
    while (!reader.prefetchComplete()) {
      // Sleep for a bit
      Thread.sleep(1000);
    }
    // the ref file blocks keys should actually resolve to the referred file blocks,
    // so we should not see additional blocks in the cache.
    Waiter.waitFor(conf, 300, () -> bc.getBackingMap().size() == 6);

    BlockCacheKey refCacheKey = new BlockCacheKey(ref.getName(), 0);
    Cacheable result = bc.getBlock(refCacheKey, true, false, true);
    assertNotNull(result);
    BlockCacheKey fileCacheKey = new BlockCacheKey(file.getPath().getName(), 0);
    assertEquals(result, bc.getBlock(fileCacheKey, true, false, true));
    assertNull(bc.getBackingMap().get(refCacheKey));
    assertNotNull(bc.getBlockForReference(refCacheKey));
  }

  @Test
  public void testPrefetchInterruptOnCapacity() throws Exception {
    conf.setLong(BUCKET_CACHE_SIZE_KEY, 1);
    conf.set(BUCKET_CACHE_BUCKETS_KEY, "3072");
    conf.setDouble("hbase.bucketcache.acceptfactor", 0.98);
    conf.setDouble("hbase.bucketcache.minfactor", 0.98);
    conf.setDouble("hbase.bucketcache.extrafreefactor", 0.0);
    conf.setLong(QUEUE_ADDITION_WAIT_TIME, 100);
    blockCache = BlockCacheFactory.createBlockCache(conf);
    cacheConf = new CacheConfig(conf, blockCache);
    Path storeFile = writeStoreFile("testPrefetchInterruptOnCapacity", 10000);
    // Prefetches the file blocks
    LOG.debug("First read should prefetch the blocks.");
    createReaderAndWaitForPrefetchInterruption(storeFile);
    Waiter.waitFor(conf, (PrefetchExecutor.getPrefetchDelay() + 1000),
      () -> PrefetchExecutor.isCompleted(storeFile));
    BucketCache bc = BucketCache.getBucketCacheFromCacheConfig(cacheConf).get();
    long evictedFirstPrefetch = bc.getStats().getEvictedCount();
    HFile.Reader reader = createReaderAndWaitForPrefetchInterruption(storeFile);
    assertEquals(evictedFirstPrefetch, bc.getStats().getEvictedCount());
    HFileScanner scanner = reader.getScanner(conf, true, true);
    scanner.seekTo();
    while (scanner.next()) {
      // do a full scan to force some evictions
      LOG.trace("Iterating the full scan to evict some blocks");
    }
    scanner.close();
    Waiter.waitFor(conf, 5000, () -> {
      for (BlockingQueue<BucketCache.RAMQueueEntry> queue : bc.writerQueues) {
        if (!queue.isEmpty()) {
          return false;
        }
      }
      return true;
    });
    // The scanner should had triggered at least 3x evictions from the prefetch,
    // as we try cache each block without interruption.
    assertTrue(bc.getStats().getEvictedCount() > evictedFirstPrefetch);
  }

  @Test
  public void testPrefetchDoesntInterruptInMemoryOnCapacity() throws Exception {
    conf.setLong(BUCKET_CACHE_SIZE_KEY, 1);
    conf.set(BUCKET_CACHE_BUCKETS_KEY, "3072");
    conf.setDouble("hbase.bucketcache.acceptfactor", 0.98);
    conf.setDouble("hbase.bucketcache.minfactor", 0.98);
    conf.setDouble("hbase.bucketcache.extrafreefactor", 0.0);
    blockCache = BlockCacheFactory.createBlockCache(conf);
    ColumnFamilyDescriptor family =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("f")).setInMemory(true).build();
    cacheConf = new CacheConfig(conf, family, blockCache, ByteBuffAllocator.HEAP);
    Path storeFile = writeStoreFile("testPrefetchDoesntInterruptInMemoryOnCapacity", 10000);
    // Prefetches the file blocks
    LOG.debug("First read should prefetch the blocks.");
    createReaderAndWaitForPrefetchInterruption(storeFile);
    BucketCache bc = BucketCache.getBucketCacheFromCacheConfig(cacheConf).get();
    Waiter.waitFor(conf, 1000, () -> PrefetchExecutor.isCompleted(storeFile));
    long evictions = bc.getStats().getEvictedCount();
    LOG.debug("Total evicted at this point: {}", evictions);
    // creates another reader, now that cache is full, no block would fit and prefetch should not
    // trigger any new evictions
    createReaderAndWaitForPrefetchInterruption(storeFile);
    assertEquals(evictions, bc.getStats().getEvictedCount());
  }

  @Test
  public void testPrefetchRunNoEvictions() throws Exception {
    conf.setLong(BUCKET_CACHE_SIZE_KEY, 1);
    conf.set(BUCKET_CACHE_BUCKETS_KEY, "3072");
    conf.setDouble("hbase.bucketcache.acceptfactor", 0.98);
    conf.setDouble("hbase.bucketcache.minfactor", 0.98);
    conf.setDouble("hbase.bucketcache.extrafreefactor", 0.0);
    conf.setLong(QUEUE_ADDITION_WAIT_TIME, 100);
    blockCache = BlockCacheFactory.createBlockCache(conf);
    cacheConf = new CacheConfig(conf, blockCache);
    Path storeFile = writeStoreFile("testPrefetchRunNoEvictions", 10000);
    // Prefetches the file blocks
    createReaderAndWaitForPrefetchInterruption(storeFile);
    Waiter.waitFor(conf, (PrefetchExecutor.getPrefetchDelay() + 1000),
      () -> PrefetchExecutor.isCompleted(storeFile));
    BucketCache bc = BucketCache.getBucketCacheFromCacheConfig(cacheConf).get();
    // Wait until all cache writer queues are empty
    Waiter.waitFor(conf, 5000, () -> {
      for (BlockingQueue<BucketCache.RAMQueueEntry> queue : bc.writerQueues) {
        if (!queue.isEmpty()) {
          return false;
        }
      }
      return true;
    });
    // With the wait time configuration, prefetch should trigger no evictions once it reaches
    // cache capacity
    assertEquals(0, bc.getStats().getEvictedCount());
  }

  @Test
  public void testPrefetchRunTriggersEvictions() throws Exception {
    conf.setLong(BUCKET_CACHE_SIZE_KEY, 1);
    conf.set(BUCKET_CACHE_BUCKETS_KEY, "3072");
    conf.setDouble("hbase.bucketcache.acceptfactor", 0.98);
    conf.setDouble("hbase.bucketcache.minfactor", 0.98);
    conf.setDouble("hbase.bucketcache.extrafreefactor", 0.0);
    conf.setLong(QUEUE_ADDITION_WAIT_TIME, 0);
    blockCache = BlockCacheFactory.createBlockCache(conf);
    cacheConf = new CacheConfig(conf, blockCache);
    Path storeFile = writeStoreFile("testPrefetchInterruptOnCapacity", 10000);
    // Prefetches the file blocks
    createReaderAndWaitForPrefetchInterruption(storeFile);
    Waiter.waitFor(conf, (PrefetchExecutor.getPrefetchDelay() + 1000),
      () -> PrefetchExecutor.isCompleted(storeFile));
    BucketCache bc = BucketCache.getBucketCacheFromCacheConfig(cacheConf).get();
    // Wait until all cache writer queues are empty
    Waiter.waitFor(conf, 5000, () -> {
      for (BlockingQueue<BucketCache.RAMQueueEntry> queue : bc.writerQueues) {
        if (!queue.isEmpty()) {
          return false;
        }
      }
      return true;
    });
    // With the wait time configuration, prefetch should trigger no evictions once it reaches
    // cache capacity
    assertNotEquals(0, bc.getStats().getEvictedCount());
  }

  @Test
  public void testPrefetchMetricProgress() throws Exception {
    conf.setLong(BUCKET_CACHE_SIZE_KEY, 200);
    blockCache = BlockCacheFactory.createBlockCache(conf);
    cacheConf = new CacheConfig(conf, blockCache);
    Path storeFile = writeStoreFile("testPrefetchMetricsProgress", 100);
    // Prefetches the file blocks
    LOG.debug("First read should prefetch the blocks.");
    readStoreFile(storeFile);
    String regionName = storeFile.getParent().getParent().getName();
    BucketCache bc = BucketCache.getBucketCacheFromCacheConfig(cacheConf).get();
    MutableLong regionCachedSize = new MutableLong(0);
    // Our file should have 6 DATA blocks. We should wait for all of them to be cached
    long waitedTime = Waiter.waitFor(conf, 300, () -> {
      if (bc.getBackingMap().size() > 0) {
        long currentSize = bc.getRegionCachedInfo().get().get(regionName);
        assertTrue(regionCachedSize.getValue() <= currentSize);
        LOG.debug("Logging progress of region caching: {}", currentSize);
        regionCachedSize.setValue(currentSize);
      }
      return bc.getBackingMap().size() == 6;
    });
  }

  private void readStoreFile(Path storeFilePath) throws Exception {
    readStoreFile(storeFilePath, (r, o) -> {
      HFileBlock block = null;
      try {
        block = r.readBlock(o, -1, false, true, false, true, null, null);
      } catch (IOException e) {
        fail(e.getMessage());
      }
      return block;
    }, (key, block) -> {
      boolean isCached = blockCache.getBlock(key, true, false, true) != null;
      if (
        block.getBlockType() == BlockType.DATA || block.getBlockType() == BlockType.ROOT_INDEX
          || block.getBlockType() == BlockType.INTERMEDIATE_INDEX
      ) {
        assertTrue(isCached);
      }
    });
  }

  private void readStoreFile(Path storeFilePath,
    BiFunction<HFile.Reader, Long, HFileBlock> readFunction,
    BiConsumer<BlockCacheKey, HFileBlock> validationFunction) throws Exception {
    // Open the file
    HFile.Reader reader = HFile.createReader(fs, storeFilePath, cacheConf, true, conf);

    while (!reader.prefetchComplete()) {
      // Sleep for a bit
      Thread.sleep(1000);
    }
    long offset = 0;
    long sizeForDataBlocks = 0;
    while (offset < reader.getTrailer().getLoadOnOpenDataOffset()) {
      HFileBlock block = readFunction.apply(reader, offset);
      BlockCacheKey blockCacheKey = new BlockCacheKey(reader.getName(), offset);
      validationFunction.accept(blockCacheKey, block);
      offset += block.getOnDiskSizeWithHeader();
    }
  }

  private HFile.Reader createReaderAndWaitForPrefetchInterruption(Path storeFilePath)
    throws Exception {
    // Open the file
    HFile.Reader reader = HFile.createReader(fs, storeFilePath, cacheConf, true, conf);

    while (!reader.prefetchComplete()) {
      // Sleep for a bit
      Thread.sleep(1000);
    }
    assertEquals(0, BucketCache.getBucketCacheFromCacheConfig(cacheConf).get().getFullyCachedFiles()
      .get().size());

    return reader;
  }

  private Path writeStoreFile(String fname, int numKVs) throws IOException {
    HFileContext meta = new HFileContextBuilder().withBlockSize(DATA_BLOCK_SIZE).build();
    return writeStoreFile(fname, meta, numKVs);
  }

  private Path writeStoreFile(int numKVs, Path regionCFDir) throws IOException {
    HFileContext meta = new HFileContextBuilder().withBlockSize(DATA_BLOCK_SIZE).build();
    return writeStoreFile(meta, numKVs, regionCFDir);
  }

  private Path writeStoreFile(String fname, HFileContext context, int numKVs) throws IOException {
    return writeStoreFile(context, numKVs, new Path(TEST_UTIL.getDataTestDir(), fname));
  }

  private Path writeStoreFile(HFileContext context, int numKVs, Path regionCFDir)
    throws IOException {
    StoreFileWriter sfw = new StoreFileWriter.Builder(conf, cacheConf, fs)
      .withOutputDir(regionCFDir).withFileContext(context).build();
    Random rand = ThreadLocalRandom.current();
    final int rowLen = 32;
    for (int i = 0; i < numKVs; ++i) {
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
