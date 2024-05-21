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
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.HConstants.BUCKET_CACHE_SIZE_KEY;
import static org.apache.hadoop.hbase.io.hfile.bucket.BucketCache.DEFAULT_ERROR_TOLERATION_DURATION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.BlockCacheFactory;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.CacheTestUtils;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to test the functionality of the DataTieringManager.
 *
 * The mock online regions are stored in {@link TestDataTieringManager#testOnlineRegions}.
 * For all tests, the setup of {@link TestDataTieringManager#testOnlineRegions} occurs only once.
 * Please refer to {@link TestDataTieringManager#setupOnlineRegions()} for the structure.
 * Additionally, a list of all store files is maintained in {@link TestDataTieringManager#hStoreFiles}.
 * The characteristics of these store files are listed below:
 * @formatter:off ## HStoreFile Information
 *
 * | HStoreFile       | Region             | Store               | DataTiering           | isHot |
 * |------------------|--------------------|---------------------|-----------------------|-------|
 * | hStoreFile0      | region1            | hStore11            | TIME_RANGE            | true  |
 * | hStoreFile1      | region1            | hStore12            | NONE                  | true  |
 * | hStoreFile2      | region2            | hStore21            | TIME_RANGE            | true  |
 * | hStoreFile3      | region2            | hStore22            | TIME_RANGE            | false |
 * @formatter:on
 */

@Category({ RegionServerTests.class, SmallTests.class })
public class TestDataTieringManager {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestDataTieringManager.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestDataTieringManager.class);
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final long DAY = 24 * 60 * 60 * 1000;
  private static Configuration defaultConf;
  private static FileSystem fs;
  private static BlockCache blockCache;
  private static CacheConfig cacheConf;
  private static Path testDir;
  private static final Map<String, HRegion> testOnlineRegions = new HashMap<>();

  private static DataTieringManager dataTieringManager;
  private static final List<HStoreFile> hStoreFiles = new ArrayList<>();

  /**
   * Represents the current lexicographically increasing string used as a row key when writing
   * HFiles. It is incremented each time {@link #nextString()} is called to generate unique row
   * keys.
   */
  private static String rowKeyString;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    testDir = TEST_UTIL.getDataTestDir(TestDataTieringManager.class.getSimpleName());
    defaultConf = TEST_UTIL.getConfiguration();
    updateCommonConfigurations();
    assertTrue(DataTieringManager.instantiate(defaultConf, testOnlineRegions));
    dataTieringManager = DataTieringManager.getInstance();
    rowKeyString = "";
  }

  private static void updateCommonConfigurations() {
    defaultConf.setBoolean(DataTieringManager.GLOBAL_DATA_TIERING_ENABLED_KEY, true);
    defaultConf.setStrings(HConstants.BUCKET_CACHE_IOENGINE_KEY, "offheap");
    defaultConf.setLong(BUCKET_CACHE_SIZE_KEY, 32);
  }

  @FunctionalInterface
  interface DataTieringMethodCallerWithPath {
    boolean call(DataTieringManager manager, Path path) throws DataTieringException;
  }

  @FunctionalInterface
  interface DataTieringMethodCallerWithKey {
    boolean call(DataTieringManager manager, BlockCacheKey key) throws DataTieringException;
  }

  @Test
  public void testDataTieringEnabledWithKey() throws IOException {
    initializeTestEnvironment();
    DataTieringMethodCallerWithKey methodCallerWithKey = DataTieringManager::isDataTieringEnabled;

    // Test with valid key
    BlockCacheKey key = new BlockCacheKey(hStoreFiles.get(0).getPath(), 0, true, BlockType.DATA);
    testDataTieringMethodWithKeyNoException(methodCallerWithKey, key, true);

    // Test with another valid key
    key = new BlockCacheKey(hStoreFiles.get(1).getPath(), 0, true, BlockType.DATA);
    testDataTieringMethodWithKeyNoException(methodCallerWithKey, key, false);

    // Test with valid key with no HFile Path
    key = new BlockCacheKey(hStoreFiles.get(0).getPath().getName(), 0);
    testDataTieringMethodWithKeyExpectingException(methodCallerWithKey, key,
      new DataTieringException("BlockCacheKey Doesn't Contain HFile Path"));
  }

  @Test
  public void testDataTieringEnabledWithPath() throws IOException {
    initializeTestEnvironment();
    DataTieringMethodCallerWithPath methodCallerWithPath = DataTieringManager::isDataTieringEnabled;

    // Test with valid path
    Path hFilePath = hStoreFiles.get(1).getPath();
    testDataTieringMethodWithPathNoException(methodCallerWithPath, hFilePath, false);

    // Test with another valid path
    hFilePath = hStoreFiles.get(3).getPath();
    testDataTieringMethodWithPathNoException(methodCallerWithPath, hFilePath, true);

    // Test with an incorrect path
    hFilePath = new Path("incorrectPath");
    testDataTieringMethodWithPathExpectingException(methodCallerWithPath, hFilePath,
      new DataTieringException("Incorrect HFile Path: " + hFilePath));

    // Test with a non-existing HRegion path
    Path basePath = hStoreFiles.get(0).getPath().getParent().getParent().getParent();
    hFilePath = new Path(basePath, "incorrectRegion/cf1/filename");
    testDataTieringMethodWithPathExpectingException(methodCallerWithPath, hFilePath,
      new DataTieringException("HRegion corresponding to " + hFilePath + " doesn't exist"));

    // Test with a non-existing HStore path
    basePath = hStoreFiles.get(0).getPath().getParent().getParent();
    hFilePath = new Path(basePath, "incorrectCf/filename");
    testDataTieringMethodWithPathExpectingException(methodCallerWithPath, hFilePath,
      new DataTieringException("HStore corresponding to " + hFilePath + " doesn't exist"));
  }

  @Test
  public void testHotDataWithKey() throws IOException {
    initializeTestEnvironment();
    DataTieringMethodCallerWithKey methodCallerWithKey = DataTieringManager::isHotData;

    // Test with valid key
    BlockCacheKey key = new BlockCacheKey(hStoreFiles.get(0).getPath(), 0, true, BlockType.DATA);
    testDataTieringMethodWithKeyNoException(methodCallerWithKey, key, true);

    // Test with another valid key
    key = new BlockCacheKey(hStoreFiles.get(3).getPath(), 0, true, BlockType.DATA);
    testDataTieringMethodWithKeyNoException(methodCallerWithKey, key, false);
  }

  @Test
  public void testHotDataWithPath() throws IOException {
    initializeTestEnvironment();
    DataTieringMethodCallerWithPath methodCallerWithPath = DataTieringManager::isHotData;

    // Test with valid path
    Path hFilePath = hStoreFiles.get(2).getPath();
    testDataTieringMethodWithPathNoException(methodCallerWithPath, hFilePath, true);

    // Test with another valid path
    hFilePath = hStoreFiles.get(3).getPath();
    testDataTieringMethodWithPathNoException(methodCallerWithPath, hFilePath, false);

    // Test with a filename where corresponding HStoreFile in not present
    hFilePath = new Path(hStoreFiles.get(0).getPath().getParent(), "incorrectFileName");
    testDataTieringMethodWithPathNoException(methodCallerWithPath, hFilePath, true);
  }

  @Test
  public void testPrefetchWhenDataTieringEnabled() throws IOException {
    setPrefetchBlocksOnOpen();
    initializeTestEnvironment();
    // Evict blocks from cache by closing the files and passing evict on close.
    // Then initialize the reader again. Since Prefetch on open is set to true, it should prefetch
    // those blocks.
    for (HStoreFile file : hStoreFiles) {
      file.closeStoreFile(true);
      file.initReader();
    }

    // Since we have one cold file among four files, only three should get prefetched.
    Optional<Map<String, Pair<String, Long>>> fullyCachedFiles = blockCache.getFullyCachedFiles();
    assertTrue("We should get the fully cached files from the cache", fullyCachedFiles.isPresent());
    Waiter.waitFor(defaultConf, 10000, () -> fullyCachedFiles.get().size() == 3);
    assertEquals("Number of fully cached files are incorrect", 3, fullyCachedFiles.get().size());
  }

  private void setPrefetchBlocksOnOpen() {
    defaultConf.setBoolean(CacheConfig.PREFETCH_BLOCKS_ON_OPEN_KEY, true);
  }

  @Test
  public void testColdDataFiles() throws IOException {
    initializeTestEnvironment();
    Set<BlockCacheKey> allCachedBlocks = new HashSet<>();
    for (HStoreFile file : hStoreFiles) {
      allCachedBlocks.add(new BlockCacheKey(file.getPath(), 0, true, BlockType.DATA));
    }

    // Verify hStoreFile3 is identified as cold data
    DataTieringMethodCallerWithPath methodCallerWithPath = DataTieringManager::isHotData;
    Path hFilePath = hStoreFiles.get(3).getPath();
    testDataTieringMethodWithPathNoException(methodCallerWithPath, hFilePath, false);

    // Verify all the other files in hStoreFiles are hot data
    for (int i = 0; i < hStoreFiles.size() - 1; i++) {
      hFilePath = hStoreFiles.get(i).getPath();
      testDataTieringMethodWithPathNoException(methodCallerWithPath, hFilePath, true);
    }

    try {
      Set<String> coldFilePaths = dataTieringManager.getColdDataFiles(allCachedBlocks);
      assertEquals(1, coldFilePaths.size());
    } catch (DataTieringException e) {
      fail("Unexpected DataTieringException: " + e.getMessage());
    }
  }

  @Test
  public void testCacheCompactedBlocksOnWriteDataTieringDisabled() throws IOException {
    setCacheCompactBlocksOnWrite();
    initializeTestEnvironment();

    HRegion region = createHRegion("table3");
    testCacheCompactedBlocksOnWrite(region, true);
  }

  @Test
  public void testCacheCompactedBlocksOnWriteWithHotData() throws IOException {
    setCacheCompactBlocksOnWrite();
    initializeTestEnvironment();

    HRegion region = createHRegion("table3", getConfWithTimeRangeDataTieringEnabled(5 * DAY));
    testCacheCompactedBlocksOnWrite(region, true);
  }

  @Test
  public void testCacheCompactedBlocksOnWriteWithColdData() throws IOException {
    setCacheCompactBlocksOnWrite();
    initializeTestEnvironment();

    HRegion region = createHRegion("table3", getConfWithTimeRangeDataTieringEnabled(DAY));
    testCacheCompactedBlocksOnWrite(region, false);
  }

  private void setCacheCompactBlocksOnWrite() {
    defaultConf.setBoolean(CacheConfig.CACHE_COMPACTED_BLOCKS_ON_WRITE_KEY, true);
  }

  private void testCacheCompactedBlocksOnWrite(HRegion region, boolean expectDataBlocksCached)
    throws IOException {
    HStore hStore = createHStore(region, "cf1");
    createTestFilesForCompaction(hStore);
    hStore.refreshStoreFiles();

    region.stores.put(Bytes.toBytes("cf1"), hStore);
    testOnlineRegions.put(region.getRegionInfo().getEncodedName(), region);

    long initialStoreFilesCount = hStore.getStorefilesCount();
    long initialCacheDataBlockCount = blockCache.getDataBlockCount();
    assertEquals(3, initialStoreFilesCount);
    assertEquals(0, initialCacheDataBlockCount);

    region.compact(true);

    long compactedStoreFilesCount = hStore.getStorefilesCount();
    long compactedCacheDataBlockCount = blockCache.getDataBlockCount();
    assertEquals(1, compactedStoreFilesCount);
    assertEquals(expectDataBlocksCached, compactedCacheDataBlockCount > 0);
  }

  private void createTestFilesForCompaction(HStore hStore) throws IOException {
    long currentTime = System.currentTimeMillis();
    Path storeDir = hStore.getStoreContext().getFamilyStoreDirectoryPath();
    Configuration configuration = hStore.getReadOnlyConfiguration();

    createHStoreFile(storeDir, configuration, currentTime - 2 * DAY);
    createHStoreFile(storeDir, configuration, currentTime - 3 * DAY);
    createHStoreFile(storeDir, configuration, currentTime - 4 * DAY);
  }

  @Test
  public void testPickColdDataFiles() throws IOException {
    initializeTestEnvironment();
    Map<String, String> coldDataFiles = dataTieringManager.getColdFilesList();
    assertEquals(1, coldDataFiles.size());
    // hStoreFiles[3] is the cold file.
    assert (coldDataFiles.containsKey(hStoreFiles.get(3).getFileInfo().getActiveFileName()));
  }

  /*
   * Verify that two cold blocks(both) are evicted when bucket reaches its capacity. The hot file
   * remains in the cache.
   */
  @Test
  public void testBlockEvictions() throws Exception {
    initializeTestEnvironment();
    long capacitySize = 40 * 1024;
    int writeThreads = 3;
    int writerQLen = 64;
    int[] bucketSizes = new int[] { 8 * 1024 + 1024 };

    // Setup: Create a bucket cache with lower capacity
    BucketCache bucketCache =
      new BucketCache("file:" + testDir + "/bucket.cache", capacitySize, 8192, bucketSizes,
        writeThreads, writerQLen, null, DEFAULT_ERROR_TOLERATION_DURATION, defaultConf);

    // Create three Cache keys with cold data files and a block with hot data.
    // hStoreFiles.get(3) is a cold data file, while hStoreFiles.get(0) is a hot file.
    Set<BlockCacheKey> cacheKeys = new HashSet<>();
    cacheKeys.add(new BlockCacheKey(hStoreFiles.get(3).getPath(), 0, true, BlockType.DATA));
    cacheKeys.add(new BlockCacheKey(hStoreFiles.get(3).getPath(), 8192, true, BlockType.DATA));
    cacheKeys.add(new BlockCacheKey(hStoreFiles.get(0).getPath(), 0, true, BlockType.DATA));

    // Create dummy data to be cached and fill the cache completely.
    CacheTestUtils.HFileBlockPair[] blocks = CacheTestUtils.generateHFileBlocks(8192, 3);

    int blocksIter = 0;
    for (BlockCacheKey key : cacheKeys) {
      bucketCache.cacheBlock(key, blocks[blocksIter++].getBlock());
      // Ensure that the block is persisted to the file.
      Waiter.waitFor(defaultConf, 10000, 100, () -> (bucketCache.getBackingMap().containsKey(key)));
    }

    // Verify that the bucket cache contains 3 blocks.
    assertEquals(3, bucketCache.getBackingMap().keySet().size());

    // Add an additional block into cache with hot data which should trigger the eviction
    BlockCacheKey newKey = new BlockCacheKey(hStoreFiles.get(2).getPath(), 0, true, BlockType.DATA);
    CacheTestUtils.HFileBlockPair[] newBlock = CacheTestUtils.generateHFileBlocks(8192, 1);

    bucketCache.cacheBlock(newKey, newBlock[0].getBlock());
    Waiter.waitFor(defaultConf, 10000, 100,
      () -> (bucketCache.getBackingMap().containsKey(newKey)));

    // Verify that the bucket cache now contains 2 hot blocks blocks only.
    // Both cold blocks of 8KB will be evicted to make room for 1 block of 8KB + an additional
    // space.
    validateBlocks(bucketCache.getBackingMap().keySet(), 2, 2, 0);
  }

  /*
   * Verify that two cold blocks(both) are evicted when bucket reaches its capacity, but one cold
   * block remains in the cache since the required space is freed.
   */
  @Test
  public void testBlockEvictionsAllColdBlocks() throws Exception {
    initializeTestEnvironment();
    long capacitySize = 40 * 1024;
    int writeThreads = 3;
    int writerQLen = 64;
    int[] bucketSizes = new int[] { 8 * 1024 + 1024 };

    // Setup: Create a bucket cache with lower capacity
    BucketCache bucketCache =
      new BucketCache("file:" + testDir + "/bucket.cache", capacitySize, 8192, bucketSizes,
        writeThreads, writerQLen, null, DEFAULT_ERROR_TOLERATION_DURATION, defaultConf);

    // Create three Cache keys with three cold data blocks.
    // hStoreFiles.get(3) is a cold data file.
    Set<BlockCacheKey> cacheKeys = new HashSet<>();
    cacheKeys.add(new BlockCacheKey(hStoreFiles.get(3).getPath(), 0, true, BlockType.DATA));
    cacheKeys.add(new BlockCacheKey(hStoreFiles.get(3).getPath(), 8192, true, BlockType.DATA));
    cacheKeys.add(new BlockCacheKey(hStoreFiles.get(3).getPath(), 16384, true, BlockType.DATA));

    // Create dummy data to be cached and fill the cache completely.
    CacheTestUtils.HFileBlockPair[] blocks = CacheTestUtils.generateHFileBlocks(8192, 3);

    int blocksIter = 0;
    for (BlockCacheKey key : cacheKeys) {
      bucketCache.cacheBlock(key, blocks[blocksIter++].getBlock());
      // Ensure that the block is persisted to the file.
      Waiter.waitFor(defaultConf, 10000, 100, () -> (bucketCache.getBackingMap().containsKey(key)));
    }

    // Verify that the bucket cache contains 3 blocks.
    assertEquals(3, bucketCache.getBackingMap().keySet().size());

    // Add an additional block into cache with hot data which should trigger the eviction
    BlockCacheKey newKey = new BlockCacheKey(hStoreFiles.get(2).getPath(), 0, true, BlockType.DATA);
    CacheTestUtils.HFileBlockPair[] newBlock = CacheTestUtils.generateHFileBlocks(8192, 1);

    bucketCache.cacheBlock(newKey, newBlock[0].getBlock());
    Waiter.waitFor(defaultConf, 10000, 100,
      () -> (bucketCache.getBackingMap().containsKey(newKey)));

    // Verify that the bucket cache now contains 1 cold block and a newly added hot block.
    validateBlocks(bucketCache.getBackingMap().keySet(), 2, 1, 1);
  }

  /*
   * Verify that a hot block evicted along with a cold block when bucket reaches its capacity.
   */
  @Test
  public void testBlockEvictionsHotBlocks() throws Exception {
    initializeTestEnvironment();
    long capacitySize = 40 * 1024;
    int writeThreads = 3;
    int writerQLen = 64;
    int[] bucketSizes = new int[] { 8 * 1024 + 1024 };

    // Setup: Create a bucket cache with lower capacity
    BucketCache bucketCache =
      new BucketCache("file:" + testDir + "/bucket.cache", capacitySize, 8192, bucketSizes,
        writeThreads, writerQLen, null, DEFAULT_ERROR_TOLERATION_DURATION, defaultConf);

    // Create three Cache keys with two hot data blocks and one cold data block
    // hStoreFiles.get(0) is a hot data file and hStoreFiles.get(3) is a cold data file.
    Set<BlockCacheKey> cacheKeys = new HashSet<>();
    cacheKeys.add(new BlockCacheKey(hStoreFiles.get(0).getPath(), 0, true, BlockType.DATA));
    cacheKeys.add(new BlockCacheKey(hStoreFiles.get(0).getPath(), 8192, true, BlockType.DATA));
    cacheKeys.add(new BlockCacheKey(hStoreFiles.get(3).getPath(), 0, true, BlockType.DATA));

    // Create dummy data to be cached and fill the cache completely.
    CacheTestUtils.HFileBlockPair[] blocks = CacheTestUtils.generateHFileBlocks(8192, 3);

    int blocksIter = 0;
    for (BlockCacheKey key : cacheKeys) {
      bucketCache.cacheBlock(key, blocks[blocksIter++].getBlock());
      // Ensure that the block is persisted to the file.
      Waiter.waitFor(defaultConf, 10000, 100, () -> (bucketCache.getBackingMap().containsKey(key)));
    }

    // Verify that the bucket cache contains 3 blocks.
    assertEquals(3, bucketCache.getBackingMap().keySet().size());

    // Add an additional block which should evict the only cold block with an additional hot block.
    BlockCacheKey newKey = new BlockCacheKey(hStoreFiles.get(2).getPath(), 0, true, BlockType.DATA);
    CacheTestUtils.HFileBlockPair[] newBlock = CacheTestUtils.generateHFileBlocks(8192, 1);

    bucketCache.cacheBlock(newKey, newBlock[0].getBlock());
    Waiter.waitFor(defaultConf, 10000, 100,
      () -> (bucketCache.getBackingMap().containsKey(newKey)));

    // Verify that the bucket cache now contains 2 hot blocks.
    // Only one of the older hot blocks is retained and other one is the newly added hot block.
    validateBlocks(bucketCache.getBackingMap().keySet(), 2, 2, 0);
  }

  @Test
  public void testFeatureKeyDisabled() throws Exception {
    DataTieringManager.resetForTestingOnly();
    defaultConf.setBoolean(DataTieringManager.GLOBAL_DATA_TIERING_ENABLED_KEY, false);
    initializeTestEnvironment();

    try {
      assertFalse(DataTieringManager.instantiate(defaultConf, testOnlineRegions));
      // Verify that the DataaTieringManager instance is not instantiated in the
      // instantiate call above.
      assertNull(DataTieringManager.getInstance());

      // Also validate that data temperature is not honoured.
      long capacitySize = 40 * 1024;
      int writeThreads = 3;
      int writerQLen = 64;
      int[] bucketSizes = new int[] { 8 * 1024 + 1024 };

      // Setup: Create a bucket cache with lower capacity
      BucketCache bucketCache =
        new BucketCache("file:" + testDir + "/bucket.cache", capacitySize, 8192, bucketSizes,
          writeThreads, writerQLen, null, DEFAULT_ERROR_TOLERATION_DURATION, defaultConf);

      // Create three Cache keys with two hot data blocks and one cold data block
      // hStoreFiles.get(0) is a hot data file and hStoreFiles.get(3) is a cold data file.
      List<BlockCacheKey> cacheKeys = new ArrayList<>();
      cacheKeys.add(new BlockCacheKey(hStoreFiles.get(0).getPath(), 0, true, BlockType.DATA));
      cacheKeys.add(new BlockCacheKey(hStoreFiles.get(0).getPath(), 8192, true, BlockType.DATA));
      cacheKeys.add(new BlockCacheKey(hStoreFiles.get(3).getPath(), 0, true, BlockType.DATA));

      // Create dummy data to be cached and fill the cache completely.
      CacheTestUtils.HFileBlockPair[] blocks = CacheTestUtils.generateHFileBlocks(8192, 3);

      int blocksIter = 0;
      for (BlockCacheKey key : cacheKeys) {
        LOG.info("Adding {}", key);
        bucketCache.cacheBlock(key, blocks[blocksIter++].getBlock());
        // Ensure that the block is persisted to the file.
        Waiter.waitFor(defaultConf, 10000, 100,
          () -> (bucketCache.getBackingMap().containsKey(key)));
      }

      // Verify that the bucket cache contains 3 blocks.
      assertEquals(3, bucketCache.getBackingMap().keySet().size());

      // Add an additional hot block, which triggers eviction.
      BlockCacheKey newKey =
        new BlockCacheKey(hStoreFiles.get(2).getPath(), 0, true, BlockType.DATA);
      CacheTestUtils.HFileBlockPair[] newBlock = CacheTestUtils.generateHFileBlocks(8192, 1);

      bucketCache.cacheBlock(newKey, newBlock[0].getBlock());
      Waiter.waitFor(defaultConf, 10000, 100,
        () -> (bucketCache.getBackingMap().containsKey(newKey)));

      // Verify that the bucket still contains the only cold block and one newly added hot block.
      // The older hot blocks are evicted and data-tiering mechanism does not kick in to evict
      // the cold block.
      validateBlocks(bucketCache.getBackingMap().keySet(), 2, 1, 1);
    } finally {
      DataTieringManager.resetForTestingOnly();
      defaultConf.setBoolean(DataTieringManager.GLOBAL_DATA_TIERING_ENABLED_KEY, true);
      assertTrue(DataTieringManager.instantiate(defaultConf, testOnlineRegions));
    }
  }

  private void validateBlocks(Set<BlockCacheKey> keys, int expectedTotalKeys, int expectedHotBlocks,
    int expectedColdBlocks) {
    int numHotBlocks = 0, numColdBlocks = 0;

    Waiter.waitFor(defaultConf, 10000, 100, () -> (expectedTotalKeys == keys.size()));
    int iter = 0;
    for (BlockCacheKey key : keys) {
      try {
        if (dataTieringManager.isHotData(key)) {
          numHotBlocks++;
        } else {
          numColdBlocks++;
        }
      } catch (Exception e) {
        fail("Unexpected exception!");
      }
    }
    assertEquals(expectedHotBlocks, numHotBlocks);
    assertEquals(expectedColdBlocks, numColdBlocks);
  }

  private void testDataTieringMethodWithPath(DataTieringMethodCallerWithPath caller, Path path,
    boolean expectedResult, DataTieringException exception) {
    try {
      boolean value = caller.call(dataTieringManager, path);
      if (exception != null) {
        fail("Expected DataTieringException to be thrown");
      }
      assertEquals(expectedResult, value);
    } catch (DataTieringException e) {
      if (exception == null) {
        fail("Unexpected DataTieringException: " + e.getMessage());
      }
      assertEquals(exception.getMessage(), e.getMessage());
    }
  }

  private void testDataTieringMethodWithKey(DataTieringMethodCallerWithKey caller,
    BlockCacheKey key, boolean expectedResult, DataTieringException exception) {
    try {
      boolean value = caller.call(dataTieringManager, key);
      if (exception != null) {
        fail("Expected DataTieringException to be thrown");
      }
      assertEquals(expectedResult, value);
    } catch (DataTieringException e) {
      if (exception == null) {
        fail("Unexpected DataTieringException: " + e.getMessage());
      }
      assertEquals(exception.getMessage(), e.getMessage());
    }
  }

  private void testDataTieringMethodWithPathExpectingException(
    DataTieringMethodCallerWithPath caller, Path path, DataTieringException exception) {
    testDataTieringMethodWithPath(caller, path, false, exception);
  }

  private void testDataTieringMethodWithPathNoException(DataTieringMethodCallerWithPath caller,
    Path path, boolean expectedResult) {
    testDataTieringMethodWithPath(caller, path, expectedResult, null);
  }

  private void testDataTieringMethodWithKeyExpectingException(DataTieringMethodCallerWithKey caller,
    BlockCacheKey key, DataTieringException exception) {
    testDataTieringMethodWithKey(caller, key, false, exception);
  }

  private void testDataTieringMethodWithKeyNoException(DataTieringMethodCallerWithKey caller,
    BlockCacheKey key, boolean expectedResult) {
    testDataTieringMethodWithKey(caller, key, expectedResult, null);
  }

  private static void initializeTestEnvironment() throws IOException {
    setupFileSystemAndCache();
    setupOnlineRegions();
  }

  private static void setupFileSystemAndCache() throws IOException {
    fs = HFileSystem.get(defaultConf);
    blockCache = BlockCacheFactory.createBlockCache(defaultConf);
    cacheConf = new CacheConfig(defaultConf, blockCache);
  }

  private static void setupOnlineRegions() throws IOException {
    testOnlineRegions.clear();
    hStoreFiles.clear();
    long day = 24 * 60 * 60 * 1000;
    long currentTime = System.currentTimeMillis();

    HRegion region1 = createHRegion("table1");

    HStore hStore11 = createHStore(region1, "cf1", getConfWithTimeRangeDataTieringEnabled(day));
    hStoreFiles.add(createHStoreFile(hStore11.getStoreContext().getFamilyStoreDirectoryPath(),
      hStore11.getReadOnlyConfiguration(), currentTime));
    hStore11.refreshStoreFiles();
    HStore hStore12 = createHStore(region1, "cf2");
    hStoreFiles.add(createHStoreFile(hStore12.getStoreContext().getFamilyStoreDirectoryPath(),
      hStore12.getReadOnlyConfiguration(), currentTime - day));
    hStore12.refreshStoreFiles();

    region1.stores.put(Bytes.toBytes("cf1"), hStore11);
    region1.stores.put(Bytes.toBytes("cf2"), hStore12);

    HRegion region2 =
      createHRegion("table2", getConfWithTimeRangeDataTieringEnabled((long) (2.5 * day)));

    HStore hStore21 = createHStore(region2, "cf1");
    hStoreFiles.add(createHStoreFile(hStore21.getStoreContext().getFamilyStoreDirectoryPath(),
      hStore21.getReadOnlyConfiguration(), currentTime - 2 * day));
    hStore21.refreshStoreFiles();
    HStore hStore22 = createHStore(region2, "cf2");
    hStoreFiles.add(createHStoreFile(hStore22.getStoreContext().getFamilyStoreDirectoryPath(),
      hStore22.getReadOnlyConfiguration(), currentTime - 3 * day));
    hStore22.refreshStoreFiles();

    region2.stores.put(Bytes.toBytes("cf1"), hStore21);
    region2.stores.put(Bytes.toBytes("cf2"), hStore22);

    for (HStoreFile file : hStoreFiles) {
      file.initReader();
    }

    testOnlineRegions.put(region1.getRegionInfo().getEncodedName(), region1);
    testOnlineRegions.put(region2.getRegionInfo().getEncodedName(), region2);
  }

  private static HRegion createHRegion(String table) throws IOException {
    return createHRegion(table, defaultConf);
  }

  private static HRegion createHRegion(String table, Configuration conf) throws IOException {
    TableName tableName = TableName.valueOf(table);

    TableDescriptor htd = TableDescriptorBuilder.newBuilder(tableName)
      .setValue(DataTieringManager.DATATIERING_KEY, conf.get(DataTieringManager.DATATIERING_KEY))
      .setValue(DataTieringManager.DATATIERING_HOT_DATA_AGE_KEY,
        conf.get(DataTieringManager.DATATIERING_HOT_DATA_AGE_KEY))
      .build();
    RegionInfo hri = RegionInfoBuilder.newBuilder(tableName).build();

    Configuration testConf = new Configuration(conf);
    CommonFSUtils.setRootDir(testConf, testDir);
    HRegionFileSystem regionFs = HRegionFileSystem.createRegionOnFileSystem(testConf, fs,
      CommonFSUtils.getTableDir(testDir, hri.getTable()), hri);

    HRegion region = new HRegion(regionFs, null, conf, htd, null);
    // Manually sets the BlockCache for the HRegion instance.
    // This is necessary because the region server is not started within this method,
    // and therefore the BlockCache needs to be explicitly configured.
    region.setBlockCache(blockCache);
    return region;
  }

  private static HStore createHStore(HRegion region, String columnFamily) throws IOException {
    return createHStore(region, columnFamily, defaultConf);
  }

  private static HStore createHStore(HRegion region, String columnFamily, Configuration conf)
    throws IOException {
    ColumnFamilyDescriptor columnFamilyDescriptor =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily))
        .setValue(DataTieringManager.DATATIERING_KEY, conf.get(DataTieringManager.DATATIERING_KEY))
        .setValue(DataTieringManager.DATATIERING_HOT_DATA_AGE_KEY,
          conf.get(DataTieringManager.DATATIERING_HOT_DATA_AGE_KEY))
        .build();

    return new HStore(region, columnFamilyDescriptor, conf, false);
  }

  private static Configuration getConfWithTimeRangeDataTieringEnabled(long hotDataAge) {
    Configuration conf = new Configuration(defaultConf);
    conf.set(DataTieringManager.DATATIERING_KEY, DataTieringType.TIME_RANGE.name());
    conf.set(DataTieringManager.DATATIERING_HOT_DATA_AGE_KEY, String.valueOf(hotDataAge));
    return conf;
  }

  private static HStoreFile createHStoreFile(Path storeDir, Configuration conf, long timestamp)
    throws IOException {
    String columnFamily = storeDir.getName();

    StoreFileWriter storeFileWriter = new StoreFileWriter.Builder(conf, cacheConf, fs)
      .withOutputDir(storeDir).withFileContext(new HFileContextBuilder().build()).build();

    writeStoreFileRandomData(storeFileWriter, Bytes.toBytes(columnFamily), timestamp);

    return new HStoreFile(fs, storeFileWriter.getPath(), conf, cacheConf, BloomType.NONE, true);
  }

  /**
   * Writes random data to a store file with rows arranged in lexicographically increasing order.
   * Each row is generated using the {@link #nextString()} method, ensuring that each subsequent row
   * is lexicographically larger than the previous one.
   */
  private static void writeStoreFileRandomData(final StoreFileWriter writer, byte[] columnFamily,
    long timestamp) throws IOException {
    int cellsPerFile = 10;
    byte[] qualifier = Bytes.toBytes("qualifier");
    byte[] value = generateRandomBytes(4 * 1024);
    try {
      for (int i = 0; i < cellsPerFile; i++) {
        byte[] row = Bytes.toBytes(nextString());
        writer.append(new KeyValue(row, columnFamily, qualifier, timestamp, value));
      }
    } finally {
      writer.appendTrackedTimestampsToMetadata();
      writer.close();
    }
  }

  private static byte[] generateRandomBytes(int sizeInBytes) {
    Random random = new Random();
    byte[] randomBytes = new byte[sizeInBytes];
    random.nextBytes(randomBytes);
    return randomBytes;
  }

  /**
   * Returns the lexicographically larger string every time it's called.
   */
  private static String nextString() {
    if (rowKeyString == null || rowKeyString.isEmpty()) {
      rowKeyString = "a";
    }
    char lastChar = rowKeyString.charAt(rowKeyString.length() - 1);
    if (lastChar < 'z') {
      rowKeyString = rowKeyString.substring(0, rowKeyString.length() - 1) + (char) (lastChar + 1);
    } else {
      rowKeyString = rowKeyString + "a";
    }
    return rowKeyString;
  }
}
