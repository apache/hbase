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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
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
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

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

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static Configuration defaultConf;
  private static FileSystem fs;
  private static CacheConfig cacheConf;
  private static Path testDir;
  private static Map<String, HRegion> testOnlineRegions;

  private static DataTieringManager dataTieringManager;
  private static List<HStoreFile> hStoreFiles;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    testDir = TEST_UTIL.getDataTestDir(TestDataTieringManager.class.getSimpleName());
    defaultConf = TEST_UTIL.getConfiguration();
    fs = HFileSystem.get(defaultConf);
    BlockCache blockCache = BlockCacheFactory.createBlockCache(defaultConf);
    cacheConf = new CacheConfig(defaultConf, blockCache);
    setupOnlineRegions();
    DataTieringManager.instantiate(testOnlineRegions);
    dataTieringManager = DataTieringManager.getInstance();
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
  public void testDataTieringEnabledWithKey() {
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
  public void testDataTieringEnabledWithPath() {
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
  public void testHotDataWithKey() {
    DataTieringMethodCallerWithKey methodCallerWithKey = DataTieringManager::isHotData;

    // Test with valid key
    BlockCacheKey key = new BlockCacheKey(hStoreFiles.get(0).getPath(), 0, true, BlockType.DATA);
    testDataTieringMethodWithKeyNoException(methodCallerWithKey, key, true);

    // Test with another valid key
    key = new BlockCacheKey(hStoreFiles.get(3).getPath(), 0, true, BlockType.DATA);
    testDataTieringMethodWithKeyNoException(methodCallerWithKey, key, false);
  }

  @Test
  public void testHotDataWithPath() {
    DataTieringMethodCallerWithPath methodCallerWithPath = DataTieringManager::isHotData;

    // Test with valid path
    Path hFilePath = hStoreFiles.get(2).getPath();
    testDataTieringMethodWithPathNoException(methodCallerWithPath, hFilePath, true);

    // Test with another valid path
    hFilePath = hStoreFiles.get(3).getPath();
    testDataTieringMethodWithPathNoException(methodCallerWithPath, hFilePath, false);

    // Test with a filename where corresponding HStoreFile in not present
    hFilePath = new Path(hStoreFiles.get(0).getPath().getParent(), "incorrectFileName");
    testDataTieringMethodWithPathNoException(methodCallerWithPath, hFilePath, false);
  }

  @Test
  public void testColdDataFiles() {
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

  private static void setupOnlineRegions() throws IOException {
    testOnlineRegions = new HashMap<>();
    hStoreFiles = new ArrayList<>();

    long day = 24 * 60 * 60 * 1000;
    long currentTime = System.currentTimeMillis();

    HRegion region1 = createHRegion("table1");

    HStore hStore11 = createHStore(region1, "cf1", getConfWithTimeRangeDataTieringEnabled(day));
    hStoreFiles
      .add(createHStoreFile(hStore11.getStoreContext().getFamilyStoreDirectoryPath(), currentTime));
    hStore11.refreshStoreFiles();
    HStore hStore12 = createHStore(region1, "cf2");
    hStoreFiles.add(createHStoreFile(hStore12.getStoreContext().getFamilyStoreDirectoryPath(),
      currentTime - day));
    hStore12.refreshStoreFiles();

    region1.stores.put(Bytes.toBytes("cf1"), hStore11);
    region1.stores.put(Bytes.toBytes("cf2"), hStore12);

    HRegion region2 =
      createHRegion("table2", getConfWithTimeRangeDataTieringEnabled((long) (2.5 * day)));

    HStore hStore21 = createHStore(region2, "cf1");
    hStoreFiles.add(createHStoreFile(hStore21.getStoreContext().getFamilyStoreDirectoryPath(),
      currentTime - 2 * day));
    hStore21.refreshStoreFiles();
    HStore hStore22 = createHStore(region2, "cf2");
    hStoreFiles.add(createHStoreFile(hStore22.getStoreContext().getFamilyStoreDirectoryPath(),
      currentTime - 3 * day));
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

    return new HRegion(regionFs, null, conf, htd, null);
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

  private static HStoreFile createHStoreFile(Path storeDir, long timestamp) throws IOException {
    String columnFamily = storeDir.getName();

    StoreFileWriter storeFileWriter = new StoreFileWriter.Builder(defaultConf, cacheConf, fs)
      .withOutputDir(storeDir).withFileContext(new HFileContextBuilder().build()).build();

    writeStoreFileRandomData(storeFileWriter, Bytes.toBytes(columnFamily), Bytes.toBytes("random"),
      timestamp);

    return new HStoreFile(fs, storeFileWriter.getPath(), defaultConf, cacheConf, BloomType.NONE,
      true);
  }

  private static void writeStoreFileRandomData(final StoreFileWriter writer, byte[] columnFamily,
    byte[] qualifier, long timestamp) throws IOException {
    try {
      for (char d = 'a'; d <= 'z'; d++) {
        for (char e = 'a'; e <= 'z'; e++) {
          byte[] b = new byte[] { (byte) d, (byte) e };
          writer.append(new KeyValue(b, columnFamily, qualifier, timestamp, b));
        }
      }
    } finally {
      writer.appendTrackedTimestampsToMetadata();
      writer.close();
    }
  }
}
