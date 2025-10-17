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
package org.apache.hadoop.hbase.mob;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.regionserver.HMobStore;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.StoreContext;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(SmallTests.class)
public class TestMobFileCache {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMobFileCache.class);

  static final Logger LOG = LoggerFactory.getLogger(TestMobFileCache.class);
  private HBaseTestingUtility UTIL;
  private HRegion region;
  private Configuration conf;
  private MobFileCache mobFileCache;
  private Date currentDate = new Date();
  private static final String TEST_CACHE_SIZE = "2";
  private static final int EXPECTED_CACHE_SIZE_ZERO = 0;
  private static final int EXPECTED_CACHE_SIZE_ONE = 1;
  private static final int EXPECTED_CACHE_SIZE_TWO = 2;
  private static final int EXPECTED_CACHE_SIZE_THREE = 3;
  private static final long EXPECTED_REFERENCE_ONE = 1;
  private static final long EXPECTED_REFERENCE_TWO = 2;

  private static final String TABLE = "tableName";
  private static final String FAMILY1 = "family1";
  private static final String FAMILY2 = "family2";
  private static final String FAMILY3 = "family3";

  private static final byte[] ROW = Bytes.toBytes("row");
  private static final byte[] ROW2 = Bytes.toBytes("row2");
  private static final byte[] VALUE = Bytes.toBytes("value");
  private static final byte[] VALUE2 = Bytes.toBytes("value2");
  private static final byte[] QF1 = Bytes.toBytes("qf1");
  private static final byte[] QF2 = Bytes.toBytes("qf2");
  private static final byte[] QF3 = Bytes.toBytes("qf3");

  @Before
  public void setUp() throws Exception {
    UTIL = HBaseTestingUtility.createLocalHTU();
    conf = UTIL.getConfiguration();
    conf.set(MobConstants.MOB_FILE_CACHE_SIZE_KEY, TEST_CACHE_SIZE);
    HTableDescriptor htd = UTIL.createTableDescriptor("testMobFileCache");
    HColumnDescriptor hcd1 = new HColumnDescriptor(FAMILY1);
    hcd1.setMobEnabled(true);
    hcd1.setMobThreshold(0);
    HColumnDescriptor hcd2 = new HColumnDescriptor(FAMILY2);
    hcd2.setMobEnabled(true);
    hcd2.setMobThreshold(0);
    HColumnDescriptor hcd3 = new HColumnDescriptor(FAMILY3);
    hcd3.setMobEnabled(true);
    hcd3.setMobThreshold(0);
    htd.addFamily(hcd1);
    htd.addFamily(hcd2);
    htd.addFamily(hcd3);
    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(htd.getTableName()).build();
    mobFileCache = new MobFileCache(conf);
    region = HBaseTestingUtility.createRegionAndWAL(regionInfo, UTIL.getDataTestDir(), conf, htd,
      mobFileCache);
  }

  @After
  public void tearDown() throws Exception {
    region.close();
    region.getFilesystem().delete(UTIL.getDataTestDir(), true);
  }

  /**
   * Create the mob store file.
   */
  private Pair<Path, StoreContext> createAndGetMobStoreFileContextPair(String family)
    throws IOException {
    ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder
      .newBuilder(Bytes.toBytes(family)).setMaxVersions(4).setMobEnabled(true).build();
    // Setting up a Store
    TableName tn = TableName.valueOf(TABLE);
    HMobStore mobStore = (HMobStore) region.getStore(columnFamilyDescriptor.getName());
    KeyValue key1 = new KeyValue(ROW, columnFamilyDescriptor.getName(), QF1, 1, VALUE);
    KeyValue key2 = new KeyValue(ROW, columnFamilyDescriptor.getName(), QF2, 1, VALUE);
    KeyValue key3 = new KeyValue(ROW2, columnFamilyDescriptor.getName(), QF3, 1, VALUE2);
    KeyValue[] keys = new KeyValue[] { key1, key2, key3 };
    int maxKeyCount = keys.length;
    HRegionInfo regionInfo = new HRegionInfo(tn);
    StoreFileWriter mobWriter = mobStore.createWriterInTmp(currentDate, maxKeyCount,
      columnFamilyDescriptor.getCompactionCompressionType(), regionInfo.getStartKey(), false);
    Path mobFilePath = mobWriter.getPath();
    String fileName = mobFilePath.getName();
    mobWriter.append(key1);
    mobWriter.append(key2);
    mobWriter.append(key3);
    mobWriter.close();
    String targetPathName = MobUtils.formatDate(currentDate);
    Path targetPath = new Path(mobStore.getPath(), targetPathName);
    mobStore.commitFile(mobFilePath, targetPath);
    return new Pair(new Path(targetPath, fileName), mobStore.getStoreContext());
  }

  @Test
  public void testMobFileCache() throws Exception {
    FileSystem fs = FileSystem.get(conf);
    Pair<Path, StoreContext> fileAndContextPair1 = createAndGetMobStoreFileContextPair(FAMILY1);
    Pair<Path, StoreContext> fileAndContextPair2 = createAndGetMobStoreFileContextPair(FAMILY2);
    Pair<Path, StoreContext> fileAndContextPair3 = createAndGetMobStoreFileContextPair(FAMILY3);

    Path file1Path = fileAndContextPair1.getFirst();
    Path file2Path = fileAndContextPair2.getFirst();
    Path file3Path = fileAndContextPair3.getFirst();

    CacheConfig cacheConf = new CacheConfig(conf);
    // Before open one file by the MobFileCache
    assertEquals(EXPECTED_CACHE_SIZE_ZERO, mobFileCache.getCacheSize());
    // Open one file by the MobFileCache
    CachedMobFile cachedMobFile1 = (CachedMobFile) mobFileCache.openFile(fs, file1Path, cacheConf,
      fileAndContextPair1.getSecond());
    assertEquals(EXPECTED_CACHE_SIZE_ONE, mobFileCache.getCacheSize());
    assertNotNull(cachedMobFile1);
    assertEquals(EXPECTED_REFERENCE_TWO, cachedMobFile1.getReferenceCount());

    // The evict is also managed by a schedule thread pool.
    // And its check period is set as 3600 seconds by default.
    // This evict should get the lock at the most time
    mobFileCache.evict(); // Cache not full, evict it
    assertEquals(EXPECTED_CACHE_SIZE_ONE, mobFileCache.getCacheSize());
    assertEquals(EXPECTED_REFERENCE_TWO, cachedMobFile1.getReferenceCount());

    mobFileCache.evictFile(file1Path.getName()); // Evict one file
    assertEquals(EXPECTED_CACHE_SIZE_ZERO, mobFileCache.getCacheSize());
    assertEquals(EXPECTED_REFERENCE_ONE, cachedMobFile1.getReferenceCount());

    cachedMobFile1.close(); // Close the cached mob file

    // Reopen three cached file
    cachedMobFile1 = (CachedMobFile) mobFileCache.openFile(fs, file1Path, cacheConf,
      fileAndContextPair1.getSecond());
    assertEquals(EXPECTED_CACHE_SIZE_ONE, mobFileCache.getCacheSize());
    CachedMobFile cachedMobFile2 = (CachedMobFile) mobFileCache.openFile(fs, file2Path, cacheConf,
      fileAndContextPair2.getSecond());
    assertEquals(EXPECTED_CACHE_SIZE_TWO, mobFileCache.getCacheSize());
    CachedMobFile cachedMobFile3 = (CachedMobFile) mobFileCache.openFile(fs, file3Path, cacheConf,
      fileAndContextPair3.getSecond());
    // Before the evict
    // Evict the cache, should close the first file 1
    assertEquals(EXPECTED_CACHE_SIZE_THREE, mobFileCache.getCacheSize());
    assertEquals(EXPECTED_REFERENCE_TWO, cachedMobFile1.getReferenceCount());
    assertEquals(EXPECTED_REFERENCE_TWO, cachedMobFile2.getReferenceCount());
    assertEquals(EXPECTED_REFERENCE_TWO, cachedMobFile3.getReferenceCount());
    mobFileCache.evict();
    assertEquals(EXPECTED_CACHE_SIZE_ONE, mobFileCache.getCacheSize());
    assertEquals(EXPECTED_REFERENCE_ONE, cachedMobFile1.getReferenceCount());
    assertEquals(EXPECTED_REFERENCE_ONE, cachedMobFile2.getReferenceCount());
    assertEquals(EXPECTED_REFERENCE_TWO, cachedMobFile3.getReferenceCount());
  }
}
