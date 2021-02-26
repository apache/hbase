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
package org.apache.hadoop.hbase.mob;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(SmallTests.class)
public class TestCachedMobFile {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCachedMobFile.class);

  static final Logger LOG = LoggerFactory.getLogger(TestCachedMobFile.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private Configuration conf = TEST_UTIL.getConfiguration();
  private CacheConfig cacheConf = new CacheConfig(conf);
  private static final String FAMILY1 = "familyName1";
  private static final String FAMILY2 = "familyName2";
  private static final long EXPECTED_REFERENCE_ZERO = 0;
  private static final long EXPECTED_REFERENCE_ONE = 1;
  private static final long EXPECTED_REFERENCE_TWO = 2;
  @Rule
  public TestName testName = new TestName();

  @Test
  public void testOpenClose() throws Exception {
    String caseName = testName.getMethodName();
    Path testDir = TEST_UTIL.getDataTestDir();
    FileSystem fs = testDir.getFileSystem(conf);
    HFileContext meta = new HFileContextBuilder().withBlockSize(8*1024).build();
    StoreFileWriter writer = new StoreFileWriter.Builder(conf, cacheConf, fs)
        .withOutputDir(testDir).withFileContext(meta).build();
    MobTestUtil.writeStoreFile(writer, caseName);
    CachedMobFile cachedMobFile = CachedMobFile.create(fs, writer.getPath(), conf, cacheConf);
    assertEquals(EXPECTED_REFERENCE_ZERO, cachedMobFile.getReferenceCount());
    cachedMobFile.open();
    assertEquals(EXPECTED_REFERENCE_ONE, cachedMobFile.getReferenceCount());
    cachedMobFile.open();
    assertEquals(EXPECTED_REFERENCE_TWO, cachedMobFile.getReferenceCount());
    cachedMobFile.close();
    assertEquals(EXPECTED_REFERENCE_ONE, cachedMobFile.getReferenceCount());
    cachedMobFile.close();
    assertEquals(EXPECTED_REFERENCE_ZERO, cachedMobFile.getReferenceCount());
  }

  @SuppressWarnings("SelfComparison")
  @Test
  public void testCompare() throws Exception {
    String caseName = testName.getMethodName();
    Path testDir = TEST_UTIL.getDataTestDir();
    FileSystem fs = testDir.getFileSystem(conf);
    Path outputDir1 = new Path(testDir, FAMILY1);
    HFileContext meta = new HFileContextBuilder().withBlockSize(8 * 1024).build();
    StoreFileWriter writer1 = new StoreFileWriter.Builder(conf, cacheConf, fs)
        .withOutputDir(outputDir1).withFileContext(meta).build();
    MobTestUtil.writeStoreFile(writer1, caseName);
    CachedMobFile cachedMobFile1 = CachedMobFile.create(fs, writer1.getPath(), conf, cacheConf);
    Path outputDir2 = new Path(testDir, FAMILY2);
    StoreFileWriter writer2 = new StoreFileWriter.Builder(conf, cacheConf, fs)
        .withOutputDir(outputDir2)
        .withFileContext(meta)
        .build();
    MobTestUtil.writeStoreFile(writer2, caseName);
    CachedMobFile cachedMobFile2 = CachedMobFile.create(fs, writer2.getPath(), conf, cacheConf);
    cachedMobFile1.access(1);
    cachedMobFile2.access(2);
    assertEquals(1, cachedMobFile1.compareTo(cachedMobFile2));
    assertEquals(-1, cachedMobFile2.compareTo(cachedMobFile1));
    assertEquals(0, cachedMobFile1.compareTo(cachedMobFile1));
  }

  @Test
  public void testReadKeyValue() throws Exception {
    Path testDir = TEST_UTIL.getDataTestDir();
    FileSystem fs = testDir.getFileSystem(conf);
    HFileContext meta = new HFileContextBuilder().withBlockSize(8 * 1024).build();
    StoreFileWriter writer = new StoreFileWriter.Builder(conf, cacheConf, fs).withOutputDir(testDir)
        .withFileContext(meta).build();
    String caseName = testName.getMethodName();
    MobTestUtil.writeStoreFile(writer, caseName);
    CachedMobFile cachedMobFile = CachedMobFile.create(fs, writer.getPath(), conf, cacheConf);
    byte[] family = Bytes.toBytes(caseName);
    byte[] qualify = Bytes.toBytes(caseName);
    // Test the start key
    byte[] startKey = Bytes.toBytes("aa"); // The start key bytes
    KeyValue expectedKey =
        new KeyValue(startKey, family, qualify, Long.MAX_VALUE, Type.Put, startKey);
    KeyValue seekKey = expectedKey.createKeyOnly(false);
    Cell cell = cachedMobFile.readCell(seekKey, false).getCell();
    MobTestUtil.assertCellEquals(expectedKey, cell);

    // Test the end key
    byte[] endKey = Bytes.toBytes("zz"); // The end key bytes
    expectedKey = new KeyValue(endKey, family, qualify, Long.MAX_VALUE, Type.Put, endKey);
    seekKey = expectedKey.createKeyOnly(false);
    cell = cachedMobFile.readCell(seekKey, false).getCell();
    MobTestUtil.assertCellEquals(expectedKey, cell);

    // Test the random key
    byte[] randomKey = Bytes.toBytes(MobTestUtil.generateRandomString(2));
    expectedKey = new KeyValue(randomKey, family, qualify, Long.MAX_VALUE, Type.Put, randomKey);
    seekKey = expectedKey.createKeyOnly(false);
    cell = cachedMobFile.readCell(seekKey, false).getCell();
    MobTestUtil.assertCellEquals(expectedKey, cell);

    // Test the key which is less than the start key
    byte[] lowerKey = Bytes.toBytes("a1"); // Smaller than "aa"
    expectedKey = new KeyValue(startKey, family, qualify, Long.MAX_VALUE, Type.Put, startKey);
    seekKey = new KeyValue(lowerKey, family, qualify, Long.MAX_VALUE, Type.Put, lowerKey);
    cell = cachedMobFile.readCell(seekKey, false).getCell();
    MobTestUtil.assertCellEquals(expectedKey, cell);

    // Test the key which is more than the end key
    byte[] upperKey = Bytes.toBytes("z{"); // Bigger than "zz"
    seekKey = new KeyValue(upperKey, family, qualify, Long.MAX_VALUE, Type.Put, upperKey);
    Assert.assertNull(cachedMobFile.readCell(seekKey, false));
  }
}
