/**
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
package org.apache.hadoop.hbase.mob;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestCachedMobFile extends TestCase{
  static final Log LOG = LogFactory.getLog(TestCachedMobFile.class);
  private Configuration conf = HBaseConfiguration.create();
  private CacheConfig cacheConf = new CacheConfig(conf);
  private static final String TABLE = "tableName";
  private static final String FAMILY = "familyName";
  private static final String FAMILY1 = "familyName1";
  private static final String FAMILY2 = "familyName2";
  private static final long EXPECTED_REFERENCE_ZERO = 0;
  private static final long EXPECTED_REFERENCE_ONE = 1;
  private static final long EXPECTED_REFERENCE_TWO = 2;

  @Test
  public void testOpenClose() throws Exception {
    String caseName = getName();
    FileSystem fs = FileSystem.get(conf);
    Path testDir = FSUtils.getRootDir(conf);
    Path outputDir = new Path(new Path(testDir, TABLE),
        FAMILY);
    HFileContext meta = new HFileContextBuilder().withBlockSize(8*1024).build();
    StoreFile.Writer writer = new StoreFile.WriterBuilder(conf, cacheConf, fs)
        .withOutputDir(outputDir).withFileContext(meta).build();
    MobTestUtil.writeStoreFile(writer, caseName);
    CachedMobFile cachedMobFile = CachedMobFile.create(fs, writer.getPath(), conf, cacheConf);
    Assert.assertEquals(EXPECTED_REFERENCE_ZERO, cachedMobFile.getReferenceCount());
    cachedMobFile.open();
    Assert.assertEquals(EXPECTED_REFERENCE_ONE, cachedMobFile.getReferenceCount());
    cachedMobFile.open();
    Assert.assertEquals(EXPECTED_REFERENCE_TWO, cachedMobFile.getReferenceCount());
    cachedMobFile.close();
    Assert.assertEquals(EXPECTED_REFERENCE_ONE, cachedMobFile.getReferenceCount());
    cachedMobFile.close();
    Assert.assertEquals(EXPECTED_REFERENCE_ZERO, cachedMobFile.getReferenceCount());
  }

  @Test
  public void testCompare() throws Exception {
    String caseName = getName();
    FileSystem fs = FileSystem.get(conf);
    Path testDir = FSUtils.getRootDir(conf);
    Path outputDir1 = new Path(new Path(testDir, TABLE),
        FAMILY1);
    HFileContext meta = new HFileContextBuilder().withBlockSize(8 * 1024).build();
    StoreFile.Writer writer1 = new StoreFile.WriterBuilder(conf, cacheConf, fs)
        .withOutputDir(outputDir1).withFileContext(meta).build();
    MobTestUtil.writeStoreFile(writer1, caseName);
    CachedMobFile cachedMobFile1 = CachedMobFile.create(fs, writer1.getPath(), conf, cacheConf);
    Path outputDir2 = new Path(new Path(testDir, TABLE),
        FAMILY2);
    StoreFile.Writer writer2 = new StoreFile.WriterBuilder(conf, cacheConf, fs)
    .withOutputDir(outputDir2)
    .withFileContext(meta)
    .build();
    MobTestUtil.writeStoreFile(writer2, caseName);
    CachedMobFile cachedMobFile2 = CachedMobFile.create(fs, writer2.getPath(), conf, cacheConf);
    cachedMobFile1.access(1);
    cachedMobFile2.access(2);
    Assert.assertEquals(cachedMobFile1.compareTo(cachedMobFile2), 1);
    Assert.assertEquals(cachedMobFile2.compareTo(cachedMobFile1), -1);
    Assert.assertEquals(cachedMobFile1.compareTo(cachedMobFile1), 0);
  }

  @Test
  public void testReadKeyValue() throws Exception {
    FileSystem fs = FileSystem.get(conf);
    Path testDir = FSUtils.getRootDir(conf);
    Path outputDir = new Path(new Path(testDir, TABLE), "familyname");
    HFileContext meta = new HFileContextBuilder().withBlockSize(8 * 1024).build();
    StoreFile.Writer writer = new StoreFile.WriterBuilder(conf, cacheConf, fs)
        .withOutputDir(outputDir).withFileContext(meta).build();
    String caseName = getName();
    MobTestUtil.writeStoreFile(writer, caseName);
    CachedMobFile cachedMobFile = CachedMobFile.create(fs, writer.getPath(), conf, cacheConf);
    byte[] family = Bytes.toBytes(caseName);
    byte[] qualify = Bytes.toBytes(caseName);
    // Test the start key
    byte[] startKey = Bytes.toBytes("aa");  // The start key bytes
    KeyValue expectedKey =
        new KeyValue(startKey, family, qualify, Long.MAX_VALUE, Type.Put, startKey);
    KeyValue seekKey = expectedKey.createKeyOnly(false);
    Cell cell = cachedMobFile.readCell(seekKey, false);
    MobTestUtil.assertCellEquals(expectedKey, cell);

    // Test the end key
    byte[] endKey = Bytes.toBytes("zz");  // The end key bytes
    expectedKey = new KeyValue(endKey, family, qualify, Long.MAX_VALUE, Type.Put, endKey);
    seekKey = expectedKey.createKeyOnly(false);
    cell = cachedMobFile.readCell(seekKey, false);
    MobTestUtil.assertCellEquals(expectedKey, cell);

    // Test the random key
    byte[] randomKey = Bytes.toBytes(MobTestUtil.generateRandomString(2));
    expectedKey = new KeyValue(randomKey, family, qualify, Long.MAX_VALUE, Type.Put, randomKey);
    seekKey = expectedKey.createKeyOnly(false);
    cell = cachedMobFile.readCell(seekKey, false);
    MobTestUtil.assertCellEquals(expectedKey, cell);

    // Test the key which is less than the start key
    byte[] lowerKey = Bytes.toBytes("a1"); // Smaller than "aa"
    expectedKey = new KeyValue(startKey, family, qualify, Long.MAX_VALUE, Type.Put, startKey);
    seekKey = new KeyValue(lowerKey, family, qualify, Long.MAX_VALUE, Type.Put, lowerKey);
    cell = cachedMobFile.readCell(seekKey, false);
    MobTestUtil.assertCellEquals(expectedKey, cell);

    // Test the key which is more than the end key
    byte[] upperKey = Bytes.toBytes("z{"); // Bigger than "zz"
    seekKey = new KeyValue(upperKey, family, qualify, Long.MAX_VALUE, Type.Put, upperKey);
    cell = cachedMobFile.readCell(seekKey, false);
    Assert.assertNull(cell);
  }
}
