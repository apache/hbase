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
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestMobFile extends TestCase {
  static final Log LOG = LogFactory.getLog(TestMobFile.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private Configuration conf = TEST_UTIL.getConfiguration();
  private CacheConfig cacheConf =  new CacheConfig(conf);
  private final String TABLE = "tableName";
  private final String FAMILY = "familyName";

  @Test
  public void testReadKeyValue() throws Exception {
    FileSystem fs = FileSystem.get(conf);
	Path testDir = FSUtils.getRootDir(conf);
    Path outputDir = new Path(new Path(testDir, TABLE), FAMILY);
    HFileContext meta = new HFileContextBuilder().withBlockSize(8*1024).build();
    StoreFile.Writer writer = new StoreFile.WriterBuilder(conf, cacheConf, fs)
            .withOutputDir(outputDir)
            .withFileContext(meta)
            .build();
    String caseName = getName();
    MobTestUtil.writeStoreFile(writer, caseName);

    MobFile mobFile = new MobFile(new StoreFile(fs, writer.getPath(),
        conf, cacheConf, BloomType.NONE));
    byte[] family = Bytes.toBytes(caseName);
    byte[] qualify = Bytes.toBytes(caseName);

    // Test the start key
    byte[] startKey = Bytes.toBytes("aa");  // The start key bytes
    KeyValue expectedKey =
        new KeyValue(startKey, family, qualify, Long.MAX_VALUE, Type.Put, startKey);
    KeyValue seekKey = expectedKey.createKeyOnly(false);
    Cell cell = mobFile.readCell(seekKey, false);
    MobTestUtil.assertCellEquals(expectedKey, cell);

    // Test the end key
    byte[] endKey = Bytes.toBytes("zz");  // The end key bytes
    expectedKey = new KeyValue(endKey, family, qualify, Long.MAX_VALUE, Type.Put, endKey);
    seekKey = expectedKey.createKeyOnly(false);
    cell = mobFile.readCell(seekKey, false);
    MobTestUtil.assertCellEquals(expectedKey, cell);

    // Test the random key
    byte[] randomKey = Bytes.toBytes(MobTestUtil.generateRandomString(2));
    expectedKey = new KeyValue(randomKey, family, qualify, Long.MAX_VALUE, Type.Put, randomKey);
    seekKey = expectedKey.createKeyOnly(false);
    cell = mobFile.readCell(seekKey, false);
    MobTestUtil.assertCellEquals(expectedKey, cell);

    // Test the key which is less than the start key
    byte[] lowerKey = Bytes.toBytes("a1"); // Smaller than "aa"
    expectedKey = new KeyValue(startKey, family, qualify, Long.MAX_VALUE, Type.Put, startKey);
    seekKey = new KeyValue(lowerKey, family, qualify, Long.MAX_VALUE, Type.Put, lowerKey);
    cell = mobFile.readCell(seekKey, false);
    MobTestUtil.assertCellEquals(expectedKey, cell);

    // Test the key which is more than the end key
    byte[] upperKey = Bytes.toBytes("z{"); // Bigger than "zz"
    seekKey = new KeyValue(upperKey, family, qualify, Long.MAX_VALUE, Type.Put, upperKey);
    cell = mobFile.readCell(seekKey, false);
    assertNull(cell);
  }

  @Test
  public void testGetScanner() throws Exception {
    FileSystem fs = FileSystem.get(conf);
    Path testDir = FSUtils.getRootDir(conf);
    Path outputDir = new Path(new Path(testDir, TABLE), FAMILY);
    HFileContext meta = new HFileContextBuilder().withBlockSize(8*1024).build();
    StoreFile.Writer writer = new StoreFile.WriterBuilder(conf, cacheConf, fs)
            .withOutputDir(outputDir)
            .withFileContext(meta)
            .build();
    MobTestUtil.writeStoreFile(writer, getName());

    MobFile mobFile = new MobFile(new StoreFile(fs, writer.getPath(),
        conf, cacheConf, BloomType.NONE));
    assertNotNull(mobFile.getScanner());
    assertTrue(mobFile.getScanner() instanceof StoreFileScanner);
  }
}
