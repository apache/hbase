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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.ReaderContext;
import org.apache.hadoop.hbase.io.hfile.ReaderContextBuilder;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestStoreFileScannerWithTagCompression {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestStoreFileScannerWithTagCompression.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static Configuration conf = TEST_UTIL.getConfiguration();
  private static CacheConfig cacheConf = new CacheConfig(TEST_UTIL.getConfiguration());
  private static Path ROOT_DIR = TEST_UTIL.getDataTestDir("TestStoreFileScannerWithTagCompression");
  private static FileSystem fs = null;

  @BeforeClass
  public static void setUp() throws IOException {
    conf.setInt("hfile.format.version", 3);
    fs = FileSystem.get(conf);
  }

  @Test
  public void testReseek() throws Exception {
    // write the file
    if (!fs.exists(ROOT_DIR)) {
      fs.mkdirs(ROOT_DIR);
    }
    Path f = StoreFileWriter.getUniqueFile(fs, ROOT_DIR);
    HFileContext meta = new HFileContextBuilder().withBlockSize(8 * 1024).withIncludesTags(true)
      .withCompressTags(true).withDataBlockEncoding(DataBlockEncoding.PREFIX).build();
    // Make a store file and write data to it.
    StoreFileWriter writer = new StoreFileWriter.Builder(conf, cacheConf, fs).withFilePath(f)
      .withFileContext(meta).build();

    writeStoreFile(writer);
    writer.close();

    ReaderContext context = new ReaderContextBuilder().withFileSystemAndPath(fs, f).build();
    StoreFileInfo storeFileInfo = StoreFileInfo.createStoreFileInfoForHFile(conf, fs, f, true);
    storeFileInfo.initHFileInfo(context);
    StoreFileReader reader = storeFileInfo.createReader(context, cacheConf);
    storeFileInfo.getHFileInfo().initMetaAndIndex(reader.getHFileReader());
    StoreFileScanner s = reader.getStoreFileScanner(false, false, false, 0, 0, false);
    try {
      // Now do reseek with empty KV to position to the beginning of the file
      KeyValue k = KeyValueUtil.createFirstOnRow(Bytes.toBytes("k2"));
      s.reseek(k);
      ExtendedCell kv = s.next();
      kv = s.next();
      kv = s.next();
      byte[] key5 = Bytes.toBytes("k5");
      assertTrue(
        Bytes.equals(key5, 0, key5.length, kv.getRowArray(), kv.getRowOffset(), kv.getRowLength()));
      List<Tag> tags = PrivateCellUtil.getTags(kv);
      assertEquals(1, tags.size());
      assertEquals("tag3", Bytes.toString(Tag.cloneValue(tags.get(0))));
    } finally {
      s.close();
    }
  }

  private void writeStoreFile(final StoreFileWriter writer) throws IOException {
    byte[] fam = Bytes.toBytes("f");
    byte[] qualifier = Bytes.toBytes("q");
    long now = EnvironmentEdgeManager.currentTime();
    byte[] b = Bytes.toBytes("k1");
    Tag t1 = new ArrayBackedTag((byte) 1, "tag1");
    Tag t2 = new ArrayBackedTag((byte) 2, "tag2");
    Tag t3 = new ArrayBackedTag((byte) 3, "tag3");
    try {
      writer.append(new KeyValue(b, fam, qualifier, now, b, new Tag[] { t1 }));
      b = Bytes.toBytes("k3");
      writer.append(new KeyValue(b, fam, qualifier, now, b, new Tag[] { t2, t1 }));
      b = Bytes.toBytes("k4");
      writer.append(new KeyValue(b, fam, qualifier, now, b, new Tag[] { t3 }));
      b = Bytes.toBytes("k5");
      writer.append(new KeyValue(b, fam, qualifier, now, b, new Tag[] { t3 }));
    } finally {
      writer.close();
    }
  }
}
