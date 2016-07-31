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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestStoreFileScannerWithTagCompression {

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf = TEST_UTIL.getConfiguration();
  private static CacheConfig cacheConf = new CacheConfig(TEST_UTIL.getConfiguration());
  private static String ROOT_DIR = TEST_UTIL.getDataTestDir(
      "TestStoreFileScannerWithTagCompression").toString();
  private static FileSystem fs = null;

  @BeforeClass
  public static void setUp() throws IOException {
    conf.setInt("hfile.format.version", 3);
    fs = FileSystem.get(conf);
  }

  @Test
  public void testReseek() throws Exception {
    // write the file
    Path f = new Path(ROOT_DIR, "testReseek");
    HFileContext meta = new HFileContextBuilder().withBlockSize(8 * 1024).withIncludesTags(true)
        .withCompressTags(true).withDataBlockEncoding(DataBlockEncoding.PREFIX).build();
    // Make a store file and write data to it.
    StoreFile.Writer writer = new StoreFile.WriterBuilder(conf, cacheConf, fs).withFilePath(f)
        .withFileContext(meta).build();

    writeStoreFile(writer);
    writer.close();

    StoreFile.Reader reader = new StoreFile.Reader(fs, f, cacheConf, conf);
    StoreFileScanner s = reader.getStoreFileScanner(false, false, false, 0, 0, false);
    try {
      // Now do reseek with empty KV to position to the beginning of the file
      KeyValue k = KeyValueUtil.createFirstOnRow(Bytes.toBytes("k2"));
      s.reseek(k);
      Cell kv = s.next();
      kv = s.next();
      kv = s.next();
      byte[] key5 = Bytes.toBytes("k5");
      assertTrue(Bytes.equals(key5, 0, key5.length, kv.getRowArray(), kv.getRowOffset(),
          kv.getRowLength()));
      List<Tag> tags = KeyValueUtil.ensureKeyValue(kv).getTags();
      assertEquals(1, tags.size());
      assertEquals("tag3", Bytes.toString(tags.get(0).getValue()));
    } finally {
      s.close();
    }
  }

  private void writeStoreFile(final StoreFile.Writer writer) throws IOException {
    byte[] fam = Bytes.toBytes("f");
    byte[] qualifier = Bytes.toBytes("q");
    long now = System.currentTimeMillis();
    byte[] b = Bytes.toBytes("k1");
    Tag t1 = new Tag((byte) 1, "tag1");
    Tag t2 = new Tag((byte) 2, "tag2");
    Tag t3 = new Tag((byte) 3, "tag3");
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
