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
package org.apache.hadoop.hbase.io.hfile;

import static org.junit.Assert.assertEquals;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Test
 */
@Category({ IOTests.class, SmallTests.class})
public class TestHFileReaderImpl {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHFileReaderImpl.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  static KeyValue toKV(String row) {
    return new KeyValue(Bytes.toBytes(row), Bytes.toBytes("family"), Bytes.toBytes("qualifier"),
        Bytes.toBytes("value"));
  }

  static String toRowStr(Cell c) {
    return Bytes.toString(c.getRowArray(), c.getRowOffset(), c.getRowLength());
  }

  Path makeNewFile() throws IOException {
    Path ncTFile = new Path(TEST_UTIL.getDataTestDir(), "basic.hfile");
    FSDataOutputStream fout = TEST_UTIL.getTestFileSystem().create(ncTFile);
    int blocksize = toKV("a").getLength() * 3;
    HFileContext context =
        new HFileContextBuilder().withBlockSize(blocksize).withIncludesTags(true).build();
    Configuration conf = TEST_UTIL.getConfiguration();
    HFile.Writer writer = HFile.getWriterFactoryNoCache(conf)
      .withOutputStream(fout).withFileContext(context).create();
    // 4 bytes * 3 * 2 for each key/value +
    // 3 for keys, 15 for values = 42 (woot)
    writer.append(toKV("c"));
    writer.append(toKV("e"));
    writer.append(toKV("g"));
    // block transition
    writer.append(toKV("i"));
    writer.append(toKV("k"));
    writer.close();
    fout.close();
    return ncTFile;
  }

  @Test
  public void testSeekBefore() throws Exception {
    Path p = makeNewFile();
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Configuration conf = TEST_UTIL.getConfiguration();
    int[] bucketSizes = { 512, 2048, 4096, 64 * 1024, 128 * 1024 };
    BucketCache bucketcache =
        new BucketCache("offheap", 128 * 1024 * 1024, 64 * 1024, bucketSizes, 5, 64 * 100, null);

    HFile.Reader reader = HFile.createReader(fs, p, new CacheConfig(conf, bucketcache), true, conf);

    // warm cache
    HFileScanner scanner = reader.getScanner(true, true);
    scanner.seekTo(toKV("i"));
    assertEquals("i", toRowStr(scanner.getCell()));
    scanner.close();

    while (bucketcache.getBlockCount() <= 0) {
      Thread.sleep(10);
    }

    // reopen again.
    scanner = reader.getScanner(true, true);
    scanner.seekTo(toKV("i"));
    assertEquals("i", toRowStr(scanner.getCell()));
    scanner.seekBefore(toKV("i"));
    assertEquals("g", toRowStr(scanner.getCell()));
    scanner.close();

    for (CachedBlock cachedBlock : Lists.newArrayList(bucketcache)) {
      BlockCacheKey cacheKey =
          new BlockCacheKey(cachedBlock.getFilename(), cachedBlock.getOffset());
      int refCount = bucketcache.getRpcRefCount(cacheKey);
      assertEquals(0, refCount);
    }

    // case 2
    scanner = reader.getScanner(true, true);
    scanner.seekTo(toKV("i"));
    assertEquals("i", toRowStr(scanner.getCell()));
    scanner.seekBefore(toKV("c"));
    scanner.close();
    for (CachedBlock cachedBlock : Lists.newArrayList(bucketcache)) {
      BlockCacheKey cacheKey =
          new BlockCacheKey(cachedBlock.getFilename(), cachedBlock.getOffset());
      int refCount = bucketcache.getRpcRefCount(cacheKey);
      assertEquals(0, refCount);
    }

    reader.close();

    // clear bucketcache
    for (CachedBlock cachedBlock : Lists.newArrayList(bucketcache)) {
      BlockCacheKey cacheKey =
          new BlockCacheKey(cachedBlock.getFilename(), cachedBlock.getOffset());
      bucketcache.evictBlock(cacheKey);
    }
    bucketcache.shutdown();

    deleteTestDir(fs);
  }

  protected void deleteTestDir(FileSystem fs) throws IOException {
    Path dataTestDir = TEST_UTIL.getDataTestDir();
    if(fs.exists(dataTestDir)) {
      fs.delete(dataTestDir, true);
    }
  }

}
