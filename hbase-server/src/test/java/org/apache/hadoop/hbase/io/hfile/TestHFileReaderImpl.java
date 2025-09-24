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
package org.apache.hadoop.hbase.io.hfile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
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
@Category({ IOTests.class, SmallTests.class })
public class TestHFileReaderImpl {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHFileReaderImpl.class);

  private final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

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
    HFile.Writer writer =
      HFile.getWriterFactoryNoCache(conf).withOutputStream(fout).withFileContext(context).create();
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

  /**
   * Test that we only count block size once per block while scanning
   */
  @Test
  public void testRecordBlockSize() throws IOException {
    Path p = makeNewFile();
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Configuration conf = TEST_UTIL.getConfiguration();
    HFile.Reader reader = HFile.createReader(fs, p, CacheConfig.DISABLED, true, conf);

    try (HFileReaderImpl.HFileScannerImpl scanner =
      (HFileReaderImpl.HFileScannerImpl) reader.getScanner(conf, true, true, false)) {
      scanner.seekTo();

      scanner.recordBlockSize(
        size -> assertTrue("expected non-zero block size on first request", size > 0));
      scanner.recordBlockSize(
        size -> assertEquals("expected zero block size on second request", 0, (int) size));

      AtomicInteger blocks = new AtomicInteger(0);
      while (scanner.next()) {
        scanner.recordBlockSize(size -> {
          blocks.incrementAndGet();
          // there's only 2 cells in the second block
          assertTrue("expected remaining block to be less than block size",
            size < toKV("a").getLength() * 3);
        });
      }

      assertEquals("expected only one remaining block but got " + blocks.get(), 1, blocks.get());
    }
  }

  @Test
  public void testReadWorksWhenCacheCorrupt() throws Exception {
    BlockCache mockedCache = mock(BlockCache.class);
    when(mockedCache.getBlock(any(), anyBoolean(), anyBoolean(), anyBoolean(), any()))
      .thenThrow(new RuntimeException("Injected error"));
    Path p = makeNewFile();
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Configuration conf = TEST_UTIL.getConfiguration();
    HFile.Reader reader = HFile.createReader(fs, p, new CacheConfig(conf, mockedCache), true, conf);
    long offset = 0;
    while (offset < reader.getTrailer().getLoadOnOpenDataOffset()) {
      HFileBlock block = reader.readBlock(offset, -1, false, true, false, true, null, null, false);
      assertNotNull(block);
      offset += block.getOnDiskSizeWithHeader();
    }
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
    HFileScanner scanner = reader.getScanner(conf, true, true);
    scanner.seekTo(toKV("i"));
    assertEquals("i", toRowStr(scanner.getCell()));
    scanner.close();

    while (bucketcache.getBlockCount() <= 0) {
      Thread.sleep(10);
    }

    // reopen again.
    scanner = reader.getScanner(conf, true, true);
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
    scanner = reader.getScanner(conf, true, true);
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
    if (fs.exists(dataTestDir)) {
      fs.delete(dataTestDir, true);
    }
  }

}
