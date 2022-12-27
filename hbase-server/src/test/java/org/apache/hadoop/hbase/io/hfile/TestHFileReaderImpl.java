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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache;
import org.apache.hadoop.hbase.regionserver.Shipper;
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

  Path makeNewFile(int blocksize) throws IOException {
    Path ncTFile = new Path(TEST_UTIL.getDataTestDir(), "basic.hfile");
    FSDataOutputStream fout = TEST_UTIL.getTestFileSystem().create(ncTFile);
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

  @Test
  public void testCheckpoint() throws IOException {
    // use very small blocksize to force every cell to be a different block. this gives us
    // more room to work below in testing checkpointing between blocks.
    Path p = makeNewFile(1);
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt(ByteBuffAllocator.MIN_ALLOCATE_SIZE_KEY, 0);

    ByteBuffAllocator allocator = ByteBuffAllocator.create(conf, true);
    CacheConfig cacheConfig = new CacheConfig(conf, null, null, allocator);
    HFile.Reader reader = HFile.createReader(fs, p, cacheConfig, true, conf);

    HFileReaderImpl.HFileScannerImpl scanner =
      (HFileReaderImpl.HFileScannerImpl) reader.getScanner(conf, true, true);

    // we do an initial checkpoint. but we'll override it below, which is to prove that
    // checkpoints can supersede each other (by updating index). if that didn't work, we'd see
    // the first prevBlock entry get released early which would fail the assertions below.
    scanner.checkpoint(Shipper.State.START);

    scanner.seekTo();

    boolean started = false;
    boolean finished = false;

    // our goal is to prove that we can clear out a slice of prevBlocks
    // skip the first prevBlock entry by calling checkpoint START at that point
    // once we get another prevBlocks entry we finish up with FILTERED
    while (scanner.next()) {
      if (scanner.prevBlocks.size() > 0) {
        if (started) {
          finished = true;
          scanner.checkpoint(Shipper.State.FILTERED);
          break;
        } else {
          started = true;
          scanner.checkpoint(Shipper.State.START);
        }
      }
    }

    assertTrue(started);
    assertTrue(finished);
    assertNotEquals(0, allocator.getUsedBufferCount());

    // checkpointing doesn't clear out prevBlocks, just releases and sets them all to null
    // make sure there are still entries
    assertNotEquals(0, scanner.prevBlocks.size());

    // we expect to find 1 block at the head of the list which is non-null. this is the one we
    // skipped above. after that any others should be null
    boolean foundNonNull = false;
    for (HFileBlock block : scanner.prevBlocks) {
      if (!foundNonNull) {
        assertNotNull(block);
        foundNonNull = true;
      } else {
        assertNull(block);
      }
    }

    // we loaded at least 3 blocks -- 1 was skipped (still in prevBlocks) and 1 is held in curBlock.
    // so skip two buffers in our check here
    assertEquals(allocator.getUsedBufferCount() - 2, allocator.getFreeBufferCount());

    scanner.shipped();

    // shipped cleans up prevBlocks and releases anything left over
    assertTrue(scanner.prevBlocks.isEmpty());
    // now just curBlock holds a buffer
    assertEquals(allocator.getUsedBufferCount() - 1, allocator.getFreeBufferCount());

    // close and validate that all buffers are returned
    scanner.close();
    assertEquals(allocator.getUsedBufferCount(), allocator.getFreeBufferCount());
  }

  @Test
  public void testSeekBefore() throws Exception {
    Path p = makeNewFile(toKV("a").getLength() * 3);
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
