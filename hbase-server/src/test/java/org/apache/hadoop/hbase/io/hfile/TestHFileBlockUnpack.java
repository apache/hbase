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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ IOTests.class, MediumTests.class })
public class TestHFileBlockUnpack {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHFileBlockUnpack.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  // repetition gives us some chance to get a good compression ratio
  private static float CHANCE_TO_REPEAT = 0.6f;

  private static final int MIN_ALLOCATION_SIZE = 10 * 1024;

  ByteBuffAllocator allocator;

  @Rule
  public TestName name = new TestName();
  private FileSystem fs;

  @Before
  public void setUp() throws Exception {
    fs = HFileSystem.get(TEST_UTIL.getConfiguration());
    Configuration conf = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
    conf.setInt(ByteBuffAllocator.MIN_ALLOCATE_SIZE_KEY, MIN_ALLOCATION_SIZE);
    allocator = ByteBuffAllocator.create(conf, true);

  }

  /**
   * If the block on disk size is less than {@link ByteBuffAllocator}'s min allocation size, that
   * block will be allocated to heap regardless of desire for off-heap. After de-compressing the
   * block, the new size may now exceed the min allocation size. This test ensures that those
   * de-compressed blocks, which will be allocated off-heap, are properly marked as
   * {@link HFileBlock#isSharedMem()} == true See https://issues.apache.org/jira/browse/HBASE-27170
   */
  @Test
  public void itUsesSharedMemoryIfUnpackedBlockExceedsMinAllocationSize() throws IOException {
    Configuration conf = TEST_UTIL.getConfiguration();
    HFileContext meta =
      new HFileContextBuilder().withCompression(Compression.Algorithm.GZ).withIncludesMvcc(false)
        .withIncludesTags(false).withBytesPerCheckSum(HFile.DEFAULT_BYTES_PER_CHECKSUM).build();

    Path path = new Path(TEST_UTIL.getDataTestDir(), name.getMethodName());
    int totalSize;
    try (FSDataOutputStream os = fs.create(path)) {
      HFileBlock.Writer hbw = new HFileBlock.Writer(conf, NoOpDataBlockEncoder.INSTANCE, meta);
      hbw.startWriting(BlockType.DATA);
      writeTestKeyValues(hbw, MIN_ALLOCATION_SIZE - 1);
      hbw.writeHeaderAndData(os);
      totalSize = hbw.getOnDiskSizeWithHeader();
      assertTrue(
        "expected generated block size " + totalSize + " to be less than " + MIN_ALLOCATION_SIZE,
        totalSize < MIN_ALLOCATION_SIZE);
    }

    try (FSDataInputStream is = fs.open(path)) {
      meta =
        new HFileContextBuilder().withHBaseCheckSum(true).withCompression(Compression.Algorithm.GZ)
          .withIncludesMvcc(false).withIncludesTags(false).build();
      ReaderContext context =
        new ReaderContextBuilder().withInputStreamWrapper(new FSDataInputStreamWrapper(is))
          .withFileSize(totalSize).withFilePath(path).withFileSystem(fs).build();
      HFileBlock.FSReaderImpl hbr = new HFileBlock.FSReaderImpl(context, meta, allocator, conf);
      hbr.setDataBlockEncoder(NoOpDataBlockEncoder.INSTANCE, conf);
      hbr.setIncludesMemStoreTS(false);
      HFileBlock blockFromHFile = hbr.readBlockData(0, -1, false, false, false);
      blockFromHFile.sanityCheck();
      assertFalse("expected hfile block to NOT be unpacked", blockFromHFile.isUnpacked());
      assertFalse("expected hfile block to NOT use shared memory", blockFromHFile.isSharedMem());

      assertTrue(
        "expected generated block size " + blockFromHFile.getOnDiskSizeWithHeader()
          + " to be less than " + MIN_ALLOCATION_SIZE,
        blockFromHFile.getOnDiskSizeWithHeader() < MIN_ALLOCATION_SIZE);
      assertTrue(
        "expected generated block uncompressed size "
          + blockFromHFile.getUncompressedSizeWithoutHeader() + " to be more than "
          + MIN_ALLOCATION_SIZE,
        blockFromHFile.getUncompressedSizeWithoutHeader() > MIN_ALLOCATION_SIZE);

      HFileBlock blockUnpacked = blockFromHFile.unpack(meta, hbr);
      assertTrue("expected unpacked block to be unpacked", blockUnpacked.isUnpacked());
      assertTrue("expected unpacked block to use shared memory", blockUnpacked.isSharedMem());
    }
  }

  static int writeTestKeyValues(HFileBlock.Writer hbw, int desiredSize) throws IOException {
    Random random = new Random(42);

    byte[] family = new byte[] { 1 };
    int rowKey = 0;
    int qualifier = 0;
    int value = 0;
    long timestamp = 0;

    int totalSize = 0;

    // go until just up to the limit. compression should bring the total on-disk size under
    while (totalSize < desiredSize) {
      rowKey = maybeIncrement(random, rowKey);
      qualifier = maybeIncrement(random, qualifier);
      value = maybeIncrement(random, value);
      timestamp = maybeIncrement(random, (int) timestamp);

      KeyValue keyValue = new KeyValue(Bytes.toBytes(rowKey), family, Bytes.toBytes(qualifier),
        timestamp, Bytes.toBytes(value));
      hbw.write(keyValue);
      totalSize += keyValue.getLength();
    }

    return totalSize;
  }

  private static int maybeIncrement(Random random, int value) {
    if (random.nextFloat() < CHANCE_TO_REPEAT) {
      return value;
    }
    return value + 1;
  }

}
