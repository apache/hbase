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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.nio.ByteBuff;
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
   * It's important that if you read and unpack the same HFileBlock twice, it results in an
   * identical buffer each time. Otherwise we end up with validation failures in block cache, since
   * contents may not match if the same block is cached twice. See
   * https://issues.apache.org/jira/browse/HBASE-27053
   */
  @Test
  public void itUnpacksIdenticallyEachTime() throws IOException {
    Path path = new Path(TEST_UTIL.getDataTestDir(), name.getMethodName());
    int totalSize = createTestBlock(path);

    // Allocate a bunch of random buffers, so we can be sure that unpack will only have "dirty"
    // buffers to choose from when allocating itself.
    Random random = new Random();
    byte[] temp = new byte[HConstants.DEFAULT_BLOCKSIZE];
    List<ByteBuff> buffs = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      ByteBuff buff = allocator.allocate(HConstants.DEFAULT_BLOCKSIZE);
      random.nextBytes(temp);
      buff.put(temp);
      buffs.add(buff);
    }

    buffs.forEach(ByteBuff::release);

    // read the same block twice. we should expect the underlying buffer below to
    // be identical each time
    HFileBlockWrapper blockOne = readBlock(path, totalSize);
    HFileBlockWrapper blockTwo = readBlock(path, totalSize);

    // first check size fields
    assertEquals(blockOne.original.getOnDiskSizeWithHeader(),
      blockTwo.original.getOnDiskSizeWithHeader());
    assertEquals(blockOne.original.getUncompressedSizeWithoutHeader(),
      blockTwo.original.getUncompressedSizeWithoutHeader());

    // next check packed buffers
    assertBuffersEqual(blockOne.original.getBufferWithoutHeader(),
      blockTwo.original.getBufferWithoutHeader(),
      blockOne.original.getOnDiskDataSizeWithHeader() - blockOne.original.headerSize());

    // now check unpacked buffers. prior to HBASE-27053, this would fail because
    // the unpacked buffer would include extra space for checksums at the end that was not written.
    // so the checksum space would be filled with random junk when re-using pooled buffers.
    assertBuffersEqual(blockOne.unpacked.getBufferWithoutHeader(),
      blockTwo.unpacked.getBufferWithoutHeader(),
      blockOne.original.getUncompressedSizeWithoutHeader());
  }

  private void assertBuffersEqual(ByteBuff bufferOne, ByteBuff bufferTwo, int expectedSize) {
    assertEquals(expectedSize, bufferOne.limit());
    assertEquals(expectedSize, bufferTwo.limit());
    assertEquals(0,
      ByteBuff.compareTo(bufferOne, 0, bufferOne.limit(), bufferTwo, 0, bufferTwo.limit()));
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
    Path path = new Path(TEST_UTIL.getDataTestDir(), name.getMethodName());
    int totalSize = createTestBlock(path);
    HFileBlockWrapper blockFromHFile = readBlock(path, totalSize);

    assertFalse("expected hfile block to NOT be unpacked", blockFromHFile.original.isUnpacked());
    assertFalse("expected hfile block to NOT use shared memory",
      blockFromHFile.original.isSharedMem());

    assertTrue(
      "expected generated block size " + blockFromHFile.original.getOnDiskSizeWithHeader()
        + " to be less than " + MIN_ALLOCATION_SIZE,
      blockFromHFile.original.getOnDiskSizeWithHeader() < MIN_ALLOCATION_SIZE);
    assertTrue(
      "expected generated block uncompressed size "
        + blockFromHFile.original.getUncompressedSizeWithoutHeader() + " to be more than "
        + MIN_ALLOCATION_SIZE,
      blockFromHFile.original.getUncompressedSizeWithoutHeader() > MIN_ALLOCATION_SIZE);

    assertTrue("expected unpacked block to be unpacked", blockFromHFile.unpacked.isUnpacked());
    assertTrue("expected unpacked block to use shared memory",
      blockFromHFile.unpacked.isSharedMem());
  }

  private final static class HFileBlockWrapper {
    private final HFileBlock original;
    private final HFileBlock unpacked;

    private HFileBlockWrapper(HFileBlock original, HFileBlock unpacked) {
      this.original = original;
      this.unpacked = unpacked;
    }
  }

  private HFileBlockWrapper readBlock(Path path, int totalSize) throws IOException {
    try (FSDataInputStream is = fs.open(path)) {
      HFileContext meta =
        new HFileContextBuilder().withHBaseCheckSum(true).withCompression(Compression.Algorithm.GZ)
          .withIncludesMvcc(false).withIncludesTags(false).build();
      ReaderContext context =
        new ReaderContextBuilder().withInputStreamWrapper(new FSDataInputStreamWrapper(is))
          .withFileSize(totalSize).withFilePath(path).withFileSystem(fs).build();
      HFileBlock.FSReaderImpl hbr =
        new HFileBlock.FSReaderImpl(context, meta, allocator, TEST_UTIL.getConfiguration());
      hbr.setDataBlockEncoder(NoOpDataBlockEncoder.INSTANCE, TEST_UTIL.getConfiguration());
      hbr.setIncludesMemStoreTS(false);
      HFileBlock blockFromHFile = hbr.readBlockData(0, -1, false, false, false);
      blockFromHFile.sanityCheck();
      return new HFileBlockWrapper(blockFromHFile, blockFromHFile.unpack(meta, hbr));
    }
  }

  private int createTestBlock(Path path) throws IOException {
    HFileContext meta =
      new HFileContextBuilder().withCompression(Compression.Algorithm.GZ).withIncludesMvcc(false)
        .withIncludesTags(false).withBytesPerCheckSum(HFile.DEFAULT_BYTES_PER_CHECKSUM).build();

    int totalSize;
    try (FSDataOutputStream os = fs.create(path)) {
      HFileBlock.Writer hbw =
        new HFileBlock.Writer(TEST_UTIL.getConfiguration(), NoOpDataBlockEncoder.INSTANCE, meta);
      hbw.startWriting(BlockType.DATA);
      writeTestKeyValues(hbw, MIN_ALLOCATION_SIZE - 1);
      hbw.writeHeaderAndData(os);
      totalSize = hbw.getOnDiskSizeWithHeader();
      assertTrue(
        "expected generated block size " + totalSize + " to be less than " + MIN_ALLOCATION_SIZE,
        totalSize < MIN_ALLOCATION_SIZE);
    }
    return totalSize;
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
