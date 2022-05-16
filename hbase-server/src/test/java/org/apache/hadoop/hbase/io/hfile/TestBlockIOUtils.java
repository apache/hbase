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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.util.BlockIOUtils;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.MultiByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;

@Category({ IOTests.class, SmallTests.class })
public class TestBlockIOUtils {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBlockIOUtils.class);

  @Rule
  public TestName testName = new TestName();

  @Rule
  public ExpectedException exception = ExpectedException.none();

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final int NUM_TEST_BLOCKS = 2;
  private static final Compression.Algorithm COMPRESSION_ALGO = Compression.Algorithm.GZ;

  @Test
  public void testIsByteBufferReadable() throws IOException {
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path p = new Path(TEST_UTIL.getDataTestDirOnTestFS(), "testIsByteBufferReadable");
    try (FSDataOutputStream out = fs.create(p)) {
      out.writeInt(23);
    }
    try (FSDataInputStream is = fs.open(p)) {
      assertFalse(BlockIOUtils.isByteBufferReadable(is));
    }
  }

  @Test
  public void testReadFully() throws IOException {
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path p = new Path(TEST_UTIL.getDataTestDirOnTestFS(), "testReadFully");
    String s = "hello world";
    try (FSDataOutputStream out = fs.create(p)) {
      out.writeBytes(s);
    }
    ByteBuff buf = new SingleByteBuff(ByteBuffer.allocate(11));
    try (FSDataInputStream in = fs.open(p)) {
      BlockIOUtils.readFully(buf, in, 11);
    }
    buf.rewind();
    byte[] heapBuf = new byte[s.length()];
    buf.get(heapBuf, 0, heapBuf.length);
    assertArrayEquals(Bytes.toBytes(s), heapBuf);
  }

  @Test
  public void testPreadWithReadFullBytes() throws IOException {
    testPreadReadFullBytesInternal(true, EnvironmentEdgeManager.currentTime());
  }

  @Test
  public void testPreadWithoutReadFullBytes() throws IOException {
    testPreadReadFullBytesInternal(false, EnvironmentEdgeManager.currentTime());
  }

  private void testPreadReadFullBytesInternal(boolean readAllBytes, long randomSeed)
    throws IOException {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean(HConstants.HFILE_PREAD_ALL_BYTES_ENABLED_KEY, readAllBytes);
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path path = new Path(TEST_UTIL.getDataTestDirOnTestFS(), testName.getMethodName());
    // give a fixed seed such we can see failure easily.
    Random rand = new Random(randomSeed);
    long totalDataBlockBytes =
      writeBlocks(TEST_UTIL.getConfiguration(), rand, COMPRESSION_ALGO, path);
    readDataBlocksAndVerify(fs, path, COMPRESSION_ALGO, totalDataBlockBytes);
  }

  private long writeBlocks(Configuration conf, Random rand, Compression.Algorithm compressAlgo,
    Path path) throws IOException {
    FileSystem fs = HFileSystem.get(conf);
    FSDataOutputStream os = fs.create(path);
    HFileContext meta =
      new HFileContextBuilder().withHBaseCheckSum(true).withCompression(compressAlgo).build();
    HFileBlock.Writer hbw = new HFileBlock.Writer(null, meta);
    long totalDataBlockBytes = 0;
    for (int i = 0; i < NUM_TEST_BLOCKS; ++i) {
      int blockTypeOrdinal = rand.nextInt(BlockType.values().length);
      if (blockTypeOrdinal == BlockType.ENCODED_DATA.ordinal()) {
        blockTypeOrdinal = BlockType.DATA.ordinal();
      }
      BlockType bt = BlockType.values()[blockTypeOrdinal];
      DataOutputStream dos = hbw.startWriting(bt);
      int size = rand.nextInt(500);
      for (int j = 0; j < size; ++j) {
        dos.writeShort(i + 1);
        dos.writeInt(j + 1);
      }

      hbw.writeHeaderAndData(os);
      totalDataBlockBytes += hbw.getOnDiskSizeWithHeader();
    }
    // append a dummy trailer and in a actual HFile it should have more data.
    FixedFileTrailer trailer = new FixedFileTrailer(3, 3);
    trailer.setFirstDataBlockOffset(0);
    trailer.setLastDataBlockOffset(totalDataBlockBytes);
    trailer.setComparatorClass(meta.getCellComparator().getClass());
    trailer.setDataIndexCount(NUM_TEST_BLOCKS);
    trailer.setCompressionCodec(compressAlgo);
    trailer.serialize(os);
    // close the stream
    os.close();
    return totalDataBlockBytes;
  }

  private void readDataBlocksAndVerify(FileSystem fs, Path path, Compression.Algorithm compressAlgo,
    long totalDataBlockBytes) throws IOException {
    FSDataInputStream is = fs.open(path);
    HFileContext fileContext =
      new HFileContextBuilder().withHBaseCheckSum(true).withCompression(compressAlgo).build();
    ReaderContext context =
      new ReaderContextBuilder().withInputStreamWrapper(new FSDataInputStreamWrapper(is))
        .withReaderType(ReaderContext.ReaderType.PREAD).withFileSize(totalDataBlockBytes)
        .withFilePath(path).withFileSystem(fs).build();
    HFileBlock.FSReader hbr =
      new HFileBlock.FSReaderImpl(context, fileContext, ByteBuffAllocator.HEAP);

    long onDiskSizeOfNextBlock = -1;
    long offset = 0;
    int numOfReadBlock = 0;
    // offset and totalBytes shares the same logic in the HFilePreadReader
    while (offset < totalDataBlockBytes) {
      HFileBlock block = hbr.readBlockData(offset, onDiskSizeOfNextBlock, true, false, false);
      numOfReadBlock++;
      try {
        onDiskSizeOfNextBlock = block.getNextBlockOnDiskSize();
        offset += block.getOnDiskSizeWithHeader();
      } finally {
        block.release();
      }
    }
    assertEquals(totalDataBlockBytes, offset);
    assertEquals(NUM_TEST_BLOCKS, numOfReadBlock);
    deleteFile(fs, path);
  }

  private void deleteFile(FileSystem fs, Path path) throws IOException {
    if (fs.exists(path)) {
      fs.delete(path, true);
    }
  }

  @Test
  public void testReadWithExtra() throws IOException {
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path p = new Path(TEST_UTIL.getDataTestDirOnTestFS(), "testReadWithExtra");
    String s = "hello world";
    try (FSDataOutputStream out = fs.create(p)) {
      out.writeBytes(s);
    }
    ByteBuff buf = new SingleByteBuff(ByteBuffer.allocate(8));
    try (FSDataInputStream in = fs.open(p)) {
      assertTrue(BlockIOUtils.readWithExtra(buf, in, 6, 2));
    }
    buf.rewind();
    byte[] heapBuf = new byte[buf.capacity()];
    buf.get(heapBuf, 0, heapBuf.length);
    assertArrayEquals(Bytes.toBytes("hello wo"), heapBuf);

    buf = new MultiByteBuff(ByteBuffer.allocate(4), ByteBuffer.allocate(4), ByteBuffer.allocate(4));
    try (FSDataInputStream in = fs.open(p)) {
      assertTrue(BlockIOUtils.readWithExtra(buf, in, 8, 3));
    }
    buf.rewind();
    heapBuf = new byte[11];
    buf.get(heapBuf, 0, heapBuf.length);
    assertArrayEquals(Bytes.toBytes("hello world"), heapBuf);

    buf.position(0).limit(12);
    try (FSDataInputStream in = fs.open(p)) {
      try {
        BlockIOUtils.readWithExtra(buf, in, 12, 0);
        fail("Should only read 11 bytes");
      } catch (IOException e) {

      }
    }
  }

  @Test
  public void testPositionalReadNoExtra() throws IOException {
    long position = 0;
    int bufOffset = 0;
    int necessaryLen = 10;
    int extraLen = 0;
    int totalLen = necessaryLen + extraLen;
    byte[] buf = new byte[totalLen];
    ByteBuff bb = new SingleByteBuff(ByteBuffer.wrap(buf, 0, totalLen));
    FSDataInputStream in = mock(FSDataInputStream.class);
    when(in.read(position, buf, bufOffset, totalLen)).thenReturn(totalLen);
    boolean ret = BlockIOUtils.preadWithExtra(bb, in, position, necessaryLen, extraLen);
    assertFalse("Expect false return when no extra bytes requested", ret);
    verify(in).read(position, buf, bufOffset, totalLen);
    verifyNoMoreInteractions(in);
  }

  @Test
  public void testPositionalReadShortReadOfNecessaryBytes() throws IOException {
    long position = 0;
    int bufOffset = 0;
    int necessaryLen = 10;
    int extraLen = 0;
    int totalLen = necessaryLen + extraLen;
    byte[] buf = new byte[totalLen];
    ByteBuff bb = new SingleByteBuff(ByteBuffer.wrap(buf, 0, totalLen));
    FSDataInputStream in = mock(FSDataInputStream.class);
    when(in.read(position, buf, bufOffset, totalLen)).thenReturn(5);
    when(in.read(5, buf, 5, 5)).thenReturn(5);
    boolean ret = BlockIOUtils.preadWithExtra(bb, in, position, necessaryLen, extraLen);
    assertFalse("Expect false return when no extra bytes requested", ret);
    verify(in).read(position, buf, bufOffset, totalLen);
    verify(in).read(5, buf, 5, 5);
    verifyNoMoreInteractions(in);
  }

  @Test
  public void testPositionalReadExtraSucceeded() throws IOException {
    long position = 0;
    int bufOffset = 0;
    int necessaryLen = 10;
    int extraLen = 5;
    int totalLen = necessaryLen + extraLen;
    byte[] buf = new byte[totalLen];
    ByteBuff bb = new SingleByteBuff(ByteBuffer.wrap(buf, 0, totalLen));
    FSDataInputStream in = mock(FSDataInputStream.class);
    when(in.read(position, buf, bufOffset, totalLen)).thenReturn(totalLen);
    boolean ret = BlockIOUtils.preadWithExtra(bb, in, position, necessaryLen, extraLen);
    assertTrue("Expect true return when reading extra bytes succeeds", ret);
    verify(in).read(position, buf, bufOffset, totalLen);
    verifyNoMoreInteractions(in);
  }

  @Test
  public void testPositionalReadExtraFailed() throws IOException {
    long position = 0;
    int bufOffset = 0;
    int necessaryLen = 10;
    int extraLen = 5;
    int totalLen = necessaryLen + extraLen;
    byte[] buf = new byte[totalLen];
    ByteBuff bb = new SingleByteBuff(ByteBuffer.wrap(buf, 0, totalLen));
    FSDataInputStream in = mock(FSDataInputStream.class);
    when(in.read(position, buf, bufOffset, totalLen)).thenReturn(necessaryLen);
    boolean ret = BlockIOUtils.preadWithExtra(bb, in, position, necessaryLen, extraLen);
    assertFalse("Expect false return when reading extra bytes fails", ret);
    verify(in).read(position, buf, bufOffset, totalLen);
    verifyNoMoreInteractions(in);
  }

  @Test
  public void testPositionalReadShortReadCompletesNecessaryAndExtraBytes() throws IOException {
    long position = 0;
    int bufOffset = 0;
    int necessaryLen = 10;
    int extraLen = 5;
    int totalLen = necessaryLen + extraLen;
    byte[] buf = new byte[totalLen];
    ByteBuff bb = new SingleByteBuff(ByteBuffer.wrap(buf, 0, totalLen));
    FSDataInputStream in = mock(FSDataInputStream.class);
    when(in.read(position, buf, bufOffset, totalLen)).thenReturn(5);
    when(in.read(5, buf, 5, 10)).thenReturn(10);
    boolean ret = BlockIOUtils.preadWithExtra(bb, in, position, necessaryLen, extraLen);
    assertTrue("Expect true return when reading extra bytes succeeds", ret);
    verify(in).read(position, buf, bufOffset, totalLen);
    verify(in).read(5, buf, 5, 10);
    verifyNoMoreInteractions(in);
  }

  @Test
  public void testPositionalReadPrematureEOF() throws IOException {
    long position = 0;
    int bufOffset = 0;
    int necessaryLen = 10;
    int extraLen = 0;
    int totalLen = necessaryLen + extraLen;
    byte[] buf = new byte[totalLen];
    ByteBuff bb = new SingleByteBuff(ByteBuffer.wrap(buf, 0, totalLen));
    FSDataInputStream in = mock(FSDataInputStream.class);
    when(in.read(position, buf, bufOffset, totalLen)).thenReturn(9);
    when(in.read(position, buf, bufOffset, totalLen)).thenReturn(-1);
    exception.expect(IOException.class);
    exception.expectMessage("EOF");
    BlockIOUtils.preadWithExtra(bb, in, position, necessaryLen, extraLen);
  }
}
