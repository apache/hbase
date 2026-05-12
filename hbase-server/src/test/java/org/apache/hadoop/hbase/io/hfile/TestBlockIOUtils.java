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

import static org.apache.hadoop.hbase.client.trace.hamcrest.AttributesMatchers.containsEntry;
import static org.apache.hadoop.hbase.client.trace.hamcrest.EventMatchers.hasAttributes;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasEnded;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasEvents;
import static org.apache.hadoop.hbase.client.trace.hamcrest.SpanDataMatchers.hasName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MatcherPredicate;
import org.apache.hadoop.hbase.client.trace.hamcrest.AttributesMatchers;
import org.apache.hadoop.hbase.client.trace.hamcrest.EventMatchers;
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
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;

@Tag(IOTests.TAG)
@Tag(SmallTests.TAG)
public class TestBlockIOUtils {

  private String methodName;

  @RegisterExtension
  private static OpenTelemetryExtension OTEL_EXT = OpenTelemetryExtension.create();

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final int NUM_TEST_BLOCKS = 2;
  private static final Compression.Algorithm COMPRESSION_ALGO = Compression.Algorithm.GZ;

  @BeforeEach
  public void setUp(TestInfo testInfo) {
    methodName = testInfo.getTestMethod().get().getName();
  }

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
    TraceUtil.trace(() -> {
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
    }, methodName);

    TEST_UTIL.waitFor(TimeUnit.MINUTES.toMillis(1), new MatcherPredicate<Iterable<SpanData>>(
      OTEL_EXT::getSpans, hasItem(allOf(hasName(methodName), hasEnded()))));
    assertThat(OTEL_EXT.getSpans(),
      hasItems(allOf(hasName(methodName),
        hasEvents(hasItem(allOf(EventMatchers.hasName("BlockIOUtils.readFully"),
          hasAttributes(containsEntry("db.hbase.io.heap_bytes_read", 11))))))));
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
    Path path = new Path(TEST_UTIL.getDataTestDirOnTestFS(), methodName);
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
    HFileBlock.Writer hbw = new HFileBlock.Writer(conf, null, meta);
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
      new HFileBlock.FSReaderImpl(context, fileContext, ByteBuffAllocator.HEAP, fs.getConf());

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

    Span span = TraceUtil.createSpan(methodName);
    try (Scope ignored = span.makeCurrent()) {
      ByteBuff buf = new SingleByteBuff(ByteBuffer.allocate(8));
      try (FSDataInputStream in = fs.open(p)) {
        assertTrue(BlockIOUtils.readWithExtra(buf, in, 6, 2));
      }
      buf.rewind();
      byte[] heapBuf = new byte[buf.capacity()];
      buf.get(heapBuf, 0, heapBuf.length);
      assertArrayEquals(Bytes.toBytes("hello wo"), heapBuf);
    } finally {
      span.end();
    }
    TEST_UTIL.waitFor(TimeUnit.MINUTES.toMillis(1), new MatcherPredicate<Iterable<SpanData>>(
      OTEL_EXT::getSpans, hasItem(allOf(hasName(methodName), hasEnded()))));
    assertThat(OTEL_EXT.getSpans(),
      hasItems(allOf(hasName(methodName),
        hasEvents(hasItem(allOf(EventMatchers.hasName("BlockIOUtils.readWithExtra"),
          hasAttributes(containsEntry("db.hbase.io.heap_bytes_read", 8L))))))));

    OTEL_EXT.clearSpans();
    span = TraceUtil.createSpan(methodName);
    try (Scope ignored = span.makeCurrent()) {
      ByteBuff buf =
        new MultiByteBuff(ByteBuffer.allocate(4), ByteBuffer.allocate(4), ByteBuffer.allocate(4));
      try (FSDataInputStream in = fs.open(p)) {
        assertTrue(BlockIOUtils.readWithExtra(buf, in, 8, 3));
      }
      buf.rewind();
      byte[] heapBuf = new byte[11];
      buf.get(heapBuf, 0, heapBuf.length);
      assertArrayEquals(Bytes.toBytes("hello world"), heapBuf);
    } finally {
      span.end();
    }
    TEST_UTIL.waitFor(TimeUnit.MINUTES.toMillis(1), new MatcherPredicate<Iterable<SpanData>>(
      OTEL_EXT::getSpans, hasItem(allOf(hasName(methodName), hasEnded()))));
    assertThat(OTEL_EXT.getSpans(),
      hasItems(allOf(hasName(methodName),
        hasEvents(hasItem(allOf(EventMatchers.hasName("BlockIOUtils.readWithExtra"),
          hasAttributes(containsEntry("db.hbase.io.heap_bytes_read", 11L))))))));

    OTEL_EXT.clearSpans();
    span = TraceUtil.createSpan(methodName);
    try (Scope ignored = span.makeCurrent()) {
      ByteBuff buf =
        new MultiByteBuff(ByteBuffer.allocate(4), ByteBuffer.allocate(4), ByteBuffer.allocate(4));
      buf.position(0).limit(12);
      try (FSDataInputStream in = fs.open(p)) {
        assertThrows(IOException.class, () -> BlockIOUtils.readWithExtra(buf, in, 12, 0),
          "Should only read 11 bytes");
      }
    } finally {
      span.end();
    }
    TEST_UTIL.waitFor(TimeUnit.MINUTES.toMillis(1), new MatcherPredicate<Iterable<SpanData>>(
      OTEL_EXT::getSpans, hasItem(allOf(hasName(methodName), hasEnded()))));
    assertThat(OTEL_EXT.getSpans(),
      hasItems(allOf(hasName(methodName),
        hasEvents(hasItem(allOf(EventMatchers.hasName("BlockIOUtils.readWithExtra"),
          hasAttributes(containsEntry("db.hbase.io.heap_bytes_read", 11L))))))));
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
    when(in.hasCapability(anyString())).thenReturn(false);
    boolean ret = TraceUtil.trace(
      () -> BlockIOUtils.preadWithExtra(bb, in, position, necessaryLen, extraLen), methodName);
    assertFalse(ret, "Expect false return when no extra bytes requested");
    verify(in).read(position, buf, bufOffset, totalLen);
    verify(in).hasCapability(anyString());
    verifyNoMoreInteractions(in);

    TEST_UTIL.waitFor(TimeUnit.MINUTES.toMillis(1), new MatcherPredicate<Iterable<SpanData>>(
      OTEL_EXT::getSpans, hasItem(allOf(hasName(methodName), hasEnded()))));
    assertThat(OTEL_EXT.getSpans(),
      hasItems(allOf(hasName(methodName),
        hasEvents(hasItem(allOf(EventMatchers.hasName("BlockIOUtils.preadWithExtra"),
          hasAttributes(containsEntry("db.hbase.io.heap_bytes_read", totalLen))))))));
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
    when(in.hasCapability(anyString())).thenReturn(false);
    boolean ret = TraceUtil.trace(
      () -> BlockIOUtils.preadWithExtra(bb, in, position, necessaryLen, extraLen), methodName);
    assertFalse(ret, "Expect false return when no extra bytes requested");
    verify(in).read(position, buf, bufOffset, totalLen);
    verify(in).read(5, buf, 5, 5);
    verify(in).hasCapability(anyString());
    verifyNoMoreInteractions(in);

    TEST_UTIL.waitFor(TimeUnit.MINUTES.toMillis(1), new MatcherPredicate<Iterable<SpanData>>(
      OTEL_EXT::getSpans, hasItem(allOf(hasName(methodName), hasEnded()))));
    assertThat(OTEL_EXT.getSpans(),
      hasItems(allOf(hasName(methodName),
        hasEvents(hasItem(allOf(EventMatchers.hasName("BlockIOUtils.preadWithExtra"),
          hasAttributes(containsEntry("db.hbase.io.heap_bytes_read", totalLen))))))));
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
    when(in.hasCapability(anyString())).thenReturn(false);
    boolean ret = TraceUtil.trace(
      () -> BlockIOUtils.preadWithExtra(bb, in, position, necessaryLen, extraLen), methodName);
    assertTrue(ret, "Expect true return when reading extra bytes succeeds");
    verify(in).read(position, buf, bufOffset, totalLen);
    verify(in).hasCapability(anyString());
    verifyNoMoreInteractions(in);

    TEST_UTIL.waitFor(TimeUnit.MINUTES.toMillis(1), new MatcherPredicate<Iterable<SpanData>>(
      OTEL_EXT::getSpans, hasItem(allOf(hasName(methodName), hasEnded()))));
    assertThat(OTEL_EXT.getSpans(),
      hasItems(allOf(hasName(methodName),
        hasEvents(hasItem(allOf(EventMatchers.hasName("BlockIOUtils.preadWithExtra"),
          hasAttributes(containsEntry("db.hbase.io.heap_bytes_read", totalLen))))))));
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
    when(in.hasCapability(anyString())).thenReturn(false);
    boolean ret = TraceUtil.trace(
      () -> BlockIOUtils.preadWithExtra(bb, in, position, necessaryLen, extraLen), methodName);
    assertFalse(ret, "Expect false return when reading extra bytes fails");
    verify(in).read(position, buf, bufOffset, totalLen);
    verify(in).hasCapability(anyString());
    verifyNoMoreInteractions(in);

    TEST_UTIL.waitFor(TimeUnit.MINUTES.toMillis(1), new MatcherPredicate<Iterable<SpanData>>(
      OTEL_EXT::getSpans, hasItem(allOf(hasName(methodName), hasEnded()))));
    assertThat(OTEL_EXT.getSpans(),
      hasItems(allOf(hasName(methodName),
        hasEvents(hasItem(allOf(EventMatchers.hasName("BlockIOUtils.preadWithExtra"),
          hasAttributes(containsEntry("db.hbase.io.heap_bytes_read", necessaryLen))))))));
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
    when(in.hasCapability(anyString())).thenReturn(false);
    boolean ret = TraceUtil.trace(
      () -> BlockIOUtils.preadWithExtra(bb, in, position, necessaryLen, extraLen), methodName);
    assertTrue(ret, "Expect true return when reading extra bytes succeeds");
    verify(in).read(position, buf, bufOffset, totalLen);
    verify(in).read(5, buf, 5, 10);
    verify(in).hasCapability(anyString());
    verifyNoMoreInteractions(in);

    TEST_UTIL.waitFor(TimeUnit.MINUTES.toMillis(1), new MatcherPredicate<Iterable<SpanData>>(
      OTEL_EXT::getSpans, hasItem(allOf(hasName(methodName), hasEnded()))));
    assertThat(OTEL_EXT.getSpans(),
      hasItems(allOf(hasName(methodName),
        hasEvents(hasItem(allOf(EventMatchers.hasName("BlockIOUtils.preadWithExtra"),
          hasAttributes(containsEntry("db.hbase.io.heap_bytes_read", totalLen))))))));
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
    when(in.read(position, buf, bufOffset, totalLen)).thenReturn(-1);
    when(in.hasCapability(anyString())).thenReturn(false);
    Span span = TraceUtil.createSpan(methodName);
    try (Scope ignored = span.makeCurrent()) {
      IOException e = assertThrows(IOException.class,
        () -> BlockIOUtils.preadWithExtra(bb, in, position, necessaryLen, extraLen));
      assertThat(e.getMessage(), containsString("EOF"));
      TraceUtil.setError(span, e);
    } finally {
      span.end();
    }
    TEST_UTIL.waitFor(TimeUnit.MINUTES.toMillis(1), new MatcherPredicate<Iterable<SpanData>>(
      OTEL_EXT::getSpans, hasItem(allOf(hasName(methodName), hasEnded()))));
    assertThat(OTEL_EXT.getSpans(),
      hasItems(allOf(hasName(methodName),
        hasEvents(hasItem(allOf(EventMatchers.hasName("BlockIOUtils.preadWithExtra"),
          hasAttributes(AttributesMatchers.isEmpty())))))));

    verify(in).read(position, buf, bufOffset, totalLen);
    verify(in).hasCapability(anyString());
    verifyNoMoreInteractions(in);
  }

  /**
   * Determine if ByteBufferPositionedReadable API is available .
   * @return true if FSDataInputStream implements ByteBufferPositionedReadable API.
   */
  private boolean isByteBufferPositionedReadable() {
    try {
      // long position, ByteBuffer buf
      FSDataInputStream.class.getMethod("read", long.class, ByteBuffer.class);
    } catch (NoSuchMethodException e) {
      return false;
    }
    return true;
  }

  public static class MyFSDataInputStream extends FSDataInputStream {
    public MyFSDataInputStream(InputStream in) {
      super(in);
    }

    // This is the ByteBufferPositionReadable API we want to test.
    // Because the API is only available in Hadoop 3.3, FSDataInputStream in older Hadoop
    // does not implement the interface, and it wouldn't compile trying to mock the method.
    // So explicitly declare the method here to make mocking possible.
    public int read(long position, ByteBuffer buf) throws IOException {
      return 0;
    }
  }

  @Test
  public void testByteBufferPositionedReadable() throws IOException {
    assumeTrue(isByteBufferPositionedReadable(),
      "Skip the test because ByteBufferPositionedReadable is not available");
    long position = 0;
    int necessaryLen = 10;
    int extraLen = 1;
    int totalLen = necessaryLen + extraLen;
    int firstReadLen = 6;
    int secondReadLen = totalLen - firstReadLen;
    ByteBuffer buf = ByteBuffer.allocate(totalLen);
    ByteBuff bb = new SingleByteBuff(buf);
    MyFSDataInputStream in = mock(MyFSDataInputStream.class);

    when(in.read(position, buf)).thenReturn(firstReadLen);
    when(in.read(firstReadLen, buf)).thenReturn(secondReadLen);
    when(in.hasCapability(anyString())).thenReturn(true);
    boolean ret = BlockIOUtils.preadWithExtra(bb, in, position, necessaryLen, extraLen);
    assertTrue(ret, "Expect true return when reading extra bytes succeeds");
    verify(in).read(position, buf);
    verify(in).read(firstReadLen, buf);
    verify(in).hasCapability(anyString());
    verifyNoMoreInteractions(in);
  }

  @Test
  public void testByteBufferPositionedReadableEOF() throws IOException {
    assumeTrue(isByteBufferPositionedReadable(),
      "Skip the test because ByteBufferPositionedReadable is not available");
    long position = 0;
    int necessaryLen = 10;
    int extraLen = 0;
    int totalLen = necessaryLen + extraLen;
    int firstReadLen = 9;
    ByteBuffer buf = ByteBuffer.allocate(totalLen);
    ByteBuff bb = new SingleByteBuff(buf);
    MyFSDataInputStream in = mock(MyFSDataInputStream.class);

    when(in.read(position, buf)).thenReturn(firstReadLen);
    when(in.read(firstReadLen, buf)).thenReturn(-1);
    when(in.hasCapability(anyString())).thenReturn(true);
    IOException e = assertThrows(IOException.class,
      () -> BlockIOUtils.preadWithExtra(bb, in, position, necessaryLen, extraLen));
    assertThat(e.getMessage(), containsString("EOF"));

    verify(in).read(position, buf);
    verify(in).read(firstReadLen, buf);
    verify(in).hasCapability(anyString());
    verifyNoMoreInteractions(in);
  }
}
