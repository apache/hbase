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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.io.util.BlockIOUtils;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.MultiByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

@Category({ IOTests.class, SmallTests.class })
public class TestBlockIOUtils {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestBlockIOUtils.class);

  @Rule
  public ExpectedException exception = ExpectedException.none();

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

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
    when(in.hasCapability(anyString())).thenReturn(false);
    boolean ret = BlockIOUtils.preadWithExtra(bb, in, position, necessaryLen, extraLen);
    assertFalse("Expect false return when no extra bytes requested", ret);
    verify(in).read(position, buf, bufOffset, totalLen);
    verify(in).hasCapability(anyString());
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
    when(in.hasCapability(anyString())).thenReturn(false);
    boolean ret = BlockIOUtils.preadWithExtra(bb, in, position, necessaryLen, extraLen);
    assertFalse("Expect false return when no extra bytes requested", ret);
    verify(in).read(position, buf, bufOffset, totalLen);
    verify(in).read(5, buf, 5, 5);
    verify(in).hasCapability(anyString());
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
    when(in.hasCapability(anyString())).thenReturn(false);
    boolean ret = BlockIOUtils.preadWithExtra(bb, in, position, necessaryLen, extraLen);
    assertTrue("Expect true return when reading extra bytes succeeds", ret);
    verify(in).read(position, buf, bufOffset, totalLen);
    verify(in).hasCapability(anyString());
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
    when(in.hasCapability(anyString())).thenReturn(false);
    boolean ret = BlockIOUtils.preadWithExtra(bb, in, position, necessaryLen, extraLen);
    assertFalse("Expect false return when reading extra bytes fails", ret);
    verify(in).read(position, buf, bufOffset, totalLen);
    verify(in).hasCapability(anyString());
    verifyNoMoreInteractions(in);
  }

  @Test
  public void testPositionalReadShortReadCompletesNecessaryAndExtraBytes()
      throws IOException {
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
    boolean ret = BlockIOUtils.preadWithExtra(bb, in, position, necessaryLen, extraLen);
    assertTrue("Expect true return when reading extra bytes succeeds", ret);
    verify(in).read(position, buf, bufOffset, totalLen);
    verify(in).read(5, buf, 5, 10);
    verify(in).hasCapability(anyString());
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
    when(in.hasCapability(anyString())).thenReturn(false);
    exception.expect(IOException.class);
    exception.expectMessage("EOF");
    BlockIOUtils.preadWithExtra(bb, in, position, necessaryLen, extraLen);
  }

  /**
   * Determine if ByteBufferPositionedReadable API is available
   * .
   * @return true if FSDataInputStream implements ByteBufferPositionedReadable API.
   */
  private boolean isByteBufferPositionedReadable() {
    try {
      //long position, ByteBuffer buf
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
    assumeTrue("Skip the test because ByteBufferPositionedReadable is not available",
      isByteBufferPositionedReadable());
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
    assertTrue("Expect true return when reading extra bytes succeeds", ret);
    verify(in).read(position, buf);
    verify(in).read(firstReadLen, buf);
    verify(in).hasCapability(anyString());
    verifyNoMoreInteractions(in);
  }

  @Test
  public void testByteBufferPositionedReadableEOF() throws IOException {
    assumeTrue("Skip the test because ByteBufferPositionedReadable is not available",
      isByteBufferPositionedReadable());
    long position = 0;
    int necessaryLen = 10;
    int extraLen = 0;
    int totalLen = necessaryLen + extraLen;
    int firstReadLen = 9;
    ByteBuffer buf = ByteBuffer.allocate(totalLen);
    ByteBuff bb = new SingleByteBuff(buf);
    MyFSDataInputStream in = mock(MyFSDataInputStream.class);

    when(in.read(position, buf)).thenReturn(firstReadLen);
    when(in.read(position, buf)).thenReturn(-1);
    when(in.hasCapability(anyString())).thenReturn(true);
    exception.expect(IOException.class);
    exception.expectMessage("EOF");
    BlockIOUtils.preadWithExtra(bb, in, position, necessaryLen, extraLen);

    verify(in).read(position, buf);
    verify(in).read(firstReadLen, buf);
    verify(in).hasCapability(anyString());
    verifyNoMoreInteractions(in);
  }
}
