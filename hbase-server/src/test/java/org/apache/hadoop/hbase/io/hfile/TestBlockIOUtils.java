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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
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

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

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
