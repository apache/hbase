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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

/**
 * Unit test suite covering HFileBlock positional read logic.
 */
@Category({SmallTests.class})
public class TestHFileBlockPositionalRead {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testPositionalReadNoExtra() throws IOException {
    long position = 0;
    int bufOffset = 0;
    int necessaryLen = 10;
    int extraLen = 0;
    int totalLen = necessaryLen + extraLen;
    byte[] buf = new byte[totalLen];
    FSDataInputStream in = mock(FSDataInputStream.class);
    when(in.read(position, buf, bufOffset, totalLen)).thenReturn(totalLen);
    boolean ret = HFileBlock.positionalReadWithExtra(in, position, buf,
        bufOffset, necessaryLen, extraLen);
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
    FSDataInputStream in = mock(FSDataInputStream.class);
    when(in.read(position, buf, bufOffset, totalLen)).thenReturn(5);
    when(in.read(5, buf, 5, 5)).thenReturn(5);
    boolean ret = HFileBlock.positionalReadWithExtra(in, position, buf,
        bufOffset, necessaryLen, extraLen);
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
    FSDataInputStream in = mock(FSDataInputStream.class);
    when(in.read(position, buf, bufOffset, totalLen)).thenReturn(totalLen);
    boolean ret = HFileBlock.positionalReadWithExtra(in, position, buf,
        bufOffset, necessaryLen, extraLen);
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
    FSDataInputStream in = mock(FSDataInputStream.class);
    when(in.read(position, buf, bufOffset, totalLen)).thenReturn(necessaryLen);
    boolean ret = HFileBlock.positionalReadWithExtra(in, position, buf,
        bufOffset, necessaryLen, extraLen);
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
    FSDataInputStream in = mock(FSDataInputStream.class);
    when(in.read(position, buf, bufOffset, totalLen)).thenReturn(5);
    when(in.read(5, buf, 5, 10)).thenReturn(10);
    boolean ret = HFileBlock.positionalReadWithExtra(in, position, buf,
        bufOffset, necessaryLen, extraLen);
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
    FSDataInputStream in = mock(FSDataInputStream.class);
    when(in.read(position, buf, bufOffset, totalLen)).thenReturn(9);
    when(in.read(position, buf, bufOffset, totalLen)).thenReturn(-1);
    exception.expect(IOException.class);
    exception.expectMessage("EOF");
    HFileBlock.positionalReadWithExtra(in, position, buf, bufOffset,
        necessaryLen, extraLen);
  }
}
