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

package org.apache.hadoop.hbase.util.vint;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/********************** tests *************************/

@Category(SmallTests.class)
public class TestFIntTool {
  @Test
  public void testLeadingZeros() {
    Assert.assertEquals(64, Long.numberOfLeadingZeros(0));
    Assert.assertEquals(63, Long.numberOfLeadingZeros(1));
    Assert.assertEquals(0, Long.numberOfLeadingZeros(Long.MIN_VALUE));
    Assert.assertEquals(0, Long.numberOfLeadingZeros(-1));
    Assert.assertEquals(1, Long.numberOfLeadingZeros(Long.MAX_VALUE));
    Assert.assertEquals(1, Long.numberOfLeadingZeros(Long.MAX_VALUE - 1));
  }

  @Test
  public void testMaxValueForNumBytes() {
    Assert.assertEquals(255, UFIntTool.maxValueForNumBytes(1));
    Assert.assertEquals(65535, UFIntTool.maxValueForNumBytes(2));
    Assert.assertEquals(0xffffff, UFIntTool.maxValueForNumBytes(3));
    Assert.assertEquals(0xffffffffffffffL, UFIntTool.maxValueForNumBytes(7));
  }

  @Test
  public void testNumBytes() {
    Assert.assertEquals(1, UFIntTool.numBytes(0));
    Assert.assertEquals(1, UFIntTool.numBytes(1));
    Assert.assertEquals(1, UFIntTool.numBytes(255));
    Assert.assertEquals(2, UFIntTool.numBytes(256));
    Assert.assertEquals(2, UFIntTool.numBytes(65535));
    Assert.assertEquals(3, UFIntTool.numBytes(65536));
    Assert.assertEquals(4, UFIntTool.numBytes(0xffffffffL));
    Assert.assertEquals(5, UFIntTool.numBytes(0x100000000L));
    Assert.assertEquals(4, UFIntTool.numBytes(Integer.MAX_VALUE));
    Assert.assertEquals(8, UFIntTool.numBytes(Long.MAX_VALUE));
    Assert.assertEquals(8, UFIntTool.numBytes(Long.MAX_VALUE - 1));
  }

  @Test
  public void testGetBytes() {
    Assert.assertArrayEquals(new byte[] { 0 }, UFIntTool.getBytes(1, 0));
    Assert.assertArrayEquals(new byte[] { 1 }, UFIntTool.getBytes(1, 1));
    Assert.assertArrayEquals(new byte[] { -1 }, UFIntTool.getBytes(1, 255));
    Assert.assertArrayEquals(new byte[] { 1, 0 }, UFIntTool.getBytes(2, 256));
    Assert.assertArrayEquals(new byte[] { 1, 3 }, UFIntTool.getBytes(2, 256 + 3));
    Assert.assertArrayEquals(new byte[] { 1, -128 }, UFIntTool.getBytes(2, 256 + 128));
    Assert.assertArrayEquals(new byte[] { 1, -1 }, UFIntTool.getBytes(2, 256 + 255));
    Assert.assertArrayEquals(new byte[] { 127, -1, -1, -1 },
      UFIntTool.getBytes(4, Integer.MAX_VALUE));
    Assert.assertArrayEquals(new byte[] { 127, -1, -1, -1, -1, -1, -1, -1 },
      UFIntTool.getBytes(8, Long.MAX_VALUE));
  }

  @Test
  public void testFromBytes() {
    Assert.assertEquals(0, UFIntTool.fromBytes(new byte[] { 0 }));
    Assert.assertEquals(1, UFIntTool.fromBytes(new byte[] { 1 }));
    Assert.assertEquals(255, UFIntTool.fromBytes(new byte[] { -1 }));
    Assert.assertEquals(256, UFIntTool.fromBytes(new byte[] { 1, 0 }));
    Assert.assertEquals(256 + 3, UFIntTool.fromBytes(new byte[] { 1, 3 }));
    Assert.assertEquals(256 + 128, UFIntTool.fromBytes(new byte[] { 1, -128 }));
    Assert.assertEquals(256 + 255, UFIntTool.fromBytes(new byte[] { 1, -1 }));
    Assert.assertEquals(Integer.MAX_VALUE, UFIntTool.fromBytes(new byte[] { 127, -1, -1, -1 }));
    Assert.assertEquals(Long.MAX_VALUE,
      UFIntTool.fromBytes(new byte[] { 127, -1, -1, -1, -1, -1, -1, -1 }));
  }

  @Test
  public void testRoundTrips() {
    long[] values = new long[] { 0, 1, 2, 255, 256, 31123, 65535, 65536, 65537, 0xfffffeL,
        0xffffffL, 0x1000000L, 0x1000001L, Integer.MAX_VALUE - 1, Integer.MAX_VALUE,
        (long) Integer.MAX_VALUE + 1, Long.MAX_VALUE - 1, Long.MAX_VALUE };
    for (int i = 0; i < values.length; ++i) {
      Assert.assertEquals(values[i], UFIntTool.fromBytes(UFIntTool.getBytes(8, values[i])));
    }
  }

  @Test
  public void testWriteBytes() throws IOException {// copied from testGetBytes
    Assert.assertArrayEquals(new byte[] { 0 }, bytesViaOutputStream(1, 0));
    Assert.assertArrayEquals(new byte[] { 1 }, bytesViaOutputStream(1, 1));
    Assert.assertArrayEquals(new byte[] { -1 }, bytesViaOutputStream(1, 255));
    Assert.assertArrayEquals(new byte[] { 1, 0 }, bytesViaOutputStream(2, 256));
    Assert.assertArrayEquals(new byte[] { 1, 3 }, bytesViaOutputStream(2, 256 + 3));
    Assert.assertArrayEquals(new byte[] { 1, -128 }, bytesViaOutputStream(2, 256 + 128));
    Assert.assertArrayEquals(new byte[] { 1, -1 }, bytesViaOutputStream(2, 256 + 255));
    Assert.assertArrayEquals(new byte[] { 127, -1, -1, -1 },
      bytesViaOutputStream(4, Integer.MAX_VALUE));
    Assert.assertArrayEquals(new byte[] { 127, -1, -1, -1, -1, -1, -1, -1 },
      bytesViaOutputStream(8, Long.MAX_VALUE));
  }

  private byte[] bytesViaOutputStream(int outputWidth, long value) throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    UFIntTool.writeBytes(outputWidth, value, os);
    return os.toByteArray();
  }
}
