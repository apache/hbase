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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestVIntTool {

  @Test
  public void testNumBytes() {
    Assert.assertEquals(1, UVIntTool.numBytes(0));
    Assert.assertEquals(1, UVIntTool.numBytes(1));
    Assert.assertEquals(1, UVIntTool.numBytes(100));
    Assert.assertEquals(1, UVIntTool.numBytes(126));
    Assert.assertEquals(1, UVIntTool.numBytes(127));
    Assert.assertEquals(2, UVIntTool.numBytes(128));
    Assert.assertEquals(2, UVIntTool.numBytes(129));
    Assert.assertEquals(5, UVIntTool.numBytes(Integer.MAX_VALUE));
  }

  @Test
  public void testWriteBytes() throws IOException {
    Assert.assertArrayEquals(new byte[] { 0 }, bytesViaOutputStream(0));
    Assert.assertArrayEquals(new byte[] { 1 }, bytesViaOutputStream(1));
    Assert.assertArrayEquals(new byte[] { 63 }, bytesViaOutputStream(63));
    Assert.assertArrayEquals(new byte[] { 127 }, bytesViaOutputStream(127));
    Assert.assertArrayEquals(new byte[] { -128, 1 }, bytesViaOutputStream(128));
    Assert.assertArrayEquals(new byte[] { -128 + 27, 1 }, bytesViaOutputStream(155));
    Assert.assertArrayEquals(UVIntTool.MAX_VALUE_BYTES, bytesViaOutputStream(Integer.MAX_VALUE));
  }

  private byte[] bytesViaOutputStream(int value) throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    UVIntTool.writeBytes(value, os);
    return os.toByteArray();
  }

  @Test
  public void testToBytes() {
    Assert.assertArrayEquals(new byte[] { 0 }, UVIntTool.getBytes(0));
    Assert.assertArrayEquals(new byte[] { 1 }, UVIntTool.getBytes(1));
    Assert.assertArrayEquals(new byte[] { 63 }, UVIntTool.getBytes(63));
    Assert.assertArrayEquals(new byte[] { 127 }, UVIntTool.getBytes(127));
    Assert.assertArrayEquals(new byte[] { -128, 1 }, UVIntTool.getBytes(128));
    Assert.assertArrayEquals(new byte[] { -128 + 27, 1 }, UVIntTool.getBytes(155));
    Assert.assertArrayEquals(UVIntTool.MAX_VALUE_BYTES, UVIntTool.getBytes(Integer.MAX_VALUE));
  }

  @Test
  public void testFromBytes() {
    Assert.assertEquals(Integer.MAX_VALUE, UVIntTool.getInt(UVIntTool.MAX_VALUE_BYTES));
  }

  @Test
  public void testRoundTrips() {
    Random random = new Random();
    for (int i = 0; i < 10000; ++i) {
      int value = random.nextInt(Integer.MAX_VALUE);
      byte[] bytes = UVIntTool.getBytes(value);
      int roundTripped = UVIntTool.getInt(bytes);
      Assert.assertEquals(value, roundTripped);
    }
  }

  @Test
  public void testInputStreams() throws IOException {
    ByteArrayInputStream is;
    is = new ByteArrayInputStream(new byte[] { 0 });
    Assert.assertEquals(0, UVIntTool.getInt(is));
    is = new ByteArrayInputStream(new byte[] { 5 });
    Assert.assertEquals(5, UVIntTool.getInt(is));
    is = new ByteArrayInputStream(new byte[] { -128 + 27, 1 });
    Assert.assertEquals(155, UVIntTool.getInt(is));
  }

}
