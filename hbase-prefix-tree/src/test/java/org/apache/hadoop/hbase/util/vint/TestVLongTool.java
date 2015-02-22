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
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.number.RandomNumberUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, SmallTests.class})
public class TestVLongTool {

  @Test
  public void testNumBytes() {
    Assert.assertEquals(1, UVLongTool.numBytes(0));
    Assert.assertEquals(1, UVLongTool.numBytes(1));
    Assert.assertEquals(1, UVLongTool.numBytes(100));
    Assert.assertEquals(1, UVLongTool.numBytes(126));
    Assert.assertEquals(1, UVLongTool.numBytes(127));
    Assert.assertEquals(2, UVLongTool.numBytes(128));
    Assert.assertEquals(2, UVLongTool.numBytes(129));
    Assert.assertEquals(9, UVLongTool.numBytes(Long.MAX_VALUE));
  }

  @Test
  public void testToBytes() {
    Assert.assertArrayEquals(new byte[] { 0 }, UVLongTool.getBytes(0));
    Assert.assertArrayEquals(new byte[] { 1 }, UVLongTool.getBytes(1));
    Assert.assertArrayEquals(new byte[] { 63 }, UVLongTool.getBytes(63));
    Assert.assertArrayEquals(new byte[] { 127 }, UVLongTool.getBytes(127));
    Assert.assertArrayEquals(new byte[] { -128, 1 }, UVLongTool.getBytes(128));
    Assert.assertArrayEquals(new byte[] { -128 + 27, 1 }, UVLongTool.getBytes(155));
    Assert.assertArrayEquals(UVLongTool.MAX_VALUE_BYTES, UVLongTool.getBytes(Long.MAX_VALUE));
  }

  @Test
  public void testFromBytes() {
    Assert.assertEquals(Long.MAX_VALUE, UVLongTool.getLong(UVLongTool.MAX_VALUE_BYTES));
  }

  @Test
  public void testFromBytesOffset() {
    Assert.assertEquals(Long.MAX_VALUE, UVLongTool.getLong(UVLongTool.MAX_VALUE_BYTES, 0));

    long ms = 1318966363481L;
//    System.out.println(ms);
    byte[] bytes = UVLongTool.getBytes(ms);
//    System.out.println(Arrays.toString(bytes));
    long roundTripped = UVLongTool.getLong(bytes, 0);
    Assert.assertEquals(ms, roundTripped);

    int calculatedNumBytes = UVLongTool.numBytes(ms);
    int actualNumBytes = bytes.length;
    Assert.assertEquals(actualNumBytes, calculatedNumBytes);

    byte[] shiftedBytes = new byte[1000];
    int shift = 33;
    System.arraycopy(bytes, 0, shiftedBytes, shift, bytes.length);
    long shiftedRoundTrip = UVLongTool.getLong(shiftedBytes, shift);
    Assert.assertEquals(ms, shiftedRoundTrip);
  }

  @Test
  public void testRoundTrips() {
    Random random = new Random();
    for (int i = 0; i < 10000; ++i) {
      long value = RandomNumberUtils.nextPositiveLong(random);
      byte[] bytes = UVLongTool.getBytes(value);
      long roundTripped = UVLongTool.getLong(bytes);
      Assert.assertEquals(value, roundTripped);
      int calculatedNumBytes = UVLongTool.numBytes(value);
      int actualNumBytes = bytes.length;
      Assert.assertEquals(actualNumBytes, calculatedNumBytes);
    }
  }

  @Test
  public void testInputStreams() throws IOException {
    ByteArrayInputStream is;
    is = new ByteArrayInputStream(new byte[] { 0 });
    Assert.assertEquals(0, UVLongTool.getLong(is));
    is = new ByteArrayInputStream(new byte[] { 5 });
    Assert.assertEquals(5, UVLongTool.getLong(is));
    is = new ByteArrayInputStream(new byte[] { -128 + 27, 1 });
    Assert.assertEquals(155, UVLongTool.getLong(is));
  }
}
