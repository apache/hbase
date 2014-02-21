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
package org.apache.hadoop.hbase.types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedByteRange;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestFixedLengthWrapper {

  static final byte[][] VALUES = new byte[][] {
    Bytes.toBytes(""), Bytes.toBytes("1"), Bytes.toBytes("22"), Bytes.toBytes("333"),
    Bytes.toBytes("4444"), Bytes.toBytes("55555"), Bytes.toBytes("666666"),
    Bytes.toBytes("7777777"), Bytes.toBytes("88888888"), Bytes.toBytes("999999999"),
  };

  /**
   * all values of {@code limit} are >= max length of a member of
   * {@code VALUES}.
   */
  static final int[] limits = { 9, 12, 15 };

  @Test
  public void testReadWrite() {
    for (int limit : limits) {
      PositionedByteRange buff = new SimplePositionedByteRange(limit);
      for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
        for (byte[] val : VALUES) {
          buff.setPosition(0);
          DataType<byte[]> type = new FixedLengthWrapper<byte[]>(new RawBytes(ord), limit);
          assertEquals(limit, type.encode(buff, val));
          buff.setPosition(0);
          byte[] actual = type.decode(buff);
          assertTrue("Decoding output differs from expected", 
            Bytes.equals(val, 0, val.length, actual, 0, val.length));
          buff.setPosition(0);
          assertEquals(limit, type.skip(buff));
        }
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInsufficientRemainingRead() {
    PositionedByteRange buff = new SimplePositionedByteRange(0);
    DataType<byte[]> type = new FixedLengthWrapper<byte[]>(new RawBytes(), 3);
    type.decode(buff);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInsufficientRemainingWrite() {
    PositionedByteRange buff = new SimplePositionedByteRange(0);
    DataType<byte[]> type = new FixedLengthWrapper<byte[]>(new RawBytes(), 3);
    type.encode(buff, Bytes.toBytes(""));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testOverflowPassthrough() {
    PositionedByteRange buff = new SimplePositionedByteRange(3);
    DataType<byte[]> type = new FixedLengthWrapper<byte[]>(new RawBytes(), 0);
    type.encode(buff, Bytes.toBytes("foo"));
  }
}
