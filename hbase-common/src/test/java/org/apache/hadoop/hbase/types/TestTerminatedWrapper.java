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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedByteRange;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestTerminatedWrapper {

  static final byte[][] VALUES = new byte[][] {
    Bytes.toBytes(""), Bytes.toBytes("1"), Bytes.toBytes("22"), Bytes.toBytes("333"),
    Bytes.toBytes("4444"), Bytes.toBytes("55555"), Bytes.toBytes("666666"),
    Bytes.toBytes("7777777"), Bytes.toBytes("88888888"), Bytes.toBytes("999999999"),
  };

  static final byte[][] TERMINATORS = new byte[][] { new byte[] { -1 }, Bytes.toBytes("foo") };

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyDelimiter() {
    new TerminatedWrapper<byte[]>(new RawBytes(), "");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullDelimiter() {
    new RawBytesTerminated((byte[]) null);
    // new TerminatedWrapper<byte[]>(new RawBytes(), (byte[]) null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEncodedValueContainsTerm() {
    DataType<byte[]> type = new TerminatedWrapper<byte[]>(new RawBytes(), "foo");
    PositionedByteRange buff = new SimplePositionedByteRange(16);
    type.encode(buff, Bytes.toBytes("hello foobar!"));
  }

  @Test
  public void testReadWrite() {
    PositionedByteRange buff = new SimplePositionedByteRange(12);
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      for (byte[] term : TERMINATORS) {
        for (byte[] val : VALUES) {
          buff.setPosition(0);
          DataType<byte[]> type = new TerminatedWrapper<byte[]>(new RawBytes(ord), term);
          assertEquals(val.length + term.length, type.encode(buff, val));
          buff.setPosition(0);
          assertArrayEquals(val, type.decode(buff));
          assertEquals(val.length + term.length, buff.getPosition());
        }
      }
    }
  }

  @Test
  public void testSkip() {
    PositionedByteRange buff = new SimplePositionedByteRange(12);
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      for (byte[] term : TERMINATORS) {
        for (byte[] val : VALUES) {
          buff.setPosition(0);
          DataType<byte[]> type = new TerminatedWrapper<byte[]>(new RawBytes(ord), term);
          int expected = type.encode(buff, val);
          buff.setPosition(0);
          assertEquals(expected, type.skip(buff));
          assertEquals(expected, buff.getPosition());
        }
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSkip() {
    PositionedByteRange buff = new SimplePositionedByteRange(Bytes.toBytes("foo"));
    DataType<byte[]> type = new TerminatedWrapper<byte[]>(new RawBytes(), new byte[] { 0x00 });
    type.skip(buff);
  }
}
