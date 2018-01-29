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

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedMutableByteRange;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, SmallTests.class})
public class TestTerminatedWrapper {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestTerminatedWrapper.class);

  static final String[] VALUES_STRINGS = new String[] {
    "", "1", "22", "333", "4444", "55555", "666666", "7777777", "88888888", "999999999",
  };

  static final byte[][] VALUES_BYTES = new byte[VALUES_STRINGS.length][];
  static {
    for (int i = 0; i < VALUES_STRINGS.length; i++) {
      VALUES_BYTES[i] = Bytes.toBytes(VALUES_STRINGS[i]);
    }
  }

  static final byte[][] TERMINATORS = new byte[][] { new byte[] { -2 }, Bytes.toBytes("foo") };

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyDelimiter() {
    new TerminatedWrapper<>(new RawBytes(), "");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullDelimiter() {
    new RawBytesTerminated((byte[]) null);
    // new TerminatedWrapper<byte[]>(new RawBytes(), (byte[]) null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEncodedValueContainsTerm() {
    DataType<byte[]> type = new TerminatedWrapper<>(new RawBytes(), "foo");
    PositionedByteRange buff = new SimplePositionedMutableByteRange(16);
    type.encode(buff, Bytes.toBytes("hello foobar!"));
  }

  @Test
  public void testReadWriteSkippable() {
    PositionedByteRange buff = new SimplePositionedMutableByteRange(14);
    for (OrderedString t : new OrderedString[] {
        OrderedString.ASCENDING, OrderedString.DESCENDING
    }) {
      for (byte[] term : TERMINATORS) {
        for (String val : VALUES_STRINGS) {
          buff.setPosition(0);
          DataType<String> type = new TerminatedWrapper<>(t, term);
          assertEquals(val.length() + 2 + term.length, type.encode(buff, val));
          buff.setPosition(0);
          assertEquals(val, type.decode(buff));
          assertEquals(val.length() + 2 + term.length, buff.getPosition());
        }
      }
    }
  }

  @Test
  public void testReadWriteNonSkippable() {
    PositionedByteRange buff = new SimplePositionedMutableByteRange(12);
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      for (byte[] term : TERMINATORS) {
        for (byte[] val : VALUES_BYTES) {
          buff.setPosition(0);
          DataType<byte[]> type = new TerminatedWrapper<>(new RawBytes(ord), term);
          assertEquals(val.length + term.length, type.encode(buff, val));
          buff.setPosition(0);
          assertArrayEquals(val, type.decode(buff));
          assertEquals(val.length + term.length, buff.getPosition());
        }
      }
    }
  }

  @Test
  public void testSkipSkippable() {
    PositionedByteRange buff = new SimplePositionedMutableByteRange(14);
    for (OrderedString t : new OrderedString[] {
        OrderedString.ASCENDING, OrderedString.DESCENDING
    }) {
      for (byte[] term : TERMINATORS) {
        for (String val : VALUES_STRINGS) {
          buff.setPosition(0);
          DataType<String> type = new TerminatedWrapper<>(t, term);
          int expected = val.length() + 2 + term.length;
          assertEquals(expected, type.encode(buff, val));
          buff.setPosition(0);
          assertEquals(expected, type.skip(buff));
          assertEquals(expected, buff.getPosition());
        }
      }
    }
  }

  @Test
  public void testSkipNonSkippable() {
    PositionedByteRange buff = new SimplePositionedMutableByteRange(12);
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      for (byte[] term : TERMINATORS) {
        for (byte[] val : VALUES_BYTES) {
          buff.setPosition(0);
          DataType<byte[]> type = new TerminatedWrapper<>(new RawBytes(ord), term);
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
    PositionedByteRange buff = new SimplePositionedMutableByteRange(Bytes.toBytes("foo"));
    DataType<byte[]> type = new TerminatedWrapper<>(new RawBytes(), new byte[] { 0x00 });
    type.skip(buff);
  }
}
