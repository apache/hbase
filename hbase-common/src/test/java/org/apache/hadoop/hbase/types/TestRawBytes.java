/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedMutableByteRange;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

@Category({MiscTests.class, SmallTests.class})
public class TestRawBytes {
  private static final byte[][] VALUES = new byte[][] {
      Bytes.toBytes(""), Bytes.toBytes("1"), Bytes.toBytes("22"), Bytes.toBytes("333"),
      Bytes.toBytes("4444"), Bytes.toBytes("55555"), Bytes.toBytes("666666"),
      Bytes.toBytes("7777777"), Bytes.toBytes("88888888"), Bytes.toBytes("999999999"),
  };

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRawBytes.class);

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testIsOrderPreservingIsTrue() {
    final DataType<byte[]> type = new RawBytes(Order.ASCENDING);

    assertTrue(type.isOrderPreserving());
  }

  @Test
  public void testGetOrderCorrectOrder() {
    final DataType<byte[]> type = new RawBytes(Order.ASCENDING);

    assertEquals(Order.ASCENDING, type.getOrder());
  }

  @Test
  public void testIsNullableIsFalse() {
    final DataType<byte[]> type = new RawBytes(Order.ASCENDING);

    assertFalse(type.isNullable());
  }

  @Test
  public void testIsSkippableIsFalse() {
    final DataType<byte[]> type = new RawBytes(Order.ASCENDING);

    assertFalse(type.isSkippable());
  }

  @Test
  public void testEncodedClassIsByteArray() {
    final DataType<byte[]> type = new RawBytes(Order.ASCENDING);

    assertEquals(byte[].class, type.encodedClass());
  }

  @Test
  public void testEncodedLength() {
    final PositionedByteRange buffer = new SimplePositionedMutableByteRange(20);
    for (final DataType<byte[]> type : new RawBytes[] { new RawBytes(Order.ASCENDING),
      new RawBytes(Order.DESCENDING) }) {
      for (final byte[] val : VALUES) {
        buffer.setPosition(0);
        type.encode(buffer, val);
        assertEquals("encodedLength does not match actual, " + Arrays.toString(val),
            buffer.getPosition(), type.encodedLength(val));
      }
    }
  }
}
