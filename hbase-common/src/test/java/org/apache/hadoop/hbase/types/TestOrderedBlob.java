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
import static org.junit.Assert.assertFalse;

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
public class TestOrderedBlob {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestOrderedBlob.class);

  private static final byte[][] VALUES = new byte[][] {
    null, Bytes.toBytes(""), Bytes.toBytes("1"), Bytes.toBytes("22"), Bytes.toBytes("333"),
    Bytes.toBytes("4444"), Bytes.toBytes("55555"), Bytes.toBytes("666666"),
    Bytes.toBytes("7777777"), Bytes.toBytes("88888888"), Bytes.toBytes("999999999"),
  };

  @Test
  public void testIsSkippableFalse() {
    final DataType<byte[]> type = new OrderedBlob(Order.ASCENDING);

    assertFalse(type.isSkippable());
  }

  @Test
  public void testEncodedLength() {
    PositionedByteRange buff = new SimplePositionedMutableByteRange(20);
    for (final DataType<byte[]> type : new OrderedBlob[] { new OrderedBlob(Order.ASCENDING),
      new OrderedBlob(Order.DESCENDING) }) {
      for (final byte[] val : VALUES) {
        buff.setPosition(0);
        type.encode(buff, val);
        assertEquals("encodedLength does not match actual, " + Bytes.toStringBinary(val),
          buff.getPosition(), type.encodedLength(val));
      }
    }
  }

  @Test
  public void testEncodedClassByteArray() {
    final DataType<byte[]> type = new OrderedBlob(Order.ASCENDING);

    assertEquals(byte[].class, type.encodedClass());
  }
}
