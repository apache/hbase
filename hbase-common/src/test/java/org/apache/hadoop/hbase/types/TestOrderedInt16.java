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
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedMutableByteRange;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

@Category({MiscTests.class, SmallTests.class})
public class TestOrderedInt16 {
  private static final Short[] VALUES = new Short[] {
    1, 22, 333, 4444
  };

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestOrderedInt16.class);

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testIsNullableIsFalse() {
    final DataType<Short> type = new OrderedInt16(Order.ASCENDING);

    assertFalse(type.isNullable());
  }

  @Test
  public void testEncodedClassIsShort() {
    final DataType<Short> type = new OrderedInt16(Order.ASCENDING);

    assertEquals(Short.class, type.encodedClass());
  }

  @Test
  public void testEncodedLength() {
    final PositionedByteRange buffer = new SimplePositionedMutableByteRange(20);
    for (final DataType<Short> type : new OrderedInt16[] { new OrderedInt16(Order.ASCENDING),
      new OrderedInt16(Order.DESCENDING) }) {
      for (final Short val : VALUES) {
        buffer.setPosition(0);
        type.encode(buffer, val);
        assertEquals("encodedLength does not match actual, " + val,
            buffer.getPosition(), type.encodedLength(val));
      }
    }
  }

  @Test
  public void testEncodeNoSupportForNull() {
    exception.expect(IllegalArgumentException.class);

    final DataType<Short> type = new OrderedInt16(Order.ASCENDING);

    type.encode(new SimplePositionedMutableByteRange(20), null);
  }

  @Test
  public void testEncodedFloatLength() {
    final PositionedByteRange buffer = new SimplePositionedMutableByteRange(20);
    for (final OrderedInt16 type : new OrderedInt16[] { new OrderedInt16(Order.ASCENDING),
      new OrderedInt16(Order.DESCENDING) }) {
      for (final Short val : VALUES) {
        buffer.setPosition(0);
        type.encodeShort(buffer, val);
        assertEquals("encodedLength does not match actual, " + val,
            buffer.getPosition(), type.encodedLength(val));
      }
    }
  }
}
