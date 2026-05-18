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
package org.apache.hadoop.hbase.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedMutableByteRange;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MiscTests.TAG)
@Tag(SmallTests.TAG)
public class TestOrderedFloat32 {
  private static final Float[] VALUES =
    new Float[] { Float.NaN, 1f, 22f, 333f, 4444f, 55555f, 666666f, 7777777f, 8888888f, 9999999f };

  @Test
  public void testIsNullableIsFalse() {
    final DataType<Float> type = new OrderedFloat32(Order.ASCENDING);

    assertFalse(type.isNullable());
  }

  @Test
  public void testEncodedClassIsFloat() {
    final DataType<Float> type = new OrderedFloat32(Order.ASCENDING);

    assertEquals(Float.class, type.encodedClass());
  }

  @Test
  public void testEncodedLength() {
    final PositionedByteRange buffer = new SimplePositionedMutableByteRange(20);
    for (final DataType<Float> type : new OrderedFloat32[] { new OrderedFloat32(Order.ASCENDING),
      new OrderedFloat32(Order.DESCENDING) }) {
      for (final Float val : VALUES) {
        buffer.setPosition(0);
        type.encode(buffer, val);
        assertEquals(buffer.getPosition(), type.encodedLength(val),
          "encodedLength does not match actual, " + val);
      }
    }
  }

  @Test
  public void testEncodeNoSupportForNull() {
    final DataType<Float> type = new OrderedFloat32(Order.ASCENDING);

    assertThrows(IllegalArgumentException.class,
      () -> type.encode(new SimplePositionedMutableByteRange(20), null));
  }

  @Test
  public void testEncodedFloatLength() {
    final PositionedByteRange buffer = new SimplePositionedMutableByteRange(20);
    for (final OrderedFloat32 type : new OrderedFloat32[] { new OrderedFloat32(Order.ASCENDING),
      new OrderedFloat32(Order.DESCENDING) }) {
      for (final Float val : VALUES) {
        buffer.setPosition(0);
        type.encodeFloat(buffer, val);
        assertEquals(buffer.getPosition(), type.encodedLength(val),
          "encodedLength does not match actual, " + val);
      }
    }
  }
}
