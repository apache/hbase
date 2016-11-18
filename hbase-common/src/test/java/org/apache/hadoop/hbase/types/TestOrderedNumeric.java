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

import java.math.BigDecimal;
import java.math.BigInteger;
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
public class TestOrderedNumeric {
  private static final Long[] LONG_VALUES = new Long[] {
    1L, 22L, 333L, 4444L, 55555L, 666666L, 7777777L, 88888888L, 999999999L
  };

  private static final Double[] DOUBLE_VALUES = new Double[] {
    Double.NaN, 1.1, 22.2, 333.3, 4444.4, 55555.5, 666666.6, 7777777.7, 88888888.8, 999999999.9
  };

  private static final BigDecimal[] BIG_DECIMAL_VALUES = new BigDecimal[] {
    new BigDecimal(1), new BigDecimal(22), new BigDecimal(333), new BigDecimal(4444),
    new BigDecimal(55555), new BigDecimal(666666), new BigDecimal(7777777),
    new BigDecimal(88888888), new BigDecimal(999999999)
  };

  private static final BigInteger[] BIG_INTEGER_VALUES = new BigInteger[] {
    new BigInteger("1"), new BigInteger("22"), new BigInteger("333"), new BigInteger("4444"),
    new BigInteger("55555"), new BigInteger("666666"), new BigInteger("7777777"),
    new BigInteger("88888888"), new BigInteger("999999999")
  };

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestOrderedNumeric.class);

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testEncodedClassIsNumber() {
    final DataType<Number> type = new OrderedNumeric(Order.ASCENDING);

    assertEquals(Number.class, type.encodedClass());
  }

  @Test
  public void testEncodedLength() {
    final PositionedByteRange buffer = new SimplePositionedMutableByteRange(20);
    for (final DataType<Number> type : new OrderedNumeric[] { new OrderedNumeric(Order.ASCENDING),
      new OrderedNumeric(Order.DESCENDING) }) {
      for (final Number val : DOUBLE_VALUES) {
        buffer.setPosition(0);
        type.encode(buffer, val);
        assertEquals("encodedLength does not match actual, " + val,
            buffer.getPosition(), type.encodedLength(val));
      }
    }
  }

  @Test
  public void testEncodedBigDecimalLength() {
    final PositionedByteRange buffer = new SimplePositionedMutableByteRange(20);
    for (final DataType<Number> type : new OrderedNumeric[] { new OrderedNumeric(Order.ASCENDING),
      new OrderedNumeric(Order.DESCENDING) }) {
      for (final Number val : BIG_DECIMAL_VALUES) {
        buffer.setPosition(0);
        type.encode(buffer, val);
        assertEquals("encodedLength does not match actual, " + val,
            buffer.getPosition(), type.encodedLength(val));
      }
    }
  }

  @Test
  public void testEncodedBigIntegerLength() {
    final PositionedByteRange buffer = new SimplePositionedMutableByteRange(20);
    for (final DataType<Number> type : new OrderedNumeric[] { new OrderedNumeric(Order.ASCENDING),
      new OrderedNumeric(Order.DESCENDING) }) {
      for (final Number val : BIG_INTEGER_VALUES) {
        buffer.setPosition(0);
        type.encode(buffer, val);
        assertEquals("encodedLength does not match actual, " + val,
            buffer.getPosition(), type.encodedLength(val));
      }
    }
  }

  @Test
  public void testEncodedNullLength() {
    final PositionedByteRange buffer = new SimplePositionedMutableByteRange(20);
    final DataType<Number> type = new OrderedNumeric(Order.ASCENDING);

    buffer.setPosition(0);
    type.encode(buffer, null);
    type.encode(new SimplePositionedMutableByteRange(20), null);

    assertEquals("encodedLength does not match actual, " + null,
        buffer.getPosition(), type.encodedLength(null));
  }

  @Test
  public void testEncodedLongLength() {
    final PositionedByteRange buffer = new SimplePositionedMutableByteRange(20);
    for (final OrderedNumeric type : new OrderedNumeric[] { new OrderedNumeric(Order.ASCENDING),
      new OrderedNumeric(Order.DESCENDING) }) {
      for (final Long val : LONG_VALUES) {
        buffer.setPosition(0);
        type.encodeLong(buffer, val);
        assertEquals("encodedLength does not match actual, " + val,
            buffer.getPosition(), type.encodedLength(val));
      }
    }
  }

  @Test
  public void testEncodedDoubleLength() {
    final PositionedByteRange buffer = new SimplePositionedMutableByteRange(20);
    for (final OrderedNumeric type : new OrderedNumeric[] { new OrderedNumeric(Order.ASCENDING),
      new OrderedNumeric(Order.DESCENDING) }) {
      for (final Double val : DOUBLE_VALUES) {
        buffer.setPosition(0);
        type.encodeDouble(buffer, val);
        assertEquals("encodedLength does not match actual, " + val,
            buffer.getPosition(), type.encodedLength(val));
      }
    }
  }
}
