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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.util.Arrays;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedMutableByteRange;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MiscTests.TAG)
@Tag(SmallTests.TAG)
public class TestStructNullExtension {

  /**
   * Verify null extension respects the type's isNullable field.
   */
  @Test
  public void testNonNullableNullExtension() {
    // not nullable
    Struct s = new StructBuilder().add(new RawStringTerminated("|")).toStruct();
    PositionedByteRange buf = new SimplePositionedMutableByteRange(4);
    assertThrows(NullPointerException.class, () -> {
      s.encode(buf, new Object[1]);
    });
  }

  /**
   * Positive cases for null extension.
   */
  @Test
  public void testNullableNullExtension() {
    // the following field members are used because they're all nullable
    StructBuilder builder =
      new StructBuilder().add(OrderedNumeric.ASCENDING).add(OrderedString.ASCENDING);
    Struct shorter = builder.toStruct();
    Struct longer = builder
      // intentionally include a wrapped instance to test wrapper behavior.
      .add(new TerminatedWrapper<>(OrderedString.ASCENDING, "/")).add(OrderedNumeric.ASCENDING)
      .toStruct();

    PositionedByteRange buf1 = new SimplePositionedMutableByteRange(7);
    Object[] val1 = new Object[] { BigDecimal.ONE, "foo" }; // => 2 bytes + 5 bytes
    assertEquals(buf1.getLength(), shorter.encode(buf1, val1),
      "Encoding shorter value wrote a surprising number of bytes.");
    int shortLen = buf1.getLength();

    // test iterator
    buf1.setPosition(0);
    StructIterator it = longer.iterator(buf1);
    it.skip();
    it.skip();
    assertEquals(buf1.getLength(), buf1.getPosition(), "Position should be at end. Broken test.");
    assertEquals(0, it.skip(), "Failed to skip null element with extended struct.");
    assertEquals(0, it.skip(), "Failed to skip null element with extended struct.");

    buf1.setPosition(0);
    it = longer.iterator(buf1);
    assertEquals(BigDecimal.ONE, it.next());
    assertEquals("foo", it.next());
    assertEquals(buf1.getLength(), buf1.getPosition(), "Position should be at end. Broken test.");
    assertNull(it.next(), "Failed to skip null element with extended struct.");
    assertNull(it.next(), "Failed to skip null element with extended struct.");

    // test Struct
    buf1.setPosition(0);
    assertArrayEquals(val1, shorter.decode(buf1), "Simple struct decoding is broken.");

    buf1.setPosition(0);
    assertArrayEquals(Arrays.copyOf(val1, 4), longer.decode(buf1),
      "Decoding short value with extended struct should append null elements.");

    // test omission of trailing members
    PositionedByteRange buf2 = new SimplePositionedMutableByteRange(7);
    buf1.setPosition(0);
    assertEquals(shortLen, longer.encode(buf2, val1),
      "Encoding a short value with extended struct should have same result as using short struct.");
    assertArrayEquals(buf1.getBytes(), buf2.getBytes(),
      "Encoding a short value with extended struct should have same result as using short struct");

    // test null trailing members
    // all fields are nullable, so nothing should hit the buffer.
    val1 = new Object[] { null, null, null, null }; // => 0 bytes
    buf1.set(0);
    buf2.set(0);
    assertEquals(buf1.getLength(), longer.encode(buf1, new Object[0]),
      "Encoding null-truncated value wrote a surprising number of bytes.");
    assertEquals(buf1.getLength(), longer.encode(buf1, val1),
      "Encoding null-extended value wrote a surprising number of bytes.");
    assertArrayEquals(buf1.getBytes(), buf2.getBytes(), "Encoded unexpected result.");
    assertArrayEquals(val1, longer.decode(buf2), "Decoded unexpected result.");

    // all fields are nullable, so only 1 should hit the buffer.
    Object[] val2 = new Object[] { BigDecimal.ONE, null, null, null }; // => 2 bytes
    buf1.set(2);
    buf2.set(2);
    assertEquals(buf1.getLength(), longer.encode(buf1, Arrays.copyOf(val2, 1)),
      "Encoding null-truncated value wrote a surprising number of bytes.");
    assertEquals(buf2.getLength(), longer.encode(buf2, val2),
      "Encoding null-extended value wrote a surprising number of bytes.");
    assertArrayEquals(buf1.getBytes(), buf2.getBytes(), "Encoded unexpected result.");
    buf2.setPosition(0);
    assertArrayEquals(val2, longer.decode(buf2), "Decoded unexpected result.");

    // all fields are nullable, so only 1, null, "foo" should hit the buffer.
    // => 2 bytes + 1 byte + 6 bytes
    Object[] val3 = new Object[] { BigDecimal.ONE, null, "foo", null };
    buf1.set(9);
    buf2.set(9);
    assertEquals(buf1.getLength(), longer.encode(buf1, Arrays.copyOf(val3, 3)),
      "Encoding null-truncated value wrote a surprising number of bytes.");
    assertEquals(buf2.getLength(), longer.encode(buf2, val3),
      "Encoding null-extended value wrote a surprising number of bytes.");
    assertArrayEquals(buf1.getBytes(), buf2.getBytes(), "Encoded unexpected result.");
    buf2.setPosition(0);
    assertArrayEquals(val3, longer.decode(buf2), "Decoded unexpected result.");
  }
}
