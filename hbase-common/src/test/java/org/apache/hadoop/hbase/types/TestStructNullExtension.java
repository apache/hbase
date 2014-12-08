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
import static org.junit.Assert.assertNull;

import java.math.BigDecimal;
import java.util.Arrays;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedMutableByteRange;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestStructNullExtension {

  /**
   * Verify null extension respects the type's isNullable field.
   */
  @Test(expected = NullPointerException.class)
  public void testNonNullableNullExtension() {
    Struct s = new StructBuilder()
        .add(new RawStringTerminated("|")) // not nullable
        .toStruct();
    PositionedByteRange buf = new SimplePositionedMutableByteRange(4);
    s.encode(buf, new Object[1]);
  }

  /**
   * Positive cases for null extension.
   */
  @Test
  public void testNullableNullExtension() {
    // the following field members are used because they're all nullable
    StructBuilder builder = new StructBuilder()
        .add(OrderedNumeric.ASCENDING)
        .add(OrderedString.ASCENDING);
    Struct shorter = builder.toStruct();
    Struct longer = builder
        // intentionally include a wrapped instance to test wrapper behavior.
        .add(new TerminatedWrapper<String>(OrderedString.ASCENDING, "/"))
        .add(OrderedNumeric.ASCENDING)
        .toStruct();

    PositionedByteRange buf1 = new SimplePositionedMutableByteRange(7);
    Object[] val1 = new Object[] { BigDecimal.ONE, "foo" }; // => 2 bytes + 5 bytes
    assertEquals("Encoding shorter value wrote a surprising number of bytes.",
      buf1.getLength(), shorter.encode(buf1, val1));
    int shortLen = buf1.getLength();

    // test iterator
    buf1.setPosition(0);
    StructIterator it = longer.iterator(buf1);
    it.skip();
    it.skip();
    assertEquals("Position should be at end. Broken test.", buf1.getLength(), buf1.getPosition());
    assertEquals("Failed to skip null element with extended struct.", 0, it.skip());
    assertEquals("Failed to skip null element with extended struct.", 0, it.skip());

    buf1.setPosition(0);
    it = longer.iterator(buf1);
    assertEquals(BigDecimal.ONE, it.next());
    assertEquals("foo", it.next());
    assertEquals("Position should be at end. Broken test.", buf1.getLength(), buf1.getPosition());
    assertNull("Failed to skip null element with extended struct.", it.next());
    assertNull("Failed to skip null element with extended struct.", it.next());

    // test Struct
    buf1.setPosition(0);
    assertArrayEquals("Simple struct decoding is broken.", val1, shorter.decode(buf1));

    buf1.setPosition(0);
    assertArrayEquals("Decoding short value with extended struct should append null elements.",
      Arrays.copyOf(val1, 4), longer.decode(buf1));

    // test omission of trailing members
    PositionedByteRange buf2 = new SimplePositionedMutableByteRange(7);
    buf1.setPosition(0);
    assertEquals(
      "Encoding a short value with extended struct should have same result as using short struct.",
      shortLen, longer.encode(buf2, val1));
    assertArrayEquals(
      "Encoding a short value with extended struct should have same result as using short struct",
      buf1.getBytes(), buf2.getBytes());

    // test null trailing members
    // all fields are nullable, so nothing should hit the buffer.
    val1 = new Object[] { null, null, null, null }; // => 0 bytes
    buf1.set(0);
    buf2.set(0);
    assertEquals("Encoding null-truncated value wrote a surprising number of bytes.",
      buf1.getLength(), longer.encode(buf1, new Object[0]));
    assertEquals("Encoding null-extended value wrote a surprising number of bytes.",
      buf1.getLength(), longer.encode(buf1, val1));
    assertArrayEquals("Encoded unexpected result.", buf1.getBytes(), buf2.getBytes());
    assertArrayEquals("Decoded unexpected result.", val1, longer.decode(buf2));

    // all fields are nullable, so only 1 should hit the buffer.
    Object[] val2 = new Object[] { BigDecimal.ONE, null, null, null }; // => 2 bytes
    buf1.set(2);
    buf2.set(2);
    assertEquals("Encoding null-truncated value wrote a surprising number of bytes.",
      buf1.getLength(), longer.encode(buf1, Arrays.copyOf(val2, 1)));
    assertEquals("Encoding null-extended value wrote a surprising number of bytes.",
      buf2.getLength(), longer.encode(buf2, val2));
    assertArrayEquals("Encoded unexpected result.", buf1.getBytes(), buf2.getBytes());
    buf2.setPosition(0);
    assertArrayEquals("Decoded unexpected result.", val2, longer.decode(buf2));

    // all fields are nullable, so only 1, null, "foo" should hit the buffer.
    // => 2 bytes + 1 byte + 6 bytes
    Object[] val3 = new Object[] { BigDecimal.ONE, null, "foo", null };
    buf1.set(9);
    buf2.set(9);
    assertEquals("Encoding null-truncated value wrote a surprising number of bytes.",
      buf1.getLength(), longer.encode(buf1, Arrays.copyOf(val3, 3)));
    assertEquals("Encoding null-extended value wrote a surprising number of bytes.",
      buf2.getLength(), longer.encode(buf2, val3));
    assertArrayEquals("Encoded unexpected result.", buf1.getBytes(), buf2.getBytes());
    buf2.setPosition(0);
    assertArrayEquals("Decoded unexpected result.", val3, longer.decode(buf2));
  }
}
