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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestOrderedBytes {

  // integer constants for testing Numeric code paths
  static final Long[] I_VALS =
    { 0L, 1L, 10L, 99L, 100L, 1234L, 9999L, 10000L, 10001L, 12345L, 123450L, Long.MAX_VALUE,
      -1L, -10L, -99L, -100L, -123L, -999L, -10000L, -10001L, -12345L, -123450L, Long.MIN_VALUE };
  static final int[] I_LENGTHS =
    { 1, 2, 2, 2, 2, 3, 3, 2, 4, 4, 4, 11, 2, 2, 2, 2, 3, 3, 2, 4, 4, 4, 11 };

  // real constants for testing Numeric code paths
  static final Double[] D_VALS =
    { 0.0, 0.00123, 0.0123, 0.123, 1.0, 10.0, 12.345, 99.0, 99.01, 99.0001, 100.0, 100.01,
      100.1, 1234.0, 1234.5, 9999.0, 9999.000001, 9999.000009, 9999.00001, 9999.00009,
      9999.000099, 9999.0001, 9999.001, 9999.01, 9999.1, 10000.0, 10001.0, 12345.0, 123450.0,
      Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN, Double.MAX_VALUE,
      -0.00123, -0.0123, -0.123, -1.0, -10.0, -12.345, -99.0, -99.01, -99.0001, -100.0, -100.01,
      -100.1, -1234.0, -1234.5, -9999.0, -9999.000001, -9999.000009, -9999.00001, -9999.00009,
      -9999.000099, -9999.0001, -9999.001, -9999.01, -9999.1, -10000.0, -10001.0, -12345.0,
      -123450.0 };
  static final int[] D_LENGTHS =
    { 1, 4, 4, 4, 2, 2, 4, 2, 3, 4, 2, 4,
      4, 3, 4, 3, 6, 6, 6, 6,
      6, 5, 5, 4, 4, 2, 4, 4, 4,
      1, 1, 1, 11,
      4, 4, 4, 2, 2, 4, 2, 3, 4, 2, 4,
      4, 3, 4, 3, 6, 6, 6, 6,
      6, 5, 5, 4, 4, 2, 4, 4,
      4 };

  // fill in other gaps in Numeric code paths
  static final BigDecimal[] BD_VALS =
    { null, BigDecimal.valueOf(Long.MAX_VALUE), BigDecimal.valueOf(Long.MIN_VALUE),
      BigDecimal.valueOf(Double.MAX_VALUE), BigDecimal.valueOf(Double.MIN_VALUE),
      BigDecimal.valueOf(Long.MAX_VALUE).multiply(BigDecimal.valueOf(100)) };
  static final int[] BD_LENGTHS =
    { 1, 11, 11, 11, 4, 12 };

  /*
   * This is the smallest difference between two doubles in D_VALS
   */
  static final double MIN_EPSILON = 0.000001;

  /**
   * Expected lengths of equivalent values should match
   */
  @Test
  public void testVerifyTestIntegrity() {
    for (int i = 0; i < I_VALS.length; i++) {
      for (int d = 0; d < D_VALS.length; d++) {
        if (Math.abs(I_VALS[i] - D_VALS[d]) < MIN_EPSILON) {
          assertEquals(
            "Test inconsistency detected: expected lengths for " + I_VALS[i] + " do not match.",
            I_LENGTHS[i], D_LENGTHS[d]);
        }
      }
    }
  }

  /**
   * Tests the variable uint64 encoding.
   * <p>
   * Building sqlite4 with -DVARINT_TOOL provides this reference:<br />
   * <code>$ ./varint_tool 240 2287 67823 16777215 4294967295 1099511627775
   *   281474976710655 72057594037927935 18446744073709551615<br />
   * 240 = f0<br />
   * 2287 = f8ff<br />
   * 67823 = f9ffff<br />
   * 16777215 = faffffff<br />
   * 4294967295 = fbffffffff<br />
   * 1099511627775 = fcffffffffff<br />
   * 281474976710655 = fdffffffffffff<br />
   * 72057594037927935 = feffffffffffffff<br />
   * 9223372036854775807 = ff7fffffffffffffff (Long.MAX_VAL)<br />
   * 9223372036854775808 = ff8000000000000000 (Long.MIN_VAL)<br />
   * 18446744073709551615 = ffffffffffffffffff<br /></code>
   * </p>
   */
  @Test
  public void testVaruint64Boundaries() {
    long vals[] =
        { 239L, 240L, 2286L, 2287L, 67822L, 67823L, 16777214L, 16777215L, 4294967294L, 4294967295L,
          1099511627774L, 1099511627775L, 281474976710654L, 281474976710655L, 72057594037927934L,
          72057594037927935L, Long.MAX_VALUE - 1, Long.MAX_VALUE, Long.MIN_VALUE + 1,
          Long.MIN_VALUE, -2L, -1L };
    int lens[] = { 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 9, 9, 9, 9 };
    assertEquals("Broken test!", vals.length, lens.length);

    /*
     * assert encoded values match decoded values. encode into target buffer
     * starting at an offset to detect over/underflow conditions.
     */
    for (boolean comp : new boolean[] { true, false }) {
      for (int i = 0; i < vals.length; i++) {
        // allocate a buffer 2-bytes larger than necessary and place our range over the center.
        byte[] a = new byte[lens[i] + 2];
        PositionedByteRange buf = new SimplePositionedMutableByteRange(a, 1, lens[i]);

        // verify encode
        assertEquals("Surprising return value.",
          lens[i], OrderedBytes.putVaruint64(buf, vals[i], comp));
        assertEquals("Surprising serialized length.", lens[i], buf.getPosition());
        assertEquals("Buffer underflow.", 0, a[0]);
        assertEquals("Buffer overflow.", 0, a[a.length - 1]);

        // verify skip
        buf.setPosition(0);
        assertEquals("Surprising return value.",
          lens[i], OrderedBytes.skipVaruint64(buf, comp));
        assertEquals("Did not skip enough bytes.", lens[i], buf.getPosition());

        // verify decode
        buf.setPosition(0);
        assertEquals("Deserialization failed.", vals[i], OrderedBytes.getVaruint64(buf, comp));
        assertEquals("Did not consume enough bytes.", lens[i], buf.getPosition());
      }
    }
  }

  /**
   * Test integer encoding. Example input values come from reference wiki
   * page.
   */
  @Test
  public void testNumericInt() {
    /*
     * assert encoded values match decoded values. encode into target buffer
     * starting at an offset to detect over/underflow conditions.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      for (int i = 0; i < I_VALS.length; i++) {
        // allocate a buffer 3-bytes larger than necessary to detect over/underflow
        byte[] a = new byte[I_LENGTHS[i] + 3];
        PositionedByteRange buf1 = new SimplePositionedMutableByteRange(a, 1, I_LENGTHS[i] + 1);
        buf1.setPosition(1);

        // verify encode
        assertEquals("Surprising return value.",
          I_LENGTHS[i], OrderedBytes.encodeNumeric(buf1, I_VALS[i], ord));
        assertEquals("Broken test: serialization did not consume entire buffer.",
          buf1.getLength(), buf1.getPosition());
        assertEquals("Surprising serialized length.", I_LENGTHS[i], buf1.getPosition() - 1);
        assertEquals("Buffer underflow.", 0, a[0]);
        assertEquals("Buffer underflow.", 0, a[1]);
        assertEquals("Buffer overflow.", 0, a[a.length - 1]);

        // verify skip
        buf1.setPosition(1);
        assertEquals("Surprising return value.", I_LENGTHS[i], OrderedBytes.skip(buf1));
        assertEquals("Did not skip enough bytes.", I_LENGTHS[i], buf1.getPosition() - 1);

        // verify decode
        buf1.setPosition(1);
        assertEquals("Deserialization failed.",
          I_VALS[i].longValue(), OrderedBytes.decodeNumericAsLong(buf1));
        assertEquals("Did not consume enough bytes.", I_LENGTHS[i], buf1.getPosition() - 1);
      }
    }

    /*
     * assert natural sort order is preserved by the codec.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      byte[][] encoded = new byte[I_VALS.length][];
      PositionedByteRange pbr = new SimplePositionedMutableByteRange();
      for (int i = 0; i < I_VALS.length; i++) {
        encoded[i] = new byte[I_LENGTHS[i]];
        OrderedBytes.encodeNumeric(pbr.set(encoded[i]), I_VALS[i], ord);
      }

      Arrays.sort(encoded, Bytes.BYTES_COMPARATOR);
      Long[] sortedVals = Arrays.copyOf(I_VALS, I_VALS.length);
      if (ord == Order.ASCENDING) Arrays.sort(sortedVals);
      else Arrays.sort(sortedVals, Collections.reverseOrder());

      for (int i = 0; i < sortedVals.length; i++) {
        pbr.set(encoded[i]);
        long decoded = OrderedBytes.decodeNumericAsLong(pbr);
        assertEquals(
          String.format(
            "Encoded representations do not preserve natural order: <%s>, <%s>, %s",
            sortedVals[i], decoded, ord),
          sortedVals[i].longValue(), decoded);
      }
    }
  }

  /**
   * Test real encoding. Example input values come from reference wiki page.
   */
  @Test
  public void testNumericReal() {
    /*
     * assert encoded values match decoded values. encode into target buffer
     * starting at an offset to detect over/underflow conditions.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      for (int i = 0; i < D_VALS.length; i++) {
        // allocate a buffer 3-bytes larger than necessary to detect over/underflow
        byte[] a = new byte[D_LENGTHS[i] + 3];
        PositionedByteRange buf1 = new SimplePositionedMutableByteRange(a, 1, D_LENGTHS[i] + 1);
        buf1.setPosition(1);

        // verify encode
        assertEquals("Surprising return value.",
          D_LENGTHS[i], OrderedBytes.encodeNumeric(buf1, D_VALS[i], ord));
        assertEquals("Broken test: serialization did not consume entire buffer.",
          buf1.getLength(), buf1.getPosition());
        assertEquals("Surprising serialized length.", D_LENGTHS[i], buf1.getPosition() - 1);
        assertEquals("Buffer underflow.", 0, a[0]);
        assertEquals("Buffer underflow.", 0, a[1]);
        assertEquals("Buffer overflow.", 0, a[a.length - 1]);

        // verify skip
        buf1.setPosition(1);
        assertEquals("Surprising return value.", D_LENGTHS[i], OrderedBytes.skip(buf1));
        assertEquals("Did not skip enough bytes.", D_LENGTHS[i], buf1.getPosition() - 1);

        // verify decode
        buf1.setPosition(1);
        assertEquals("Deserialization failed.",
          D_VALS[i].doubleValue(), OrderedBytes.decodeNumericAsDouble(buf1), MIN_EPSILON);
        assertEquals("Did not consume enough bytes.", D_LENGTHS[i], buf1.getPosition() - 1);
      }
    }

    /*
     * assert natural sort order is preserved by the codec.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      byte[][] encoded = new byte[D_VALS.length][];
      PositionedByteRange pbr = new SimplePositionedMutableByteRange();
      for (int i = 0; i < D_VALS.length; i++) {
        encoded[i] = new byte[D_LENGTHS[i]];
        OrderedBytes.encodeNumeric(pbr.set(encoded[i]), D_VALS[i], ord);
      }

      Arrays.sort(encoded, Bytes.BYTES_COMPARATOR);
      Double[] sortedVals = Arrays.copyOf(D_VALS, D_VALS.length);
      if (ord == Order.ASCENDING) Arrays.sort(sortedVals);
      else Arrays.sort(sortedVals, Collections.reverseOrder());

      for (int i = 0; i < sortedVals.length; i++) {
        pbr.set(encoded[i]);
        double decoded = OrderedBytes.decodeNumericAsDouble(pbr);
        assertEquals(
          String.format(
            "Encoded representations do not preserve natural order: <%s>, <%s>, %s",
            sortedVals[i], decoded, ord),
            sortedVals[i].doubleValue(), decoded, MIN_EPSILON);
      }
    }
  }

  /**
   * Fill gaps in Numeric encoding testing.
   */
  @Test
  public void testNumericOther() {
    /*
     * assert encoded values match decoded values. encode into target buffer
     * starting at an offset to detect over/underflow conditions.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      for (int i = 0; i < BD_VALS.length; i++) {
        // allocate a buffer 3-bytes larger than necessary to detect over/underflow
        byte[] a = new byte[BD_LENGTHS[i] + 3];
        PositionedByteRange buf1 = new SimplePositionedMutableByteRange(a, 1, BD_LENGTHS[i] + 1);
        buf1.setPosition(1);

        // verify encode
        assertEquals("Surprising return value.",
          BD_LENGTHS[i], OrderedBytes.encodeNumeric(buf1, BD_VALS[i], ord));
        assertEquals("Broken test: serialization did not consume entire buffer.",
          buf1.getLength(), buf1.getPosition());
        assertEquals("Surprising serialized length.", BD_LENGTHS[i], buf1.getPosition() - 1);
        assertEquals("Buffer underflow.", 0, a[0]);
        assertEquals("Buffer underflow.", 0, a[1]);
        assertEquals("Buffer overflow.", 0, a[a.length - 1]);

        // verify skip
        buf1.setPosition(1);
        assertEquals("Surprising return value.", BD_LENGTHS[i], OrderedBytes.skip(buf1));
        assertEquals("Did not skip enough bytes.", BD_LENGTHS[i], buf1.getPosition() - 1);

        // verify decode
        buf1.setPosition(1);
        BigDecimal decoded = OrderedBytes.decodeNumericAsBigDecimal(buf1);
        if (null == BD_VALS[i]) {
          assertEquals(BD_VALS[i], decoded);
        } else {
          assertEquals("Deserialization failed.", 0, BD_VALS[i].compareTo(decoded));
        }
        assertEquals("Did not consume enough bytes.", BD_LENGTHS[i], buf1.getPosition() - 1);
      }
    }
  }

  /**
   * Verify Real and Int encodings are compatible.
   */
  @Test
  public void testNumericIntRealCompatibility() {
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      for (int i = 0; i < I_VALS.length; i++) {
        // verify primitives
        PositionedByteRange pbri = new SimplePositionedMutableByteRange(I_LENGTHS[i]);
        PositionedByteRange pbrr = new SimplePositionedMutableByteRange(I_LENGTHS[i]);
        OrderedBytes.encodeNumeric(pbri, I_VALS[i], ord);
        OrderedBytes.encodeNumeric(pbrr, I_VALS[i], ord);
        assertArrayEquals("Integer and real encodings differ.", pbri.getBytes(), pbrr.getBytes());
        pbri.setPosition(0);
        pbrr.setPosition(0);
        assertEquals((long) I_VALS[i], OrderedBytes.decodeNumericAsLong(pbri));
        assertEquals((long) I_VALS[i], (long) OrderedBytes.decodeNumericAsDouble(pbrr));

        // verify BigDecimal for Real encoding
        BigDecimal bd = BigDecimal.valueOf(I_VALS[i]);
        PositionedByteRange pbrbd = new SimplePositionedMutableByteRange(I_LENGTHS[i]);
        OrderedBytes.encodeNumeric(pbrbd, bd, ord);
        assertArrayEquals("Integer and BigDecimal encodings differ.",
          pbri.getBytes(), pbrbd.getBytes());
        pbri.setPosition(0);
        assertEquals("Value not preserved when decoding as Long",
          0, bd.compareTo(BigDecimal.valueOf(OrderedBytes.decodeNumericAsLong(pbri))));
      }
    }
  }

  /**
   * Test int8 encoding.
   */
  @Test
  public void testInt8() {
    Byte[] vals =
      { Byte.MIN_VALUE, Byte.MIN_VALUE / 2, 0, Byte.MAX_VALUE / 2, Byte.MAX_VALUE };

    /*
     * assert encoded values match decoded values. encode into target buffer
     * starting at an offset to detect over/underflow conditions.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      for (int i = 0; i < vals.length; i++) {
        // allocate a buffer 3-bytes larger than necessary to detect over/underflow
        byte[] a = new byte[2 + 3];
        PositionedByteRange buf1 = new SimplePositionedMutableByteRange(a, 1, 2 + 1);
        buf1.setPosition(1);

        // verify encode
        assertEquals("Surprising return value.",
          2, OrderedBytes.encodeInt8(buf1, vals[i], ord));
        assertEquals("Broken test: serialization did not consume entire buffer.",
          buf1.getLength(), buf1.getPosition());
        assertEquals("Surprising serialized length.", 2, buf1.getPosition() - 1);
        assertEquals("Buffer underflow.", 0, a[0]);
        assertEquals("Buffer underflow.", 0, a[1]);
        assertEquals("Buffer overflow.", 0, a[a.length - 1]);

        // verify skip
        buf1.setPosition(1);
        assertEquals("Surprising return value.", 2, OrderedBytes.skip(buf1));
        assertEquals("Did not skip enough bytes.", 2, buf1.getPosition() - 1);

        // verify decode
        buf1.setPosition(1);
        assertEquals("Deserialization failed.",
          vals[i].byteValue(), OrderedBytes.decodeInt8(buf1));
        assertEquals("Did not consume enough bytes.", 2, buf1.getPosition() - 1);
      }
    }

    /*
     * assert natural sort order is preserved by the codec.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      byte[][] encoded = new byte[vals.length][2];
      PositionedByteRange pbr = new SimplePositionedMutableByteRange();
      for (int i = 0; i < vals.length; i++) {
        OrderedBytes.encodeInt8(pbr.set(encoded[i]), vals[i], ord);
      }

      Arrays.sort(encoded, Bytes.BYTES_COMPARATOR);
      Byte[] sortedVals = Arrays.copyOf(vals, vals.length);
      if (ord == Order.ASCENDING) Arrays.sort(sortedVals);
      else Arrays.sort(sortedVals, Collections.reverseOrder());

      for (int i = 0; i < sortedVals.length; i++) {
        int decoded = OrderedBytes.decodeInt8(pbr.set(encoded[i]));
        assertEquals(
          String.format(
            "Encoded representations do not preserve natural order: <%s>, <%s>, %s",
            sortedVals[i], decoded, ord),
            sortedVals[i].byteValue(), decoded);
      }
    }
  }

  /**
   * Test int16 encoding.
   */
  @Test
  public void testInt16() {
    Short[] vals =
      { Short.MIN_VALUE, Short.MIN_VALUE / 2, 0, Short.MAX_VALUE / 2, Short.MAX_VALUE };

    /*
     * assert encoded values match decoded values. encode into target buffer
     * starting at an offset to detect over/underflow conditions.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      for (int i = 0; i < vals.length; i++) {
        // allocate a buffer 3-bytes larger than necessary to detect over/underflow
        byte[] a = new byte[3 + 3];
        PositionedByteRange buf1 = new SimplePositionedMutableByteRange(a, 1, 3 + 1);
        buf1.setPosition(1);

        // verify encode
        assertEquals("Surprising return value.",
          3, OrderedBytes.encodeInt16(buf1, vals[i], ord));
        assertEquals("Broken test: serialization did not consume entire buffer.",
          buf1.getLength(), buf1.getPosition());
        assertEquals("Surprising serialized length.", 3, buf1.getPosition() - 1);
        assertEquals("Buffer underflow.", 0, a[0]);
        assertEquals("Buffer underflow.", 0, a[1]);
        assertEquals("Buffer overflow.", 0, a[a.length - 1]);

        // verify skip
        buf1.setPosition(1);
        assertEquals("Surprising return value.", 3, OrderedBytes.skip(buf1));
        assertEquals("Did not skip enough bytes.", 3, buf1.getPosition() - 1);

        // verify decode
        buf1.setPosition(1);
        assertEquals("Deserialization failed.",
          vals[i].shortValue(), OrderedBytes.decodeInt16(buf1));
        assertEquals("Did not consume enough bytes.", 3, buf1.getPosition() - 1);
      }
    }

    /*
     * assert natural sort order is preserved by the codec.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      byte[][] encoded = new byte[vals.length][3];
      PositionedByteRange pbr = new SimplePositionedMutableByteRange();
      for (int i = 0; i < vals.length; i++) {
        OrderedBytes.encodeInt16(pbr.set(encoded[i]), vals[i], ord);
      }

      Arrays.sort(encoded, Bytes.BYTES_COMPARATOR);
      Short[] sortedVals = Arrays.copyOf(vals, vals.length);
      if (ord == Order.ASCENDING) Arrays.sort(sortedVals);
      else Arrays.sort(sortedVals, Collections.reverseOrder());

      for (int i = 0; i < sortedVals.length; i++) {
        int decoded = OrderedBytes.decodeInt16(pbr.set(encoded[i]));
        assertEquals(
          String.format(
            "Encoded representations do not preserve natural order: <%s>, <%s>, %s",
            sortedVals[i], decoded, ord),
            sortedVals[i].shortValue(), decoded);
      }
    }
  }

  /**
   * Test int32 encoding.
   */
  @Test
  public void testInt32() {
    Integer[] vals =
      { Integer.MIN_VALUE, Integer.MIN_VALUE / 2, 0, Integer.MAX_VALUE / 2, Integer.MAX_VALUE };

    /*
     * assert encoded values match decoded values. encode into target buffer
     * starting at an offset to detect over/underflow conditions.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      for (int i = 0; i < vals.length; i++) {
        // allocate a buffer 3-bytes larger than necessary to detect over/underflow
        byte[] a = new byte[5 + 3];
        PositionedByteRange buf1 = new SimplePositionedMutableByteRange(a, 1, 5 + 1);
        buf1.setPosition(1);

        // verify encode
        assertEquals("Surprising return value.",
          5, OrderedBytes.encodeInt32(buf1, vals[i], ord));
        assertEquals("Broken test: serialization did not consume entire buffer.",
          buf1.getLength(), buf1.getPosition());
        assertEquals("Surprising serialized length.", 5, buf1.getPosition() - 1);
        assertEquals("Buffer underflow.", 0, a[0]);
        assertEquals("Buffer underflow.", 0, a[1]);
        assertEquals("Buffer overflow.", 0, a[a.length - 1]);

        // verify skip
        buf1.setPosition(1);
        assertEquals("Surprising return value.", 5, OrderedBytes.skip(buf1));
        assertEquals("Did not skip enough bytes.", 5, buf1.getPosition() - 1);

        // verify decode
        buf1.setPosition(1);
        assertEquals("Deserialization failed.",
          vals[i].intValue(), OrderedBytes.decodeInt32(buf1));
        assertEquals("Did not consume enough bytes.", 5, buf1.getPosition() - 1);
      }
    }

    /*
     * assert natural sort order is preserved by the codec.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      byte[][] encoded = new byte[vals.length][5];
      PositionedByteRange pbr = new SimplePositionedMutableByteRange();
      for (int i = 0; i < vals.length; i++) {
        OrderedBytes.encodeInt32(pbr.set(encoded[i]), vals[i], ord);
      }

      Arrays.sort(encoded, Bytes.BYTES_COMPARATOR);
      Integer[] sortedVals = Arrays.copyOf(vals, vals.length);
      if (ord == Order.ASCENDING) Arrays.sort(sortedVals);
      else Arrays.sort(sortedVals, Collections.reverseOrder());

      for (int i = 0; i < sortedVals.length; i++) {
        int decoded = OrderedBytes.decodeInt32(pbr.set(encoded[i]));
        assertEquals(
          String.format(
            "Encoded representations do not preserve natural order: <%s>, <%s>, %s",
            sortedVals[i], decoded, ord),
            sortedVals[i].intValue(), decoded);
      }
    }
  }

  /**
   * Test int64 encoding.
   */
  @Test
  public void testInt64() {
    Long[] vals = { Long.MIN_VALUE, Long.MIN_VALUE / 2, 0L, Long.MAX_VALUE / 2, Long.MAX_VALUE };

    /*
     * assert encoded values match decoded values. encode into target buffer
     * starting at an offset to detect over/underflow conditions.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      for (int i = 0; i < vals.length; i++) {
        // allocate a buffer 3-bytes larger than necessary to detect over/underflow
        byte[] a = new byte[9 + 3];
        PositionedByteRange buf1 = new SimplePositionedMutableByteRange(a, 1, 9 + 1);
        buf1.setPosition(1);

        // verify encode
        assertEquals("Surprising return value.",
          9, OrderedBytes.encodeInt64(buf1, vals[i], ord));
        assertEquals("Broken test: serialization did not consume entire buffer.",
          buf1.getLength(), buf1.getPosition());
        assertEquals("Surprising serialized length.", 9, buf1.getPosition() - 1);
        assertEquals("Buffer underflow.", 0, a[0]);
        assertEquals("Buffer underflow.", 0, a[1]);
        assertEquals("Buffer overflow.", 0, a[a.length - 1]);

        // verify skip
        buf1.setPosition(1);
        assertEquals("Surprising return value.", 9, OrderedBytes.skip(buf1));
        assertEquals("Did not skip enough bytes.", 9, buf1.getPosition() - 1);

        // verify decode
        buf1.setPosition(1);
        assertEquals("Deserialization failed.",
          vals[i].longValue(), OrderedBytes.decodeInt64(buf1));
        assertEquals("Did not consume enough bytes.", 9, buf1.getPosition() - 1);
      }
    }

    /*
     * assert natural sort order is preserved by the codec.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      byte[][] encoded = new byte[vals.length][9];
      PositionedByteRange pbr = new SimplePositionedMutableByteRange();
      for (int i = 0; i < vals.length; i++) {
        OrderedBytes.encodeInt64(pbr.set(encoded[i]), vals[i], ord);
      }

      Arrays.sort(encoded, Bytes.BYTES_COMPARATOR);
      Long[] sortedVals = Arrays.copyOf(vals, vals.length);
      if (ord == Order.ASCENDING) Arrays.sort(sortedVals);
      else Arrays.sort(sortedVals, Collections.reverseOrder());

      for (int i = 0; i < sortedVals.length; i++) {
        long decoded = OrderedBytes.decodeInt64(pbr.set(encoded[i]));
        assertEquals(
          String.format(
            "Encoded representations do not preserve natural order: <%s>, <%s>, %s",
            sortedVals[i], decoded, ord),
            sortedVals[i].longValue(), decoded);
      }
    }
  }

  /**
   * Test float32 encoding.
   */
  @Test
  public void testFloat32() {
    Float[] vals =
      { Float.MIN_VALUE, Float.MIN_VALUE + 1.0f, 0.0f, Float.MAX_VALUE / 2.0f, Float.MAX_VALUE };

    /*
     * assert encoded values match decoded values. encode into target buffer
     * starting at an offset to detect over/underflow conditions.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      for (int i = 0; i < vals.length; i++) {
        // allocate a buffer 3-bytes larger than necessary to detect over/underflow
        byte[] a = new byte[5 + 3];
        PositionedByteRange buf1 = new SimplePositionedMutableByteRange(a, 1, 5 + 1);
        buf1.setPosition(1);

        // verify encode
        assertEquals("Surprising return value.",
          5, OrderedBytes.encodeFloat32(buf1, vals[i], ord));
        assertEquals("Broken test: serialization did not consume entire buffer.",
          buf1.getLength(), buf1.getPosition());
        assertEquals("Surprising serialized length.", 5, buf1.getPosition() - 1);
        assertEquals("Buffer underflow.", 0, a[0]);
        assertEquals("Buffer underflow.", 0, a[1]);
        assertEquals("Buffer overflow.", 0, a[a.length - 1]);

        // verify skip
        buf1.setPosition(1);
        assertEquals("Surprising return value.", 5, OrderedBytes.skip(buf1));
        assertEquals("Did not skip enough bytes.", 5, buf1.getPosition() - 1);

        // verify decode
        buf1.setPosition(1);
        assertEquals("Deserialization failed.",
          Float.floatToIntBits(vals[i].floatValue()),
          Float.floatToIntBits(OrderedBytes.decodeFloat32(buf1)));
        assertEquals("Did not consume enough bytes.", 5, buf1.getPosition() - 1);
      }
    }

    /*
     * assert natural sort order is preserved by the codec.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      byte[][] encoded = new byte[vals.length][5];
      PositionedByteRange pbr = new SimplePositionedMutableByteRange();
      for (int i = 0; i < vals.length; i++) {
        OrderedBytes.encodeFloat32(pbr.set(encoded[i]), vals[i], ord);
      }

      Arrays.sort(encoded, Bytes.BYTES_COMPARATOR);
      Float[] sortedVals = Arrays.copyOf(vals, vals.length);
      if (ord == Order.ASCENDING) Arrays.sort(sortedVals);
      else Arrays.sort(sortedVals, Collections.reverseOrder());

      for (int i = 0; i < sortedVals.length; i++) {
        float decoded = OrderedBytes.decodeFloat32(pbr.set(encoded[i]));
        assertEquals(
          String.format(
            "Encoded representations do not preserve natural order: <%s>, <%s>, %s",
            sortedVals[i], decoded, ord),
            Float.floatToIntBits(sortedVals[i].floatValue()),
            Float.floatToIntBits(decoded));
      }
    }
  }

  /**
   * Test float64 encoding.
   */
  @Test
  public void testFloat64() {
    Double[] vals =
      { Double.MIN_VALUE, Double.MIN_VALUE + 1.0, 0.0, Double.MAX_VALUE / 2.0, Double.MAX_VALUE };

    /*
     * assert encoded values match decoded values. encode into target buffer
     * starting at an offset to detect over/underflow conditions.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      for (int i = 0; i < vals.length; i++) {
        // allocate a buffer 3-bytes larger than necessary to detect over/underflow
        byte[] a = new byte[9 + 3];
        PositionedByteRange buf1 = new SimplePositionedMutableByteRange(a, 1, 9 + 1);
        buf1.setPosition(1);

        // verify encode
        assertEquals("Surprising return value.",
          9, OrderedBytes.encodeFloat64(buf1, vals[i], ord));
        assertEquals("Broken test: serialization did not consume entire buffer.",
          buf1.getLength(), buf1.getPosition());
        assertEquals("Surprising serialized length.", 9, buf1.getPosition() - 1);
        assertEquals("Buffer underflow.", 0, a[0]);
        assertEquals("Buffer underflow.", 0, a[1]);
        assertEquals("Buffer overflow.", 0, a[a.length - 1]);

        // verify skip
        buf1.setPosition(1);
        assertEquals("Surprising return value.", 9, OrderedBytes.skip(buf1));
        assertEquals("Did not skip enough bytes.", 9, buf1.getPosition() - 1);

        // verify decode
        buf1.setPosition(1);
        assertEquals("Deserialization failed.",
          Double.doubleToLongBits(vals[i].doubleValue()),
          Double.doubleToLongBits(OrderedBytes.decodeFloat64(buf1)));
        assertEquals("Did not consume enough bytes.", 9, buf1.getPosition() - 1);
      }
    }

    /*
     * assert natural sort order is preserved by the codec.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      byte[][] encoded = new byte[vals.length][9];
      PositionedByteRange pbr = new SimplePositionedMutableByteRange();
      for (int i = 0; i < vals.length; i++) {
        OrderedBytes.encodeFloat64(pbr.set(encoded[i]), vals[i], ord);
      }

      Arrays.sort(encoded, Bytes.BYTES_COMPARATOR);
      Double[] sortedVals = Arrays.copyOf(vals, vals.length);
      if (ord == Order.ASCENDING) Arrays.sort(sortedVals);
      else Arrays.sort(sortedVals, Collections.reverseOrder());

      for (int i = 0; i < sortedVals.length; i++) {
        double decoded = OrderedBytes.decodeFloat64(pbr.set(encoded[i]));
        assertEquals(
          String.format(
            "Encoded representations do not preserve natural order: <%s>, <%s>, %s",
            sortedVals[i], decoded, ord),
            Double.doubleToLongBits(sortedVals[i].doubleValue()),
            Double.doubleToLongBits(decoded));
      }
    }
  }

  /**
   * Test string encoding.
   */
  @Test
  public void testString() {
    String[] vals = { "foo", "baaaar", "bazz" };
    int expectedLengths[] = { 5, 8, 6 };

    /*
     * assert encoded values match decoded values. encode into target buffer
     * starting at an offset to detect over/underflow conditions.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      for (int i = 0; i < vals.length; i++) {
        // allocate a buffer 3-bytes larger than necessary to detect over/underflow
        byte[] a = new byte[expectedLengths[i] + 3];
        PositionedByteRange buf1 = new SimplePositionedMutableByteRange(a, 1,
            expectedLengths[i] + 1);
        buf1.setPosition(1);

        // verify encode
        assertEquals("Surprising return value.",
          expectedLengths[i], OrderedBytes.encodeString(buf1, vals[i], ord));
        assertEquals("Broken test: serialization did not consume entire buffer.",
          buf1.getLength(), buf1.getPosition());
        assertEquals("Surprising serialized length.", expectedLengths[i], buf1.getPosition() - 1);
        assertEquals("Buffer underflow.", 0, a[0]);
        assertEquals("Buffer underflow.", 0, a[1]);
        assertEquals("Buffer overflow.", 0, a[a.length - 1]);

        // verify skip
        buf1.setPosition(1);
        assertEquals("Surprising return value.", expectedLengths[i], OrderedBytes.skip(buf1));
        assertEquals("Did not skip enough bytes.", expectedLengths[i], buf1.getPosition() - 1);

        // verify decode
        buf1.setPosition(1);
        assertEquals("Deserialization failed.", vals[i], OrderedBytes.decodeString(buf1));
        assertEquals("Did not consume enough bytes.", expectedLengths[i], buf1.getPosition() - 1);
      }
    }

    /*
     * assert natural sort order is preserved by the codec.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      byte[][] encoded = new byte[vals.length][];
      PositionedByteRange pbr = new SimplePositionedMutableByteRange();
      for (int i = 0; i < vals.length; i++) {
        encoded[i] = new byte[expectedLengths[i]];
        OrderedBytes.encodeString(pbr.set(encoded[i]), vals[i], ord);
      }

      Arrays.sort(encoded, Bytes.BYTES_COMPARATOR);
      String[] sortedVals = Arrays.copyOf(vals, vals.length);
      if (ord == Order.ASCENDING) Arrays.sort(sortedVals);
      else Arrays.sort(sortedVals, Collections.reverseOrder());

      for (int i = 0; i < sortedVals.length; i++) {
        pbr.set(encoded[i]);
        String decoded = OrderedBytes.decodeString(pbr);
        assertEquals(
          String.format(
            "Encoded representations do not preserve natural order: <%s>, <%s>, %s",
            sortedVals[i], decoded, ord),
          sortedVals[i], decoded);
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testStringNoNullChars() {
    PositionedByteRange buff = new SimplePositionedMutableByteRange(3);
    OrderedBytes.encodeString(buff, "\u0000", Order.ASCENDING);
  }

  /**
   * Test length estimation algorithms for BlobVar encoding. Does not cover
   * 0-length input case properly.
   */
  @Test
  public void testBlobVarLencodedLength() {
    int[][] values = {
        /* decoded length, encoded length
         * ceil((n bytes * 8 bits/input byte) / 7 bits/encoded byte) + 1 header
         */
        { 1, 3 }, { 2, 4 }, { 3, 5 }, { 4, 6 },
        { 5, 7 }, { 6, 8 }, { 7, 9 }, { 8, 11 }
      };

    for (int[] pair : values) {
      assertEquals(pair[1], OrderedBytes.blobVarEncodedLength(pair[0]));
      assertEquals(pair[0], OrderedBytes.blobVarDecodedLength(pair[1]));
    }
  }

  /**
   * Test BlobVar encoding.
   */
  @Test
  public void testBlobVar() {
    byte[][] vals =
        { "".getBytes(), "foo".getBytes(), "foobarbazbub".getBytes(),
          { (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa,
            (byte) 0xaa, /* 7 bytes of alternating bits; testing around HBASE-9893 */ },
          { (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa,
            (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa },
          { (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa,
            (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa,
            (byte) 0xaa, (byte) 0xaa, /* 14 bytes of alternating bits; testing around HBASE-9893 */ },
          { (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55,
            (byte) 0x55, /* 7 bytes of alternating bits; testing around HBASE-9893 */ },
          { (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55,
            (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55 },
          { (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55,
            (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55,
            (byte) 0x55, (byte) 0x55, /* 14 bytes of alternating bits; testing around HBASE-9893 */ },
          "1".getBytes(), "22".getBytes(), "333".getBytes(), "4444".getBytes(),
          "55555".getBytes(), "666666".getBytes(), "7777777".getBytes(), "88888888".getBytes()
        };

    /*
     * assert encoded values match decoded values. encode into target buffer
     * starting at an offset to detect over/underflow conditions.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      for (byte[] val : vals) {
        // allocate a buffer 3-bytes larger than necessary to detect over/underflow
        int expectedLen = OrderedBytes.blobVarEncodedLength(val.length);
        byte[] a = new byte[expectedLen + 3];
        PositionedByteRange buf1 = new SimplePositionedMutableByteRange(a, 1, expectedLen + 1);
        buf1.setPosition(1);

        // verify encode
        assertEquals("Surprising return value.",
          expectedLen, OrderedBytes.encodeBlobVar(buf1, val, ord));
        assertEquals("Broken test: serialization did not consume entire buffer.",
          buf1.getLength(), buf1.getPosition());
        assertEquals("Surprising serialized length.", expectedLen, buf1.getPosition() - 1);
        assertEquals("Buffer underflow.", 0, a[0]);
        assertEquals("Buffer underflow.", 0, a[1]);
        assertEquals("Buffer overflow.", 0, a[a.length - 1]);

        // verify skip
        buf1.setPosition(1);
        assertEquals("Surprising return value.", expectedLen, OrderedBytes.skip(buf1));
        assertEquals("Did not skip enough bytes.", expectedLen, buf1.getPosition() - 1);

        // verify decode
        buf1.setPosition(1);
        assertArrayEquals("Deserialization failed.", val, OrderedBytes.decodeBlobVar(buf1));
        assertEquals("Did not consume enough bytes.", expectedLen, buf1.getPosition() - 1);
      }
    }

    /*
     * assert natural sort order is preserved by the codec.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      byte[][] encoded = new byte[vals.length][];
      PositionedByteRange pbr = new SimplePositionedMutableByteRange();
      for (int i = 0; i < vals.length; i++) {
        encoded[i] = new byte[OrderedBytes.blobVarEncodedLength(vals[i].length)];
        OrderedBytes.encodeBlobVar(pbr.set(encoded[i]), vals[i], ord);
      }

      Arrays.sort(encoded, Bytes.BYTES_COMPARATOR);
      byte[][] sortedVals = Arrays.copyOf(vals, vals.length);
      if (ord == Order.ASCENDING) Arrays.sort(sortedVals, Bytes.BYTES_COMPARATOR);
      else Arrays.sort(sortedVals, Collections.reverseOrder(Bytes.BYTES_COMPARATOR));

      for (int i = 0; i < sortedVals.length; i++) {
        pbr.set(encoded[i]);
        byte[] decoded = OrderedBytes.decodeBlobVar(pbr);
        assertArrayEquals(
          String.format(
            "Encoded representations do not preserve natural order: <%s>, <%s>, %s",
            Arrays.toString(sortedVals[i]), Arrays.toString(decoded), ord),
          sortedVals[i], decoded);
      }
    }
  }

  /**
   * Test BlobCopy encoding.
   */
  @Test
  public void testBlobCopy() {
    byte[][] vals =
      { "".getBytes(), "foo".getBytes(), "foobarbazbub".getBytes(),
        { (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa,
          (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa, (byte) 0xaa },
        { (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55,
          (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55, (byte) 0x55 },
      };

    /*
     * assert encoded values match decoded values. encode into target buffer
     * starting at an offset to detect over/underflow conditions.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      for (byte[] val : vals) {
        // allocate a buffer 3-bytes larger than necessary to detect over/underflow
        int expectedLen = val.length + (Order.ASCENDING == ord ? 1 : 2);
        byte[] a = new byte[expectedLen + 3];
        PositionedByteRange buf1 = new SimplePositionedMutableByteRange(a, 1, expectedLen + 1);
        buf1.setPosition(1);

        // verify encode
        assertEquals("Surprising return value.",
          expectedLen, OrderedBytes.encodeBlobCopy(buf1, val, ord));
        assertEquals("Broken test: serialization did not consume entire buffer.",
          buf1.getLength(), buf1.getPosition());
        assertEquals("Surprising serialized length.", expectedLen, buf1.getPosition() - 1);
        assertEquals("Buffer underflow.", 0, a[0]);
        assertEquals("Buffer underflow.", 0, a[1]);
        assertEquals("Buffer overflow.", 0, a[a.length - 1]);

        // verify skip
        buf1.setPosition(1);
        assertEquals("Surprising return value.", expectedLen, OrderedBytes.skip(buf1));
        assertEquals("Did not skip enough bytes.", expectedLen, buf1.getPosition() - 1);

        // verify decode
        buf1.setPosition(1);
        assertArrayEquals("Deserialization failed.", val, OrderedBytes.decodeBlobCopy(buf1));
        assertEquals("Did not consume enough bytes.", expectedLen, buf1.getPosition() - 1);
      }
    }

    /*
     * assert natural sort order is preserved by the codec.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      byte[][] encoded = new byte[vals.length][];
      PositionedByteRange pbr = new SimplePositionedMutableByteRange();
      for (int i = 0; i < vals.length; i++) {
        encoded[i] = new byte[vals[i].length + (Order.ASCENDING == ord ? 1 : 2)];
        OrderedBytes.encodeBlobCopy(pbr.set(encoded[i]), vals[i], ord);
      }

      Arrays.sort(encoded, Bytes.BYTES_COMPARATOR);
      byte[][] sortedVals = Arrays.copyOf(vals, vals.length);
      if (ord == Order.ASCENDING) Arrays.sort(sortedVals, Bytes.BYTES_COMPARATOR);
      else Arrays.sort(sortedVals, Collections.reverseOrder(Bytes.BYTES_COMPARATOR));

      for (int i = 0; i < sortedVals.length; i++) {
        pbr.set(encoded[i]);
        byte[] decoded = OrderedBytes.decodeBlobCopy(pbr);
        assertArrayEquals(
          String.format(
            "Encoded representations do not preserve natural order: <%s>, <%s>, %s",
            Arrays.toString(sortedVals[i]), Arrays.toString(decoded), ord),
          sortedVals[i], decoded);
      }
    }

    /*
     * assert byte[] segments are serialized correctly.
     */
    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      byte[] a = new byte[3 + (Order.ASCENDING == ord ? 1 : 2) + 2];
      PositionedByteRange buf =
          new SimplePositionedMutableByteRange(a, 1, 3 + (Order.ASCENDING == ord ? 1 : 2));
      OrderedBytes.encodeBlobCopy(buf, "foobarbaz".getBytes(), 3, 3, ord);
      buf.setPosition(0);
      assertArrayEquals("bar".getBytes(), OrderedBytes.decodeBlobCopy(buf));
    }
  }

  /**
   * Assert invalid input byte[] are rejected by BlobCopy
   */
  @Test(expected = IllegalArgumentException.class)
  public void testBlobCopyNoZeroBytes() {
    byte[] val = { 0x01, 0x02, 0x00, 0x03 };
    // TODO: implementation detail leaked here.
    byte[] ascExpected = { 0x38, 0x01, 0x02, 0x00, 0x03 };
    PositionedByteRange buf = new SimplePositionedMutableByteRange(val.length + 1);
    OrderedBytes.encodeBlobCopy(buf, val, Order.ASCENDING);
    assertArrayEquals(ascExpected, buf.getBytes());
    buf.set(val.length + 2);
    OrderedBytes.encodeBlobCopy(buf, val, Order.DESCENDING);
    fail("test should never get here.");
  }

  /**
   * Test generic skip logic
   */
  @Test
  public void testSkip() {
    BigDecimal longMax = BigDecimal.valueOf(Long.MAX_VALUE);
    double negInf = Double.NEGATIVE_INFINITY;
    BigDecimal negLarge = longMax.multiply(longMax).negate();
    BigDecimal negMed = new BigDecimal("-10.0");
    BigDecimal negSmall = new BigDecimal("-0.0010");
    long zero = 0l;
    BigDecimal posSmall = negSmall.negate();
    BigDecimal posMed = negMed.negate();
    BigDecimal posLarge = negLarge.negate();
    double posInf = Double.POSITIVE_INFINITY;
    double nan = Double.NaN;
    byte int8 = 100;
    short int16 = 100;
    int int32 = 100;
    long int64 = 100l;
    float float32 = 100.0f;
    double float64 = 100.0d;
    String text = "hello world.";
    byte[] blobVar = Bytes.toBytes("foo");
    byte[] blobCopy = Bytes.toBytes("bar");

    for (Order ord : new Order[] { Order.ASCENDING, Order.DESCENDING }) {
      PositionedByteRange buff = new SimplePositionedMutableByteRange(30);
      int o;
      o = OrderedBytes.encodeNull(buff, ord);
      buff.setPosition(0);
      assertEquals(o, OrderedBytes.skip(buff));

      buff.setPosition(0);
      o = OrderedBytes.encodeNumeric(buff, negInf, ord);
      buff.setPosition(0);
      assertEquals(o, OrderedBytes.skip(buff));

      buff.setPosition(0);
      o = OrderedBytes.encodeNumeric(buff, negLarge, ord);
      buff.setPosition(0);
      assertEquals(o, OrderedBytes.skip(buff));

      buff.setPosition(0);
      o = OrderedBytes.encodeNumeric(buff, negMed, ord);
      buff.setPosition(0);
      assertEquals(o, OrderedBytes.skip(buff));

      buff.setPosition(0);
      o = OrderedBytes.encodeNumeric(buff, negSmall, ord);
      buff.setPosition(0);
      assertEquals(o, OrderedBytes.skip(buff));

      buff.setPosition(0);
      o = OrderedBytes.encodeNumeric(buff, zero, ord);
      buff.setPosition(0);
      assertEquals(o, OrderedBytes.skip(buff));

      buff.setPosition(0);
      o = OrderedBytes.encodeNumeric(buff, posSmall, ord);
      buff.setPosition(0);
      assertEquals(o, OrderedBytes.skip(buff));

      buff.setPosition(0);
      o = OrderedBytes.encodeNumeric(buff, posMed, ord);
      buff.setPosition(0);
      assertEquals(o, OrderedBytes.skip(buff));

      buff.setPosition(0);
      o = OrderedBytes.encodeNumeric(buff, posLarge, ord);
      buff.setPosition(0);
      assertEquals(o, OrderedBytes.skip(buff));

      buff.setPosition(0);
      o = OrderedBytes.encodeNumeric(buff, posInf, ord);
      buff.setPosition(0);
      assertEquals(o, OrderedBytes.skip(buff));

      buff.setPosition(0);
      o = OrderedBytes.encodeNumeric(buff, nan, ord);
      buff.setPosition(0);
      assertEquals(o, OrderedBytes.skip(buff));

      buff.setPosition(0);
      o = OrderedBytes.encodeInt8(buff, int8, ord);
      buff.setPosition(0);
      assertEquals(o, OrderedBytes.skip(buff));

      buff.setPosition(0);
      o = OrderedBytes.encodeInt16(buff, int16, ord);
      buff.setPosition(0);
      assertEquals(o, OrderedBytes.skip(buff));

      buff.setPosition(0);
      o = OrderedBytes.encodeInt32(buff, int32, ord);
      buff.setPosition(0);
      assertEquals(o, OrderedBytes.skip(buff));

      buff.setPosition(0);
      o = OrderedBytes.encodeInt64(buff, int64, ord);
      buff.setPosition(0);
      assertEquals(o, OrderedBytes.skip(buff));

      buff.setPosition(0);
      o = OrderedBytes.encodeFloat32(buff, float32, ord);
      buff.setPosition(0);
      assertEquals(o, OrderedBytes.skip(buff));

      buff.setPosition(0);
      o = OrderedBytes.encodeFloat64(buff, float64, ord);
      buff.setPosition(0);
      assertEquals(o, OrderedBytes.skip(buff));

      buff.setPosition(0);
      o = OrderedBytes.encodeString(buff, text, ord);
      buff.setPosition(0);
      assertEquals(o, OrderedBytes.skip(buff));

      buff.setPosition(0);
      o = OrderedBytes.encodeBlobVar(buff, blobVar, ord);
      buff.setPosition(0);
      assertEquals(o, OrderedBytes.skip(buff));

      // blobCopy is special in that it runs to the end of the target buffer.
      buff.set(blobCopy.length + (Order.ASCENDING == ord ? 1 : 2));
      o = OrderedBytes.encodeBlobCopy(buff, blobCopy, ord);
      buff.setPosition(0);
      assertEquals(o, OrderedBytes.skip(buff));
    }
  }
}
