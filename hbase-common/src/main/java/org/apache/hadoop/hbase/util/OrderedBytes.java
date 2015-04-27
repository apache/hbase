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

import static org.apache.hadoop.hbase.util.Order.ASCENDING;
import static org.apache.hadoop.hbase.util.Order.DESCENDING;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.charset.Charset;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

import com.google.common.annotations.VisibleForTesting;

/**
 * Utility class that handles ordered byte arrays. That is, unlike
 * {@link Bytes}, these methods produce byte arrays which maintain the sort
 * order of the original values.
 * <h3>Encoding Format summary</h3>
 * <p>
 * Each value is encoded as one or more bytes. The first byte of the encoding,
 * its meaning, and a terse description of the bytes that follow is given by
 * the following table:
 * </p>
 * <table summary="Encodings">
 * <tr><th>Content Type</th><th>Encoding</th></tr>
 * <tr><td>NULL</td><td>0x05</td></tr>
 * <tr><td>negative infinity</td><td>0x07</td></tr>
 * <tr><td>negative large</td><td>0x08, ~E, ~M</td></tr>
 * <tr><td>negative medium</td><td>0x13-E, ~M</td></tr>
 * <tr><td>negative small</td><td>0x14, -E, ~M</td></tr>
 * <tr><td>zero</td><td>0x15</td></tr>
 * <tr><td>positive small</td><td>0x16, ~-E, M</td></tr>
 * <tr><td>positive medium</td><td>0x17+E, M</td></tr>
 * <tr><td>positive large</td><td>0x22, E, M</td></tr>
 * <tr><td>positive infinity</td><td>0x23</td></tr>
 * <tr><td>NaN</td><td>0x25</td></tr>
 * <tr><td>fixed-length 32-bit integer</td><td>0x27, I</td></tr>
 * <tr><td>fixed-length 64-bit integer</td><td>0x28, I</td></tr>
 * <tr><td>fixed-length 8-bit integer</td><td>0x29</td></tr>
 * <tr><td>fixed-length 16-bit integer</td><td>0x2a</td></tr>
 * <tr><td>fixed-length 32-bit float</td><td>0x30, F</td></tr>
 * <tr><td>fixed-length 64-bit float</td><td>0x31, F</td></tr>
 * <tr><td>TEXT</td><td>0x33, T</td></tr>
 * <tr><td>variable length BLOB</td><td>0x35, B</td></tr>
 * <tr><td>byte-for-byte BLOB</td><td>0x36, X</td></tr>
 * </table>
 *
 * <h3>Null Encoding</h3>
 * <p>
 * Each value that is a NULL encodes as a single byte of 0x05. Since every
 * other value encoding begins with a byte greater than 0x05, this forces NULL
 * values to sort first.
 * </p>
 * <h3>Text Encoding</h3>
 * <p>
 * Each text value begins with a single byte of 0x33 and ends with a single
 * byte of 0x00. There are zero or more intervening bytes that encode the text
 * value. The intervening bytes are chosen so that the encoding will sort in
 * the desired collating order. The intervening bytes may not contain a 0x00
 * character; the only 0x00 byte allowed in a text encoding is the final byte.
 * </p>
 * <p>
 * The text encoding ends in 0x00 in order to ensure that when there are two
 * strings where one is a prefix of the other that the shorter string will
 * sort first.
 * </p>
 * <h3>Binary Encoding</h3>
 * <p>
 * There are two encoding strategies for binary fields, referred to as
 * "BlobVar" and "BlobCopy". BlobVar is less efficient in both space and
 * encoding time. It has no limitations on the range of encoded values.
 * BlobCopy is a byte-for-byte copy of the input data followed by a
 * termination byte. It is extremely fast to encode and decode. It carries the
 * restriction of not allowing a 0x00 value in the input byte[] as this value
 * is used as the termination byte.
 * </p>
 * <h4>BlobVar</h4>
 * <p>
 * "BlobVar" encodes the input byte[] in a manner similar to a variable length
 * integer encoding. As with the other {@code OrderedBytes} encodings,
 * the first encoded byte is used to indicate what kind of value follows. This
 * header byte is 0x37 for BlobVar encoded values. As with the traditional
 * varint encoding, the most significant bit of each subsequent encoded
 * {@code byte} is used as a continuation marker. The 7 remaining bits
 * contain the 7 most significant bits of the first unencoded byte. The next
 * encoded byte starts with a continuation marker in the MSB. The least
 * significant bit from the first unencoded byte follows, and the remaining 6
 * bits contain the 6 MSBs of the second unencoded byte. The encoding
 * continues, encoding 7 bytes on to 8 encoded bytes. The MSB of the final
 * encoded byte contains a termination marker rather than a continuation
 * marker, and any remaining bits from the final input byte. Any trailing bits
 * in the final encoded byte are zeros.
 * </p>
 * <h4>BlobCopy</h4>
 * <p>
 * "BlobCopy" is a simple byte-for-byte copy of the input data. It uses 0x38
 * as the header byte, and is terminated by 0x00 in the DESCENDING case. This
 * alternative encoding is faster and more space-efficient, but it cannot
 * accept values containing a 0x00 byte in DESCENDING order.
 * </p>
 * <h3>Variable-length Numeric Encoding</h3>
 * <p>
 * Numeric values must be coded so as to sort in numeric order. We assume that
 * numeric values can be both integer and floating point values. Clients must
 * be careful to use inspection methods for encoded values (such as
 * {@link #isNumericInfinite(PositionedByteRange)} and
 * {@link #isNumericNaN(PositionedByteRange)} to protect against decoding
 * values into object which do not support these numeric concepts (such as
 * {@link Long} and {@link BigDecimal}).
 * </p>
 * <p>
 * Simplest cases first: If the numeric value is a NaN, then the encoding is a
 * single byte of 0x25. This causes NaN values to sort after every other
 * numeric value.
 * </p>
 * <p>
 * If the numeric value is a negative infinity then the encoding is a single
 * byte of 0x07. Since every other numeric value except NaN has a larger
 * initial byte, this encoding ensures that negative infinity will sort prior
 * to every other numeric value other than NaN.
 * </p>
 * <p>
 * If the numeric value is a positive infinity then the encoding is a single
 * byte of 0x23. Every other numeric value encoding begins with a smaller
 * byte, ensuring that positive infinity always sorts last among numeric
 * values. 0x23 is also smaller than 0x33, the initial byte of a text value,
 * ensuring that every numeric value sorts before every text value.
 * </p>
 * <p>
 * If the numeric value is exactly zero then it is encoded as a single byte of
 * 0x15. Finite negative values will have initial bytes of 0x08 through 0x14
 * and finite positive values will have initial bytes of 0x16 through 0x22.
 * </p>
 * <p>
 * For all numeric values, we compute a mantissa M and an exponent E. The
 * mantissa is a base-100 representation of the value. The exponent E
 * determines where to put the decimal point.
 * </p>
 * <p>
 * Each centimal digit of the mantissa is stored in a byte. If the value of
 * the centimal digit is X (hence X&ge;0 and X&le;99) then the byte value will
 * be 2*X+1 for every byte of the mantissa, except for the last byte which
 * will be 2*X+0. The mantissa must be the minimum number of bytes necessary
 * to represent the value; trailing X==0 digits are omitted. This means that
 * the mantissa will never contain a byte with the value 0x00.
 * </p>
 * <p>
 * If we assume all digits of the mantissa occur to the right of the decimal
 * point, then the exponent E is the power of one hundred by which one must
 * multiply the mantissa to recover the original value.
 * </p>
 * <p>
 * Values are classified as large, medium, or small according to the value of
 * E. If E is 11 or more, the value is large. For E between 0 and 10, the
 * value is medium. For E less than zero, the value is small.
 * </p>
 * <p>
 * Large positive values are encoded as a single byte 0x22 followed by E as a
 * varint and then M. Medium positive values are a single byte of 0x17+E
 * followed by M. Small positive values are encoded as a single byte 0x16
 * followed by the ones-complement of the varint for -E followed by M.
 * </p>
 * <p>
 * Small negative values are encoded as a single byte 0x14 followed by -E as a
 * varint and then the ones-complement of M. Medium negative values are
 * encoded as a byte 0x13-E followed by the ones-complement of M. Large
 * negative values consist of the single byte 0x08 followed by the
 * ones-complement of the varint encoding of E followed by the ones-complement
 * of M.
 * </p>
 * <h3>Fixed-length Integer Encoding</h3>
 * <p>
 * All 4-byte integers are serialized to a 5-byte, fixed-width, sortable byte
 * format. All 8-byte integers are serialized to the equivelant 9-byte format.
 * Serialization is performed by writing a header byte, inverting the integer
 * sign bit and writing the resulting bytes to the byte array in big endian
 * order.
 * </p>
 * <h3>Fixed-length Floating Point Encoding</h3>
 * <p>
 * 32-bit and 64-bit floating point numbers are encoded to a 5-byte and 9-byte
 * encoding format, respectively. The format is identical, save for the
 * precision respected in each step of the operation.
 * <p>
 * This format ensures the following total ordering of floating point values:
 * Float.NEGATIVE_INFINITY &lt; -Float.MAX_VALUE &lt; ... &lt;
 * -Float.MIN_VALUE &lt; -0.0 &lt; +0.0; &lt; Float.MIN_VALUE &lt; ... &lt;
 * Float.MAX_VALUE &lt; Float.POSITIVE_INFINITY &lt; Float.NaN
 * </p>
 * <p>
 * Floating point numbers are encoded as specified in IEEE 754. A 32-bit
 * single precision float consists of a sign bit, 8-bit unsigned exponent
 * encoded in offset-127 notation, and a 23-bit significand. The format is
 * described further in the <a
 * href="http://en.wikipedia.org/wiki/Single_precision"> Single Precision
 * Floating Point Wikipedia page</a>
 * </p>
 * <p>
 * The value of a normal float is -1 <sup>sign bit</sup> &times;
 * 2<sup>exponent - 127</sup> &times; 1.significand
 * </p>
 * <p>
 * The IEE754 floating point format already preserves sort ordering for
 * positive floating point numbers when the raw bytes are compared in most
 * significant byte order. This is discussed further at <a href=
 * "http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.htm">
 * http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.htm</a>
 * </p>
 * <p>
 * Thus, we need only ensure that negative numbers sort in the the exact
 * opposite order as positive numbers (so that say, negative infinity is less
 * than negative 1), and that all negative numbers compare less than any
 * positive number. To accomplish this, we invert the sign bit of all floating
 * point numbers, and we also invert the exponent and significand bits if the
 * floating point number was negative.
 * </p>
 * <p>
 * More specifically, we first store the floating point bits into a 32-bit int
 * {@code j} using {@link Float#floatToIntBits}. This method collapses
 * all NaNs into a single, canonical NaN value but otherwise leaves the bits
 * unchanged. We then compute
 * </p>
 *
 * <pre>
 * j &circ;= (j &gt;&gt; (Integer.SIZE - 1)) | Integer.MIN_SIZE
 * </pre>
 * <p>
 * which inverts the sign bit and XOR's all other bits with the sign bit
 * itself. Comparing the raw bytes of {@code j} in most significant byte
 * order is equivalent to performing a single precision floating point
 * comparison on the underlying bits (ignoring NaN comparisons, as NaNs don't
 * compare equal to anything when performing floating point comparisons).
 * </p>
 * <p>
 * The resulting integer is then converted into a byte array by serializing
 * the integer one byte at a time in most significant byte order. The
 * serialized integer is prefixed by a single header byte. All serialized
 * values are 5 bytes in length.
 * </p>
 * <p>
 * {@code OrderedBytes} encodings are heavily influenced by the
 * <a href="http://sqlite.org/src4/doc/trunk/www/key_encoding.wiki">SQLite4 Key
 * Encoding</a>. Slight deviations are make in the interest of order
 * correctness and user extensibility. Fixed-width {@code Long} and
 * {@link Double} encodings are based on implementations from the now defunct
 * Orderly library.
 * </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class OrderedBytes {

  /*
   * These constants define header bytes used to identify encoded values. Note
   * that the values here are not exhaustive as the Numeric format encodes
   * portions of its value within the header byte. The values listed here are
   * directly applied to persisted data -- DO NOT modify the values specified
   * here. Instead, gaps are placed intentionally between values so that new
   * implementations can be inserted into the total ordering enforced here.
   */
  private static final byte NULL = 0x05;
  // room for 1 expansion type
  private static final byte NEG_INF = 0x07;
  private static final byte NEG_LARGE = 0x08;
  private static final byte NEG_MED_MIN = 0x09;
  private static final byte NEG_MED_MAX = 0x13;
  private static final byte NEG_SMALL = 0x14;
  private static final byte ZERO = 0x15;
  private static final byte POS_SMALL = 0x16;
  private static final byte POS_MED_MIN = 0x17;
  private static final byte POS_MED_MAX = 0x21;
  private static final byte POS_LARGE = 0x22;
  private static final byte POS_INF = 0x23;
  // room for 2 expansion type
  private static final byte NAN = 0x26;
  // room for 2 expansion types
  private static final byte FIXED_INT8 = 0x29;
  private static final byte FIXED_INT16 = 0x2a;
  private static final byte FIXED_INT32 = 0x2b;
  private static final byte FIXED_INT64 = 0x2c;
  // room for 3 expansion types
  private static final byte FIXED_FLOAT32 = 0x30;
  private static final byte FIXED_FLOAT64 = 0x31;
  // room for 2 expansion type
  private static final byte TEXT = 0x34;
  // room for 2 expansion type
  private static final byte BLOB_VAR = 0x37;
  private static final byte BLOB_COPY = 0x38;

  /*
   * The following constant values are used by encoding implementations
   */

  public static final Charset UTF8 = Charset.forName("UTF-8");
  private static final byte TERM = 0x00;
  private static final BigDecimal E8 = BigDecimal.valueOf(1e8);
  private static final BigDecimal E32 = BigDecimal.valueOf(1e32);
  private static final BigDecimal EN2 = BigDecimal.valueOf(1e-2);
  private static final BigDecimal EN10 = BigDecimal.valueOf(1e-10);

  /**
   * Max precision guaranteed to fit into a {@code long}.
   */
  public static final int MAX_PRECISION = 31;

  /**
   * The context used to normalize {@link BigDecimal} values.
   */
  public static final MathContext DEFAULT_MATH_CONTEXT =
      new MathContext(MAX_PRECISION, RoundingMode.HALF_UP);

  /**
   * Creates the standard exception when the encoded header byte is unexpected for the decoding
   * context.
   * @param header value used in error message.
   */
  private static IllegalArgumentException unexpectedHeader(byte header) {
    throw new IllegalArgumentException("unexpected value in first byte: 0x"
        + Long.toHexString(header));
  }

  /**
   * Perform unsigned comparison between two long values. Conforms to the same interface as
   * {@link Comparator#compare(Object, Object)}.
   */
  private static int unsignedCmp(long x1, long x2) {
    int cmp;
    if ((cmp = (x1 < x2 ? -1 : (x1 == x2 ? 0 : 1))) == 0) return 0;
    // invert the result when either value is negative
    if ((x1 < 0) != (x2 < 0)) return -cmp;
    return cmp;
  }

  /**
   * Write a 32-bit unsigned integer to {@code dst} as 4 big-endian bytes.
   * @return number of bytes written.
   */
  private static int putUint32(PositionedByteRange dst, int val) {
    dst.put((byte) (val >>> 24))
       .put((byte) (val >>> 16))
       .put((byte) (val >>> 8))
       .put((byte) val);
    return 4;
  }

  /**
   * Encode an unsigned 64-bit unsigned integer {@code val} into {@code dst}.
   * @param dst The destination to which encoded bytes are written.
   * @param val The value to write.
   * @param comp Compliment the encoded value when {@code comp} is true.
   * @return number of bytes written.
   */
  @VisibleForTesting
  static int putVaruint64(PositionedByteRange dst, long val, boolean comp) {
    int w, y, len = 0;
    final int offset = dst.getOffset(), start = dst.getPosition();
    byte[] a = dst.getBytes();
    Order ord = comp ? DESCENDING : ASCENDING;
    if (-1 == unsignedCmp(val, 241L)) {
      dst.put((byte) val);
      len = dst.getPosition() - start;
      ord.apply(a, offset + start, len);
      return len;
    }
    if (-1 == unsignedCmp(val, 2288L)) {
      y = (int) (val - 240);
      dst.put((byte) (y / 256 + 241))
         .put((byte) (y % 256));
      len = dst.getPosition() - start;
      ord.apply(a, offset + start, len);
      return len;
    }
    if (-1 == unsignedCmp(val, 67824L)) {
      y = (int) (val - 2288);
      dst.put((byte) 249)
         .put((byte) (y / 256))
         .put((byte) (y % 256));
      len = dst.getPosition() - start;
      ord.apply(a, offset + start, len);
      return len;
    }
    y = (int) val;
    w = (int) (val >>> 32);
    if (w == 0) {
      if (-1 == unsignedCmp(y, 16777216L)) {
        dst.put((byte) 250)
           .put((byte) (y >>> 16))
           .put((byte) (y >>> 8))
           .put((byte) y);
        len = dst.getPosition() - start;
        ord.apply(a, offset + start, len);
        return len;
      }
      dst.put((byte) 251);
      putUint32(dst, y);
      len = dst.getPosition() - start;
      ord.apply(a, offset + start, len);
      return len;
    }
    if (-1 == unsignedCmp(w, 256L)) {
      dst.put((byte) 252)
         .put((byte) w);
      putUint32(dst, y);
      len = dst.getPosition() - start;
      ord.apply(a, offset + start, len);
      return len;
    }
    if (-1 == unsignedCmp(w, 65536L)) {
      dst.put((byte) 253)
         .put((byte) (w >>> 8))
         .put((byte) w);
      putUint32(dst, y);
      len = dst.getPosition() - start;
      ord.apply(a, offset + start, len);
      return len;
    }
    if (-1 == unsignedCmp(w, 16777216L)) {
      dst.put((byte) 254)
         .put((byte) (w >>> 16))
         .put((byte) (w >>> 8))
         .put((byte) w);
      putUint32(dst, y);
      len = dst.getPosition() - start;
      ord.apply(a, offset + start, len);
      return len;
    }
    dst.put((byte) 255);
    putUint32(dst, w);
    putUint32(dst, y);
    len = dst.getPosition() - start;
    ord.apply(a, offset + start, len);
    return len;
  }

  /**
   * Inspect {@code src} for an encoded varuint64 for its length in bytes.
   * Preserves the state of {@code src}.
   * @param src source buffer
   * @param comp if true, parse the compliment of the value.
   * @return the number of bytes consumed by this value.
   */
  @VisibleForTesting
  static int lengthVaruint64(PositionedByteRange src, boolean comp) {
    int a0 = (comp ? DESCENDING : ASCENDING).apply(src.peek()) & 0xff;
    if (a0 <= 240) return 1;
    if (a0 >= 241 && a0 <= 248) return 2;
    if (a0 == 249) return 3;
    if (a0 == 250) return 4;
    if (a0 == 251) return 5;
    if (a0 == 252) return 6;
    if (a0 == 253) return 7;
    if (a0 == 254) return 8;
    if (a0 == 255) return 9;
    throw unexpectedHeader(src.peek());
  }

  /**
   * Skip {@code src} over the encoded varuint64.
   * @param src source buffer
   * @param cmp if true, parse the compliment of the value.
   * @return the number of bytes skipped.
   */
  @VisibleForTesting
  static int skipVaruint64(PositionedByteRange src, boolean cmp) {
    final int len = lengthVaruint64(src, cmp);
    src.setPosition(src.getPosition() + len);
    return len;
  }

  /**
   * Decode a sequence of bytes in {@code src} as a varuint64. Compliment the
   * encoded value when {@code comp} is true.
   * @return the decoded value.
   */
  @VisibleForTesting
  static long getVaruint64(PositionedByteRange src, boolean comp) {
    assert src.getRemaining() >= lengthVaruint64(src, comp);
    final long ret;
    Order ord = comp ? DESCENDING : ASCENDING;
    byte x = src.get();
    final int a0 = ord.apply(x) & 0xff, a1, a2, a3, a4, a5, a6, a7, a8;
    if (-1 == unsignedCmp(a0, 241)) {
      return a0;
    }
    x = src.get();
    a1 = ord.apply(x) & 0xff;
    if (-1 == unsignedCmp(a0, 249)) {
      return (a0 - 241) * 256 + a1 + 240;
    }
    x = src.get();
    a2 = ord.apply(x) & 0xff;
    if (a0 == 249) {
      return 2288 + 256 * a1 + a2;
    }
    x = src.get();
    a3 = ord.apply(x) & 0xff;
    if (a0 == 250) {
      return (a1 << 16) | (a2 << 8) | a3;
    }
    x = src.get();
    a4 = ord.apply(x) & 0xff;
    ret = (((long) a1) << 24) | (a2 << 16) | (a3 << 8) | a4;
    if (a0 == 251) {
      return ret;
    }
    x = src.get();
    a5 = ord.apply(x) & 0xff;
    if (a0 == 252) {
      return (ret << 8) | a5;
    }
    x = src.get();
    a6 = ord.apply(x) & 0xff;
    if (a0 == 253) {
      return (ret << 16) | (a5 << 8) | a6;
    }
    x = src.get();
    a7 = ord.apply(x) & 0xff;
    if (a0 == 254) {
      return (ret << 24) | (a5 << 16) | (a6 << 8) | a7;
    }
    x = src.get();
    a8 = ord.apply(x) & 0xff;
    return (ret << 32) | (((long) a5) << 24) | (a6 << 16) | (a7 << 8) | a8;
  }

  /**
   * Strip all trailing zeros to ensure that no digit will be zero and round
   * using our default context to ensure precision doesn't exceed max allowed.
   * From Phoenix's {@code NumberUtil}.
   * @return new {@link BigDecimal} instance
   */
  @VisibleForTesting
  static BigDecimal normalize(BigDecimal val) {
    return null == val ? null : val.stripTrailingZeros().round(DEFAULT_MATH_CONTEXT);
  }

  /**
   * Read significand digits from {@code src} according to the magnitude
   * of {@code e}.
   * @param src The source from which to read encoded digits.
   * @param e The magnitude of the first digit read.
   * @param comp Treat encoded bytes as compliments when {@code comp} is true.
   * @return The decoded value.
   * @throws IllegalArgumentException when read exceeds the remaining length
   *     of {@code src}.
   */
  private static BigDecimal decodeSignificand(PositionedByteRange src, int e, boolean comp) {
    // TODO: can this be made faster?
    byte[] a = src.getBytes();
    final int start = src.getPosition(), offset = src.getOffset(), remaining = src.getRemaining();
    Order ord = comp ? DESCENDING : ASCENDING;
    BigDecimal m = BigDecimal.ZERO;
    e--;
    for (int i = 0;; i++) {
      if (i > remaining) {
        // we've exceeded this range's window
        src.setPosition(start);
        throw new IllegalArgumentException(
            "Read exceeds range before termination byte found. offset: " + offset + " position: "
                + (start + i));
      }
      // base-100 digits are encoded as val * 2 + 1 except for the termination digit.
      m = m.add( // m +=
        new BigDecimal(BigInteger.ONE, e * -2).multiply( // 100 ^ p * [decoded digit]
          BigDecimal.valueOf((ord.apply(a[offset + start + i]) & 0xff) / 2)));
      e--;
      // detect termination digit
      if ((ord.apply(a[offset + start + i]) & 1) == 0) {
        src.setPosition(start + i + 1);
        break;
      }
    }
    return normalize(m);
  }

  /**
   * Skip {@code src} over the significand bytes.
   * @param src The source from which to read encoded digits.
   * @param comp Treat encoded bytes as compliments when {@code comp} is true.
   * @return the number of bytes skipped.
   */
  private static int skipSignificand(PositionedByteRange src, boolean comp) {
    byte[] a = src.getBytes();
    final int offset = src.getOffset(), start = src.getPosition();
    int i = src.getPosition();
    while (((comp ? DESCENDING : ASCENDING).apply(a[offset + i++]) & 1) != 0)
      ;
    src.setPosition(i);
    return i - start;
  }

  /**
   * <p>
   * Encode the small magnitude floating point number {@code val} using the
   * key encoding. The caller guarantees that 1.0 > abs(val) > 0.0.
   * </p>
   * <p>
   * A floating point value is encoded as an integer exponent {@code E} and a
   * mantissa {@code M}. The original value is equal to {@code (M * 100^E)}.
   * {@code E} is set to the smallest value possible without making {@code M}
   * greater than or equal to 1.0.
   * </p>
   * <p>
   * For this routine, {@code E} will always be zero or negative, since the
   * original value is less than one. The encoding written by this routine is
   * the ones-complement of the varint of the negative of {@code E} followed
   * by the mantissa:
   * <pre>
   *   Encoding:   ~-E  M
   * </pre>
   * </p>
   * @param dst The destination to which encoded digits are written.
   * @param val The value to encode.
   * @return the number of bytes written.
   */
  private static int encodeNumericSmall(PositionedByteRange dst, BigDecimal val) {
    // TODO: this can be done faster?
    // assert 1.0 > abs(val) > 0.0
    BigDecimal abs = val.abs();
    assert BigDecimal.ZERO.compareTo(abs) < 0 && BigDecimal.ONE.compareTo(abs) > 0;
    byte[] a = dst.getBytes();
    boolean isNeg = val.signum() == -1;
    final int offset = dst.getOffset(), start = dst.getPosition();
    int e = 0, d, startM;

    if (isNeg) { /* Small negative number: 0x14, -E, ~M */
      dst.put(NEG_SMALL);
    } else { /* Small positive number: 0x16, ~-E, M */
      dst.put(POS_SMALL);
    }

    // normalize abs(val) to determine E
    while (abs.compareTo(EN10) < 0) { abs = abs.movePointRight(8); e += 4; }
    while (abs.compareTo(EN2) < 0) { abs = abs.movePointRight(2); e++; }

    putVaruint64(dst, e, !isNeg); // encode appropriate E value.

    // encode M by peeling off centimal digits, encoding x as 2x+1
    startM = dst.getPosition();
    // TODO: 18 is an arbitrary encoding limit. Reevaluate once we have a better handling of
    // numeric scale.
    for (int i = 0; i < 18 && abs.compareTo(BigDecimal.ZERO) != 0; i++) {
      abs = abs.movePointRight(2);
      d = abs.intValue();
      dst.put((byte) ((2 * d + 1) & 0xff));
      abs = abs.subtract(BigDecimal.valueOf(d));
    }
    a[offset + dst.getPosition() - 1] &= 0xfe; // terminal digit should be 2x
    if (isNeg) {
      // negative values encoded as ~M
      DESCENDING.apply(a, offset + startM, dst.getPosition() - startM);
    }
    return dst.getPosition() - start;
  }

  /**
   * Encode the large magnitude floating point number {@code val} using
   * the key encoding. The caller guarantees that {@code val} will be
   * finite and abs(val) >= 1.0.
   * <p>
   * A floating point value is encoded as an integer exponent {@code E}
   * and a mantissa {@code M}. The original value is equal to
   * {@code (M * 100^E)}. {@code E} is set to the smallest value
   * possible without making {@code M} greater than or equal to 1.0.
   * </p>
   * <p>
   * Each centimal digit of the mantissa is stored in a byte. If the value of
   * the centimal digit is {@code X} (hence {@code X>=0} and
   * {@code X<=99}) then the byte value will be {@code 2*X+1} for
   * every byte of the mantissa, except for the last byte which will be
   * {@code 2*X+0}. The mantissa must be the minimum number of bytes
   * necessary to represent the value; trailing {@code X==0} digits are
   * omitted. This means that the mantissa will never contain a byte with the
   * value {@code 0x00}.
   * </p>
   * <p>
   * If {@code E > 10}, then this routine writes of {@code E} as a
   * varint followed by the mantissa as described above. Otherwise, if
   * {@code E <= 10}, this routine only writes the mantissa and leaves
   * the {@code E} value to be encoded as part of the opening byte of the
   * field by the calling function.
   *
   * <pre>
   *   Encoding:  M       (if E<=10)
   *              E M     (if E>10)
   * </pre>
   * </p>
   * @param dst The destination to which encoded digits are written.
   * @param val The value to encode.
   * @return the number of bytes written.
   */
  private static int encodeNumericLarge(PositionedByteRange dst, BigDecimal val) {
    // TODO: this can be done faster
    BigDecimal abs = val.abs();
    byte[] a = dst.getBytes();
    boolean isNeg = val.signum() == -1;
    final int start = dst.getPosition(), offset = dst.getOffset();
    int e = 0, d, startM;

    if (isNeg) { /* Large negative number: 0x08, ~E, ~M */
      dst.put(NEG_LARGE);
    } else { /* Large positive number: 0x22, E, M */
      dst.put(POS_LARGE);
    }

    // normalize abs(val) to determine E
    while (abs.compareTo(E32) >= 0 && e <= 350) { abs = abs.movePointLeft(32); e +=16; }
    while (abs.compareTo(E8) >= 0 && e <= 350) { abs = abs.movePointLeft(8); e+= 4; }
    while (abs.compareTo(BigDecimal.ONE) >= 0 && e <= 350) { abs = abs.movePointLeft(2); e++; }

    // encode appropriate header byte and/or E value.
    if (e > 10) { /* large number, write out {~,}E */
      putVaruint64(dst, e, isNeg);
    } else {
      if (isNeg) { /* Medium negative number: 0x13-E, ~M */
        dst.put(start, (byte) (NEG_MED_MAX - e));
      } else { /* Medium positive number: 0x17+E, M */
        dst.put(start, (byte) (POS_MED_MIN + e));
      }
    }

    // encode M by peeling off centimal digits, encoding x as 2x+1
    startM = dst.getPosition();
    // TODO: 18 is an arbitrary encoding limit. Reevaluate once we have a better handling of
    // numeric scale.
    for (int i = 0; i < 18 && abs.compareTo(BigDecimal.ZERO) != 0; i++) {
      abs = abs.movePointRight(2);
      d = abs.intValue();
      dst.put((byte) (2 * d + 1));
      abs = abs.subtract(BigDecimal.valueOf(d));
    }

    a[offset + dst.getPosition() - 1] &= 0xfe; // terminal digit should be 2x
    if (isNeg) {
      // negative values encoded as ~M
      DESCENDING.apply(a, offset + startM, dst.getPosition() - startM);
    }
    return dst.getPosition() - start;
  }

  /**
   * Encode a numerical value using the variable-length encoding.
   * @param dst The destination to which encoded digits are written.
   * @param val The value to encode.
   * @param ord The {@link Order} to respect while encoding {@code val}.
   * @return the number of bytes written.
   */
  public static int encodeNumeric(PositionedByteRange dst, long val, Order ord) {
    return encodeNumeric(dst, BigDecimal.valueOf(val), ord);
  }

  /**
   * Encode a numerical value using the variable-length encoding.
   * @param dst The destination to which encoded digits are written.
   * @param val The value to encode.
   * @param ord The {@link Order} to respect while encoding {@code val}.
   * @return the number of bytes written.
   */
  public static int encodeNumeric(PositionedByteRange dst, double val, Order ord) {
    if (val == 0.0) {
      dst.put(ord.apply(ZERO));
      return 1;
    }
    if (Double.isNaN(val)) {
      dst.put(ord.apply(NAN));
      return 1;
    }
    if (val == Double.NEGATIVE_INFINITY) {
      dst.put(ord.apply(NEG_INF));
      return 1;
    }
    if (val == Double.POSITIVE_INFINITY) {
      dst.put(ord.apply(POS_INF));
      return 1;
    }
    return encodeNumeric(dst, BigDecimal.valueOf(val), ord);
  }

  /**
   * Encode a numerical value using the variable-length encoding.
   * @param dst The destination to which encoded digits are written.
   * @param val The value to encode.
   * @param ord The {@link Order} to respect while encoding {@code val}.
   * @return the number of bytes written.
   */
  public static int encodeNumeric(PositionedByteRange dst, BigDecimal val, Order ord) {
    final int len, offset = dst.getOffset(), start = dst.getPosition();
    if (null == val) {
      return encodeNull(dst, ord);
    } else if (BigDecimal.ZERO.compareTo(val) == 0) {
      dst.put(ord.apply(ZERO));
      return 1;
    }
    BigDecimal abs = val.abs();
    if (BigDecimal.ONE.compareTo(abs) <= 0) { // abs(v) >= 1.0
      len = encodeNumericLarge(dst, normalize(val));
    } else { // 1.0 > abs(v) >= 0.0
      len = encodeNumericSmall(dst, normalize(val));
    }
    ord.apply(dst.getBytes(), offset + start, len);
    return len;
  }

  /**
   * Decode a {@link BigDecimal} from {@code src}. Assumes {@code src} encodes
   * a value in Numeric encoding and is within the valid range of
   * {@link BigDecimal} values. {@link BigDecimal} does not support {@code NaN}
   * or {@code Infinte} values.
   * @see #decodeNumericAsDouble(PositionedByteRange)
   */
  private static BigDecimal decodeNumericValue(PositionedByteRange src) {
    final int e;
    byte header = src.get();
    boolean dsc = -1 == Integer.signum(header);
    header = dsc ? DESCENDING.apply(header) : header;

    if (header == NULL) return null;
    if (header == NEG_LARGE) { /* Large negative number: 0x08, ~E, ~M */
      e = (int) getVaruint64(src, !dsc);
      return decodeSignificand(src, e, !dsc).negate();
    }
    if (header >= NEG_MED_MIN && header <= NEG_MED_MAX) {
      /* Medium negative number: 0x13-E, ~M */
      e = NEG_MED_MAX - header;
      return decodeSignificand(src, e, !dsc).negate();
    }
    if (header == NEG_SMALL) { /* Small negative number: 0x14, -E, ~M */
      e = (int) -getVaruint64(src, dsc);
      return decodeSignificand(src, e, !dsc).negate();
    }
    if (header == ZERO) {
      return BigDecimal.ZERO;
    }
    if (header == POS_SMALL) { /* Small positive number: 0x16, ~-E, M */
      e = (int) -getVaruint64(src, !dsc);
      return decodeSignificand(src, e, dsc);
    }
    if (header >= POS_MED_MIN && header <= POS_MED_MAX) {
      /* Medium positive number: 0x17+E, M */
      e = header - POS_MED_MIN;
      return decodeSignificand(src, e, dsc);
    }
    if (header == POS_LARGE) { /* Large positive number: 0x22, E, M */
      e = (int) getVaruint64(src, dsc);
      return decodeSignificand(src, e, dsc);
    }
    throw unexpectedHeader(header);
  }

  /**
   * Decode a primitive {@code double} value from the Numeric encoding. Numeric
   * encoding is based on {@link BigDecimal}; in the event the encoded value is
   * larger than can be represented in a {@code double}, this method performs
   * an implicit narrowing conversion as described in
   * {@link BigDecimal#doubleValue()}.
   * @throws NullPointerException when the encoded value is {@code NULL}.
   * @throws IllegalArgumentException when the encoded value is not a Numeric.
   * @see #encodeNumeric(PositionedByteRange, double, Order)
   * @see BigDecimal#doubleValue()
   */
  public static double decodeNumericAsDouble(PositionedByteRange src) {
    // TODO: should an encoded NULL value throw unexpectedHeader() instead?
    if (isNull(src)) {
      throw new NullPointerException("A null value cannot be decoded to a double.");
    }
    if (isNumericNaN(src)) {
      src.get();
      return Double.NaN;
    }
    if (isNumericZero(src)) {
      src.get();
      return Double.valueOf(0.0);
    }

    byte header = -1 == Integer.signum(src.peek()) ? DESCENDING.apply(src.peek()) : src.peek();

    if (header == NEG_INF) {
      src.get();
      return Double.NEGATIVE_INFINITY;
    } else if (header == POS_INF) {
      src.get();
      return Double.POSITIVE_INFINITY;
    } else {
      return decodeNumericValue(src).doubleValue();
    }
  }

  /**
   * Decode a primitive {@code long} value from the Numeric encoding. Numeric
   * encoding is based on {@link BigDecimal}; in the event the encoded value is
   * larger than can be represented in a {@code long}, this method performs an
   * implicit narrowing conversion as described in
   * {@link BigDecimal#doubleValue()}.
   * @throws NullPointerException when the encoded value is {@code NULL}.
   * @throws IllegalArgumentException when the encoded value is not a Numeric.
   * @see #encodeNumeric(PositionedByteRange, long, Order)
   * @see BigDecimal#longValue()
   */
  public static long decodeNumericAsLong(PositionedByteRange src) {
    // TODO: should an encoded NULL value throw unexpectedHeader() instead?
    if (isNull(src)) throw new NullPointerException();
    if (!isNumeric(src)) throw unexpectedHeader(src.peek());
    if (isNumericNaN(src)) throw unexpectedHeader(src.peek());
    if (isNumericInfinite(src)) throw unexpectedHeader(src.peek());

    if (isNumericZero(src)) {
      src.get();
      return Long.valueOf(0);
    }
    return decodeNumericValue(src).longValue();
  }

  /**
   * Decode a {@link BigDecimal} value from the variable-length encoding.
   * @throws IllegalArgumentException when the encoded value is not a Numeric.
   * @see #encodeNumeric(PositionedByteRange, BigDecimal, Order)
   */
  public static BigDecimal decodeNumericAsBigDecimal(PositionedByteRange src) {
    if (isNull(src)) {
      src.get();
      return null;
    }
    if (!isNumeric(src)) throw unexpectedHeader(src.peek());
    if (isNumericNaN(src)) throw unexpectedHeader(src.peek());
    if (isNumericInfinite(src)) throw unexpectedHeader(src.peek());
    return decodeNumericValue(src);
  }

  /**
   * Encode a String value. String encoding is 0x00-terminated and so it does
   * not support {@code \u0000} codepoints in the value.
   * @param dst The destination to which the encoded value is written.
   * @param val The value to encode.
   * @param ord The {@link Order} to respect while encoding {@code val}.
   * @return the number of bytes written.
   * @throws IllegalArgumentException when {@code val} contains a {@code \u0000}.
   */
  public static int encodeString(PositionedByteRange dst, String val, Order ord) {
    if (null == val) {
      return encodeNull(dst, ord);
    }
    if (val.contains("\u0000"))
      throw new IllegalArgumentException("Cannot encode String values containing '\\u0000'");
    final int offset = dst.getOffset(), start = dst.getPosition();
    dst.put(TEXT);
    // TODO: is there no way to decode into dst directly?
    dst.put(val.getBytes(UTF8));
    dst.put(TERM);
    ord.apply(dst.getBytes(), offset + start, dst.getPosition() - start);
    return dst.getPosition() - start;
  }

  /**
   * Decode a String value.
   */
  public static String decodeString(PositionedByteRange src) {
    final byte header = src.get();
    if (header == NULL || header == DESCENDING.apply(NULL))
      return null;
    assert header == TEXT || header == DESCENDING.apply(TEXT);
    Order ord = header == TEXT ? ASCENDING : DESCENDING;
    byte[] a = src.getBytes();
    final int offset = src.getOffset(), start = src.getPosition();
    final byte terminator = ord.apply(TERM);
    int rawStartPos = offset + start, rawTermPos = rawStartPos;
    for (; a[rawTermPos] != terminator; rawTermPos++)
      ;
    src.setPosition(rawTermPos - offset + 1); // advance position to TERM + 1
    if (DESCENDING == ord) {
      // make a copy so that we don't disturb encoded value with ord.
      byte[] copy = new byte[rawTermPos - rawStartPos];
      System.arraycopy(a, rawStartPos, copy, 0, copy.length);
      ord.apply(copy);
      return new String(copy, UTF8);
    } else {
      return new String(a, rawStartPos, rawTermPos - rawStartPos, UTF8);
    }
  }

  /**
   * Calculate the expected BlobVar encoded length based on unencoded length.
   */
  public static int blobVarEncodedLength(int len) {
    if (0 == len)
      return 2; // 1-byte header + 1-byte terminator
    else
      return (int)
          Math.ceil(
            (len * 8) // 8-bits per input byte
            / 7.0)    // 7-bits of input data per encoded byte, rounded up
          + 1;        // + 1-byte header
  }

  /**
   * Calculate the expected BlobVar decoded length based on encoded length.
   */
  @VisibleForTesting
  static int blobVarDecodedLength(int len) {
    return
        ((len
          - 1) // 1-byte header
          * 7) // 7-bits of payload per encoded byte
          / 8; // 8-bits per byte
  }

  /**
   * Encode a Blob value using a modified varint encoding scheme.
   * <p>
   * This format encodes a byte[] value such that no limitations on the input
   * value are imposed. The first byte encodes the encoding scheme that
   * follows, {@link #BLOB_VAR}. Each encoded byte thereafter consists of a
   * header bit followed by 7 bits of payload. A header bit of '1' indicates
   * continuation of the encoding. A header bit of '0' indicates this byte
   * contains the last of the payload. An empty input value is encoded as the
   * header byte immediately followed by a termination byte {@code 0x00}. This
   * is not ambiguous with the encoded value of {@code []}, which results in
   * {@code [0x80, 0x00]}.
   * </p>
   * @return the number of bytes written.
   */
  public static int encodeBlobVar(PositionedByteRange dst, byte[] val, int voff, int vlen,
      Order ord) {
    if (null == val) {
      return encodeNull(dst, ord);
    }
    // Empty value is null-terminated. All other values are encoded as 7-bits per byte.
    assert dst.getRemaining() >= blobVarEncodedLength(vlen) : "buffer overflow expected.";
    final int offset = dst.getOffset(), start = dst.getPosition();
    dst.put(BLOB_VAR);
    if (0 == vlen) {
      dst.put(TERM);
    } else {
      byte s = 1, t = 0;
      for (int i = voff; i < vlen; i++) {
        dst.put((byte) (0x80 | t | ((val[i] & 0xff) >>> s)));
        if (s < 7) {
          t = (byte) (val[i] << (7 - s));
          s++;
        } else {
          dst.put((byte) (0x80 | val[i]));
          s = 1;
          t = 0;
        }
      }
      if (s > 1) {
        dst.put((byte) (0x7f & t));
      } else {
        dst.getBytes()[offset + dst.getPosition() - 1] &= 0x7f;
      }
    }
    ord.apply(dst.getBytes(), offset + start, dst.getPosition() - start);
    return dst.getPosition() - start;
  }

  /**
   * Encode a blob value using a modified varint encoding scheme.
   * @return the number of bytes written.
   * @see #encodeBlobVar(PositionedByteRange, byte[], int, int, Order)
   */
  public static int encodeBlobVar(PositionedByteRange dst, byte[] val, Order ord) {
    return encodeBlobVar(dst, val, 0, null != val ? val.length : 0, ord);
  }

  /**
   * Decode a blob value that was encoded using BlobVar encoding.
   */
  public static byte[] decodeBlobVar(PositionedByteRange src) {
    final byte header = src.get();
    if (header == NULL || header == DESCENDING.apply(NULL)) {
      return null;
    }
    assert header == BLOB_VAR || header == DESCENDING.apply(BLOB_VAR);
    Order ord = BLOB_VAR == header ? ASCENDING : DESCENDING;
    if (src.peek() == ord.apply(TERM)) {
      // skip empty input buffer.
      src.get();
      return new byte[0];
    }
    final int offset = src.getOffset(), start = src.getPosition();
    int end;
    byte[] a = src.getBytes();
    for (end = start; (byte) (ord.apply(a[offset + end]) & 0x80) != TERM; end++)
      ;
    end++; // increment end to 1-past last byte
    // create ret buffer using length of encoded data + 1 (header byte)
    PositionedByteRange ret = new SimplePositionedMutableByteRange(blobVarDecodedLength(end - start
        + 1));
    int s = 6;
    byte t = (byte) ((ord.apply(a[offset + start]) << 1) & 0xff);
    for (int i = start + 1; i < end; i++) {
      if (s == 7) {
        ret.put((byte) (t | (ord.apply(a[offset + i]) & 0x7f)));
        i++;
               // explicitly reset t -- clean up overflow buffer after decoding
               // a full cycle and retain assertion condition below. This happens
        t = 0; // when the LSB in the last encoded byte is 1. (HBASE-9893)
      } else {
        ret.put((byte) (t | ((ord.apply(a[offset + i]) & 0x7f) >>> s)));
      }
      if (i == end) break;
      t = (byte) ((ord.apply(a[offset + i]) << 8 - s) & 0xff);
      s = s == 1 ? 7 : s - 1;
    }
    src.setPosition(end);
    assert t == 0 : "Unexpected bits remaining after decoding blob.";
    assert ret.getPosition() == ret.getLength() : "Allocated unnecessarily large return buffer.";
    return ret.getBytes();
  }

  /**
   * Encode a Blob value as a byte-for-byte copy. BlobCopy encoding in
   * DESCENDING order is NULL terminated so as to preserve proper sorting of
   * {@code []} and so it does not support {@code 0x00} in the value.
   * @return the number of bytes written.
   * @throws IllegalArgumentException when {@code ord} is DESCENDING and
   *    {@code val} contains a {@code 0x00} byte.
   */
  public static int encodeBlobCopy(PositionedByteRange dst, byte[] val, int voff, int vlen,
      Order ord) {
    if (null == val) {
      encodeNull(dst, ord);
      if (ASCENDING == ord) return 1;
      else {
        // DESCENDING ordered BlobCopy requires a termination bit to preserve
        // sort-order semantics of null values.
        dst.put(ord.apply(TERM));
        return 2;
      }
    }
    // Blobs as final entry in a compound key are written unencoded.
    assert dst.getRemaining() >= vlen + (ASCENDING == ord ? 1 : 2);
    if (DESCENDING == ord) {
      for (int i = 0; i < vlen; i++) {
        if (TERM == val[voff + i]) {
          throw new IllegalArgumentException("0x00 bytes not permitted in value.");
        }
      }
    }
    final int offset = dst.getOffset(), start = dst.getPosition();
    dst.put(BLOB_COPY);
    dst.put(val, voff, vlen);
    // DESCENDING ordered BlobCopy requires a termination bit to preserve
    // sort-order semantics of null values.
    if (DESCENDING == ord) dst.put(TERM);
    ord.apply(dst.getBytes(), offset + start, dst.getPosition() - start);
    return dst.getPosition() - start;
  }

  /**
   * Encode a Blob value as a byte-for-byte copy. BlobCopy encoding in
   * DESCENDING order is NULL terminated so as to preserve proper sorting of
   * {@code []} and so it does not support {@code 0x00} in the value.
   * @return the number of bytes written.
   * @throws IllegalArgumentException when {@code ord} is DESCENDING and
   *    {@code val} contains a {@code 0x00} byte.
   * @see #encodeBlobCopy(PositionedByteRange, byte[], int, int, Order)
   */
  public static int encodeBlobCopy(PositionedByteRange dst, byte[] val, Order ord) {
    return encodeBlobCopy(dst, val, 0, null != val ? val.length : 0, ord);
  }

  /**
   * Decode a Blob value, byte-for-byte copy.
   * @see #encodeBlobCopy(PositionedByteRange, byte[], int, int, Order)
   */
  public static byte[] decodeBlobCopy(PositionedByteRange src) {
    byte header = src.get();
    if (header == NULL || header == DESCENDING.apply(NULL)) {
      return null;
    }
    assert header == BLOB_COPY || header == DESCENDING.apply(BLOB_COPY);
    Order ord = header == BLOB_COPY ? ASCENDING : DESCENDING;
    final int length = src.getRemaining() - (ASCENDING == ord ? 0 : 1);
    byte[] ret = new byte[length];
    src.get(ret);
    ord.apply(ret, 0, ret.length);
    // DESCENDING ordered BlobCopy requires a termination bit to preserve
    // sort-order semantics of null values.
    if (DESCENDING == ord) src.get();
    return ret;
  }

  /**
   * Encode a null value.
   * @param dst The destination to which encoded digits are written.
   * @param ord The {@link Order} to respect while encoding {@code val}.
   * @return the number of bytes written.
   */
  public static int encodeNull(PositionedByteRange dst, Order ord) {
    dst.put(ord.apply(NULL));
    return 1;
  }

  /**
   * Encode an {@code int8} value using the fixed-length encoding.
   * @return the number of bytes written.
   * @see #encodeInt64(PositionedByteRange, long, Order)
   * @see #decodeInt8(PositionedByteRange)
   */
  public static int encodeInt8(PositionedByteRange dst, byte val, Order ord) {
    final int offset = dst.getOffset(), start = dst.getPosition();
    dst.put(FIXED_INT8)
       .put((byte) (val ^ 0x80));
    ord.apply(dst.getBytes(), offset + start, 2);
    return 2;
  }

  /**
   * Decode an {@code int8} value.
   * @see #encodeInt8(PositionedByteRange, byte, Order)
   */
  public static byte decodeInt8(PositionedByteRange src) {
    final byte header = src.get();
    assert header == FIXED_INT8 || header == DESCENDING.apply(FIXED_INT8);
    Order ord = header == FIXED_INT8 ? ASCENDING : DESCENDING;
    return (byte)((ord.apply(src.get()) ^ 0x80) & 0xff);
  }

  /**
   * Encode an {@code int16} value using the fixed-length encoding.
   * @return the number of bytes written.
   * @see #encodeInt64(PositionedByteRange, long, Order)
   * @see #decodeInt16(PositionedByteRange)
   */
  public static int encodeInt16(PositionedByteRange dst, short val, Order ord) {
    final int offset = dst.getOffset(), start = dst.getPosition();
    dst.put(FIXED_INT16)
       .put((byte) ((val >> 8) ^ 0x80))
       .put((byte) val);
    ord.apply(dst.getBytes(), offset + start, 3);
    return 3;
  }

  /**
   * Decode an {@code int16} value.
   * @see #encodeInt16(PositionedByteRange, short, Order)
   */
  public static short decodeInt16(PositionedByteRange src) {
    final byte header = src.get();
    assert header == FIXED_INT16 || header == DESCENDING.apply(FIXED_INT16);
    Order ord = header == FIXED_INT16 ? ASCENDING : DESCENDING;
    short val = (short) ((ord.apply(src.get()) ^ 0x80) & 0xff);
    val = (short) ((val << 8) + (ord.apply(src.get()) & 0xff));
    return val;
  }

  /**
   * Encode an {@code int32} value using the fixed-length encoding.
   * @return the number of bytes written.
   * @see #encodeInt64(PositionedByteRange, long, Order)
   * @see #decodeInt32(PositionedByteRange)
   */
  public static int encodeInt32(PositionedByteRange dst, int val, Order ord) {
    final int offset = dst.getOffset(), start = dst.getPosition();
    dst.put(FIXED_INT32)
        .put((byte) ((val >> 24) ^ 0x80))
        .put((byte) (val >> 16))
        .put((byte) (val >> 8))
        .put((byte) val);
    ord.apply(dst.getBytes(), offset + start, 5);
    return 5;
  }

  /**
   * Decode an {@code int32} value.
   * @see #encodeInt32(PositionedByteRange, int, Order)
   */
  public static int decodeInt32(PositionedByteRange src) {
    final byte header = src.get();
    assert header == FIXED_INT32 || header == DESCENDING.apply(FIXED_INT32);
    Order ord = header == FIXED_INT32 ? ASCENDING : DESCENDING;
    int val = (ord.apply(src.get()) ^ 0x80) & 0xff;
    for (int i = 1; i < 4; i++) {
      val = (val << 8) + (ord.apply(src.get()) & 0xff);
    }
    return val;
  }

  /**
   * Encode an {@code int64} value using the fixed-length encoding.
   * <p>
   * This format ensures that all longs sort in their natural order, as they
   * would sort when using signed long comparison.
   * </p>
   * <p>
   * All Longs are serialized to an 8-byte, fixed-width sortable byte format.
   * Serialization is performed by inverting the integer sign bit and writing
   * the resulting bytes to the byte array in big endian order. The encoded
   * value is prefixed by the {@link #FIXED_INT64} header byte. This encoding
   * is designed to handle java language primitives and so Null values are NOT
   * supported by this implementation.
   * </p>
   * <p>
   * For example:
   * </p>
   * <pre>
   * Input:   0x0000000000000005 (5)
   * Result:  0x288000000000000005
   *
   * Input:   0xfffffffffffffffb (-4)
   * Result:  0x280000000000000004
   *
   * Input:   0x7fffffffffffffff (Long.MAX_VALUE)
   * Result:  0x28ffffffffffffffff
   *
   * Input:   0x8000000000000000 (Long.MIN_VALUE)
   * Result:  0x287fffffffffffffff
   * </pre>
   * <p>
   * This encoding format, and much of this documentation string, is based on
   * Orderly's {@code FixedIntWritableRowKey}.
   * </p>
   * @return the number of bytes written.
   * @see #decodeInt64(PositionedByteRange)
   */
  public static int encodeInt64(PositionedByteRange dst, long val, Order ord) {
    final int offset = dst.getOffset(), start = dst.getPosition();
    dst.put(FIXED_INT64)
       .put((byte) ((val >> 56) ^ 0x80))
       .put((byte) (val >> 48))
       .put((byte) (val >> 40))
       .put((byte) (val >> 32))
       .put((byte) (val >> 24))
       .put((byte) (val >> 16))
       .put((byte) (val >> 8))
       .put((byte) val);
    ord.apply(dst.getBytes(), offset + start, 9);
    return 9;
  }

  /**
   * Decode an {@code int64} value.
   * @see #encodeInt64(PositionedByteRange, long, Order)
   */
  public static long decodeInt64(PositionedByteRange src) {
    final byte header = src.get();
    assert header == FIXED_INT64 || header == DESCENDING.apply(FIXED_INT64);
    Order ord = header == FIXED_INT64 ? ASCENDING : DESCENDING;
    long val = (ord.apply(src.get()) ^ 0x80) & 0xff;
    for (int i = 1; i < 8; i++) {
      val = (val << 8) + (ord.apply(src.get()) & 0xff);
    }
    return val;
  }

  /**
   * Encode a 32-bit floating point value using the fixed-length encoding.
   * Encoding format is described at length in
   * {@link #encodeFloat64(PositionedByteRange, double, Order)}.
   * @return the number of bytes written.
   * @see #decodeFloat32(PositionedByteRange)
   * @see #encodeFloat64(PositionedByteRange, double, Order)
   */
  public static int encodeFloat32(PositionedByteRange dst, float val, Order ord) {
    final int offset = dst.getOffset(), start = dst.getPosition();
    int i = Float.floatToIntBits(val);
    i ^= ((i >> Integer.SIZE - 1) | Integer.MIN_VALUE);
    dst.put(FIXED_FLOAT32)
        .put((byte) (i >> 24))
        .put((byte) (i >> 16))
        .put((byte) (i >> 8))
        .put((byte) i);
    ord.apply(dst.getBytes(), offset + start, 5);
    return 5;
  }

  /**
   * Decode a 32-bit floating point value using the fixed-length encoding.
   * @see #encodeFloat32(PositionedByteRange, float, Order)
   */
  public static float decodeFloat32(PositionedByteRange src) {
    final byte header = src.get();
    assert header == FIXED_FLOAT32 || header == DESCENDING.apply(FIXED_FLOAT32);
    Order ord = header == FIXED_FLOAT32 ? ASCENDING : DESCENDING;
    int val = ord.apply(src.get()) & 0xff;
    for (int i = 1; i < 4; i++) {
      val = (val << 8) + (ord.apply(src.get()) & 0xff);
    }
    val ^= (~val >> Integer.SIZE - 1) | Integer.MIN_VALUE;
    return Float.intBitsToFloat(val);
  }

  /**
   * Encode a 64-bit floating point value using the fixed-length encoding.
   * <p>
   * This format ensures the following total ordering of floating point
   * values: Double.NEGATIVE_INFINITY &lt; -Double.MAX_VALUE &lt; ... &lt;
   * -Double.MIN_VALUE &lt; -0.0 &lt; +0.0; &lt; Double.MIN_VALUE &lt; ...
   * &lt; Double.MAX_VALUE &lt; Double.POSITIVE_INFINITY &lt; Double.NaN
   * </p>
   * <p>
   * Floating point numbers are encoded as specified in IEEE 754. A 64-bit
   * double precision float consists of a sign bit, 11-bit unsigned exponent
   * encoded in offset-1023 notation, and a 52-bit significand. The format is
   * described further in the <a
   * href="http://en.wikipedia.org/wiki/Double_precision"> Double Precision
   * Floating Point Wikipedia page</a> </p>
   * <p>
   * The value of a normal float is -1 <sup>sign bit</sup> &times;
   * 2<sup>exponent - 1023</sup> &times; 1.significand
   * </p>
   * <p>
   * The IEE754 floating point format already preserves sort ordering for
   * positive floating point numbers when the raw bytes are compared in most
   * significant byte order. This is discussed further at <a href=
   * "http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.htm"
   * > http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.
   * htm</a>
   * </p>
   * <p>
   * Thus, we need only ensure that negative numbers sort in the the exact
   * opposite order as positive numbers (so that say, negative infinity is
   * less than negative 1), and that all negative numbers compare less than
   * any positive number. To accomplish this, we invert the sign bit of all
   * floating point numbers, and we also invert the exponent and significand
   * bits if the floating point number was negative.
   * </p>
   * <p>
   * More specifically, we first store the floating point bits into a 64-bit
   * long {@code l} using {@link Double#doubleToLongBits}. This method
   * collapses all NaNs into a single, canonical NaN value but otherwise
   * leaves the bits unchanged. We then compute
   * </p>
   * <pre>
   * l &circ;= (l &gt;&gt; (Long.SIZE - 1)) | Long.MIN_SIZE
   * </pre>
   * <p>
   * which inverts the sign bit and XOR's all other bits with the sign bit
   * itself. Comparing the raw bytes of {@code l} in most significant
   * byte order is equivalent to performing a double precision floating point
   * comparison on the underlying bits (ignoring NaN comparisons, as NaNs
   * don't compare equal to anything when performing floating point
   * comparisons).
   * </p>
   * <p>
   * The resulting long integer is then converted into a byte array by
   * serializing the long one byte at a time in most significant byte order.
   * The serialized integer is prefixed by a single header byte. All
   * serialized values are 9 bytes in length.
   * </p>
   * <p>
   * This encoding format, and much of this highly detailed documentation
   * string, is based on Orderly's {@code DoubleWritableRowKey}.
   * </p>
   * @return the number of bytes written.
   * @see #decodeFloat64(PositionedByteRange)
   */
  public static int encodeFloat64(PositionedByteRange dst, double val, Order ord) {
    final int offset = dst.getOffset(), start = dst.getPosition();
    long lng = Double.doubleToLongBits(val);
    lng ^= ((lng >> Long.SIZE - 1) | Long.MIN_VALUE);
    dst.put(FIXED_FLOAT64)
        .put((byte) (lng >> 56))
        .put((byte) (lng >> 48))
        .put((byte) (lng >> 40))
        .put((byte) (lng >> 32))
        .put((byte) (lng >> 24))
        .put((byte) (lng >> 16))
        .put((byte) (lng >> 8))
        .put((byte) lng);
    ord.apply(dst.getBytes(), offset + start, 9);
    return 9;
  }

  /**
   * Decode a 64-bit floating point value using the fixed-length encoding.
   * @see #encodeFloat64(PositionedByteRange, double, Order)
   */
  public static double decodeFloat64(PositionedByteRange src) {
    final byte header = src.get();
    assert header == FIXED_FLOAT64 || header == DESCENDING.apply(FIXED_FLOAT64);
    Order ord = header == FIXED_FLOAT64 ? ASCENDING : DESCENDING;
    long val = ord.apply(src.get()) & 0xff;
    for (int i = 1; i < 8; i++) {
      val = (val << 8) + (ord.apply(src.get()) & 0xff);
    }
    val ^= (~val >> Long.SIZE - 1) | Long.MIN_VALUE;
    return Double.longBitsToDouble(val);
  }

  /**
   * Returns true when {@code src} appears to be positioned an encoded value,
   * false otherwise.
   */
  public static boolean isEncodedValue(PositionedByteRange src) {
    return isNull(src) || isNumeric(src) || isFixedInt32(src) || isFixedInt64(src)
        || isFixedFloat32(src) || isFixedFloat64(src) || isText(src) || isBlobCopy(src)
        || isBlobVar(src);
  }

  /**
   * Return true when the next encoded value in {@code src} is null, false
   * otherwise.
   */
  public static boolean isNull(PositionedByteRange src) {
    return NULL ==
        (-1 == Integer.signum(src.peek()) ? DESCENDING : ASCENDING).apply(src.peek());
  }

  /**
   * Return true when the next encoded value in {@code src} uses Numeric
   * encoding, false otherwise. {@code NaN}, {@code +/-Inf} are valid Numeric
   * values.
   */
  public static boolean isNumeric(PositionedByteRange src) {
    byte x = (-1 == Integer.signum(src.peek()) ? DESCENDING : ASCENDING).apply(src.peek());
    return x >= NEG_INF && x <= NAN;
  }

  /**
   * Return true when the next encoded value in {@code src} uses Numeric
   * encoding and is {@code Infinite}, false otherwise.
   */
  public static boolean isNumericInfinite(PositionedByteRange src) {
    byte x = (-1 == Integer.signum(src.peek()) ? DESCENDING : ASCENDING).apply(src.peek());
    return NEG_INF == x || POS_INF == x;
  }

  /**
   * Return true when the next encoded value in {@code src} uses Numeric
   * encoding and is {@code NaN}, false otherwise.
   */
  public static boolean isNumericNaN(PositionedByteRange src) {
    return NAN == (-1 == Integer.signum(src.peek()) ? DESCENDING : ASCENDING).apply(src.peek());
  }

  /**
   * Return true when the next encoded value in {@code src} uses Numeric
   * encoding and is {@code 0}, false otherwise.
   */
  public static boolean isNumericZero(PositionedByteRange src) {
    return ZERO ==
        (-1 == Integer.signum(src.peek()) ? DESCENDING : ASCENDING).apply(src.peek());
  }

  /**
   * Return true when the next encoded value in {@code src} uses fixed-width
   * Int32 encoding, false otherwise.
   */
  public static boolean isFixedInt32(PositionedByteRange src) {
    return FIXED_INT32 ==
        (-1 == Integer.signum(src.peek()) ? DESCENDING : ASCENDING).apply(src.peek());
  }

  /**
   * Return true when the next encoded value in {@code src} uses fixed-width
   * Int64 encoding, false otherwise.
   */
  public static boolean isFixedInt64(PositionedByteRange src) {
    return FIXED_INT64 ==
        (-1 == Integer.signum(src.peek()) ? DESCENDING : ASCENDING).apply(src.peek());
  }

  /**
   * Return true when the next encoded value in {@code src} uses fixed-width
   * Float32 encoding, false otherwise.
   */
  public static boolean isFixedFloat32(PositionedByteRange src) {
    return FIXED_FLOAT32 ==
        (-1 == Integer.signum(src.peek()) ? DESCENDING : ASCENDING).apply(src.peek());
  }

  /**
   * Return true when the next encoded value in {@code src} uses fixed-width
   * Float64 encoding, false otherwise.
   */
  public static boolean isFixedFloat64(PositionedByteRange src) {
    return FIXED_FLOAT64 ==
        (-1 == Integer.signum(src.peek()) ? DESCENDING : ASCENDING).apply(src.peek());
  }

  /**
   * Return true when the next encoded value in {@code src} uses Text encoding,
   * false otherwise.
   */
  public static boolean isText(PositionedByteRange src) {
    return TEXT ==
        (-1 == Integer.signum(src.peek()) ? DESCENDING : ASCENDING).apply(src.peek());
  }

  /**
   * Return true when the next encoded value in {@code src} uses BlobVar
   * encoding, false otherwise.
   */
  public static boolean isBlobVar(PositionedByteRange src) {
    return BLOB_VAR ==
        (-1 == Integer.signum(src.peek()) ? DESCENDING : ASCENDING).apply(src.peek());
  }

  /**
   * Return true when the next encoded value in {@code src} uses BlobCopy
   * encoding, false otherwise.
   */
  public static boolean isBlobCopy(PositionedByteRange src) {
    return BLOB_COPY ==
        (-1 == Integer.signum(src.peek()) ? DESCENDING : ASCENDING).apply(src.peek());
  }

  /**
   * Skip {@code buff}'s position forward over one encoded value.
   * @return number of bytes skipped.
   */
  public static int skip(PositionedByteRange src) {
    final int start = src.getPosition();
    byte header = src.get();
    Order ord = (-1 == Integer.signum(header)) ? DESCENDING : ASCENDING;
    header = ord.apply(header);

    switch (header) {
      case NULL:
      case NEG_INF:
        return 1;
      case NEG_LARGE: /* Large negative number: 0x08, ~E, ~M */
        skipVaruint64(src, DESCENDING != ord);
        skipSignificand(src, DESCENDING != ord);
        return src.getPosition() - start;
      case NEG_MED_MIN: /* Medium negative number: 0x13-E, ~M */
      case NEG_MED_MIN + 0x01:
      case NEG_MED_MIN + 0x02:
      case NEG_MED_MIN + 0x03:
      case NEG_MED_MIN + 0x04:
      case NEG_MED_MIN + 0x05:
      case NEG_MED_MIN + 0x06:
      case NEG_MED_MIN + 0x07:
      case NEG_MED_MIN + 0x08:
      case NEG_MED_MIN + 0x09:
      case NEG_MED_MAX:
        skipSignificand(src, DESCENDING != ord);
        return src.getPosition() - start;
      case NEG_SMALL: /* Small negative number: 0x14, -E, ~M */
        skipVaruint64(src, DESCENDING == ord);
        skipSignificand(src, DESCENDING != ord);
        return src.getPosition() - start;
      case ZERO:
        return 1;
      case POS_SMALL: /* Small positive number: 0x16, ~-E, M */
        skipVaruint64(src, DESCENDING != ord);
        skipSignificand(src, DESCENDING == ord);
        return src.getPosition() - start;
      case POS_MED_MIN: /* Medium positive number: 0x17+E, M */
      case POS_MED_MIN + 0x01:
      case POS_MED_MIN + 0x02:
      case POS_MED_MIN + 0x03:
      case POS_MED_MIN + 0x04:
      case POS_MED_MIN + 0x05:
      case POS_MED_MIN + 0x06:
      case POS_MED_MIN + 0x07:
      case POS_MED_MIN + 0x08:
      case POS_MED_MIN + 0x09:
      case POS_MED_MAX:
        skipSignificand(src, DESCENDING == ord);
        return src.getPosition() - start;
      case POS_LARGE: /* Large positive number: 0x22, E, M */
        skipVaruint64(src, DESCENDING == ord);
        skipSignificand(src, DESCENDING == ord);
        return src.getPosition() - start;
      case POS_INF:
        return 1;
      case NAN:
        return 1;
      case FIXED_INT8:
        src.setPosition(src.getPosition() + 1);
        return src.getPosition() - start;
      case FIXED_INT16:
        src.setPosition(src.getPosition() + 2);
        return src.getPosition() - start;
      case FIXED_INT32:
        src.setPosition(src.getPosition() + 4);
        return src.getPosition() - start;
      case FIXED_INT64:
        src.setPosition(src.getPosition() + 8);
        return src.getPosition() - start;
      case FIXED_FLOAT32:
        src.setPosition(src.getPosition() + 4);
        return src.getPosition() - start;
      case FIXED_FLOAT64:
        src.setPosition(src.getPosition() + 8);
        return src.getPosition() - start;
      case TEXT:
        // for null-terminated values, skip to the end.
        do {
          header = ord.apply(src.get());
        } while (header != TERM);
        return src.getPosition() - start;
      case BLOB_VAR:
        // read until we find a 0 in the MSB
        do {
          header = ord.apply(src.get());
        } while ((byte) (header & 0x80) != TERM);
        return src.getPosition() - start;
      case BLOB_COPY:
        if (Order.DESCENDING == ord) {
          // if descending, read to termination byte.
          do {
            header = ord.apply(src.get());
          } while (header != TERM);
          return src.getPosition() - start;
        } else {
          // otherwise, just skip to the end.
          src.setPosition(src.getLength());
          return src.getPosition() - start;
        }
      default:
        throw unexpectedHeader(header);
    }
  }

  /**
   * Return the number of encoded entries remaining in {@code buff}. The
   * state of {@code buff} is not modified through use of this method.
   */
  public static int length(PositionedByteRange buff) {
    PositionedByteRange b =
        new SimplePositionedMutableByteRange(buff.getBytes(), buff.getOffset(), buff.getLength());
    b.setPosition(buff.getPosition());
    int cnt = 0;
    for (; isEncodedValue(b); skip(buff), cnt++)
      ;
    return cnt;
  }
}
