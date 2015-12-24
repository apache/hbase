/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.util;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.io.ByteBufferOutputStream;
import org.apache.hadoop.hbase.io.util.StreamUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.WritableUtils;

import sun.nio.ch.DirectBuffer;

/**
 * Utility functions for working with byte buffers, such as reading/writing
 * variable-length long numbers.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class ByteBufferUtils {

  // "Compressed integer" serialization helper constants.
  public final static int VALUE_MASK = 0x7f;
  public final static int NEXT_BIT_SHIFT = 7;
  public final static int NEXT_BIT_MASK = 1 << 7;
  private static final boolean UNSAFE_AVAIL = UnsafeAccess.isAvailable();
  private static final boolean UNSAFE_UNALIGNED = UnsafeAccess.unaligned();

  private ByteBufferUtils() {
  }

  /**
   * Similar to {@link WritableUtils#writeVLong(java.io.DataOutput, long)},
   * but writes to a {@link ByteBuffer}.
   */
  public static void writeVLong(ByteBuffer out, long i) {
    if (i >= -112 && i <= 127) {
      out.put((byte) i);
      return;
    }

    int len = -112;
    if (i < 0) {
      i ^= -1L; // take one's complement
      len = -120;
    }

    long tmp = i;
    while (tmp != 0) {
      tmp = tmp >> 8;
      len--;
    }

    out.put((byte) len);

    len = (len < -120) ? -(len + 120) : -(len + 112);

    for (int idx = len; idx != 0; idx--) {
      int shiftbits = (idx - 1) * 8;
      long mask = 0xFFL << shiftbits;
      out.put((byte) ((i & mask) >> shiftbits));
    }
  }

  /**
   * Similar to {@link WritableUtils#readVLong(DataInput)} but reads from a
   * {@link ByteBuffer}.
   */
  public static long readVLong(ByteBuffer in) {
    byte firstByte = in.get();
    int len = WritableUtils.decodeVIntSize(firstByte);
    if (len == 1) {
      return firstByte;
    }
    long i = 0;
    for (int idx = 0; idx < len-1; idx++) {
      byte b = in.get();
      i = i << 8;
      i = i | (b & 0xFF);
    }
    return (WritableUtils.isNegativeVInt(firstByte) ? (i ^ -1L) : i);
  }


  /**
   * Put in buffer integer using 7 bit encoding. For each written byte:
   * 7 bits are used to store value
   * 1 bit is used to indicate whether there is next bit.
   * @param value Int to be compressed.
   * @param out Where to put compressed data
   * @return Number of bytes written.
   * @throws IOException on stream error
   */
   public static int putCompressedInt(OutputStream out, final int value)
      throws IOException {
    int i = 0;
    int tmpvalue = value;
    do {
      byte b = (byte) (tmpvalue & VALUE_MASK);
      tmpvalue >>>= NEXT_BIT_SHIFT;
      if (tmpvalue != 0) {
        b |= (byte) NEXT_BIT_MASK;
      }
      out.write(b);
      i++;
    } while (tmpvalue != 0);
    return i;
  }

   /**
    * Put in output stream 32 bit integer (Big Endian byte order).
    * @param out Where to put integer.
    * @param value Value of integer.
    * @throws IOException On stream error.
    */
   public static void putInt(OutputStream out, final int value)
       throws IOException {
     // We have writeInt in ByteBufferOutputStream so that it can directly write
     // int to underlying
     // ByteBuffer in one step.
     if (out instanceof ByteBufferOutputStream) {
       ((ByteBufferOutputStream) out).writeInt(value);
     } else {
       StreamUtils.writeInt(out, value);
     }
   }

  public static byte toByte(ByteBuffer buffer, int offset) {
    if (UnsafeAccess.isAvailable()) {
      return UnsafeAccess.toByte(buffer, offset);
    } else {
      return buffer.get(offset);
    }
  }

  /**
   * Copy the data to the output stream and update position in buffer.
   * @param out the stream to write bytes to
   * @param in the buffer to read bytes from
   * @param length the number of bytes to copy
   */
  public static void moveBufferToStream(OutputStream out, ByteBuffer in,
      int length) throws IOException {
    copyBufferToStream(out, in, in.position(), length);
    skip(in, length);
  }

  /**
   * Copy data from a buffer to an output stream. Does not update the position
   * in the buffer.
   * @param out the stream to write bytes to
   * @param in the buffer to read bytes from
   * @param offset the offset in the buffer (from the buffer's array offset)
   *      to start copying bytes from
   * @param length the number of bytes to copy
   */
  public static void copyBufferToStream(OutputStream out, ByteBuffer in,
      int offset, int length) throws IOException {
    if (in.hasArray()) {
      out.write(in.array(), in.arrayOffset() + offset,
          length);
    } else {
      for (int i = 0; i < length; ++i) {
        out.write(toByte(in, offset + i));
      }
    }
  }

  public static int putLong(OutputStream out, final long value,
      final int fitInBytes) throws IOException {
    long tmpValue = value;
    for (int i = 0; i < fitInBytes; ++i) {
      out.write((byte) (tmpValue & 0xff));
      tmpValue >>>= 8;
    }
    return fitInBytes;
  }

  public static int putByte(ByteBuffer buffer, int offset, byte b) {
    if (UnsafeAccess.isAvailable()) {
      return UnsafeAccess.putByte(buffer, offset, b);
    } else {
      buffer.put(offset, b);
      return offset + 1;
    }
  }

  /**
   * Check how many bytes are required to store value.
   * @param value Value which size will be tested.
   * @return How many bytes are required to store value.
   */
  public static int longFitsIn(final long value) {
    if (value < 0) {
      return 8;
    }

    if (value < (1l << 4 * 8)) {
      // no more than 4 bytes
      if (value < (1l << 2 * 8)) {
        if (value < (1l << 1 * 8)) {
          return 1;
        }
        return 2;
      }
      if (value < (1l << 3 * 8)) {
        return 3;
      }
      return 4;
    }
    // more than 4 bytes
    if (value < (1l << 6 * 8)) {
      if (value < (1l << 5 * 8)) {
        return 5;
      }
      return 6;
    }
    if (value < (1l << 7 * 8)) {
      return 7;
    }
    return 8;
  }

  /**
   * Check how many bytes is required to store value.
   * @param value Value which size will be tested.
   * @return How many bytes are required to store value.
   */
  public static int intFitsIn(final int value) {
    if (value < 0) {
      return 4;
    }

    if (value < (1 << 2 * 8)) {
      if (value < (1 << 1 * 8)) {
        return 1;
      }
      return 2;
    }
    if (value <= (1 << 3 * 8)) {
      return 3;
    }
    return 4;
  }

  /**
   * Read integer from stream coded in 7 bits and increment position.
   * @return the integer that has been read
   * @throws IOException
   */
  public static int readCompressedInt(InputStream input)
      throws IOException {
    int result = 0;
    int i = 0;
    byte b;
    do {
      b = (byte) input.read();
      result += (b & VALUE_MASK) << (NEXT_BIT_SHIFT * i);
      i++;
      if (i > Bytes.SIZEOF_INT + 1) {
        throw new IllegalStateException(
            "Corrupted compressed int (too long: " + (i + 1) + " bytes)");
      }
    } while (0 != (b & NEXT_BIT_MASK));
    return result;
  }

  /**
   * Read integer from buffer coded in 7 bits and increment position.
   * @return Read integer.
   */
  public static int readCompressedInt(ByteBuffer buffer) {
    byte b = buffer.get();
    if ((b & NEXT_BIT_MASK) != 0) {
      return (b & VALUE_MASK) + (readCompressedInt(buffer) << NEXT_BIT_SHIFT);
    }
    return b & VALUE_MASK;
  }

  /**
   * Read long which was written to fitInBytes bytes and increment position.
   * @param fitInBytes In how many bytes given long is stored.
   * @return The value of parsed long.
   * @throws IOException
   */
  public static long readLong(InputStream in, final int fitInBytes)
      throws IOException {
    long tmpLong = 0;
    for (int i = 0; i < fitInBytes; ++i) {
      tmpLong |= (in.read() & 0xffl) << (8 * i);
    }
    return tmpLong;
  }

  /**
   * Read long which was written to fitInBytes bytes and increment position.
   * @param fitInBytes In how many bytes given long is stored.
   * @return The value of parsed long.
   */
  public static long readLong(ByteBuffer in, final int fitInBytes) {
    long tmpLength = 0;
    for (int i = 0; i < fitInBytes; ++i) {
      tmpLength |= (in.get() & 0xffl) << (8l * i);
    }
    return tmpLength;
  }

  /**
   * Copy the given number of bytes from the given stream and put it at the
   * current position of the given buffer, updating the position in the buffer.
   * @param out the buffer to write data to
   * @param in the stream to read data from
   * @param length the number of bytes to read/write
   */
  public static void copyFromStreamToBuffer(ByteBuffer out,
      DataInputStream in, int length) throws IOException {
    if (out.hasArray()) {
      in.readFully(out.array(), out.position() + out.arrayOffset(),
          length);
      skip(out, length);
    } else {
      for (int i = 0; i < length; ++i) {
        out.put(in.readByte());
      }
    }
  }

  /**
   * Copy from the InputStream to a new heap ByteBuffer until the InputStream is exhausted.
   */
  public static ByteBuffer drainInputStreamToBuffer(InputStream is) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
    IOUtils.copyBytes(is, baos, 4096, true);
    ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
    buffer.rewind();
    return buffer;
  }

  /**
   * Copy one buffer's whole data to another. Write starts at the current position of 'out' buffer.
   * Note : This will advance the position marker of {@code out} but not change the position maker
   * for {@code in}. The position and limit of the {@code in} buffer to be set properly by caller.
   * @param in source buffer
   * @param out destination buffer
   */
  public static void copyFromBufferToBuffer(ByteBuffer in, ByteBuffer out) {
    if (UnsafeAccess.isAvailable()) {
      int length = in.remaining();
      UnsafeAccess.copy(in, in.position(), out, out.position(), length);
      out.position(out.position() + length);
    } else {
      out.put(in);
    }
  }

  /**
   * Copy from one buffer to another from given offset. This will be absolute positional copying and
   * won't affect the position of any of the buffers.
   * @param in
   * @param out
   * @param sourceOffset
   * @param destinationOffset
   * @param length
   */
  public static int copyFromBufferToBuffer(ByteBuffer in, ByteBuffer out, int sourceOffset,
      int destinationOffset, int length) {
    if (in.hasArray() && out.hasArray()) {
      System.arraycopy(in.array(), sourceOffset + in.arrayOffset(), out.array(), out.arrayOffset()
          + destinationOffset, length);
    } else if (UNSAFE_AVAIL) {
      UnsafeAccess.copy(in, sourceOffset, out, destinationOffset, length);
    } else {
      int outOldPos = out.position();
      out.position(destinationOffset);
      ByteBuffer inDup = in.duplicate();
      inDup.position(sourceOffset).limit(sourceOffset + length);
      out.put(inDup);
      out.position(outOldPos);
    }
    return destinationOffset + length;
  }

  /**
   * Copy from one buffer to another from given offset.
   * <p>
   * Note : This will advance the position marker of {@code out} but not change the position maker
   * for {@code in}
   * @param in source buffer
   * @param out destination buffer
   * @param sourceOffset offset in the source buffer
   * @param length how many bytes to copy
   */
  public static void copyFromBufferToBuffer(ByteBuffer in,
      ByteBuffer out, int sourceOffset, int length) {
    if (in.hasArray() && out.hasArray()) {
      System.arraycopy(in.array(), sourceOffset + in.arrayOffset(), out.array(), out.position()
          + out.arrayOffset(), length);
      skip(out, length);
    } else if (UNSAFE_AVAIL) {
      UnsafeAccess.copy(in, sourceOffset, out, out.position(), length);
      skip(out, length);
    } else {
      ByteBuffer inDup = in.duplicate();
      inDup.position(sourceOffset).limit(sourceOffset + length);
      out.put(inDup);
    }
  }

  /**
   * Find length of common prefix of two parts in the buffer
   * @param buffer Where parts are located.
   * @param offsetLeft Offset of the first part.
   * @param offsetRight Offset of the second part.
   * @param limit Maximal length of common prefix.
   * @return Length of prefix.
   */
  public static int findCommonPrefix(ByteBuffer buffer, int offsetLeft,
      int offsetRight, int limit) {
    int prefix = 0;

    for (; prefix < limit; ++prefix) {
      if (buffer.get(offsetLeft + prefix) != buffer.get(offsetRight + prefix)) {
        break;
      }
    }

    return prefix;
  }

  /**
   * Find length of common prefix in two arrays.
   * @param left Array to be compared.
   * @param leftOffset Offset in left array.
   * @param leftLength Length of left array.
   * @param right Array to be compared.
   * @param rightOffset Offset in right array.
   * @param rightLength Length of right array.
   */
  public static int findCommonPrefix(
      byte[] left, int leftOffset, int leftLength,
      byte[] right, int rightOffset, int rightLength) {
    int length = Math.min(leftLength, rightLength);
    int result = 0;

    while (result < length &&
        left[leftOffset + result] == right[rightOffset + result]) {
      result++;
    }

    return result;
  }

  /**
   * Find length of common prefix in two arrays.
   * @param left ByteBuffer to be compared.
   * @param leftOffset Offset in left ByteBuffer.
   * @param leftLength Length of left ByteBuffer.
   * @param right ByteBuffer to be compared.
   * @param rightOffset Offset in right ByteBuffer.
   * @param rightLength Length of right ByteBuffer.
   */
  public static int findCommonPrefix(ByteBuffer left, int leftOffset, int leftLength,
      ByteBuffer right, int rightOffset, int rightLength) {
    int length = Math.min(leftLength, rightLength);
    int result = 0;

    while (result < length && ByteBufferUtils.toByte(left, leftOffset + result) == ByteBufferUtils
        .toByte(right, rightOffset + result)) {
      result++;
    }

    return result;
  }

  /**
   * Check whether two parts in the same buffer are equal.
   * @param buffer In which buffer there are parts
   * @param offsetLeft Beginning of first part.
   * @param lengthLeft Length of the first part.
   * @param offsetRight Beginning of the second part.
   * @param lengthRight Length of the second part.
   * @return True if equal
   */
  public static boolean arePartsEqual(ByteBuffer buffer,
      int offsetLeft, int lengthLeft,
      int offsetRight, int lengthRight) {
    if (lengthLeft != lengthRight) {
      return false;
    }

    if (buffer.hasArray()) {
      return 0 == Bytes.compareTo(
          buffer.array(), buffer.arrayOffset() + offsetLeft, lengthLeft,
          buffer.array(), buffer.arrayOffset() + offsetRight, lengthRight);
    }

    for (int i = 0; i < lengthRight; ++i) {
      if (buffer.get(offsetLeft + i) != buffer.get(offsetRight + i)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Increment position in buffer.
   * @param buffer In this buffer.
   * @param length By that many bytes.
   */
  public static void skip(ByteBuffer buffer, int length) {
    buffer.position(buffer.position() + length);
  }

  public static void extendLimit(ByteBuffer buffer, int numBytes) {
    buffer.limit(buffer.limit() + numBytes);
  }

  /**
   * Copy the bytes from position to limit into a new byte[] of the exact length and sets the
   * position and limit back to their original values (though not thread safe).
   * @param buffer copy from here
   * @param startPosition put buffer.get(startPosition) into byte[0]
   * @return a new byte[] containing the bytes in the specified range
   */
  public static byte[] toBytes(ByteBuffer buffer, int startPosition) {
    int originalPosition = buffer.position();
    byte[] output = new byte[buffer.limit() - startPosition];
    buffer.position(startPosition);
    buffer.get(output);
    buffer.position(originalPosition);
    return output;
  }

  /**
   * Copy the given number of bytes from specified offset into a new byte[]
   * @param buffer
   * @param offset
   * @param length
   * @return a new byte[] containing the bytes in the specified range
   */
  public static byte[] toBytes(ByteBuffer buffer, int offset, int length) {
    byte[] output = new byte[length];
    for (int i = 0; i < length; i++) {
      output[i] = buffer.get(offset + i);
    }
    return output;
  }

  public static boolean equals(ByteBuffer buf1, int o1, int l1, ByteBuffer buf2, int o2, int l2) {
    // Since we're often comparing adjacent sorted data,
    // it's usual to have equal arrays except for the very last byte
    // so check that first
    if (toByte(buf1, o1 + l1 - 1) != toByte(buf2, o2 + l2 - 1)) return false;
    return compareTo(buf1, o1, l1, buf2, o2, l2) == 0;
  }

  public static int compareTo(ByteBuffer buf1, int o1, int l1, ByteBuffer buf2, int o2, int l2) {
    if (UNSAFE_UNALIGNED) {
      long offset1Adj, offset2Adj;
      Object refObj1 = null, refObj2 = null;
      if (buf1.isDirect()) {
        offset1Adj = o1 + ((DirectBuffer) buf1).address();
      } else {
        offset1Adj = o1 + buf1.arrayOffset() + UnsafeAccess.BYTE_ARRAY_BASE_OFFSET;
        refObj1 = buf1.array();
      }
      if (buf2.isDirect()) {
        offset2Adj = o2 + ((DirectBuffer) buf2).address();
      } else {
        offset2Adj = o2 + buf2.arrayOffset() + UnsafeAccess.BYTE_ARRAY_BASE_OFFSET;
        refObj2 = buf2.array();
      }
      return compareToUnsafe(refObj1, offset1Adj, l1, refObj2, offset2Adj, l2);
    }
    int end1 = o1 + l1;
    int end2 = o2 + l2;
    for (int i = o1, j = o2; i < end1 && j < end2; i++, j++) {
      int a = buf1.get(i) & 0xFF;
      int b = buf2.get(j) & 0xFF;
      if (a != b) {
        return a - b;
      }
    }
    return l1 - l2;
  }

  public static boolean equals(ByteBuffer buf1, int o1, int l1, byte[] buf2, int o2, int l2) {
    // Since we're often comparing adjacent sorted data,
    // it's usual to have equal arrays except for the very last byte
    // so check that first
    if (toByte(buf1, o1 + l1 - 1) != buf2[o2 + l2 - 1]) return false;
    return compareTo(buf1, o1, l1, buf2, o2, l2) == 0;
  }

  public static int compareTo(ByteBuffer buf1, int o1, int l1, byte[] buf2, int o2, int l2) {
    if (UNSAFE_UNALIGNED) {
      long offset1Adj;
      Object refObj1 = null;
      if (buf1.isDirect()) {
        offset1Adj = o1 + ((DirectBuffer) buf1).address();
      } else {
        offset1Adj = o1 + buf1.arrayOffset() + UnsafeAccess.BYTE_ARRAY_BASE_OFFSET;
        refObj1 = buf1.array();
      }
      return compareToUnsafe(refObj1, offset1Adj, l1, buf2, o2
          + UnsafeAccess.BYTE_ARRAY_BASE_OFFSET, l2);
    }
    int end1 = o1 + l1;
    int end2 = o2 + l2;
    for (int i = o1, j = o2; i < end1 && j < end2; i++, j++) {
      int a = buf1.get(i) & 0xFF;
      int b = buf2[j] & 0xFF;
      if (a != b) {
        return a - b;
      }
    }
    return l1 - l2;
  }

  static int compareToUnsafe(Object obj1, long o1, int l1, Object obj2, long o2, int l2) {
    final int minLength = Math.min(l1, l2);
    final int minWords = minLength / Bytes.SIZEOF_LONG;

    /*
     * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a time is no slower than
     * comparing 4 bytes at a time even on 32-bit. On the other hand, it is substantially faster on
     * 64-bit.
     */
    int j = minWords << 3; // Same as minWords * SIZEOF_LONG
    for (int i = 0; i < j; i += Bytes.SIZEOF_LONG) {
      long lw = UnsafeAccess.theUnsafe.getLong(obj1, o1 + i);
      long rw = UnsafeAccess.theUnsafe.getLong(obj2, o2 + i);
      long diff = lw ^ rw;
      if (diff != 0) {
        return lessThanUnsignedLong(lw, rw) ? -1 : 1;
      }
    }
    int offset = j;

    if (minLength - offset >= Bytes.SIZEOF_INT) {
      int il = UnsafeAccess.theUnsafe.getInt(obj1, o1 + offset);
      int ir = UnsafeAccess.theUnsafe.getInt(obj2, o2 + offset);
      if (il != ir) {
        return lessThanUnsignedInt(il, ir) ? -1 : 1;
      }
      offset += Bytes.SIZEOF_INT;
    }
    if (minLength - offset >= Bytes.SIZEOF_SHORT) {
      short sl = UnsafeAccess.theUnsafe.getShort(obj1, o1 + offset);
      short sr = UnsafeAccess.theUnsafe.getShort(obj2, o2 + offset);
      if (sl != sr) {
        return lessThanUnsignedShort(sl, sr) ? -1 : 1;
      }
      offset += Bytes.SIZEOF_SHORT;
    }
    if (minLength - offset == 1) {
      int a = (UnsafeAccess.theUnsafe.getByte(obj1, o1 + offset) & 0xff);
      int b = (UnsafeAccess.theUnsafe.getByte(obj2, o2 + offset) & 0xff);
      if (a != b) {
        return a - b;
      }
    }
    return l1 - l2;
  }

  /*
   * Both values are passed as is read by Unsafe. When platform is Little Endian, have to convert
   * to corresponding Big Endian value and then do compare. We do all writes in Big Endian format.
   */
  private static boolean lessThanUnsignedLong(long x1, long x2) {
    if (UnsafeAccess.littleEndian) {
      x1 = Long.reverseBytes(x1);
      x2 = Long.reverseBytes(x2);
    }
    return (x1 + Long.MIN_VALUE) < (x2 + Long.MIN_VALUE);
  }

  /*
   * Both values are passed as is read by Unsafe. When platform is Little Endian, have to convert
   * to corresponding Big Endian value and then do compare. We do all writes in Big Endian format.
   */
  private static boolean lessThanUnsignedInt(int x1, int x2) {
    if (UnsafeAccess.littleEndian) {
      x1 = Integer.reverseBytes(x1);
      x2 = Integer.reverseBytes(x2);
    }
    return (x1 & 0xffffffffL) < (x2 & 0xffffffffL);
  }

  /*
   * Both values are passed as is read by Unsafe. When platform is Little Endian, have to convert
   * to corresponding Big Endian value and then do compare. We do all writes in Big Endian format.
   */
  private static boolean lessThanUnsignedShort(short x1, short x2) {
    if (UnsafeAccess.littleEndian) {
      x1 = Short.reverseBytes(x1);
      x2 = Short.reverseBytes(x2);
    }
    return (x1 & 0xffff) < (x2 & 0xffff);
  }

  /**
   * Reads a short value at the given buffer's offset.
   * @param buffer
   * @param offset
   * @return short value at offset
   */
  public static short toShort(ByteBuffer buffer, int offset) {
    if (UNSAFE_UNALIGNED) {
      return UnsafeAccess.toShort(buffer, offset);
    } else {
      return buffer.getShort(offset);
    }
  }

  /**
   * Reads an int value at the given buffer's offset.
   * @param buffer
   * @param offset
   * @return int value at offset
   */
  public static int toInt(ByteBuffer buffer, int offset) {
    if (UNSAFE_UNALIGNED) {
      return UnsafeAccess.toInt(buffer, offset);
    } else {
      return buffer.getInt(offset);
    }
  }

  /**
   * Reads a long value at the given buffer's offset.
   * @param buffer
   * @param offset
   * @return long value at offset
   */
  public static long toLong(ByteBuffer buffer, int offset) {
    if (UNSAFE_UNALIGNED) {
      return UnsafeAccess.toLong(buffer, offset);
    } else {
      return buffer.getLong(offset);
    }
  }

  /**
   * Put an int value out to the given ByteBuffer's current position in big-endian format.
   * This also advances the position in buffer by int size.
   * @param buffer the ByteBuffer to write to
   * @param val int to write out
   */
  public static void putInt(ByteBuffer buffer, int val) {
    if (UNSAFE_UNALIGNED) {
      int newPos = UnsafeAccess.putInt(buffer, buffer.position(), val);
      buffer.position(newPos);
    } else {
      buffer.putInt(val);
    }
  }

  /**
   * Reads a double value at the given buffer's offset.
   * @param buffer
   * @param offset offset where double is
   * @return double value at offset
   */
  public static double toDouble(ByteBuffer buffer, int offset) {
    return Double.longBitsToDouble(toLong(buffer, offset));
  }

  /**
   * Reads a BigDecimal value at the given buffer's offset.
   * @param buffer
   * @param offset
   * @return BigDecimal value at offset
   */
  public static BigDecimal toBigDecimal(ByteBuffer buffer, int offset, int length) {
    if (buffer == null || length < Bytes.SIZEOF_INT + 1 ||
      (offset + length > buffer.limit())) {
      return null;
    }

    int scale = toInt(buffer, offset);
    byte[] tcBytes = new byte[length - Bytes.SIZEOF_INT];
    copyFromBufferToArray(tcBytes, buffer, offset + Bytes.SIZEOF_INT, 0, length - Bytes.SIZEOF_INT);
    return new BigDecimal(new BigInteger(tcBytes), scale);
  }

  /**
   * Put a short value out to the given ByteBuffer's current position in big-endian format.
   * This also advances the position in buffer by short size.
   * @param buffer the ByteBuffer to write to
   * @param val short to write out
   */
  public static void putShort(ByteBuffer buffer, short val) {
    if (UNSAFE_UNALIGNED) {
      int newPos = UnsafeAccess.putShort(buffer, buffer.position(), val);
      buffer.position(newPos);
    } else {
      buffer.putShort(val);
    }
  }

  /**
   * Put a long value out to the given ByteBuffer's current position in big-endian format.
   * This also advances the position in buffer by long size.
   * @param buffer the ByteBuffer to write to
   * @param val long to write out
   */
  public static void putLong(ByteBuffer buffer, long val) {
    if (UNSAFE_UNALIGNED) {
      int newPos = UnsafeAccess.putLong(buffer, buffer.position(), val);
      buffer.position(newPos);
    } else {
      buffer.putLong(val);
    }
  }
  /**
   * Copies the bytes from given array's offset to length part into the given buffer. Puts the bytes
   * to buffer's current position. This also advances the position in the 'out' buffer by 'length'
   * @param out
   * @param in
   * @param inOffset
   * @param length
   */
  public static void copyFromArrayToBuffer(ByteBuffer out, byte[] in, int inOffset, int length) {
    if (out.hasArray()) {
      System.arraycopy(in, inOffset, out.array(), out.arrayOffset() + out.position(), length);
      // Move the position in out by length
      out.position(out.position() + length);
    } else if (UNSAFE_AVAIL) {
      UnsafeAccess.copy(in, inOffset, out, out.position(), length);
      // Move the position in out by length
      out.position(out.position() + length);
    } else {
      out.put(in, inOffset, length);
    }
  }

  /**
   * Copies specified number of bytes from given offset of 'in' ByteBuffer to
   * the array.
   * @param out
   * @param in
   * @param sourceOffset
   * @param destinationOffset
   * @param length
   */
  public static void copyFromBufferToArray(byte[] out, ByteBuffer in, int sourceOffset,
      int destinationOffset, int length) {
    if (in.hasArray()) {
      System.arraycopy(in.array(), sourceOffset + in.arrayOffset(), out, destinationOffset, length);
    } else if (UNSAFE_AVAIL) {
      UnsafeAccess.copy(in, sourceOffset, out, destinationOffset, length);
    } else {
      int oldPos = in.position();
      in.position(sourceOffset);
      in.get(out, destinationOffset, length);
      in.position(oldPos);
    }
  }

  public static void writeByteBuffer(OutputStream out, ByteBuffer b, int offset, int length)
      throws IOException {
    // We have write which takes ByteBuffer in ByteBufferOutputStream so that it
    // can directly write
    // bytes from the src ByteBuffer to the destination ByteBuffer. This avoid
    // need for temp array
    // creation and copy
    if (out instanceof ByteBufferOutputStream) {
      ((ByteBufferOutputStream) out).write(b, offset, length);
    } else {
      ByteBufferUtils.copyBufferToStream(out, b, offset, length);
    }
  }
  // For testing purpose
  public static String toStringBinary(final ByteBuffer b, int off, int len) {
    StringBuilder result = new StringBuilder();
    // Just in case we are passed a 'len' that is > buffer length...
    if (off >= b.capacity())
      return result.toString();
    if (off + len > b.capacity())
      len = b.capacity() - off;
    for (int i = off; i < off + len; ++i) {
      int ch = b.get(i) & 0xFF;
      if ((ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z')
          || " `~!@#$%^&*()-_=+[]{}|;:'\",.<>/?".indexOf(ch) >= 0) {
        result.append((char) ch);
      } else {
        result.append(String.format("\\x%02X", ch));
      }
    }
    return result.toString();
  }

  public static String toStringBinary(final ByteBuffer b) {
    return toStringBinary(b, 0, b.capacity());
  }
}
