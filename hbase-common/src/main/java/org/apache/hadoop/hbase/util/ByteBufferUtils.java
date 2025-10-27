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
package org.apache.hadoop.hbase.util;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.apache.hadoop.hbase.io.ByteBufferWriter;
import org.apache.hadoop.hbase.io.util.StreamUtils;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.unsafe.HBasePlatformDependent;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.WritableUtils;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Utility functions for working with byte buffers, such as reading/writing variable-length long
 * numbers.
 */
@InterfaceAudience.Private
public final class ByteBufferUtils {
  // "Compressed integer" serialization helper constants.
  public final static int VALUE_MASK = 0x7f;
  public final static int NEXT_BIT_SHIFT = 7;
  public final static int NEXT_BIT_MASK = 1 << 7;
  final static boolean UNSAFE_AVAIL = HBasePlatformDependent.isUnsafeAvailable();

  /*
   * The VarHandles below must be used directly, not via non-static aliases. Non-static usage of a
   * VarHandle is much slower than static usage.
   */
  private static final VarHandle BYTE_BUFFER_SHORT_VIEW_BIG_ENDIAN_VAR_HANDLE =
    MethodHandles.byteBufferViewVarHandle(short[].class, ByteOrder.BIG_ENDIAN);
  private static final VarHandle BYTE_BUFFER_INT_VIEW_BIG_ENDIAN_VAR_HANDLE =
    MethodHandles.byteBufferViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
  private static final VarHandle BYTE_BUFFER_LONG_VIEW_BIG_ENDIAN_VAR_HANDLE =
    MethodHandles.byteBufferViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);
  private static final VarHandle BYTE_ARRAY_LONG_VIEW_BIG_ENDIAN_VAR_HANDLE =
    MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

  private ByteBufferUtils() {
  }

  static abstract class Comparer {
    abstract int compareTo(byte[] buf1, int o1, int l1, ByteBuffer buf2, int o2, int l2);

    abstract int compareTo(ByteBuffer buf1, int o1, int l1, ByteBuffer buf2, int o2, int l2);
  }

  static abstract class CommonPrefixer {
    abstract int findCommonPrefix(ByteBuffer left, int leftOffset, int leftLength, byte[] right,
      int rightOffset, int rightLength);

    abstract int findCommonPrefix(ByteBuffer left, int leftOffset, int leftLength, ByteBuffer right,
      int rightOffset, int rightLength);
  }

  /**
   * <a href=
   * "https://github.com/google/guava/blob/v21.0/guava/src/com/google/common/primitives/UnsignedBytes.java#L362">Adapted
   * from Guava</a>
   */
  static class ComparerHolder {
    static final Comparer BEST_COMPARER = getBestComparer();

    static Comparer getBestComparer() {
      return VarHandleComparer.INSTANCE;
    }

    static final class VarHandleComparer extends Comparer {

      static final VarHandleComparer INSTANCE = new VarHandleComparer();
      private static final int STRIDE = Long.BYTES;

      @Override
      int compareTo(byte[] buf1, int o1, int l1, ByteBuffer buf2, int o2, int l2) {
        final int minLength = Math.min(l1, l2);
        int strideLimit = minLength & ~(STRIDE - 1);
        int i;

        for (i = 0; i < strideLimit; i += STRIDE) {
          // big-endian fetches are faster than little-endian because the bytes don't need to get
          // reversed
          long lw = (long) BYTE_ARRAY_LONG_VIEW_BIG_ENDIAN_VAR_HANDLE.get(buf1, o1 + i);
          long rw = (long) BYTE_BUFFER_LONG_VIEW_BIG_ENDIAN_VAR_HANDLE.get(buf2, o2 + i);
          if (lw != rw) {
            return ((lw + Long.MIN_VALUE) < (rw + Long.MIN_VALUE)) ? -1 : 1;
          }
        }

        // The epilogue to cover the last (minLength % stride) elements.
        for (; i < minLength; i++) {
          int il = buf1[o1 + i] & 0xFF;
          int ir = buf2.get(o2 + i) & 0xFF;
          if (il != ir) {
            return il - ir;
          }
        }
        return l1 - l2;
      }

      @Override
      public int compareTo(ByteBuffer buf1, int o1, int l1, ByteBuffer buf2, int o2, int l2) {
        final int minLength = Math.min(l1, l2);
        int strideLimit = minLength & ~(STRIDE - 1);
        int i;

        for (i = 0; i < strideLimit; i += STRIDE) {
          // big-endian fetches are faster than little-endian because the bytes don't need to get
          // reversed
          long lw = (long) BYTE_BUFFER_LONG_VIEW_BIG_ENDIAN_VAR_HANDLE.get(buf1, o1 + i);
          long rw = (long) BYTE_BUFFER_LONG_VIEW_BIG_ENDIAN_VAR_HANDLE.get(buf2, o2 + i);
          if (lw != rw) {
            return ((lw + Long.MIN_VALUE) < (rw + Long.MIN_VALUE)) ? -1 : 1;
          }
        }

        // The epilogue to cover the last (minLength % stride) elements.
        for (; i < minLength; i++) {
          int il = buf1.get(o1 + i) & 0xFF;
          int ir = buf2.get(o2 + i) & 0xFF;
          if (il != ir) {
            return il - ir;
          }
        }
        return l1 - l2;
      }
    }
  }

  static class CommonPrefixerHolder {
    static final CommonPrefixer BEST_COMMON_PREFIXER = getBestCommonPrefixer();

    static CommonPrefixer getBestCommonPrefixer() {
      return VarHandleCommonPrefixer.INSTANCE;
    }

    static final class VarHandleCommonPrefixer extends CommonPrefixer {

      static final VarHandleCommonPrefixer INSTANCE = new VarHandleCommonPrefixer();

      private static final int STRIDE = Long.BYTES;

      @Override
      public int findCommonPrefix(ByteBuffer left, int leftOffset, int leftLength, byte[] right,
        int rightOffset, int rightLength) {
        final int minLength = Math.min(leftLength, rightLength);
        int strideLimit = minLength & ~(STRIDE - 1);
        int i;

        for (i = 0; i < strideLimit; i += STRIDE) {
          // big-endian fetches are faster than little-endian because the bytes don't need to get
          // reversed
          long lw = (long) BYTE_BUFFER_LONG_VIEW_BIG_ENDIAN_VAR_HANDLE.get(left, leftOffset + i);
          long rw = (long) BYTE_ARRAY_LONG_VIEW_BIG_ENDIAN_VAR_HANDLE.get(right, rightOffset + i);

          if (lw != rw) {
            return i + (Long.numberOfLeadingZeros(lw ^ rw) / Bytes.SIZEOF_LONG);
          }
        }

        // The epilogue to cover the last (minLength % stride) elements.
        for (; i < minLength; i++) {
          byte il = left.get(leftOffset + i);
          byte ir = right[rightOffset + i];
          if (il != ir) {
            return i;
          }
        }

        return i;
      }

      @Override
      public int findCommonPrefix(ByteBuffer left, int leftOffset, int leftLength, ByteBuffer right,
        int rightOffset, int rightLength) {
        final int minLength = Math.min(leftLength, rightLength);
        int strideLimit = minLength & ~(STRIDE - 1);
        int i;

        for (i = 0; i < strideLimit; i += STRIDE) {
          // big-endian fetches are faster than little-endian because the bytes don't need to get
          // reversed
          long lw = (long) BYTE_BUFFER_LONG_VIEW_BIG_ENDIAN_VAR_HANDLE.get(left, leftOffset + i);
          long rw = (long) BYTE_BUFFER_LONG_VIEW_BIG_ENDIAN_VAR_HANDLE.get(right, rightOffset + i);

          if (lw != rw) {
            return i + (Long.numberOfLeadingZeros(lw ^ rw) / Bytes.SIZEOF_LONG);
          }
        }

        // The epilogue to cover the last (minLength % stride) elements.
        for (; i < minLength; i++) {
          byte il = left.get(leftOffset + i);
          byte ir = right.get(rightOffset + i);
          if (il != ir) {
            return i;
          }
        }

        return i;
      }
    }
  }

  /**
   * Similar to {@link WritableUtils#writeVLong(java.io.DataOutput, long)}, but writes to a
   * {@link ByteBuffer}.
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
   * Similar to {@link WritableUtils#readVLong(java.io.DataInput)} but reads from a
   * {@link ByteBuff}.
   */
  public static long readVLong(ByteBuff buf) {
    byte firstByte = buf.get();
    int len = WritableUtils.decodeVIntSize(firstByte);
    if (len == 1) {
      return firstByte;
    } else {
      int remaining = len - 1;
      long i = 0;
      int offsetFromPos = 0;
      if (remaining >= Bytes.SIZEOF_INT) {
        // The int read has to be converted to unsigned long so the & op
        i = (buf.getIntAfterPosition(offsetFromPos) & 0x00000000ffffffffL);
        remaining -= Bytes.SIZEOF_INT;
        offsetFromPos += Bytes.SIZEOF_INT;
      }
      if (remaining >= Bytes.SIZEOF_SHORT) {
        short s = buf.getShortAfterPosition(offsetFromPos);
        i = i << 16;
        i = i | (s & 0xFFFF);
        remaining -= Bytes.SIZEOF_SHORT;
        offsetFromPos += Bytes.SIZEOF_SHORT;
      }
      for (int idx = 0; idx < remaining; idx++) {
        byte b = buf.getByteAfterPosition(offsetFromPos + idx);
        i = i << 8;
        i = i | (b & 0xFF);
      }
      buf.skip(len - 1);
      return WritableUtils.isNegativeVInt(firstByte) ? ~i : i;
    }
  }

  /**
   * Similar to {@link WritableUtils#readVLong(DataInput)} but reads from a {@link ByteBuffer}.
   */
  public static long readVLong(ByteBuffer buf) {
    byte firstByte = buf.get();
    int len = WritableUtils.decodeVIntSize(firstByte);
    if (len == 1) {
      return firstByte;
    } else {
      int remaining = len - 1;
      long i = 0;
      int offsetFromPos = 0;
      if (remaining >= Bytes.SIZEOF_INT) {
        // The int read has to be converted to unsigned long so the & op
        i = (buf.getInt(buf.position() + offsetFromPos) & 0x00000000ffffffffL);
        remaining -= Bytes.SIZEOF_INT;
        offsetFromPos += Bytes.SIZEOF_INT;
      }
      if (remaining >= Bytes.SIZEOF_SHORT) {
        short s = buf.getShort(buf.position() + offsetFromPos);
        i = i << 16;
        i = i | (s & 0xFFFF);
        remaining -= Bytes.SIZEOF_SHORT;
        offsetFromPos += Bytes.SIZEOF_SHORT;
      }
      for (int idx = 0; idx < remaining; idx++) {
        byte b = buf.get(buf.position() + offsetFromPos + idx);
        i = i << 8;
        i = i | (b & 0xFF);
      }
      buf.position(buf.position() + len - 1);
      return WritableUtils.isNegativeVInt(firstByte) ? ~i : i;
    }
  }

  /**
   * Put in buffer integer using 7 bit encoding. For each written byte: 7 bits are used to store
   * value 1 bit is used to indicate whether there is next bit.
   * @param value Int to be compressed.
   * @param out   Where to put compressed data
   * @return Number of bytes written.
   * @throws IOException on stream error
   */
  public static int putCompressedInt(OutputStream out, final int value) throws IOException {
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
   * @param out   Where to put integer.
   * @param value Value of integer.
   * @throws IOException On stream error.
   */
  public static void putInt(OutputStream out, final int value) throws IOException {
    // We have writeInt in ByteBufferOutputStream so that it can directly write
    // int to underlying
    // ByteBuffer in one step.
    if (out instanceof ByteBufferWriter) {
      ((ByteBufferWriter) out).writeInt(value);
    } else {
      StreamUtils.writeInt(out, value);
    }
  }

  public static byte toByte(ByteBuffer buffer, int offset) {
    // For some reason there is no way to get one byte from a ByteBuffer via a VarHandle, so
    // for now just do things the slow way.
    return buffer.get(offset);
  }

  /**
   * Copy the data to the output stream and update position in buffer.
   * @param out    the stream to write bytes to
   * @param in     the buffer to read bytes from
   * @param length the number of bytes to copy
   */
  public static void moveBufferToStream(OutputStream out, ByteBuffer in, int length)
    throws IOException {
    copyBufferToStream(out, in, in.position(), length);
    skip(in, length);
  }

  /**
   * Copy data from a buffer to an output stream. Does not update the position in the buffer.
   * @param out    the stream to write bytes to
   * @param in     the buffer to read bytes from
   * @param offset the offset in the buffer (from the buffer's array offset) to start copying bytes
   *               from
   * @param length the number of bytes to copy
   */
  public static void copyBufferToStream(OutputStream out, ByteBuffer in, int offset, int length)
    throws IOException {
    if (out instanceof ByteBufferWriter) {
      ((ByteBufferWriter) out).write(in, offset, length);
    } else if (in.hasArray()) {
      out.write(in.array(), in.arrayOffset() + offset, length);
    } else {
      for (int i = 0; i < length; ++i) {
        out.write(toByte(in, offset + i));
      }
    }
  }

  /**
   * Copy data from a buffer to an output stream. Does not update the position in the buffer.
   * @param out    the output stream to write bytes to
   * @param in     the buffer to read bytes from
   * @param offset the offset in the buffer (from the buffer's array offset) to start copying bytes
   *               from
   * @param length the number of bytes to copy
   */
  public static void copyBufferToStream(DataOutput out, ByteBuffer in, int offset, int length)
    throws IOException {
    if (out instanceof ByteBufferWriter) {
      ((ByteBufferWriter) out).write(in, offset, length);
    } else if (in.hasArray()) {
      out.write(in.array(), in.arrayOffset() + offset, length);
    } else {
      for (int i = 0; i < length; ++i) {
        out.write(toByte(in, offset + i));
      }
    }
  }

  public static int putLong(OutputStream out, final long value, final int fitInBytes)
    throws IOException {
    long tmpValue = value;
    for (int i = 0; i < fitInBytes; ++i) {
      out.write((byte) (tmpValue & 0xff));
      tmpValue >>>= 8;
    }
    return fitInBytes;
  }

  public static int putByte(ByteBuffer buffer, int offset, byte b) {
    if (UNSAFE_AVAIL) {
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

    if (value < (1L << (4 * 8))) {
      // no more than 4 bytes
      if (value < (1L << (2 * 8))) {
        if (value < (1L << (1 * 8))) {
          return 1;
        }
        return 2;
      }
      if (value < (1L << (3 * 8))) {
        return 3;
      }
      return 4;
    }
    // more than 4 bytes
    if (value < (1L << (6 * 8))) {
      if (value < (1L << (5 * 8))) {
        return 5;
      }
      return 6;
    }
    if (value < (1L << (7 * 8))) {
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

    if (value < (1 << (2 * 8))) {
      if (value < (1 << (1 * 8))) {
        return 1;
      }
      return 2;
    }
    if (value <= (1 << (3 * 8))) {
      return 3;
    }
    return 4;
  }

  /**
   * Read integer from stream coded in 7 bits and increment position.
   * @return the integer that has been read
   * @throws IOException on stream error
   */
  public static int readCompressedInt(InputStream input) throws IOException {
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
   * @throws IOException on stream error
   */
  public static long readLong(InputStream in, final int fitInBytes) throws IOException {
    long tmpLong = 0;
    for (int i = 0; i < fitInBytes; ++i) {
      tmpLong |= (in.read() & 0xffL) << (8 * i);
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
      tmpLength |= (in.get() & 0xffL) << (8L * i);
    }
    return tmpLength;
  }

  /**
   * Copy the given number of bytes from the given stream and put it at the current position of the
   * given buffer, updating the position in the buffer.
   * @param out    the buffer to write data to
   * @param in     the stream to read data from
   * @param length the number of bytes to read/write
   */
  public static void copyFromStreamToBuffer(ByteBuffer out, DataInputStream in, int length)
    throws IOException {
    if (out.hasArray()) {
      in.readFully(out.array(), out.position() + out.arrayOffset(), length);
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
   * Note : This will advance the position marker of {@code out} and also change the position maker
   * for {@code in}.
   * @param in  source buffer
   * @param out destination buffer
   */
  public static void copyFromBufferToBuffer(ByteBuffer in, ByteBuffer out) {
    if (in.hasArray() && out.hasArray()) {
      int length = in.remaining();
      System.arraycopy(in.array(), in.arrayOffset(), out.array(), out.arrayOffset(), length);
      out.position(out.position() + length);
      in.position(in.limit());
    } else if (UNSAFE_AVAIL) {
      int length = in.remaining();
      UnsafeAccess.copy(in, in.position(), out, out.position(), length);
      out.position(out.position() + length);
      in.position(in.limit());
    } else {
      out.put(in);
    }
  }

  /**
   * Copy from one buffer to another from given offset. This will be absolute positional copying and
   * won't affect the position of any of the buffers.
   * @param in                input bytebuffer
   * @param out               destination bytebuffer
   * @param sourceOffset      offset of source buffer
   * @param destinationOffset offset of destination buffer
   * @param length            the number of bytes to copy
   */
  public static void copyFromBufferToBuffer(ByteBuffer in, ByteBuffer out, int sourceOffset,
    int destinationOffset, int length) {
    if (in.hasArray() && out.hasArray()) {
      System.arraycopy(in.array(), sourceOffset + in.arrayOffset(), out.array(),
        out.arrayOffset() + destinationOffset, length);
    } else if (UNSAFE_AVAIL) {
      UnsafeAccess.copy(in, sourceOffset, out, destinationOffset, length);
    } else {
      ByteBuffer outDup = out.duplicate();
      outDup.position(destinationOffset);
      ByteBuffer inDup = in.duplicate();
      inDup.position(sourceOffset).limit(sourceOffset + length);
      outDup.put(inDup);
    }
    // We used to return a result but disabled; return destinationOffset + length;
  }

  /**
   * Copy from one buffer to another from given offset.
   * <p>
   * Note : This will advance the position marker of {@code out} but not change the position maker
   * for {@code in}
   * @param in           source buffer
   * @param out          destination buffer
   * @param sourceOffset offset in the source buffer
   * @param length       how many bytes to copy
   */
  public static void copyFromBufferToBuffer(ByteBuffer in, ByteBuffer out, int sourceOffset,
    int length) {
    if (in.hasArray() && out.hasArray()) {
      System.arraycopy(in.array(), sourceOffset + in.arrayOffset(), out.array(),
        out.position() + out.arrayOffset(), length);
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
   * Find length of common prefix in two arrays.
   * @param left        Array to be compared.
   * @param leftOffset  Offset in left array.
   * @param leftLength  Length of left array.
   * @param right       Array to be compared.
   * @param rightOffset Offset in right array.
   * @param rightLength Length of right array.
   */
  public static int findCommonPrefix(byte[] left, int leftOffset, int leftLength, byte[] right,
    int rightOffset, int rightLength) {
    return Bytes.findCommonPrefix(left, right, leftLength, rightLength, leftOffset, rightOffset);
  }

  /**
   * Find length of common prefix in two arrays.
   * @param left        ByteBuffer to be compared.
   * @param leftOffset  Offset in left ByteBuffer.
   * @param leftLength  Length of left ByteBuffer.
   * @param right       ByteBuffer to be compared.
   * @param rightOffset Offset in right ByteBuffer.
   * @param rightLength Length of right ByteBuffer.
   */
  public static int findCommonPrefix(ByteBuffer left, int leftOffset, int leftLength,
    ByteBuffer right, int rightOffset, int rightLength) {
    return CommonPrefixerHolder.BEST_COMMON_PREFIXER.findCommonPrefix(left, leftOffset, leftLength,
      right, rightOffset, rightLength);
  }

  /**
   * Find length of common prefix in two arrays.
   * @param left        ByteBuffer to be compared.
   * @param leftOffset  Offset in left ByteBuffer.
   * @param leftLength  Length of left ByteBuffer.
   * @param right       Array to be compared
   * @param rightOffset Offset in right Array.
   * @param rightLength Length of right Array.
   */
  public static int findCommonPrefix(ByteBuffer left, int leftOffset, int leftLength, byte[] right,
    int rightOffset, int rightLength) {
    return CommonPrefixerHolder.BEST_COMMON_PREFIXER.findCommonPrefix(left, leftOffset, leftLength,
      right, rightOffset, rightLength);
  }

  /**
   * Check whether two parts in the same buffer are equal.
   * @param buffer      In which buffer there are parts
   * @param offsetLeft  Beginning of first part.
   * @param lengthLeft  Length of the first part.
   * @param offsetRight Beginning of the second part.
   * @param lengthRight Length of the second part.
   * @return True if equal
   */
  public static boolean arePartsEqual(ByteBuffer buffer, int offsetLeft, int lengthLeft,
    int offsetRight, int lengthRight) {
    if (lengthLeft != lengthRight) {
      return false;
    }

    if (buffer.hasArray()) {
      return 0 == Bytes.compareTo(buffer.array(), buffer.arrayOffset() + offsetLeft, lengthLeft,
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
   * @param buffer        copy from here
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
   * @param buffer input bytebuffer to read
   * @param offset input offset where Bytes are
   * @param length the number of bytes to read
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
    if ((l1 == 0) || (l2 == 0)) {
      // both 0 length, return true, or else false
      return l1 == l2;
    }
    // Since we're often comparing adjacent sorted data,
    // it's usual to have equal arrays except for the very last byte
    // so check that first
    if (toByte(buf1, o1 + l1 - 1) != toByte(buf2, o2 + l2 - 1)) {
      return false;
    }
    return compareTo(buf1, o1, l1, buf2, o2, l2) == 0;
  }

  /**
   * ByteBuffer to hash offset to start from length to hash
   */
  public static int hashCode(ByteBuffer buf, int offset, int length) {
    int hash = 1;
    for (int i = offset; i < offset + length; i++) {
      hash = (31 * hash) + (int) toByte(buf, i);
    }
    return hash;
  }

  public static int compareTo(ByteBuffer buf1, int o1, int l1, ByteBuffer buf2, int o2, int l2) {
    return ComparerHolder.BEST_COMPARER.compareTo(buf1, o1, l1, buf2, o2, l2);
  }

  public static boolean equals(ByteBuffer buf1, int o1, int l1, byte[] buf2, int o2, int l2) {
    if ((l1 == 0) || (l2 == 0)) {
      // both 0 length, return true, or else false
      return l1 == l2;
    }
    // Since we're often comparing adjacent sorted data,
    // it's usual to have equal arrays except for the very last byte
    // so check that first
    if (toByte(buf1, o1 + l1 - 1) != buf2[o2 + l2 - 1]) {
      return false;
    }
    return compareTo(buf1, o1, l1, buf2, o2, l2) == 0;
  }

  // The below two methods show up in lots of places. Versions of them in commons util and in
  // Cassandra. In guava too? They are copied from ByteBufferUtils. They are here as static
  // privates. Seems to make code smaller and make Hotspot happier (comes of compares and study
  // of compiled code via jitwatch).

  public static int compareTo(byte[] buf1, int o1, int l1, ByteBuffer buf2, int o2, int l2) {
    return ComparerHolder.BEST_COMPARER.compareTo(buf1, o1, l1, buf2, o2, l2);
  }

  public static int compareTo(ByteBuffer buf1, int o1, int l1, byte[] buf2, int o2, int l2) {
    return compareTo(buf2, o2, l2, buf1, o1, l1) * -1;
  }

  /**
   * Reads a short value at the given buffer's offset.
   * @param buffer input byte buffer to read
   * @param offset input offset where short is
   * @return short value at offset
   */
  public static short toShort(ByteBuffer buffer, int offset) {
    return (short) BYTE_BUFFER_SHORT_VIEW_BIG_ENDIAN_VAR_HANDLE.get(buffer, offset);
  }

  /**
   * Reads an int value at the given buffer's current position. Also advances the buffer's position
   */
  public static int toInt(ByteBuffer buffer) {
    int i = (int) BYTE_BUFFER_INT_VIEW_BIG_ENDIAN_VAR_HANDLE.get(buffer, buffer.position());
    buffer.position(buffer.position() + Integer.BYTES);
    return i;
  }

  /**
   * Reads an int value at the given buffer's offset.
   * @param buffer input byte buffer to read
   * @param offset input offset where int is
   * @return int value at offset
   */
  public static int toInt(ByteBuffer buffer, int offset) {
    return (int) BYTE_BUFFER_INT_VIEW_BIG_ENDIAN_VAR_HANDLE.get(buffer, offset);
  }

  /**
   * Converts a ByteBuffer to an int value
   * @param buf    The ByteBuffer
   * @param offset Offset to int value
   * @param length Number of bytes used to store the int value.
   * @return the int value if there's not enough bytes left in the buffer after the given offset
   */
  public static int readAsInt(ByteBuffer buf, int offset, final int length) {
    if (offset + length > buf.limit()) {
      throw new IllegalArgumentException("offset (" + offset + ") + length (" + length
        + ") exceed the" + " limit of the buffer: " + buf.limit());
    }
    int n = 0;
    for (int i = offset; i < (offset + length); i++) {
      n <<= 8;
      n ^= toByte(buf, i) & 0xFF;
    }
    return n;
  }

  /**
   * Reads a long value at the given buffer's offset.
   * @param buffer input byte buffer to read
   * @param offset input offset where Long is
   * @return long value at offset
   */
  public static long toLong(ByteBuffer buffer, int offset) {
    return (long) BYTE_BUFFER_LONG_VIEW_BIG_ENDIAN_VAR_HANDLE.get(buffer, offset);
  }

  /**
   * Put an int value out to the given ByteBuffer's current position in big-endian format. This also
   * advances the position in buffer by int size.
   * @param buffer the ByteBuffer to write to
   * @param val    int to write out
   */
  public static void putInt(ByteBuffer buffer, int val) {
    BYTE_BUFFER_INT_VIEW_BIG_ENDIAN_VAR_HANDLE.set(buffer, buffer.position(), val);
    buffer.position(buffer.position() + Integer.BYTES);
  }

  public static int putInt(ByteBuffer buffer, int index, int val) {
    BYTE_BUFFER_INT_VIEW_BIG_ENDIAN_VAR_HANDLE.set(buffer, buffer.position(), val);
    return index + Integer.BYTES;
  }

  /**
   * Reads a double value at the given buffer's offset.
   * @param buffer input byte buffer to read
   * @param offset offset where double is
   * @return double value at offset
   */
  public static double toDouble(ByteBuffer buffer, int offset) {
    return Double.longBitsToDouble(toLong(buffer, offset));
  }

  /**
   * Reads a BigDecimal value at the given buffer's offset.
   * @param buffer input bytebuffer to read
   * @param offset input offset
   * @return BigDecimal value at offset
   */
  public static BigDecimal toBigDecimal(ByteBuffer buffer, int offset, int length) {
    if (buffer == null || length < Bytes.SIZEOF_INT + 1 || (offset + length > buffer.limit())) {
      return null;
    }

    int scale = toInt(buffer, offset);
    byte[] tcBytes = new byte[length - Bytes.SIZEOF_INT];
    copyFromBufferToArray(tcBytes, buffer, offset + Bytes.SIZEOF_INT, 0, length - Bytes.SIZEOF_INT);
    return new BigDecimal(new BigInteger(tcBytes), scale);
  }

  /**
   * Put a short value out to the given ByteBuffer's current position in big-endian format. This
   * also advances the position in buffer by short size.
   * @param buffer the ByteBuffer to write to
   * @param val    short to write out
   */
  public static void putShort(ByteBuffer buffer, short val) {
    BYTE_BUFFER_SHORT_VIEW_BIG_ENDIAN_VAR_HANDLE.set(buffer, buffer.position(), val);
    buffer.position(buffer.position() + Short.BYTES);
  }

  public static int putShort(ByteBuffer buffer, int index, short val) {
    BYTE_BUFFER_SHORT_VIEW_BIG_ENDIAN_VAR_HANDLE.set(buffer, buffer.position(), val);
    return index + Short.BYTES;
  }

  public static int putAsShort(ByteBuffer buf, int index, int val) {
    buf.put(index + 1, (byte) val);
    val >>= 8;
    buf.put(index, (byte) val);
    return index + Bytes.SIZEOF_SHORT;
  }

  /**
   * Put a long value out to the given ByteBuffer's current position in big-endian format. This also
   * advances the position in buffer by long size.
   * @param buffer the ByteBuffer to write to
   * @param val    long to write out
   */
  public static void putLong(ByteBuffer buffer, long val) {
    BYTE_BUFFER_LONG_VIEW_BIG_ENDIAN_VAR_HANDLE.set(buffer, buffer.position(), val);
    buffer.position(buffer.position() + Long.BYTES);
  }

  public static int putLong(ByteBuffer buffer, int index, long val) {
    BYTE_BUFFER_LONG_VIEW_BIG_ENDIAN_VAR_HANDLE.set(buffer, buffer.position(), val);
    return index + Long.BYTES;
  }

  /**
   * Copies the bytes from given array's offset to length part into the given buffer. Puts the bytes
   * to buffer's current position. This also advances the position in the 'out' buffer by 'length'
   * @param out      output bytebuffer to copy to
   * @param in       input array to copy from
   * @param inOffset input offset to copy from
   * @param length   the number of bytes to copy
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
   * Copies bytes from given array's offset to length part into the given buffer. Puts the bytes to
   * buffer's given position. This doesn't affect the position of buffer.
   * @param out       output bytebuffer to copy to
   * @param outOffset output buffer offset
   * @param in        input array to copy from
   * @param inOffset  input offset to copy from
   * @param length    the number of bytes to copy
   */
  public static void copyFromArrayToBuffer(ByteBuffer out, int outOffset, byte[] in, int inOffset,
    int length) {
    if (out.hasArray()) {
      System.arraycopy(in, inOffset, out.array(), out.arrayOffset() + outOffset, length);
    } else if (UNSAFE_AVAIL) {
      UnsafeAccess.copy(in, inOffset, out, outOffset, length);
    } else {
      ByteBuffer outDup = out.duplicate();
      outDup.position(outOffset);
      outDup.put(in, inOffset, length);
    }
  }

  /**
   * Copies specified number of bytes from given offset of 'in' ByteBuffer to the array. This
   * doesn't affect the position of buffer.
   * @param out               output array to copy input bytebuffer to
   * @param in                input bytebuffer to copy from
   * @param sourceOffset      offset of source bytebuffer
   * @param destinationOffset offset of destination array
   * @param length            the number of bytes to copy
   */
  public static void copyFromBufferToArray(byte[] out, ByteBuffer in, int sourceOffset,
    int destinationOffset, int length) {
    if (in.hasArray()) {
      System.arraycopy(in.array(), sourceOffset + in.arrayOffset(), out, destinationOffset, length);
    } else if (UNSAFE_AVAIL) {
      UnsafeAccess.copy(in, sourceOffset, out, destinationOffset, length);
    } else {
      ByteBuffer inDup = in.duplicate();
      inDup.position(sourceOffset);
      inDup.get(out, destinationOffset, length);
    }
  }

  /**
   * Similar to {@link Arrays#copyOfRange(byte[], int, int)}
   * @param original the buffer from which the copy has to happen
   * @param from     the starting index
   * @param to       the ending index
   * @return a byte[] created out of the copy
   */
  public static byte[] copyOfRange(ByteBuffer original, int from, int to) {
    int newLength = to - from;
    if (newLength < 0) {
      throw new IllegalArgumentException(from + " > " + to);
    }
    byte[] copy = new byte[newLength];
    ByteBufferUtils.copyFromBufferToArray(copy, original, from, 0, newLength);
    return copy;
  }

  // For testing purpose
  public static String toStringBinary(final ByteBuffer b, int off, int len) {
    StringBuilder result = new StringBuilder();
    // Just in case we are passed a 'len' that is > buffer length...
    if (off >= b.capacity()) {
      return result.toString();
    }
    if (off + len > b.capacity()) {
      len = b.capacity() - off;
    }
    for (int i = off; i < off + len; ++i) {
      int ch = b.get(i) & 0xFF;
      if (
        (ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z')
          || " `~!@#$%^&*()-_=+[]{}|;:'\",.<>/?".indexOf(ch) >= 0
      ) {
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

  /**
   * Find index of passed delimiter.
   * @return Index of delimiter having started from start of <code>b</code> moving rightward.
   */
  public static int searchDelimiterIndex(ByteBuffer b, int offset, final int length,
    final int delimiter) {
    for (int i = offset, n = offset + length; i < n; i++) {
      if (b.get(i) == delimiter) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Find index of passed delimiter walking from end of buffer backwards.
   * @return Index of delimiter
   */
  public static int searchDelimiterIndexInReverse(ByteBuffer b, int offset, int length,
    int delimiter) {
    for (int i = offset + length - 1; i >= offset; i--) {
      if (b.get(i) == delimiter) {
        return i;
      }
    }
    return -1;
  }
}
