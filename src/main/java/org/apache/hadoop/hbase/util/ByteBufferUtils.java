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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.io.encoding.
    EncoderBufferTooSmallException;
import org.apache.hadoop.io.WritableUtils;

/**
 * Utility functions for working with byte buffers, such as reading/writing
 * variable-length long numbers.
 */
public final class ByteBufferUtils {

  // "Compressed integer" serialization helper constants.
  private final static int VALUE_MASK = 0x7f;
  private final static int NEXT_BIT_SHIFT = 7;
  private final static int NEXT_BIT_MASK = 1 << 7;

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
      i ^= -1L; // take one's complement'
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
     for (int i = Bytes.SIZEOF_INT - 1 ; i >= 0 ; --i) {
       out.write((byte) (value >>> (i * 8)));
     }
   }

  /**
   * Copy byte to the output stream.
   * @param b byte to be copied
   * @param out Where to put compressed data
   * @return Number of written bytes.
   * @throws IOException on stream error
   */
  public static int copyToStream(OutputStream out, byte b)
      throws IOException {
    out.write(b);
    return Bytes.SIZEOF_BYTE;
  }

  /**
   * Copy the data to the output stream and update position in buffer.
   * @param out Write bytes here.
   * @param buffer Source buffer in certain position.
   * @param length Length of copy.
   * @return Number of written bytes.
   * @throws IOException on stream error
   */
  public static int copyToStream(OutputStream out, ByteBuffer buffer,
      int length) throws IOException {
    if (buffer.hasArray()) {
      out.write(buffer.array(), buffer.arrayOffset() + buffer.position(),
          length);
      skip(buffer, length);
    } else {
      for (int i = 0 ; i < length ; ++i) {
        out.write(buffer.get());
      }
    }
    return length;
  }

  /**
   * Copy the data to the output stream
   * @param out Write bytes here.
   * @param in Source buffer in certain position.
   * @param offset In the buffer from position 0.
   * @param length Length of copy.
   * @return Number of written bytes.
   * @throws IOException on stream error
   */
  public static int copyToStream(OutputStream out, ByteBuffer in,
      int offset, int length) throws IOException {
    if (in.hasArray()) {
      out.write(in.array(), in.arrayOffset() + offset,
          length);
    } else {
      for (int i = 0 ; i < length ; ++i) {
        out.write(in.get(offset + i));
      }
    }
    return length;
  }

  public static int putLong(OutputStream out, final long length,
      final int fitInBytes) throws IOException {
    long tmpLength = length;
    for (int i = 0 ; i < fitInBytes ; ++i) {
      out.write((byte) (tmpLength & 0xff));
      tmpLength >>>= 8;
    }
    return fitInBytes;
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
   * @return Read integer.
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
  public static long readLong(InputStream input, final int fitInBytes)
      throws IOException {
    long tmpLong = 0;
    for (int i = 0 ; i < fitInBytes ; ++i) {
      tmpLong |= (input.read() & 0xffl) << (8 * i);
    }
    return tmpLong;
  }

  /**
   * Read long which was written to fitInBytes bytes and increment position.
   * @param fitInBytes In how many bytes given long is stored.
   * @return The value of parsed long.
   */
  public static long readLong(ByteBuffer buffer, final int fitInBytes) {
    long tmpLength = 0;
    for (int i = 0 ; i < fitInBytes ; ++i) {
      tmpLength |= (buffer.get() & 0xffl) << (8l * i);
    }
    return tmpLength;
  }

  /**
   * Asserts that we have 'length' bytes remaining in 'buffer'.
   * @param buffer Where are we looking for remaining bytes.
   * @param length How many bytes do we need.
   * @throws EncoderBufferTooSmallException If there are no enough bytes.
   */
  public static void ensureSpace(ByteBuffer buffer, int length)
      throws EncoderBufferTooSmallException {
    if (buffer.position() + length > buffer.limit()) {
      throw new EncoderBufferTooSmallException(
          "Buffer position=" + buffer.position() +
          ", buffer limit=" + buffer.limit() +
          ", length to be written=" + length);
    }
  }

  /**
   * Copy 'length' bytes from 'source' and put it at the current position of
   * 'buffer'. Update position in 'buffer' afterwards.
   * @param source From where data should be read.
   * @param buffer Write data here.
   * @param length Read that many bytes.
   * @throws IOException If there is problem in source.
   */
  public static void copyFromStream(DataInputStream source,
      ByteBuffer buffer, int length) throws IOException {
    if (buffer.hasArray()) {
      source.readFully(buffer.array(), buffer.position() + buffer.arrayOffset(),
          length);
      skip(buffer, length);
    } else {
      for (int i = 0 ; i < length ; ++i) {
        buffer.put(source.readByte());
      }
    }
  }

  /**
   * Copy from one buffer to another from given offset
   * @param source From where copy.
   * @param destination Where to copy.
   * @param sourceOffset Offset in the source buffer
   * @param length How many bytes will be copied.
   * @throws IOException
   */
  public static void copyFromBuffer(ByteBuffer source,
      ByteBuffer destination, int sourceOffset, int length) {
    if (source.hasArray() && destination.hasArray()) {
      System.arraycopy(source.array(), sourceOffset + source.arrayOffset(),
          destination.array(), destination.position() +
          destination.arrayOffset(), length);
      skip(destination, length);
    } else {
      for (int i = 0 ; i < length ; ++i) {
        destination.put(source.get(sourceOffset + i));
      }
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

    for (; prefix < limit ; ++prefix) {
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
   * @param rightArray Offset in right array.
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
   * Check whether two parts in the same buffer are equal.
   * @param buffer In which buffer there are parts
   * @param offsetLeft Beginning of first part.
   * @param lengthLeft Length of the first part.
   * @param offsetRight Beginning of the second part.
   * @param lengthRight Length of the second part.
   * @return
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

    for (int i = 0 ; i < lengthRight ; ++i) {
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

  /**
   * Read int, assuming it is stored in N bytes with no special encoding.
   * @param source From where read bytes.
   * @param intLength How long is the integer
   * @return The value of the integer.
   * @throws IOException On IO error.
   */
  public static int readCompressedInt(InputStream source, int intLength)
      throws IOException {
    int result = 0;
    for (int i = 0 ; i < intLength ; ++i) {
      result = (result << 8) + (source.read() & 0xff);
    }
    return result;
  }

  /**
   * Read int, assuming it is stored in N bytes with no special encoding.
   * @param buffer Read bytes from this buffer.
   * @param intLength The lenght of the integer in bytes.
   * @return The value of the integer.
   */
  public static int readCompressedInt(ByteBuffer buffer, int intLength) {
    int result = 0;
    for (int i = 0 ; i < intLength ; ++i) {
      result = (result << 8) + (buffer.get() & 0xff);
    }
    return result;
  }

}
