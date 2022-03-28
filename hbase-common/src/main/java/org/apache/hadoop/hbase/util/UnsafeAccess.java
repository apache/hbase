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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.hadoop.hbase.unsafe.HBasePlatformDependent;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.apache.hbase.thirdparty.io.netty.util.internal.PlatformDependent;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class UnsafeAccess {

  /** The offset to the first element in a byte array. */
  public static final long BYTE_ARRAY_BASE_OFFSET;

  public static final boolean LITTLE_ENDIAN =
    ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);

  // This number limits the number of bytes to copy per call to Unsafe's
  // copyMemory method. A limit is imposed to allow for safepoint polling
  // during a large copy
  static final long UNSAFE_COPY_THRESHOLD = 1024L * 1024L;
  static {
    if (HBasePlatformDependent.isUnsafeAvailable()) {
      BYTE_ARRAY_BASE_OFFSET = HBasePlatformDependent.arrayBaseOffset(byte[].class);
    } else {
      BYTE_ARRAY_BASE_OFFSET = -1;
    }
  }

  private UnsafeAccess() {
  }

  // APIs to read primitive data from a byte[] using Unsafe way
  /**
   * Converts a byte array to a short value considering it was written in big-endian format.
   * @param bytes byte array
   * @param offset offset into array
   * @return the short value
   */
  public static short toShort(byte[] bytes, int offset) {
    if (LITTLE_ENDIAN) {
      return Short
        .reverseBytes(HBasePlatformDependent.getShort(bytes, offset + BYTE_ARRAY_BASE_OFFSET));
    } else {
      return HBasePlatformDependent.getShort(bytes, offset + BYTE_ARRAY_BASE_OFFSET);
    }
  }

  /**
   * Converts a byte array to an int value considering it was written in big-endian format.
   * @param bytes byte array
   * @param offset offset into array
   * @return the int value
   */
  public static int toInt(byte[] bytes, int offset) {
    if (LITTLE_ENDIAN) {
      return Integer
        .reverseBytes(HBasePlatformDependent.getInt(bytes, offset + BYTE_ARRAY_BASE_OFFSET));
    } else {
      return HBasePlatformDependent.getInt(bytes, offset + BYTE_ARRAY_BASE_OFFSET);
    }
  }

  /**
   * Converts a byte array to a long value considering it was written in big-endian format.
   * @param bytes byte array
   * @param offset offset into array
   * @return the long value
   */
  public static long toLong(byte[] bytes, int offset) {
    if (LITTLE_ENDIAN) {
      return Long
        .reverseBytes(HBasePlatformDependent.getLong(bytes, offset + BYTE_ARRAY_BASE_OFFSET));
    } else {
      return HBasePlatformDependent.getLong(bytes, offset + BYTE_ARRAY_BASE_OFFSET);
    }
  }

  // APIs to write primitive data to a byte[] using Unsafe way
  /**
   * Put a short value out to the specified byte array position in big-endian format.
   * @param bytes the byte array
   * @param offset position in the array
   * @param val short to write out
   * @return incremented offset
   */
  public static int putShort(byte[] bytes, int offset, short val) {
    if (LITTLE_ENDIAN) {
      val = Short.reverseBytes(val);
    }
    HBasePlatformDependent.putShort(bytes, offset + BYTE_ARRAY_BASE_OFFSET, val);
    return offset + Bytes.SIZEOF_SHORT;
  }

  /**
   * Put an int value out to the specified byte array position in big-endian format.
   * @param bytes the byte array
   * @param offset position in the array
   * @param val int to write out
   * @return incremented offset
   */
  public static int putInt(byte[] bytes, int offset, int val) {
    if (LITTLE_ENDIAN) {
      val = Integer.reverseBytes(val);
    }
    HBasePlatformDependent.putInt(bytes, offset + BYTE_ARRAY_BASE_OFFSET, val);
    return offset + Bytes.SIZEOF_INT;
  }

  /**
   * Put a long value out to the specified byte array position in big-endian format.
   * @param bytes the byte array
   * @param offset position in the array
   * @param val long to write out
   * @return incremented offset
   */
  public static int putLong(byte[] bytes, int offset, long val) {
    if (LITTLE_ENDIAN) {
      val = Long.reverseBytes(val);
    }
    HBasePlatformDependent.putLong(bytes, offset + BYTE_ARRAY_BASE_OFFSET, val);
    return offset + Bytes.SIZEOF_LONG;
  }

  // APIs to read primitive data from a ByteBuffer using Unsafe way
  /**
   * Reads a short value at the given buffer's offset considering it was written in big-endian
   * format.
   * @return short value at offset
   */
  public static short toShort(ByteBuffer buf, int offset) {
    if (LITTLE_ENDIAN) {
      return Short.reverseBytes(getAsShort(buf, offset));
    }
    return getAsShort(buf, offset);
  }

  /**
   * Reads a short value at the given Object's offset considering it was written in big-endian
   * format.
   * @return short value at offset
   */
  public static short toShort(Object ref, long offset) {
    if (LITTLE_ENDIAN) {
      return Short.reverseBytes(HBasePlatformDependent.getShort(ref, offset));
    }
    return HBasePlatformDependent.getShort(ref, offset);
  }

  /**
   * Reads bytes at the given offset as a short value.
   * @return short value at offset
   */
  private static short getAsShort(ByteBuffer buf, int offset) {
    if (buf.isDirect()) {
      return HBasePlatformDependent.getShort(directBufferAddress(buf) + offset);
    }
    return HBasePlatformDependent.getShort(buf.array(),
      BYTE_ARRAY_BASE_OFFSET + buf.arrayOffset() + offset);
  }

  /**
   * Reads an int value at the given buffer's offset considering it was written in big-endian
   * format.
   * @return int value at offset
   */
  public static int toInt(ByteBuffer buf, int offset) {
    if (LITTLE_ENDIAN) {
      return Integer.reverseBytes(getAsInt(buf, offset));
    }
    return getAsInt(buf, offset);
  }

  /**
   * Reads a int value at the given Object's offset considering it was written in big-endian format.
   * @param ref
   * @param offset
   * @return int value at offset
   */
  public static int toInt(Object ref, long offset) {
    if (LITTLE_ENDIAN) {
      return Integer.reverseBytes(HBasePlatformDependent.getInt(ref, offset));
    }
    return HBasePlatformDependent.getInt(ref, offset);
  }

  /**
   * Reads bytes at the given offset as an int value.
   * @return int value at offset
   */
  private static int getAsInt(ByteBuffer buf, int offset) {
    if (buf.isDirect()) {
      return HBasePlatformDependent.getInt(directBufferAddress(buf) + offset);
    }
    return HBasePlatformDependent.getInt(buf.array(),
      BYTE_ARRAY_BASE_OFFSET + buf.arrayOffset() + offset);
  }

  /**
   * Reads a long value at the given buffer's offset considering it was written in big-endian
   * format.
   * @return long value at offset
   */
  public static long toLong(ByteBuffer buf, int offset) {
    if (LITTLE_ENDIAN) {
      return Long.reverseBytes(getAsLong(buf, offset));
    }
    return getAsLong(buf, offset);
  }

  /**
   * Reads a long value at the given Object's offset considering it was written in big-endian
   * format.
   * @return long value at offset
   */
  public static long toLong(Object ref, long offset) {
    if (LITTLE_ENDIAN) {
      return Long.reverseBytes(HBasePlatformDependent.getLong(ref, offset));
    }
    return HBasePlatformDependent.getLong(ref, offset);
  }

  /**
   * Reads bytes at the given offset as a long value.
   * @return long value at offset
   */
  private static long getAsLong(ByteBuffer buf, int offset) {
    if (buf.isDirect()) {
      return HBasePlatformDependent.getLong(directBufferAddress(buf) + offset);
    }
    return HBasePlatformDependent.getLong(buf.array(),
      BYTE_ARRAY_BASE_OFFSET + buf.arrayOffset() + offset);
  }

  /**
   * Returns the byte at the given offset
   * @param buf the buffer to read
   * @param offset the offset at which the byte has to be read
   * @return the byte at the given offset
   */
  public static byte toByte(ByteBuffer buf, int offset) {
    if (buf.isDirect()) {
      return HBasePlatformDependent.getByte(directBufferAddress(buf) + offset);
    } else {
      return HBasePlatformDependent.getByte(buf.array(),
        BYTE_ARRAY_BASE_OFFSET + buf.arrayOffset() + offset);
    }
  }

  /**
   * Returns the byte at the given offset of the object
   * @return the byte at the given offset
   */
  public static byte toByte(Object ref, long offset) {
    return HBasePlatformDependent.getByte(ref, offset);
  }

  /**
   * Put an int value out to the specified ByteBuffer offset in big-endian format.
   * @param buf the ByteBuffer to write to
   * @param offset offset in the ByteBuffer
   * @param val int to write out
   * @return incremented offset
   */
  public static int putInt(ByteBuffer buf, int offset, int val) {
    if (LITTLE_ENDIAN) {
      val = Integer.reverseBytes(val);
    }
    if (buf.isDirect()) {
      HBasePlatformDependent.putInt(directBufferAddress(buf) + offset, val);
    } else {
      HBasePlatformDependent.putInt(buf.array(),
        offset + buf.arrayOffset() + BYTE_ARRAY_BASE_OFFSET, val);
    }
    return offset + Bytes.SIZEOF_INT;
  }

  // APIs to copy data. This will be direct memory location copy and will be much faster
  /**
   * Copies the bytes from given array's offset to length part into the given buffer.
   * @param src source array
   * @param srcOffset offset into source buffer
   * @param dest destination buffer
   * @param destOffset offset into destination buffer
   * @param length length of data to copy
   */
  public static void copy(byte[] src, int srcOffset, ByteBuffer dest, int destOffset, int length) {
    long destAddress = destOffset;
    Object destBase = null;
    if (dest.isDirect()) {
      destAddress = destAddress + directBufferAddress(dest);
    } else {
      destAddress = destAddress + BYTE_ARRAY_BASE_OFFSET + dest.arrayOffset();
      destBase = dest.array();
    }
    long srcAddress = srcOffset + BYTE_ARRAY_BASE_OFFSET;
    unsafeCopy(src, srcAddress, destBase, destAddress, length);
  }

  private static void unsafeCopy(Object src, long srcAddr, Object dst, long destAddr, long len) {
    while (len > 0) {
      long size = (len > UNSAFE_COPY_THRESHOLD) ? UNSAFE_COPY_THRESHOLD : len;
      HBasePlatformDependent.copyMemory(src, srcAddr, dst, destAddr, size);
      len -= size;
      srcAddr += size;
      destAddr += size;
    }
  }

  /**
   * Copies specified number of bytes from given offset of {@code src} ByteBuffer to the
   * {@code dest} array.
   * @param src source buffer
   * @param srcOffset offset into source buffer
   * @param dest destination array
   * @param destOffset offset into destination buffer
   * @param length length of data to copy
   */
  public static void copy(ByteBuffer src, int srcOffset, byte[] dest, int destOffset, int length) {
    long srcAddress = srcOffset;
    Object srcBase = null;
    if (src.isDirect()) {
      srcAddress = srcAddress + directBufferAddress(src);
    } else {
      srcAddress = srcAddress + BYTE_ARRAY_BASE_OFFSET + src.arrayOffset();
      srcBase = src.array();
    }
    long destAddress = destOffset + BYTE_ARRAY_BASE_OFFSET;
    unsafeCopy(srcBase, srcAddress, dest, destAddress, length);
  }

  /**
   * Copies specified number of bytes from given offset of {@code src} buffer into the {@code dest}
   * buffer.
   * @param src source buffer
   * @param srcOffset offset into source buffer
   * @param dest destination buffer
   * @param destOffset offset into destination buffer
   * @param length length of data to copy
   */
  public static void copy(ByteBuffer src, int srcOffset, ByteBuffer dest, int destOffset,
    int length) {
    long srcAddress, destAddress;
    Object srcBase = null, destBase = null;
    if (src.isDirect()) {
      srcAddress = srcOffset + directBufferAddress(src);
    } else {
      srcAddress = (long) srcOffset + src.arrayOffset() + BYTE_ARRAY_BASE_OFFSET;
      srcBase = src.array();
    }
    if (dest.isDirect()) {
      destAddress = destOffset + directBufferAddress(dest);
    } else {
      destAddress = destOffset + BYTE_ARRAY_BASE_OFFSET + dest.arrayOffset();
      destBase = dest.array();
    }
    unsafeCopy(srcBase, srcAddress, destBase, destAddress, length);
  }

  // APIs to add primitives to BBs
  /**
   * Put a short value out to the specified BB position in big-endian format.
   * @param buf the byte buffer
   * @param offset position in the buffer
   * @param val short to write out
   * @return incremented offset
   */
  public static int putShort(ByteBuffer buf, int offset, short val) {
    if (LITTLE_ENDIAN) {
      val = Short.reverseBytes(val);
    }
    if (buf.isDirect()) {
      HBasePlatformDependent.putShort(directBufferAddress(buf) + offset, val);
    } else {
      HBasePlatformDependent.putShort(buf.array(),
        BYTE_ARRAY_BASE_OFFSET + buf.arrayOffset() + offset, val);
    }
    return offset + Bytes.SIZEOF_SHORT;
  }

  /**
   * Put a long value out to the specified BB position in big-endian format.
   * @param buf the byte buffer
   * @param offset position in the buffer
   * @param val long to write out
   * @return incremented offset
   */
  public static int putLong(ByteBuffer buf, int offset, long val) {
    if (LITTLE_ENDIAN) {
      val = Long.reverseBytes(val);
    }
    if (buf.isDirect()) {
      HBasePlatformDependent.putLong(directBufferAddress(buf) + offset, val);
    } else {
      HBasePlatformDependent.putLong(buf.array(),
        BYTE_ARRAY_BASE_OFFSET + buf.arrayOffset() + offset, val);
    }
    return offset + Bytes.SIZEOF_LONG;
  }

  /**
   * Put a byte value out to the specified BB position in big-endian format.
   * @param buf the byte buffer
   * @param offset position in the buffer
   * @param b byte to write out
   * @return incremented offset
   */
  public static int putByte(ByteBuffer buf, int offset, byte b) {
    if (buf.isDirect()) {
      HBasePlatformDependent.putByte(directBufferAddress(buf) + offset, b);
    } else {
      HBasePlatformDependent.putByte(buf.array(),
        BYTE_ARRAY_BASE_OFFSET + buf.arrayOffset() + offset, b);
    }
    return offset + 1;
  }

  public static long directBufferAddress(ByteBuffer buf) {
    return PlatformDependent.directBufferAddress(buf);
  }

  public static void freeDirectBuffer(ByteBuffer buffer) {
    // here we just use the method in netty
    PlatformDependent.freeDirectBuffer(buffer);
  }
}
