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

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class UnsafeAccess {

  private static final Log LOG = LogFactory.getLog(UnsafeAccess.class);

  static final Unsafe theUnsafe;

  /** The offset to the first element in a byte array. */
  static final long BYTE_ARRAY_BASE_OFFSET;

  static final boolean littleEndian = ByteOrder.nativeOrder()
      .equals(ByteOrder.LITTLE_ENDIAN);

  static {
    theUnsafe = (Unsafe) AccessController.doPrivileged(new PrivilegedAction<Object>() {
      @Override
      public Object run() {
        try {
          Field f = Unsafe.class.getDeclaredField("theUnsafe");
          f.setAccessible(true);
          return f.get(null);
        } catch (Throwable e) {
          LOG.warn("sun.misc.Unsafe is not accessible", e);
        }
        return null;
      }
    });

    if(theUnsafe != null){
      BYTE_ARRAY_BASE_OFFSET = theUnsafe.arrayBaseOffset(byte[].class);
    } else{
      BYTE_ARRAY_BASE_OFFSET = -1;
    }
  }

  private UnsafeAccess(){}

  /**
   * @return true when the running JVM is having sun's Unsafe package available in it.
   */
  public static boolean isAvailable() {
    return theUnsafe != null;
  }

  // APIs to read primitive data from a byte[] using Unsafe way
  /**
   * Converts a byte array to a short value.
   * @param bytes byte array
   * @param offset offset into array
   * @return the short value
   */
  public static short toShort(byte[] bytes, int offset) {
    if (littleEndian) {
      return Short.reverseBytes(theUnsafe.getShort(bytes, offset + BYTE_ARRAY_BASE_OFFSET));
    } else {
      return theUnsafe.getShort(bytes, offset + BYTE_ARRAY_BASE_OFFSET);
    }
  }

  /**
   * Converts a byte array to an int value.
   * @param bytes byte array
   * @param offset offset into array
   * @return the int value
   */
  public static int toInt(byte[] bytes, int offset) {
    if (littleEndian) {
      return Integer.reverseBytes(theUnsafe.getInt(bytes, offset + BYTE_ARRAY_BASE_OFFSET));
    } else {
      return theUnsafe.getInt(bytes, offset + BYTE_ARRAY_BASE_OFFSET);
    }
  }

  /**
   * Converts a byte array to a long value.
   * @param bytes byte array
   * @param offset offset into array
   * @return the long value
   */
  public static long toLong(byte[] bytes, int offset) {
    if (littleEndian) {
      return Long.reverseBytes(theUnsafe.getLong(bytes, offset + BYTE_ARRAY_BASE_OFFSET));
    } else {
      return theUnsafe.getLong(bytes, offset + BYTE_ARRAY_BASE_OFFSET);
    }
  }

  // APIs to write primitive data to a byte[] using Unsafe way
  /**
   * Put a short value out to the specified byte array position.
   * @param bytes the byte array
   * @param offset position in the array
   * @param val short to write out
   * @return incremented offset
   */
  public static int putShort(byte[] bytes, int offset, short val) {
    if (littleEndian) {
      val = Short.reverseBytes(val);
    }
    theUnsafe.putShort(bytes, offset + BYTE_ARRAY_BASE_OFFSET, val);
    return offset + Bytes.SIZEOF_SHORT;
  }

  /**
   * Put an int value out to the specified byte array position.
   * @param bytes the byte array
   * @param offset position in the array
   * @param val int to write out
   * @return incremented offset
   */
  public static int putInt(byte[] bytes, int offset, int val) {
    if (littleEndian) {
      val = Integer.reverseBytes(val);
    }
    theUnsafe.putInt(bytes, offset + BYTE_ARRAY_BASE_OFFSET, val);
    return offset + Bytes.SIZEOF_INT;
  }

  /**
   * Put a long value out to the specified byte array position.
   * @param bytes the byte array
   * @param offset position in the array
   * @param val long to write out
   * @return incremented offset
   */
  public static int putLong(byte[] bytes, int offset, long val) {
    if (littleEndian) {
      val = Long.reverseBytes(val);
    }
    theUnsafe.putLong(bytes, offset + BYTE_ARRAY_BASE_OFFSET, val);
    return offset + Bytes.SIZEOF_LONG;
  }

  // APIs to read primitive data from a ByteBuffer using Unsafe way
  /**
   * Reads a short value at the given buffer's offset.
   * @param buf
   * @param offset
   * @return short value at offset
   */
  public static short toShort(ByteBuffer buf, int offset) {
    short ret;
    if (buf.isDirect()) {
      ret = theUnsafe.getShort(((DirectBuffer) buf).address() + offset);
    } else {
      ret = theUnsafe.getShort(buf.array(), BYTE_ARRAY_BASE_OFFSET + buf.arrayOffset() + offset);
    }
    if (littleEndian) {
      return Short.reverseBytes(ret);
    }
    return ret;
  }

  /**
   * Reads an int value at the given buffer's offset.
   * @param buf
   * @param offset
   * @return int value at offset
   */
  public static int toInt(ByteBuffer buf, int offset) {
    int ret;
    if (buf.isDirect()) {
      ret = theUnsafe.getInt(((DirectBuffer) buf).address() + offset);
    } else {
      ret = theUnsafe.getInt(buf.array(), BYTE_ARRAY_BASE_OFFSET + buf.arrayOffset() + offset);
    }
    if (littleEndian) {
      return Integer.reverseBytes(ret);
    }
    return ret;
  }

  /**
   * Reads a long value at the given buffer's offset.
   * @param buf
   * @param offset
   * @return long value at offset
   */
  public static long toLong(ByteBuffer buf, int offset) {
    long ret;
    if (buf.isDirect()) {
      ret = theUnsafe.getLong(((DirectBuffer) buf).address() + offset);
    } else {
      ret = theUnsafe.getLong(buf.array(), BYTE_ARRAY_BASE_OFFSET + buf.arrayOffset() + offset);
    }
    if (littleEndian) {
      return Long.reverseBytes(ret);
    }
    return ret;
  }

  // APIs to copy data. This will be direct memory location copy and will be much faster
  /**
   * Copies the bytes from given array's offset to length part into the given buffer. Puts the bytes
   * to buffer's current position.
   * @param src
   * @param srcOffset
   * @param dest
   * @param destOffset
   * @param length
   */
  public static void copy(byte[] src, int srcOffset, ByteBuffer dest, int destOffset, int length) {
    long destAddress = destOffset;
    Object destBase = null;
    if (dest.isDirect()) {
      destAddress = destAddress + ((DirectBuffer) dest).address();
    } else {
      destAddress = destAddress + BYTE_ARRAY_BASE_OFFSET + dest.arrayOffset();
      destBase = dest.array();
    }
    long srcAddress = srcOffset + BYTE_ARRAY_BASE_OFFSET;
    theUnsafe.copyMemory(src, srcAddress, destBase, destAddress, length);
  }

  /**
   * Copies specified number of bytes from given offset of 'in' ByteBuffer to the array.
   * @param src
   * @param srcOffset
   * @param dest
   * @param destOffset
   * @param length
   */
  public static void copy(ByteBuffer src, int srcOffset, byte[] dest, int destOffset,
      int length) {
    long srcAddress = srcOffset;
    Object srcBase = null;
    if (src.isDirect()) {
      srcAddress = srcAddress + ((DirectBuffer) src).address();
    } else {
      srcAddress = srcAddress + BYTE_ARRAY_BASE_OFFSET + src.arrayOffset();
      srcBase = src.array();
    }
    long destAddress = destOffset + BYTE_ARRAY_BASE_OFFSET;
    theUnsafe.copyMemory(srcBase, srcAddress, dest, destAddress, length);
  }
}
