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

import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.unsafe.HBasePlatformDependent;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.hbase.thirdparty.io.netty.util.internal.PlatformDependent;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class UnsafeAccess {

  /** The offset to the first element in a byte array. */
  public static final long BYTE_ARRAY_BASE_OFFSET;

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

  // APIs to copy data. This will be direct memory location copy and will be much faster
  /**
   * Copies the bytes from given array's offset to length part into the given buffer.
   * @param src        source array
   * @param srcOffset  offset into source buffer
   * @param dest       destination buffer
   * @param destOffset offset into destination buffer
   * @param length     length of data to copy
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
   * @param src        source buffer
   * @param srcOffset  offset into source buffer
   * @param dest       destination array
   * @param destOffset offset into destination buffer
   * @param length     length of data to copy
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
   * @param src        source buffer
   * @param srcOffset  offset into source buffer
   * @param dest       destination buffer
   * @param destOffset offset into destination buffer
   * @param length     length of data to copy
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

  /**
   * Put a byte value out to the specified BB position in big-endian format.
   * @param buf    the byte buffer
   * @param offset position in the buffer
   * @param b      byte to write out
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
}
