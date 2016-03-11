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
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="REC_CATCH_EXCEPTION",
  justification="If exception, presume unaligned")
public final class UnsafeAccess {

  private static final Log LOG = LogFactory.getLog(UnsafeAccess.class);

  public static final Unsafe theUnsafe;

  /** The offset to the first element in a byte array. */
  public static final int BYTE_ARRAY_BASE_OFFSET;

  // This number limits the number of bytes to copy per call to Unsafe's
  // copyMemory method. A limit is imposed to allow for safepoint polling
  // during a large copy
  static final long UNSAFE_COPY_THRESHOLD = 1024L * 1024L;
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

  // APIs to copy data. This will be direct memory location copy and will be much faster
  /**
   * Copies the bytes from given array's offset to length part into the given buffer.
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
    unsafeCopy(src, srcAddress, destBase, destAddress, length);
  }

  private static void unsafeCopy(Object src, long srcAddr, Object dst, long destAddr, long len) {
    while (len > 0) {
      long size = (len > UNSAFE_COPY_THRESHOLD) ? UNSAFE_COPY_THRESHOLD : len;
      theUnsafe.copyMemory(src, srcAddr, dst, destAddr, len);
      len -= size;
      srcAddr += size;
      destAddr += size;
    }
  }

  /**
   * Copies specified number of bytes from given offset of {@code src} ByteBuffer to the
   * {@code dest} array.
   *
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
    unsafeCopy(srcBase, srcAddress, dest, destAddress, length);
  }

  /**
   * Copies specified number of bytes from given offset of {@code src} buffer into the {@code dest}
   * buffer.
   *
   * @param src
   * @param srcOffset
   * @param dest
   * @param destOffset
   * @param length
   */
  public static void copy(ByteBuffer src, int srcOffset, ByteBuffer dest, int destOffset,
      int length) {
    long srcAddress, destAddress;
    Object srcBase = null, destBase = null;
    if (src.isDirect()) {
      srcAddress = srcOffset + ((DirectBuffer) src).address();
    } else {
      srcAddress = srcOffset +  src.arrayOffset() + BYTE_ARRAY_BASE_OFFSET;
      srcBase = src.array();
    }
    if (dest.isDirect()) {
      destAddress = destOffset + ((DirectBuffer) dest).address();
    } else {
      destAddress = destOffset + BYTE_ARRAY_BASE_OFFSET + dest.arrayOffset();
      destBase = dest.array();
    }
    unsafeCopy(srcBase, srcAddress, destBase, destAddress, length);
  }
}
