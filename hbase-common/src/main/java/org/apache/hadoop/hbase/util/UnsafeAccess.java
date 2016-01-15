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
import java.lang.reflect.Method;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

import sun.misc.Unsafe;

@InterfaceAudience.Private
@InterfaceStability.Evolving
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="REC_CATCH_EXCEPTION",
  justification="If exception, presume unaligned")
public final class UnsafeAccess {

  private static final Log LOG = LogFactory.getLog(UnsafeAccess.class);

  public static final Unsafe theUnsafe;

  /** The offset to the first element in a byte array. */
  public static final int BYTE_ARRAY_BASE_OFFSET;
  private static boolean unaligned = false;

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
      try {
        // Using java.nio.Bits#unaligned() to check for unaligned-access capability
        Class<?> clazz = Class.forName("java.nio.Bits");
        Method m = clazz.getDeclaredMethod("unaligned");
        m.setAccessible(true);
        unaligned = (boolean) m.invoke(null);
      } catch (Exception e) {
        unaligned = false; // FindBugs: Causes REC_CATCH_EXCEPTION. Suppressed.
      }
    } else{
      BYTE_ARRAY_BASE_OFFSET = -1;
    }
  }

  private UnsafeAccess(){}
  
  public static boolean isAvailable() {
    return theUnsafe != null;
  }

  /**
   * @return true when running JVM is having sun's Unsafe package available in it and underlying
   *         system having unaligned-access capability.
   */
  public static boolean unaligned() {
    return unaligned;
  }

  public static final boolean littleEndian = ByteOrder.nativeOrder()
      .equals(ByteOrder.LITTLE_ENDIAN);
}
