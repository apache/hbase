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
import java.security.AccessController;
import java.security.PrivilegedAction;
import sun.misc.Unsafe;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("restriction")
@InterfaceAudience.Private
public class UnsafeAvailChecker {

  private static final Unsafe UNSAFE;
  private static final String CLASS_NAME = "sun.misc.Unsafe";
  private static final Logger LOG = LoggerFactory.getLogger(UnsafeAvailChecker.class);
  private static boolean unaligned = false;
  // Split java.version on non-digit chars:
  private static final int majorVersion =
      Integer.parseInt(System.getProperty("java.version").split("\\D+")[0]);

  static {
    UNSAFE = AccessController.doPrivileged(new PrivilegedAction<Unsafe>() {
      @Override
      public Unsafe run() {
        try {
          Class<?> clazz = Class.forName(CLASS_NAME);
          Field f = clazz.getDeclaredField("theUnsafe");
          f.setAccessible(true);
          return (Unsafe) f.get(null);
        } catch (Throwable e) {
          LOG.warn("sun.misc.Unsafe is not available/accessible", e);
        }
        return null;
      }
    });
    // When Unsafe itself is not available/accessible consider unaligned as false.
    if (UNSAFE != null) {
      String arch = System.getProperty("os.arch");
      if ("ppc64".equals(arch) || "ppc64le".equals(arch) || "aarch64".equals(arch)) {
        // java.nio.Bits.unaligned() wrongly returns false on ppc (JDK-8165231),
        unaligned = true;
      } else {
        try {
          Class<?> bitsClass =
              Class.forName("java.nio.Bits", false, ClassLoader.getSystemClassLoader());
          if (majorVersion >= 9) {
            // Java 9/10 and 11/12 have different field names.
            Field unalignedField =
                bitsClass.getDeclaredField(majorVersion >= 11 ? "UNALIGNED" : "unaligned");
            unaligned = UNSAFE.getBoolean(UNSAFE.staticFieldBase(unalignedField),
              UNSAFE.staticFieldOffset(unalignedField));
          } else {
            // Using java.nio.Bits#unaligned() to check for unaligned-access capability
            Method unalignedMethod = bitsClass.getDeclaredMethod("unaligned");
            unalignedMethod.setAccessible(true);
            unaligned = (Boolean) unalignedMethod.invoke(null);
          }
        } catch (Throwable t) {
          LOG.warn(
            "unaligned check failed. Unsafe based read/write of primitive types won't be used", t);
        }
      }
    }
  }

  /**
   * @return true when running JVM is having sun's Unsafe package available in it and it is
   *         accessible.
   */
  public static boolean isAvailable() {
    return UNSAFE != null;
  }

  /**
   * @return true when running JVM is having sun's Unsafe package available in it and underlying
   *         system having unaligned-access capability.
   */
  public static boolean unaligned() {
    return unaligned;
  }

  private UnsafeAvailChecker() {
    // private constructor to avoid instantiation
  }
}
