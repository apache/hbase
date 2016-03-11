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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

@InterfaceAudience.Private
public class UnsafeAvailChecker {

  private static final String CLASS_NAME = "sun.misc.Unsafe";
  private static final Log LOG = LogFactory.getLog(UnsafeAvailChecker.class);
  private static boolean avail = false;
  private static boolean unaligned = false;

  static {
    avail = AccessController.doPrivileged(new PrivilegedAction<Boolean>() {
      @Override
      public Boolean run() {
        try {
          Class<?> clazz = Class.forName(CLASS_NAME);
          Field f = clazz.getDeclaredField("theUnsafe");
          f.setAccessible(true);
          return f.get(null) != null;
        } catch (Throwable e) {
          LOG.warn("sun.misc.Unsafe is not available/accessible", e);
        }
        return false;
      }
    });
    // When Unsafe itself is not available/accessible consider unaligned as false.
    if (avail) {
      try {
        // Using java.nio.Bits#unaligned() to check for unaligned-access capability
        Class<?> clazz = Class.forName("java.nio.Bits");
        Method m = clazz.getDeclaredMethod("unaligned");
        m.setAccessible(true);
        unaligned = (boolean) m.invoke(null);
      } catch (Exception e) {
        LOG.warn("java.nio.Bits#unaligned() check failed."
            + "Unsafe based read/write of primitive types won't be used", e);
      }
    }
  }

  /**
   * @return true when running JVM is having sun's Unsafe package available in it and it is
   *         accessible.
   */
  public static boolean isAvailable() {
    return avail;
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