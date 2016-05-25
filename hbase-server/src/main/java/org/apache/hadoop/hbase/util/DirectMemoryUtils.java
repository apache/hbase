/**
 *
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

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Locale;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

import com.google.common.base.Preconditions;

/**
 * Utilities for interacting with and monitoring DirectByteBuffer allocations.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DirectMemoryUtils {
  private static final Log LOG = LogFactory.getLog(DirectMemoryUtils.class);
  private static final String MEMORY_USED = "MemoryUsed";
  private static final MBeanServer BEAN_SERVER;
  private static final ObjectName NIO_DIRECT_POOL;
  private static final boolean HAS_MEMORY_USED_ATTRIBUTE;

  static {
    // initialize singletons. Only maintain a reference to the MBeanServer if
    // we're able to consume it -- hence convoluted logic.
    ObjectName n = null;
    MBeanServer s = null;
    Object a = null;
    try {
      n = new ObjectName("java.nio:type=BufferPool,name=direct");
    } catch (MalformedObjectNameException e) {
      LOG.warn("Unable to initialize ObjectName for DirectByteBuffer allocations.");
    } finally {
      NIO_DIRECT_POOL = n;
    }
    if (NIO_DIRECT_POOL != null) {
      s = ManagementFactory.getPlatformMBeanServer();
    }
    BEAN_SERVER = s;
    if (BEAN_SERVER != null) {
      try {
        a = BEAN_SERVER.getAttribute(NIO_DIRECT_POOL, MEMORY_USED);
      } catch (JMException e) {
        LOG.debug("Failed to retrieve nio.BufferPool direct MemoryUsed attribute: " + e);
      }
    }
    HAS_MEMORY_USED_ATTRIBUTE = a != null;
  }

  /**
   * @return the setting of -XX:MaxDirectMemorySize as a long. Returns 0 if
   *         -XX:MaxDirectMemorySize is not set.
   */
  public static long getDirectMemorySize() {
    RuntimeMXBean runtimemxBean = ManagementFactory.getRuntimeMXBean();
    List<String> arguments = runtimemxBean.getInputArguments();
    long multiplier = 1; //for the byte case.
    for (String s : arguments) {
      if (s.contains("-XX:MaxDirectMemorySize=")) {
        String memSize = s.toLowerCase(Locale.ROOT)
            .replace("-xx:maxdirectmemorysize=", "").trim();

        if (memSize.contains("k")) {
          multiplier = 1024;
        }

        else if (memSize.contains("m")) {
          multiplier = 1048576;
        }

        else if (memSize.contains("g")) {
          multiplier = 1073741824;
        }
        memSize = memSize.replaceAll("[^\\d]", "");

        long retValue = Long.parseLong(memSize);
        return retValue * multiplier;
      }
    }
    return 0;
  }

  /**
   * @return the current amount of direct memory used.
   */
  public static long getDirectMemoryUsage() {
    if (BEAN_SERVER == null || NIO_DIRECT_POOL == null || !HAS_MEMORY_USED_ATTRIBUTE) return 0;
    try {
      Long value = (Long) BEAN_SERVER.getAttribute(NIO_DIRECT_POOL, MEMORY_USED);
      return value == null ? 0 : value;
    } catch (JMException e) {
      // should print further diagnostic information?
      return 0;
    }
  }

  /**
   * DirectByteBuffers are garbage collected by using a phantom reference and a
   * reference queue. Every once a while, the JVM checks the reference queue and
   * cleans the DirectByteBuffers. However, as this doesn't happen
   * immediately after discarding all references to a DirectByteBuffer, it's
   * easy to OutOfMemoryError yourself using DirectByteBuffers. This function
   * explicitly calls the Cleaner method of a DirectByteBuffer.
   * 
   * @param toBeDestroyed
   *          The DirectByteBuffer that will be "cleaned". Utilizes reflection.
   *          
   */
  public static void destroyDirectByteBuffer(ByteBuffer toBeDestroyed)
      throws IllegalArgumentException, IllegalAccessException,
      InvocationTargetException, SecurityException, NoSuchMethodException {

    Preconditions.checkArgument(toBeDestroyed.isDirect(),
        "toBeDestroyed isn't direct!");

    Method cleanerMethod = toBeDestroyed.getClass().getMethod("cleaner");
    cleanerMethod.setAccessible(true);
    Object cleaner = cleanerMethod.invoke(toBeDestroyed);
    Method cleanMethod = cleaner.getClass().getMethod("clean");
    cleanMethod.setAccessible(true);
    cleanMethod.invoke(cleaner);
  }
}
