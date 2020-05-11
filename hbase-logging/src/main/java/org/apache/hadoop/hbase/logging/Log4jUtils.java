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
package org.apache.hadoop.hbase.logging;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Set;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A bridge class for operating on log4j, such as changing log level, etc.
 * <p/>
 * Will call the methods in {@link InternalLog4jUtils} to actually operate on the log4j stuff.
 */
@InterfaceAudience.Private
public final class Log4jUtils {

  private static final String INTERNAL_UTILS_CLASS_NAME =
    "org.apache.hadoop.hbase.logging.InternalLog4jUtils";

  private Log4jUtils() {
  }

  // load class when calling to avoid introducing class not found exception on log4j when loading
  // this class even without calling any of the methods below.
  private static Method getMethod(String methodName, Class<?>... args) {
    try {
      Class<?> clazz = Class.forName(INTERNAL_UTILS_CLASS_NAME);
      return clazz.getDeclaredMethod(methodName, args);
    } catch (ClassNotFoundException | NoSuchMethodException e) {
      throw new AssertionError("should not happen", e);
    }
  }

  private static void throwUnchecked(Throwable throwable) {
    if (throwable instanceof RuntimeException) {
      throw (RuntimeException) throwable;
    }
    if (throwable instanceof Error) {
      throw (Error) throwable;
    }
  }

  public static void setLogLevel(String loggerName, String levelName) {
    Method method = getMethod("setLogLevel", String.class, String.class);
    try {
      method.invoke(null, loggerName, levelName);
    } catch (IllegalAccessException e) {
      throw new AssertionError("should not happen", e);
    } catch (InvocationTargetException e) {
      throwUnchecked(e.getCause());
      throw new AssertionError("should not happen", e.getCause());
    }
  }

  public static String getEffectiveLevel(String loggerName) {
    Method method = getMethod("getEffectiveLevel", String.class);
    try {
      return (String) method.invoke(null, loggerName);
    } catch (IllegalAccessException e) {
      throw new AssertionError("should not happen", e);
    } catch (InvocationTargetException e) {
      throwUnchecked(e.getCause());
      throw new AssertionError("should not happen", e.getCause());
    }
  }

  @SuppressWarnings("unchecked")
  public static Set<File> getActiveLogFiles() throws IOException {
    Method method = getMethod("getActiveLogFiles");
    try {
      return (Set<File>) method.invoke(null);
    } catch (IllegalAccessException e) {
      throw new AssertionError("should not happen", e);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      throwUnchecked(cause);
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new AssertionError("should not happen", cause);
    }
  }

  /**
   * Disables Zk- and HBase client logging
   */
  public static void disableZkAndClientLoggers() {
    // disable zookeeper log to avoid it mess up command output
    setLogLevel("org.apache.zookeeper", "OFF");
    // disable hbase zookeeper tool log to avoid it mess up command output
    setLogLevel("org.apache.hadoop.hbase.zookeeper", "OFF");
    // disable hbase client log to avoid it mess up command output
    setLogLevel("org.apache.hadoop.hbase.client", "OFF");
  }

  /**
   * Switches the logger for the given class to DEBUG level.
   * @param clazz The class for which to switch to debug logging.
   */
  public static void enableDebug(Class<?> clazz) {
    setLogLevel(clazz.getName(), "DEBUG");
  }
}
