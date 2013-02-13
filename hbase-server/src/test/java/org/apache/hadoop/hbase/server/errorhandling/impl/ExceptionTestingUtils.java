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
package org.apache.hadoop.hbase.server.errorhandling.impl;

/**
 * Utility class for testing error propagation
 */
public class ExceptionTestingUtils {

  /**
   * Determine if the stack trace contains the given calling class
   * @param stack trace to examine
   * @param clazz Class to search for
   * @return <tt>true</tt> if the stack contains the calling class
   */
  public static boolean stackContainsClass(StackTraceElement[] stack, Class<?> clazz) {
    String name = clazz.getName();
    for (StackTraceElement elem : stack) {
      if (elem.getClassName().equals(name)) return true;
    }
    return false;
  }
}