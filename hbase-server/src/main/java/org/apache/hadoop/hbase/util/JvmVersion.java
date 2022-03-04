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

import java.util.HashSet;
import java.util.Set;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Utility class to get and check the current JVM version.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public abstract class JvmVersion {
  private static Set<String> BAD_JVM_VERSIONS = new HashSet<>();
  static {
    BAD_JVM_VERSIONS.add("1.6.0_18");
  }

  /**
   * Return true if the current JVM version is known to be unstable with HBase.
   */
  public static boolean isBadJvmVersion() {
    String version = System.getProperty("java.version");
    return version != null && BAD_JVM_VERSIONS.contains(version);
  }

  /**
   * Return the current JVM version information.
   */
  public static String getVersion() {
    return System.getProperty("java.vm.vendor", "UNKNOWN_VM_VENDOR") + ' ' +
      System.getProperty("java.version", "UNKNOWN_JAVA_VERSION") + '-' +
      System.getProperty("java.vm.version", "UNKNOWN_VM_VERSION");
  }
}
