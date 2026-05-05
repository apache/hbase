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
package org.apache.hadoop.hbase.ipc;

import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ShadedPrefixUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ShadedPrefixUtil.class);

  // Marked with '!' to prevent shading tools from replacing this value
  static final String ORIGINAL_BASE = "org!apache!hadoop!hbase".replace('!', '.');

  private static final ShadedPrefixUtil INSTANCE =
    newInstance(ShadedPrefixUtil.class.getPackage().getName());

  /**
   * The shaded equivalent of "org.apache.hadoop.hbase", or null if not in a shaded environment.
   * <ul>
   * <li>Prefix-prepend shading (e.g., current package "a.b.c.org.apache.hadoop.hbase.ipc"):
   * "a.b.c.org.apache.hadoop.hbase"</li>
   * <li>Full-replacement shading (e.g., current package "shaded.hbase1.ipc"): "shaded.hbase1"</li>
   * </ul>
   */
  private final String shadedBase;

  /**
   * Cache from original class name to its resolved (shaded or original) equivalent. Class.forName
   * for a missing class is expensive as it searches all classloaders, so we memoize the result to
   * pay that cost at most once per distinct class name.
   */
  private final ConcurrentHashMap<String, String> resolvedClassNames = new ConcurrentHashMap<>();

  private ShadedPrefixUtil(String shadedBase) {
    this.shadedBase = shadedBase;
  }

  static ShadedPrefixUtil newInstance(String currentPackageName) {
    // The ipc suffix identifies which sub-package this class lives in
    final String ipcSuffix = ".ipc";
    final String originalPackage = ORIGINAL_BASE + ipcSuffix;

    if (currentPackageName == null || currentPackageName.equals(originalPackage)) {
      LOG.debug("{} is not a shaded package", currentPackageName);
      return new ShadedPrefixUtil(null);
    }

    if (!currentPackageName.endsWith(ipcSuffix)) {
      LOG.debug("Cannot determine shading mapping for {}", currentPackageName);
      return new ShadedPrefixUtil(null);
    }

    // Works for both prefix-prepend and full-replacement shading strategies:
    // - "a.b.c.org.apache.hadoop.hbase.ipc" -> shadedBase = "a.b.c.org.apache.hadoop.hbase"
    // - "shaded.hbase1.ipc" -> shadedBase = "shaded.hbase1"
    String detectedShadedBase =
      currentPackageName.substring(0, currentPackageName.length() - ipcSuffix.length());
    LOG.debug("{} is a shaded package, shadedBase={}", currentPackageName, detectedShadedBase);
    return new ShadedPrefixUtil(detectedShadedBase);
  }

  String getShadedBase() {
    return shadedBase;
  }

  /**
   * Maps an original HBase class name to its shaded equivalent by replacing the
   * "org.apache.hadoop.hbase" prefix with the detected shaded base package. Returns the original
   * class name unchanged if not in a shaded environment or if the class name does not start with
   * the original HBase package. Note: this is a pure string transformation with no class loading.
   * Use {@link #resolveShading(String)} when class existence must be verified.
   */
  String applyShading(String className) {
    if (shadedBase == null || className == null) {
      return className;
    }
    if (!className.startsWith(ORIGINAL_BASE + ".")) {
      return className;
    }
    return shadedBase + className.substring(ORIGINAL_BASE.length());
  }

  /**
   * Returns the effective class name for the given original HBase class name. Prefers the shaded
   * class name when it can be loaded, and falls back to the original when the shaded class is not
   * available (e.g., when exception classes are intentionally excluded from shading). The result is
   * cached so that Class.forName is invoked at most once per distinct class name.
   */
  String resolveShading(String originalName) {
    if (shadedBase == null || originalName == null) {
      return originalName;
    }
    return resolvedClassNames.computeIfAbsent(originalName, this::computeResolvedName);
  }

  private String computeResolvedName(String originalName) {
    String shadedName = applyShading(originalName);
    if (shadedName.equals(originalName)) {
      return originalName;
    }
    try {
      Class.forName(shadedName, false, ShadedPrefixUtil.class.getClassLoader());
      return shadedName;
    } catch (Throwable t) {
      LOG.debug("Shaded class {} not found, falling back to {}", shadedName, originalName);
      return originalName;
    }
  }

  public static ShadedPrefixUtil getInstance() {
    return INSTANCE;
  }
}
