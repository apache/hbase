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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ShadedPrefixUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ShadedPrefixUtil.class);

  private static final ShadedPrefixUtil INSTANCE =
    newInstance(ShadedPrefixUtil.class.getPackage().getName());

  /**
   * If it is a shaded class, it informs what the shaded pattern is. If null, it means there is no
   * shaded pattern, which means it is not a shaded class.
   */
  private final String shadedPrefix;

  private ShadedPrefixUtil(String shadedPrefix) {
    this.shadedPrefix = shadedPrefix;
  }

  static ShadedPrefixUtil newInstance(String currentPackageName) {
    // Marked with '!' to prevent shading
    final String originalPackagePrefix = "org!apache!hadoop!hbase".replace('!', '.');

    if (StringUtils.startsWith(currentPackageName, originalPackagePrefix)) {
      LOG.debug("{} is not a shaded package", currentPackageName);
      return new ShadedPrefixUtil(null);
    }

    String shadedPattern =
      StringUtils.substringBefore(currentPackageName, "." + originalPackagePrefix);
    if (
      StringUtils.isEmpty(shadedPattern) || StringUtils.equals(shadedPattern, currentPackageName)
    ) {
      LOG.debug("Cannot find shaded pattern for {}", currentPackageName);
      return new ShadedPrefixUtil(null);
    }

    if (!StringUtils.equals(StringUtils.right(shadedPattern, 1), ".")) {
      shadedPattern = shadedPattern + ".";
    }

    LOG.debug("{} is a shaded package, shadedPattern={}", currentPackageName, shadedPattern);
    return new ShadedPrefixUtil(shadedPattern);
  }

  String getShadedPrefix() {
    return shadedPrefix;
  }

  String applyShadedPrefix(String className) {
    if (shadedPrefix == null || className == null) {
      return className;
    }
    return shadedPrefix + className;
  }

  public static ShadedPrefixUtil getInstance() {
    return INSTANCE;
  }
}
