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
package org.apache.hadoop.hbase.util;

import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.base.Strings;

/**
 * Helper class for coprocessor host when configuration changes.
 */
@InterfaceAudience.Private
public final class CoprocessorConfigurationUtil {

  private CoprocessorConfigurationUtil() {
  }

  /**
   * Check configuration change by comparing current loaded coprocessors with configuration values.
   * This method is useful when the configuration object has been updated but we need to determine
   * if coprocessor configuration has actually changed compared to what's currently loaded.
   * @param coprocessorHost  the coprocessor host to check current loaded coprocessors (can be null)
   * @param conf             the configuration to check
   * @param configurationKey the configuration keys to check
   * @return true if configuration has changed, false otherwise
   */
  public static boolean checkConfigurationChange(CoprocessorHost<?, ?> coprocessorHost,
    Configuration conf, String... configurationKey) {
    Preconditions.checkArgument(configurationKey != null, "Configuration Key(s) must be provided");
    Preconditions.checkArgument(conf != null, "Configuration must be provided");

    if (coprocessorHost == null) {
      // If no coprocessor host exists, check if any coprocessors are now configured
      return hasCoprocessorsConfigured(conf, configurationKey);
    }

    // Get currently loaded coprocessor class names
    Set<String> currentlyLoaded = coprocessorHost.getCoprocessorClassNames();

    // Get coprocessor class names from configuration
    Set<String> configuredClasses = new HashSet<>();
    for (String key : configurationKey) {
      String[] classes = conf.getStrings(key);
      if (classes != null) {
        for (String className : classes) {
          // Handle the className|priority|path format
          String[] classNameToken = className.split("\\|");
          String actualClassName = classNameToken[0].trim();
          if (!Strings.isNullOrEmpty(actualClassName)) {
            configuredClasses.add(actualClassName);
          }
        }
      }
    }

    // Compare the two sets
    return !currentlyLoaded.equals(configuredClasses);
  }

  /**
   * Helper method to check if there are any coprocessors configured.
   */
  private static boolean hasCoprocessorsConfigured(Configuration conf, String... configurationKey) {
    if (
      !conf.getBoolean(CoprocessorHost.COPROCESSORS_ENABLED_CONF_KEY,
        CoprocessorHost.DEFAULT_COPROCESSORS_ENABLED)
    ) {
      return false;
    }

    for (String key : configurationKey) {
      String[] coprocessors = conf.getStrings(key);
      if (coprocessors != null && coprocessors.length > 0) {
        return true;
      }
    }
    return false;
  }
}
