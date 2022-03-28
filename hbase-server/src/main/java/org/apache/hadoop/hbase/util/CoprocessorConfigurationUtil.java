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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * Helper class for coprocessor host when configuration changes.
 */
@InterfaceAudience.Private
public final class CoprocessorConfigurationUtil {

  private CoprocessorConfigurationUtil() {
  }

  public static boolean checkConfigurationChange(Configuration oldConfig, Configuration newConfig,
    String... configurationKey) {
    Preconditions.checkArgument(configurationKey != null, "Configuration Key(s) must be provided");
    boolean isConfigurationChange = false;
    for (String key : configurationKey) {
      String oldValue = oldConfig.get(key);
      String newValue = newConfig.get(key);
      // check if the coprocessor key has any difference
      if (!StringUtils.equalsIgnoreCase(oldValue, newValue)) {
        isConfigurationChange = true;
        break;
      }
    }
    return isConfigurationChange;
  }
}
