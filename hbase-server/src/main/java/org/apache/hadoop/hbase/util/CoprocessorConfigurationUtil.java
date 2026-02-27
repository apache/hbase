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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.security.access.BulkLoadReadOnlyController;
import org.apache.hadoop.hbase.security.access.EndpointReadOnlyController;
import org.apache.hadoop.hbase.security.access.MasterReadOnlyController;
import org.apache.hadoop.hbase.security.access.RegionReadOnlyController;
import org.apache.hadoop.hbase.security.access.RegionServerReadOnlyController;
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

  private static List<String> getCoprocessorsFromConfig(Configuration conf,
    String configurationKey) {
    String[] existing = conf.getStrings(configurationKey);
    return existing != null ? new ArrayList<>(Arrays.asList(existing)) : new ArrayList<>();
  }

  public static void addCoprocessors(Configuration conf, String configurationKey,
    List<String> coprocessorsToAdd) {
    List<String> existing = getCoprocessorsFromConfig(conf, configurationKey);

    boolean isModified = false;

    for (String coprocessor : coprocessorsToAdd) {
      if (!existing.contains(coprocessor)) {
        existing.add(coprocessor);
        isModified = true;
      }
    }

    if (isModified) {
      conf.setStrings(configurationKey, existing.toArray(new String[0]));
    }
  }

  public static void removeCoprocessors(Configuration conf, String configurationKey,
    List<String> coprocessorsToRemove) {
    List<String> existing = getCoprocessorsFromConfig(conf, configurationKey);

    if (existing.isEmpty()) {
      return;
    }

    boolean isModified = false;

    for (String coprocessor : coprocessorsToRemove) {
      if (existing.contains(coprocessor)) {
        existing.remove(coprocessor);
        isModified = true;
      }
    }

    if (isModified) {
      conf.setStrings(configurationKey, existing.toArray(new String[0]));
    }
  }

  private static List<String> getReadOnlyCoprocessors(String configurationKey) {
    return switch (configurationKey) {
      case CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY -> List
        .of(MasterReadOnlyController.class.getName());

      case CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY -> List
        .of(RegionServerReadOnlyController.class.getName());

      case CoprocessorHost.REGION_COPROCESSOR_CONF_KEY -> List.of(
        RegionReadOnlyController.class.getName(), BulkLoadReadOnlyController.class.getName(),
        EndpointReadOnlyController.class.getName());

      default -> throw new IllegalArgumentException(
        "Unsupported coprocessor configuration key: " + configurationKey);
    };
  }

  public static void syncReadOnlyConfigurations(boolean readOnlyMode, Configuration conf,
    String configurationKey) {
    conf.setBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY, readOnlyMode);

    List<String> cpList = getReadOnlyCoprocessors(configurationKey);
    // If readonly is true then add the coprocessor of master
    if (readOnlyMode) {
      CoprocessorConfigurationUtil.addCoprocessors(conf, configurationKey, cpList);
    } else {
      CoprocessorConfigurationUtil.removeCoprocessors(conf, configurationKey, cpList);
    }
  }
}
