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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.CoprocessorReloadTask;
import org.apache.hadoop.hbase.security.access.BulkLoadReadOnlyController;
import org.apache.hadoop.hbase.security.access.EndpointReadOnlyController;
import org.apache.hadoop.hbase.security.access.MasterReadOnlyController;
import org.apache.hadoop.hbase.security.access.RegionReadOnlyController;
import org.apache.hadoop.hbase.security.access.RegionServerReadOnlyController;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.base.Strings;

/**
 * Helper class for coprocessor host when configuration changes.
 */
@InterfaceAudience.Private
public final class CoprocessorConfigurationUtil {
  private static final Logger LOG = LoggerFactory.getLogger(CoprocessorConfigurationUtil.class);

  private CoprocessorConfigurationUtil() {
  }

  /**
   * Check configuration change by comparing current loaded coprocessors with configuration values.
   * This method is useful when the configuration object has been updated, but we need to determine
   * if the coprocessor configuration has actually changed compared to what's currently loaded.
   * <p>
   * <b>Note:</b> This method only detects changes in the set of coprocessor class names. It does
   * <b>not</b> detect changes to priority or path for coprocessors that are already loaded with the
   * same class name. If you need to update the priority or path of an existing coprocessor, you
   * must restart the region/regionserver/master.
   * @param coprocessorHost  the coprocessor host to check current loaded coprocessors (can be null)
   * @param conf             the configuration to check
   * @param configurationKey the configuration keys to check
   * @return true if configuration has changed, false otherwise
   */
  public static boolean checkConfigurationChange(CoprocessorHost<?, ?> coprocessorHost,
    Configuration conf, String... configurationKey) {
    Preconditions.checkArgument(configurationKey != null, "Configuration Key(s) must be provided");
    Preconditions.checkArgument(conf != null, "Configuration must be provided");

    if (
      !conf.getBoolean(CoprocessorHost.COPROCESSORS_ENABLED_CONF_KEY,
        CoprocessorHost.DEFAULT_COPROCESSORS_ENABLED)
    ) {
      return false;
    }

    if (coprocessorHost == null) {
      // If no coprocessor host exists, check if any coprocessors are now configured
      return hasCoprocessorsConfigured(conf, configurationKey);
    }

    // Get currently loaded coprocessor class names
    Set<String> currentlyLoaded = coprocessorHost.getCoprocessorClassNames();

    // Get coprocessor class names from configuration
    // Only class names are compared; priority and path changes are not detected
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
    for (String key : configurationKey) {
      String[] coprocessors = conf.getStrings(key);
      if (coprocessors != null && coprocessors.length > 0) {
        return true;
      }
    }
    return false;
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

  /**
   * This method adds or removes relevant ReadOnlyController coprocessors to the provided
   * configuration based on whether read-only mode is enabled in the provided Configuration.
   * @param conf               The up-to-date configuration used to determine how to handle
   *                           coprocessors
   * @param coprocessorConfKey The configuration key name
   */
  public static void syncReadOnlyConfigurations(Configuration conf, String coprocessorConfKey) {
    boolean isReadOnlyModeEnabled = conf.getBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY,
      HConstants.HBASE_GLOBAL_READONLY_ENABLED_DEFAULT);

    List<String> cpList = getReadOnlyCoprocessors(coprocessorConfKey);
    if (isReadOnlyModeEnabled) {
      CoprocessorConfigurationUtil.addCoprocessors(conf, coprocessorConfKey, cpList);
    } else {
      CoprocessorConfigurationUtil.removeCoprocessors(conf, coprocessorConfKey, cpList);
    }
  }

  /**
   * Check whether ReadOnlyController coprocessors have been loaded in the provided configuration.
   * @param conf               the configuration we are checking
   * @param coprocessorConfKey configuration key used for setting master, region server, or region
   *                           coprocessors
   * @return true if the ReadOnlyCoprocessors are loaded in the configuration; false otherwise
   */
  public static boolean areReadOnlyCoprocessorsLoaded(Configuration conf,
    String coprocessorConfKey) {
    // Using a HashSet will improve performance when searching for read-only coprocessors
    HashSet<String> allCoprocessors =
      new HashSet<>(getCoprocessorsFromConfig(conf, coprocessorConfKey));
    List<String> readOnlyCoprocessors = getReadOnlyCoprocessors(coprocessorConfKey);
    return allCoprocessors.containsAll(readOnlyCoprocessors);
  }

  /**
   * Takes an updated configuration and updates the coprocessors for that configuration key in the
   * current configuration.
   * @param currentConf        the configuration currently used by the master, region server, or
   *                           region
   * @param updatedConf        the updated version of the configuration whose coprocessors we want
   *                           to copy
   * @param coprocessorConfKey configuration key used for setting master, region server, or region
   *                           coprocessors
   */
  public static void updateCoprocessorListInConf(Configuration currentConf,
    Configuration updatedConf, String coprocessorConfKey) {
    String[] updatedCoprocessorList = updatedConf.getStrings(coprocessorConfKey);
    if (updatedCoprocessorList != null) {
      currentConf.setStrings(coprocessorConfKey, updatedCoprocessorList);
    } else {
      currentConf.unset(coprocessorConfKey);
    }
  }

  /**
   * Gets the name of a component based on the provided coprocessor configuration key.
   * @param coprocessorConfKey configuration key used for setting master, region server, or region
   *                           coprocessors
   * @return the component type - Master, Region Server, or Region
   */
  public static String getComponentName(String coprocessorConfKey) {
    return switch (coprocessorConfKey) {
      case CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY -> "Master";
      case CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY -> "Region Server";
      case CoprocessorHost.REGION_COPROCESSOR_CONF_KEY -> "Region";
      default -> throw new IllegalArgumentException(
        "Unsupported coprocessor configuration key: " + coprocessorConfKey);
    };
  }

  /**
   * This method updates the coprocessors on the master, region server, or region if a change has
   * been detected. Detected changes include changes in coprocessors or changes in read-only mode
   * configuration. If a change is detected, then new coprocessors are loaded using the provided
   * reload method. The new value for the read-only config variable is updated as well.
   * @param newConf                   an updated configuration
   * @param originalIsReadOnlyEnabled the original value for
   *                                  {@value HConstants#HBASE_GLOBAL_READONLY_ENABLED_KEY}
   * @param coprocessorHost           the coprocessor host for HMaster, HRegionServer, or HRegion
   * @param coprocessorConfKey        configuration key used for setting master, region server, or
   *                                  region coprocessors
   * @param isMaintenanceMode         whether maintenance mode is active (mainly for HMaster)
   * @param instance                  string value of the instance calling this method (mainly helps
   *                                  with tracking region logging)
   * @param reloadTask                lambda function that reloads coprocessors on the master,
   *                                  region server, or region
   */
  public static void maybeUpdateCoprocessors(Configuration newConf,
    boolean originalIsReadOnlyEnabled, CoprocessorHost<?, ?> coprocessorHost,
    String coprocessorConfKey, boolean isMaintenanceMode, String instance,
    CoprocessorReloadTask reloadTask) {

    boolean maybeUpdatedReadOnlyMode = ConfigurationUtil.isReadOnlyModeEnabledInConf(newConf);
    boolean hasReadOnlyModeChanged = originalIsReadOnlyEnabled != maybeUpdatedReadOnlyMode;
    boolean hasCoprocessorConfigChanged = CoprocessorConfigurationUtil
      .checkConfigurationChange(coprocessorHost, newConf, coprocessorConfKey);

    // update region server coprocessor if the configuration has changed.
    if ((hasCoprocessorConfigChanged || hasReadOnlyModeChanged) && !isMaintenanceMode) {
      LOG.info("Updating coprocessors for {} {} because the configuration has changed",
        getComponentName(coprocessorConfKey), instance);
      CoprocessorConfigurationUtil.syncReadOnlyConfigurations(newConf, coprocessorConfKey);
      reloadTask.reload(newConf);
    }

    if (hasReadOnlyModeChanged) {
      LOG.info("Config {} has been dynamically changed to {} for {} {}",
        HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY, maybeUpdatedReadOnlyMode,
        getComponentName(coprocessorConfKey), instance);
    }
  }
}
