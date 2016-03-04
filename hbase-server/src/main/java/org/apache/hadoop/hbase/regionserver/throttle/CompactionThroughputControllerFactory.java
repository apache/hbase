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
package org.apache.hadoop.hbase.regionserver.throttle;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.util.ReflectionUtils;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public final class CompactionThroughputControllerFactory {

  private static final Log LOG = LogFactory.getLog(CompactionThroughputControllerFactory.class);

  public static final String HBASE_THROUGHPUT_CONTROLLER_KEY =
      "hbase.regionserver.throughput.controller";

  private CompactionThroughputControllerFactory() {
  }

  private static final Class<? extends ThroughputController>
      DEFAULT_THROUGHPUT_CONTROLLER_CLASS = PressureAwareCompactionThroughputController.class;

  // for backward compatibility and may not be supported in the future
  private static final String DEPRECATED_NAME_OF_PRESSURE_AWARE_THROUGHPUT_CONTROLLER_CLASS =
    "org.apache.hadoop.hbase.regionserver.compactions.PressureAwareCompactionThroughputController";
  private static final String DEPRECATED_NAME_OF_NO_LIMIT_THROUGHPUT_CONTROLLER_CLASS =
    "org.apache.hadoop.hbase.regionserver.compactions.NoLimitThroughputController";

  public static ThroughputController create(RegionServerServices server,
      Configuration conf) {
    Class<? extends ThroughputController> clazz = getThroughputControllerClass(conf);
    ThroughputController controller = ReflectionUtils.newInstance(clazz, conf);
    controller.setup(server);
    return controller;
  }

  public static Class<? extends ThroughputController> getThroughputControllerClass(
      Configuration conf) {
    String className =
        conf.get(HBASE_THROUGHPUT_CONTROLLER_KEY, DEFAULT_THROUGHPUT_CONTROLLER_CLASS.getName());
    className = resolveDeprecatedClassName(className);
    try {
      return Class.forName(className).asSubclass(ThroughputController.class);
    } catch (Exception e) {
      LOG.warn(
        "Unable to load configured throughput controller '" + className
            + "', load default throughput controller "
            + DEFAULT_THROUGHPUT_CONTROLLER_CLASS.getName() + " instead", e);
      return DEFAULT_THROUGHPUT_CONTROLLER_CLASS;
    }
  }

  /**
   * Resolve deprecated class name to keep backward compatibiliy
   * @param oldName old name of the class
   * @return the new name if there is any
   */
  private static String resolveDeprecatedClassName(String oldName) {
    String className = oldName.trim();
    if (className.equals(DEPRECATED_NAME_OF_PRESSURE_AWARE_THROUGHPUT_CONTROLLER_CLASS)) {
      className = PressureAwareCompactionThroughputController.class.getName();
    } else if (className.equals(DEPRECATED_NAME_OF_NO_LIMIT_THROUGHPUT_CONTROLLER_CLASS)) {
      className = NoLimitThroughputController.class.getName();
    }
    if (!className.equals(oldName)) {
      LOG.warn(oldName + " is deprecated, please use " + className + " instead");
    }
    return className;
  }
}