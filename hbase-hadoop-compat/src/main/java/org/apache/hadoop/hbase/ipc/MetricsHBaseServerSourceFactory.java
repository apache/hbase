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

package org.apache.hadoop.hbase.ipc;

public abstract class MetricsHBaseServerSourceFactory {
  /**
   * The name of the metrics
   */
  static final String METRICS_NAME = "IPC";

  /**
   * Description
   */
  static final String METRICS_DESCRIPTION = "Metrics about HBase Server IPC";

  /**
   * The Suffix of the JMX Context that a MetricsHBaseServerSource will register under.
   *
   * JMX_CONTEXT will be created by createContextName(serverClassName) + METRICS_JMX_CONTEXT_SUFFIX
   */
  static final String METRICS_JMX_CONTEXT_SUFFIX = ",sub=" + METRICS_NAME;

  abstract MetricsHBaseServerSource create(String serverName, MetricsHBaseServerWrapper wrapper);

  /**
   * From the name of the class that's starting up create the
   * context that an IPC source should register itself.
   *
   * @param serverName The name of the class that's starting up.
   * @return The Camel Cased context name.
   */
  protected static String createContextName(String serverName) {
    if (serverName.startsWith("HMaster")) {
      return "Master";
    } else if (serverName.startsWith("HRegion")) {
      return "RegionServer";
    }
    return "IPC";
  }
}
