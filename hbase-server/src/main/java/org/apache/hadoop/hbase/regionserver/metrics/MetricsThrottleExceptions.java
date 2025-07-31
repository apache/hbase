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
package org.apache.hadoop.hbase.regionserver.metrics;

import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.quotas.RpcThrottlingException;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class MetricsThrottleExceptions {

  /**
   * The name of the metrics
   */
  private static final String METRICS_NAME = "ThrottleExceptions";

  /**
   * The name of the metrics context that metrics will be under.
   */
  private static final String METRICS_CONTEXT = "regionserver";

  /**
   * Description
   */
  private static final String METRICS_DESCRIPTION = "Metrics about RPC throttling exceptions";

  /**
   * The name of the metrics context that metrics will be under in jmx
   */
  private static final String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;

  private final MetricRegistry registry;

  public MetricsThrottleExceptions(MetricRegistry sharedRegistry) {
    registry = sharedRegistry;
  }

  /**
   * Record a throttle exception with contextual information.
   * @param throttleType the type of throttle exception
   * @param user         the user who triggered the throttle
   * @param table        the table that was being accessed
   */
  public void recordThrottleException(RpcThrottlingException.Type throttleType, String user,
    String table) {
    String metricName = qualifyThrottleMetric(throttleType, user, table);
    registry.counter(metricName).increment();
  }

  private static String qualifyThrottleMetric(RpcThrottlingException.Type throttleType, String user,
    String table) {
    return String.format("Type_%s_User_%s_Table_%s", throttleType.name(), sanitizeMetricName(user),
      sanitizeMetricName(table));
  }

  private static String sanitizeMetricName(String name) {
    if (name == null) {
      return "unknown";
    }
    // Only replace characters that are problematic for JMX ObjectNames
    // Keep meaningful characters like hyphens, periods, etc.
    return name.replaceAll("[,=:*?\"\\n]", "_");
  }

}
