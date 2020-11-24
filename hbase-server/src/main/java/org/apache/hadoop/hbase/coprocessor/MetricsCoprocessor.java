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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.hadoop.hbase.coprocessor;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.metrics.MetricRegistries;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.metrics.MetricRegistryInfo;

/**
 * Utility class for tracking metrics for various types of coprocessors. Each coprocessor instance
 * creates its own MetricRegistry which is exported as an individual MetricSource. MetricRegistries
 * are ref counted using the hbase-metric module interfaces.
 */
@InterfaceAudience.Private
public class MetricsCoprocessor {

  // Master coprocessor metrics
  private static final String MASTER_COPROC_METRICS_NAME = "Coprocessor.Master";
  private static final String MASTER_COPROC_METRICS_CONTEXT = "master";
  private static final String MASTER_COPROC_METRICS_DESCRIPTION
      = "Metrics about HBase MasterObservers";
  private static final String MASTER_COPROC_METRICS_JMX_CONTEXT
      = "Master,sub=" + MASTER_COPROC_METRICS_NAME;

  // RegionServer coprocessor metrics
  private static final String RS_COPROC_METRICS_NAME = "Coprocessor.RegionServer";
  private static final String RS_COPROC_METRICS_CONTEXT = "regionserver";
  private static final String RS_COPROC_METRICS_DESCRIPTION
      = "Metrics about HBase RegionServerObservers";
  private static final String RS_COPROC_METRICS_JMX_CONTEXT = "RegionServer,sub="
      + RS_COPROC_METRICS_NAME;

  // Region coprocessor metrics
  private static final String REGION_COPROC_METRICS_NAME = "Coprocessor.Region";
  private static final String REGION_COPROC_METRICS_CONTEXT = "regionserver";
  private static final String REGION_COPROC_METRICS_DESCRIPTION
      = "Metrics about HBase RegionObservers";
  private static final String REGION_COPROC_METRICS_JMX_CONTEXT
      = "RegionServer,sub=" + REGION_COPROC_METRICS_NAME;

  // WAL coprocessor metrics
  private static final String WAL_COPROC_METRICS_NAME = "Coprocessor.WAL";
  private static final String WAL_COPROC_METRICS_CONTEXT = "regionserver";
  private static final String WAL_COPROC_METRICS_DESCRIPTION
      = "Metrics about HBase WALObservers";
  private static final String WAL_COPROC_METRICS_JMX_CONTEXT
      = "RegionServer,sub=" + WAL_COPROC_METRICS_NAME;

  private static String suffix(String metricName, String cpName) {
    return new StringBuilder(metricName)
        .append(".")
        .append("CP_")
        .append(cpName)
        .toString();
  }

  static MetricRegistryInfo createRegistryInfoForMasterCoprocessor(String clazz) {
    return new MetricRegistryInfo(
        suffix(MASTER_COPROC_METRICS_NAME, clazz),
        MASTER_COPROC_METRICS_DESCRIPTION,
        suffix(MASTER_COPROC_METRICS_JMX_CONTEXT, clazz),
        MASTER_COPROC_METRICS_CONTEXT, false);
  }

  public static MetricRegistry createRegistryForMasterCoprocessor(String clazz) {
    return MetricRegistries.global().create(createRegistryInfoForMasterCoprocessor(clazz));
  }

  static MetricRegistryInfo createRegistryInfoForRSCoprocessor(String clazz) {
    return new MetricRegistryInfo(
        suffix(RS_COPROC_METRICS_NAME, clazz),
        RS_COPROC_METRICS_DESCRIPTION,
        suffix(RS_COPROC_METRICS_JMX_CONTEXT, clazz),
        RS_COPROC_METRICS_CONTEXT, false);
  }

  public static MetricRegistry createRegistryForRSCoprocessor(String clazz) {
    return MetricRegistries.global().create(createRegistryInfoForRSCoprocessor(clazz));
  }

  public static MetricRegistryInfo createRegistryInfoForRegionCoprocessor(String clazz) {
    return new MetricRegistryInfo(
        suffix(REGION_COPROC_METRICS_NAME, clazz),
        REGION_COPROC_METRICS_DESCRIPTION,
        suffix(REGION_COPROC_METRICS_JMX_CONTEXT, clazz),
        REGION_COPROC_METRICS_CONTEXT, false);
  }

  public static MetricRegistry createRegistryForRegionCoprocessor(String clazz) {
    return MetricRegistries.global().create(createRegistryInfoForRegionCoprocessor(clazz));
  }

  public static MetricRegistryInfo createRegistryInfoForWALCoprocessor(String clazz) {
    return new MetricRegistryInfo(
        suffix(WAL_COPROC_METRICS_NAME, clazz),
        WAL_COPROC_METRICS_DESCRIPTION,
        suffix(WAL_COPROC_METRICS_JMX_CONTEXT, clazz),
        WAL_COPROC_METRICS_CONTEXT, false);
  }

  public static MetricRegistry createRegistryForWALCoprocessor(String clazz) {
    return MetricRegistries.global().create(createRegistryInfoForWALCoprocessor(clazz));
  }

  public static void removeRegistry(MetricRegistry registry) {
    if (registry == null) {
      return;
    }
    MetricRegistries.global().remove(registry.getMetricRegistryInfo());
  }
}
