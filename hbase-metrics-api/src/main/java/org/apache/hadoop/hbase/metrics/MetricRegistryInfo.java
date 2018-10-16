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

package org.apache.hadoop.hbase.metrics;


import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * HBase Metrics are grouped in different MetricRegistry'ies. All metrics that correspond to a
 * subcomponent (like RPC, GC, WAL) are managed in a single MetricRegistry.
 * This class holds the name and description and JMX related context names for such group of
 * metrics.
 */
@InterfaceAudience.Private
public class MetricRegistryInfo {

  protected final String metricsName;
  protected final String metricsDescription;
  protected final String metricsContext;
  protected final String metricsJmxContext;
  protected final boolean existingSource;

  public MetricRegistryInfo(
      String metricsName,
      String metricsDescription,
      String metricsJmxContext,
      String metricsContext,
      boolean existingSource) {
    this.metricsName = metricsName;
    this.metricsDescription = metricsDescription;
    this.metricsContext = metricsContext;
    this.metricsJmxContext = metricsJmxContext;
    this.existingSource = existingSource;
  }

  /**
   * Get the metrics context.  For hadoop metrics2 system this is usually an all lowercased string.
   * eg. regionserver, master, thriftserver
   *
   * @return The string context used to register this source to hadoop's metrics2 system.
   */
  public String getMetricsContext() {
    return metricsContext;
  }

  /**
   * Get the description of what this source exposes.
   */
  public String getMetricsDescription() {
    return metricsDescription;
  }

  /**
   * Get the name of the context in JMX that this source will be exposed through.
   * This is in ObjectName format. With the default context being Hadoop -&gt; HBase
   */
  public String getMetricsJmxContext() {
    return metricsJmxContext;
  }

  /**
   * Get the name of the metrics that are being exported by this source.
   * Eg. IPC, GC, WAL
   */
  public String getMetricsName() {
    return metricsName;
  }

  /**
   * Returns whether or not this MetricRegistry is for an existing BaseSource
   * @return true if this MetricRegistry is for an existing BaseSource.
   */
  public boolean isExistingSource() {
    return existingSource;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof MetricRegistryInfo) {
      return this.hashCode() == obj.hashCode();
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(metricsName)
        .append(metricsDescription)
        .append(metricsContext)
        .append(metricsJmxContext)
        .toHashCode();
  }
}
