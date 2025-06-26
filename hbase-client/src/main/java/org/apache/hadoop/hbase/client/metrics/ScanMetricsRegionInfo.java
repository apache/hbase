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
package org.apache.hadoop.hbase.client.metrics;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.hbase.ServerName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * POJO for capturing region level details when region level scan metrics are enabled. <br>
 * <br>
 * Currently, encoded region name and server name (host name, ports and startcode) are captured as
 * region details. <br>
 * <br>
 * Instance of this class serves as key in the Map returned by
 * {@link ServerSideScanMetrics#collectMetricsByRegion()} or
 * {@link ServerSideScanMetrics#collectMetricsByRegion(boolean)}.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ScanMetricsRegionInfo {
  /**
   * Users should only compare against this constant by reference and should not make any
   * assumptions regarding content of the constant.
   */
  public static final ScanMetricsRegionInfo EMPTY_SCAN_METRICS_REGION_INFO =
    new ScanMetricsRegionInfo(null, null);

  private final String encodedRegionName;
  private final ServerName serverName;

  ScanMetricsRegionInfo(String encodedRegionName, ServerName serverName) {
    this.encodedRegionName = encodedRegionName;
    this.serverName = serverName;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ScanMetricsRegionInfo other)) {
      return false;
    }
    return new EqualsBuilder().append(encodedRegionName, other.encodedRegionName)
      .append(serverName, other.serverName).isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37).append(encodedRegionName).append(serverName).toHashCode();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "[encodedRegionName=" + encodedRegionName + ",serverName="
      + serverName + "]";
  }

  public String getEncodedRegionName() {
    return encodedRegionName;
  }

  public ServerName getServerName() {
    return serverName;
  }
}
