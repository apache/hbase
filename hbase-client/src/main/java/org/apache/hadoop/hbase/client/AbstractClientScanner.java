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
package org.apache.hadoop.hbase.client;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Helper class for custom client scanners.
 */
@InterfaceAudience.Private
public abstract class AbstractClientScanner implements ResultScanner {
  protected ScanMetrics scanMetrics = null;
  protected List<ScanMetrics> scanMetricsByRegion;

  /**
   * Check and initialize list for collecting scan metrics if application wants to collect scan
   * metrics per region
   */
  protected void initScanMetricsByRegion(Scan scan) {
    // check if application wants to collect scan metrics
    if (scan.isScanMetricsEnabled() && scan.isScanMetricsByRegionEnabled()) {
      scanMetricsByRegion = new ArrayList<>();
    }
  }

  /**
   * Check and initialize if application wants to collect scan metrics
   */
  protected void initScanMetrics(Scan scan) {
    // check if application wants to collect scan metrics
    if (scan.isScanMetricsEnabled()) {
      if (scanMetricsByRegion != null) {
        scanMetrics = new ScanMetrics();
        scanMetricsByRegion.add(scanMetrics);
      } else if (scanMetrics == null) {
        // Only initialize once
        this.scanMetrics = new ScanMetrics();
      }
    }
  }

  /**
   * Used internally accumulating metrics on scan. To enable collection of metrics on a Scanner,
   * call {@link Scan#setScanMetricsEnabled(boolean)}.
   * @return Returns the running {@link ScanMetrics} instance or null if scan metrics not enabled.
   */
  @Override
  public ScanMetrics getScanMetrics() {
    if (scanMetricsByRegion != null) {
      if (scanMetricsByRegion.isEmpty()) {
        return null;
      } else if (scanMetricsByRegion.size() == 1) {
        return scanMetricsByRegion.get(0);
      }
      ScanMetrics overallScanMetrics = new ScanMetrics();
      for (ScanMetrics otherScanMetrics : scanMetricsByRegion) {
        overallScanMetrics.combineMetrics(otherScanMetrics);
      }
      return overallScanMetrics;
    } else {
      return scanMetrics;
    }
  }

  @Override
  public List<ScanMetrics> getScanMetricsByRegion() {
    return scanMetricsByRegion;
  }
}
