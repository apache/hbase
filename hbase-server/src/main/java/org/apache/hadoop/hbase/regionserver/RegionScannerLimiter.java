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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Limit max count of rows filtered per scan request.
 */
@InterfaceAudience.Private
public class RegionScannerLimiter implements ConfigurationObserver {

  private static final Logger LOG = LoggerFactory.getLogger(RegionScannerLimiter.class);

  public static final String HBASE_SERVER_SCANNER_MAX_ROWS_FILTERED_PER_REQUEST_KEY =
    "hbase.server.scanner.max.rows.filtered.per.request";

  private static RegionScannerLimiter INSTANCE;

  // Max count of rows filtered per request. If zero, it means no limitation.
  private volatile long maxRowsFilteredPerRequest = 0;

  private RegionScannerLimiter(Configuration conf) {
    updateLimiterConf(conf, HBASE_SERVER_SCANNER_MAX_ROWS_FILTERED_PER_REQUEST_KEY);
  }

  private void updateLimiterConf(Configuration conf, String configKey) {
    try {
      if (conf.get(configKey) == null) {
        return;
      }

      long targetValue = conf.getLong(configKey, -1);
      if (targetValue < 0) {
        LOG.warn("Invalid parameter, should be greater than or equal to zero, target value: {}",
          targetValue);
        return;
      }
      if (maxRowsFilteredPerRequest == targetValue) {
        return;
      }

      LOG.info("Config key={}, old value={}, new value={}", configKey, maxRowsFilteredPerRequest,
        targetValue);
      this.maxRowsFilteredPerRequest = targetValue;
    } catch (Exception e) {
      LOG.error("Failed to update config key: {}", configKey, e);
    }
  }

  public long getMaxRowsFilteredPerRequest() {
    return this.maxRowsFilteredPerRequest;
  }

  public static RegionScannerLimiter get() {
    return INSTANCE;
  }

  public static synchronized RegionScannerLimiter create(Configuration conf) {
    if (INSTANCE == null) {
      INSTANCE = new RegionScannerLimiter(conf);
    }
    return INSTANCE;
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    updateLimiterConf(conf, HBASE_SERVER_SCANNER_MAX_ROWS_FILTERED_PER_REQUEST_KEY);
  }
}
