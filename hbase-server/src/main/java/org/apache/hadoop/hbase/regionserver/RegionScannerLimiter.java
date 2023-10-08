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

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Limit max count of rows filtered per scan request. This Limiter applies globally to scan
 * requests, and the config key is
 * {@link RegionScannerLimiter#HBASE_SERVER_SCANNER_MAX_ROWS_FILTERED_PER_REQUEST_KEY}. When heavily
 * filtered scan requests frequently cause high load on the RegionServer, you can set the
 * {@link RegionScannerLimiter#HBASE_SERVER_SCANNER_MAX_ROWS_FILTERED_PER_REQUEST_KEY} to a larger
 * value (e.g. 100,000) to limit those scan requests. If you want to kill the scan request at the
 * same time, you can set
 * {@link RegionScannerLimiter#HBASE_SERVER_SCANNER_MAX_ROWS_FILTERED_REACHED_REQUEST_KILLED_KEY} to
 * true. If you want to disable this feature, just set the
 * {@link RegionScannerLimiter#HBASE_SERVER_SCANNER_MAX_ROWS_FILTERED_PER_REQUEST_KEY} to 0.
 */
@InterfaceAudience.Private
public class RegionScannerLimiter implements ConfigurationObserver {

  private static final Logger LOG = LoggerFactory.getLogger(RegionScannerLimiter.class);

  public static final String HBASE_SERVER_SCANNER_MAX_ROWS_FILTERED_PER_REQUEST_KEY =
    "hbase.server.scanner.max.rows.filtered.per.request";

  public static final String HBASE_SERVER_SCANNER_MAX_ROWS_FILTERED_REACHED_REQUEST_KILLED_KEY =
    "hbase.server.scanner.max.rows.filtered.reached.request.killed";

  // Max count of rows filtered per scan request. If equals zero, it means no limitation.
  // Note: No limitation by default.
  private volatile long maxRowsFilteredPerRequest = 0;
  // Killing scan request when TRUE.
  private volatile boolean requestKilled = false;

  private final ConcurrentMap<String, Boolean> scanners = new ConcurrentHashMap<>();

  public RegionScannerLimiter(Configuration conf) {
    onConfigurationChange(conf);
  }

  private <T> void updateLimiterConf(Configuration conf, String configKey, T oldValue,
    Function<String, T> applyFunc) {
    try {
      if (conf.get(configKey) == null) {
        return;
      }
      T targetValue = applyFunc.apply(configKey);
      if (targetValue != null) {
        LOG.info("Config key={}, old value={}, new value={}", configKey, oldValue, targetValue);
      }
    } catch (Exception e) {
      LOG.error("Failed to update config key: {}", configKey, e);
    }
  }

  public long getMaxRowsFilteredPerRequest() {
    return this.maxRowsFilteredPerRequest;
  }

  public boolean isFilterRowsLimitReached(String scannerName) {
    return scanners.getOrDefault(scannerName, false);
  }

  public void setFilterRowsLimitReached(String scannerName, boolean limitReached) {
    scanners.put(scannerName, limitReached);
  }

  public void removeScanner(String scannerName) {
    scanners.remove(scannerName);
  }

  public boolean killRequest() {
    return requestKilled;
  }

  @VisibleForTesting
  public ConcurrentMap<String, Boolean> getScanners() {
    return scanners;
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    updateLimiterConf(conf, HBASE_SERVER_SCANNER_MAX_ROWS_FILTERED_PER_REQUEST_KEY,
      maxRowsFilteredPerRequest, configKey -> {
        long targetValue = conf.getLong(configKey, -1);
        if (targetValue < 0) {
          LOG.warn("Invalid parameter, should be greater than or equal to zero, target value: {}",
            targetValue);
          return null;
        }
        if (maxRowsFilteredPerRequest == targetValue) {
          return null;
        }
        maxRowsFilteredPerRequest = targetValue;
        return targetValue;
      });
    updateLimiterConf(conf, HBASE_SERVER_SCANNER_MAX_ROWS_FILTERED_REACHED_REQUEST_KILLED_KEY,
      requestKilled, configKey -> {
        boolean targetValue = conf.getBoolean(configKey, false);
        if (targetValue == requestKilled) {
          return null;
        }
        requestKilled = targetValue;
        return targetValue;
      });
  }
}
