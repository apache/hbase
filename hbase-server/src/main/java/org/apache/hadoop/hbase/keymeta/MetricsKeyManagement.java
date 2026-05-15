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
package org.apache.hadoop.hbase.keymeta;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.keymeta.MetricsKeyManagementSource;
import org.apache.hadoop.metrics2.MetricsExecutor;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Server-side wrapper for Key Management metrics. Provides convenience methods for incrementing
 * counters at instrumentation points and manages periodic gauge computation.
 */
@InterfaceAudience.Private
public class MetricsKeyManagement {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsKeyManagement.class);

  private final MetricsKeyManagementSource source;
  private final GaugeComputeRunnable gaugeRunnable;

  public MetricsKeyManagement(Configuration conf, ManagedKeyDataCache cache) {
    this(CompatibilitySingletonFactory.getInstance(MetricsKeyManagementSource.class), cache);

    if (gaugeRunnable != null) {
      long period = conf.getLong(HConstants.CRYPTO_MANAGED_KEYS_METRICS_PERIOD_CONF_KEY,
        HConstants.CRYPTO_MANAGED_KEYS_METRICS_PERIOD_DEFAULT);
      ScheduledExecutorService executor =
        CompatibilitySingletonFactory.getInstance(MetricsExecutor.class).getExecutor();
      executor.scheduleWithFixedDelay(gaugeRunnable, period, period, TimeUnit.MILLISECONDS);
    }
  }

  MetricsKeyManagement(MetricsKeyManagementSource source) {
    this(source, null);
  }

  private MetricsKeyManagement(MetricsKeyManagementSource source, ManagedKeyDataCache cache) {
    this.source = source;
    if (cache != null) {
      source.registerCacheMetrics();
      this.gaugeRunnable = new GaugeComputeRunnable(cache);
    } else {
      this.gaugeRunnable = null;
    }
  }

  public static boolean isEnabled(Configuration conf) {
    return conf.getBoolean(HConstants.CRYPTO_MANAGED_KEYS_METRICS_ENABLED_CONF_KEY,
      HConstants.CRYPTO_MANAGED_KEYS_METRICS_ENABLED_DEFAULT);
  }

  // Write-path methods
  public void writeKeyLookupRequest() {
    source.incrementWriteKeyLookupRequests();
  }

  public void writeKeyLookupCacheHit() {
    source.incrementWriteKeyLookupCacheHit();
  }

  public void writeKeyLookupCacheMiss() {
    source.incrementWriteKeyLookupCacheMiss();
  }

  public void writeKeyLookupResultUsable() {
    source.incrementWriteKeyLookupResultUsable();
  }

  public void writeKeyLookupResultDisabled() {
    source.incrementWriteKeyLookupResultDisabled();
  }

  // Read-path methods
  public void readKeyLookupRequest() {
    source.incrementReadKeyLookupRequests();
  }

  public void readKeyLookupCacheHit() {
    source.incrementReadKeyLookupCacheHit();
  }

  public void readKeyLookupCacheMiss() {
    source.incrementReadKeyLookupCacheMiss();
  }

  public void readKeyLookupResultUsable() {
    source.incrementReadKeyLookupResultUsable();
  }

  public void readKeyLookupResultDisabled() {
    source.incrementReadKeyLookupResultDisabled();
  }

  // Provider methods
  public void providerCallCompleted(long timeMs) {
    source.incrementProviderCallCount();
    source.updateProviderCallTime(timeMs);
  }

  public void providerCallFailed(long timeMs) {
    source.incrementProviderCallCount();
    source.updateProviderCallTime(timeMs);
    source.incrementProviderCallFailures();
  }

  /** Manually triggers gauge computation. Visible for testing. */
  void computeGauges() {
    if (gaugeRunnable != null) {
      gaugeRunnable.run();
    }
  }

  private class GaugeComputeRunnable implements Runnable {
    private final ManagedKeyDataCache cache;

    GaugeComputeRunnable(ManagedKeyDataCache cache) {
      this.cache = cache;
    }

    @Override
    public void run() {
      try {
        source.setKeyCacheSize(cache.getGenericCacheEntryCount());
        source.setKeyCacheActiveSize(cache.getActiveCacheEntryCount());
        source.setKeyCacheUsableCount(cache.getUsableEntryCount());
      } catch (Exception e) {
        LOG.warn("Failed to compute key management cache gauge metrics", e);
      }
    }
  }
}
