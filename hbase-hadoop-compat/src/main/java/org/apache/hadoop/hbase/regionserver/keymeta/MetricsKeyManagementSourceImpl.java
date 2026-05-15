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
package org.apache.hadoop.hbase.regionserver.keymeta;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Hadoop2 implementation of {@link MetricsKeyManagementSource}. Registers counters and histograms
 * with the Hadoop Metrics2 system for automatic JMX and sink emission. Cache gauges are managed
 * outside the registry and only emitted when freshly computed (controlled by an external timer).
 */
@InterfaceAudience.Private
public class MetricsKeyManagementSourceImpl extends BaseSourceImpl
  implements MetricsKeyManagementSource {

  // Cache counters — registered lazily via registerCacheMetrics(), null on master.
  private MutableFastCounter writeKeyLookupRequests;
  private MutableFastCounter writeKeyLookupCacheHit;
  private MutableFastCounter writeKeyLookupCacheMiss;
  private MutableFastCounter writeKeyLookupResultUsable;
  private MutableFastCounter writeKeyLookupResultDisabled;
  private MutableFastCounter readKeyLookupRequests;
  private MutableFastCounter readKeyLookupCacheHit;
  private MutableFastCounter readKeyLookupCacheMiss;
  private MutableFastCounter readKeyLookupResultUsable;
  private MutableFastCounter readKeyLookupResultDisabled;

  // Provider counters — always registered.
  private final MutableFastCounter providerCallCount;
  private final MetricHistogram providerCallTime;
  private final MutableFastCounter providerCallFailures;

  // Cache gauges — managed outside the registry so we can conditionally emit them.
  private volatile long keyCacheSize;
  private volatile long keyCacheActiveSize;
  private volatile long keyCacheUsableCount;
  private final AtomicBoolean gaugesDirty = new AtomicBoolean(false);

  public MetricsKeyManagementSourceImpl() {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
  }

  public MetricsKeyManagementSourceImpl(String metricsName, String metricsDescription,
    String metricsContext, String metricsJmxContext) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);

    // Provider counters and histogram — always registered.
    providerCallCount =
      getMetricsRegistry().newCounter(PROVIDER_CALL_COUNT, PROVIDER_CALL_COUNT_DESC, 0L);
    providerCallTime =
      getMetricsRegistry().newTimeHistogram(PROVIDER_CALL_TIME, PROVIDER_CALL_TIME_DESC);
    providerCallFailures =
      getMetricsRegistry().newCounter(PROVIDER_CALL_FAILURES, PROVIDER_CALL_FAILURES_DESC, 0L);
  }

  @Override
  public void registerCacheMetrics() {
    writeKeyLookupRequests = getMetricsRegistry().newCounter(WRITE_KEY_LOOKUP_REQUESTS,
      WRITE_KEY_LOOKUP_REQUESTS_DESC, 0L);
    writeKeyLookupCacheHit = getMetricsRegistry().newCounter(WRITE_KEY_LOOKUP_CACHE_HIT,
      WRITE_KEY_LOOKUP_CACHE_HIT_DESC, 0L);
    writeKeyLookupCacheMiss = getMetricsRegistry().newCounter(WRITE_KEY_LOOKUP_CACHE_MISS,
      WRITE_KEY_LOOKUP_CACHE_MISS_DESC, 0L);
    writeKeyLookupResultUsable = getMetricsRegistry().newCounter(WRITE_KEY_LOOKUP_RESULT_USABLE,
      WRITE_KEY_LOOKUP_RESULT_USABLE_DESC, 0L);
    writeKeyLookupResultDisabled = getMetricsRegistry().newCounter(WRITE_KEY_LOOKUP_RESULT_DISABLED,
      WRITE_KEY_LOOKUP_RESULT_DISABLED_DESC, 0L);
    readKeyLookupRequests =
      getMetricsRegistry().newCounter(READ_KEY_LOOKUP_REQUESTS, READ_KEY_LOOKUP_REQUESTS_DESC, 0L);
    readKeyLookupCacheHit = getMetricsRegistry().newCounter(READ_KEY_LOOKUP_CACHE_HIT,
      READ_KEY_LOOKUP_CACHE_HIT_DESC, 0L);
    readKeyLookupCacheMiss = getMetricsRegistry().newCounter(READ_KEY_LOOKUP_CACHE_MISS,
      READ_KEY_LOOKUP_CACHE_MISS_DESC, 0L);
    readKeyLookupResultUsable = getMetricsRegistry().newCounter(READ_KEY_LOOKUP_RESULT_USABLE,
      READ_KEY_LOOKUP_RESULT_USABLE_DESC, 0L);
    readKeyLookupResultDisabled = getMetricsRegistry().newCounter(READ_KEY_LOOKUP_RESULT_DISABLED,
      READ_KEY_LOOKUP_RESULT_DISABLED_DESC, 0L);
  }

  @Override
  public void getMetrics(MetricsCollector metricsCollector, boolean all) {
    MetricsRecordBuilder mrb = metricsCollector.addRecord(metricsRegistry.info());
    // Counters and histograms are always emitted.
    metricsRegistry.snapshot(mrb, all);
    if (metricsAdapter != null) {
      metricsAdapter.snapshotAllMetrics(registry, mrb);
    }
    // Cache gauges are only emitted when freshly computed.
    if (gaugesDirty.compareAndSet(true, false)) {
      mrb.addGauge(Interns.info(KEY_CACHE_SIZE, KEY_CACHE_SIZE_DESC), keyCacheSize);
      mrb.addGauge(Interns.info(KEY_CACHE_ACTIVE_SIZE, KEY_CACHE_ACTIVE_SIZE_DESC),
        keyCacheActiveSize);
      mrb.addGauge(Interns.info(KEY_CACHE_USABLE_COUNT, KEY_CACHE_USABLE_COUNT_DESC),
        keyCacheUsableCount);
    }
  }

  @Override
  public void incrementWriteKeyLookupRequests() {
    if (writeKeyLookupRequests != null) writeKeyLookupRequests.incr();
  }

  @Override
  public void incrementWriteKeyLookupCacheHit() {
    if (writeKeyLookupCacheHit != null) writeKeyLookupCacheHit.incr();
  }

  @Override
  public void incrementWriteKeyLookupCacheMiss() {
    if (writeKeyLookupCacheMiss != null) writeKeyLookupCacheMiss.incr();
  }

  @Override
  public void incrementWriteKeyLookupResultUsable() {
    if (writeKeyLookupResultUsable != null) writeKeyLookupResultUsable.incr();
  }

  @Override
  public void incrementWriteKeyLookupResultDisabled() {
    if (writeKeyLookupResultDisabled != null) writeKeyLookupResultDisabled.incr();
  }

  @Override
  public void incrementReadKeyLookupRequests() {
    if (readKeyLookupRequests != null) readKeyLookupRequests.incr();
  }

  @Override
  public void incrementReadKeyLookupCacheHit() {
    if (readKeyLookupCacheHit != null) readKeyLookupCacheHit.incr();
  }

  @Override
  public void incrementReadKeyLookupCacheMiss() {
    if (readKeyLookupCacheMiss != null) readKeyLookupCacheMiss.incr();
  }

  @Override
  public void incrementReadKeyLookupResultUsable() {
    if (readKeyLookupResultUsable != null) readKeyLookupResultUsable.incr();
  }

  @Override
  public void incrementReadKeyLookupResultDisabled() {
    if (readKeyLookupResultDisabled != null) readKeyLookupResultDisabled.incr();
  }

  @Override
  public void incrementProviderCallCount() {
    providerCallCount.incr();
  }

  @Override
  public void updateProviderCallTime(long timeMs) {
    providerCallTime.add(timeMs);
  }

  @Override
  public void incrementProviderCallFailures() {
    providerCallFailures.incr();
  }

  @Override
  public void setKeyCacheSize(long size) {
    keyCacheSize = size;
    gaugesDirty.set(true);
  }

  @Override
  public void setKeyCacheActiveSize(long size) {
    keyCacheActiveSize = size;
    gaugesDirty.set(true);
  }

  @Override
  public void setKeyCacheUsableCount(long count) {
    keyCacheUsableCount = count;
    gaugesDirty.set(true);
  }
}
