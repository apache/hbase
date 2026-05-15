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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.hadoop.hbase.testclassification.MetricsTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Tests the conditional gauge emission in {@link MetricsKeyManagementSourceImpl}. Gauges are only
 * emitted when freshly computed (dirty flag set), not on every metrics collection cycle.
 */
@Tag(MetricsTests.TAG)
@Tag(SmallTests.TAG)
public class TestMetricsKeyManagementSourceGauges {

  private MetricsKeyManagementSourceImpl source;

  @BeforeAll
  public static void classSetUp() {
    DefaultMetricsSystem.setMiniClusterMode(true);
  }

  @BeforeEach
  public void setUp() {
    source =
      new MetricsKeyManagementSourceImpl("testGauges", "testGauges", "testGauges", "testGauges");
    source.registerCacheMetrics();
  }

  @Test
  public void testGaugesNotEmittedWhenNotDirty() {
    Map<String, Number> gauges = collectGauges();
    assertFalse(gauges.containsKey("keycachesize"));
    assertFalse(gauges.containsKey("keycacheactivesize"));
    assertFalse(gauges.containsKey("keycacheusablecount"));
  }

  @Test
  public void testGaugesEmittedWhenDirty() {
    source.setKeyCacheSize(100);
    source.setKeyCacheActiveSize(50);
    source.setKeyCacheUsableCount(75);

    Map<String, Number> gauges = collectGauges();
    assertEquals(100L, gauges.get("keycachesize").longValue());
    assertEquals(50L, gauges.get("keycacheactivesize").longValue());
    assertEquals(75L, gauges.get("keycacheusablecount").longValue());
  }

  @Test
  public void testGaugesClearedAfterEmission() {
    source.setKeyCacheSize(100);
    // First collection picks up the gauges and clears the dirty flag.
    Map<String, Number> gauges = collectGauges();
    assertTrue(gauges.containsKey("keycachesize"));

    // Second collection without new set calls should not emit gauges.
    gauges = collectGauges();
    assertFalse(gauges.containsKey("keycachesize"));
  }

  @Test
  public void testCountersAlwaysEmitted() {
    source.incrementWriteKeyLookupRequests();
    source.incrementReadKeyLookupRequests();
    source.incrementProviderCallCount();

    Map<String, Long> counters = collectCounters();
    assertEquals(1L, counters.get("writekeylookup" + "requests").longValue());
    assertEquals(1L, counters.get("readkeylookup" + "requests").longValue());
    assertEquals(1L, counters.get("providercallcount").longValue());
  }

  private Map<String, Number> collectGauges() {
    GaugeCollector collector = new GaugeCollector();
    ((MetricsSource) source).getMetrics(collector, true);
    return collector.gauges;
  }

  private Map<String, Long> collectCounters() {
    GaugeCollector collector = new GaugeCollector();
    ((MetricsSource) source).getMetrics(collector, true);
    return collector.counters;
  }

  private static String canonicalize(String name) {
    return name.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9 ]", "");
  }

  /**
   * Minimal MetricsCollector that captures gauges and counters from a single getMetrics() call.
   */
  private static class GaugeCollector implements MetricsCollector {
    final Map<String, Number> gauges = new HashMap<>();
    final Map<String, Long> counters = new HashMap<>();

    @Override
    public MetricsRecordBuilder addRecord(String s) {
      return new RecordBuilder();
    }

    @Override
    public MetricsRecordBuilder addRecord(MetricsInfo metricsInfo) {
      return new RecordBuilder();
    }

    private class RecordBuilder extends MetricsRecordBuilder {
      @Override
      public MetricsRecordBuilder tag(MetricsInfo info, String value) {
        return this;
      }

      @Override
      public MetricsRecordBuilder add(MetricsTag tag) {
        return this;
      }

      @Override
      public MetricsRecordBuilder add(AbstractMetric metric) {
        gauges.put(canonicalize(metric.name()), metric.value());
        return this;
      }

      @Override
      public MetricsRecordBuilder addCounter(MetricsInfo info, int value) {
        counters.put(canonicalize(info.name()), (long) value);
        return this;
      }

      @Override
      public MetricsRecordBuilder addCounter(MetricsInfo info, long value) {
        counters.put(canonicalize(info.name()), value);
        return this;
      }

      @Override
      public MetricsRecordBuilder addGauge(MetricsInfo info, int value) {
        gauges.put(canonicalize(info.name()), (long) value);
        return this;
      }

      @Override
      public MetricsRecordBuilder addGauge(MetricsInfo info, long value) {
        gauges.put(canonicalize(info.name()), value);
        return this;
      }

      @Override
      public MetricsRecordBuilder addGauge(MetricsInfo info, float value) {
        gauges.put(canonicalize(info.name()), (double) value);
        return this;
      }

      @Override
      public MetricsRecordBuilder addGauge(MetricsInfo info, double value) {
        gauges.put(canonicalize(info.name()), value);
        return this;
      }

      @Override
      public MetricsRecordBuilder setContext(String value) {
        return this;
      }

      @Override
      public MetricsCollector parent() {
        return GaugeCollector.this;
      }

      @Override
      public MetricsCollector endRecord() {
        return GaugeCollector.this;
      }
    }
  }
}
