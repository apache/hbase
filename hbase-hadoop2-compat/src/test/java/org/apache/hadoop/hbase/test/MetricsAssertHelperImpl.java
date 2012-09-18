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

package org.apache.hadoop.hbase.test;

import org.apache.hadoop.hbase.metrics.BaseMetricsSource;
import org.apache.hadoop.hbase.metrics.BaseMetricsSourceImpl;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsTag;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 *  A helper class that will allow tests to get into hadoop2's metrics2 values.
 */
public class MetricsAssertHelperImpl implements MetricsAssertHelper {

  private Map<String, String> tags = new HashMap<String, String>();
  private Map<String, Number> gauges = new HashMap<String, Number>();
  private Map<String, Long> counters = new HashMap<String, Long>();

  public class MockMetricsBuilder implements MetricsCollector {

    @Override
    public MetricsRecordBuilder addRecord(String s) {
      return new MockRecordBuilder(this);
    }

    @Override
    public MetricsRecordBuilder addRecord(MetricsInfo metricsInfo) {
      return new MockRecordBuilder(this);
    }
  }

  public class MockRecordBuilder extends MetricsRecordBuilder {

    private final MetricsCollector mockMetricsBuilder;

    public MockRecordBuilder(MetricsCollector mockMetricsBuilder) {

      this.mockMetricsBuilder = mockMetricsBuilder;
    }

    @Override
    public MetricsRecordBuilder tag(MetricsInfo metricsInfo, String s) {

      tags.put(metricsInfo.name(), s);
      return this;
    }

    @Override
    public MetricsRecordBuilder add(MetricsTag metricsTag) {
      tags.put(canonicalizeMetricName(metricsTag.name()), metricsTag.value());
      return this;
    }

    @Override
    public MetricsRecordBuilder add(AbstractMetric abstractMetric) {
      gauges.put(canonicalizeMetricName(abstractMetric.name()), abstractMetric.value());
      return this;
    }

    @Override
    public MetricsRecordBuilder setContext(String s) {
      return this;
    }

    @Override
    public MetricsRecordBuilder addCounter(MetricsInfo metricsInfo, int i) {
      counters.put(canonicalizeMetricName(metricsInfo.name()), Long.valueOf(i));
      return this;
    }

    @Override
    public MetricsRecordBuilder addCounter(MetricsInfo metricsInfo, long l) {
      counters.put(canonicalizeMetricName(metricsInfo.name()), Long.valueOf(l));
      return this;
    }

    @Override
    public MetricsRecordBuilder addGauge(MetricsInfo metricsInfo, int i) {
      gauges.put(canonicalizeMetricName(metricsInfo.name()), Long.valueOf(i));
      return this;
    }

    @Override
    public MetricsRecordBuilder addGauge(MetricsInfo metricsInfo, long l) {
      gauges.put(canonicalizeMetricName(metricsInfo.name()), Long.valueOf(l));
      return this;
    }

    @Override
    public MetricsRecordBuilder addGauge(MetricsInfo metricsInfo, float v) {
      gauges.put(canonicalizeMetricName(metricsInfo.name()), Double.valueOf(v));
      return this;
    }

    @Override
    public MetricsRecordBuilder addGauge(MetricsInfo metricsInfo, double v) {
      gauges.put(canonicalizeMetricName(metricsInfo.name()), Double.valueOf(v));
      return this;
    }

    @Override
    public MetricsCollector parent() {
      return mockMetricsBuilder;
    }
  }

  @Override
  public void assertTag(String name, String expected, BaseMetricsSource source) {
    getMetrics(source);
    String cName = canonicalizeMetricName(name);
    assertEquals("Tags should be equal", expected, tags.get(cName));
  }

  @Override
  public void assertGauge(String name, long expected, BaseMetricsSource source) {
    getMetrics(source);
    String cName = canonicalizeMetricName(name);
    assertEquals("Metrics Should be equal", Long.valueOf(expected), gauges.get(cName));
  }

  @Override
  public void assertGaugeGt(String name, long expected, BaseMetricsSource source) {
    getMetrics(source);
    String cName = canonicalizeMetricName(name);
    assertNotNull(gauges.get(cName));
    long found = gauges.get(cName).longValue();
    assertTrue(name + " (" + found + ") should be greater than " + expected, found > expected);
  }

  @Override
  public void assertGaugeLt(String name, long expected, BaseMetricsSource source) {
    getMetrics(source);
    String cName = canonicalizeMetricName(name);
    assertNotNull(gauges.get(cName));
    long found = gauges.get(cName).longValue();
    assertTrue(name + "(" + found + ") should be less than " + expected, found < expected);
  }

  @Override
  public void assertGauge(String name, double expected, BaseMetricsSource source) {
    getMetrics(source);
    String cName = canonicalizeMetricName(name);
    assertEquals("Metrics Should be equal", Double.valueOf(expected), gauges.get(cName));
  }

  @Override
  public void assertGaugeGt(String name, double expected, BaseMetricsSource source) {
    getMetrics(source);
    String cName = canonicalizeMetricName(name);
    assertNotNull(gauges.get(cName));
    double found = gauges.get(cName).doubleValue();
    assertTrue(name + "(" + found + ") should be greater than " + expected, found > expected);
  }

  @Override
  public void assertGaugeLt(String name, double expected, BaseMetricsSource source) {
    getMetrics(source);
    String cName = canonicalizeMetricName(name);
    assertNotNull(gauges.get(cName));
    double found = gauges.get(cName).doubleValue();
    assertTrue(name + "(" + found + ") should be less than " + expected, found < expected);
  }

  @Override
  public void assertCounter(String name, long expected, BaseMetricsSource source) {
    getMetrics(source);
    String cName = canonicalizeMetricName(name);
    assertEquals("Metrics Counters should be equal", Long.valueOf(expected), counters.get(cName));
  }

  @Override
  public void assertCounterGt(String name, long expected, BaseMetricsSource source) {
    getMetrics(source);
    String cName = canonicalizeMetricName(name);
    assertNotNull(counters.get(cName));
    long found = gauges.get(cName).longValue();
    assertTrue(name + " (" + found + ") should be greater than " + expected, found > expected);
  }

  @Override
  public void assertCounterLt(String name, long expected, BaseMetricsSource source) {
    getMetrics(source);
    String cName = canonicalizeMetricName(name);
    assertNotNull(counters.get(cName));
    long found = gauges.get(cName).longValue();
    assertTrue(name + "(" + found + ") should be less than " + expected, found < expected);
  }

  private void reset() {
    tags.clear();
    gauges.clear();
    counters.clear();
  }

  private void getMetrics(BaseMetricsSource source) {
    reset();
    if (!(source instanceof BaseMetricsSourceImpl)) {
      assertTrue(false);
    }
    BaseMetricsSourceImpl impl = (BaseMetricsSourceImpl) source;

    impl.getMetrics(new MockMetricsBuilder(), true);

  }

  private String canonicalizeMetricName(String in) {
    return in.toLowerCase().replaceAll("[^A-Za-z0-9 ]", "");
  }
}
