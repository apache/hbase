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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.hbase.metrics.BaseSource;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;

/**
 *  A helper class that will allow tests to get into hadoop2's metrics2 values.
 */
public class MetricsAssertHelperImpl implements MetricsAssertHelper {
  private Map<String, String> tags = new HashMap<>();
  private Map<String, Number> gauges = new HashMap<>();
  private Map<String, Long> counters = new HashMap<>();

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

      tags.put(canonicalizeMetricName(metricsInfo.name()), s);
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
  public void init() {
    // Make sure that the metrics system doesn't throw an exception when
    // registering a source with the same name
    DefaultMetricsSystem.setMiniClusterMode(true);
  }

  @Override
  public void assertTag(String name, String expected, BaseSource source) {
    getMetrics(source);
    String cName = canonicalizeMetricName(name);
    assertEquals("Tags should be equal", expected, tags.get(cName));
  }

  @Override
  public void assertGauge(String name, long expected, BaseSource source) {
    long found = getGaugeLong(name, source);
    assertEquals("Metrics Should be equal", (long) Long.valueOf(expected), found);
  }

  @Override
  public void assertGaugeGt(String name, long expected, BaseSource source) {
    double found = getGaugeDouble(name, source);
    assertTrue(name + " (" + found + ") should be greater than " + expected, found > expected);
  }

  @Override
  public void assertGaugeLt(String name, long expected, BaseSource source) {
    double found = getGaugeDouble(name, source);
    assertTrue(name + "(" + found + ") should be less than " + expected, found < expected);
  }

  @Override
  public void assertGauge(String name, double expected, BaseSource source) {
    double found = getGaugeDouble(name, source);
    assertEquals("Metrics Should be equal", (double) Double.valueOf(expected), found, 0.01);
  }

  @Override
  public void assertGaugeGt(String name, double expected, BaseSource source) {
    double found = getGaugeDouble(name, source);
    assertTrue(name + "(" + found + ") should be greater than " + expected, found > expected);
  }

  @Override
  public void assertGaugeLt(String name, double expected, BaseSource source) {
    double found = getGaugeDouble(name, source);
    assertTrue(name + "(" + found + ") should be less than " + expected, found < expected);
  }

  @Override
  public void assertCounter(String name, long expected, BaseSource source) {
    long found = getCounter(name, source);
    assertEquals(name + "(" + found + ") should be equal", (long) Long.valueOf(expected), found);
  }

  @Override
  public void assertCounterGt(String name, long expected, BaseSource source) {
    long found = getCounter(name, source);
    assertTrue(name + " (" + found + ") should be greater than " + expected, found > expected);
  }

  @Override
  public void assertCounterLt(String name, long expected, BaseSource source) {
    long found = getCounter(name, source);
    assertTrue(name + "(" + found + ") should be less than " + expected, found < expected);
  }

  @Override
  public long getCounter(String name, BaseSource source) {
    getMetrics(source);
    String cName = canonicalizeMetricName(name);
    assertNotNull("Should get counter "+cName + " but did not",counters.get(cName));
    return  counters.get(cName).longValue();
  }

  @Override
  public boolean checkCounterExists(String name, BaseSource source) {
    getMetrics(source);
    String cName = canonicalizeMetricName(name);
    return counters.get(cName) != null;
  }

  @Override
  public boolean checkGaugeExists(String name, BaseSource source) {
    getMetrics(source);
    String cName = canonicalizeMetricName(name);
    return gauges.get(cName) != null;
  }

  @Override
  public double getGaugeDouble(String name, BaseSource source) {
    getMetrics(source);
    String cName = canonicalizeMetricName(name);
    assertNotNull("Should get gauge "+cName + " but did not",gauges.get(cName));
    return  gauges.get(cName).doubleValue();
  }

  @Override
  public long getGaugeLong(String name, BaseSource source) {
    getMetrics(source);
    String cName = canonicalizeMetricName(name);
    assertNotNull("Should get gauge " + cName + " but did not", gauges.get(cName));
    return gauges.get(cName).longValue();
  }

  @Override
  public String toDebugString(BaseSource source) {
    getMetrics(source);
    StringBuilder sb = new StringBuilder();
    sb.append("Tags=").append(tags).append(", Counters=").append(counters);
    return sb.append(", Gauges=").append(gauges).toString();
  }

  private void reset() {
    tags.clear();
    gauges.clear();
    counters.clear();
  }

  private void getMetrics(BaseSource source) {
    reset();
    if (!(source instanceof MetricsSource)) {
      assertTrue("The Source passed must be a MetricsSource", false);
    }
    MetricsSource impl = (MetricsSource) source;

    impl.getMetrics(new MockMetricsBuilder(), true);

  }

  private String canonicalizeMetricName(String in) {
    return in.toLowerCase(Locale.ROOT).replaceAll("[^A-Za-z0-9 ]", "");
  }
}
