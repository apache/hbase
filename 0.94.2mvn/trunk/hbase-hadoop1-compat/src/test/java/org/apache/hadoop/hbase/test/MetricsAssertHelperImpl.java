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
import org.apache.hadoop.metrics2.Metric;
import org.apache.hadoop.metrics2.MetricsBuilder;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsTag;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 *  A helper class that will allow tests to get into hadoop1's metrics2 values.
 */
public class MetricsAssertHelperImpl implements MetricsAssertHelper {

  private Map<String, String> tags = new HashMap<String, String>();
  private Map<String, Number> gauges = new HashMap<String, Number>();
  private Map<String, Long> counters = new HashMap<String, Long>();

  public class MockMetricsBuilder implements MetricsBuilder {

    @Override
    public MetricsRecordBuilder addRecord(String s) {
      return new MockRecordBuilder();
    }
  }

  public class MockRecordBuilder extends MetricsRecordBuilder {

    @Override
    public MetricsRecordBuilder tag(String s, String s1, String s2) {
      tags.put(canonicalizeMetricName(s), s2);
      return this;
    }

    @Override
    public MetricsRecordBuilder add(MetricsTag metricsTag) {
      tags.put(canonicalizeMetricName(metricsTag.name()), metricsTag.value());
      return this;
    }

    @Override
    public MetricsRecordBuilder setContext(String s) {
      return this;
    }

    @Override
    public MetricsRecordBuilder addCounter(String s, String s1, int i) {
      counters.put(canonicalizeMetricName(s), Long.valueOf(i));
      return this;
    }

    @Override
    public MetricsRecordBuilder addCounter(String s, String s1, long l) {
      counters.put(canonicalizeMetricName(s), Long.valueOf(l));
      return this;
    }

    @Override
    public MetricsRecordBuilder addGauge(String s, String s1, int i) {
      gauges.put(canonicalizeMetricName(s), Long.valueOf(i));
      return this;
    }

    @Override
    public MetricsRecordBuilder addGauge(String s, String s1, long l) {
      gauges.put(canonicalizeMetricName(s), Long.valueOf(l));
      return this;
    }

    @Override
    public MetricsRecordBuilder addGauge(String s, String s1, float v) {
      gauges.put(canonicalizeMetricName(s), Double.valueOf(v));
      return this;
    }

    @Override
    public MetricsRecordBuilder addGauge(String s, String s1, double v) {
      gauges.put(canonicalizeMetricName(s), Double.valueOf(v));
      return this;
    }

    @Override
    public MetricsRecordBuilder add(Metric metric) {
      gauges.put(canonicalizeMetricName(metric.name()), metric.value());
      return this;
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
    long found = getGaugeLong(name, source);
    assertEquals("Metrics Should be equal", (long) Long.valueOf(expected), found);
  }

  @Override
  public void assertGaugeGt(String name, long expected, BaseMetricsSource source) {
    double found = getGaugeDouble(name, source);
    assertTrue(name + " (" + found + ") should be greater than " + expected, found > expected);
  }

  @Override
  public void assertGaugeLt(String name, long expected, BaseMetricsSource source) {
    double found = getGaugeDouble(name, source);
    assertTrue(name + "(" + found + ") should be less than " + expected, found < expected);
  }

  @Override
  public void assertGauge(String name, double expected, BaseMetricsSource source) {
    double found = getGaugeDouble(name, source);
    assertEquals("Metrics Should be equal", (double) Double.valueOf(expected), found);
  }

  @Override
  public void assertGaugeGt(String name, double expected, BaseMetricsSource source) {
    double found = getGaugeDouble(name, source);
    assertTrue(name + "(" + found + ") should be greater than " + expected, found > expected);
  }

  @Override
  public void assertGaugeLt(String name, double expected, BaseMetricsSource source) {
    double found = getGaugeDouble(name, source);
    assertTrue(name + "(" + found + ") should be less than " + expected, found < expected);
  }

  @Override
  public void assertCounter(String name, long expected, BaseMetricsSource source) {
    long found = getCounter(name, source);
    assertEquals("Metrics Counters should be equal", (long) Long.valueOf(expected), found);
  }

  @Override
  public void assertCounterGt(String name, long expected, BaseMetricsSource source) {
    long found = getCounter(name, source);
    assertTrue(name + " (" + found + ") should be greater than " + expected, found > expected);
  }

  @Override
  public void assertCounterLt(String name, long expected, BaseMetricsSource source) {
    long found = getCounter(name, source);
    assertTrue(name + "(" + found + ") should be less than " + expected, found < expected);
  }

  @Override
  public long getCounter(String name, BaseMetricsSource source) {
    getMetrics(source);
    String cName = canonicalizeMetricName(name);
    assertNotNull(counters.get(cName));
    return  counters.get(cName).longValue();
  }

  @Override
  public double getGaugeDouble(String name, BaseMetricsSource source) {
    getMetrics(source);
    String cName = canonicalizeMetricName(name);
    assertNotNull(gauges.get(cName));
    return  gauges.get(cName).doubleValue();
  }

  @Override
  public long getGaugeLong(String name, BaseMetricsSource source) {
    getMetrics(source);
    String cName = canonicalizeMetricName(name);
    assertNotNull(gauges.get(cName));
    return  gauges.get(cName).longValue();
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
