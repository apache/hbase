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
package org.apache.hadoop.hbase.metrics.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.hbase.metrics.Counter;
import org.apache.hadoop.hbase.metrics.Gauge;
import org.apache.hadoop.hbase.metrics.Meter;
import org.apache.hadoop.hbase.metrics.Metric;
import org.apache.hadoop.hbase.metrics.MetricRegistryInfo;
import org.apache.hadoop.hbase.metrics.Timer;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(SmallTests.TAG)
public class TestMetricRegistryImpl {

  private MetricRegistryInfo info;
  private MetricRegistryImpl registry;

  @BeforeEach
  public void setUp() {
    info = new MetricRegistryInfo("foo", "bar", "baz", "foobar", false);
    registry = new MetricRegistryImpl(info);
  }

  @Test
  public void testCounter() {
    Counter counter = registry.counter("mycounter");
    assertNotNull(counter);
    counter.increment(42L);
    Optional<Metric> metric = registry.get("mycounter");
    assertTrue(metric.isPresent());
    assertEquals(42L, (long) ((Counter) metric.get()).getCount());
  }

  @Test
  public void testRegisterGauge() {
    registry.register("mygauge", new Gauge<Long>() {
      @Override
      public Long getValue() {
        return 42L;
      }
    });
    Optional<Metric> metric = registry.get("mygauge");
    assertTrue(metric.isPresent());
    assertEquals(42L, (long) ((Gauge<Long>) metric.get()).getValue());
  }

  @Test
  public void testRegisterGaugeLambda() {
    // register a Gauge using lambda expression
    registry.register("gaugeLambda", () -> 42L);
    Optional<Metric> metric = registry.get("gaugeLambda");
    assertTrue(metric.isPresent());
    assertEquals(42L, (long) ((Gauge<Long>) metric.get()).getValue());
  }

  @Test
  public void testTimer() {
    Timer timer = registry.timer("mytimer");
    assertNotNull(timer);
    timer.updateNanos(100);
  }

  @Test
  public void testMeter() {
    Meter meter = registry.meter("mymeter");
    assertNotNull(meter);
    meter.mark();
  }

  @Test
  public void testRegister() {
    CounterImpl counter = new CounterImpl();
    registry.register("mycounter", counter);
    counter.increment(42L);

    Optional<Metric> metric = registry.get("mycounter");
    assertTrue(metric.isPresent());
    assertEquals(42L, (long) ((Counter) metric.get()).getCount());
  }

  @Test
  public void testDoubleRegister() {
    Gauge<Long> g1 = registry.register("mygauge", () -> 42L);
    Gauge<Long> g2 = registry.register("mygauge", () -> 52L);

    // second gauge is ignored if it exists
    assertEquals(g1, g2);

    Optional<Metric> metric = registry.get("mygauge");
    assertTrue(metric.isPresent());
    assertEquals(42L, (long) ((Gauge<Long>) metric.get()).getValue());

    Counter c1 = registry.counter("mycounter");
    Counter c2 = registry.counter("mycounter");

    assertEquals(c1, c2);
  }

  @Test
  public void testGetMetrics() {
    CounterImpl counter = new CounterImpl();
    registry.register("mycounter", counter);
    Gauge<Long> gauge = registry.register("mygauge", () -> 42L);
    Timer timer = registry.timer("mytimer");

    Map<String, Metric> metrics = registry.getMetrics();
    assertEquals(3, metrics.size());

    assertEquals(counter, metrics.get("mycounter"));
    assertEquals(gauge, metrics.get("mygauge"));
    assertEquals(timer, metrics.get("mytimer"));
  }

  @Test
  public void testRemove() {
    CounterImpl counter1 = new CounterImpl();
    CounterImpl counter2 = new CounterImpl();
    registry.register("mycounter", counter1);

    boolean removed = registry.remove("mycounter", counter2);
    Optional<Metric> metric = registry.get("mycounter");
    assertFalse(removed);
    assertTrue(metric.isPresent());
    assertEquals(metric.get(), counter1);

    removed = registry.remove("mycounter");
    metric = registry.get("mycounter");
    assertTrue(removed);
    assertFalse(metric.isPresent());
  }
}
