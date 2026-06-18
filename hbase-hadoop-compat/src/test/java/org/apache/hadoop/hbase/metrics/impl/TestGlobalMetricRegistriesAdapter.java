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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.util.Collection;
import org.apache.hadoop.hbase.metrics.MetricRegistries;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.metrics.MetricRegistryInfo;
import org.apache.hadoop.hbase.testclassification.MetricsTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.impl.MetricsExportHelper;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MetricsTests.TAG)
@Tag(SmallTests.TAG)
public class TestGlobalMetricRegistriesAdapter {

  private static final String METRICS_NAME = "TestGlobalMetricRegistriesAdapter";
  private static final String COUNTER_NAME = "requests";
  private static final MetricRegistryInfo INFO =
    new MetricRegistryInfo(METRICS_NAME, "Metrics for GlobalMetricRegistriesAdapter tests",
      "RegionServer,sub=" + METRICS_NAME, "regionserver", false);

  private GlobalMetricRegistriesAdapter adapter;

  @BeforeEach
  public void setUp() {
    MetricRegistries.global().clear();
    DefaultMetricsSystem.shutdown();
    DefaultMetricsSystem.initialize("globalMetricRegistriesAdapterTest");
    DefaultMetricsSystem.instance().start();
    adapter = GlobalMetricRegistriesAdapter.init();
  }

  @AfterEach
  public void tearDown() {
    MetricRegistries.global().clear();
    if (adapter != null) {
      // First unregister sources for the cleared registries, then stop the adapter's executor.
      runAdapter();
      adapter.stop();
      runAdapter();
    }
    DefaultMetricsSystem.shutdown();
  }

  @Test
  public void testExportUsesRecreatedMetricRegistry() {
    MetricRegistry registryBefore = MetricRegistries.global().create(INFO);
    registryBefore.counter(COUNTER_NAME).increment(1);
    runAdapter();

    assertEquals(1, getExportedCounterValue());

    assertTrue(MetricRegistries.global().remove(INFO));
    MetricRegistry registryAfter = MetricRegistries.global().create(INFO);
    assertNotSame(registryBefore, registryAfter);
    registryAfter.counter(COUNTER_NAME).increment(2);
    runAdapter();

    assertEquals(2, getExportedCounterValue());
  }

  private void runAdapter() {
    try {
      // Trigger the adapter synchronously so this small test does not wait for the 10s scheduler.
      Method doRun = GlobalMetricRegistriesAdapter.class.getDeclaredMethod("doRun");
      doRun.setAccessible(true);
      doRun.invoke(adapter);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError("Failed to run GlobalMetricRegistriesAdapter", e);
    }
  }

  private long getExportedCounterValue() {
    Collection<MetricsRecord> exportedMetrics = MetricsExportHelper.export();
    for (MetricsRecord exported : exportedMetrics) {
      if (exported.name().equals(INFO.getMetricsName())) {
        for (AbstractMetric metric : exported.metrics()) {
          if (metric.name().equals("Requests")) {
            return metric.value().longValue();
          }
        }
      }
    }
    return -1;
  }
}
