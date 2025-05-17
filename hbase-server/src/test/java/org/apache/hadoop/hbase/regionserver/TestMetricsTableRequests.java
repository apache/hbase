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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.metrics.Metric;
import org.apache.hadoop.hbase.metrics.MetricRegistries;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.metrics.MetricRegistryInfo;
import org.apache.hadoop.hbase.metrics.Snapshot;
import org.apache.hadoop.hbase.metrics.impl.DropwizardMeter;
import org.apache.hadoop.hbase.metrics.impl.HistogramImpl;
import org.apache.hadoop.hbase.regionserver.metrics.MetricsTableRequests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestMetricsTableRequests {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMetricsTableRequests.class);

  @Test
  public void testMetricsTableLatencies() {
    TableName tn1 = TableName.valueOf("table1");
    TableName tn2 = TableName.valueOf("table2");
    MetricsTableRequests requests1 = new MetricsTableRequests(tn1, new Configuration());
    MetricsTableRequests requests2 = new MetricsTableRequests(tn2, new Configuration());
    assertTrue("'requests' is actually " + requests1.getClass(),
      requests1 instanceof MetricsTableRequests);
    assertTrue("'requests' is actually " + requests2.getClass(),
      requests2 instanceof MetricsTableRequests);

    MetricRegistryInfo info1 = requests1.getMetricRegistryInfo();
    MetricRegistryInfo info2 = requests2.getMetricRegistryInfo();
    Optional<MetricRegistry> registry1 = MetricRegistries.global().get(info1);
    assertTrue(registry1.isPresent());
    Optional<MetricRegistry> registry2 = MetricRegistries.global().get(info2);
    assertTrue(registry2.isPresent());

    requests1.updateGet(500L, 5000L);
    Snapshot latencies1SnapshotGet =
      ((HistogramImpl) registry1.get().get("getTime").get()).snapshot();
    assertEquals(500, latencies1SnapshotGet.get999thPercentile());
    Snapshot blockBytesScanned1SnapshotGet =
      ((HistogramImpl) registry1.get().get("getBlockBytesScanned").get()).snapshot();
    assertEquals(5000, blockBytesScanned1SnapshotGet.get999thPercentile());

    requests1.updatePut(50L);
    Snapshot latencies1SnapshotPut =
      ((HistogramImpl) registry1.get().get("putTime").get()).snapshot();
    assertEquals(50, latencies1SnapshotPut.get99thPercentile());

    requests2.updateGet(300L, 3000L);
    Snapshot latencies2SnapshotGet =
      ((HistogramImpl) registry2.get().get("getTime").get()).snapshot();
    assertEquals(300, latencies2SnapshotGet.get999thPercentile());
    Snapshot blockBytesScanned2SnapshotGet =
      ((HistogramImpl) registry2.get().get("getBlockBytesScanned").get()).snapshot();
    assertEquals(3000, blockBytesScanned2SnapshotGet.get999thPercentile());

    requests2.updatePut(75L);
    Snapshot latencies2SnapshotPut =
      ((HistogramImpl) registry2.get().get("putTime").get()).snapshot();
    assertEquals(75, latencies2SnapshotPut.get99thPercentile());
  }

  @Test
  public void testTableQueryMeterSwitch() {
    TableName tn1 = TableName.valueOf("table1");
    Configuration conf = new Configuration();
    boolean enableTableQueryMeter =
      conf.getBoolean(MetricsTableRequests.ENABLE_TABLE_QUERY_METER_METRICS_KEY,
        MetricsTableRequests.ENABLE_TABLE_QUERY_METER_METRICS_KEY_DEFAULT);
    // disable
    assertFalse(enableTableQueryMeter);
    MetricsTableRequests requests = new MetricsTableRequests(tn1, conf);
    assertTrue("'requests' is actually " + requests.getClass(),
      requests instanceof MetricsTableRequests);

    MetricRegistryInfo info = requests.getMetricRegistryInfo();
    Optional<MetricRegistry> registry = MetricRegistries.global().get(info);
    assertTrue(registry.isPresent());
    requests.updateTableReadQueryMeter(500L);
    Optional<Metric> read = registry.get().get("tableReadQueryPerSecond");
    assertFalse(read.isPresent());

    // enable
    conf.setBoolean(MetricsTableRequests.ENABLE_TABLE_QUERY_METER_METRICS_KEY, true);
    enableTableQueryMeter =
      conf.getBoolean(MetricsTableRequests.ENABLE_TABLE_QUERY_METER_METRICS_KEY,
        MetricsTableRequests.ENABLE_TABLE_QUERY_METER_METRICS_KEY_DEFAULT);
    assertTrue(enableTableQueryMeter);
    requests = new MetricsTableRequests(tn1, conf);
    assertTrue("'requests' is actually " + requests.getClass(),
      requests instanceof MetricsTableRequests);

    info = requests.getMetricRegistryInfo();
    registry = MetricRegistries.global().get(info);
    assertTrue(registry.isPresent());
    requests.updateTableReadQueryMeter(500L);
    read = registry.get().get("tableReadQueryPerSecond");
    assertTrue(read.isPresent());
    assertEquals(((DropwizardMeter) read.get()).getCount(), 500);
  }

  /**
   * When a new MetricRegistry object is created, it needs to be re-registered (HBASE-29322)
   */
  @Test
  public void testMetricRegistryReregistered() {
    TableName tn = TableName.valueOf("test_table");
    // Create a new MetricRegistry for the first time
    MetricsTableRequests requests = new MetricsTableRequests(tn, new Configuration());
    MetricRegistryInfo info = requests.getMetricRegistryInfo();
    MetricRegistry oldRegistry = requests.getMetricRegistry();
    // Register the MetricRegistry with the DefaultMetricsSystem
    MetricsSourceAdapter adapter = new MetricsSourceAdapter(oldRegistry);
    DefaultMetricsSystem.instance().register(info.getMetricsJmxContext(),
      info.getMetricsDescription(), adapter);

    Optional<MetricRegistry> registry1 = MetricRegistries.global().get(info);
    assertTrue(registry1.isPresent());
    assertEquals(oldRegistry, registry1.get());
    // remove old MetricRegistry
    requests.removeRegistry();

    // Recreate a new MetricRegistry
    requests = new MetricsTableRequests(tn, new Configuration());
    MetricRegistry newRegistry = requests.getMetricRegistry();
    Optional<MetricRegistry> registry2 = MetricRegistries.global().get(info);
    assertTrue(registry2.isPresent());
    assertEquals(newRegistry, registry2.get());

    // The old MetricRegistry is still registered in the DefaultMetricsSystem
    MetricsSourceAdapter metricsSource =
      (MetricsSourceAdapter) DefaultMetricsSystem.instance().getSource(info.getMetricsJmxContext());
    assertEquals(oldRegistry, metricsSource.registry);
    assertNotEquals(newRegistry, metricsSource.registry);

    // Re-register the new MetricRegistry in DefaultMetricsSystem
    DefaultMetricsSystem.instance().unregisterSource(info.getMetricsJmxContext());
    MetricsSourceAdapter newAdapter = new MetricsSourceAdapter(newRegistry);
    DefaultMetricsSystem.instance().register(info.getMetricsJmxContext(),
      info.getMetricsDescription(), newAdapter);

    // The old MetricRegistry is replaced
    MetricsSourceAdapter newMetricsSource =
      (MetricsSourceAdapter) DefaultMetricsSystem.instance().getSource(info.getMetricsJmxContext());
    assertNotEquals(oldRegistry, newMetricsSource.registry);
    assertEquals(newRegistry, newMetricsSource.registry);
  }

  private static class MetricsSourceAdapter implements MetricsSource {
    private final MetricRegistry registry;

    MetricsSourceAdapter(MetricRegistry registry) {
      this.registry = registry;
    }

    @Override
    public void getMetrics(MetricsCollector collector, boolean all) {

    }
  }
}
