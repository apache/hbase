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
package org.apache.hadoop.hbase.regionserver.metrics;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.metrics.MetricRegistries;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.metrics.MetricRegistryInfo;
import org.apache.hadoop.hbase.metrics.Snapshot;
import org.apache.hadoop.hbase.metrics.impl.HistogramImpl;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.impl.MetricsExportHelper;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@Tag(MediumTests.TAG)
@Tag(RegionServerTests.TAG)
public class TestMetricsTableRequestsRegister {

  private static HBaseTestingUtil UTIL;
  private static Admin admin;
  private static final String CF = "cf";
  private static final int PUT_COUNT = 10;
  private TableName tableName;
  private MetricRegistryInfo info;

  @BeforeAll
  public static void beforeAll() throws Exception {
    UTIL = new HBaseTestingUtil();
    UTIL.startMiniCluster(1);
    admin = UTIL.getAdmin();
  }

  @BeforeEach
  public void setUp(TestInfo testInfo) throws Exception {
    tableName = TableName.valueOf(testInfo.getTestMethod().get().getName());
    info = buildRegistryInfo(tableName);

    TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF));
    admin.createTable(tdb.build());
    UTIL.waitTableAvailable(tableName);
  }

  @AfterAll
  public static void afterAll() throws Exception {
    if (UTIL != null) {
      UTIL.shutdownMiniCluster();
    }
  }

  @Test
  public void testMetricsTableRequestsReRegisterAfterAlter() throws Exception {
    // Wait for the metrics to be registered
    await().atMost(15, TimeUnit.SECONDS).until(() -> getRegisteredMetrics(info) != null);
    assertTrue(isMetricsExported(info));

    generatePutMetrics(PUT_COUNT);

    Optional<MetricRegistry> metricRegistryBefore = MetricRegistries.global().get(info);
    assertTrue(metricRegistryBefore.isPresent());
    assertEquals(PUT_COUNT, getMetricsSnapshot(metricRegistryBefore.get(), "putTime").getCount());
    assertEquals(PUT_COUNT, getMetricsExportedCount(info, "PutTime_num_ops"));

    // Alter table to force region reopen
    ColumnFamilyDescriptorBuilder cfd =
      ColumnFamilyDescriptorBuilder.newBuilder(CF.getBytes()).setMaxVersions(2);
    admin.modifyColumnFamily(tableName, cfd.build());
    UTIL.waitTableAvailable(tableName);

    generatePutMetrics(PUT_COUNT * 2);

    await().atMost(15, TimeUnit.SECONDS)
      .until(() -> !metricRegistryBefore.get().equals(MetricRegistries.global().get(info).get()));

    Optional<MetricRegistry> metricRegistryAfter = MetricRegistries.global().get(info);
    assertTrue(metricRegistryAfter.isPresent());
    // Check that the registry has been updated
    assertNotEquals(metricRegistryBefore, metricRegistryAfter);
    assertEquals(PUT_COUNT * 2,
      getMetricsSnapshot(metricRegistryAfter.get(), "putTime").getCount());

    // Check that the exported metrics have been updated
    await().atMost(15, TimeUnit.SECONDS)
      .until(() -> getMetricsExportedCount(info, "PutTime_num_ops") == PUT_COUNT * 2);
  }

  @Test
  public void testMetricsTableRequestsUnRegisterAfterDrop() throws Exception {
    // Wait for the metrics to be registered
    await().atMost(15, TimeUnit.SECONDS).until(() -> getRegisteredMetrics(info) != null);
    assertTrue(isMetricsExported(info));

    generatePutMetrics(PUT_COUNT);

    Optional<MetricRegistry> metricRegistryBefore = MetricRegistries.global().get(info);
    assertTrue(metricRegistryBefore.isPresent());
    assertEquals(PUT_COUNT, getMetricsSnapshot(metricRegistryBefore.get(), "putTime").getCount());
    assertEquals(PUT_COUNT, getMetricsExportedCount(info, "PutTime_num_ops"));

    // Disable and delete the table to trigger the metrics un-registration
    admin.disableTable(tableName);
    admin.deleteTable(tableName);

    Optional<MetricRegistry> metricRegistryAfter = MetricRegistries.global().get(info);
    assertFalse(metricRegistryAfter.isPresent());

    // Check that the exported metrics have been removed
    await().atMost(15, TimeUnit.SECONDS)
      .until(() -> getRegisteredMetrics(info) == null && !isMetricsExported(info));
  }

  private void generatePutMetrics(int count) throws Exception {
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      UTIL.loadNumericRows(table, CF.getBytes(), 0, count);
    }
  }

  private MetricsSource getRegisteredMetrics(MetricRegistryInfo metricRegistryInfo) {
    return DefaultMetricsSystem.instance().getSource(metricRegistryInfo.getMetricsJmxContext());
  }

  private boolean isMetricsExported(MetricRegistryInfo metricRegistryInfo) {
    return MetricsExportHelper.export().stream()
      .anyMatch(exported -> exported.name().equals(metricRegistryInfo.getMetricsName()));
  }

  private Snapshot getMetricsSnapshot(MetricRegistry metricRegistry, String metricName) {
    return ((HistogramImpl) metricRegistry.get(metricName).get()).snapshot();
  }

  private int getMetricsExportedCount(MetricRegistryInfo metricRegistryInfo, String metricsName) {
    Collection<MetricsRecord> exportedMetrics = MetricsExportHelper.export();
    for (MetricsRecord exported : exportedMetrics) {
      if (exported.name().equals(metricRegistryInfo.getMetricsName())) {
        for (AbstractMetric metric : exported.metrics()) {
          if (metric.name().equals(metricsName)) {
            return metric.value().intValue();
          }
        }
      }
    }
    return -1;
  }

  private MetricRegistryInfo buildRegistryInfo(TableName tableName) {
    String metricsName = "TableRequests_Namespace_" + tableName.getNamespaceAsString() + "_table_"
      + tableName.getQualifierAsString();
    String metricsJmx = "RegionServer,sub=" + metricsName;
    return new MetricRegistryInfo(metricsName,
      "Metrics about Tables on a single HBase RegionServer", metricsJmx, "regionserver", false);
  }
}
