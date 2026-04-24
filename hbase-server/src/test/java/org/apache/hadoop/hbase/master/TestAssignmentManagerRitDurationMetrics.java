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
package org.apache.hadoop.hbase.master;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@Tag(MasterTests.TAG)
@Tag(MediumTests.TAG)
public class TestAssignmentManagerRitDurationMetrics {

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final byte[] FAMILY = Bytes.toBytes("family");
  private static final long WAIT_TIMEOUT_MS = 10_000L;

  private static HMaster MASTER;
  private static final String RIT_DURATION_NUM_OPS_METRIC = "RitDuration_num_ops";
  private static final String RIT_DURATION_MAX_METRIC = "RitDuration_max";

  @BeforeAll
  public static void startCluster() throws Exception {
    TEST_UTIL.startMiniCluster(2);
    MASTER = TEST_UTIL.getMiniHBaseCluster().getMaster();
  }

  @AfterAll
  public static void after() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testRitDurationHistogramMetric(TestInfo testInfo) throws Exception {
    TableName tableName = TableName.valueOf(testInfo.getTestMethod().orElseThrow().getName());
    try (Table table = TEST_UTIL.createTable(tableName, FAMILY)) {
      RegionInfo regionInfo =
        MASTER.getAssignmentManager().getRegionStates().getRegionsOfTable(tableName).get(0);
      TEST_UTIL.waitFor(WAIT_TIMEOUT_MS,
        () -> !MASTER.getAssignmentManager().isRegionInTransition(regionInfo)
          && MASTER.getAssignmentManager().getRegionStates().getRegionServerOfRegion(regionInfo)
              != null);

      MetricsAssignmentManagerSource amSource =
        MASTER.getAssignmentManager().getAssignmentManagerMetrics().getMetricsProcSource();
      long ritDurationNumOps =
        getMetricValue(snapshotMetrics(amSource), RIT_DURATION_NUM_OPS_METRIC);

      ServerName current =
        MASTER.getAssignmentManager().getRegionStates().getRegionServerOfRegion(regionInfo);
      ServerName target = MASTER.getServerManager().getOnlineServersList().stream()
        .filter(sn -> !sn.equals(current)).findFirst()
        .orElseThrow(() -> new IllegalStateException("Need at least two regionservers"));

      TEST_UTIL.getAdmin().move(regionInfo.getEncodedNameAsBytes(), target);
      TEST_UTIL.waitFor(WAIT_TIMEOUT_MS,
        () -> target.equals(
          MASTER.getAssignmentManager().getRegionStates().getRegionServerOfRegion(regionInfo))
          && !MASTER.getAssignmentManager().isRegionInTransition(regionInfo));

      MetricsRecord metricsRecord = snapshotMetrics(amSource);
      assertEquals(ritDurationNumOps + 1,
        getMetricValue(metricsRecord, RIT_DURATION_NUM_OPS_METRIC));
      assertTrue(getMetricValue(metricsRecord, RIT_DURATION_MAX_METRIC) > 0,
        "ritDuration histogram should export a positive max value");
    }
  }

  private MetricsRecord snapshotMetrics(MetricsAssignmentManagerSource amSource) {
    MetricsCollectorImpl collector = new MetricsCollectorImpl();
    assertInstanceOf(MetricsSource.class, amSource,
      "MetricsAssignmentManagerSource should also implement MetricsSource");
    ((MetricsSource) amSource).getMetrics(collector, true);
    assertEquals(1, collector.getRecords().size());
    return collector.getRecords().get(0);
  }

  private long getMetricValue(MetricsRecord record, String metricName) {
    for (AbstractMetric metric : record.metrics()) {
      if (metricName.equals(metric.name())) {
        return metric.value().longValue();
      }
    }
    throw new AssertionError("Metric not found: " + metricName);
  }
}
