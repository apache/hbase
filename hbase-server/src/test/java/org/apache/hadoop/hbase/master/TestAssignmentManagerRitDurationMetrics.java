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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.TableDescriptorChecker;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(MasterTests.TAG)
@Tag(MediumTests.TAG)
public class TestAssignmentManagerRitDurationMetrics {

  private static final Logger LOG =
    LoggerFactory.getLogger(TestAssignmentManagerRitDurationMetrics.class);

  private static final MetricsAssertHelper METRICS_HELPER =
    CompatibilityFactory.getInstance(MetricsAssertHelper.class);

  private static SingleProcessHBaseCluster CLUSTER;
  private static HMaster MASTER;
  private static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final int MSG_INTERVAL = 1000;

  private String methodName;

  @BeforeAll
  public static void startCluster() throws Exception {
    LOG.info("Starting cluster");
    Configuration conf = TEST_UTIL.getConfiguration();

    // Enable sanity check for coprocessor, so that region reopen fails on the RS
    conf.setBoolean(TableDescriptorChecker.TABLE_SANITY_CHECKS, true);
    // set RIT stuck warning threshold to a small value
    conf.setInt(HConstants.METRICS_RIT_STUCK_WARNING_THRESHOLD, 20);
    // set msgInterval to 1 second
    conf.setInt("hbase.regionserver.msginterval", MSG_INTERVAL);
    // set tablesOnMaster to none
    conf.set("hbase.balancer.tablesOnMaster", "none");
    // set client sync wait timeout to 5sec
    conf.setInt("hbase.client.sync.wait.timeout.msec", 5000);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    conf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 2500);
    // set a small interval for updating rit metrics
    conf.setInt(AssignmentManager.RIT_CHORE_INTERVAL_MSEC_CONF_KEY, MSG_INTERVAL);
    // set a small assign attempts for avoiding assert when retrying. (HBASE-20533)
    conf.setInt(AssignmentManager.ASSIGN_MAX_ATTEMPTS, 3);
    // keep rs online so it can report the failed opens.
    conf.setBoolean(CoprocessorHost.ABORT_ON_ERROR_KEY, false);

    TEST_UTIL.startMiniCluster(2);
    CLUSTER = TEST_UTIL.getHBaseCluster();
    MASTER = CLUSTER.getMaster();
    // Disable sanity check for coprocessor, so that modify table runs on the HMaster
    MASTER.getConfiguration().setBoolean(TableDescriptorChecker.TABLE_SANITY_CHECKS, false);
  }

  @AfterAll
  public static void after() throws Exception {
    LOG.info("AFTER {} <= IS THIS NULL?", TEST_UTIL);
    TEST_UTIL.shutdownMiniCluster();
  }

  @BeforeEach
  public void setUp(TestInfo testInfo) throws Exception {
    methodName = testInfo.getTestMethod().get().getName();
  }

  @Test
  public void testRitDurationHistogramMetric() throws Exception {
    final TableName tableName = TableName.valueOf(methodName);
    final byte[] family = Bytes.toBytes("family");
    try (Table table = TEST_UTIL.createTable(tableName, family)) {
      final byte[] row = Bytes.toBytes("row");
      final byte[] qualifier = Bytes.toBytes("qualifier");
      final byte[] value = Bytes.toBytes("value");

      Put put = new Put(row);
      put.addColumn(family, qualifier, value);
      table.put(put);
      Thread.sleep(MSG_INTERVAL * 2);

      MetricsAssignmentManagerSource amSource =
        MASTER.getAssignmentManager().getAssignmentManagerMetrics().getMetricsProcSource();
      long ritDurationNumOps = getRitCountFromRegionStates(amSource);

      RegionInfo regionInfo = MASTER.getAssignmentManager().getRegionStates()
        .getRegionsOfTable(tableName).iterator().next();
      ServerName current =
        MASTER.getAssignmentManager().getRegionStates().getRegionServerOfRegion(regionInfo);
      ServerName target = MASTER.getServerManager().getOnlineServersList().stream()
        .filter(sn -> !sn.equals(current)).findFirst()
        .orElseThrow(() -> new IllegalStateException("Need at least two regionservers"));

      TEST_UTIL.getAdmin().move(regionInfo.getEncodedNameAsBytes(), target);
      TEST_UTIL.waitFor(10_000, () -> getRitCountFromRegionStates(amSource) > ritDurationNumOps);
      TEST_UTIL.waitUntilNoRegionTransitScheduled();
      Thread.sleep(MSG_INTERVAL * 5);
      assertEquals(ritDurationNumOps + 1, getRitCountFromRegionStates(amSource));
    }
  }

  private long getRitCountFromRegionStates(MetricsAssignmentManagerSource amSource) {
    return METRICS_HELPER.getCounter("ritDurationNumOps", amSource);
  }
}
