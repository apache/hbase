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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos;

@Category({MasterTests.class, MediumTests.class})
public class TestMasterMetrics {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMasterMetrics.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestMasterMetrics.class);
  private static final MetricsAssertHelper metricsHelper = CompatibilityFactory
      .getInstance(MetricsAssertHelper.class);

  private static MiniHBaseCluster cluster;
  private static HMaster master;
  private static HBaseTestingUtility TEST_UTIL;

  public static class MyMaster extends HMaster {
    public MyMaster(Configuration conf) throws IOException, KeeperException, InterruptedException {
      super(conf);
    }
    @Override
    protected void tryRegionServerReport(
        long reportStartTime, long reportEndTime) {
      // do nothing
    }

  }

  @BeforeClass
  public static void startCluster() throws Exception {
    LOG.info("Starting cluster");
    TEST_UTIL = new HBaseTestingUtility();
    // Set master class and use default values for other options.
    StartMiniClusterOption option = StartMiniClusterOption.builder()
        .masterClass(MyMaster.class).build();
    TEST_UTIL.startMiniCluster(option);
    cluster = TEST_UTIL.getHBaseCluster();
    LOG.info("Waiting for active/ready master");
    cluster.waitForActiveAndReadyMaster();
    master = cluster.getMaster();
  }

  @AfterClass
  public static void after() throws Exception {
    if (TEST_UTIL != null) {
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  @Test
  public void testClusterRequests() throws Exception {

    // sending fake request to master to see how metric value has changed

    RegionServerStatusProtos.RegionServerReportRequest.Builder request =
        RegionServerStatusProtos.RegionServerReportRequest.newBuilder();
    ServerName serverName = cluster.getMaster(0).getServerName();
    request.setServer(ProtobufUtil.toServerName(serverName));
    long expectedRequestNumber = 10000;

    MetricsMasterSource masterSource = master.getMasterMetrics().getMetricsSource();
    ClusterStatusProtos.ServerLoad sl = ClusterStatusProtos.ServerLoad.newBuilder()
                                           .setTotalNumberOfRequests(expectedRequestNumber)
                                           .build();
    request.setLoad(sl);

    master.getMasterRpcServices().regionServerReport(null, request.build());
    boolean tablesOnMaster = LoadBalancer.isTablesOnMaster(TEST_UTIL.getConfiguration());
    if (tablesOnMaster) {
      metricsHelper.assertCounter("cluster_requests", expectedRequestNumber, masterSource);
    } else {
      metricsHelper.assertCounterGt("cluster_requests", expectedRequestNumber, masterSource);

    }

    expectedRequestNumber = 15000;

    sl = ClusterStatusProtos.ServerLoad.newBuilder()
        .setTotalNumberOfRequests(expectedRequestNumber)
        .build();
    request.setLoad(sl);

    master.getMasterRpcServices().regionServerReport(null, request.build());
    if (tablesOnMaster) {
      metricsHelper.assertCounter("cluster_requests", expectedRequestNumber, masterSource);
    } else {
      metricsHelper.assertCounterGt("cluster_requests", expectedRequestNumber, masterSource);
    }

    master.stopMaster();
  }

  @Test
  public void testDefaultMasterMetrics() throws Exception {
    MetricsMasterSource masterSource = master.getMasterMetrics().getMetricsSource();
    boolean tablesOnMaster = LoadBalancer.isTablesOnMaster(TEST_UTIL.getConfiguration());
    metricsHelper.assertGauge( "numRegionServers",1 + (tablesOnMaster? 1: 0), masterSource);
    metricsHelper.assertGauge( "averageLoad", 1 + (tablesOnMaster? 0: 1), masterSource);
    metricsHelper.assertGauge( "numDeadRegionServers", 0, masterSource);
    metricsHelper.assertGauge("numDrainingRegionServers", 0, masterSource);

    metricsHelper.assertGauge("masterStartTime", master.getMasterStartTime(), masterSource);
    metricsHelper.assertGauge("masterActiveTime", master.getMasterActiveTime(), masterSource);

    metricsHelper.assertTag("isActiveMaster", "true", masterSource);
    metricsHelper.assertTag("serverName", master.getServerName().toString(), masterSource);
    metricsHelper.assertTag("clusterId", master.getClusterId(), masterSource);
    metricsHelper.assertTag("zookeeperQuorum", master.getZooKeeper().getQuorum(), masterSource);

    metricsHelper.assertCounter(MetricsMasterSource.SERVER_CRASH_METRIC_PREFIX+"SubmittedCount",
      0, masterSource);
  }

  @Test
  public void testDefaultMasterProcMetrics() throws Exception {
    MetricsMasterProcSource masterSource = master.getMasterMetrics().getMetricsProcSource();
    metricsHelper.assertGauge("numMasterWALs", master.getNumWALFiles(), masterSource);
  }
}
