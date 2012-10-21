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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.master.metrics.MasterMetricsSource;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestMasterMetrics {

  private static final Log LOG = LogFactory.getLog(TestMasterMetrics.class);
  private static final MetricsAssertHelper metricsHelper = CompatibilityFactory
      .getInstance(MetricsAssertHelper.class);

  private static MiniHBaseCluster cluster;
  private static HMaster master;
  private static HBaseTestingUtility TEST_UTIL;


  @BeforeClass
  public static void startCluster() throws Exception {
    LOG.info("Starting cluster");
    TEST_UTIL = new HBaseTestingUtility();
    TEST_UTIL.startMiniCluster(1, 1);
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

  @Test(timeout = 300000)
  public void testClusterRequests() throws Exception {

    // sending fake request to master to see how metric value has changed
    RegionServerStatusProtos.RegionServerReportRequest.Builder request =
        RegionServerStatusProtos.RegionServerReportRequest.newBuilder();
    HRegionServer rs = cluster.getRegionServer(0);
    request.setServer(ProtobufUtil.toServerName(rs.getServerName()));

    HBaseProtos.ServerLoad sl = HBaseProtos.ServerLoad.newBuilder()
                                           .setTotalNumberOfRequests(10000)
                                           .build();
    master.getMetrics().getMetricsSource().init();
    request.setLoad(sl);
    master.regionServerReport(null, request.build());

    metricsHelper.assertCounter("cluster_requests", 10000,
        master.getMetrics().getMetricsSource());
    master.stopMaster();
  }

  @Test
  public void testDefaultMasterMetrics() throws Exception {
    MasterMetricsSource source = master.getMetrics().getMetricsSource();
    metricsHelper.assertGauge( "numRegionServers", 1, source);
    metricsHelper.assertGauge( "averageLoad", 2, source);
    metricsHelper.assertGauge( "numDeadRegionServers", 0, source);

    metricsHelper.assertGauge("masterStartTime", master.getMasterStartTime(), source);
    metricsHelper.assertGauge("masterActiveTime", master.getMasterActiveTime(), source);

    metricsHelper.assertTag("isActiveMaster", "true", source);
    metricsHelper.assertTag("serverName", master.getServerName().toString(), source);
    metricsHelper.assertTag("clusterId", master.getClusterId(), source);
    metricsHelper.assertTag("zookeeperQuorum", master.getZooKeeper().getQuorum(), source);

  }
}
