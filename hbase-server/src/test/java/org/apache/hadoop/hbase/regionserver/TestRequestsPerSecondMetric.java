/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;


import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Validate requestsPerSecond metric.
 */
@Category({ RegionServerTests.class, MediumTests.class })
public class TestRequestsPerSecondMetric {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRequestsPerSecondMetric.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final long METRICS_PERIOD = 2000L;
  private static Configuration conf;


  @BeforeClass
  public static void setup() throws Exception {
    conf = UTIL.getConfiguration();
    conf.setLong(HConstants.REGIONSERVER_METRICS_PERIOD, METRICS_PERIOD);
    UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void teardown() throws Exception {
    UTIL.shutdownMiniCluster();
  }


  @Test
  /**
   * This test will confirm no negative value in requestsPerSecond metric during any region
   * transition(close region/remove region/move region).
   * Firstly, load 2000 random rows for 25 regions and will trigger a metric.
   * Now, metricCache will have a current read and write requests count.
   * Next, we disable a table and all of its 25 regions will be closed.
   * As part of region close, his metric will also be removed from metricCache.
   * prior to HBASE-23237, we do not remove/reset his metric so we incorrectly compute
   * (currentRequestCount - lastRequestCount) which result into negative value.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  public void testNoNegativeSignAtRequestsPerSecond() throws IOException, InterruptedException {
    final TableName TABLENAME = TableName.valueOf("t");
    final String FAMILY = "f";
    Admin admin = UTIL.getAdmin();
    UTIL.createMultiRegionTable(TABLENAME, FAMILY.getBytes(),25);
    Table table = admin.getConnection().getTable(TABLENAME);
    ServerName serverName = admin.getRegionServers().iterator().next();
    HRegionServer regionServer = UTIL.getMiniHBaseCluster().getRegionServer(serverName);
    MetricsRegionServerWrapperImpl metricsWrapper  =
        new MetricsRegionServerWrapperImpl(regionServer);
    MetricsRegionServerWrapperImpl.RegionServerMetricsWrapperRunnable metricsServer
        = metricsWrapper.new RegionServerMetricsWrapperRunnable();
    metricsServer.run();
    UTIL.loadRandomRows(table, FAMILY.getBytes(), 1, 2000);
    Thread.sleep(METRICS_PERIOD);
    metricsServer.run();
    admin.disableTable(TABLENAME);
    Thread.sleep(METRICS_PERIOD);
    metricsServer.run();
    Assert.assertTrue(metricsWrapper.getRequestsPerSecond() > -1);
  }
}