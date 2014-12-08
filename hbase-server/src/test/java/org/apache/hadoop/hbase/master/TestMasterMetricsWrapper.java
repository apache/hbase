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

import static org.junit.Assert.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestMasterMetricsWrapper {
  private static final Log LOG = LogFactory.getLog(TestMasterMetricsWrapper.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setup() throws Exception {
    TEST_UTIL.startMiniCluster(1, 4);
  }

  @AfterClass
  public static void teardown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test (timeout = 30000)
  public void testInfo() {
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    MetricsMasterWrapperImpl info = new MetricsMasterWrapperImpl(master);
    assertEquals(master.getAverageLoad(), info.getAverageLoad(), 0);
    assertEquals(master.getClusterId(), info.getClusterId());
    assertEquals(master.getMasterActiveTime(), info.getActiveTime());
    assertEquals(master.getMasterStartTime(), info.getStartTime());
    assertEquals(master.getCoprocessors().length, info.getCoprocessors().length);
    assertEquals(master.getServerManager().getOnlineServersList().size(), info.getNumRegionServers());
    assertTrue(info.getNumRegionServers() == 4);

    String zkServers = info.getZookeeperQuorum();
    assertEquals(zkServers.split(",").length, TEST_UTIL.getZkCluster().getZooKeeperServerNum());

    final int index = 3;
    LOG.info("Stopping " + TEST_UTIL.getMiniHBaseCluster().getRegionServer(index));
    TEST_UTIL.getMiniHBaseCluster().stopRegionServer(index, false);
    TEST_UTIL.getMiniHBaseCluster().waitOnRegionServer(index);
    // We stopped the regionserver but could take a while for the master to notice it so hang here
    // until it does... then move forward to see if metrics wrapper notices.
    while (TEST_UTIL.getHBaseCluster().getMaster().getServerManager().getOnlineServers().size() !=
        index) {
      Threads.sleep(10);
    }
    assertTrue(info.getNumRegionServers() == 3);
    assertTrue(info.getNumDeadRegionServers() == 1);
  }
}