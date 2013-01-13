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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestMXBean {

  private static final HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();

  @BeforeClass
  public static void setup() throws Exception {
    TEST_UTIL.startMiniCluster(1, 4);
  }

  @AfterClass
  public static void teardown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private void verifyRegionServers(Map<String, HServerLoad> regions) {
    Set<String> expected = new HashSet<String>();
    for (int i = 0; i < 4; ++i) {
      HRegionServer rs = TEST_UTIL.getMiniHBaseCluster().getRegionServer(i);
      expected.add(rs.getServerName().getServerName());
    }

    int found = 0;
    for (java.util.Map.Entry<String, HServerLoad> entry : regions.entrySet()) {
      if (expected.contains(entry.getKey())) {
        ++found;
      }
    }
    Assert.assertEquals(4, found);
  }

  @Test
  public void testInfo() {
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    MXBeanImpl info = MXBeanImpl.init(master);
    Assert.assertEquals(master.getAverageLoad(), info.getAverageLoad());
    Assert.assertEquals(master.getClusterId(), info.getClusterId());
    Assert.assertEquals(master.getMasterActiveTime(),
        info.getMasterActiveTime());
    Assert.assertEquals(master.getMasterStartTime(),
        info.getMasterStartTime());
    Assert.assertEquals(master.getCoprocessors().length,
        info.getCoprocessors().length);
    Assert.assertEquals(master.getServerManager().getOnlineServersList().size(),
        info.getRegionServers().size());
    Assert.assertEquals(master.getAssignmentManager().isRegionsInTransition(),
        info.getRegionsInTransition().length > 0);
    Assert.assertTrue(info.getRegionServers().size() == 4);

    String zkServers = info.getZookeeperQuorum();
    Assert.assertEquals(zkServers.split(",").length,
        TEST_UTIL.getZkCluster().getZooKeeperServerNum());

    verifyRegionServers(info.getRegionServers());

    TEST_UTIL.getMiniHBaseCluster().stopRegionServer(3, false);
    TEST_UTIL.getMiniHBaseCluster().waitOnRegionServer(3);
    Assert.assertTrue(info.getRegionServers().size() == 3);
    Assert.assertTrue(info.getDeadRegionServers().length == 1);

  }

}
