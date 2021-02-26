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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.FlakeyTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({FlakeyTests.class, LargeTests.class})
public class TestMasterFailover {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMasterFailover.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestMasterFailover.class);
  @Rule public TestName name = new TestName();

  /**
   * Simple test of master failover.
   * <p>
   * Starts with three masters.  Kills a backup master.  Then kills the active
   * master.  Ensures the final master becomes active and we can still contact
   * the cluster.
   */
  @Test
  public void testSimpleMasterFailover() throws Exception {
    final int NUM_MASTERS = 3;
    final int NUM_RS = 3;

    // Start the cluster
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    try {
      StartMiniClusterOption option = StartMiniClusterOption.builder()
          .numMasters(NUM_MASTERS).numRegionServers(NUM_RS).numDataNodes(NUM_RS).build();
      TEST_UTIL.startMiniCluster(option);
      MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();

      // get all the master threads
      List<MasterThread> masterThreads = cluster.getMasterThreads();

      // wait for each to come online
      for (MasterThread mt : masterThreads) {
        assertTrue(mt.isAlive());
      }

      // verify only one is the active master and we have right number
      int numActive = 0;
      int activeIndex = -1;
      ServerName activeName = null;
      HMaster active = null;
      for (int i = 0; i < masterThreads.size(); i++) {
        if (masterThreads.get(i).getMaster().isActiveMaster()) {
          numActive++;
          activeIndex = i;
          active = masterThreads.get(activeIndex).getMaster();
          activeName = active.getServerName();
        }
      }
      assertEquals(1, numActive);
      assertEquals(NUM_MASTERS, masterThreads.size());
      LOG.info("Active master " + activeName);

      // Check that ClusterStatus reports the correct active and backup masters
      assertNotNull(active);
      ClusterMetrics status = active.getClusterMetrics();
      assertEquals(activeName, status.getMasterName());
      assertEquals(2, status.getBackupMasterNames().size());

      // attempt to stop one of the inactive masters
      int backupIndex = (activeIndex == 0 ? 1 : activeIndex - 1);
      HMaster master = cluster.getMaster(backupIndex);
      LOG.debug("\n\nStopping a backup master: " + master.getServerName() + "\n");
      cluster.stopMaster(backupIndex, false);
      cluster.waitOnMaster(backupIndex);

      // Verify still one active master and it's the same
      for (int i = 0; i < masterThreads.size(); i++) {
        if (masterThreads.get(i).getMaster().isActiveMaster()) {
          assertEquals(activeName, masterThreads.get(i).getMaster().getServerName());
          activeIndex = i;
          active = masterThreads.get(activeIndex).getMaster();
        }
      }
      assertEquals(1, numActive);
      assertEquals(2, masterThreads.size());
      int rsCount = masterThreads.get(activeIndex).getMaster().getClusterMetrics()
        .getLiveServerMetrics().size();
      LOG.info("Active master " + active.getServerName() + " managing " + rsCount +
          " regions servers");
      assertEquals(3, rsCount);

      // wait for the active master to acknowledge loss of the backup from ZK
      final HMaster activeFinal = active;
      TEST_UTIL.waitFor(
        TimeUnit.SECONDS.toMillis(30), () -> activeFinal.getBackupMasters().size() == 1);

      // Check that ClusterStatus reports the correct active and backup masters
      assertNotNull(active);
      status = active.getClusterMetrics();
      assertEquals(activeName, status.getMasterName());
      assertEquals(1, status.getBackupMasterNames().size());

      // kill the active master
      LOG.debug("\n\nStopping the active master " + active.getServerName() + "\n");
      cluster.stopMaster(activeIndex, false);
      cluster.waitOnMaster(activeIndex);

      // wait for an active master to show up and be ready
      assertTrue(cluster.waitForActiveAndReadyMaster());

      LOG.debug("\n\nVerifying backup master is now active\n");
      // should only have one master now
      assertEquals(1, masterThreads.size());

      // and he should be active
      active = masterThreads.get(0).getMaster();
      assertNotNull(active);
      status = active.getClusterMetrics();
      ServerName masterName = status.getMasterName();
      assertNotNull(masterName);
      assertEquals(active.getServerName(), masterName);
      assertTrue(active.isActiveMaster());
      assertEquals(0, status.getBackupMasterNames().size());
      int rss = status.getLiveServerMetrics().size();
      LOG.info("Active master {} managing {} region servers", masterName.getServerName(), rss);
      assertEquals(3, rss);
    } finally {
      // Stop the cluster
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  /**
   * Test meta in transition when master failover.
   * This test used to manipulate region state up in zk. That is not allowed any more in hbase2
   * so I removed that messing. That makes this test anemic.
   */
  @Test
  public void testMetaInTransitionWhenMasterFailover() throws Exception {
    // Start the cluster
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    TEST_UTIL.startMiniCluster();
    try {
      MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
      LOG.info("Cluster started");

      HMaster activeMaster = cluster.getMaster();
      ServerName metaServerName = cluster.getServerHoldingMeta();
      HRegionServer hrs = cluster.getRegionServer(metaServerName);

      // Now kill master, meta should remain on rs, where we placed it before.
      LOG.info("Aborting master");
      activeMaster.abort("test-kill");
      cluster.waitForMasterToStop(activeMaster.getServerName(), 30000);
      LOG.info("Master has aborted");

      // meta should remain where it was
      RegionState metaState = MetaTableLocator.getMetaRegionState(hrs.getZooKeeper());
      assertEquals("hbase:meta should be online on RS",
          metaState.getServerName(), metaServerName);
      assertEquals("hbase:meta should be online on RS", State.OPEN, metaState.getState());

      // Start up a new master
      LOG.info("Starting up a new master");
      activeMaster = cluster.startMaster().getMaster();
      LOG.info("Waiting for master to be ready");
      cluster.waitForActiveAndReadyMaster();
      LOG.info("Master is ready");

      // ensure meta is still deployed on RS
      metaState = MetaTableLocator.getMetaRegionState(activeMaster.getZooKeeper());
      assertEquals("hbase:meta should be online on RS",
          metaState.getServerName(), metaServerName);
      assertEquals("hbase:meta should be online on RS", State.OPEN, metaState.getState());

      // Done, shutdown the cluster
    } finally {
      TEST_UTIL.shutdownMiniCluster();
    }
  }
}

