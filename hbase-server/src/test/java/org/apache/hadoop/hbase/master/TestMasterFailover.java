/*
 *
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.testclassification.FlakeyTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({FlakeyTests.class, LargeTests.class})
public class TestMasterFailover {
  private static final Log LOG = LogFactory.getLog(TestMasterFailover.class);

  // TODO: Next test to add is with testing permutations of the RIT or the RS
  //       killed are hosting ROOT and hbase:meta regions.

  private void log(String string) {
    LOG.info("\n\n" + string + " \n\n");
  }

  /**
   * Simple test of master failover.
   * <p>
   * Starts with three masters.  Kills a backup master.  Then kills the active
   * master.  Ensures the final master becomes active and we can still contact
   * the cluster.
   */
  @Test (timeout=240000)
  public void testSimpleMasterFailover() throws Exception {

    final int NUM_MASTERS = 3;
    final int NUM_RS = 3;

    // Start the cluster
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

    TEST_UTIL.startMiniCluster(NUM_MASTERS, NUM_RS);
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
    ClusterStatus status = active.getClusterStatus();
    assertTrue(status.getMaster().equals(activeName));
    assertEquals(2, status.getBackupMastersSize());
    assertEquals(2, status.getBackupMasters().size());

    // attempt to stop one of the inactive masters
    int backupIndex = (activeIndex == 0 ? 1 : activeIndex - 1);
    HMaster master = cluster.getMaster(backupIndex);
    LOG.debug("\n\nStopping a backup master: " + master.getServerName() + "\n");
    cluster.stopMaster(backupIndex, false);
    cluster.waitOnMaster(backupIndex);

    // Verify still one active master and it's the same
    for (int i = 0; i < masterThreads.size(); i++) {
      if (masterThreads.get(i).getMaster().isActiveMaster()) {
        assertTrue(activeName.equals(masterThreads.get(i).getMaster().getServerName()));
        activeIndex = i;
        active = masterThreads.get(activeIndex).getMaster();
      }
    }
    assertEquals(1, numActive);
    assertEquals(2, masterThreads.size());
    int rsCount = masterThreads.get(activeIndex).getMaster().getClusterStatus().getServersSize();
    LOG.info("Active master " + active.getServerName() + " managing " + rsCount +  " regions servers");
    assertEquals(3, rsCount);

    // Check that ClusterStatus reports the correct active and backup masters
    assertNotNull(active);
    status = active.getClusterStatus();
    assertTrue(status.getMaster().equals(activeName));
    assertEquals(1, status.getBackupMastersSize());
    assertEquals(1, status.getBackupMasters().size());

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
    status = active.getClusterStatus();
    ServerName mastername = status.getMaster();
    assertTrue(mastername.equals(active.getServerName()));
    assertTrue(active.isActiveMaster());
    assertEquals(0, status.getBackupMastersSize());
    assertEquals(0, status.getBackupMasters().size());
    int rss = status.getServersSize();
    LOG.info("Active master " + mastername.getServerName() + " managing " +
      rss +  " region servers");
    assertEquals(3, rss);

    // Stop the cluster
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Test meta in transition when master failover
   */
  @Test(timeout = 180000)
  public void testMetaInTransitionWhenMasterFailover() throws Exception {
    final int NUM_MASTERS = 1;
    final int NUM_RS = 1;

    // Start the cluster
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    TEST_UTIL.startMiniCluster(NUM_MASTERS, NUM_RS);
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    log("Cluster started");

    log("Moving meta off the master");
    HMaster activeMaster = cluster.getMaster();
    HRegionServer rs = cluster.getRegionServer(0);
    ServerName metaServerName = cluster.getLiveRegionServerThreads()
      .get(0).getRegionServer().getServerName();
    activeMaster.move(HRegionInfo.FIRST_META_REGIONINFO.getEncodedNameAsBytes(),
      Bytes.toBytes(metaServerName.getServerName()));
    TEST_UTIL.waitUntilNoRegionsInTransition(60000);
    assertEquals("Meta should be assigned on expected regionserver",
      metaServerName, activeMaster.getMetaTableLocator()
        .getMetaRegionLocation(activeMaster.getZooKeeper()));

    // Now kill master, meta should remain on rs, where we placed it before.
    log("Aborting master");
    activeMaster.abort("test-kill");
    cluster.waitForMasterToStop(activeMaster.getServerName(), 30000);
    log("Master has aborted");

    // meta should remain where it was
    RegionState metaState =
      MetaTableLocator.getMetaRegionState(rs.getZooKeeper());
    assertEquals("hbase:meta should be online on RS",
      metaState.getServerName(), rs.getServerName());
    assertEquals("hbase:meta should be online on RS",
      metaState.getState(), State.OPEN);

    // Start up a new master
    log("Starting up a new master");
    activeMaster = cluster.startMaster().getMaster();
    log("Waiting for master to be ready");
    cluster.waitForActiveAndReadyMaster();
    log("Master is ready");

    // ensure meta is still deployed on RS
    metaState =
      MetaTableLocator.getMetaRegionState(activeMaster.getZooKeeper());
    assertEquals("hbase:meta should be online on RS",
      metaState.getServerName(), rs.getServerName());
    assertEquals("hbase:meta should be online on RS",
      metaState.getState(), State.OPEN);

    // Update meta state as OPENING, then kill master
    // that simulates, that RS successfully deployed, but
    // RPC was lost right before failure.
    // region server should expire (how it can be verified?)
    MetaTableLocator.setMetaLocation(activeMaster.getZooKeeper(),
      rs.getServerName(), State.OPENING);
    Region meta = rs.getRegion(HRegionInfo.FIRST_META_REGIONINFO.getEncodedName());
    rs.removeRegion(meta, null);
    ((HRegion)meta).close();

    log("Aborting master");
    activeMaster.abort("test-kill");
    cluster.waitForMasterToStop(activeMaster.getServerName(), 30000);
    log("Master has aborted");

    // Start up a new master
    log("Starting up a new master");
    activeMaster = cluster.startMaster().getMaster();
    log("Waiting for master to be ready");
    cluster.waitForActiveAndReadyMaster();
    log("Master is ready");

    TEST_UTIL.waitUntilNoRegionsInTransition(60000);
    log("Meta was assigned");

    metaState =
      MetaTableLocator.getMetaRegionState(activeMaster.getZooKeeper());
    assertEquals("hbase:meta should be online on RS",
      metaState.getServerName(), rs.getServerName());
    assertEquals("hbase:meta should be online on RS",
      metaState.getState(), State.OPEN);

    // Update meta state as CLOSING, then kill master
    // that simulates, that RS successfully deployed, but
    // RPC was lost right before failure.
    // region server should expire (how it can be verified?)
    MetaTableLocator.setMetaLocation(activeMaster.getZooKeeper(),
      rs.getServerName(), State.CLOSING);

    log("Aborting master");
    activeMaster.abort("test-kill");
    cluster.waitForMasterToStop(activeMaster.getServerName(), 30000);
    log("Master has aborted");

    rs.getRSRpcServices().closeRegion(null, ProtobufUtil.buildCloseRegionRequest(
      rs.getServerName(), HRegionInfo.FIRST_META_REGIONINFO.getRegionName()));

    // Start up a new master
    log("Starting up a new master");
    activeMaster = cluster.startMaster().getMaster();
    assertNotNull(activeMaster);
    log("Waiting for master to be ready");
    cluster.waitForActiveAndReadyMaster();
    log("Master is ready");

    TEST_UTIL.waitUntilNoRegionsInTransition(60000);
    log("Meta was assigned");

    // Done, shutdown the cluster
    TEST_UTIL.shutdownMiniCluster();
  }
}

