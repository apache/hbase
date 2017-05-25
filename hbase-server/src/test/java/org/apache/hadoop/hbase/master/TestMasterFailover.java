/**
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

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.testclassification.FlakeyTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({FlakeyTests.class, LargeTests.class})
public class TestMasterFailover {
  private static final Log LOG = LogFactory.getLog(TestMasterFailover.class);

  HRegion createRegion(final HRegionInfo  hri, final Path rootdir, final Configuration c,
      final HTableDescriptor htd)
  throws IOException {
    HRegion r = HBaseTestingUtility.createRegionAndWAL(hri, rootdir, c, htd);
    // The above call to create a region will create an wal file.  Each
    // log file create will also create a running thread to do syncing.  We need
    // to close out this log else we will have a running thread trying to sync
    // the file system continuously which is ugly when dfs is taken away at the
    // end of the test.
    HBaseTestingUtility.closeRegionAndWAL(r);
    return r;
  }

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
   * @throws Exception
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
    assertEquals(4, rsCount);

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
    assertEquals(4, rss);

    // Stop the cluster
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Test region in pending_open/close when master failover
   */
  @Test (timeout=180000)
  public void testPendingOpenOrCloseWhenMasterFailover() throws Exception {
    final int NUM_MASTERS = 1;
    final int NUM_RS = 1;

    // Create config to use for this cluster
    Configuration conf = HBaseConfiguration.create();

    // Start the cluster
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(conf);
    TEST_UTIL.startMiniCluster(NUM_MASTERS, NUM_RS);
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    log("Cluster started");

    // get all the master threads
    List<MasterThread> masterThreads = cluster.getMasterThreads();
    assertEquals(1, masterThreads.size());

    // only one master thread, let's wait for it to be initialized
    assertTrue(cluster.waitForActiveAndReadyMaster());
    HMaster master = masterThreads.get(0).getMaster();
    assertTrue(master.isActiveMaster());
    assertTrue(master.isInitialized());

    // Create a table with a region online
    Table onlineTable = TEST_UTIL.createTable(TableName.valueOf("onlineTable"), "family");
    onlineTable.close();
    // Create a table in META, so it has a region offline
    HTableDescriptor offlineTable = new HTableDescriptor(
      TableName.valueOf(Bytes.toBytes("offlineTable")));
    offlineTable.addFamily(new HColumnDescriptor(Bytes.toBytes("family")));

    FileSystem filesystem = FileSystem.get(conf);
    Path rootdir = FSUtils.getRootDir(conf);
    FSTableDescriptors fstd = new FSTableDescriptors(conf, filesystem, rootdir);
    fstd.createTableDescriptor(offlineTable);

    HRegionInfo hriOffline = new HRegionInfo(offlineTable.getTableName(), null, null);
    createRegion(hriOffline, rootdir, conf, offlineTable);
    MetaTableAccessor.addRegionToMeta(master.getConnection(), hriOffline);

    log("Regions in hbase:meta and namespace have been created");

    // at this point we only expect 3 regions to be assigned out
    // (catalogs and namespace, + 1 online region)
    assertEquals(3, cluster.countServedRegions());
    HRegionInfo hriOnline = null;
    try (RegionLocator locator =
        TEST_UTIL.getConnection().getRegionLocator(TableName.valueOf("onlineTable"))) {
      hriOnline = locator.getRegionLocation(HConstants.EMPTY_START_ROW).getRegionInfo();
    }
    RegionStates regionStates = master.getAssignmentManager().getRegionStates();
    RegionStateStore stateStore = master.getAssignmentManager().getRegionStateStore();

    // Put the online region in pending_close. It is actually already opened.
    // This is to simulate that the region close RPC is not sent out before failover
    RegionState oldState = regionStates.getRegionState(hriOnline);
    RegionState newState = new RegionState(
      hriOnline, State.PENDING_CLOSE, oldState.getServerName());
    stateStore.updateRegionState(HConstants.NO_SEQNUM, newState, oldState);

    // Put the offline region in pending_open. It is actually not opened yet.
    // This is to simulate that the region open RPC is not sent out before failover
    oldState = new RegionState(hriOffline, State.OFFLINE);
    newState = new RegionState(hriOffline, State.PENDING_OPEN, newState.getServerName());
    stateStore.updateRegionState(HConstants.NO_SEQNUM, newState, oldState);

    HRegionInfo failedClose = new HRegionInfo(offlineTable.getTableName(), null, null);
    createRegion(failedClose, rootdir, conf, offlineTable);
    MetaTableAccessor.addRegionToMeta(master.getConnection(), failedClose);

    oldState = new RegionState(failedClose, State.PENDING_CLOSE);
    newState = new RegionState(failedClose, State.FAILED_CLOSE, newState.getServerName());
    stateStore.updateRegionState(HConstants.NO_SEQNUM, newState, oldState);

    HRegionInfo failedOpen = new HRegionInfo(offlineTable.getTableName(), null, null);
    createRegion(failedOpen, rootdir, conf, offlineTable);
    MetaTableAccessor.addRegionToMeta(master.getConnection(), failedOpen);

    // Simulate a region transitioning to failed open when the region server reports the
    // transition as FAILED_OPEN
    oldState = new RegionState(failedOpen, State.PENDING_OPEN);
    newState = new RegionState(failedOpen, State.FAILED_OPEN, newState.getServerName());
    stateStore.updateRegionState(HConstants.NO_SEQNUM, newState, oldState);

    HRegionInfo failedOpenNullServer = new HRegionInfo(offlineTable.getTableName(), null, null);
    LOG.info("Failed open NUll server " + failedOpenNullServer.getEncodedName());
    createRegion(failedOpenNullServer, rootdir, conf, offlineTable);
    MetaTableAccessor.addRegionToMeta(master.getConnection(), failedOpenNullServer);

    // Simulate a region transitioning to failed open when the master couldn't find a plan for
    // the region
    oldState = new RegionState(failedOpenNullServer, State.OFFLINE);
    newState = new RegionState(failedOpenNullServer, State.FAILED_OPEN, null);
    stateStore.updateRegionState(HConstants.NO_SEQNUM, newState, oldState);

    // Stop the master
    log("Aborting master");
    cluster.abortMaster(0);
    cluster.waitOnMaster(0);
    log("Master has aborted");

    // Start up a new master
    log("Starting up a new master");
    master = cluster.startMaster().getMaster();
    log("Waiting for master to be ready");
    cluster.waitForActiveAndReadyMaster();
    log("Master is ready");

    // Wait till no region in transition any more
    TEST_UTIL.waitUntilNoRegionsInTransition(60000);

    // Get new region states since master restarted
    regionStates = master.getAssignmentManager().getRegionStates();

    // Both pending_open (RPC sent/not yet) regions should be online
    assertTrue(regionStates.isRegionOnline(hriOffline));
    assertTrue(regionStates.isRegionOnline(hriOnline));
    assertTrue(regionStates.isRegionOnline(failedClose));
    assertTrue(regionStates.isRegionOnline(failedOpenNullServer));
    assertTrue(regionStates.isRegionOnline(failedOpen));

    log("Done with verification, shutting down cluster");

    // Done, shutdown the cluster
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
    assertEquals("hbase:meta should be onlined on RS",
      metaState.getServerName(), rs.getServerName());
    assertEquals("hbase:meta should be onlined on RS",
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
    assertEquals("hbase:meta should be onlined on RS",
      metaState.getServerName(), rs.getServerName());
    assertEquals("hbase:meta should be onlined on RS",
      metaState.getState(), State.OPEN);

    // Update meta state as PENDING_OPEN, then kill master
    // that simulates, that RS successfully deployed, but
    // RPC was lost right before failure.
    // region server should expire (how it can be verified?)
    MetaTableLocator.setMetaLocation(activeMaster.getZooKeeper(),
      rs.getServerName(), State.PENDING_OPEN);
    Region meta = rs.getFromOnlineRegions(HRegionInfo.FIRST_META_REGIONINFO.getEncodedName());
    rs.removeFromOnlineRegions(meta, null);
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
    assertEquals("hbase:meta should be onlined on RS",
      metaState.getServerName(), rs.getServerName());
    assertEquals("hbase:meta should be onlined on RS",
      metaState.getState(), State.OPEN);

    // Update meta state as PENDING_CLOSE, then kill master
    // that simulates, that RS successfully deployed, but
    // RPC was lost right before failure.
    // region server should expire (how it can be verified?)
    MetaTableLocator.setMetaLocation(activeMaster.getZooKeeper(),
      rs.getServerName(), State.PENDING_CLOSE);

    log("Aborting master");
    activeMaster.abort("test-kill");
    cluster.waitForMasterToStop(activeMaster.getServerName(), 30000);
    log("Master has aborted");

    rs.getRSRpcServices().closeRegion(null, ProtobufUtil.buildCloseRegionRequest(
      rs.getServerName(), HRegionInfo.FIRST_META_REGIONINFO.getEncodedName()));

    // Start up a new master
    log("Starting up a new master");
    activeMaster = cluster.startMaster().getMaster();
    log("Waiting for master to be ready");
    cluster.waitForActiveAndReadyMaster();
    log("Master is ready");

    TEST_UTIL.waitUntilNoRegionsInTransition(60000);
    log("Meta was assigned");

    // Done, shutdown the cluster
    TEST_UTIL.shutdownMiniCluster();
  }
}

