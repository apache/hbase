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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.RegionTransition;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableStateManager;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionMergeTransactionImpl;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKTableStateManager;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.data.Stat;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestMasterFailover {
  private static final Log LOG = LogFactory.getLog(TestMasterFailover.class);

  /**
   * Complex test of master failover that tests as many permutations of the
   * different possible states that regions in transition could be in within ZK.
   * <p>
   * This tests the proper handling of these states by the failed-over master
   * and includes a thorough testing of the timeout code as well.
   * <p>
   * Starts with a single master and three regionservers.
   * <p>
   * Creates two tables, enabledTable and disabledTable, each containing 5
   * regions.  The disabledTable is then disabled.
   * <p>
   * After reaching steady-state, the master is killed.  We then mock several
   * states in ZK.
   * <p>
   * After mocking them, we will startup a new master which should become the
   * active master and also detect that it is a failover.  The primary test
   * passing condition will be that all regions of the enabled table are
   * assigned and all the regions of the disabled table are not assigned.
   * <p>
   * The different scenarios to be tested are below:
   * <p>
   * <b>ZK State:  OFFLINE</b>
   * <p>A node can get into OFFLINE state if</p>
   * <ul>
   * <li>An RS fails to open a region, so it reverts the state back to OFFLINE
   * <li>The Master is assigning the region to a RS before it sends RPC
   * </ul>
   * <p>We will mock the scenarios</p>
   * <ul>
   * <li>Master has assigned an enabled region but RS failed so a region is
   *     not assigned anywhere and is sitting in ZK as OFFLINE</li>
   * <li>This seems to cover both cases?</li>
   * </ul>
   * <p>
   * <b>ZK State:  CLOSING</b>
   * <p>A node can get into CLOSING state if</p>
   * <ul>
   * <li>An RS has begun to close a region
   * </ul>
   * <p>We will mock the scenarios</p>
   * <ul>
   * <li>Region of enabled table was being closed but did not complete
   * <li>Region of disabled table was being closed but did not complete
   * </ul>
   * <p>
   * <b>ZK State:  CLOSED</b>
   * <p>A node can get into CLOSED state if</p>
   * <ul>
   * <li>An RS has completed closing a region but not acknowledged by master yet
   * </ul>
   * <p>We will mock the scenarios</p>
   * <ul>
   * <li>Region of a table that should be enabled was closed on an RS
   * <li>Region of a table that should be disabled was closed on an RS
   * </ul>
   * <p>
   * <b>ZK State:  OPENING</b>
   * <p>A node can get into OPENING state if</p>
   * <ul>
   * <li>An RS has begun to open a region
   * </ul>
   * <p>We will mock the scenarios</p>
   * <ul>
   * <li>RS was opening a region of enabled table but never finishes
   * </ul>
   * <p>
   * <b>ZK State:  OPENED</b>
   * <p>A node can get into OPENED state if</p>
   * <ul>
   * <li>An RS has finished opening a region but not acknowledged by master yet
   * </ul>
   * <p>We will mock the scenarios</p>
   * <ul>
   * <li>Region of a table that should be enabled was opened on an RS
   * <li>Region of a table that should be disabled was opened on an RS
   * </ul>
   * @throws Exception
   */
  @Test (timeout=240000)
  public void testMasterFailoverWithMockedRIT() throws Exception {

    final int NUM_MASTERS = 1;
    final int NUM_RS = 3;

    // Create config to use for this cluster
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean("hbase.assignment.usezk", true);

    // Start the cluster
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(conf);
    TEST_UTIL.startMiniCluster(NUM_MASTERS, NUM_RS);
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    log("Cluster started");

    // Create a ZKW to use in the test
    ZooKeeperWatcher zkw = HBaseTestingUtility.getZooKeeperWatcher(TEST_UTIL);

    // get all the master threads
    List<MasterThread> masterThreads = cluster.getMasterThreads();
    assertEquals(1, masterThreads.size());

    // only one master thread, let's wait for it to be initialized
    assertTrue(cluster.waitForActiveAndReadyMaster());
    HMaster master = masterThreads.get(0).getMaster();
    assertTrue(master.isActiveMaster());
    assertTrue(master.isInitialized());

    // disable load balancing on this master
    master.balanceSwitch(false);

    // create two tables in META, each with 10 regions
    byte [] FAMILY = Bytes.toBytes("family");
    byte [][] SPLIT_KEYS = new byte [][] {
        new byte[0], Bytes.toBytes("aaa"), Bytes.toBytes("bbb"),
        Bytes.toBytes("ccc"), Bytes.toBytes("ddd"), Bytes.toBytes("eee"),
        Bytes.toBytes("fff"), Bytes.toBytes("ggg"), Bytes.toBytes("hhh"),
        Bytes.toBytes("iii"), Bytes.toBytes("jjj")
    };

    byte [] enabledTable = Bytes.toBytes("enabledTable");
    HTableDescriptor htdEnabled = new HTableDescriptor(TableName.valueOf(enabledTable));
    htdEnabled.addFamily(new HColumnDescriptor(FAMILY));

    FileSystem filesystem = FileSystem.get(conf);
    Path rootdir = FSUtils.getRootDir(conf);
    FSTableDescriptors fstd = new FSTableDescriptors(conf, filesystem, rootdir);
    // Write the .tableinfo
    fstd.createTableDescriptor(htdEnabled);

    HRegionInfo hriEnabled = new HRegionInfo(htdEnabled.getTableName(), null, null);
    createRegion(hriEnabled, rootdir, conf, htdEnabled);

    List<HRegionInfo> enabledRegions = TEST_UTIL.createMultiRegionsInMeta(
        TEST_UTIL.getConfiguration(), htdEnabled, SPLIT_KEYS);

    TableName disabledTable = TableName.valueOf("disabledTable");
    HTableDescriptor htdDisabled = new HTableDescriptor(disabledTable);
    htdDisabled.addFamily(new HColumnDescriptor(FAMILY));
    // Write the .tableinfo
    fstd.createTableDescriptor(htdDisabled);
    HRegionInfo hriDisabled = new HRegionInfo(htdDisabled.getTableName(), null, null);
    createRegion(hriDisabled, rootdir, conf, htdDisabled);
    List<HRegionInfo> disabledRegions = TEST_UTIL.createMultiRegionsInMeta(
        TEST_UTIL.getConfiguration(), htdDisabled, SPLIT_KEYS);

    TableName tableWithMergingRegions = TableName.valueOf("tableWithMergingRegions");
    TEST_UTIL.createTable(tableWithMergingRegions, FAMILY, new byte [][] {Bytes.toBytes("m")});

    log("Regions in hbase:meta and namespace have been created");

    // at this point we only expect 4 regions to be assigned out
    // (catalogs and namespace, + 2 merging regions)
    assertEquals(4, cluster.countServedRegions());

    // Move merging regions to the same region server
    AssignmentManager am = master.getAssignmentManager();
    RegionStates regionStates = am.getRegionStates();
    List<HRegionInfo> mergingRegions = regionStates.getRegionsOfTable(tableWithMergingRegions);
    assertEquals(2, mergingRegions.size());
    HRegionInfo a = mergingRegions.get(0);
    HRegionInfo b = mergingRegions.get(1);
    HRegionInfo newRegion = RegionMergeTransactionImpl.getMergedRegionInfo(a, b);
    ServerName mergingServer = regionStates.getRegionServerOfRegion(a);
    ServerName serverB = regionStates.getRegionServerOfRegion(b);
    if (!serverB.equals(mergingServer)) {
      RegionPlan plan = new RegionPlan(b, serverB, mergingServer);
      am.balance(plan);
      assertTrue(am.waitForAssignment(b));
    }

    // Let's just assign everything to first RS
    HRegionServer hrs = cluster.getRegionServer(0);
    ServerName serverName = hrs.getServerName();
    HRegionInfo closingRegion = enabledRegions.remove(0);
    // we'll need some regions to already be assigned out properly on live RS
    List<HRegionInfo> enabledAndAssignedRegions = new ArrayList<HRegionInfo>();
    enabledAndAssignedRegions.add(enabledRegions.remove(0));
    enabledAndAssignedRegions.add(enabledRegions.remove(0));
    enabledAndAssignedRegions.add(closingRegion);

    List<HRegionInfo> disabledAndAssignedRegions = new ArrayList<HRegionInfo>();
    disabledAndAssignedRegions.add(disabledRegions.remove(0));
    disabledAndAssignedRegions.add(disabledRegions.remove(0));

    // now actually assign them
    for (HRegionInfo hri : enabledAndAssignedRegions) {
      master.assignmentManager.addPlan(hri.getEncodedName(),
          new RegionPlan(hri, null, serverName));
      master.assignRegion(hri);
    }

    for (HRegionInfo hri : disabledAndAssignedRegions) {
      master.assignmentManager.addPlan(hri.getEncodedName(),
          new RegionPlan(hri, null, serverName));
      master.assignRegion(hri);
    }

    // wait for no more RIT
    log("Waiting for assignment to finish");
    ZKAssign.blockUntilNoRIT(zkw);
    log("Assignment completed");

    // Stop the master
    log("Aborting master");
    cluster.abortMaster(0);
    cluster.waitOnMaster(0);
    log("Master has aborted");

    /*
     * Now, let's start mocking up some weird states as described in the method
     * javadoc.
     */

    List<HRegionInfo> regionsThatShouldBeOnline = new ArrayList<HRegionInfo>();
    List<HRegionInfo> regionsThatShouldBeOffline = new ArrayList<HRegionInfo>();

    log("Beginning to mock scenarios");

    // Disable the disabledTable in ZK
    TableStateManager zktable = new ZKTableStateManager(zkw);
    zktable.setTableState(disabledTable, ZooKeeperProtos.Table.State.DISABLED);

    /*
     *  ZK = OFFLINE
     */

    // Region that should be assigned but is not and is in ZK as OFFLINE
    // Cause: This can happen if the master crashed after creating the znode but before sending the
    //  request to the region server
    HRegionInfo region = enabledRegions.remove(0);
    regionsThatShouldBeOnline.add(region);
    ZKAssign.createNodeOffline(zkw, region, serverName);

    /*
     * ZK = CLOSING
     */
    // Cause: Same as offline.
    regionsThatShouldBeOnline.add(closingRegion);
    ZKAssign.createNodeClosing(zkw, closingRegion, serverName);

    /*
     * ZK = CLOSED
     */

    // Region of enabled table closed but not ack
    //Cause: Master was down while the region server updated the ZK status.
    region = enabledRegions.remove(0);
    regionsThatShouldBeOnline.add(region);
    int version = ZKAssign.createNodeClosing(zkw, region, serverName);
    ZKAssign.transitionNodeClosed(zkw, region, serverName, version);

    // Region of disabled table closed but not ack
    region = disabledRegions.remove(0);
    regionsThatShouldBeOffline.add(region);
    version = ZKAssign.createNodeClosing(zkw, region, serverName);
    ZKAssign.transitionNodeClosed(zkw, region, serverName, version);

    /*
     * ZK = OPENED
     */

    // Region of enabled table was opened on RS
    // Cause: as offline
    region = enabledRegions.remove(0);
    regionsThatShouldBeOnline.add(region);
    ZKAssign.createNodeOffline(zkw, region, serverName);
    ProtobufUtil.openRegion(hrs.getRSRpcServices(), hrs.getServerName(), region);
    while (true) {
      byte [] bytes = ZKAssign.getData(zkw, region.getEncodedName());
      RegionTransition rt = RegionTransition.parseFrom(bytes);
      if (rt != null && rt.getEventType().equals(EventType.RS_ZK_REGION_OPENED)) {
        break;
      }
      Thread.sleep(100);
    }

    // Region of disable table was opened on RS
    // Cause: Master failed while updating the status for this region server.
    region = disabledRegions.remove(0);
    regionsThatShouldBeOffline.add(region);
    ZKAssign.createNodeOffline(zkw, region, serverName);
    ProtobufUtil.openRegion(hrs.getRSRpcServices(), hrs.getServerName(), region);
    while (true) {
      byte [] bytes = ZKAssign.getData(zkw, region.getEncodedName());
      RegionTransition rt = RegionTransition.parseFrom(bytes);
      if (rt != null && rt.getEventType().equals(EventType.RS_ZK_REGION_OPENED)) {
        break;
      }
      Thread.sleep(100);
    }

    /*
     * ZK = MERGING
     */

    // Regions of table of merging regions
    // Cause: Master was down while merging was going on
    hrs.getCoordinatedStateManager().
      getRegionMergeCoordination().startRegionMergeTransaction(newRegion, mergingServer, a, b);

    /*
     * ZK = NONE
     */

    /*
     * DONE MOCKING
     */

    log("Done mocking data up in ZK");

    // Start up a new master
    log("Starting up a new master");
    master = cluster.startMaster().getMaster();
    log("Waiting for master to be ready");
    cluster.waitForActiveAndReadyMaster();
    log("Master is ready");

    // Get new region states since master restarted
    regionStates = master.getAssignmentManager().getRegionStates();
    // Merging region should remain merging
    assertTrue(regionStates.isRegionInState(a, State.MERGING));
    assertTrue(regionStates.isRegionInState(b, State.MERGING));
    assertTrue(regionStates.isRegionInState(newRegion, State.MERGING_NEW));
    // Now remove the faked merging znode, merging regions should be
    // offlined automatically, otherwise it is a bug in AM.
    ZKAssign.deleteNodeFailSilent(zkw, newRegion);

    // Failover should be completed, now wait for no RIT
    log("Waiting for no more RIT");
    ZKAssign.blockUntilNoRIT(zkw);
    log("No more RIT in ZK, now doing final test verification");

    // Grab all the regions that are online across RSs
    Set<HRegionInfo> onlineRegions = new TreeSet<HRegionInfo>();
    for (JVMClusterUtil.RegionServerThread rst :
      cluster.getRegionServerThreads()) {
      onlineRegions.addAll(ProtobufUtil.getOnlineRegions(
        rst.getRegionServer().getRSRpcServices()));
    }

    // Now, everything that should be online should be online
    for (HRegionInfo hri : regionsThatShouldBeOnline) {
      assertTrue(onlineRegions.contains(hri));
    }

    // Everything that should be offline should not be online
    for (HRegionInfo hri : regionsThatShouldBeOffline) {
      if (onlineRegions.contains(hri)) {
       LOG.debug(hri);
      }
      assertFalse(onlineRegions.contains(hri));
    }

    log("Done with verification, all passed, shutting down cluster");

    // Done, shutdown the cluster
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Complex test of master failover that tests as many permutations of the
   * different possible states that regions in transition could be in within ZK
   * pointing to an RS that has died while no master is around to process it.
   * <p>
   * This tests the proper handling of these states by the failed-over master
   * and includes a thorough testing of the timeout code as well.
   * <p>
   * Starts with a single master and two regionservers.
   * <p>
   * Creates two tables, enabledTable and disabledTable, each containing 5
   * regions.  The disabledTable is then disabled.
   * <p>
   * After reaching steady-state, the master is killed.  We then mock several
   * states in ZK.  And one of the RS will be killed.
   * <p>
   * After mocking them and killing an RS, we will startup a new master which
   * should become the active master and also detect that it is a failover.  The
   * primary test passing condition will be that all regions of the enabled
   * table are assigned and all the regions of the disabled table are not
   * assigned.
   * <p>
   * The different scenarios to be tested are below:
   * <p>
   * <b>ZK State:  CLOSING</b>
   * <p>A node can get into CLOSING state if</p>
   * <ul>
   * <li>An RS has begun to close a region
   * </ul>
   * <p>We will mock the scenarios</p>
   * <ul>
   * <li>Region was being closed but the RS died before finishing the close
   * </ul>
   * <b>ZK State:  OPENED</b>
   * <p>A node can get into OPENED state if</p>
   * <ul>
   * <li>An RS has finished opening a region but not acknowledged by master yet
   * </ul>
   * <p>We will mock the scenarios</p>
   * <ul>
   * <li>Region of a table that should be enabled was opened by a now-dead RS
   * <li>Region of a table that should be disabled was opened by a now-dead RS
   * </ul>
   * <p>
   * <b>ZK State:  NONE</b>
   * <p>A region could not have a transition node if</p>
   * <ul>
   * <li>The server hosting the region died and no master processed it
   * </ul>
   * <p>We will mock the scenarios</p>
   * <ul>
   * <li>Region of enabled table was on a dead RS that was not yet processed
   * <li>Region of disabled table was on a dead RS that was not yet processed
   * </ul>
   * @throws Exception
   */
  @Test (timeout=180000)
  public void testMasterFailoverWithMockedRITOnDeadRS() throws Exception {

    final int NUM_MASTERS = 1;
    final int NUM_RS = 2;

    // Create and start the cluster
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean("hbase.assignment.usezk", true);

    conf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, 1);
    conf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MAXTOSTART, 2);
    TEST_UTIL.startMiniCluster(NUM_MASTERS, NUM_RS);
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    log("Cluster started");

    // Create a ZKW to use in the test
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(TEST_UTIL.getConfiguration(),
        "unittest", new Abortable() {

          @Override
          public void abort(String why, Throwable e) {
            LOG.error("Fatal ZK Error: " + why, e);
            org.junit.Assert.assertFalse("Fatal ZK error", true);
          }

          @Override
          public boolean isAborted() {
            return false;
          }

    });

    // get all the master threads
    List<MasterThread> masterThreads = cluster.getMasterThreads();
    assertEquals(1, masterThreads.size());

    // only one master thread, let's wait for it to be initialized
    assertTrue(cluster.waitForActiveAndReadyMaster());
    HMaster master = masterThreads.get(0).getMaster();
    assertTrue(master.isActiveMaster());
    assertTrue(master.isInitialized());

    // disable load balancing on this master
    master.balanceSwitch(false);

    // create two tables in META, each with 30 regions
    byte [] FAMILY = Bytes.toBytes("family");
    byte[][] SPLIT_KEYS =
        TEST_UTIL.getRegionSplitStartKeys(Bytes.toBytes("aaa"), Bytes.toBytes("zzz"), 30);

    byte [] enabledTable = Bytes.toBytes("enabledTable");
    HTableDescriptor htdEnabled = new HTableDescriptor(TableName.valueOf(enabledTable));
    htdEnabled.addFamily(new HColumnDescriptor(FAMILY));
    FileSystem filesystem = FileSystem.get(conf);
    Path rootdir = FSUtils.getRootDir(conf);
    FSTableDescriptors fstd = new FSTableDescriptors(conf, filesystem, rootdir);
    // Write the .tableinfo
    fstd.createTableDescriptor(htdEnabled);
    HRegionInfo hriEnabled = new HRegionInfo(htdEnabled.getTableName(),
        null, null);
    createRegion(hriEnabled, rootdir, conf, htdEnabled);

    List<HRegionInfo> enabledRegions = TEST_UTIL.createMultiRegionsInMeta(
        TEST_UTIL.getConfiguration(), htdEnabled, SPLIT_KEYS);

    TableName disabledTable =
        TableName.valueOf("disabledTable");
    HTableDescriptor htdDisabled = new HTableDescriptor(disabledTable);
    htdDisabled.addFamily(new HColumnDescriptor(FAMILY));
    // Write the .tableinfo
    fstd.createTableDescriptor(htdDisabled);
    HRegionInfo hriDisabled = new HRegionInfo(htdDisabled.getTableName(), null, null);
    createRegion(hriDisabled, rootdir, conf, htdDisabled);

    List<HRegionInfo> disabledRegions = TEST_UTIL.createMultiRegionsInMeta(
        TEST_UTIL.getConfiguration(), htdDisabled, SPLIT_KEYS);

    log("Regions in hbase:meta and Namespace have been created");

    // at this point we only expect 2 regions to be assigned out (catalogs and namespace  )
    assertEquals(2, cluster.countServedRegions());

    // The first RS will stay online
    List<RegionServerThread> regionservers =
      cluster.getRegionServerThreads();
    HRegionServer hrs = regionservers.get(0).getRegionServer();

    // The second RS is going to be hard-killed
    RegionServerThread hrsDeadThread = regionservers.get(1);
    HRegionServer hrsDead = hrsDeadThread.getRegionServer();
    ServerName deadServerName = hrsDead.getServerName();

    // we'll need some regions to already be assigned out properly on live RS
    List<HRegionInfo> enabledAndAssignedRegions = new ArrayList<HRegionInfo>();
    enabledAndAssignedRegions.addAll(enabledRegions.subList(0, 6));
    enabledRegions.removeAll(enabledAndAssignedRegions);
    List<HRegionInfo> disabledAndAssignedRegions = new ArrayList<HRegionInfo>();
    disabledAndAssignedRegions.addAll(disabledRegions.subList(0, 6));
    disabledRegions.removeAll(disabledAndAssignedRegions);

    // now actually assign them
    for (HRegionInfo hri : enabledAndAssignedRegions) {
      master.assignmentManager.addPlan(hri.getEncodedName(),
          new RegionPlan(hri, null, hrs.getServerName()));
      master.assignRegion(hri);
    }
    for (HRegionInfo hri : disabledAndAssignedRegions) {
      master.assignmentManager.addPlan(hri.getEncodedName(),
          new RegionPlan(hri, null, hrs.getServerName()));
      master.assignRegion(hri);
    }

    log("Waiting for assignment to finish");
    ZKAssign.blockUntilNoRIT(zkw);
    master.assignmentManager.waitUntilNoRegionsInTransition(60000);
    log("Assignment completed");

    assertTrue(" Table must be enabled.", master.getAssignmentManager()
        .getTableStateManager().isTableState(TableName.valueOf("enabledTable"),
        ZooKeeperProtos.Table.State.ENABLED));
    // we also need regions assigned out on the dead server
    List<HRegionInfo> enabledAndOnDeadRegions = new ArrayList<HRegionInfo>();
    enabledAndOnDeadRegions.addAll(enabledRegions.subList(0, 6));
    enabledRegions.removeAll(enabledAndOnDeadRegions);
    List<HRegionInfo> disabledAndOnDeadRegions = new ArrayList<HRegionInfo>();
    disabledAndOnDeadRegions.addAll(disabledRegions.subList(0, 6));
    disabledRegions.removeAll(disabledAndOnDeadRegions);

    // set region plan to server to be killed and trigger assign
    for (HRegionInfo hri : enabledAndOnDeadRegions) {
      master.assignmentManager.addPlan(hri.getEncodedName(),
          new RegionPlan(hri, null, deadServerName));
      master.assignRegion(hri);
    }
    for (HRegionInfo hri : disabledAndOnDeadRegions) {
      master.assignmentManager.addPlan(hri.getEncodedName(),
          new RegionPlan(hri, null, deadServerName));
      master.assignRegion(hri);
    }

    // wait for no more RIT
    log("Waiting for assignment to finish");
    ZKAssign.blockUntilNoRIT(zkw);
    master.assignmentManager.waitUntilNoRegionsInTransition(60000);
    log("Assignment completed");

    // Due to master.assignRegion(hri) could fail to assign a region to a specified RS
    // therefore, we need make sure that regions are in the expected RS
    verifyRegionLocation(hrs, enabledAndAssignedRegions);
    verifyRegionLocation(hrs, disabledAndAssignedRegions);
    verifyRegionLocation(hrsDead, enabledAndOnDeadRegions);
    verifyRegionLocation(hrsDead, disabledAndOnDeadRegions);

    assertTrue(" Didn't get enough regions of enabledTalbe on live rs.",
      enabledAndAssignedRegions.size() >= 2);
    assertTrue(" Didn't get enough regions of disalbedTable on live rs.",
      disabledAndAssignedRegions.size() >= 2);
    assertTrue(" Didn't get enough regions of enabledTalbe on dead rs.",
      enabledAndOnDeadRegions.size() >= 2);
    assertTrue(" Didn't get enough regions of disalbedTable on dead rs.",
      disabledAndOnDeadRegions.size() >= 2);

    // Stop the master
    log("Aborting master");
    cluster.abortMaster(0);
    cluster.waitOnMaster(0);
    log("Master has aborted");

    /*
     * Now, let's start mocking up some weird states as described in the method
     * javadoc.
     */

    List<HRegionInfo> regionsThatShouldBeOnline = new ArrayList<HRegionInfo>();
    List<HRegionInfo> regionsThatShouldBeOffline = new ArrayList<HRegionInfo>();

    log("Beginning to mock scenarios");

    // Disable the disabledTable in ZK
    TableStateManager zktable = new ZKTableStateManager(zkw);
    zktable.setTableState(disabledTable, ZooKeeperProtos.Table.State.DISABLED);

    assertTrue(" The enabled table should be identified on master fail over.",
        zktable.isTableState(TableName.valueOf("enabledTable"),
          ZooKeeperProtos.Table.State.ENABLED));

    /*
     * ZK = CLOSING
     */

    // Region of enabled table being closed on dead RS but not finished
    HRegionInfo region = enabledAndOnDeadRegions.remove(0);
    regionsThatShouldBeOnline.add(region);
    ZKAssign.createNodeClosing(zkw, region, deadServerName);
    LOG.debug("\n\nRegion of enabled table was CLOSING on dead RS\n" +
        region + "\n\n");

    // Region of disabled table being closed on dead RS but not finished
    region = disabledAndOnDeadRegions.remove(0);
    regionsThatShouldBeOffline.add(region);
    ZKAssign.createNodeClosing(zkw, region, deadServerName);
    LOG.debug("\n\nRegion of disabled table was CLOSING on dead RS\n" +
        region + "\n\n");

    /*
     * ZK = CLOSED
     */

    // Region of enabled on dead server gets closed but not ack'd by master
    region = enabledAndOnDeadRegions.remove(0);
    regionsThatShouldBeOnline.add(region);
    int version = ZKAssign.createNodeClosing(zkw, region, deadServerName);
    ZKAssign.transitionNodeClosed(zkw, region, deadServerName, version);
    LOG.debug("\n\nRegion of enabled table was CLOSED on dead RS\n" +
        region + "\n\n");

    // Region of disabled on dead server gets closed but not ack'd by master
    region = disabledAndOnDeadRegions.remove(0);
    regionsThatShouldBeOffline.add(region);
    version = ZKAssign.createNodeClosing(zkw, region, deadServerName);
    ZKAssign.transitionNodeClosed(zkw, region, deadServerName, version);
    LOG.debug("\n\nRegion of disabled table was CLOSED on dead RS\n" +
        region + "\n\n");

    /*
     * ZK = OPENING
     */

    // RS was opening a region of enabled table then died
    region = enabledRegions.remove(0);
    regionsThatShouldBeOnline.add(region);
    ZKAssign.createNodeOffline(zkw, region, deadServerName);
    ZKAssign.transitionNodeOpening(zkw, region, deadServerName);
    LOG.debug("\n\nRegion of enabled table was OPENING on dead RS\n" +
        region + "\n\n");

    // RS was opening a region of disabled table then died
    region = disabledRegions.remove(0);
    regionsThatShouldBeOffline.add(region);
    ZKAssign.createNodeOffline(zkw, region, deadServerName);
    ZKAssign.transitionNodeOpening(zkw, region, deadServerName);
    LOG.debug("\n\nRegion of disabled table was OPENING on dead RS\n" +
        region + "\n\n");

    /*
     * ZK = OPENED
     */

    // Region of enabled table was opened on dead RS
    region = enabledRegions.remove(0);
    regionsThatShouldBeOnline.add(region);
    ZKAssign.createNodeOffline(zkw, region, deadServerName);
    ProtobufUtil.openRegion(hrsDead.getRSRpcServices(),
      hrsDead.getServerName(), region);
    while (true) {
      byte [] bytes = ZKAssign.getData(zkw, region.getEncodedName());
      RegionTransition rt = RegionTransition.parseFrom(bytes);
      if (rt != null && rt.getEventType().equals(EventType.RS_ZK_REGION_OPENED)) {
        break;
      }
      Thread.sleep(100);
    }
    LOG.debug("\n\nRegion of enabled table was OPENED on dead RS\n" + region + "\n\n");

    // Region of disabled table was opened on dead RS
    region = disabledRegions.remove(0);
    regionsThatShouldBeOffline.add(region);
    ZKAssign.createNodeOffline(zkw, region, deadServerName);
    ProtobufUtil.openRegion(hrsDead.getRSRpcServices(),
      hrsDead.getServerName(), region);
    while (true) {
      byte [] bytes = ZKAssign.getData(zkw, region.getEncodedName());
      RegionTransition rt = RegionTransition.parseFrom(bytes);
      if (rt != null && rt.getEventType().equals(EventType.RS_ZK_REGION_OPENED)) {
        break;
      }
      Thread.sleep(100);
    }
    LOG.debug("\n\nRegion of disabled table was OPENED on dead RS\n" + region + "\n\n");

    /*
     * ZK = NONE
     */

    // Region of enabled table was open at steady-state on dead RS
    region = enabledRegions.remove(0);
    regionsThatShouldBeOnline.add(region);
    ZKAssign.createNodeOffline(zkw, region, deadServerName);
    ProtobufUtil.openRegion(hrsDead.getRSRpcServices(),
      hrsDead.getServerName(), region);
    while (true) {
      byte [] bytes = ZKAssign.getData(zkw, region.getEncodedName());
      RegionTransition rt = RegionTransition.parseFrom(bytes);
      if (rt != null && rt.getEventType().equals(EventType.RS_ZK_REGION_OPENED)) {
        ZKAssign.deleteOpenedNode(zkw, region.getEncodedName(), rt.getServerName());
        LOG.debug("DELETED " + rt);
        break;
      }
      Thread.sleep(100);
    }
    LOG.debug("\n\nRegion of enabled table was open at steady-state on dead RS"
        + "\n" + region + "\n\n");

    // Region of disabled table was open at steady-state on dead RS
    region = disabledRegions.remove(0);
    regionsThatShouldBeOffline.add(region);
    ZKAssign.createNodeOffline(zkw, region, deadServerName);
    ProtobufUtil.openRegion(hrsDead.getRSRpcServices(),
      hrsDead.getServerName(), region);
    while (true) {
      byte [] bytes = ZKAssign.getData(zkw, region.getEncodedName());
      RegionTransition rt = RegionTransition.parseFrom(bytes);
      if (rt != null && rt.getEventType().equals(EventType.RS_ZK_REGION_OPENED)) {
        ZKAssign.deleteOpenedNode(zkw, region.getEncodedName(), rt.getServerName());
        break;
      }
      Thread.sleep(100);
    }
    LOG.debug("\n\nRegion of disabled table was open at steady-state on dead RS"
      + "\n" + region + "\n\n");

    /*
     * DONE MOCKING
     */

    log("Done mocking data up in ZK");

    // Kill the RS that had a hard death
    log("Killing RS " + deadServerName);
    hrsDead.abort("Killing for unit test");
    log("RS " + deadServerName + " killed");

    // Start up a new master.  Wait until regionserver is completely down
    // before starting new master because of hbase-4511.
    while (hrsDeadThread.isAlive()) {
      Threads.sleep(10);
    }
    log("Starting up a new master");
    master = cluster.startMaster().getMaster();
    log("Waiting for master to be ready");
    assertTrue(cluster.waitForActiveAndReadyMaster());
    log("Master is ready");

    // Wait until SSH processing completed for dead server.
    while (master.getServerManager().areDeadServersInProgress()) {
      Thread.sleep(10);
    }

    // Failover should be completed, now wait for no RIT
    log("Waiting for no more RIT");
    ZKAssign.blockUntilNoRIT(zkw);
    log("No more RIT in ZK");
    long now = System.currentTimeMillis();
    long maxTime = 120000;
    boolean done = master.assignmentManager.waitUntilNoRegionsInTransition(maxTime);
    if (!done) {
      RegionStates regionStates = master.getAssignmentManager().getRegionStates();
      LOG.info("rit=" + regionStates.getRegionsInTransition());
    }
    long elapsed = System.currentTimeMillis() - now;
    assertTrue("Elapsed=" + elapsed + ", maxTime=" + maxTime + ", done=" + done,
      elapsed < maxTime);
    log("No more RIT in RIT map, doing final test verification");

    // Grab all the regions that are online across RSs
    Set<HRegionInfo> onlineRegions = new TreeSet<HRegionInfo>();
    now = System.currentTimeMillis();
    maxTime = 30000;
    for (JVMClusterUtil.RegionServerThread rst :
        cluster.getRegionServerThreads()) {
      try {
        HRegionServer rs = rst.getRegionServer();
        while (!rs.getRegionsInTransitionInRS().isEmpty()) {
          elapsed = System.currentTimeMillis() - now;
          assertTrue("Test timed out in getting online regions", elapsed < maxTime);
          if (rs.isAborted() || rs.isStopped()) {
            // This region server is stopped, skip it.
            break;
          }
          Thread.sleep(100);
        }
        onlineRegions.addAll(ProtobufUtil.getOnlineRegions(rs.getRSRpcServices()));
      } catch (RegionServerStoppedException e) {
        LOG.info("Got RegionServerStoppedException", e);
      }
    }

    // Now, everything that should be online should be online
    for (HRegionInfo hri : regionsThatShouldBeOnline) {
      assertTrue("region=" + hri.getRegionNameAsString() + ", " + onlineRegions.toString(),
        onlineRegions.contains(hri));
    }

    // Everything that should be offline should not be online
    for (HRegionInfo hri : regionsThatShouldBeOffline) {
      assertFalse(onlineRegions.contains(hri));
    }

    log("Done with verification, all passed, shutting down cluster");

    // Done, shutdown the cluster
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Verify regions are on the expected region server
   */
  private void verifyRegionLocation(HRegionServer hrs, List<HRegionInfo> regions)
      throws IOException {
    List<HRegionInfo> tmpOnlineRegions =
      ProtobufUtil.getOnlineRegions(hrs.getRSRpcServices());
    Iterator<HRegionInfo> itr = regions.iterator();
    while (itr.hasNext()) {
      HRegionInfo tmp = itr.next();
      if (!tmpOnlineRegions.contains(tmp)) {
        itr.remove();
      }
    }
  }

  HRegion createRegion(final HRegionInfo  hri, final Path rootdir, final Configuration c,
      final HTableDescriptor htd)
  throws IOException {
    HRegion r = HRegion.createHRegion(hri, rootdir, c, htd);
    // The above call to create a region will create an wal file.  Each
    // log file create will also create a running thread to do syncing.  We need
    // to close out this log else we will have a running thread trying to sync
    // the file system continuously which is ugly when dfs is taken away at the
    // end of the test.
    HRegion.closeHRegion(r);
    return r;
  }

  // TODO: Next test to add is with testing permutations of the RIT or the RS
  //       killed are hosting ROOT and hbase:meta regions.

  private void log(String string) {
    LOG.info("\n\n" + string + " \n\n");
  }

  @Test (timeout=180000)
  public void testShouldCheckMasterFailOverWhenMETAIsInOpenedState()
      throws Exception {
    LOG.info("Starting testShouldCheckMasterFailOverWhenMETAIsInOpenedState");
    final int NUM_MASTERS = 1;
    final int NUM_RS = 2;

    // Start the cluster
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt("hbase.master.info.port", -1);
    conf.setBoolean("hbase.assignment.usezk", true);

    TEST_UTIL.startMiniCluster(NUM_MASTERS, NUM_RS);
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();

    // Find regionserver carrying meta.
    List<RegionServerThread> regionServerThreads =
      cluster.getRegionServerThreads();
    Region metaRegion = null;
    HRegionServer metaRegionServer = null;
    for (RegionServerThread regionServerThread : regionServerThreads) {
      HRegionServer regionServer = regionServerThread.getRegionServer();
      metaRegion = regionServer.getOnlineRegion(HRegionInfo.FIRST_META_REGIONINFO.getRegionName());
      regionServer.abort("");
      if (null != metaRegion) {
        metaRegionServer = regionServer;
        break;
      }
    }

    TEST_UTIL.shutdownMiniHBaseCluster();

    // Create a ZKW to use in the test
    ZooKeeperWatcher zkw =
      HBaseTestingUtility.createAndForceNodeToOpenedState(TEST_UTIL,
          metaRegion, metaRegionServer.getServerName());

    LOG.info("Staring cluster for second time");
    TEST_UTIL.startMiniHBaseCluster(NUM_MASTERS, NUM_RS);

    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    while (!master.isInitialized()) {
      Thread.sleep(100);
    }
    // Failover should be completed, now wait for no RIT
    log("Waiting for no more RIT");
    ZKAssign.blockUntilNoRIT(zkw);

    zkw.close();
    // Stop the cluster
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * This tests a RIT in offline state will get re-assigned after a master restart
   */
  @Test(timeout=240000)
  public void testOfflineRegionReAssginedAfterMasterRestart() throws Exception {
    final TableName table = TableName.valueOf("testOfflineRegionReAssginedAfterMasterRestart");
    final int NUM_MASTERS = 1;
    final int NUM_RS = 2;

    // Create config to use for this cluster
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean("hbase.assignment.usezk", true);

    // Start the cluster
    final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(conf);
    TEST_UTIL.startMiniCluster(NUM_MASTERS, NUM_RS);
    log("Cluster started");

    TEST_UTIL.createTable(table, Bytes.toBytes("family"));
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    RegionStates regionStates = master.getAssignmentManager().getRegionStates();
    HRegionInfo hri = regionStates.getRegionsOfTable(table).get(0);
    ServerName serverName = regionStates.getRegionServerOfRegion(hri);
    TEST_UTIL.assertRegionOnServer(hri, serverName, 200);

    ServerName dstName = null;
    for (ServerName tmpServer : master.serverManager.getOnlineServers().keySet()) {
      if (!tmpServer.equals(serverName)) {
        dstName = tmpServer;
        break;
      }
    }
    // find a different server
    assertTrue(dstName != null);
    // shutdown HBase cluster
    TEST_UTIL.shutdownMiniHBaseCluster();
    // create a RIT node in offline state
    ZooKeeperWatcher zkw = TEST_UTIL.getZooKeeperWatcher();
    ZKAssign.createNodeOffline(zkw, hri, dstName);
    Stat stat = new Stat();
    byte[] data =
        ZKAssign.getDataNoWatch(zkw, hri.getEncodedName(), stat);
    assertTrue(data != null);
    RegionTransition rt = RegionTransition.parseFrom(data);
    assertTrue(rt.getEventType() == EventType.M_ZK_REGION_OFFLINE);

    LOG.info(hri.getEncodedName() + " region is in offline state with source server=" + serverName
        + " and dst server=" + dstName);

    // start HBase cluster
    TEST_UTIL.startMiniHBaseCluster(NUM_MASTERS, NUM_RS);

    while (true) {
      master = TEST_UTIL.getHBaseCluster().getMaster();
      if (master != null && master.isInitialized()) {
        ServerManager serverManager = master.getServerManager();
        if (!serverManager.areDeadServersInProgress()) {
          break;
        }
      }
      Thread.sleep(200);
    }

    // verify the region is assigned
    master = TEST_UTIL.getHBaseCluster().getMaster();
    master.getAssignmentManager().waitForAssignment(hri);
    regionStates = master.getAssignmentManager().getRegionStates();
    RegionState newState = regionStates.getRegionState(hri);
    assertTrue(newState.isOpened());
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
   * Test region in pending_open/close and failed_open/close when master failover
   */
  @Test (timeout=180000)
  @SuppressWarnings("deprecation")
  public void testPendingOpenOrCloseWhenMasterFailover() throws Exception {
    final int NUM_MASTERS = 1;
    final int NUM_RS = 1;

    // Create config to use for this cluster
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean("hbase.assignment.usezk", false);

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
    master.getAssignmentManager().waitUntilNoRegionsInTransition(60000);

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
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean("hbase.assignment.usezk", false);
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(conf);
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

    rs.getRSRpcServices().closeRegion(null, RequestConverter.buildCloseRegionRequest(
      rs.getServerName(), HRegionInfo.FIRST_META_REGIONINFO.getEncodedName(), false));

    // Start up a new master
    log("Starting up a new master");
    activeMaster = cluster.startMaster().getMaster();
    log("Waiting for master to be ready");
    cluster.waitForActiveAndReadyMaster();
    log("Master is ready");

    TEST_UTIL.waitUntilNoRegionsInTransition(60000);
    log("Meta was assigned");

    rs.getRSRpcServices().closeRegion(
      null,
      RequestConverter.buildCloseRegionRequest(rs.getServerName(),
        HRegionInfo.FIRST_META_REGIONINFO.getEncodedName(), false));

    // Set a dummy server to check if master reassigns meta on restart
    MetaTableLocator.setMetaLocation(activeMaster.getZooKeeper(),
      ServerName.valueOf("dummyserver.example.org", 1234, -1L), State.OPEN);

    log("Aborting master");
    activeMaster.stop("test-kill");

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

    // Done, shutdown the cluster
    TEST_UTIL.shutdownMiniCluster();
  }
}

