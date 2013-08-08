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
package org.apache.hadoop.hbase;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;


/**
 * Test the draining servers feature.
 *
 * This is typically an integration test: a unit test would be to check that the
 * master does no assign regions to a regionserver marked as drained.
 *
 * @see <a href="https://issues.apache.org/jira/browse/HBASE-4298">HBASE-4298</a>
 */
@Category(MediumTests.class)
public class TestDrainingServer {
  private static final Log LOG = LogFactory.getLog(TestDrainingServer.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final int NB_SLAVES = 5;
  private static final int COUNT_OF_REGIONS = NB_SLAVES * 2;

  /**
   * Spin up a cluster with a bunch of regions on it.
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(NB_SLAVES);
    TEST_UTIL.getHBaseCluster().waitForActiveAndReadyMaster();
    TEST_UTIL.getConfiguration().setBoolean("hbase.master.enabletable.roundrobin", true);

    final List<String> families = new ArrayList<String>(1);
    families.add("family");
    TEST_UTIL.createRandomTable("table", families, 1, 0, 0, COUNT_OF_REGIONS, 0);
    // Ensure a stable env
    TEST_UTIL.getHBaseAdmin().setBalancerRunning(false, false);

    boolean ready = false;
    while (!ready){
      waitForAllRegionsOnline();

      // Assert that every regionserver has some regions on it.
      int i = 0;
      ready = true;
      while (i < NB_SLAVES && ready){
        HRegionServer hrs = TEST_UTIL.getMiniHBaseCluster().getRegionServer(i);
        if (ProtobufUtil.getOnlineRegions(hrs).isEmpty()){
          ready = false;
        }
        i++;
      }

      if (!ready){
        TEST_UTIL.getHBaseAdmin().setBalancerRunning(true, true);
        Assert.assertTrue("Can't start a balance!", TEST_UTIL.getHBaseAdmin().balancer());
        TEST_UTIL.getHBaseAdmin().setBalancerRunning(false, false);
        Thread.sleep(100);
      }
    }
  }

  private static HRegionServer setDrainingServer(final HRegionServer hrs)
  throws KeeperException {
    LOG.info("Making " + hrs.getServerName() + " the draining server; " +
      "it has " + hrs.getNumberOfOnlineRegions() + " online regions");
    ZooKeeperWatcher zkw = hrs.getZooKeeper();
    String hrsDrainingZnode =
      ZKUtil.joinZNode(zkw.drainingZNode, hrs.getServerName().toString());
    ZKUtil.createWithParents(zkw, hrsDrainingZnode);
    return hrs;
  }

  private static HRegionServer unsetDrainingServer(final HRegionServer hrs)
  throws KeeperException {
    ZooKeeperWatcher zkw = hrs.getZooKeeper();
    String hrsDrainingZnode =
      ZKUtil.joinZNode(zkw.drainingZNode, hrs.getServerName().toString());
    ZKUtil.deleteNode(zkw, hrsDrainingZnode);
    return hrs;
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Test adding server to draining servers and then move regions off it.
   * Make sure that no regions are moved back to the draining server.
   * @throws IOException
   * @throws KeeperException
   */
  @Test  // (timeout=30000)
  public void testDrainingServerOffloading()
  throws Exception {
    // I need master in the below.
    HMaster master = TEST_UTIL.getMiniHBaseCluster().getMaster();
    HRegionInfo hriToMoveBack = null;
    // Set first server as draining server.
    HRegionServer drainingServer =
      setDrainingServer(TEST_UTIL.getMiniHBaseCluster().getRegionServer(0));
    try {
      final int regionsOnDrainingServer =
        drainingServer.getNumberOfOnlineRegions();
      Assert.assertTrue(regionsOnDrainingServer > 0);
      List<HRegionInfo> hris = ProtobufUtil.getOnlineRegions(drainingServer);
      for (HRegionInfo hri : hris) {
        // Pass null and AssignmentManager will chose a random server BUT it
        // should exclude draining servers.
        master.moveRegion(null,
          RequestConverter.buildMoveRegionRequest(hri.getEncodedNameAsBytes(), null));
        // Save off region to move back.
        hriToMoveBack = hri;
      }
      // Wait for regions to come back on line again.
      waitForAllRegionsOnline();
      Assert.assertEquals(0, drainingServer.getNumberOfOnlineRegions());
    } finally {
      unsetDrainingServer(drainingServer);
    }
    // Now we've unset the draining server, we should be able to move a region
    // to what was the draining server.
    master.moveRegion(null,
      RequestConverter.buildMoveRegionRequest(hriToMoveBack.getEncodedNameAsBytes(),
      Bytes.toBytes(drainingServer.getServerName().toString())));
    // Wait for regions to come back on line again.
    waitForAllRegionsOnline();
    Assert.assertEquals(1, drainingServer.getNumberOfOnlineRegions());
  }

  /**
   * Test that draining servers are ignored even after killing regionserver(s).
   * Verify that the draining server is not given any of the dead servers regions.
   * @throws KeeperException
   * @throws IOException
   */
  @Test  (timeout=30000)
  public void testDrainingServerWithAbort() throws KeeperException, Exception {
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();

    waitForAllRegionsOnline();

    final long regionCount = TEST_UTIL.getMiniHBaseCluster().countServedRegions();

    // Let's get a copy of the regions today.
    Collection<HRegion> regions = getRegions();
    LOG.info("All regions: " + regions);

      // Choose the draining server
    HRegionServer drainingServer = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);
    final int regionsOnDrainingServer = drainingServer.getNumberOfOnlineRegions();
    Assert.assertTrue(regionsOnDrainingServer > 0);

    ServerManager sm = master.getServerManager();

    Collection<HRegion> regionsBefore = drainingServer.getOnlineRegionsLocalContext();
    LOG.info("Regions of drained server are: "+ regionsBefore );

    try {
      // Add first server to draining servers up in zk.
      setDrainingServer(drainingServer);

      //wait for the master to receive and manage the event
      while  (sm.createDestinationServersList().contains(drainingServer.getServerName())) {
        Thread.sleep(1);
      }

      LOG.info("The available servers are: "+ sm.createDestinationServersList());

      Assert.assertEquals("Nothing should have happened here.", regionsOnDrainingServer,
        drainingServer.getNumberOfOnlineRegions());
      Assert.assertFalse("We should not have regions in transition here. List is: " +
          master.getAssignmentManager().getRegionStates().getRegionsInTransition(),
          master.getAssignmentManager().getRegionStates().isRegionsInTransition());

      // Kill a few regionservers.
      for (int aborted = 0; aborted <= 2; aborted++) {
        HRegionServer hrs = TEST_UTIL.getMiniHBaseCluster().getRegionServer(aborted + 1);
        hrs.abort("Aborting");
      }

      // Wait for regions to come back online again.  waitForAllRegionsOnline can come back before
      // we've assigned out regions on the cluster so retry if we are shy the wanted number
      Collection<HRegion> regionsAfter = null;
      for (int i = 0; i < 1000; i++) {
        waitForAllRegionsOnline();
        regionsAfter = getRegions();
        if (regionsAfter.size() >= regionCount) break;
        LOG.info("Expecting " + regionCount + " but only " + regionsAfter);
        Threads.sleep(10);
      }
      LOG.info("Regions of drained server: " + regionsAfter + ", all regions: " + getRegions());
      Assert.assertEquals("Test conditions are not met: regions were" +
        " created/deleted during the test. ",
        regionCount, TEST_UTIL.getMiniHBaseCluster().countServedRegions());

      // Assert the draining server still has the same regions.
      regionsAfter = drainingServer.getOnlineRegionsLocalContext();
      StringBuilder result = new StringBuilder();
      for (HRegion r: regionsAfter){
        if (!regionsBefore.contains(r)){
          result.append(r).append(" was added after the drain");
          if (regions.contains(r)){
            result.append("(existing region");
          } else {
            result.append("(new region)");
          }
          result.append("; ");
        }
      }
      for (HRegion r: regionsBefore){
        if (!regionsAfter.contains(r)){
          result.append(r).append(" was removed after the drain; ");
        }
      }
      Assert.assertTrue("Errors are: "+ result.toString(), result.length()==0);

    } finally {
      unsetDrainingServer(drainingServer);
    }
  }

  private Collection<HRegion> getRegions() {
    Collection<HRegion> regions = new ArrayList<HRegion>();
    List<RegionServerThread> rsthreads =
      TEST_UTIL.getMiniHBaseCluster().getLiveRegionServerThreads();
    for (RegionServerThread t: rsthreads) {
      HRegionServer rs = t.getRegionServer();
      Collection<HRegion> lr = rs.getOnlineRegionsLocalContext();
      LOG.info("Found " + lr + " on " + rs);
      regions.addAll(lr);
    }
    return regions;
  }

  private static void waitForAllRegionsOnline() throws Exception {
    // Wait for regions to come back on line again.
    boolean done = false;
    while (!done) {
      Thread.sleep(1);

      // Nothing in ZK RIT for a start
      ZKAssign.blockUntilNoRIT(TEST_UTIL.getZooKeeperWatcher());

      // Then we want all the regions to be marked as available...
      if (!isAllRegionsOnline()) continue;

      // And without any work in progress on the master side
      if (TEST_UTIL.getMiniHBaseCluster().getMaster().
          getAssignmentManager().getRegionStates().isRegionsInTransition()) continue;

      // nor on the region server side
      done = true;
      for (JVMClusterUtil.RegionServerThread rs :
          TEST_UTIL.getMiniHBaseCluster().getLiveRegionServerThreads()) {
        if (!rs.getRegionServer().getRegionsInTransitionInRS().isEmpty()) {
          done = false;
        }
        // Sleep some else we spam the log w/ notice that servers are not yet alive.
        Threads.sleep(10);
      }
    }
  }

  private static boolean isAllRegionsOnline() {
    return TEST_UTIL.getMiniHBaseCluster().countServedRegions() >=
        (COUNT_OF_REGIONS + 2 /*catalog and namespace regions*/);
  }
}