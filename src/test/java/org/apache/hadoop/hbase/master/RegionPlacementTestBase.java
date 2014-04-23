/**
 * Copyright The Apache Software Foundation
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class RegionPlacementTestBase {
  protected final static RegionMovementTestHelper TEST_UTIL = new RegionMovementTestHelper();
  protected final static int META_REGION_OVERHEAD = 1;
  protected final static int ROOT_REGION_OVERHEAD = 1;
  final static Log LOG = LogFactory.getLog(RegionPlacementTestBase.class);
  protected static HBaseAdmin admin;
  protected static RegionPlacement rp;
  private static AssignmentPlan.POSITION[] positions = AssignmentPlan.POSITION.values();
  private static int sleepTime = 10;

  protected int lastRegionOnPrimaryRSCount = 0;
  protected int REGION_NUM = 10;


  protected static void setUpCluster(int numSlaves) throws IOException, InterruptedException {
    Configuration conf = TEST_UTIL.getConfiguration();
    // Enable the favored nodes based load balancer
    conf.set("hbase.loadbalancer.impl",
        "org.apache.hadoop.hbase.master.RegionManager$AssignmentLoadBalancer");

    conf.setInt("hbase.regionserver.msginterval", 1000);
    // Make sure lots of regions can be opening on a
    conf.setInt("hbase.regions.nobalancing.count", 20);
    // Allow lots of regions to get assigned at a time
    conf.setInt("hbase.regions.percheckin", 20);
    TEST_UTIL.startMiniCluster(numSlaves);
    sleepTime = 3 * TEST_UTIL.getConfiguration().
        getInt("hbase.regionserver.msginterval", 1000);
    admin = TEST_UTIL.getHBaseAdmin();
    rp = new RegionPlacement(conf);
  }

  /**
   * To verify the region assignment status.
   * It will check the assignment plan consistency between META and
   * region servers.
   * Also it will verify weather the number of region movement and
   * the number regions on the primary region server are expected
   *
   * @param plan
   * @param regionMovementNum
   * @param numRegionsOnPrimaryRS
   * @throws InterruptedException
   * @throws java.io.IOException
   */
  protected void verifyRegionAssignment(AssignmentPlan plan,
      int regionMovementNum, int numRegionsOnPrimaryRS)
      throws InterruptedException, IOException {
    // Verify the assignment plan in META is consistent with the expected plan.
    verifyMETAUpdated(plan);

    // Verify the number of region movement is expected
    TEST_UTIL.verifyRegionMovementNum(regionMovementNum);

    // Verify the number of regions is assigned to the primary region server
    // based on the plan is expected
    verifyRegionOnPrimaryRS(numRegionsOnPrimaryRS);

    // Verify all the online region server are updated with the assignment plan
    verifyRegionServerUpdated(plan);
  }

  /**
   * Test to kill some region server and verify the number of region movement
   * and the number of region on primary region server are still expected
   *
   * @param server
   * @throws InterruptedException
   * @throws java.io.IOException
   */
  protected void verifyKillRegionServerWithMetaOrRoot(HRegionServer server,
      int expectedRegionOnPrimary)
      throws InterruptedException, IOException {
    assertNotNull(server);

    // Verify this region server with META is also hosting user regions
    int expectedRegionMovement = server.getOnlineRegions().size();
    assertTrue("All the user regions are assigned to this region server: " +
            server.getServerInfo().getHostnamePort(),
        (expectedRegionMovement < REGION_NUM)
    );
    assertTrue("No user region is assigned to this region server: " +
            server.getServerInfo().getHostnamePort(),
        (expectedRegionMovement > 1)
    );

    // Kill the region server;
    server.kill();

    // Verify the user regions previously on the killed rs are reassigned.
    TEST_UTIL.verifyRegionMovementNum(expectedRegionMovement);

    // Verify only expectedRegionOnPrimary of the user regions are assigned
    // to the primary region server based on the plan.
    verifyRegionOnPrimaryRS(expectedRegionOnPrimary);
  }

  /**
   * Get the region server who is currently hosting ROOT
   *
   * @return
   */
  protected HRegionServer getRegionServerWithROOT() throws IOException {
    return getRegionServerWithRootOrMeta(false /* getMeta */, true /* getRoot */);
  }

  /**
   * Get the region server who is currently hosting META
   *
   * @return
   */
  protected HRegionServer getRegionServerWithMETA() throws IOException {
    return getRegionServerWithRootOrMeta(true /* getMeta */, false /* getRoot */);
  }


  protected HRegionServer getRegionServerWithRootOrMeta(boolean getMeta, boolean getRoot)
      throws IOException {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    HMaster master = cluster.getMaster();
    for (int i = 0; i < cluster.getRegionServers().size(); i++) {
      HRegionServer rs = cluster.getRegionServer(i);
      HServerAddress addr = rs.getServerInfo().getServerAddress();
      if (getMeta && master.getRegionManager().isMetaServer(addr)) {
        return rs;
      }
      if (getRoot && master.getRegionManager().isRootServer(addr)) {
        return rs;
      }

    }
    return null;
  }

  protected void resetLastRegionOnPrimary() throws IOException {
    this.lastRegionOnPrimaryRSCount = getNumRegionisOnPrimaryRS();
  }




  /**
   * Shuffle the assignment plan by switching two favored node positions.
   *
   * @param plan The assignment plan
   * @param p1   The first switch position
   * @param p2   The second switch position
   * @return
   */
  protected AssignmentPlan shuffleAssignmentPlan(AssignmentPlan plan,
      AssignmentPlan.POSITION p1, AssignmentPlan.POSITION p2) {
    AssignmentPlan shuffledPlan = new AssignmentPlan();

    for (Map.Entry<HRegionInfo, List<HServerAddress>> entry :
        plan.getAssignmentMap().entrySet()) {
      HRegionInfo region = entry.getKey();
      final List<HServerAddress> originalServersList = entry.getValue();

      // copy the server list from the original plan
      List<HServerAddress> shuffledServerList = new ArrayList<HServerAddress>();
      shuffledServerList.addAll(originalServersList);

      // start to shuffle
      shuffledServerList.set(p1.ordinal(), originalServersList.get(p2.ordinal()));
      shuffledServerList.set(p2.ordinal(), originalServersList.get(p1.ordinal()));

      // update the plan
      shuffledPlan.updateAssignmentPlan(region, shuffledServerList);
    }
    return shuffledPlan;
  }

  /**
   * Verify the number of user regions is assigned to the primary
   * region server based on the plan is expected
   *
   * @param expectedNum
   * @throws java.io.IOException
   */
  protected void verifyRegionOnPrimaryRS(int expectedNum)
      throws IOException {
    resetLastRegionOnPrimary();
    assertEquals("Only " + expectedNum + " of user regions running " +
            "on the primary region server", expectedNum,
        lastRegionOnPrimaryRSCount
    );
  }

  /**
   * Verify the meta has updated to the latest assignment plan
   *
   * @param expectedPlan
   * @throws java.io.IOException
   */
  private void verifyMETAUpdated(AssignmentPlan expectedPlan)
      throws IOException, InterruptedException {

    AssignmentPlan planFromMETA = null;
    int retries = 0;
    while ((planFromMETA == null
        || planFromMETA.getAssignmentMap().size() != expectedPlan.getAssignmentMap().size())
        && retries < 10) {
      planFromMETA = rp.getExistingAssignmentPlan();
      Thread.sleep((++retries) * sleepTime);
    }
    RegionPlacement.printAssignmentPlan(expectedPlan);
    RegionPlacement.printAssignmentPlan(planFromMETA);
    assertTrue("The assignment plan is NOT consistent with the expected plan ",
        planFromMETA.equals(expectedPlan));
  }

  /**
   * Verify all the online region servers has been updated to the
   * latest assignment plan
   *
   * @param plan
   * @throws java.io.IOException
   */
  protected void verifyRegionServerUpdated(AssignmentPlan plan) throws IOException {
    // Verify all region servers contain the correct favored nodes information
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    for (int i = 0; i < cluster.getRegionServers().size(); i++) {
      HRegionServer rs = cluster.getRegionServer(i);
      for (HRegion region : rs.getOnlineRegions()) {
        InetSocketAddress[] favoredSockedAddress = region.getFavoredNodes();
        List<HServerAddress> favoredServerList =
            plan.getAssignment(region.getRegionInfo());

        // All regions are supposed to have favored nodes,
        // except for META and ROOT
        if (favoredServerList == null) {
          HTableDescriptor desc = region.getTableDesc();
          // Verify they are ROOT and META regions since no favored nodes
          assertNull(favoredSockedAddress);
          assertTrue("User region " +
                  region.getTableDesc().getNameAsString() +
                  " should have favored nodes",
              (desc.isRootRegion() || desc.isMetaRegion())
          );
        } else {
          // For user region, the favored nodes in the region server should be
          // identical to favored nodes in the assignmentPlan
          assertTrue(favoredSockedAddress.length == favoredServerList.size());
          assertTrue(favoredServerList.size() > 0);
          for (int j = 0; j < favoredServerList.size(); j++) {
            InetSocketAddress addrFromRS = favoredSockedAddress[j];
            InetSocketAddress addrFromPlan =
                favoredServerList.get(j).getInetSocketAddress();

            assertNotNull(addrFromRS);
            assertNotNull(addrFromPlan);
            assertTrue("Region server " + rs.getHServerInfo().getHostnamePort()
                + " has the " + positions[j] +
                " for region " + region.getRegionNameAsString() + " is " +
                addrFromRS + " which is inconsistent with the plan "
                + addrFromPlan, addrFromRS.equals(addrFromPlan));
          }
        }
      }
    }
  }

  /**
   * Check whether regions are assigned to servers consistent with the explicit
   * hints that are persisted in the META table.
   * Also keep track of the number of the regions are assigned to the
   * primary region server.
   *
   * @return the number of regions are assigned to the primary region server
   * @throws java.io.IOException
   */
  protected int getNumRegionisOnPrimaryRS() throws IOException {
    final AtomicInteger regionOnPrimaryNum = new AtomicInteger(0);
    final AtomicInteger totalRegionNum = new AtomicInteger(0);
    LOG.info("The start of region placement verification");
    org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor
        visitor = new org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor() {
      @Override
      public boolean processRow(Result result) throws IOException {
        try {
          byte[] regionInfo = result.getValue(HConstants.CATALOG_FAMILY,
              HConstants.REGIONINFO_QUALIFIER);
          byte[] server = result.getValue(HConstants.CATALOG_FAMILY,
              HConstants.SERVER_QUALIFIER);
          byte[] favoredNodes = result.getValue(HConstants.CATALOG_FAMILY,
              "favorednodes".getBytes());
          AssignmentPlan.POSITION[] positions = AssignmentPlan.POSITION.values();
          if (regionInfo != null) {
            HRegionInfo info = Writables.getHRegionInfo(regionInfo);
            totalRegionNum.incrementAndGet();
            if (server != null) {
              String serverString = new String(server);
              if (favoredNodes != null) {
                String[] splits = new String(favoredNodes).split(",");
                String placement = "[NOT FAVORED NODE]";
                for (int i = 0; i < splits.length; i++) {
                  if (splits[i].equals(serverString)) {
                    placement = positions[i].toString();
                    if (i == AssignmentPlan.POSITION.PRIMARY.ordinal()) {
                      regionOnPrimaryNum.incrementAndGet();
                    }
                    break;
                  }
                }
                LOG.info(info.getRegionNameAsString() + " on " +
                    serverString + " " + placement);
              } else {
                LOG.info(info.getRegionNameAsString() + " running on " +
                    serverString + " but there is no favored region server");
              }
            } else {
              LOG.info(info.getRegionNameAsString() +
                  " not assigned to any server");
            }
          }
          return true;
        } catch (RuntimeException e) {
          LOG.error("Result=" + result);
          throw e;
        }
      }
    };
    org.apache.hadoop.hbase.client.MetaScanner.metaScan(TEST_UTIL.getConfiguration(), visitor);
    LOG.info("There are " + regionOnPrimaryNum.intValue() + " out of " +
        totalRegionNum.intValue() + " regions running on the primary" +
        " region servers");
    return regionOnPrimaryNum.intValue();
  }

  protected boolean verifyNumPrimaries(Map<Integer, Integer> expected, AssignmentPlan plan) {
    Map<HServerAddress, List<HRegionInfo>> assignment =
        new HashMap<HServerAddress, List<HRegionInfo>>();
    for (Map.Entry<HRegionInfo, List<HServerAddress>> entry : plan.getAssignmentMap().entrySet()) {
      HServerAddress primary = entry.getValue().get(0);
      List<HRegionInfo> regions = assignment.get(primary);
      if (regions == null) {
        regions = new ArrayList<HRegionInfo>();
        assignment.put(primary, regions);
      }
      regions.add(entry.getKey());
    }
    // see how many servers are with a specific number of regions
    Map<Integer, Integer> rswithNumRegions = new HashMap<Integer, Integer>();
    for (Map.Entry<HServerAddress, List<HRegionInfo>> entry : assignment.entrySet()) {
      Integer numRegions = entry.getValue().size();
      Integer numServers = rswithNumRegions.get(numRegions);
      if (numServers == null) {
        numServers = 1;
      } else {
        numServers++;
      }
      rswithNumRegions.put(numRegions, numServers);
    }
    return expected.equals(rswithNumRegions);
  }

  protected void assertPinned(String tableName, MiniHBaseCluster cluster,
      Set<HServerAddress> servers, HRegionServer unusedServer) throws IOException {
    // The un-used server should have no regions from testPinnedTable
    for (HRegion hr : unusedServer.getOnlineRegions()) {
      assertTrue("The Un-used server should not have a user table on it",
          !hr.getTableDesc().getNameAsString().equals(tableName));
    }

    // all of the regions of testPinnedTable are on the pinned servers
    int onPinned = 0;
    for (int i = 0; i < cluster.getRegionServers().size(); i++) {
      HRegionServer hrs = cluster.getRegionServer(i);
      for (HRegion hRegion : hrs.getOnlineRegions()) {
        if (hRegion.getTableDesc().getNameAsString().equals(tableName)) {
          assertTrue("Region " + hRegion.toString() + " should be on one of the pinned servers",
              servers.contains(hrs.getServerInfo().getServerAddress()));
          onPinned += 1;
        }
      }
    }
    assertEquals("Expecting " + REGION_NUM + " regions on pinned servers", REGION_NUM, onPinned);
  }

  protected void cleanUp() throws IOException, InterruptedException {
    final HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    for (HTableDescriptor htd : admin.listTables()) {
      try {
        if (!htd.isMetaTable()) {
          admin.disableTable(htd.getName());
          admin.deleteTable(htd.getName());
        }
      } catch (IOException ioe) {
        // Ignored. so that we can try and remove all tables.
      }
    }

    TEST_UTIL.waitOnStableRegionMovement();
    TEST_UTIL.resetLastOpenedRegionCount();
    resetLastRegionOnPrimary();
    TEST_UTIL.waitOnStableRegionMovement();
  }
}
