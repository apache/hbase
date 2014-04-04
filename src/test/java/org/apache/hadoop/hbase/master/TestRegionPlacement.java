/**
 * Copyright 2009 The Apache Software Foundation
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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.master.AssignmentPlan.POSITION;
import org.apache.hadoop.hbase.master.RegionManager.AssignmentLoadBalancer;
import org.apache.hadoop.hbase.master.RegionManager.LoadBalancer;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.TagRunner;
import org.apache.hadoop.hbase.util.Writables;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(TagRunner.class)
public class TestRegionPlacement {
  final static Log LOG = LogFactory.getLog(TestRegionPlacement.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static int SLAVES = 4;
  private final static int META_REGION_NUM = 2; // META and ROOT
  private final static int META_REGION_OVERHEAD = 1;
  private final static int ROOT_REGION_OVERHEAD = 1;
  private static HBaseAdmin admin;
  private static RegionPlacement rp;
  private static POSITION[] positions = AssignmentPlan.POSITION.values();
  private int lastRegionOpenedCount = 0;
  private int lastRegionOnPrimaryRSCount = 0;
  private int REGION_NUM = 10;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    // Enable the favored nodes based load balancer
    conf.set("hbase.loadbalancer.impl",
        "org.apache.hadoop.hbase.master.RegionManager$AssignmentLoadBalancer");

    conf.setInt("hbase.master.meta.thread.rescanfrequency", 5000);
    conf.setInt("hbase.regionserver.msginterval", 1000);
    conf.setLong("hbase.regionserver.transientAssignment.regionHoldPeriod", 2000);
    TEST_UTIL.startMiniCluster(SLAVES);
    admin = new HBaseAdmin(conf);
    rp = new RegionPlacement(conf);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @After
  public void cleanUpTables() throws IOException {
    final HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    for(HTableDescriptor htd: admin.listTables()) {
      try {
        if (!htd.isMetaTable() ) {
          admin.disableTable(htd.getName());
          admin.deleteTable(htd.getName());
        }
      } catch (IOException ioe) {
        // Ignored. so that we can try and remove all tables.
      }
    }
  }


  @Test
  public void testLoadBalancerImpl() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    HMaster m = cluster.getMaster();
    LoadBalancer loadBalancer = m.getRegionManager().getLoadBalancer();
    // Verify the master runs with the correct load balancer.
    assertTrue(loadBalancer instanceof AssignmentLoadBalancer);
  }

  /**
   * Test whether the regionservers are balanced by the number of primary
   * regions assigned. Create two tables and check whether the primaries are
   * placed like we expected
   *
   * @throws Exception
   */
  @Test(timeout = 360000)
  public void testPrimaryPlacement() throws Exception {
    // Create a table with REGION_NUM regions.
    createTable("testPrimaryPlacement", REGION_NUM);
    AssignmentPlan plan = rp.getExistingAssignmentPlan();
    Map<Integer, Integer> expected = new HashMap<Integer, Integer>();
    // we expect 2 regionservers with 2 regions and 2 with 3 regions
    expected.put(2, 2);
    expected.put(3, 2);
    assertTrue(verifyNumPrimaries(expected, plan));

    //create additional table with 5 regions
    createTable("testPrimaryPlacement2", 5);
    expected.clear();
    // after this we expect 3 regionservers with 4 regions and one with 3
    expected.put(4, 3);
    expected.put(3, 1);
    plan = rp.getExistingAssignmentPlan();
    assertTrue(verifyNumPrimaries(expected, plan));
  }

  public boolean verifyNumPrimaries(Map<Integer, Integer> expected, AssignmentPlan plan) {
    Map<HServerAddress, List<HRegionInfo>> assignment = new HashMap<HServerAddress, List<HRegionInfo>>();
    for (Entry<HRegionInfo, List<HServerAddress>> entry : plan.getAssignmentMap().entrySet()) {
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
    for (Entry<HServerAddress, List<HRegionInfo>> entry : assignment.entrySet()) {
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

  @Test(timeout = 360000)
  public void testRegionPlacement() throws Exception {
    AssignmentPlan currentPlan;

    // Reset all of the counters.
    resetLastOpenedRegionCount();

    // Create a table with REGION_NUM regions.
    createTable("testRegionAssignment", REGION_NUM);

    // Test case1: Verify the region assignment for the exiting table
    // is consistent with the assignment plan and all the region servers get
    // correctly favored nodes updated.
    // Verify all the user region are assigned. REGION_NUM regions are opened
    verifyRegionMovementNum(REGION_NUM);

    // Get the assignment plan from scanning the META table
    currentPlan = rp.getExistingAssignmentPlan();
    RegionPlacement.printAssignmentPlan(currentPlan);
    // Verify the plan from the META has covered all the user regions
    assertEquals(REGION_NUM, currentPlan.getAssignmentMap().keySet().size());

    // Verify all the user regions are assigned to the primary region server
    // based on the plan
    verifyRegionOnPrimaryRS(REGION_NUM);

    // Verify all the region server are update with the latest favored nodes
    verifyRegionServerUpdated(currentPlan);
    RegionPlacement.printAssignmentPlan(currentPlan);
    // Test Case 2: To verify whether the region placement tools can
    // correctly update the new assignment plan to META and Region Server.
    // The new assignment plan is generated by shuffle the existing assignment
    // plan by switching PRIMARY, SECONDARY and TERTIARY nodes.
    // Shuffle the plan by switching the secondary region server with
    // the tertiary.

    // Shuffle the secondary with tertiary favored nodes
    AssignmentPlan shuffledPlan = this.shuffleAssignmentPlan(currentPlan,
        AssignmentPlan.POSITION.SECONDARY, AssignmentPlan.POSITION.TERTIARY);

    // Let the region placement update the META and Region Servers
    rp.updateAssignmentPlan(shuffledPlan);

    // Verify the region assignment. There are supposed to no region reassignment
    // All the regions are still on the primary regio region server
    verifyRegionAssignment(shuffledPlan,0, REGION_NUM);

    // Shuffle the plan by switching the primary with secondary and
    // verify the region reassignment is consistent with the plan.
    shuffledPlan = this.shuffleAssignmentPlan(currentPlan,
        AssignmentPlan.POSITION.PRIMARY, AssignmentPlan.POSITION.SECONDARY);

    // Let the region placement update the META and Region Servers
    rp.updateAssignmentPlan(shuffledPlan);
    verifyRegionAssignment(shuffledPlan, REGION_NUM, REGION_NUM);

    // Test Case 3: Kill the region server with META region and verify the
    // region movements and region on primary region server are expected.
    HRegionServer meta = this.getRegionServerWithMETA();
    // Get the expected the num of the regions on the its primary region server
    Collection<HRegion> regionOnMetaRegionServer = meta.getOnlineRegions();
    int expectedRegionOnPrimaryRS =
      lastRegionOnPrimaryRSCount - regionOnMetaRegionServer.size() +
      META_REGION_OVERHEAD;
    verifyKillRegionServerWithMetaOrRoot(meta, expectedRegionOnPrimaryRS);
    RegionPlacement.printAssignmentPlan(currentPlan);
    // Test Case 4: Kill the region sever with ROOT and verify the
    // region movements and region on primary region server are expected.
    HRegionServer root = this.getRegionServerWithROOT();
    // Get the expected the num of the regions on the its primary region server
    Collection<HRegion> regionOnRootRegionServer = root.getOnlineRegions();
    expectedRegionOnPrimaryRS = lastRegionOnPrimaryRSCount -
      regionOnRootRegionServer.size() + ROOT_REGION_OVERHEAD;

    // Adjust the number by removing the regions just moved to the ROOT region server
    for (HRegion region : regionOnRootRegionServer) {
      if (regionOnMetaRegionServer.contains(region))
        expectedRegionOnPrimaryRS ++;
    }
    verifyKillRegionServerWithMetaOrRoot(root, expectedRegionOnPrimaryRS);
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
   * @throws IOException
   */
  private void verifyRegionAssignment(AssignmentPlan plan,
      int regionMovementNum, int numRegionsOnPrimaryRS)
  throws InterruptedException, IOException {
    // Verify the assignment plan in META is consistent with the expected plan.
    verifyMETAUpdated(plan);

    // Verify the number of region movement is expected
    verifyRegionMovementNum(regionMovementNum);

    // Verify the number of regions is assigned to the primary region server
    // based on the plan is expected
    verifyRegionOnPrimaryRS(numRegionsOnPrimaryRS);

    // Verify all the online region server are updated with the assignment plan
    verifyRegionServerUpdated(plan);
  }

  /**
   * Test to kill some region server and verify the number of region movement
   * and the number of region on primary region server are still expected
   * @param server
   * @throws InterruptedException
   * @throws IOException
   */
  private void verifyKillRegionServerWithMetaOrRoot(HRegionServer server,
      int expectedRegionOnPrimary)
  throws InterruptedException, IOException{
    assertNotNull(server);

    // Verify this region server with META is also hosting user regions
    int expectedRegionMovement = server.getOnlineRegions().size();
    assertTrue("All the user regions are assigned to this region server: " +
        server.getServerInfo().getHostnamePort(),
        (expectedRegionMovement < REGION_NUM));
    assertTrue("NO the user region is assigned to this region server: " +
        server.getServerInfo().getHostnamePort(),
        (expectedRegionMovement > 1) );

    // Kill the region server;
    server.kill();

    // Verify the user regions previously on the killed rs are reassigned.
    verifyRegionMovementNum(expectedRegionMovement);

    // Verify only expectedRegionOnPrimary of the user regions are assigned
    // to the primary region server based on the plan.
    verifyRegionOnPrimaryRS(expectedRegionOnPrimary);
  }

  /**
   * Get the region server who is currently hosting ROOT
   * @return
   */
  private HRegionServer getRegionServerWithROOT() {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    HMaster master = cluster.getMaster();
    for (int i = 0; i < SLAVES; i++) {
      HRegionServer rs = cluster.getRegionServer(i);
      HServerAddress addr = rs.getServerInfo().getServerAddress();
      if (master.getRegionManager().isRootServer(addr)) {
        return rs;
      }
    }
    return null;
  }

  /**
   * Get the region server who is currently hosting META
   * @return
   */
  private HRegionServer getRegionServerWithMETA() {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    HMaster master = cluster.getMaster();
    for (int i = 0; i < SLAVES; i++) {
      HRegionServer rs = cluster.getRegionServer(i);
      HServerAddress addr = rs.getServerInfo().getServerAddress();
      if (master.getRegionManager().isMetaServer(addr)) {
        return rs;
      }
    }
    return null;
  }
  /**
   * Verify the number of region movement is expected
   * @param expected
   * @throws InterruptedException
   */
  private void verifyRegionMovementNum(int expected)
  throws InterruptedException {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    HMaster m = cluster.getMaster();

    int retry = 10;
    long sleep = 3 * TEST_UTIL.getConfiguration().
      getInt("hbase.regionserver.msginterval", 1000);
    int attempt = 0;
    int currentRegionOpened, regionMovement;
    do {
      currentRegionOpened = m.getMetrics().getRegionsOpened();
      regionMovement= currentRegionOpened - lastRegionOpenedCount;
      LOG.debug("There are " + regionMovement + "/" + expected +
          " regions moved after " + attempt + " attempts");
      Thread.sleep((++attempt) * sleep);
    } while (regionMovement < expected && attempt <= retry);

    // update the lastRegionOpenedCount
    resetLastOpenedRegionCount(currentRegionOpened);

    assertEquals("There are only " + regionMovement + " instead of "
          + expected + " region movement for " + attempt + " attempts",
          regionMovement, expected);
  }

  private void resetLastOpenedRegionCount() {
    resetLastOpenedRegionCount(TEST_UTIL.getHBaseCluster().getMaster().getMetrics().getRegionsOpened());
  }

  private void resetLastOpenedRegionCount(int newCount) {
    this.lastRegionOpenedCount = newCount;
  }

  /**
   * Shuffle the assignment plan by switching two favored node positions.
   * @param plan The assignment plan
   * @param p1 The first switch position
   * @param p2 The second switch position
   * @return
   */
  private AssignmentPlan shuffleAssignmentPlan(AssignmentPlan plan,
      AssignmentPlan.POSITION p1, AssignmentPlan.POSITION p2) {
    AssignmentPlan shuffledPlan = new AssignmentPlan();

    for (Map.Entry<HRegionInfo, List<HServerAddress>> entry :
      plan.getAssignmentMap().entrySet()) {
      HRegionInfo region = entry.getKey();

      // copy the server list from the original plan
      List<HServerAddress> shuffledServerList = new ArrayList<HServerAddress>();
      shuffledServerList.addAll(entry.getValue());

      // start to shuffle
      shuffledServerList.set(p1.ordinal(), entry.getValue().get(p2.ordinal()));
      shuffledServerList.set(p2.ordinal(), entry.getValue().get(p1.ordinal()));

      // update the plan
      shuffledPlan.updateAssignmentPlan(region, shuffledServerList);
    }
    return shuffledPlan;
  }

  /**
   * Verify the number of user regions is assigned to the primary
   * region server based on the plan is expected
   * @param expectedNum
   * @throws IOException
   */
  private void verifyRegionOnPrimaryRS(int expectedNum)
  throws IOException {
    this.lastRegionOnPrimaryRSCount = getNumRegionisOnPrimaryRS();
    assertEquals("Only " +  expectedNum + " of user regions running " +
        "on the primary region server", expectedNum ,
        lastRegionOnPrimaryRSCount);
  }

  /**
   * Verify the meta has updated to the latest assignment plan
   * @param expectedPlan
   * @throws IOException
   */
  private void verifyMETAUpdated(AssignmentPlan expectedPlan)
  throws IOException {
    AssignmentPlan planFromMETA = rp.getExistingAssignmentPlan();
    assertTrue("The assignment plan is NOT consistent with the expected plan ",
        planFromMETA.equals(expectedPlan));
  }

  /**
   * Verify all the online region servers has been updated to the
   * latest assignment plan
   * @param plan
   * @throws IOException
   */
  private void verifyRegionServerUpdated(AssignmentPlan plan) throws IOException {
    // Verify all region servers contain the correct favored nodes information
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    for (int i = 0; i < SLAVES; i++) {
      HRegionServer rs = cluster.getRegionServer(i);
      for (HRegion region: rs.getOnlineRegions()) {
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
              (desc.isRootRegion() || desc.isMetaRegion()));
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
   * @return the number of regions are assigned to the primary region server
   * @throws IOException
   */
  private int getNumRegionisOnPrimaryRS() throws IOException {
    final AtomicInteger regionOnPrimaryNum = new AtomicInteger(0);
    final AtomicInteger totalRegionNum = new AtomicInteger(0);
    LOG.info("The start of region placement verification");
    MetaScannerVisitor visitor = new MetaScannerVisitor() {
      @Override
      public boolean processRow(Result result) throws IOException {
        try {
          byte[] regionInfo = result.getValue(HConstants.CATALOG_FAMILY,
              HConstants.REGIONINFO_QUALIFIER);
          byte[] server = result.getValue(HConstants.CATALOG_FAMILY,
              HConstants.SERVER_QUALIFIER);
          byte[] favoredNodes = result.getValue(HConstants.CATALOG_FAMILY,
              "favorednodes".getBytes());
          POSITION[] positions = AssignmentPlan.POSITION.values();
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
    MetaScanner.metaScan(TEST_UTIL.getConfiguration(), visitor);
    LOG.info("There are " + regionOnPrimaryNum.intValue() + " out of " +
        totalRegionNum.intValue() + " regions running on the primary" +
            " region servers" );
    return regionOnPrimaryNum.intValue() ;
  }

  /**
   * Create a table with specified table name and region number.
   * @param table
   * @param regionNum
   * @return
   * @throws IOException
   */
   private static void createTable(String table, int regionNum)
   throws IOException {
    byte[] tableName = Bytes.toBytes(table);
    int expectedRegions = regionNum;
    byte[][] splitKeys = new byte[expectedRegions - 1][];
    for (int i = 1; i < expectedRegions; i++) {
      byte splitKey = (byte) i;
      splitKeys[i - 1] = new byte[] { splitKey, splitKey, splitKey };
    }

    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    admin.createTable(desc, splitKeys);

    HTable ht = new HTable(TEST_UTIL.getConfiguration(), tableName);
    Map<HRegionInfo, HServerAddress> regions = ht.getRegionsInfo();
    assertEquals("Tried to create " + expectedRegions + " regions "
        + "but only found " + regions.size(), expectedRegions, regions.size());
   }

   /**
    * Used to test the correctness of this class.
    */
   @Test
   public void testRandomizedMatrix() {
     int rows = 100;
     int cols = 100;
     float[][] matrix = new float[rows][cols];
     Random random = new Random();
     for (int i = 0; i < rows; i++) {
       for (int j = 0; j < cols; j++) {
         matrix[i][j] = random.nextFloat();
       }
     }

     // Test that inverting a transformed matrix gives the original matrix.
     RegionPlacement.RandomizedMatrix rm =
       new RegionPlacement.RandomizedMatrix(rows, cols);
     float[][] transformed = rm.transform(matrix);
     float[][] invertedTransformed = rm.invert(transformed);
     for (int i = 0; i < rows; i++) {
       for (int j = 0; j < cols; j++) {
         if (matrix[i][j] != invertedTransformed[i][j]) {
           throw new RuntimeException();
         }
       }
     }

     // Test that the indices on a transformed matrix can be inverted to give
     // the same values on the original matrix.
     int[] transformedIndices = new int[rows];
     for (int i = 0; i < rows; i++) {
       transformedIndices[i] = random.nextInt(cols);
     }
     int[] invertedTransformedIndices = rm.invertIndices(transformedIndices);
     float[] transformedValues = new float[rows];
     float[] invertedTransformedValues = new float[rows];
     for (int i = 0; i < rows; i++) {
       transformedValues[i] = transformed[i][transformedIndices[i]];
       invertedTransformedValues[i] = matrix[i][invertedTransformedIndices[i]];
     }
     Arrays.sort(transformedValues);
     Arrays.sort(invertedTransformedValues);
     if (!Arrays.equals(transformedValues, invertedTransformedValues)) {
       throw new RuntimeException();
     }
   }

  /**
   * Download current assignment plan, serialize it to json and deserialize the json.
   * The two plans should be identical.
   */
  @Test
  public void testJsonToAP() {
    try {
      createTable("testJsonAssignmentPlan", 3);

      AssignmentPlan currentPlan = rp.getExistingAssignmentPlan();
      RegionPlacement.printAssignmentPlan(currentPlan);
      AssignmentPlanData data = AssignmentPlanData.constructFromAssignmentPlan(currentPlan);

      String jsonStr = new ObjectMapper().defaultPrettyPrintingWriter().writeValueAsString(data);
      LOG.info("Json version of current assignment plan: " + jsonStr);
      AssignmentPlan loadedPlan = rp.loadPlansFromJson(jsonStr);
      RegionPlacement.printAssignmentPlan(loadedPlan);
      assertEquals("Loaded plan should be the same with current plan", currentPlan, loadedPlan);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException();
    }
  }
}

