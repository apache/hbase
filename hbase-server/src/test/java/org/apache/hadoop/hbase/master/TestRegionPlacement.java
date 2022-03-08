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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.favored.FavoredNodeAssignmentHelper;
import org.apache.hadoop.hbase.favored.FavoredNodeLoadBalancer;
import org.apache.hadoop.hbase.favored.FavoredNodesPlan;
import org.apache.hadoop.hbase.favored.FavoredNodesPlan.Position;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MasterTests.class, MediumTests.class})
public class TestRegionPlacement {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionPlacement.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRegionPlacement.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static int SLAVES = 10;
  private static Connection CONNECTION;
  private static Admin admin;
  private static RegionPlacementMaintainer rp;
  private static Position[] positions = Position.values();
  private int lastRegionOnPrimaryRSCount = 0;
  private int REGION_NUM = 10;
  private Map<RegionInfo, ServerName[]> favoredNodesAssignmentPlan = new HashMap<>();

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    // Enable the favored nodes based load balancer
    conf.setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
        FavoredNodeLoadBalancer.class, LoadBalancer.class);
    conf.setBoolean("hbase.tests.use.shortcircuit.reads", false);
    TEST_UTIL.startMiniCluster(SLAVES);
    CONNECTION = TEST_UTIL.getConnection();
    admin = CONNECTION.getAdmin();
    rp = new RegionPlacementMaintainer(conf);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Ignore ("Test for unfinished feature") @Test
  public void testRegionPlacement() throws Exception {
    String tableStr = "testRegionAssignment";
    TableName table = TableName.valueOf(tableStr);
    // Create a table with REGION_NUM regions.
    createTable(table, REGION_NUM);

    TEST_UTIL.waitTableAvailable(table);

    // Verify all the user regions are assigned to the primary region server
    // based on the plan
    verifyRegionOnPrimaryRS(REGION_NUM);

    FavoredNodesPlan currentPlan = rp.getRegionAssignmentSnapshot().getExistingAssignmentPlan();
    // Verify all the region server are update with the latest favored nodes
    verifyRegionServerUpdated(currentPlan);
    // Test Case 2: To verify whether the region placement tools can
    // correctly update the new assignment plan to hbase:meta and Region Server.
    // The new assignment plan is generated by shuffle the existing assignment
    // plan by switching PRIMARY, SECONDARY and TERTIARY nodes.
    // Shuffle the plan by switching the secondary region server with
    // the tertiary.

    // Shuffle the secondary with tertiary favored nodes
    FavoredNodesPlan shuffledPlan = this.shuffleAssignmentPlan(currentPlan,
        FavoredNodesPlan.Position.SECONDARY, FavoredNodesPlan.Position.TERTIARY);
    // Let the region placement update the hbase:meta and Region Servers
    rp.updateAssignmentPlan(shuffledPlan);

    // Verify the region assignment. There are supposed to no region reassignment
    // All the regions are still on the primary region server
    verifyRegionAssignment(shuffledPlan,0, REGION_NUM);

    // Shuffle the plan by switching the primary with secondary and
    // verify the region reassignment is consistent with the plan.
    shuffledPlan = this.shuffleAssignmentPlan(currentPlan,
        FavoredNodesPlan.Position.PRIMARY, FavoredNodesPlan.Position.SECONDARY);

    // Let the region placement update the hbase:meta and Region Servers
    rp.updateAssignmentPlan(shuffledPlan);

    verifyRegionAssignment(shuffledPlan, REGION_NUM, REGION_NUM);

    // also verify that the AssignmentVerificationReport has the correct information
    RegionPlacementMaintainer rp = new RegionPlacementMaintainer(TEST_UTIL.getConfiguration());
    // we are interested in only one table (and hence one report)
    rp.setTargetTableName(new String[]{tableStr});
    List<AssignmentVerificationReport> reports = rp.verifyRegionPlacement(false);
    AssignmentVerificationReport report = reports.get(0);
    assertTrue(report.getRegionsWithoutValidFavoredNodes().isEmpty());
    assertTrue(report.getNonFavoredAssignedRegions().isEmpty());
    assertTrue(report.getTotalFavoredAssignments() >= REGION_NUM);
    assertTrue(report.getNumRegionsOnFavoredNodeByPosition(FavoredNodesPlan.Position.PRIMARY) != 0);
    assertTrue(report.getNumRegionsOnFavoredNodeByPosition(FavoredNodesPlan.Position.SECONDARY) == 0);
    assertTrue(report.getNumRegionsOnFavoredNodeByPosition(FavoredNodesPlan.Position.TERTIARY) == 0);
    assertTrue(report.getUnassignedRegions().isEmpty());

    // Check when a RS stops, the regions get assigned to their secondary/tertiary
    killRandomServerAndVerifyAssignment();

    // also verify that the AssignmentVerificationReport has the correct information
    reports = rp.verifyRegionPlacement(false);
    report = reports.get(0);
    assertTrue(report.getRegionsWithoutValidFavoredNodes().isEmpty());
    assertTrue(report.getNonFavoredAssignedRegions().isEmpty());
    assertTrue(report.getTotalFavoredAssignments() >= REGION_NUM);
    assertTrue(report.getNumRegionsOnFavoredNodeByPosition(FavoredNodesPlan.Position.PRIMARY) > 0);
    assertTrue("secondary " +
    report.getNumRegionsOnFavoredNodeByPosition(FavoredNodesPlan.Position.SECONDARY) + " tertiary "
        + report.getNumRegionsOnFavoredNodeByPosition(FavoredNodesPlan.Position.TERTIARY),
        (report.getNumRegionsOnFavoredNodeByPosition(FavoredNodesPlan.Position.SECONDARY) > 0
        || report.getNumRegionsOnFavoredNodeByPosition(FavoredNodesPlan.Position.TERTIARY) > 0));
    assertTrue((report.getNumRegionsOnFavoredNodeByPosition(FavoredNodesPlan.Position.PRIMARY) +
        report.getNumRegionsOnFavoredNodeByPosition(FavoredNodesPlan.Position.SECONDARY) +
        report.getNumRegionsOnFavoredNodeByPosition(FavoredNodesPlan.Position.TERTIARY)) == REGION_NUM);
    RegionPlacementMaintainer.printAssignmentPlan(currentPlan);
  }

  private void killRandomServerAndVerifyAssignment()
      throws IOException, InterruptedException, KeeperException {
    ServerName serverToKill = null;
    int killIndex = 0;
    Random rand = ThreadLocalRandom.current();
    ServerName metaServer = TEST_UTIL.getHBaseCluster().getServerHoldingMeta();
    LOG.debug("Server holding meta " + metaServer);
    boolean isNamespaceServer = false;
    do {
      // kill a random non-meta server carrying at least one region
      killIndex = rand.nextInt(SLAVES);
      serverToKill = TEST_UTIL.getHBaseCluster().getRegionServer(killIndex).getServerName();
      Collection<HRegion> regs =
          TEST_UTIL.getHBaseCluster().getRegionServer(killIndex).getOnlineRegionsLocalContext();
      isNamespaceServer = false;
      for (HRegion r : regs) {
        if (r.getRegionInfo().getTable().getNamespaceAsString()
            .equals(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR)) {
          isNamespaceServer = true;
          break;
        }
      }
    } while (ServerName.isSameAddress(metaServer, serverToKill) || isNamespaceServer ||
        TEST_UTIL.getHBaseCluster().getRegionServer(killIndex).getNumberOfOnlineRegions() == 0);
    LOG.debug("Stopping RS " + serverToKill);
    Map<RegionInfo, Pair<ServerName, ServerName>> regionsToVerify = new HashMap<>();
    // mark the regions to track
    for (Map.Entry<RegionInfo, ServerName[]> entry : favoredNodesAssignmentPlan.entrySet()) {
      ServerName s = entry.getValue()[0];
      if (ServerName.isSameAddress(s, serverToKill)) {
        regionsToVerify.put(entry.getKey(), new Pair<>(entry.getValue()[1], entry.getValue()[2]));
        LOG.debug("Adding " + entry.getKey() + " with sedcondary/tertiary " +
            entry.getValue()[1] + " " + entry.getValue()[2]);
      }
    }
    int orig = TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager().getNumRegionsOpened();
    TEST_UTIL.getHBaseCluster().stopRegionServer(serverToKill);
    TEST_UTIL.getHBaseCluster().waitForRegionServerToStop(serverToKill, 60000);
    int curr = TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager().getNumRegionsOpened();
    while (curr - orig < regionsToVerify.size()) {
      LOG.debug("Waiting for " + regionsToVerify.size() + " to come online " +
          " Current #regions " + curr + " Original #regions " + orig);
      Thread.sleep(200);
      curr = TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager().getNumRegionsOpened();
    }
    // now verify
    for (Map.Entry<RegionInfo, Pair<ServerName, ServerName>> entry : regionsToVerify.entrySet()) {
      ServerName newDestination = TEST_UTIL.getHBaseCluster().getMaster()
          .getAssignmentManager().getRegionStates().getRegionServerOfRegion(entry.getKey());
      Pair<ServerName, ServerName> secondaryTertiaryServers = entry.getValue();
      LOG.debug("New destination for region " + entry.getKey().getEncodedName() +
          " " + newDestination +". Secondary/Tertiary are " + secondaryTertiaryServers.getFirst()
          + "/" + secondaryTertiaryServers.getSecond());
      if (!(ServerName.isSameAddress(newDestination, secondaryTertiaryServers.getFirst())||
          ServerName.isSameAddress(newDestination, secondaryTertiaryServers.getSecond()))){
        fail("Region " + entry.getKey() + " not present on any of the expected servers");
      }
    }
    // start(reinstate) region server since we killed one before
    TEST_UTIL.getHBaseCluster().startRegionServer();
  }

  /**
   * Used to test the correctness of this class.
   */
  @Ignore ("Test for unfinished feature") @Test
  public void testRandomizedMatrix() {
    int rows = 100;
    int cols = 100;
    float[][] matrix = new float[rows][cols];
    Random rand = ThreadLocalRandom.current();
    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < cols; j++) {
        matrix[i][j] = rand.nextFloat();
      }
    }

    // Test that inverting a transformed matrix gives the original matrix.
    RegionPlacementMaintainer.RandomizedMatrix rm =
      new RegionPlacementMaintainer.RandomizedMatrix(rows, cols);
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
      transformedIndices[i] = rand.nextInt(cols);
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
   * Shuffle the assignment plan by switching two favored node positions.
   * @param plan The assignment plan
   * @param p1 The first switch position
   * @param p2 The second switch position
   * @return the shuffled assignment plan
   */
  private FavoredNodesPlan shuffleAssignmentPlan(FavoredNodesPlan plan,
      FavoredNodesPlan.Position p1, FavoredNodesPlan.Position p2) throws IOException {
    FavoredNodesPlan shuffledPlan = new FavoredNodesPlan();

    Map<String, RegionInfo> regionToHRegion =
        rp.getRegionAssignmentSnapshot().getRegionNameToRegionInfoMap();
    for (Map.Entry<String, List<ServerName>> entry :
      plan.getAssignmentMap().entrySet()) {

      // copy the server list from the original plan
      List<ServerName> shuffledServerList = new ArrayList<>();
      shuffledServerList.addAll(entry.getValue());

      // start to shuffle
      shuffledServerList.set(p1.ordinal(), entry.getValue().get(p2.ordinal()));
      shuffledServerList.set(p2.ordinal(), entry.getValue().get(p1.ordinal()));

      // update the plan
      shuffledPlan.updateFavoredNodesMap(regionToHRegion.get(entry.getKey()), shuffledServerList);
    }
    return shuffledPlan;
  }

  /**
   * To verify the region assignment status.
   * It will check the assignment plan consistency between hbase:meta and
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
  private void verifyRegionAssignment(FavoredNodesPlan plan,
      int regionMovementNum, int numRegionsOnPrimaryRS)
  throws InterruptedException, IOException {
    // Verify the assignment plan in hbase:meta is consistent with the expected plan.
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
   * Verify the meta has updated to the latest assignment plan
   * @param expectedPlan the region assignment plan
   * @throws IOException if an IO problem is encountered
   */
  private void verifyMETAUpdated(FavoredNodesPlan expectedPlan)
  throws IOException {
    FavoredNodesPlan planFromMETA = rp.getRegionAssignmentSnapshot().getExistingAssignmentPlan();
    assertTrue("The assignment plan is NOT consistent with the expected plan ",
        planFromMETA.equals(expectedPlan));
  }

  /**
   * Verify the number of region movement is expected
   */
  private void verifyRegionMovementNum(int expected)
      throws InterruptedException, IOException {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    HMaster m = cluster.getMaster();
    int lastRegionOpenedCount = m.getAssignmentManager().getNumRegionsOpened();
    // get the assignments start to execute
    m.balance();

    int retry = 10;
    long sleep = 3000;
    int attempt = 0;
    int currentRegionOpened, regionMovement;
    do {
      currentRegionOpened = m.getAssignmentManager().getNumRegionsOpened();
      regionMovement= currentRegionOpened - lastRegionOpenedCount;
      LOG.debug("There are " + regionMovement + "/" + expected +
          " regions moved after " + attempt + " attempts");
      Thread.sleep((++attempt) * sleep);
    } while (regionMovement != expected && attempt <= retry);

    // update the lastRegionOpenedCount
    lastRegionOpenedCount = currentRegionOpened;

    assertEquals("There are only " + regionMovement + " instead of "
          + expected + " region movement for " + attempt + " attempts", expected, regionMovement);
  }

  /**
   * Verify the number of user regions is assigned to the primary
   * region server based on the plan is expected
   * @param expectedNum the expected number of assigned regions
   * @throws IOException
   */
  private void verifyRegionOnPrimaryRS(int expectedNum)
      throws IOException {
    lastRegionOnPrimaryRSCount = getNumRegionisOnPrimaryRS();
    assertEquals("Only " +  expectedNum + " of user regions running " +
        "on the primary region server", expectedNum ,
        lastRegionOnPrimaryRSCount);
  }

  /**
   * Verify all the online region servers has been updated to the
   * latest assignment plan
   * @param plan
   * @throws IOException
   */
  private void verifyRegionServerUpdated(FavoredNodesPlan plan) throws IOException {
    // Verify all region servers contain the correct favored nodes information
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    for (int i = 0; i < SLAVES; i++) {
      HRegionServer rs = cluster.getRegionServer(i);
      for (Region region: rs.getRegions(TableName.valueOf("testRegionAssignment"))) {
        InetSocketAddress[] favoredSocketAddress = rs.getFavoredNodesForRegion(
            region.getRegionInfo().getEncodedName());
        String regionName = region.getRegionInfo().getRegionNameAsString();
        List<ServerName> favoredServerList = plan.getAssignmentMap().get(regionName);

        // All regions are supposed to have favored nodes,
        // except for hbase:meta and ROOT
        if (favoredServerList == null) {
          TableDescriptor desc = region.getTableDescriptor();
          // Verify they are ROOT and hbase:meta regions since no favored nodes
          assertNull(favoredSocketAddress);
          assertTrue("User region " +
              region.getTableDescriptor().getTableName() +
              " should have favored nodes", desc.isMetaRegion());
        } else {
          // For user region, the favored nodes in the region server should be
          // identical to favored nodes in the assignmentPlan
          assertTrue(favoredSocketAddress.length == favoredServerList.size());
          assertTrue(favoredServerList.size() > 0);
          for (int j = 0; j < favoredServerList.size(); j++) {
            InetSocketAddress addrFromRS = favoredSocketAddress[j];
            InetSocketAddress addrFromPlan = InetSocketAddress.createUnresolved(
                favoredServerList.get(j).getHostname(), favoredServerList.get(j).getPort());

            assertNotNull(addrFromRS);
            assertNotNull(addrFromPlan);
            assertTrue("Region server " + rs.getServerName().getAddress()
                + " has the " + positions[j] +
                " for region " + region.getRegionInfo().getRegionNameAsString() + " is " +
                addrFromRS + " which is inconsistent with the plan "
                + addrFromPlan, addrFromRS.equals(addrFromPlan));
          }
        }
      }
    }
  }

  /**
   * Check whether regions are assigned to servers consistent with the explicit
   * hints that are persisted in the hbase:meta table.
   * Also keep track of the number of the regions are assigned to the
   * primary region server.
   * @return the number of regions are assigned to the primary region server
   * @throws IOException
   */
  private int getNumRegionisOnPrimaryRS() throws IOException {
    final AtomicInteger regionOnPrimaryNum = new AtomicInteger(0);
    final AtomicInteger totalRegionNum = new AtomicInteger(0);
    LOG.info("The start of region placement verification");
    MetaTableAccessor.Visitor visitor = new MetaTableAccessor.Visitor() {
      @Override
      public boolean visit(Result result) throws IOException {
        try {
          @SuppressWarnings("deprecation")
          RegionInfo info = MetaTableAccessor.getRegionInfo(result);
          if(info.getTable().getNamespaceAsString()
              .equals(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR)) {
            return true;
          }
          byte[] server = result.getValue(HConstants.CATALOG_FAMILY,
              HConstants.SERVER_QUALIFIER);
          byte[] favoredNodes = result.getValue(HConstants.CATALOG_FAMILY,
              FavoredNodeAssignmentHelper.FAVOREDNODES_QUALIFIER);
          // Add the favored nodes into assignment plan
          ServerName[] favoredServerList =
              FavoredNodeAssignmentHelper.getFavoredNodesList(favoredNodes);
          favoredNodesAssignmentPlan.put(info, favoredServerList);

          Position[] positions = Position.values();
          if (info != null) {
            totalRegionNum.incrementAndGet();
            if (server != null) {
              ServerName serverName =
                  ServerName.valueOf(Bytes.toString(server), -1);
              if (favoredNodes != null) {
                String placement = "[NOT FAVORED NODE]";
                for (int i = 0; i < favoredServerList.length; i++) {
                  if (favoredServerList[i].equals(serverName)) {
                    placement = positions[i].toString();
                    if (i == Position.PRIMARY.ordinal()) {
                      regionOnPrimaryNum.incrementAndGet();
                    }
                    break;
                  }
                }
                LOG.info(info.getRegionNameAsString() + " on " +
                    serverName + " " + placement);
              } else {
                LOG.info(info.getRegionNameAsString() + " running on " +
                    serverName + " but there is no favored region server");
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
    MetaTableAccessor.fullScanRegions(CONNECTION, visitor);
    LOG.info("There are " + regionOnPrimaryNum.intValue() + " out of " +
        totalRegionNum.intValue() + " regions running on the primary" +
        " region servers" );
    return regionOnPrimaryNum.intValue() ;
  }

  /**
   * Create a table with specified table name and region number.
   * @param tableName the name of the table to be created
   * @param regionNum number of regions to create
   * @throws IOException
   */
  private static void createTable(TableName tableName, int regionNum)
      throws IOException {
    int expectedRegions = regionNum;
    byte[][] splitKeys = new byte[expectedRegions - 1][];
    for (int i = 1; i < expectedRegions; i++) {
      byte splitKey = (byte) i;
      splitKeys[i - 1] = new byte[] { splitKey, splitKey, splitKey };
    }

    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    admin.createTable(desc, splitKeys);

    try (RegionLocator r = CONNECTION.getRegionLocator(tableName)) {
      List<HRegionLocation> regions = r.getAllRegionLocations();
      assertEquals("Tried to create " + expectedRegions + " regions "
          + "but only found " + regions.size(), expectedRegions, regions.size());
    }
  }
}
