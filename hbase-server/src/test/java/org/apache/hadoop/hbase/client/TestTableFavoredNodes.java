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
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.favored.FavoredNodesPlan.Position.PRIMARY;
import static org.apache.hadoop.hbase.favored.FavoredNodesPlan.Position.SECONDARY;
import static org.apache.hadoop.hbase.favored.FavoredNodesPlan.Position.TERTIARY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.favored.FavoredNodeAssignmentHelper;
import org.apache.hadoop.hbase.favored.FavoredNodeLoadBalancer;
import org.apache.hadoop.hbase.favored.FavoredNodesManager;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category({ClientTests.class, MediumTests.class})
public class TestTableFavoredNodes {

  private static final Log LOG = LogFactory.getLog(TestTableFavoredNodes.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static int WAIT_TIMEOUT = 60000;
  private final static int SLAVES = 8;
  private FavoredNodesManager fnm;
  private RegionStates regionStates;
  private Admin admin;

  private final byte[][] splitKeys = new byte[][] {Bytes.toBytes(1), Bytes.toBytes(9)};
  private final int NUM_REGIONS = splitKeys.length + 1;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    // Setting FavoredNodeBalancer will enable favored nodes
    conf.setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
        FavoredNodeLoadBalancer.class, LoadBalancer.class);
    conf.set(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, "" + SLAVES);
    TEST_UTIL.startMiniCluster(SLAVES);
    TEST_UTIL.getMiniHBaseCluster().waitForActiveAndReadyMaster(WAIT_TIMEOUT);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.cleanupTestDir();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setup() throws IOException {
    fnm = TEST_UTIL.getMiniHBaseCluster().getMaster().getFavoredNodesManager();
    admin = TEST_UTIL.getAdmin();
    admin.setBalancerRunning(false, true);
    admin.enableCatalogJanitor(false);
    regionStates =
      TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStates();
  }

  /*
   * Create a table with FN enabled and check if all its regions have favored nodes set.
   */
  @Test
  public void testCreateTable() throws Exception {

    TableName tableName = TableName.valueOf("createTable");
    TEST_UTIL.createTable(tableName, Bytes.toBytes("f"), splitKeys);
    TEST_UTIL.waitUntilAllRegionsAssigned(tableName);

    // All regions should have favored nodes
    checkIfFavoredNodeInformationIsCorrect(tableName);

    List<HRegionInfo> regions = admin.getTableRegions(tableName);

    TEST_UTIL.deleteTable(tableName);

    checkNoFNForDeletedTable(regions);
  }

  /*
   * Checks if favored node information is removed on table truncation.
   */
  @Test
  public void testTruncateTable() throws Exception {

    TableName tableName = TableName.valueOf("truncateTable");
    TEST_UTIL.createTable(tableName, Bytes.toBytes("f"), splitKeys);
    TEST_UTIL.waitUntilAllRegionsAssigned(tableName);

    // All regions should have favored nodes
    checkIfFavoredNodeInformationIsCorrect(tableName);

    List<HRegionInfo> regions = admin.getTableRegions(tableName);
    TEST_UTIL.truncateTable(tableName, true);

    checkNoFNForDeletedTable(regions);
    checkIfFavoredNodeInformationIsCorrect(tableName);

    regions = admin.getTableRegions(tableName);
    TEST_UTIL.truncateTable(tableName, false);
    checkNoFNForDeletedTable(regions);

    TEST_UTIL.deleteTable(tableName);
  }

  /*
   * Check if daughters inherit at-least 2 FN from parent after region split.
   */
  @Test
  public void testSplitTable() throws Exception {

    TableName tableName = TableName.valueOf("splitRegions");
    TEST_UTIL.createTable(tableName, Bytes.toBytes("f"), splitKeys);
    TEST_UTIL.waitUntilAllRegionsAssigned(tableName);

    checkIfFavoredNodeInformationIsCorrect(tableName);

    byte[] splitPoint = Bytes.toBytes(0);
    RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName);
    HRegionInfo parent = locator.getRegionLocation(splitPoint).getRegionInfo();
    List<ServerName> parentFN = fnm.getFavoredNodes(parent);
    assertNotNull("FN should not be null for region: " + parent, parentFN);

    admin.split(tableName, splitPoint);

    TEST_UTIL.waitUntilNoRegionsInTransition(WAIT_TIMEOUT);
    waitUntilTableRegionCountReached(tableName, NUM_REGIONS + 1);

    // All regions should have favored nodes
    checkIfFavoredNodeInformationIsCorrect(tableName);

    // Get the daughters of parent.
    HRegionInfo daughter1 = locator.getRegionLocation(parent.getStartKey(), true).getRegionInfo();
    List<ServerName> daughter1FN = fnm.getFavoredNodes(daughter1);

    HRegionInfo daughter2 = locator.getRegionLocation(splitPoint, true).getRegionInfo();
    List<ServerName> daughter2FN = fnm.getFavoredNodes(daughter2);

    checkIfDaughterInherits2FN(parentFN, daughter1FN);
    checkIfDaughterInherits2FN(parentFN, daughter2FN);

    assertEquals("Daughter's PRIMARY FN should be PRIMARY of parent",
        parentFN.get(PRIMARY.ordinal()), daughter1FN.get(PRIMARY.ordinal()));
    assertEquals("Daughter's SECONDARY FN should be SECONDARY of parent",
        parentFN.get(SECONDARY.ordinal()), daughter1FN.get(SECONDARY.ordinal()));

    assertEquals("Daughter's PRIMARY FN should be PRIMARY of parent",
        parentFN.get(PRIMARY.ordinal()), daughter2FN.get(PRIMARY.ordinal()));
    assertEquals("Daughter's SECONDARY FN should be TERTIARY of parent",
        parentFN.get(TERTIARY.ordinal()), daughter2FN.get(SECONDARY.ordinal()));

    // Major compact table and run catalog janitor. Parent's FN should be removed
    TEST_UTIL.getMiniHBaseCluster().compact(tableName, true);
    assertEquals("Parent region should have been cleaned", 1, admin.runCatalogScan());
    assertNull("Parent FN should be null", fnm.getFavoredNodes(parent));

    List<HRegionInfo> regions = admin.getTableRegions(tableName);

    TEST_UTIL.deleteTable(tableName);

    checkNoFNForDeletedTable(regions);
  }

  /*
   * Check if merged region inherits FN from one of its regions.
   */
  @Test
  public void testMergeTable() throws Exception {

    TableName tableName = TableName.valueOf("mergeRegions");
    TEST_UTIL.createTable(tableName, Bytes.toBytes("f"), splitKeys);
    TEST_UTIL.waitUntilAllRegionsAssigned(tableName);

    checkIfFavoredNodeInformationIsCorrect(tableName);

    RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName);
    HRegionInfo regionA = locator.getRegionLocation(HConstants.EMPTY_START_ROW).getRegionInfo();
    HRegionInfo regionB = locator.getRegionLocation(splitKeys[0]).getRegionInfo();

    List<ServerName> regionAFN = fnm.getFavoredNodes(regionA);
    LOG.info("regionA: " + regionA.getEncodedName() + " with FN: " + fnm.getFavoredNodes(regionA));
    LOG.info("regionB: " + regionA.getEncodedName() + " with FN: " + fnm.getFavoredNodes(regionB));

    admin.mergeRegionsAsync(regionA.getEncodedNameAsBytes(),
        regionB.getEncodedNameAsBytes(), false).get(60, TimeUnit.SECONDS);

    TEST_UTIL.waitUntilNoRegionsInTransition(WAIT_TIMEOUT);
    waitUntilTableRegionCountReached(tableName, NUM_REGIONS - 1);

    // All regions should have favored nodes
    checkIfFavoredNodeInformationIsCorrect(tableName);

    HRegionInfo mergedRegion =
      locator.getRegionLocation(HConstants.EMPTY_START_ROW).getRegionInfo();
    List<ServerName> mergedFN = fnm.getFavoredNodes(mergedRegion);

    assertArrayEquals("Merged region doesn't match regionA's FN",
        regionAFN.toArray(), mergedFN.toArray());

    // Major compact table and run catalog janitor. Parent FN should be removed
    TEST_UTIL.getMiniHBaseCluster().compact(tableName, true);
    assertEquals("Merge parents should have been cleaned", 1, admin.runCatalogScan());
    assertNull("Parent FN should be null", fnm.getFavoredNodes(regionA));
    assertNull("Parent FN should be null", fnm.getFavoredNodes(regionB));

    List<HRegionInfo> regions = admin.getTableRegions(tableName);

    TEST_UTIL.deleteTable(tableName);

    checkNoFNForDeletedTable(regions);
  }

  private void checkNoFNForDeletedTable(List<HRegionInfo> regions) {
    for (HRegionInfo region : regions) {
      assertNull("FN not null for deleted table's region: " + region, fnm.getFavoredNodes(region));
    }
  }

  /*
   * This checks the following:
   *
   * 1. Do all regions of the table have favored nodes updated in master?
   * 2. Is the number of favored nodes correct for a region? Is the start code -1?
   * 3. Is the FN information consistent between Master and the respective RegionServer?
   */
  private void checkIfFavoredNodeInformationIsCorrect(TableName tableName) throws Exception {

    /*
     * Since we need HRegionServer to check for consistency of FN between Master and RS,
     * lets construct a map for each serverName lookup. Makes it easy later.
     */
    Map<ServerName, HRegionServer> snRSMap = Maps.newHashMap();
    for (JVMClusterUtil.RegionServerThread rst :
      TEST_UTIL.getMiniHBaseCluster().getLiveRegionServerThreads()) {
      snRSMap.put(rst.getRegionServer().getServerName(), rst.getRegionServer());
    }
    // Also include master, since it can also host user regions.
    for (JVMClusterUtil.MasterThread rst :
      TEST_UTIL.getMiniHBaseCluster().getLiveMasterThreads()) {
      snRSMap.put(rst.getMaster().getServerName(), rst.getMaster());
    }

    RegionLocator regionLocator = admin.getConnection().getRegionLocator(tableName);
    for (HRegionLocation regionLocation : regionLocator.getAllRegionLocations()) {

      HRegionInfo regionInfo = regionLocation.getRegionInfo();
      List<ServerName> fnList = fnm.getFavoredNodes(regionInfo);

      // 1. Does each region have favored node?
      assertNotNull("Favored nodes should not be null for region:" + regionInfo, fnList);

      // 2. Do we have the right number of favored nodes? Is start code -1?
      assertEquals("Incorrect favored nodes for region:" + regionInfo + " fnlist: " + fnList,
        FavoredNodeAssignmentHelper.FAVORED_NODES_NUM, fnList.size());
      for (ServerName sn : fnList) {
        assertEquals("FN should not have startCode, fnlist:" + fnList, -1, sn.getStartcode());
      }

      // 3. Check if the regionServers have all the FN updated and in sync with Master
      HRegionServer regionServer = snRSMap.get(regionLocation.getServerName());
      assertNotNull("RS should not be null for regionLocation: " + regionLocation, regionServer);

      InetSocketAddress[] rsFavNodes =
        regionServer.getFavoredNodesForRegion(regionInfo.getEncodedName());
      assertNotNull("RS " + regionLocation.getServerName()
        + " does not have FN for region: " + regionInfo, rsFavNodes);

      List<ServerName> fnFromRS = Lists.newArrayList();
      for (InetSocketAddress addr : rsFavNodes) {
        fnFromRS.add(ServerName.valueOf(addr.getHostName(), addr.getPort(),
          ServerName.NON_STARTCODE));
      }

      fnFromRS.removeAll(fnList);
      assertEquals("Inconsistent FN bet RS and Master, RS diff: " + fnFromRS
        + " List on master: "  + fnList, 0, fnFromRS.size());
    }
  }

  private void checkIfDaughterInherits2FN(List<ServerName> parentFN, List<ServerName> daughterFN) {

    assertNotNull(parentFN);
    assertNotNull(daughterFN);

    List<ServerName> favoredNodes = Lists.newArrayList(daughterFN);
    favoredNodes.removeAll(parentFN);

    /*
     * With a small cluster its likely some FN might accidentally get shared. Its likely the
     * 3rd FN the balancer chooses might still belong to the parent in which case favoredNodes
     * size would be 0.
     */
    assertTrue("Daughter FN:" + daughterFN + " should have inherited 2 FN from parent FN:"
      + parentFN, favoredNodes.size() <= 1);
  }

  private void waitUntilTableRegionCountReached(final TableName tableName, final int numRegions)
      throws Exception {
    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return regionStates.getRegionsOfTable(tableName).size() == numRegions;
      }
    });
  }
}