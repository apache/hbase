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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.master.balancer.FavoredNodeAssignmentHelper;
import org.apache.hadoop.hbase.master.balancer.FavoredNodeLoadBalancer;
import org.apache.hadoop.hbase.master.balancer.FavoredNodes.Position;
import org.apache.hadoop.hbase.master.balancer.LoadBalancerFactory;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestRegionPlacement {
  final static Log LOG = LogFactory.getLog(TestRegionPlacement.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static int SLAVES = 4;
  private static HBaseAdmin admin;
  private static Position[] positions = Position.values();
  private int REGION_NUM = 10;
  private Map<HRegionInfo, ServerName[]> favoredNodesAssignmentPlan =
      new HashMap<HRegionInfo, ServerName[]>();

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    // Enable the favored nodes based load balancer
    conf.setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
        FavoredNodeLoadBalancer.class, LoadBalancer.class);
    TEST_UTIL.startMiniCluster(SLAVES);
    admin = new HBaseAdmin(conf);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testGetFavoredNodes() {
    LoadBalancer balancer = LoadBalancerFactory.getLoadBalancer(TEST_UTIL.getConfiguration());
    HRegionInfo regionInfo = new HRegionInfo("oneregion".getBytes());
    List<ServerName> servers = new ArrayList<ServerName>();
    for (int i = 0; i < 10; i++) {
      ServerName server = new ServerName("foo"+i+":1234",-1);
      servers.add(server);
    }
    // test that we have enough favored nodes after we call randomAssignment
    balancer.randomAssignment(regionInfo, servers);
    assertTrue(((FavoredNodeLoadBalancer)balancer).getFavoredNodes(regionInfo).size() == 3);
    List<HRegionInfo> regions = new ArrayList<HRegionInfo>(100);
    for (int i = 0; i < 100; i++) {
      HRegionInfo region = new HRegionInfo(("foobar"+i).getBytes());
      regions.add(region);
    }
    // test that we have enough favored nodes after we call roundRobinAssignment
    balancer.roundRobinAssignment(regions, servers);
    for (int i = 0; i < 100; i++) {
      assertTrue(((FavoredNodeLoadBalancer)balancer).getFavoredNodes(regions.get(i)).size() == 3);
    }
  }

  @Test(timeout = 180000)
  public void testRegionPlacement() throws Exception {
    // Create a table with REGION_NUM regions.
    createTable("testRegionAssignment", REGION_NUM);

    TEST_UTIL.waitTableAvailable(Bytes.toBytes("testRegionAssignment"));

    // Verify all the user regions are assigned to the primary region server
    // based on the plan
    countRegionOnPrimaryRS(REGION_NUM);

    // Verify all the region server are update with the latest favored nodes
    verifyRegionServerUpdated();
  }

  /**
   * Verify the number of user regions is assigned to the primary
   * region server based on the plan is expected
   * @param expectedNum.
   * @throws IOException
   */
  private void countRegionOnPrimaryRS(int expectedNum)
      throws IOException {
    int lastRegionOnPrimaryRSCount = getNumRegionisOnPrimaryRS();
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
  private void verifyRegionServerUpdated() throws IOException {
    // Verify all region servers contain the correct favored nodes information
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    for (int i = 0; i < SLAVES; i++) {
      HRegionServer rs = cluster.getRegionServer(i);
      for (HRegion region: rs.getOnlineRegions(Bytes.toBytes("testRegionAssignment"))) {
        InetSocketAddress[] favoredSocketAddress = rs.getFavoredNodesForRegion(
            region.getRegionInfo().getEncodedName());
        ServerName[] favoredServerList = favoredNodesAssignmentPlan.get(region.getRegionInfo());

        // All regions are supposed to have favored nodes,
        // except for META and ROOT
        if (favoredServerList == null) {
          HTableDescriptor desc = region.getTableDesc();
          // Verify they are ROOT and META regions since no favored nodes
          assertNull(favoredSocketAddress);
          assertTrue("User region " +
              region.getTableDesc().getNameAsString() +
              " should have favored nodes",
              (desc.isRootRegion() || desc.isMetaRegion()));
        } else {
          // For user region, the favored nodes in the region server should be
          // identical to favored nodes in the assignmentPlan
          assertTrue(favoredSocketAddress.length == favoredServerList.length);
          assertTrue(favoredServerList.length > 0);
          for (int j = 0; j < favoredServerList.length; j++) {
            InetSocketAddress addrFromRS = favoredSocketAddress[j];
            InetSocketAddress addrFromPlan = InetSocketAddress.createUnresolved(
                favoredServerList[j].getHostname(), favoredServerList[j].getPort());

            assertNotNull(addrFromRS);
            assertNotNull(addrFromPlan);
            assertTrue("Region server " + rs.getServerName().getHostAndPort()
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
      public boolean processRow(Result result) throws IOException {
        try {
          HRegionInfo info = MetaScanner.getHRegionInfo(result);
          byte[] server = result.getValue(HConstants.CATALOG_FAMILY,
              HConstants.SERVER_QUALIFIER);
          byte[] startCode = result.getValue(HConstants.CATALOG_FAMILY,
              HConstants.STARTCODE_QUALIFIER);
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
                  new ServerName(Bytes.toString(server),Bytes.toLong(startCode));
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

      @Override
      public void close() throws IOException {}
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
    Map<HRegionInfo, ServerName> regions = ht.getRegionLocations();
    assertEquals("Tried to create " + expectedRegions + " regions "
        + "but only found " + regions.size(), expectedRegions, regions.size());
  }
}
