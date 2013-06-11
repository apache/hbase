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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  private final static int SLAVES = 10;
  private static HBaseAdmin admin;
  private static Position[] positions = Position.values();
  private int REGION_NUM = 10;
  private Map<HRegionInfo, ServerName[]> favoredNodesAssignmentPlan =
      new HashMap<HRegionInfo, ServerName[]>();
  private final static int PRIMARY = Position.PRIMARY.ordinal();
  private final static int SECONDARY = Position.SECONDARY.ordinal();
  private final static int TERTIARY = Position.TERTIARY.ordinal();

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    // Enable the favored nodes based load balancer
    conf.setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
        FavoredNodeLoadBalancer.class, LoadBalancer.class);
    conf.setBoolean("hbase.tests.use.shortcircuit.reads", false);
    TEST_UTIL.startMiniCluster(SLAVES);
    admin = new HBaseAdmin(conf);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testFavoredNodesPresentForRoundRobinAssignment() {
    LoadBalancer balancer = LoadBalancerFactory.getLoadBalancer(TEST_UTIL.getConfiguration());
    balancer.setMasterServices(TEST_UTIL.getMiniHBaseCluster().getMaster());
    List<ServerName> servers = new ArrayList<ServerName>();
    for (int i = 0; i < SLAVES; i++) {
      ServerName server = TEST_UTIL.getMiniHBaseCluster().getRegionServer(i).getServerName();
      servers.add(server);
    }
    List<HRegionInfo> regions = new ArrayList<HRegionInfo>(1);
    HRegionInfo region = new HRegionInfo(("foobar").getBytes());
    regions.add(region);
    Map<ServerName,List<HRegionInfo>> assignmentMap = balancer.roundRobinAssignment(regions,
        servers);
    Set<ServerName> serverBefore = assignmentMap.keySet();
    List<ServerName> favoredNodesBefore =
        ((FavoredNodeLoadBalancer)balancer).getFavoredNodes(region);
    assertTrue(favoredNodesBefore.size() == 3);
    // the primary RS should be the one that the balancer's assignment returns
    assertTrue(ServerName.isSameHostnameAndPort(serverBefore.iterator().next(),
        favoredNodesBefore.get(PRIMARY)));
    // now remove the primary from the list of available servers
    List<ServerName> removedServers = removeMatchingServers(serverBefore, servers);
    // call roundRobinAssignment with the modified servers list
    assignmentMap = balancer.roundRobinAssignment(regions, servers);
    List<ServerName> favoredNodesAfter =
        ((FavoredNodeLoadBalancer)balancer).getFavoredNodes(region);
    assertTrue(favoredNodesAfter.size() == 3);
    // We don't expect the favored nodes assignments to change in multiple calls
    // to the roundRobinAssignment method in the balancer (relevant for AssignmentManager.assign
    // failures)
    assertTrue(favoredNodesAfter.containsAll(favoredNodesBefore));
    Set<ServerName> serverAfter = assignmentMap.keySet();
    // We expect the new RegionServer assignee to be one of the favored nodes
    // chosen earlier.
    assertTrue(ServerName.isSameHostnameAndPort(serverAfter.iterator().next(),
                 favoredNodesBefore.get(SECONDARY)) ||
               ServerName.isSameHostnameAndPort(serverAfter.iterator().next(),
                 favoredNodesBefore.get(TERTIARY)));

    // put back the primary in the list of available servers
    servers.addAll(removedServers);
    // now roundRobinAssignment with the modified servers list should return the primary
    // as the regionserver assignee
    assignmentMap = balancer.roundRobinAssignment(regions, servers);
    Set<ServerName> serverWithPrimary = assignmentMap.keySet();
    assertTrue(serverBefore.containsAll(serverWithPrimary));

    // Make all the favored nodes unavailable for assignment
    removeMatchingServers(favoredNodesAfter, servers);
    // call roundRobinAssignment with the modified servers list
    assignmentMap = balancer.roundRobinAssignment(regions, servers);
    List<ServerName> favoredNodesNow =
        ((FavoredNodeLoadBalancer)balancer).getFavoredNodes(region);
    assertTrue(favoredNodesNow.size() == 3);
    assertTrue(!favoredNodesNow.contains(favoredNodesAfter.get(PRIMARY)) &&
        !favoredNodesNow.contains(favoredNodesAfter.get(SECONDARY)) &&
        !favoredNodesNow.contains(favoredNodesAfter.get(TERTIARY)));
  }

  @Test
  public void testFavoredNodesPresentForRandomAssignment() {
    LoadBalancer balancer = LoadBalancerFactory.getLoadBalancer(TEST_UTIL.getConfiguration());
    balancer.setMasterServices(TEST_UTIL.getMiniHBaseCluster().getMaster());
    List<ServerName> servers = new ArrayList<ServerName>();
    for (int i = 0; i < SLAVES; i++) {
      ServerName server = TEST_UTIL.getMiniHBaseCluster().getRegionServer(i).getServerName();
      servers.add(server);
    }
    List<HRegionInfo> regions = new ArrayList<HRegionInfo>(1);
    HRegionInfo region = new HRegionInfo(("foobar").getBytes());
    regions.add(region);
    ServerName serverBefore = balancer.randomAssignment(region, servers);
    List<ServerName> favoredNodesBefore =
        ((FavoredNodeLoadBalancer)balancer).getFavoredNodes(region);
    assertTrue(favoredNodesBefore.size() == 3);
    // the primary RS should be the one that the balancer's assignment returns
    assertTrue(ServerName.isSameHostnameAndPort(serverBefore,favoredNodesBefore.get(PRIMARY)));
    // now remove the primary from the list of servers
    removeMatchingServers(serverBefore, servers);
    // call randomAssignment with the modified servers list
    ServerName serverAfter = balancer.randomAssignment(region, servers);
    List<ServerName> favoredNodesAfter =
        ((FavoredNodeLoadBalancer)balancer).getFavoredNodes(region);
    assertTrue(favoredNodesAfter.size() == 3);
    // We don't expect the favored nodes assignments to change in multiple calls
    // to the randomAssignment method in the balancer (relevant for AssignmentManager.assign
    // failures)
    assertTrue(favoredNodesAfter.containsAll(favoredNodesBefore));
    // We expect the new RegionServer assignee to be one of the favored nodes
    // chosen earlier.
    assertTrue(ServerName.isSameHostnameAndPort(serverAfter, favoredNodesBefore.get(SECONDARY)) ||
               ServerName.isSameHostnameAndPort(serverAfter, favoredNodesBefore.get(TERTIARY)));
    // Make all the favored nodes unavailable for assignment
    removeMatchingServers(favoredNodesAfter, servers);
    // call randomAssignment with the modified servers list
    balancer.randomAssignment(region, servers);
    List<ServerName> favoredNodesNow =
        ((FavoredNodeLoadBalancer)balancer).getFavoredNodes(region);
    assertTrue(favoredNodesNow.size() == 3);
    assertTrue(!favoredNodesNow.contains(favoredNodesAfter.get(PRIMARY)) &&
        !favoredNodesNow.contains(favoredNodesAfter.get(SECONDARY)) &&
        !favoredNodesNow.contains(favoredNodesAfter.get(TERTIARY)));
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

  private List<ServerName> removeMatchingServers(ServerName serverWithoutStartCode,
      List<ServerName> servers) {
    List<ServerName> serversToRemove = new ArrayList<ServerName>();
    for (ServerName s : servers) {
      if (ServerName.isSameHostnameAndPort(s, serverWithoutStartCode)) {
        serversToRemove.add(s);
      }
    }
    servers.removeAll(serversToRemove);
    return serversToRemove;
  }

  private List<ServerName> removeMatchingServers(Collection<ServerName> serversWithoutStartCode,
      List<ServerName> servers) {
    List<ServerName> serversToRemove = new ArrayList<ServerName>();
    for (ServerName s : serversWithoutStartCode) {
      serversToRemove.addAll(removeMatchingServers(s, servers));
    }
    return serversToRemove;
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
                  new ServerName(Bytes.toString(server), -1);
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
