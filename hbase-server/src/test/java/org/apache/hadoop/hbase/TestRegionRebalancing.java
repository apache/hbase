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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.FlakeyTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

/**
 * Test whether region re-balancing works. (HBASE-71)
 * The test only works for cluster wide balancing, not per table wide.
 * Increase the margin a little to make StochasticLoadBalancer result acceptable.
 */
@Category({FlakeyTests.class, LargeTests.class})
@RunWith(value = Parameterized.class)
public class TestRegionRebalancing {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionRebalancing.class);

  @Parameters
  public static Collection<Object[]> data() {
    Object[][] balancers =
        new String[][] { { "org.apache.hadoop.hbase.master.balancer.SimpleLoadBalancer" },
            { "org.apache.hadoop.hbase.master.balancer.StochasticLoadBalancer" } };
    return Arrays.asList(balancers);
  }

  private static final byte[] FAMILY_NAME = Bytes.toBytes("col");
  private static final Logger LOG = LoggerFactory.getLogger(TestRegionRebalancing.class);
  private final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private RegionLocator regionLocator;
  private HTableDescriptor desc;
  private String balancerName;

  public TestRegionRebalancing(String balancerName) {
    this.balancerName = balancerName;

  }

  @After
  public void after() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void before() throws Exception {
    UTIL.getConfiguration().set("hbase.master.loadbalancer.class", this.balancerName);
    // set minCostNeedBalance to 0, make sure balancer run
    UTIL.startMiniCluster(1);
    this.desc = new HTableDescriptor(TableName.valueOf("test"));
    this.desc.addFamily(new HColumnDescriptor(FAMILY_NAME));
  }

  /**
   * For HBASE-71. Try a few different configurations of starting and stopping
   * region servers to see if the assignment or regions is pretty balanced.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testRebalanceOnRegionServerNumberChange()
  throws IOException, InterruptedException {
    try(Connection connection = ConnectionFactory.createConnection(UTIL.getConfiguration());
        Admin admin = connection.getAdmin()) {
      admin.createTable(this.desc, Arrays.copyOfRange(HBaseTestingUtility.KEYS,
          1, HBaseTestingUtility.KEYS.length));
      this.regionLocator = connection.getRegionLocator(this.desc.getTableName());

      MetaTableAccessor.fullScanMetaAndPrint(admin.getConnection());

      assertEquals("Test table should have right number of regions",
        HBaseTestingUtility.KEYS.length,
        this.regionLocator.getStartKeys().length);

      // verify that the region assignments are balanced to start out
      assertRegionsAreBalanced();

      // add a region server - total of 2
      LOG.info("Started second server=" +
        UTIL.getHBaseCluster().startRegionServer().getRegionServer().getServerName());
      UTIL.getHBaseCluster().getMaster().balance();
      assertRegionsAreBalanced();

      // On a balanced cluster, calling balance() should return true
      assert(UTIL.getHBaseCluster().getMaster().balance() == true);

      // if we add a server, then the balance() call should return true
      // add a region server - total of 3
      LOG.info("Started third server=" +
          UTIL.getHBaseCluster().startRegionServer().getRegionServer().getServerName());
      waitForAllRegionsAssigned();
      assert(UTIL.getHBaseCluster().getMaster().balance() == true);
      assertRegionsAreBalanced();

      // kill a region server - total of 2
      LOG.info("Stopped third server=" + UTIL.getHBaseCluster().stopRegionServer(2, false));
      UTIL.getHBaseCluster().waitOnRegionServer(2);
      waitOnCrashProcessing();
      UTIL.getHBaseCluster().getMaster().balance();
      assertRegionsAreBalanced();

      // start two more region servers - total of 4
      LOG.info("Readding third server=" +
          UTIL.getHBaseCluster().startRegionServer().getRegionServer().getServerName());
      LOG.info("Added fourth server=" +
          UTIL.getHBaseCluster().startRegionServer().getRegionServer().getServerName());
      waitOnCrashProcessing();
      waitForAllRegionsAssigned();
      assert(UTIL.getHBaseCluster().getMaster().balance() == true);
      assertRegionsAreBalanced();
      for (int i = 0; i < 6; i++){
        LOG.info("Adding " + (i + 5) + "th region server");
        UTIL.getHBaseCluster().startRegionServer();
      }
      waitForAllRegionsAssigned();
      assert(UTIL.getHBaseCluster().getMaster().balance() == true);
      assertRegionsAreBalanced();
      regionLocator.close();
    }
  }

  /**
   * Wait on crash processing. Balancer won't run if processing a crashed server.
   */
  private void waitOnCrashProcessing() {
    while (UTIL.getHBaseCluster().getMaster().getServerManager().areDeadServersInProgress()) {
      LOG.info("Waiting on processing of crashed server before proceeding...");
      Threads.sleep(1000);
    }
  }

  /**
   * Determine if regions are balanced. Figure out the total, divide by the
   * number of online servers, then test if each server is +/- 1 of average
   * rounded up.
   */
  private void assertRegionsAreBalanced() throws IOException {
    // TODO: Fix this test.  Old balancer used to run with 'slop'.  New
    // balancer does not.
    boolean success = false;
    float slop = (float)UTIL.getConfiguration().getFloat("hbase.regions.slop", 0.1f);
    if (slop <= 0) slop = 1;

    for (int i = 0; i < 5; i++) {
      success = true;
      // make sure all the regions are reassigned before we test balance
      waitForAllRegionsAssigned();

      long regionCount = UTIL.getMiniHBaseCluster().countServedRegions();
      List<HRegionServer> servers = getOnlineRegionServers();
      double avg = (double)regionCount / (double)servers.size();
      int avgLoadPlusSlop = (int)Math.ceil(avg * (1 + slop));
      int avgLoadMinusSlop = (int)Math.floor(avg * (1 - slop)) - 1;
      // Increase the margin a little to accommodate StochasticLoadBalancer
      if (this.balancerName.contains("StochasticLoadBalancer")) {
        avgLoadPlusSlop++;
        avgLoadMinusSlop--;
      }
      LOG.debug("There are " + servers.size() + " servers and " + regionCount
        + " regions. Load Average: " + avg + " low border: " + avgLoadMinusSlop
        + ", up border: " + avgLoadPlusSlop + "; attempt: " + i);

      for (HRegionServer server : servers) {
        int serverLoad =
          ProtobufUtil.getOnlineRegions(server.getRSRpcServices()).size();
        LOG.debug(server.getServerName() + " Avg: " + avg + " actual: " + serverLoad);
        if (!(avg > 2.0 && serverLoad <= avgLoadPlusSlop
            && serverLoad >= avgLoadMinusSlop)) {
          for (RegionInfo hri :
              ProtobufUtil.getOnlineRegions(server.getRSRpcServices())) {
            if (hri.isMetaRegion()) serverLoad--;
            // LOG.debug(hri.getRegionNameAsString());
          }
          if (!(serverLoad <= avgLoadPlusSlop && serverLoad >= avgLoadMinusSlop)) {
            LOG.debug(server.getServerName() + " Isn't balanced!!! Avg: " + avg +
                " actual: " + serverLoad + " slop: " + slop);
            success = false;
            break;
          }
        }
      }

      if (!success) {
        // one or more servers are not balanced. sleep a little to give it a
        // chance to catch up. then, go back to the retry loop.
        try {
          Thread.sleep(10000);
        } catch (InterruptedException e) {}

        UTIL.getHBaseCluster().getMaster().balance();
        continue;
      }

      // if we get here, all servers were balanced, so we should just return.
      return;
    }
    // if we get here, we tried 5 times and never got to short circuit out of
    // the retry loop, so this is a failure.
    fail("After 5 attempts, region assignments were not balanced.");
  }

  private List<HRegionServer> getOnlineRegionServers() {
    List<HRegionServer> list = new ArrayList<>();
    for (JVMClusterUtil.RegionServerThread rst :
        UTIL.getHBaseCluster().getRegionServerThreads()) {
      if (rst.getRegionServer().isOnline()) {
        list.add(rst.getRegionServer());
      }
    }
    return list;
  }

  /**
   * Wait until all the regions are assigned.
   */
  private void waitForAllRegionsAssigned() throws IOException {
    int totalRegions = HBaseTestingUtility.KEYS.length;
    try {
        Thread.sleep(200);
    } catch (InterruptedException e) {
      throw new InterruptedIOException();
    }
    while (UTIL.getMiniHBaseCluster().countServedRegions() < totalRegions) {
    // while (!cluster.getMaster().allRegionsAssigned()) {
      LOG.debug("Waiting for there to be "+ totalRegions +" regions, but there are "
        + UTIL.getMiniHBaseCluster().countServedRegions() + " right now.");
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        throw new InterruptedIOException();
      }
    }
    UTIL.waitUntilNoRegionsInTransition();
  }

}

