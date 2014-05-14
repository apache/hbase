/**
 * Copyright 2008 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional infomation
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
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.InjectionEvent;
import org.apache.hadoop.hbase.util.InjectionHandler;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.StringBytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test whether region rebalancing works. (HBASE-71)
 * Test HBASE-3663 whether region rebalancing works after a new server booted
 * especially when no server has more regions than the ceils of avg load
 */
@Category(MediumTests.class)
public class TestRegionRebalancing {
  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  final Log LOG = LogFactory.getLog(this.getClass().getName());

  private static final StringBytes TABLE_NAME = new StringBytes("test");

  HTable table;

  HTableDescriptor desc;

  final byte[] FIVE_HUNDRED_KBYTES;

  final byte [] FAMILY_NAME = Bytes.toBytes("col");

  final Random rand = new Random(57473);

  /** constructor */
  public TestRegionRebalancing() {
    FIVE_HUNDRED_KBYTES = new byte[500 * 1024];
    for (int i = 0; i < 500 * 1024; i++) {
      FIVE_HUNDRED_KBYTES[i] = 'x';
    }

    desc = new HTableDescriptor("test");
    desc.addFamily(new HColumnDescriptor(FAMILY_NAME));
  }

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(3);

    byte[][] splitKeys = new byte[70 - 10][];
    for (int i = 10; i < 70; i++) {
      splitKeys[i - 10] = Bytes.toBytes("row_" + i);
    }

    TEST_UTIL.createTable(TABLE_NAME, new byte[][] { FAMILY_NAME }, 3,
        splitKeys);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * In this case, create 16 servers here, there will be 17 servers and 62 regions totally.
   * When one of the server shuts down, the avg load is 3.875.
   * When this server comes back, the avg load will be 3.64
   * Set the slot number near 0, so no server's load will large than 4.
   * The load balance algorithm should handle this case properly.
   */
  // Marked as unstable and recorded in #4219255
  @Category(UnstableTests.class)
  @Test(timeout = 300000)
  public void testRebalancing() throws IOException {
    for (int i = 3; i < 16; i++) {
      LOG.info("Starting region server " + i);
      TEST_UTIL.getHBaseCluster().startRegionServer();
      checkingServerStatus();
    }

    setInjectionHandler();

    LOG.debug("Restart: killing 1 region server.");
    TEST_UTIL.getHBaseCluster().stopRegionServer(2, false);
    TEST_UTIL.getHBaseCluster().waitOnRegionServer(2);
    assertRegionsAreBalanced();

    LOG.debug("Restart: adding that region server back");
    TEST_UTIL.getHBaseCluster().startRegionServer();
    assertRegionsAreBalanced();
  }

  private void setInjectionHandler() {
    InjectionHandler.set(
        new InjectionHandler() {
        @Override
        protected boolean _falseCondition(InjectionEvent event, Object... args) {
          if (event.equals(InjectionEvent.HREGIONSERVER_REPORT_RESPONSE)) {
            double num = rand.nextDouble();
            HMsg msgs[] = (HMsg[]) args;
            if (msgs.length > 0 && num < 0.45) {
              StringBuilder sb = new StringBuilder(
              "Causing a false condition to be true. "
                  + " rand prob = " + num + " msgs.length is " + msgs.length +
              ". Messages received from the master that are ignored : \t" );
              for (int i = 0; i < msgs.length; i++) {
                sb.append(msgs[i].toString());
                sb.append("\t");
              }
              LOG.debug(sb.toString());
              return true;
            }
          }
          return false;
        }});
  }

  private void checkingServerStatus() {
    List<HRegionServer> servers = getOnlineRegionServers();
    double avg = TEST_UTIL.getHBaseCluster().getMaster().getAverageLoad();
    for (HRegionServer server : servers) {
      int serverLoad = server.getOnlineRegions().size();
      LOG.debug(server.hashCode() + " Avg: " + avg + " actual: " + serverLoad);
    }
  }

  /** figure out how many regions are currently being served. */
  private int getRegionCount() {
    int total = 0;
    for (HRegionServer server : getOnlineRegionServers()) {
      total += server.getOnlineRegions().size();
    }
    return total;
  }

  /**
   * Determine if regions are balanced. Figure out the total, divide by the
   * number of online servers, then test if each server is +/- 1 of average
   * rounded up.
   */
  private void assertRegionsAreBalanced() {
    boolean success = false;
    float slop =
        TEST_UTIL.getConfiguration()
            .getFloat("hbase.regions.slop", (float) 0.1);
    if (slop <= 0) slop = 1;

    // waiting for load balancer running
    try {
      Thread.sleep(MiniHBaseCluster.WAIT_FOR_LOADBALANCER);
    } catch (InterruptedException e1) {
      LOG.error("Got InterruptedException when waiting for load balance " + e1);
    }

    for (int i = 0; i < 5; i++) {
      success = true;
      // make sure all the regions are reassigned before we test balance
      waitForAllRegionsAssigned();

      int regionCount = getRegionCount();
      List<HRegionServer> servers = getOnlineRegionServers();
      double avg = TEST_UTIL.getHBaseCluster().getMaster().getAverageLoad();
      int avgLoadPlusSlop = (int)Math.ceil(avg * (1 + slop));
      int avgLoadMinusSlop = (int)Math.floor(avg * (1 - slop)) - 1;

      LOG.debug("There are " + servers.size() + " servers and " + regionCount
        + " regions. Load Average: " + avg + " low border: " + avgLoadMinusSlop
        + ", up border: " + avgLoadPlusSlop + "; attempt: " + i);

      for (HRegionServer server : servers) {
        int serverLoad = server.getOnlineRegions().size();
        LOG.debug(server.hashCode() + " Avg: " + avg + " actual: " + serverLoad);
        if (!(avg > 2.0 && serverLoad <= avgLoadPlusSlop
            && serverLoad >= avgLoadMinusSlop)) {
          LOG.debug(server.hashCode() + " Isn't balanced!!! Avg: " + avg +
              " actual: " + serverLoad + " slop: " + slop);
          success = false;
        }
      }

      if (!success) {
        // one or more servers are not balanced. sleep a little to give it a
        // chance to catch up. then, go back to the retry loop.
        try {
          Thread.sleep(10000);
        } catch (InterruptedException e) {}

        continue;
      }

      // if we get here, all servers were balanced, so we should just return.
      return;
    }
    // if we get here, we tried 5 times and never got to short circuit out of
    // the retry loop, so this is a failure.
    Assert.fail("After 5 attempts, region assignments were not balanced.");
  }

  private List<HRegionServer> getOnlineRegionServers() {
    List<HRegionServer> list = new ArrayList<HRegionServer>();
    for (JVMClusterUtil.RegionServerThread rst : TEST_UTIL.getHBaseCluster()
        .getRegionServerThreads()) {
      if (rst.getRegionServer().isOnline()) {
        list.add(rst.getRegionServer());
      }
    }
    return list;
  }

  /**
   * Wait until all the regions are assigned.
   */
  private void waitForAllRegionsAssigned() {
    while (getRegionCount() < 62) {
      LOG.debug("Waiting for there to be 62 regions, but there are " + getRegionCount() + " right now.");
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {}
    }
  }
}

