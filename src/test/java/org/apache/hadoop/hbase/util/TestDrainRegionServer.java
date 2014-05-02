/*
 * Copyright 2014 The Apache Software Foundation
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
package org.apache.hadoop.hbase.util;

import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.master.RegionMovementTestHelper;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.master.AssignmentPlan;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This test runs drain and undrain on single region server from the cluster to test
 * RollingRestart utility. Before testing it sets up a cluster of 5 region servers and then pick one.
 *
 * Test requires write access to file system and takes more than a minute to run.
 */
public class TestDrainRegionServer {

  private final static RegionMovementTestHelper TEST_UTIL = new RegionMovementTestHelper();
  private static final byte[] FAM_NAM = Bytes.toBytes("f");

  private static final int REGION_SERVERS = 5;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().set("hbase.loadbalancer.impl",
        "org.apache.hadoop.hbase.master.RegionManager$AssignmentLoadBalancer");
    TEST_UTIL.startMiniCluster(REGION_SERVERS);
  }

  @Test
  public void testDrainAndUndrain() throws Exception {

    byte[] tableName = Bytes.toBytes("testBlacklistRegionServerWithoutTimeout");

    HTable table = TEST_UTIL.createTable(
        new StringBytes(tableName), new byte[][] { FAM_NAM } , 1, TEST_UTIL.getTmpKeys());
    table.close();

    List<JVMClusterUtil.RegionServerThread> servers =
        TEST_UTIL.getHBaseCluster().getLiveRegionServerThreads();

    AssignmentPlan ap = TEST_UTIL.getHBaseCluster().
      getMaster().regionPlacement.getNewAssignmentPlan();
    TEST_UTIL.getHBaseCluster().getMaster().regionPlacement.updateAssignmentPlan(ap);

    // Wait for rebalance to to complete
    TEST_UTIL.waitOnStableRegionMovement();

    HRegionServer server = null;

    // Lets select a server which does not have META/ROOT
    for (JVMClusterUtil.RegionServerThread serverThread : servers) {
      server = serverThread.getRegionServer();
      for (HRegion region : server.getOnlineRegions()) {
        if (region.getRegionInfo().isMetaRegion() ||
            region.getRegionInfo().isRootRegion() ||
            region.getRegionNameAsString().contains(",,")) {
          server = null;
          break;
        }
      }
      if (server != null) {
        break;
      }
    }

    if (server == null) {
      fail("Unable to find any server");
    }

    RollingRestart rollingRestart = getRollingRestart(server);
    File drainFile = File.createTempFile("TestDrainRegionServer_", ".bin");
    drainFile.deleteOnExit();

    int regionsCount = server.getOnlineRegions().size();

    assertTrue("Server to be drained has to have more than 0 regions", regionsCount > 0);

    rollingRestart.drainServer(drainFile);

    TEST_UTIL.waitOnStableRegionMovement();

    assertEquals("Drained server expected to have 0 online regions", 0, server.getOnlineRegions().size());

    rollingRestart.undrainServer(drainFile);

    TEST_UTIL.waitOnStableRegionMovement();

    assertEquals("Undrained server expected to have same number of online regions as before",
      regionsCount, server.getOnlineRegions().size());
  }

  private RollingRestart getRollingRestart(HRegionServer server) throws IOException {
    HServerInfo serverInfo = server.getServerInfo();
    String serverName = serverInfo.getHostname();
    int port = serverInfo.getServerAddress().getPort();
    int sleepIntervalAfterRestart = RollingRestart.DEFAULT_SLEEP_AFTER_RESTART_INTERVAL;
    int regionDrainInterval = RollingRestart.DEFAULT_REGION_DRAIN_INTERVAL;
    int regionUndrainInterval = RollingRestart.DEFAULT_REGION_UNDRAIN_INTERVAL;
    int getOpFrequency = RollingRestart.DEFAULT_GETOP_FREQUENCY;
    int sleepIntervalBeforeRestart = RollingRestart.DEFAULT_SLEEP_BEFORE_RESTART_INTERVAL;

    RollingRestart rollingRestart = new RollingRestart(serverName, regionDrainInterval, regionUndrainInterval,
        sleepIntervalAfterRestart, sleepIntervalBeforeRestart,
        getOpFrequency, true, port, TEST_UTIL.getConfiguration());
    rollingRestart.setup();
    return rollingRestart;
  }
}
