/**
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

package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.client.TableServers;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestGetRegionLocation {
  private static final Log LOG = LogFactory.getLog(TestGetRegionLocation.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static Configuration CONF = TEST_UTIL.getConfiguration();
  private final static byte[] TABLE = Bytes.toBytes("table");
  private final static byte[] FAMILY = Bytes.toBytes("family");
  private final static byte[][] FAMILIES = { FAMILY };
  private final static byte[] START_KEY = Bytes.toBytes("aaa");
  private final static byte[] END_KEY = Bytes.toBytes("zzz");
  private final static int NUM_REGIONS = 10;
  private final static int NUM_SLAVES = 4;

  @Before
  public void setUp() throws IOException, InterruptedException {
    // Use assignment plan so that regions are not moved unexpectedly.
    TEST_UTIL.getConfiguration().set(HConstants.LOAD_BALANCER_IMPL,
        "org.apache.hadoop.hbase.master.RegionManager$AssignmentLoadBalancer");
    TEST_UTIL.startMiniCluster(NUM_SLAVES);
    TEST_UTIL.createTable(TABLE, FAMILIES, 1, START_KEY, END_KEY, NUM_REGIONS);
  }

  @After
  public void tearDown() throws IOException, InterruptedException {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * This test tests the getRegionLocation() call in the
   * (Thrift)HRegionInterface API.
   *
   * @throws InterruptedException
   * @throws IOException
   */
  @Test
  public void testGetRegionLocation() throws InterruptedException, IOException {
    List<HRegionServer> regionServers = TEST_UTIL.getOnlineRegionServers();
    List<HServerInfo> serverInfos = new ArrayList<>();
    Map<HRegionInfo, HServerInfo> regionInfoToServerInfoMap = new HashMap<>();

    // Get the list of servers, and regions, and regions->servers mapping.
    for (HRegionServer regionServer : regionServers) {
      serverInfos.add(regionServer.getServerInfo());
      for (HRegion or : regionServer.getOnlineRegions()) {
        regionInfoToServerInfoMap.put(or.getRegionInfo(),
          regionServer.getServerInfo());
      }
    }

    try (TableServers connection = new TableServers(CONF)) {
      // Iterate through each of the servers, and check if the locations for
      // all the regions are correct.
      for (HServerInfo serverInfo : serverInfos) {
        HRegionInterface server =
          connection.getHRegionConnection(serverInfo.getServerAddress());

        for (Map.Entry<HRegionInfo, HServerInfo> entry :
          regionInfoToServerInfoMap.entrySet()) {
          HRegionInfo info = entry.getKey();

          // Get the location for this particular region's start key...
          HRegionLocation location =
            server.getLocation(info.getTableDesc().getName(),
              info.getStartKey(),
              false);

          // ... which should be the same as the actual location of this region.
          Assert.assertEquals(
            "getLocation() returned an incorrect server location for region: "
                + info.getRegionNameAsString(),
            entry.getValue().getServerAddress(),
            location.getServerAddress());
        }
      }

      // Now let us try moving a random region (let's pick the first)
      // to a different location, and see if the getLocation works with reloading
      Map.Entry<HRegionInfo, HServerInfo> firstRegion =
        regionInfoToServerInfoMap.entrySet().iterator().next();
      HRegionInfo firstRegionInfo = firstRegion.getKey();
      HServerInfo firstServerInfo = firstRegion.getValue();

      LOG.info("Region: " + firstRegionInfo.getRegionNameAsString() +
        " was located at " + firstServerInfo.getServerAddress().toString());

      // Pick up a server to query getRegionLocation
      HRegionInterface server =
        connection.getHRegionConnection(
          serverInfos.iterator().next().getServerAddress());

      // Check that the location before moving is sane
      HRegionLocation locationBeforeMoving =
        server.getLocation(firstRegionInfo.getTableDesc().getName(),
          firstRegionInfo.getStartKey(), false);

      Assert.assertEquals(
        "getLocation() returned an incorrect server location for region: " +
          firstRegionInfo.getRegionNameAsString(),
          firstServerInfo.getServerAddress(),
          locationBeforeMoving.getServerAddress()
      );

      HServerInfo targetServer = null;
      // Find a targetServer to host the first region
      for (HServerInfo serverInfo : serverInfos) {
        if (serverInfo.equals(firstServerInfo)) {
          continue;
        }
        targetServer = serverInfo;
      }

      LOG.info("Region: " + firstRegionInfo.getRegionNameAsString()
          + " will be moved from: " + firstServerInfo.getServerAddress()
          + " to: " + targetServer.getServerAddress().toString());

      // Now move the region to the target server
      TEST_UTIL.getHBaseAdmin().moveRegion(firstRegionInfo.getRegionName(),
        targetServer.getServerAddress().toString());

      // Wait till the region becomes assigned.
      TEST_UTIL.waitForOnlineRegionsToBeAssigned(NUM_REGIONS);
      TEST_UTIL.waitForTableConsistent();

      HRegionLocation staleLocationAfterMoving =
        server.getLocation(firstRegionInfo.getTableDesc().getName(),
          firstRegionInfo.getStartKey(), false);

      LOG.info("As per (stale) cache, region: " +
        firstRegionInfo.getRegionNameAsString() +
        " was located at " +
        staleLocationAfterMoving.getServerAddress().toString());

      // Getting the location after reloading the cache.
      HRegionLocation newLocationAfterMoving =
        server.getLocation(firstRegionInfo.getTableDesc().getName(),
        firstRegionInfo.getStartKey(), true);

      LOG.info("As per (fresh) cache, region: " +
        firstRegionInfo.getRegionNameAsString() +
        " was located at " +
        newLocationAfterMoving.getServerAddress().toString());

      // The new location after reloading the cache, should be the same as
      // what we expect.
      Assert.assertEquals(
        "getLocation() returned a stale server location for region: " +
          firstRegionInfo.getRegionNameAsString(),
        targetServer.getServerAddress(),
        newLocationAfterMoving.getServerAddress()
      );
    }
  }

}
