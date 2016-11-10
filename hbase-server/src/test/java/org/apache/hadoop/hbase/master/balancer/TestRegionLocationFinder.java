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
package org.apache.hadoop.hbase.master.balancer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MasterTests.class, SmallTests.class})
public class TestRegionLocationFinder {
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static MiniHBaseCluster cluster;

  private final static TableName tableName = TableName.valueOf("table");
  private final static byte[] FAMILY = Bytes.toBytes("cf");
  private static Table table;
  private final static int ServerNum = 5;

  private static RegionLocationFinder finder = new RegionLocationFinder();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    cluster = TEST_UTIL.startMiniCluster(1, ServerNum);
    table = TEST_UTIL.createTable(tableName, FAMILY, HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE);
    TEST_UTIL.waitTableAvailable(tableName, 1000);
    TEST_UTIL.loadTable(table, FAMILY);

    for (int i = 0; i < ServerNum; i++) {
      HRegionServer server = cluster.getRegionServer(i);
      for (Region region : server.getOnlineRegions(tableName)) {
        region.flush(true);
      }
    }

    finder.setConf(TEST_UTIL.getConfiguration());
    finder.setServices(cluster.getMaster());
    finder.setClusterStatus(cluster.getMaster().getClusterStatus());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    table.close();
    TEST_UTIL.deleteTable(tableName);
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testInternalGetTopBlockLocation() throws Exception {
    for (int i = 0; i < ServerNum; i++) {
      HRegionServer server = cluster.getRegionServer(i);
      for (Region region : server.getOnlineRegions(tableName)) {
        // get region's hdfs block distribution by region and RegionLocationFinder, 
        // they should have same result
        HDFSBlocksDistribution blocksDistribution1 = region.getHDFSBlocksDistribution();
        HDFSBlocksDistribution blocksDistribution2 = finder.getBlockDistribution(region
            .getRegionInfo());
        assertEquals(blocksDistribution1.getUniqueBlocksTotalWeight(),
          blocksDistribution2.getUniqueBlocksTotalWeight());
        if (blocksDistribution1.getUniqueBlocksTotalWeight() != 0) {
          assertEquals(blocksDistribution1.getTopHosts().get(0), blocksDistribution2.getTopHosts()
              .get(0));
        }
      }
    }
  }

  @Test
  public void testMapHostNameToServerName() throws Exception {
    List<String> topHosts = new ArrayList<String>();
    for (int i = 0; i < ServerNum; i++) {
      HRegionServer server = cluster.getRegionServer(i);
      String serverHost = server.getServerName().getHostname();
      if (!topHosts.contains(serverHost)) {
        topHosts.add(serverHost);
      }
    }
    List<ServerName> servers = finder.mapHostNameToServerName(topHosts);
    // mini cluster, all rs in one host
    assertEquals(1, topHosts.size());
    for (int i = 0; i < ServerNum; i++) {
      ServerName server = cluster.getRegionServer(i).getServerName();
      assertTrue(servers.contains(server));
    }
  }

  @Test
  public void testGetTopBlockLocations() throws Exception {
    for (int i = 0; i < ServerNum; i++) {
      HRegionServer server = cluster.getRegionServer(i);
      for (Region region : server.getOnlineRegions(tableName)) {
        List<ServerName> servers = finder.getTopBlockLocations(region
            .getRegionInfo());
        // test table may have empty region
        if (region.getHDFSBlocksDistribution().getUniqueBlocksTotalWeight() == 0) {
          continue;
        }
        List<String> topHosts = region.getHDFSBlocksDistribution().getTopHosts();
        // rs and datanode may have different host in local machine test
        if (!topHosts.contains(server.getServerName().getHostname())) {
          continue;
        }
        for (int j = 0; j < ServerNum; j++) {
          ServerName serverName = cluster.getRegionServer(j).getServerName();
          assertTrue(servers.contains(serverName));
        }
      }
    }
  }

  @Test
  public void testRefreshAndWait() throws Exception {
    finder.getCache().invalidateAll();
    for (int i = 0; i < ServerNum; i++) {
      HRegionServer server = cluster.getRegionServer(i);
      List<Region> regions = server.getOnlineRegions(tableName);
      if (regions.size() <= 0) {
        continue;
      }
      List<HRegionInfo> regionInfos = new ArrayList<HRegionInfo>(regions.size());
      for (Region region : regions) {
        regionInfos.add(region.getRegionInfo());
      }
      finder.refreshAndWait(regionInfos);
      for (HRegionInfo regionInfo : regionInfos) {
        assertNotNull(finder.getCache().getIfPresent(regionInfo));
      }
    }
  }
}
