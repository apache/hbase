/*
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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, MediumTests.class })
public class TestRegionLocationFinder {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionLocationFinder.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static MiniHBaseCluster cluster;

  private final static TableName tableName = TableName.valueOf("table");
  private final static byte[] FAMILY = Bytes.toBytes("cf");
  private static Table table;
  private final static int ServerNum = 5;

  private static RegionLocationFinder finder = new RegionLocationFinder();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    cluster = TEST_UTIL.startMiniCluster(ServerNum);
    table = TEST_UTIL.createTable(tableName, FAMILY, HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE);
    TEST_UTIL.waitTableAvailable(tableName, 1000);
    TEST_UTIL.loadTable(table, FAMILY);

    for (int i = 0; i < ServerNum; i++) {
      HRegionServer server = cluster.getRegionServer(i);
      for (HRegion region : server.getRegions(tableName)) {
        region.flush(true);
      }
    }

    finder.setConf(TEST_UTIL.getConfiguration());
    finder.setServices(cluster.getMaster());
    finder.setClusterMetrics(cluster.getMaster().getClusterMetrics());
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
      for (HRegion region : server.getRegions(tableName)) {
        // get region's hdfs block distribution by region and RegionLocationFinder,
        // they should have same result
        HDFSBlocksDistribution blocksDistribution1 = region.getHDFSBlocksDistribution();
        HDFSBlocksDistribution blocksDistribution2 =
          finder.getBlockDistribution(region.getRegionInfo());
        assertEquals(blocksDistribution1.getUniqueBlocksTotalWeight(),
          blocksDistribution2.getUniqueBlocksTotalWeight());
        if (blocksDistribution1.getUniqueBlocksTotalWeight() != 0) {
          assertEquals(blocksDistribution1.getTopHosts().get(0),
            blocksDistribution2.getTopHosts().get(0));
        }
      }
    }
  }

  @Test
  public void testMapHostNameToServerName() throws Exception {
    List<String> topHosts = new ArrayList<>();
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
      for (HRegion region : server.getRegions(tableName)) {
        List<ServerName> servers = finder.getTopBlockLocations(region.getRegionInfo());
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
      List<HRegion> regions = server.getRegions(tableName);
      if (regions.size() <= 0) {
        continue;
      }
      List<RegionInfo> regionInfos = new ArrayList<>(regions.size());
      for (HRegion region : regions) {
        regionInfos.add(region.getRegionInfo());
      }
      finder.refreshAndWait(regionInfos);
      for (RegionInfo regionInfo : regionInfos) {
        assertNotNull(finder.getCache().getIfPresent(regionInfo));
      }
    }
  }

  @Test
  public void testRefreshRegionsWithChangedLocality() throws InterruptedException {
    TableDescriptor table =
      TableDescriptorBuilder.newBuilder(TableName.valueOf("RegionLocationFinder")).build();

    int numRegions = 100;
    List<RegionInfo> regions = new ArrayList<>(numRegions);

    for (int i = 1; i <= numRegions; i++) {
      byte[] startKey = i == 0 ? HConstants.EMPTY_START_ROW : Bytes.toBytes(i);
      byte[] endKey = i == numRegions ? HConstants.EMPTY_BYTE_ARRAY : Bytes.toBytes(i + 1);
      RegionInfo region = RegionInfoBuilder.newBuilder(table.getTableName()).setStartKey(startKey)
        .setEndKey(endKey).build();
      regions.add(region);
    }

    ServerName testServer = ServerName.valueOf("host-0", 12345, 12345);
    RegionInfo testRegion = regions.get(0);

    RegionLocationFinder finder = new RegionLocationFinder() {
      @Override
      protected HDFSBlocksDistribution internalGetTopBlockLocation(RegionInfo region) {
        return generate(region);
      }
    };

    // cache for comparison later
    Map<RegionInfo, HDFSBlocksDistribution> cache = new HashMap<>();
    for (RegionInfo region : regions) {
      HDFSBlocksDistribution hbd = finder.getBlockDistribution(region);
      cache.put(region, hbd);
    }

    finder
      .setClusterMetrics(getMetricsWithLocality(testServer, testRegion.getRegionName(), 0.123f));

    // everything should be same as cached, because metrics were null before
    for (RegionInfo region : regions) {
      HDFSBlocksDistribution hbd = finder.getBlockDistribution(region);
      assertSame(cache.get(region), hbd);
    }

    finder
      .setClusterMetrics(getMetricsWithLocality(testServer, testRegion.getRegionName(), 0.345f));

    // cache refresh happens in a background thread, so we need to wait for the value to
    // update before running assertions.
    long now = System.currentTimeMillis();
    HDFSBlocksDistribution cached = cache.get(testRegion);
    HDFSBlocksDistribution newValue;
    do {
      Thread.sleep(1_000);
      newValue = finder.getBlockDistribution(testRegion);
    } while (cached == newValue && System.currentTimeMillis() - now < 30_000);

    // locality changed just for our test region, so it should no longer be the same
    for (RegionInfo region : regions) {
      HDFSBlocksDistribution hbd = finder.getBlockDistribution(region);
      if (region.equals(testRegion)) {
        assertNotSame(cache.get(region), hbd);
      } else {
        assertSame(cache.get(region), hbd);
      }
    }
  }

  private static HDFSBlocksDistribution generate(RegionInfo region) {
    HDFSBlocksDistribution distribution = new HDFSBlocksDistribution();
    int seed = region.hashCode();
    Random rand = new Random(seed);
    int size = 1 + rand.nextInt(10);
    for (int i = 0; i < size; i++) {
      distribution.addHostsAndBlockWeight(new String[] { "host-" + i }, 1 + rand.nextInt(100));
    }
    return distribution;
  }

  private ClusterMetrics getMetricsWithLocality(ServerName serverName, byte[] region,
    float locality) {
    RegionMetrics regionMetrics = mock(RegionMetrics.class);
    when(regionMetrics.getDataLocality()).thenReturn(locality);

    Map<byte[], RegionMetrics> regionMetricsMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    regionMetricsMap.put(region, regionMetrics);

    ServerMetrics serverMetrics = mock(ServerMetrics.class);
    when(serverMetrics.getRegionMetrics()).thenReturn(regionMetricsMap);

    Map<ServerName, ServerMetrics> serverMetricsMap = new HashMap<>();
    serverMetricsMap.put(serverName, serverMetrics);

    ClusterMetrics metrics = mock(ClusterMetrics.class);
    when(metrics.getLiveServerMetrics()).thenReturn(serverMetricsMap);

    return metrics;
  }
}
