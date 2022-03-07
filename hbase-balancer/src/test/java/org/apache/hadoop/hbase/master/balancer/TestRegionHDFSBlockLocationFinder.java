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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.HDFSBlocksDistribution.HostAndWeight;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, SmallTests.class })
public class TestRegionHDFSBlockLocationFinder {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionHDFSBlockLocationFinder.class);

  private static final Random RNG = new Random(); // This test depends on Random#setSeed
  private static TableDescriptor TD;
  private static List<RegionInfo> REGIONS;

  private RegionHDFSBlockLocationFinder finder;

  private static HDFSBlocksDistribution generate(RegionInfo region) {
    HDFSBlocksDistribution distribution = new HDFSBlocksDistribution();
    int seed = region.hashCode();
    RNG.setSeed(seed);
    int size = 1 + RNG.nextInt(10);
    for (int i = 0; i < size; i++) {
      distribution.addHostsAndBlockWeight(new String[] { "host-" + i }, 1 + RNG.nextInt(100));
    }
    return distribution;
  }

  @BeforeClass
  public static void setUpBeforeClass() {
    TD = TableDescriptorBuilder.newBuilder(TableName.valueOf("RegionLocationFinder")).build();
    int numRegions = 100;
    REGIONS = new ArrayList<>(numRegions);
    for (int i = 1; i <= numRegions; i++) {
      byte[] startKey = i == 0 ? HConstants.EMPTY_START_ROW : Bytes.toBytes(i);
      byte[] endKey = i == numRegions ? HConstants.EMPTY_BYTE_ARRAY : Bytes.toBytes(i + 1);
      RegionInfo region = RegionInfoBuilder.newBuilder(TD.getTableName()).setStartKey(startKey)
        .setEndKey(endKey).build();
      REGIONS.add(region);
    }
  }

  @Before
  public void setUp() {
    finder = new RegionHDFSBlockLocationFinder();
    finder.setClusterInfoProvider(new DummyClusterInfoProvider(null) {

      @Override
      public TableDescriptor getTableDescriptor(TableName tableName) throws IOException {
        return TD;
      }

      @Override
      public List<RegionInfo> getAssignedRegions() {
        return REGIONS;
      }

      @Override
      public HDFSBlocksDistribution computeHDFSBlocksDistribution(Configuration conf,
        TableDescriptor tableDescriptor, RegionInfo regionInfo) throws IOException {
        return generate(regionInfo);
      }
    });
  }

  @Test
  public void testMapHostNameToServerName() throws Exception {
    assertTrue(finder.mapHostNameToServerName(null).isEmpty());

    List<String> hosts = new ArrayList<>();
    for (int i = 0; i < 10; i += 2) {
      hosts.add("host-" + i);
    }
    assertTrue(finder.mapHostNameToServerName(hosts).isEmpty());

    Map<ServerName, ServerMetrics> serverMetrics = new HashMap<>();
    for (int i = 0; i < 10; i += 2) {
      ServerName sn = ServerName.valueOf("host-" + i, 12345, 12345);
      serverMetrics.put(sn, null);
    }
    ClusterMetrics metrics = mock(ClusterMetrics.class);
    when(metrics.getLiveServerMetrics()).thenReturn(serverMetrics);

    finder.setClusterMetrics(metrics);
    List<ServerName> sns = finder.mapHostNameToServerName(hosts);
    assertEquals(5, sns.size());
    for (int i = 0; i < 5; i++) {
      ServerName sn = sns.get(i);
      assertEquals("host-" + (2 * i), sn.getHostname());
      assertEquals(12345, sn.getPort());
      assertEquals(12345, sn.getStartcode());
    }
  }

  @Test
  public void testRefreshAndWait() throws Exception {
    finder.getCache().invalidateAll();
    for (RegionInfo region : REGIONS) {
      assertNull(finder.getCache().getIfPresent(region));
    }
    finder.refreshAndWait(REGIONS);
    for (RegionInfo region : REGIONS) {
      assertNotNull(finder.getCache().getIfPresent(region));
    }
  }

  private void assertHostAndWeightEquals(HDFSBlocksDistribution expected,
    HDFSBlocksDistribution actual) {
    Map<String, HostAndWeight> expectedMap = expected.getHostAndWeights();
    Map<String, HostAndWeight> actualMap = actual.getHostAndWeights();
    assertEquals(expectedMap.size(), actualMap.size());
    expectedMap.forEach((k, expectedHostAndWeight) -> {
      HostAndWeight actualHostAndWeight = actualMap.get(k);
      assertEquals(expectedHostAndWeight.getHost(), actualHostAndWeight.getHost());
      assertEquals(expectedHostAndWeight.getWeight(), actualHostAndWeight.getWeight());
      assertEquals(expectedHostAndWeight.getWeightForSsd(), actualHostAndWeight.getWeightForSsd());
    });
  }

  @Test
  public void testGetBlockDistribution() {
    Map<RegionInfo, HDFSBlocksDistribution> cache = new HashMap<>();
    for (RegionInfo region : REGIONS) {
      HDFSBlocksDistribution hbd = finder.getBlockDistribution(region);
      assertHostAndWeightEquals(generate(region), hbd);
      cache.put(region, hbd);
    }
    // the instance should be cached
    for (RegionInfo region : REGIONS) {
      HDFSBlocksDistribution hbd = finder.getBlockDistribution(region);
      assertSame(cache.get(region), hbd);
    }
  }

  @Test
  public void testGetTopBlockLocations() {
    Map<ServerName, ServerMetrics> serverMetrics = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      ServerName sn = ServerName.valueOf("host-" + i, 12345, 12345);
      serverMetrics.put(sn, null);
    }
    ClusterMetrics metrics = mock(ClusterMetrics.class);
    when(metrics.getLiveServerMetrics()).thenReturn(serverMetrics);
    finder.setClusterMetrics(metrics);
    for (RegionInfo region : REGIONS) {
      List<ServerName> servers = finder.getTopBlockLocations(region);
      long previousWeight = Long.MAX_VALUE;
      HDFSBlocksDistribution hbd = generate(region);
      for (ServerName server : servers) {
        long weight = hbd.getWeight(server.getHostname());
        assertTrue(weight <= previousWeight);
        previousWeight = weight;
      }
    }
  }

  @Test
  public void testRefreshRegionsWithChangedLocality() throws InterruptedException {
    ServerName testServer = ServerName.valueOf("host-0", 12345, 12345);
    RegionInfo testRegion = REGIONS.get(0);

    Map<RegionInfo, HDFSBlocksDistribution> cache = new HashMap<>();
    for (RegionInfo region : REGIONS) {
      HDFSBlocksDistribution hbd = finder.getBlockDistribution(region);
      assertHostAndWeightEquals(generate(region), hbd);
      cache.put(region, hbd);
    }

    finder.setClusterMetrics(getMetricsWithLocality(testServer, testRegion.getRegionName(),
      0.123f));

    // everything should be cached, because metrics were null before
    for (RegionInfo region : REGIONS) {
      HDFSBlocksDistribution hbd = finder.getBlockDistribution(region);
      assertSame(cache.get(region), hbd);
    }

    finder.setClusterMetrics(getMetricsWithLocality(testServer, testRegion.getRegionName(),
      0.345f));

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
    for (RegionInfo region : REGIONS) {
      HDFSBlocksDistribution hbd = finder.getBlockDistribution(region);
      if (region.equals(testRegion)) {
        assertNotSame(cache.get(region), hbd);
      } else {
        assertSame(cache.get(region), hbd);
      }
    }
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
