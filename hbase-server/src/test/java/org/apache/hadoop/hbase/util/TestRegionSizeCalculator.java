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
package org.apache.hadoop.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(SmallTests.class)
public class TestRegionSizeCalculator {

  private Configuration configuration = new Configuration();
  private final long megabyte = 1024L * 1024L;

  @Test
  public void testSimpleTestCase() throws Exception {

    HTable table = mockTable("region1", "region2", "region3");

    HBaseAdmin admin = mockAdmin(
      mockServer(
        mockRegion("region1", 123),
        mockRegion("region3", 1232)
      ),
      mockServer(
        mockRegion("region2",  54321),
        mockRegion("otherTableRegion", 110)
      )
    );

    RegionSizeCalculator calculator = new RegionSizeCalculator(table, admin);

    assertEquals(123 * megabyte, calculator.getRegionSize("region1".getBytes()));
    assertEquals(54321 * megabyte, calculator.getRegionSize("region2".getBytes()));
    assertEquals(1232 * megabyte, calculator.getRegionSize("region3".getBytes()));
    // if region is not inside our table, it should return 0
    assertEquals(0 * megabyte, calculator.getRegionSize("otherTableRegion".getBytes()));

    assertEquals(3, calculator.getRegionSizeMap().size());
  }


  /**
   * When size of region in megabytes is larger than largest possible integer there could be
   * error caused by lost of precision.
   * */
  @Test
  public void testLargeRegion() throws Exception {

    HTable table = mockTable("largeRegion");

    HBaseAdmin admin = mockAdmin(
      mockServer(
        mockRegion("largeRegion", Integer.MAX_VALUE)
      )
    );

    RegionSizeCalculator calculator = new RegionSizeCalculator(table, admin);

    assertEquals(((long) Integer.MAX_VALUE) * megabyte, calculator.getRegionSize("largeRegion".getBytes()));
  }

  /** When calculator is disabled, it should return 0 for each request.*/
  @Test
  public void testDisabled() throws Exception {
    String regionName = "cz.goout:/index.html";
    HTable table = mockTable(regionName);

    HBaseAdmin admin = mockAdmin(
      mockServer(
        mockRegion(regionName, 999)
      )
    );

    //first request on enabled calculator
    RegionSizeCalculator calculator = new RegionSizeCalculator(table, admin);
    assertEquals(999 * megabyte, calculator.getRegionSize(regionName.getBytes()));

    //then disabled calculator.
    configuration.setBoolean(RegionSizeCalculator.ENABLE_REGIONSIZECALCULATOR, false);
    RegionSizeCalculator disabledCalculator = new RegionSizeCalculator(table, admin);
    assertEquals(0 * megabyte, disabledCalculator.getRegionSize(regionName.getBytes()));

    assertEquals(0, disabledCalculator.getRegionSizeMap().size());
  }

  /**
   * Makes some table with given region names.
   * */
  private HTable mockTable(String... regionNames) throws IOException {
    HTable mockedTable = Mockito.mock(HTable.class);
    when(mockedTable.getConfiguration()).thenReturn(configuration);
    when(mockedTable.getTableName()).thenReturn("sizeTestTable".getBytes());
    NavigableMap<HRegionInfo, ServerName> regionLocations = new TreeMap<HRegionInfo, ServerName>();
    when(mockedTable.getRegionLocations()).thenReturn(regionLocations);

    for (String regionName : regionNames) {
      HRegionInfo info = Mockito.mock(HRegionInfo.class);
      when(info.getRegionName()).thenReturn(regionName.getBytes());
      regionLocations.put(info, null);//we are not interested in values
    }

    return mockedTable;
  }

  /**
   * Creates mock returing ClusterStatus info about given servers.
  */
  private HBaseAdmin mockAdmin(ServerLoad... servers) throws Exception {
    //get clusterstatus
    HBaseAdmin mockAdmin = Mockito.mock(HBaseAdmin.class);
    ClusterStatus clusterStatus = mockCluster(servers);
    when(mockAdmin.getClusterStatus()).thenReturn(clusterStatus);
    return mockAdmin;
  }

  /**
   * Creates mock of region with given name and size.
   *
   * @param  fileSizeMb number of megabytes occupied by region in file store in megabytes
   * */
  private RegionLoad mockRegion(String regionName, int fileSizeMb) {
    RegionLoad region = Mockito.mock(RegionLoad.class);
    when(region.getName()).thenReturn(regionName.getBytes());
    when(region.getNameAsString()).thenReturn(regionName);
    when(region.getStorefileSizeMB()).thenReturn(fileSizeMb);
    return region;
  }

  private ClusterStatus mockCluster(ServerLoad[] servers) {
    List<ServerName> serverNames = new ArrayList<ServerName>();

    ClusterStatus clusterStatus = Mockito.mock(ClusterStatus.class);
    when(clusterStatus.getServers()).thenReturn(serverNames);

    int serverCounter = 0;
    for (ServerLoad server : servers) {
      ServerName serverName = mock(ServerName.class);
      when(serverName.getServerName()).thenReturn("server" + (serverCounter++));
      serverNames.add(serverName);
      when(clusterStatus.getLoad(serverName)).thenReturn(server);
    }

    return clusterStatus;
  }

  /** Creates mock of region server with given regions*/
  private ServerLoad mockServer(RegionLoad... regions) {
    ServerLoad serverLoad = Mockito.mock(ServerLoad.class);
    Map<byte[], RegionLoad> regionMap = new TreeMap<byte[], RegionLoad>(Bytes.BYTES_COMPARATOR);

    for (RegionLoad regionName : regions) {
      regionMap.put(regionName.getName(), regionName);
    }

    when(serverLoad.getRegionsLoad()).thenReturn(regionMap);
    return serverLoad;
  }

}
