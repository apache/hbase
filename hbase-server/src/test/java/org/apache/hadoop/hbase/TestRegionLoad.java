/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase;

import static org.junit.Assert.*;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@Category({MiscTests.class, MediumTests.class})
public class TestRegionLoad {

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static Admin admin;

  private static final TableName TABLE_1 = TableName.valueOf("table_1");
  private static final TableName TABLE_2 = TableName.valueOf("table_2");
  private static final TableName TABLE_3 = TableName.valueOf("table_3");
  private static final TableName[] tables = new TableName[]{TABLE_1, TABLE_2, TABLE_3};

  @BeforeClass
  public static void beforeClass() throws Exception {
    UTIL.startMiniCluster(4);
    admin = UTIL.getAdmin();
    admin.setBalancerRunning(false, true);
    createTables();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    for (TableName table : tables) {
      UTIL.deleteTableIfAny(table);
    }
    UTIL.shutdownMiniCluster();
  }

  private static void createTables() throws IOException, InterruptedException {
    byte[] FAMILY = Bytes.toBytes("f");
    for (TableName tableName : tables) {
      Table table = UTIL.createMultiRegionTable(tableName, FAMILY, 16);
      UTIL.waitTableAvailable(tableName);
      UTIL.loadTable(table, FAMILY);
    }
  }

  @Test
  public void testRegionLoad() throws Exception {

    // Check if regions match with the regionLoad from the server
    for (ServerName serverName : admin.getClusterStatus().getServers()) {
      List<HRegionInfo> regions = admin.getOnlineRegions(serverName);
      Collection<RegionLoad> regionLoads = admin.getRegionLoad(serverName).values();
      checkRegionsAndRegionLoads(regions, regionLoads);
    }

    // Check if regionLoad matches the table's regions and nothing is missed
    for (TableName table : new TableName[]{TABLE_1, TABLE_2, TABLE_3}) {
      List<HRegionInfo> tableRegions = admin.getTableRegions(table);

      List<RegionLoad> regionLoads = Lists.newArrayList();
      for (ServerName serverName : admin.getClusterStatus().getServers()) {
        regionLoads.addAll(admin.getRegionLoad(serverName, table).values());
      }
      checkRegionsAndRegionLoads(tableRegions, regionLoads);
    }

    // Check RegionLoad matches the regionLoad from ClusterStatus
    ClusterStatus clusterStatus = admin.getClusterStatus();
    for (ServerName serverName : clusterStatus.getServers()) {
      ServerLoad serverLoad = clusterStatus.getLoad(serverName);
      Map<byte[], RegionLoad> regionLoads = admin.getRegionLoad(serverName);
      compareRegionLoads(serverLoad.getRegionsLoad(), regionLoads);
    }
  }

  private void compareRegionLoads(Map<byte[], RegionLoad> regionLoadCluster,
      Map<byte[], RegionLoad> regionLoads) {

    assertEquals("No of regionLoads from clusterStatus and regionloads from RS doesn't match",
        regionLoadCluster.size(), regionLoads.size());

    // The contents of region load from cluster and server should match
    for (byte[] regionName : regionLoadCluster.keySet()) {
      regionLoads.remove(regionName);
    }
    assertEquals("regionLoads from SN should be empty", 0, regionLoads.size());
  }

  private void checkRegionsAndRegionLoads(Collection<HRegionInfo> regions,
      Collection<RegionLoad> regionLoads) {

    assertEquals("No of regions and regionloads doesn't match",
        regions.size(), regionLoads.size());

    Map<byte[], RegionLoad> regionLoadMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (RegionLoad regionLoad : regionLoads) {
      regionLoadMap.put(regionLoad.getName(), regionLoad);
    }
    for (HRegionInfo info : regions) {
      assertTrue("Region not in regionLoadMap region:" + info.getRegionNameAsString() +
          " regionMap: " + regionLoadMap, regionLoadMap.containsKey(info.getRegionName()));
    }
  }
}
