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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Maps;

@Category({ MiscTests.class, MediumTests.class })
public class TestRegionMetrics {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionMetrics.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static Admin admin;

  private static final TableName TABLE_1 = TableName.valueOf("table_1");
  private static final TableName TABLE_2 = TableName.valueOf("table_2");
  private static final TableName TABLE_3 = TableName.valueOf("table_3");
  private static final TableName[] tables = new TableName[] { TABLE_1, TABLE_2, TABLE_3 };

  @BeforeClass
  public static void beforeClass() throws Exception {
    UTIL.startMiniCluster(4);
    admin = UTIL.getAdmin();
    admin.balancerSwitch(false, true);
    byte[] FAMILY = Bytes.toBytes("f");
    for (TableName tableName : tables) {
      Table table = UTIL.createMultiRegionTable(tableName, FAMILY, 16);
      UTIL.waitTableAvailable(tableName);
      UTIL.loadTable(table, FAMILY);
    }
  }

  @AfterClass
  public static void afterClass() throws Exception {
    for (TableName table : tables) {
      UTIL.deleteTableIfAny(table);
    }
    UTIL.shutdownMiniCluster();
  }


  @Test
  public void testRegionMetrics() throws Exception {

    // Check if regions match with the RegionMetrics from the server
    for (ServerName serverName : admin.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS))
        .getLiveServerMetrics().keySet()) {
      List<RegionInfo> regions = admin.getRegions(serverName);
      Collection<RegionMetrics> regionMetricsList =
          admin.getRegionMetrics(serverName);
      checkRegionsAndRegionMetrics(regions, regionMetricsList);
    }

    // Check if regionMetrics matches the table's regions and nothing is missed
    for (TableName table : new TableName[] { TABLE_1, TABLE_2, TABLE_3 }) {
      List<RegionInfo> tableRegions = admin.getRegions(table);

      List<RegionMetrics> regionMetrics = new ArrayList<>();
      for (ServerName serverName : admin.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS))
          .getLiveServerMetrics().keySet()) {
        regionMetrics.addAll(admin.getRegionMetrics(serverName, table));
      }
      checkRegionsAndRegionMetrics(tableRegions, regionMetrics);
    }

    // Check RegionMetrics matches the RegionMetrics from ClusterStatus
    ClusterMetrics clusterStatus = admin.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS));
    for (Map.Entry<ServerName, ServerMetrics> entry : clusterStatus.getLiveServerMetrics()
        .entrySet()) {
      ServerName serverName = entry.getKey();
      ServerMetrics serverMetrics = entry.getValue();
      List<RegionMetrics> regionMetrics = admin.getRegionMetrics(serverName);
      assertEquals(serverMetrics.getRegionMetrics().size(), regionMetrics.size());
    }
  }

  private void checkRegionsAndRegionMetrics(Collection<RegionInfo> regions,
      Collection<RegionMetrics> regionMetrics) {

    assertEquals("No of regions and regionMetrics doesn't match", regions.size(),
        regionMetrics.size());

    Map<byte[], RegionMetrics> regionMetricsMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (RegionMetrics r : regionMetrics) {
      regionMetricsMap.put(r.getRegionName(), r);
    }
    for (RegionInfo info : regions) {
      assertTrue("Region not in RegionMetricsMap region:"
          + info.getRegionNameAsString() + " regionMap: "
          + regionMetricsMap, regionMetricsMap.containsKey(info.getRegionName()));
    }
  }
}
