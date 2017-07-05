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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@RunWith(Parameterized.class)
@Category({ MiscTests.class, MediumTests.class })
public class TestAsyncClusterAdminApi extends TestAsyncAdminBase {

  @Test
  public void testRegionLoad() throws Exception {
    // Turn off the balancer
    admin.setBalancerOn(false).join();
    TableName[] tables =
        new TableName[] { TableName.valueOf(tableName.getNameAsString() + "1"),
            TableName.valueOf(tableName.getNameAsString() + "2"),
            TableName.valueOf(tableName.getNameAsString() + "3") };
    createAndLoadTable(tables);
    // Check if regions match with the regionLoad from the server
    Collection<ServerName> servers = admin.getRegionServers().get();
    for (ServerName serverName : servers) {
      List<HRegionInfo> regions = admin.getOnlineRegions(serverName).get();
      checkRegionsAndRegionLoads(regions, admin.getRegionLoads(serverName).get());
    }

    // Check if regionLoad matches the table's regions and nothing is missed
    for (TableName table : tables) {
      List<HRegionInfo> tableRegions = admin.getTableRegions(table).get();
      List<RegionLoad> regionLoads = Lists.newArrayList();
      for (ServerName serverName : servers) {
        regionLoads.addAll(admin.getRegionLoads(serverName, Optional.of(table)).get());
      }
      checkRegionsAndRegionLoads(tableRegions, regionLoads);
    }

    // Check RegionLoad matches the regionLoad from ClusterStatus
    ClusterStatus clusterStatus = admin.getClusterStatus().get();
    for (ServerName serverName : clusterStatus.getServers()) {
      ServerLoad serverLoad = clusterStatus.getLoad(serverName);
      compareRegionLoads(serverLoad.getRegionsLoad().values(), admin.getRegionLoads(serverName)
          .get());
    }
  }

  private void compareRegionLoads(Collection<RegionLoad> regionLoadCluster,
      Collection<RegionLoad> regionLoads) {

    assertEquals("No of regionLoads from clusterStatus and regionloads from RS doesn't match",
      regionLoadCluster.size(), regionLoads.size());

    for (RegionLoad loadCluster : regionLoadCluster) {
      boolean matched = false;
      for (RegionLoad load : regionLoads) {
        if (Bytes.equals(loadCluster.getName(), load.getName())) {
          matched = true;
          continue;
        }
      }
      assertTrue("The contents of region load from cluster and server should match", matched);
    }
  }

  private void checkRegionsAndRegionLoads(Collection<HRegionInfo> regions,
      Collection<RegionLoad> regionLoads) {

    assertEquals("No of regions and regionloads doesn't match", regions.size(), regionLoads.size());

    Map<byte[], RegionLoad> regionLoadMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (RegionLoad regionLoad : regionLoads) {
      regionLoadMap.put(regionLoad.getName(), regionLoad);
    }
    for (HRegionInfo info : regions) {
      assertTrue("Region not in regionLoadMap region:" + info.getRegionNameAsString()
          + " regionMap: " + regionLoadMap, regionLoadMap.containsKey(info.getRegionName()));
    }
  }

  private void createAndLoadTable(TableName[] tables) {
    for (TableName table : tables) {
      TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(table);
      builder.addColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).build());
      admin.createTable(builder.build(), Bytes.toBytes("aaaaa"), Bytes.toBytes("zzzzz"), 16).join();
      RawAsyncTable asyncTable = ASYNC_CONN.getRawTable(table);
      List<Put> puts = new ArrayList<>();
      for (byte[] row : HBaseTestingUtility.ROWS) {
        puts.add(new Put(row).addColumn(FAMILY, Bytes.toBytes("q"), Bytes.toBytes("v")));
      }
      asyncTable.putAll(puts).join();
    }
  }
}
