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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;

@Category({MediumTests.class})
public class TestHDFSBlocksDistributionRPC {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHDFSBlocksDistributionRPC.class);


  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final Configuration conf = UTIL.getConfiguration();
  private static final int SLAVES = 4;

  private static final TableName TABLE_1 = TableName.valueOf("table_1");
  private static final TableName TABLE_2 = TableName.valueOf("table_2");
  private static final TableName TABLE_3 = TableName.valueOf("table_3");
  private static final TableName[] tables = new TableName[]{TABLE_1, TABLE_2, TABLE_3};
  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final int REGIONS = 16;

  private static Admin admin;

  @BeforeClass
  public static void beforeClass() throws Exception {
    conf.set(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, ""+ SLAVES);
    UTIL.startMiniCluster(SLAVES);
    UTIL.getDFSCluster().waitClusterUp();
    UTIL.getMiniHBaseCluster().waitForActiveAndReadyMaster();
    admin = UTIL.getAdmin();
    admin.balancerSwitch(false, true);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    for (TableName table : tables) {
      UTIL.deleteTable(table);
    }
    UTIL.shutdownMiniCluster();
  }

  private static void createTables() throws IOException {
    for (TableName tableName : tables) {
      UTIL.createMultiRegionTable(tableName, FAMILY, REGIONS);
      UTIL.waitUntilAllRegionsAssigned(tableName);
      assertEquals("Region mismatch", REGIONS,
          admin.getConnection().getRegionLocator(tableName).getAllRegionLocations().size());
      UTIL.loadTable(admin.getConnection().getTable(tableName), FAMILY);
      UTIL.flush(tableName);
    }
  }

  @Test
  public void testHDFSBlockDistribution() throws Exception {

    createTables();

    // Wait until region server report is done.
    int rsHeartBeatInterval = conf.getInt("hbase.regionserver.msginterval", 3 * 1000);
    Thread.sleep(2 * rsHeartBeatInterval);

    // Check if regions match with the hdfsblock dist from the server
    ClusterMetrics clusterMetrics = admin.getClusterMetrics();
    for (ServerName sn : clusterMetrics.getServersName()) {
      List<RegionInfo> regions = admin.getRegions(sn);
      Map<byte[], HDFSBlocksDistribution> blockDistributionMap = getHDFSBlockDistribution(sn);
      checkRegionsAndBlockDist(sn, regions, blockDistributionMap,
          clusterMetrics.getLiveServerMetrics().get(sn));
    }
  }

  private void checkRegionsAndBlockDist(ServerName sn, Collection<RegionInfo> regions,
      Map<byte[], HDFSBlocksDistribution> blockDistributionMap, ServerMetrics load)
      throws IOException {

    assertEquals("No of regions and block dist map doesn't match",
        regions.size(), blockDistributionMap.size());

    for (RegionInfo info : regions) {
      assertTrue("Region not in block dist map:" + info.getRegionNameAsString() +
              " regionMap: " + blockDistributionMap,
          blockDistributionMap.containsKey(info.getRegionName()));

      HDFSBlocksDistribution proto = blockDistributionMap.get(info.getRegionName());

      // Check protobuf object's locality with that from cluster metrics.
      float locality = load.getRegionMetrics().get(info.getRegionName()).getDataLocality();
      assertEquals("Locality from cluster metrics and getBlockDist doesn't match: "
          + info.getRegionNameAsString(),
          locality, proto.getBlockLocalityIndex(sn.getHostname()), 0);

      if (locality > 0) {
        assertTrue("No host and weight present for region: " + info.getRegionNameAsString(),
            proto.getHostAndWeights().size() > 0);
      }

      // Compute block dist and check if all hosts and weight match.
      HDFSBlocksDistribution computed = computeBlockDistribution(info);
      assertEquals("Host and Weight does not match for " + info.getRegionNameAsString(),
          computed.getHostAndWeights().size(), proto.getHostAndWeights().size());
      assertEquals("Total weight doesn't match for " + info.getRegionNameAsString(),
          computed.getUniqueBlocksTotalWeight(), proto.getUniqueBlocksTotalWeight());

      for (HDFSBlocksDistribution.HostAndWeight hostAndWeight : computed.getTopHostsWithWeights()) {
        HDFSBlocksDistribution.HostAndWeight protoHAndW =
            proto.getHostAndWeights().get(hostAndWeight.getHost());
        assertEquals("Weight doesn't match", hostAndWeight.getWeight(), protoHAndW.getWeight());
      }
    }
  }

  private HDFSBlocksDistribution computeBlockDistribution(RegionInfo info) throws IOException {
    return HRegion.computeHDFSBlocksDistribution(UTIL.getConfiguration(),
        admin.getDescriptor(info.getTable()), info);
  }

  private Map<byte[], HDFSBlocksDistribution> getHDFSBlockDistribution(
      final ServerName sn) throws IOException {

    HRegionServer regionServer = UTIL.getHBaseCluster().getRegionServer(sn);
    List<AdminProtos.HDFSBlocksDistribution> blockDistribution =
        ProtobufUtil.getHDFSBlockDistribution(null, regionServer.getRSRpcServices());
    Map<byte[], HDFSBlocksDistribution> blkDistMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (AdminProtos.HDFSBlocksDistribution hdfsBlockDistribution : blockDistribution) {
      byte[] regionName = hdfsBlockDistribution.getRegion().getValue().toByteArray();
      blkDistMap.put(regionName,
          HDFSBlocksDistribution.convertBlockDistribution(hdfsBlockDistribution));
    }
    return blkDistMap;
  }
}