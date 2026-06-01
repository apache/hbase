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
package org.apache.hadoop.hbase.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.OnlineRegions;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@Tag(MiscTests.TAG)
@Tag(LargeTests.TAG)
public class TestTestingHBaseClusterImplForCPs {

  private static TestingHBaseCluster CLUSTER;

  private static TableName NAME = TableName.valueOf("test");

  private static byte[] CF = Bytes.toBytes("cf");

  private static Connection CONN;

  private static Admin ADMIN;

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    CLUSTER = TestingHBaseCluster.create(TestingHBaseClusterOption.builder().numMasters(2)
      .numRegionServers(3).numDataNodes(3).build());
    CLUSTER.start();
    CONN = ConnectionFactory.createConnection(CLUSTER.getConf());
    ADMIN = CONN.getAdmin();
    ADMIN.createTable(TableDescriptorBuilder.newBuilder(NAME)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF)).build());
    ADMIN.balancerSwitch(false, true);
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    Closeables.close(ADMIN, true);
    Closeables.close(CONN, true);
    if (CLUSTER.isClusterRunning()) {
      CLUSTER.stop();
    }
  }

  @Test
  public void testGetRegion() throws IOException {
    List<RegionInfo> infos = ADMIN.getRegions(NAME);
    assertEquals(1, infos.size());
    RegionInfo info = infos.get(0);
    Region region = CLUSTER.getRegion(info).get();
    ServerName loc;
    try (RegionLocator locator = CONN.getRegionLocator(NAME)) {
      loc = locator.getRegionLocation(info.getStartKey()).getServerName();
    }
    OnlineRegions onlineRegionsInterface = CLUSTER.getOnlineRegionsInterface(loc).get();
    List<? extends Region> regions = onlineRegionsInterface.getRegions(NAME);
    assertEquals(1, regions.size());
    assertSame(region, regions.get(0));

    assertFalse(CLUSTER
      .getRegion(RegionInfoBuilder.newBuilder(TableName.valueOf("whatever")).build()).isPresent());
    assertFalse(CLUSTER.getOnlineRegionsInterface(ServerName.valueOf("whatever,1,1")).isPresent());
  }
}
