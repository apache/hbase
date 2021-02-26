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

package org.apache.hadoop.hbase.util;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.RackManager;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


@Category({ MiscTests.class, LargeTests.class})
public class TestRegionMover3 {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionMover3.class);

  @Rule
  public TestName name = new TestName();

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static ServerName rs0;
  private static ServerName rs1;
  private static ServerName rs2;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(3);
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    rs0 = cluster.getRegionServer(0).getServerName();
    rs1 = cluster.getRegionServer(1).getServerName();
    rs2 = cluster.getRegionServer(2).getServerName();
    TEST_UTIL.getAdmin().balancerSwitch(false, true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("fam1")).build();
    int startKey = 0;
    int endKey = 80000;
    TEST_UTIL.getAdmin().createTable(tableDesc, Bytes.toBytes(startKey), Bytes.toBytes(endKey), 9);
  }

  @Test
  public void testRegionUnloadWithRack() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    Admin admin = TEST_UTIL.getAdmin();
    Table table = TEST_UTIL.getConnection().getTable(tableName);
    List<Put> puts = IntStream.range(10, 50000)
      .mapToObj(i -> new Put(Bytes.toBytes(i))
      .addColumn(Bytes.toBytes("fam1"), Bytes.toBytes("q1"), Bytes.toBytes("val_" + i)))
      .collect(Collectors.toList());
    table.put(puts);
    admin.flush(tableName);
    admin.compact(tableName);
    Thread.sleep(3000);
    HRegionServer hRegionServer0 = cluster.getRegionServer(0);
    HRegionServer hRegionServer1 = cluster.getRegionServer(1);
    HRegionServer hRegionServer2 = cluster.getRegionServer(2);
    int numRegions0 = hRegionServer0.getNumberOfOnlineRegions();
    int numRegions1 = hRegionServer1.getNumberOfOnlineRegions();
    int numRegions2 = hRegionServer2.getNumberOfOnlineRegions();

    Assert.assertTrue(numRegions0 >= 3);
    Assert.assertTrue(numRegions1 >= 3);
    Assert.assertTrue(numRegions2 >= 3);
    int totalRegions = numRegions0 + numRegions1 + numRegions2;

    // source RS: rs0
    String sourceRSName = rs0.getAddress().toString();

    // move all regions from rs1 to rs0
    for (HRegion region : hRegionServer1.getRegions()) {
      TEST_UTIL.getAdmin().move(region.getRegionInfo().getEncodedNameAsBytes(), rs0);
    }
    TEST_UTIL.waitFor(5000, () -> {
      int newNumRegions0 = hRegionServer0.getNumberOfOnlineRegions();
      int newNumRegions1 = hRegionServer1.getNumberOfOnlineRegions();
      return newNumRegions1 == 0 && newNumRegions0 == (numRegions0 + numRegions1);
    });

    // regionMover obj on rs0. While unloading regions from rs0
    // with default rackManager, which resolves "/default-rack" for each server, no region
    // is moved while using unloadFromRack() as all rs belong to same rack.
    RegionMover.RegionMoverBuilder rmBuilder =
      new RegionMover.RegionMoverBuilder(sourceRSName, TEST_UTIL.getConfiguration())
        .ack(true)
        .maxthreads(8);
    try (RegionMover regionMover = rmBuilder.build()) {
      regionMover.unloadFromRack();
      int newNumRegions0 = hRegionServer0.getNumberOfOnlineRegions();
      int newNumRegions1 = hRegionServer1.getNumberOfOnlineRegions();
      int newNumRegions2 = hRegionServer2.getNumberOfOnlineRegions();
      Assert.assertEquals(0, newNumRegions1);
      Assert.assertEquals(totalRegions, newNumRegions0 + newNumRegions2);
    }

    // use custom rackManager, which resolves "rack-1" for rs0 and rs1,
    // while "rack-2" for rs2. Hence, unloadFromRack() from rs0 should move all
    // regions that belong to rs0 to rs2 only, and nothing should be moved to rs1
    // as rs0 and rs1 belong to same rack.
    rmBuilder.rackManager(new MockRackManager());
    try (RegionMover regionMover = rmBuilder.build()) {
      regionMover.unloadFromRack();
      int newNumRegions0 = hRegionServer0.getNumberOfOnlineRegions();
      int newNumRegions1 = hRegionServer1.getNumberOfOnlineRegions();
      int newNumRegions2 = hRegionServer2.getNumberOfOnlineRegions();
      Assert.assertEquals(0, newNumRegions0);
      Assert.assertEquals(0, newNumRegions1);
      Assert.assertEquals(totalRegions, newNumRegions2);
    }

  }

  private static class MockRackManager extends RackManager {

    private static final String RACK_2 = "rack-2";
    private static final String RACK_1 = "rack-1";

    @Override
    public String getRack(ServerName server) {
      return rs2.equals(server) ? RACK_2 : RACK_1;
    }

    @Override
    public List<String> getRack(List<ServerName> servers) {
      List<String> racks = new ArrayList<>();
      servers.forEach(serverName -> {
        if (rs2.equals(serverName)) {
          racks.add(RACK_2);
        } else {
          racks.add(RACK_1);
        }
      });
      return racks;
    }
  }

}
