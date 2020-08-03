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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Tests for Region Mover Load/Unload functionality with and without ack mode and also to test
 * exclude functionality useful for rack decommissioning
 */
@Category({ MiscTests.class, LargeTests.class})
public class TestRegionMover2 {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionMover2.class);

  @Rule
  public TestName name = new TestName();

  private static final Logger LOG = LoggerFactory.getLogger(TestRegionMover2.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.client.retries.number", 4);
    TEST_UTIL.startMiniCluster(3);
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

  @After
  public void tearDown() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    TEST_UTIL.getAdmin().disableTable(tableName);
    TEST_UTIL.getAdmin().deleteTable(tableName);
  }

  @Test
  public void testWithSplitRegions() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    Admin admin = TEST_UTIL.getAdmin();
    Table table = TEST_UTIL.getConnection().getTable(tableName);
    List<Put> puts = new ArrayList<>();
    for (int i = 10; i < 50000; i++) {
      puts.add(new Put(Bytes.toBytes(i))
        .addColumn(Bytes.toBytes("fam1"), Bytes.toBytes("q1"), Bytes.toBytes("val_" + i)));
    }
    table.put(puts);
    admin.flush(tableName);
    admin.compact(tableName);
    HRegionServer regionServer = cluster.getRegionServer(0);
    String rsName = regionServer.getServerName().getAddress().toString();
    int numRegions = regionServer.getNumberOfOnlineRegions();
    List<HRegion> hRegions = regionServer.getRegions().stream()
      .filter(hRegion -> hRegion.getRegionInfo().getTable().equals(tableName))
      .collect(Collectors.toList());

    RegionMover.RegionMoverBuilder rmBuilder =
      new RegionMover.RegionMoverBuilder(rsName, TEST_UTIL.getConfiguration()).ack(true)
        .maxthreads(8);
    try (RegionMover rm = rmBuilder.build()) {
      LOG.debug("Unloading {}", regionServer.getServerName());
      rm.unload();
      Assert.assertEquals(0, regionServer.getNumberOfOnlineRegions());
      LOG.debug("Successfully Unloaded, now Loading");
      HRegion hRegion = hRegions.get(1);
      if (hRegion.getRegionInfo().getStartKey().length == 0) {
        hRegion = hRegions.get(0);
      }
      int startKey = 0;
      int endKey = Integer.MAX_VALUE;
      if (hRegion.getRegionInfo().getStartKey().length > 0) {
        startKey = Bytes.toInt(hRegion.getRegionInfo().getStartKey());
      }
      if (hRegion.getRegionInfo().getEndKey().length > 0) {
        endKey = Bytes.toInt(hRegion.getRegionInfo().getEndKey());
      }
      int midKey = startKey + (endKey - startKey) / 2;
      admin.splitRegionAsync(hRegion.getRegionInfo().getRegionName(), Bytes.toBytes(midKey))
        .get(5, TimeUnit.SECONDS);
      Assert.assertTrue(rm.load());
      Assert.assertEquals(numRegions - 1, regionServer.getNumberOfOnlineRegions());
    }
  }

  @Test
  public void testFailedRegionMove() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    Admin admin = TEST_UTIL.getAdmin();
    Table table = TEST_UTIL.getConnection().getTable(tableName);
    List<Put> puts = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      puts.add(new Put(Bytes.toBytes("rowkey_" + i))
        .addColumn(Bytes.toBytes("fam1"), Bytes.toBytes("q1"), Bytes.toBytes("val_" + i)));
    }
    table.put(puts);
    admin.flush(tableName);
    HRegionServer regionServer = cluster.getRegionServer(0);
    String rsName = regionServer.getServerName().getAddress().toString();
    List<HRegion> hRegions = regionServer.getRegions().stream()
      .filter(hRegion -> hRegion.getRegionInfo().getTable().equals(tableName))
      .collect(Collectors.toList());
    RegionMover.RegionMoverBuilder rmBuilder =
      new RegionMover.RegionMoverBuilder(rsName, TEST_UTIL.getConfiguration()).ack(true)
        .maxthreads(8);
    try (RegionMover rm = rmBuilder.build()) {
      LOG.debug("Unloading {}", regionServer.getServerName());
      rm.unload();
      Assert.assertEquals(0, regionServer.getNumberOfOnlineRegions());
      LOG.debug("Successfully Unloaded, now Loading");
      admin.offline(hRegions.get(0).getRegionInfo().getRegionName());
      // loading regions will fail because of offline region
      Assert.assertFalse(rm.load());
    }
  }

}
