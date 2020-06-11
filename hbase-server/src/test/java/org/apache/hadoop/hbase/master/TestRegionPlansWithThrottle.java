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

package org.apache.hadoop.hbase.master;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, MediumTests.class})
public class TestRegionPlansWithThrottle {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionPlansWithThrottle.class);

  private static HMaster hMaster;

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(StartMiniClusterOption.builder().numRegionServers(2).build());
    hMaster = UTIL.getMiniHBaseCluster().getMaster();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testExecuteRegionPlansWithThrottling() throws Exception {
    final TableName tableName = TableName.valueOf("testExecuteRegionPlansWithThrottling");

    TableDescriptorBuilder.ModifyableTableDescriptor tableDescriptor =
      new TableDescriptorBuilder.ModifyableTableDescriptor(tableName).setColumnFamily(
        new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(Bytes.toBytes("cf")));

    UTIL.getAdmin().createTable(tableDescriptor);
    Table table = UTIL.getConnection().getTable(tableName);

    List<Put> puts = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      Put put = new Put(Bytes.toBytes("row" + i));
      byte[] q = Bytes.toBytes("q1");
      byte[] v = Bytes.toBytes("v" + i);
      put.addColumn(Bytes.toBytes("cf"), q, v);
      puts.add(put);
    }
    table.put(puts);
    UTIL.getAdmin().flush(tableName);
    UTIL.getAdmin().split(tableName, Bytes.toBytes("v5"));

    List<RegionPlan> plans = new ArrayList<>();
    List<RegionInfo> regionInfos = UTIL.getAdmin().getRegions(tableName);
    for (RegionInfo regionInfo : regionInfos) {
      plans.add(
        new RegionPlan(regionInfo, UTIL.getHBaseCluster().getRegionServer(0).getServerName(),
          UTIL.getHBaseCluster().getRegionServer(1).getServerName()));
    }
    List<RegionPlan> successPlans = hMaster.executeRegionPlansWithThrottling(plans);
    Assert.assertEquals(regionInfos.size(), successPlans.size());
  }

}
