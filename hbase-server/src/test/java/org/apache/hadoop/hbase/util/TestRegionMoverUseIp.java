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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MiscTests.class, LargeTests.class })
public class TestRegionMoverUseIp {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionMoverUseIp.class);
  private static final Logger LOG = LoggerFactory.getLogger(TestRegionMoverUseIp.class);

  @Rule
  public TestName name = new TestName();

  private static HBaseTestingUtil TEST_UTIL;
  private static ServerName rs0;
  private static ServerName rs1;
  private static ServerName rs2;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean(HConstants.HBASE_SERVER_USEIP_ENABLED_KEY, true);
    TEST_UTIL = new HBaseTestingUtil(conf);
    TEST_UTIL.startMiniCluster(3);

    SingleProcessHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    rs0 = cluster.getRegionServer(0).getServerName();
    rs1 = cluster.getRegionServer(1).getServerName();
    rs2 = cluster.getRegionServer(2).getServerName();
    LOG.info("rs0 hostname=" + rs0.getHostname());
    LOG.info("rs1 hostname=" + rs1.getHostname());
    LOG.info("rs2 hostname=" + rs2.getHostname());
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
  public void testRegionUnloadUesIp() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    SingleProcessHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    Admin admin = TEST_UTIL.getAdmin();
    Table table = TEST_UTIL.getConnection().getTable(tableName);
    List<Put> puts = IntStream.range(10, 50000).mapToObj(i -> new Put(Bytes.toBytes(i))
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
    RegionMover.RegionMoverBuilder rmBuilder =
      new RegionMover.RegionMoverBuilder(sourceRSName, TEST_UTIL.getConfiguration()).ack(true)
        .maxthreads(8);
    try (RegionMover regionMover = rmBuilder.build()) {
      regionMover.unload();
      int newNumRegions0 = hRegionServer0.getNumberOfOnlineRegions();
      int newNumRegions1 = hRegionServer1.getNumberOfOnlineRegions();
      int newNumRegions2 = hRegionServer2.getNumberOfOnlineRegions();
      Assert.assertEquals(0, newNumRegions0);
      Assert.assertEquals(totalRegions, newNumRegions1 + newNumRegions2);
    }
  }
}
