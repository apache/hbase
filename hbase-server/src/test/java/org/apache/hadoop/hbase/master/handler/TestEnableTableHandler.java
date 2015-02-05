/**
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
package org.apache.hadoop.hbase.master.handler;

import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({ MediumTests.class })
public class TestEnableTableHandler {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Log LOG = LogFactory.getLog(TestEnableTableHandler.class);
  private static final byte[] FAMILYNAME = Bytes.toBytes("fam");

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.getConfiguration().set("hbase.balancer.tablesOnMaster", "hbase:meta");
    TEST_UTIL.startMiniCluster(1);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test(timeout = 300000)
  public void testEnableTableWithNoRegionServers() throws Exception {
    final TableName tableName = TableName.valueOf("testEnableTableWithNoRegionServers");
    final MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    final HMaster m = cluster.getMaster();
    final HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    final HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(FAMILYNAME));
    admin.createTable(desc);
    admin.disableTable(tableName);
    TEST_UTIL.waitTableDisabled(tableName.getName());

    admin.enableTable(tableName);
    TEST_UTIL.waitTableEnabled(tableName);

    // disable once more
    admin.disableTable(tableName);

    // now stop region servers
    JVMClusterUtil.RegionServerThread rs = cluster.getRegionServerThreads().get(0);
    rs.getRegionServer().stop("stop");
    cluster.waitForRegionServerToStop(rs.getRegionServer().getServerName(), 10000);

    TEST_UTIL.waitUntilAllRegionsAssigned(TableName.META_TABLE_NAME);

    admin.enableTable(tableName);
    assertTrue(admin.isTableEnabled(tableName));

    JVMClusterUtil.RegionServerThread rs2 = cluster.startRegionServer();
    m.getAssignmentManager().assign(admin.getTableRegions(tableName));
    TEST_UTIL.waitUntilAllRegionsAssigned(tableName);
    List<HRegionInfo> onlineRegions = admin.getOnlineRegions(
        rs2.getRegionServer().getServerName());
    assertEquals(2, onlineRegions.size());
    assertEquals(tableName, onlineRegions.get(1).getTable());
  }

}
