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
package org.apache.hadoop.hbase.master.balancer;

import static org.apache.hadoop.hbase.favored.FavoredNodeAssignmentHelper.FAVORED_NODES_NUM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.favored.FavoredNodesManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

/*
 * This case tests a scenario when a cluster with tables is moved from Stochastic Load Balancer
 * to FavoredStochasticLoadBalancer and the generation of favored nodes after switch.
 */
@Category(MediumTests.class)
public class TestFavoredNodeTableImport {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestFavoredNodeTableImport.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestFavoredNodeTableImport.class);

  private static final int SLAVES = 3;
  private static final int REGION_NUM = 20;
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final Configuration conf = UTIL.getConfiguration();

  @After
  public void stopCluster() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testTableCreation() throws Exception {

    conf.set(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, StochasticLoadBalancer.class.getName());

    LOG.info("Starting up cluster");
    UTIL.startMiniCluster(SLAVES);
    while (!UTIL.getMiniHBaseCluster().getMaster().isInitialized()) {
      Threads.sleep(1);
    }
    Admin admin = UTIL.getAdmin();
    admin.balancerSwitch(false, true);

    String tableName = "testFNImport";
    TableDescriptorBuilder.ModifyableTableDescriptor tableDescriptor =
      new TableDescriptorBuilder.ModifyableTableDescriptor(TableName.valueOf(tableName));

    tableDescriptor.setColumnFamily(
      new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(
        HConstants.CATALOG_FAMILY));
    admin.createTable(tableDescriptor, Bytes.toBytes("a"), Bytes.toBytes("z"), REGION_NUM);
    UTIL.waitTableAvailable(tableDescriptor.getTableName());
    admin.balancerSwitch(true, true);

    LOG.info("Shutting down cluster");
    UTIL.shutdownMiniHBaseCluster();

    Thread.sleep(2000);
    LOG.info("Starting cluster again with FN Balancer");
    UTIL.getConfiguration().set(HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
        FavoredStochasticBalancer.class.getName());
    UTIL.restartHBaseCluster(SLAVES);
    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    while (!master.isInitialized()) {
      Threads.sleep(1);
    }
    UTIL.waitTableAvailable(tableDescriptor.getTableName());
    UTIL.waitUntilNoRegionsInTransition(10000);
    assertTrue(master.isBalancerOn());

    FavoredNodesManager fnm = master.getFavoredNodesManager();
    assertNotNull(fnm);

    admin = UTIL.getAdmin();
    List<RegionInfo> regionsOfTable = admin.getRegions(TableName.valueOf(tableName));
    for (RegionInfo rInfo : regionsOfTable) {
      assertNotNull(rInfo);
      assertNotNull(fnm);
      List<ServerName> fns = fnm.getFavoredNodes(rInfo);
      LOG.info("FNS {} {}", rInfo, fns);
      assertNotNull(rInfo.toString(), fns);
      Set<ServerName> favNodes = Sets.newHashSet(fns);
      assertNotNull(favNodes);
      assertEquals("Required no of favored nodes not found.", FAVORED_NODES_NUM, favNodes.size());
      for (ServerName fn : favNodes) {
        assertEquals("StartCode invalid for:" + fn, ServerName.NON_STARTCODE, fn.getStartcode());
      }
    }
  }
}
