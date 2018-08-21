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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.NavigableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MasterTests.class, LargeTests.class})
public class TestMasterRestartAfterDisablingTable {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMasterRestartAfterDisablingTable.class);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestMasterRestartAfterDisablingTable.class);

  @Rule
  public TestName name = new TestName();

  @Test
  public void testForCheckingIfEnableAndDisableWorksFineAfterSwitch()
      throws Exception {
    final int NUM_MASTERS = 2;
    final int NUM_REGIONS_TO_CREATE = 4;

    // Start the cluster
    log("Starting cluster");
    Configuration conf = HBaseConfiguration.create();
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(conf);
    StartMiniClusterOption option = StartMiniClusterOption.builder()
        .numMasters(NUM_MASTERS).build();
    TEST_UTIL.startMiniCluster(option);
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    log("Waiting for active/ready master");
    cluster.waitForActiveAndReadyMaster();

    // Create a table with regions
    final TableName tableName = TableName.valueOf(name.getMethodName());
    byte[] family = Bytes.toBytes("family");
    log("Creating table with " + NUM_REGIONS_TO_CREATE + " regions");
    Table ht = TEST_UTIL.createMultiRegionTable(tableName, family, NUM_REGIONS_TO_CREATE);
    int numRegions = -1;
    try (RegionLocator r = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
      numRegions = r.getStartKeys().length;
    }
    numRegions += 1; // catalogs
    log("Waiting for no more RIT\n");
    TEST_UTIL.waitUntilNoRegionsInTransition(60000);
    log("Disabling table\n");
    TEST_UTIL.getAdmin().disableTable(tableName);

    NavigableSet<String> regions = HBaseTestingUtility.getAllOnlineRegions(cluster);
    assertEquals(
        "The number of regions for the table tableRestart should be 0 and only"
            + "the catalog and namespace tables should be present.", 2, regions.size());

    List<MasterThread> masterThreads = cluster.getMasterThreads();
    MasterThread activeMaster = null;
    if (masterThreads.get(0).getMaster().isActiveMaster()) {
      activeMaster = masterThreads.get(0);
    } else {
      activeMaster = masterThreads.get(1);
    }
    activeMaster.getMaster().stop(
        "stopping the active master so that the backup can become active");
    cluster.hbaseCluster.waitOnMaster(activeMaster);
    cluster.waitForActiveAndReadyMaster();

    assertTrue("The table should not be in enabled state",
        cluster.getMaster().getTableStateManager().isTableState(
        TableName.valueOf(name.getMethodName()), TableState.State.DISABLED,
        TableState.State.DISABLING));
    log("Enabling table\n");
    // Need a new Admin, the previous one is on the old master
    Admin admin = TEST_UTIL.getAdmin();
    admin.enableTable(tableName);
    admin.close();
    log("Waiting for no more RIT\n");
    TEST_UTIL.waitUntilNoRegionsInTransition(60000);
    log("Verifying there are " + numRegions + " assigned on cluster\n");
    regions = HBaseTestingUtility.getAllOnlineRegions(cluster);
    assertEquals("The assigned regions were not onlined after master"
        + " switch except for the catalog and namespace tables.",
          6, regions.size());
    assertTrue("The table should be in enabled state",
        cluster.getMaster().getTableStateManager()
        .isTableState(TableName.valueOf(name.getMethodName()), TableState.State.ENABLED));
    ht.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  private void log(String msg) {
    LOG.debug("\n\nTRR: " + msg + "\n");
  }
}

