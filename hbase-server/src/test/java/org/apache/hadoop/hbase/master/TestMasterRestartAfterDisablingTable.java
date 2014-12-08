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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.NavigableSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestMasterRestartAfterDisablingTable {

  private static final Log LOG = LogFactory.getLog(TestMasterRestartAfterDisablingTable.class);

  @Test
  public void testForCheckingIfEnableAndDisableWorksFineAfterSwitch()
      throws Exception {
    final int NUM_MASTERS = 2;
    final int NUM_RS = 1;
    final int NUM_REGIONS_TO_CREATE = 4;

    // Start the cluster
    log("Starting cluster");
    Configuration conf = HBaseConfiguration.create();
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(conf);
    TEST_UTIL.startMiniCluster(NUM_MASTERS, NUM_RS);
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    log("Waiting for active/ready master");
    cluster.waitForActiveAndReadyMaster();
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "testmasterRestart", null);
    HMaster master = cluster.getMaster();

    // Create a table with regions
    TableName table = TableName.valueOf("tableRestart");
    byte[] family = Bytes.toBytes("family");
    log("Creating table with " + NUM_REGIONS_TO_CREATE + " regions");
    HTable ht = TEST_UTIL.createTable(table, family);
    int numRegions = TEST_UTIL.createMultiRegions(conf, ht, family,
        NUM_REGIONS_TO_CREATE);
    numRegions += 1; // catalogs
    log("Waiting for no more RIT\n");
    blockUntilNoRIT(zkw, master);
    log("Disabling table\n");
    TEST_UTIL.getHBaseAdmin().disableTable(table);

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

    assertTrue("The table should not be in enabled state", cluster.getMaster()
        .getAssignmentManager().getTableStateManager().isTableState(
        TableName.valueOf("tableRestart"), ZooKeeperProtos.Table.State.DISABLED,
        ZooKeeperProtos.Table.State.DISABLING));
    log("Enabling table\n");
    // Need a new Admin, the previous one is on the old master
    Admin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    admin.enableTable(table);
    admin.close();
    log("Waiting for no more RIT\n");
    blockUntilNoRIT(zkw, master);
    log("Verifying there are " + numRegions + " assigned on cluster\n");
    regions = HBaseTestingUtility.getAllOnlineRegions(cluster);
    assertEquals("The assigned regions were not onlined after master"
        + " switch except for the catalog and namespace tables.",
          6, regions.size());
    assertTrue("The table should be in enabled state", cluster.getMaster()
        .getAssignmentManager().getTableStateManager()
        .isTableState(TableName.valueOf("tableRestart"), ZooKeeperProtos.Table.State.ENABLED));
    ht.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  private void log(String msg) {
    LOG.debug("\n\nTRR: " + msg + "\n");
  }

  private void blockUntilNoRIT(ZooKeeperWatcher zkw, HMaster master)
      throws KeeperException, InterruptedException {
    ZKAssign.blockUntilNoRIT(zkw);
    master.assignmentManager.waitUntilNoRegionsInTransition(60000);
  }
}

