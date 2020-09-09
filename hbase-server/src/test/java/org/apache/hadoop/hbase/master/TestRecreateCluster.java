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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperHelper;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Test reuse data directory when cluster failover with a set of new region servers with
 * different hostnames. For any hbase system table and user table can be assigned normally after
 * cluster restart
 */
@Category({ LargeTests.class })
public class TestRecreateCluster {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRecreateCluster.class);

  @Rule
  public TestName name = new TestName();

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final int NUM_RS = 3;
  private static final long LIVE_REGION_SERVER_TIMEOUT_MS = Duration.ofMinutes(3).toMillis();

  @Test
  public void testRecreateCluster_UserTableDisabled() throws Exception {
    TEST_UTIL.startMiniCluster(NUM_RS);
    try {
      TableName tableName = TableName.valueOf("t1");
      prepareDataBeforeRecreate(TEST_UTIL, tableName);
      TEST_UTIL.getAdmin().disableTable(tableName);
      TEST_UTIL.waitTableDisabled(tableName.getName());
      restartCluster();
      TEST_UTIL.getAdmin().enableTable(tableName);
      validateDataAfterRecreate(TEST_UTIL, tableName);
    } finally {
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  @Test
  public void testRecreateCluster_UserTableEnabled() throws Exception {
    TEST_UTIL.startMiniCluster(NUM_RS);
    try {
      TableName tableName = TableName.valueOf("t1");
      prepareDataBeforeRecreate(TEST_UTIL, tableName);
      restartCluster();
      validateDataAfterRecreate(TEST_UTIL, tableName);
    } finally {
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  private void restartCluster() throws Exception {
    // flush cache so that everything is on disk
    TEST_UTIL.getMiniHBaseCluster().flushcache();

    // delete all wal data
    Path walRootPath = TEST_UTIL.getMiniHBaseCluster().getMaster().getWALRootDir();
    TEST_UTIL.shutdownMiniHBaseCluster();
    TEST_UTIL.getDFSCluster().getFileSystem().delete(
        new Path(walRootPath.toString(), HConstants.HREGION_LOGDIR_NAME), true);
    TEST_UTIL.getDFSCluster().getFileSystem().delete(
        new Path(walRootPath.toString(), WALProcedureStore.MASTER_PROCEDURE_LOGDIR), true);
    TEST_UTIL.getDFSCluster().getFileSystem().delete(
        new Path(walRootPath.toString(), HConstants.HREGION_OLDLOGDIR_NAME), true);

    // delete all zk data
    ZooKeeper zk = ZooKeeperHelper
        .getConnectedZooKeeper(TEST_UTIL.getZooKeeperWatcher().getQuorum(), 10000);
    ZKUtil.deleteRecursive(zk, TEST_UTIL.getZooKeeperWatcher().getZNodePaths().baseZNode);
    TEST_UTIL.shutdownMiniZKCluster();

    // restart zk and hbase
    TEST_UTIL.startMiniZKCluster();
    StartMiniClusterOption option = StartMiniClusterOption.builder()
        .numRegionServers(NUM_RS).build();
    TEST_UTIL.startMiniHBaseCluster(option);
  }

  private void prepareDataBeforeRecreate(
      HBaseTestingUtility testUtil, TableName tableName) throws Exception {
    Table table = testUtil.createTable(tableName, "f");
    Put put = new Put(Bytes.toBytes("r1"));
    put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("c"), Bytes.toBytes("v"));
    table.put(put);

    ensureTableNotColocatedWithSystemTable(tableName, TableName.NAMESPACE_TABLE_NAME);
  }

  private void ensureTableNotColocatedWithSystemTable(TableName userTable, TableName systemTable)
      throws IOException, InterruptedException {
    MiniHBaseCluster hbaseCluster = TEST_UTIL.getHBaseCluster();
    assertTrue("Please start more than 1 regionserver",
        hbaseCluster.getRegionServerThreads().size() > 1);

    int userTableServerNum = getServerNumForTableWithOnlyOneRegion(userTable);
    int systemTableServerNum = getServerNumForTableWithOnlyOneRegion(systemTable);

    if (userTableServerNum != systemTableServerNum) {
      // no-ops if user table and system are already on a different host
      return;
    }

    int destServerNum = (systemTableServerNum + 1) % NUM_RS;
    assertTrue(systemTableServerNum != destServerNum);

    HRegionServer systemTableServer = hbaseCluster.getRegionServer(systemTableServerNum);
    HRegionServer destServer = hbaseCluster.getRegionServer(destServerNum);
    assertTrue(!systemTableServer.equals(destServer));
    // make sure the dest server is live before moving region
    hbaseCluster.waitForRegionServerToStart(destServer.getServerName().getHostname(),
        destServer.getServerName().getPort(), LIVE_REGION_SERVER_TIMEOUT_MS);
    // move region of userTable to a different regionserver not co-located with system table
    TEST_UTIL.moveRegionAndWait(TEST_UTIL.getAdmin().getRegions(userTable).get(0),
        destServer.getServerName());
  }

  private int getServerNumForTableWithOnlyOneRegion(TableName tableName) throws IOException {
    List<RegionInfo> tableRegionInfos = TEST_UTIL.getAdmin().getRegions(tableName);
    assertEquals(1, tableRegionInfos.size());
    return TEST_UTIL.getHBaseCluster()
        .getServerWith(tableRegionInfos.get(0).getRegionName());
  }

  private void validateDataAfterRecreate(
      HBaseTestingUtility testUtil, TableName tableName) throws Exception {
    Table t1 = testUtil.getConnection().getTable(tableName);
    Get get = new Get(Bytes.toBytes("r1"));
    get.addColumn(Bytes.toBytes("f"), Bytes.toBytes("c"));
    Result result = t1.get(get);
    assertTrue(result.advance());
    Cell cell = result.current();
    assertEquals("v", Bytes.toString(cell.getValueArray(),
        cell.getValueOffset(), cell.getValueLength()));
    assertFalse(result.advance());
  }

}
