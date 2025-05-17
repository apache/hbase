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
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Test reuse storefiles within data directory when cluster failover with a set of new region
 * servers with different hostnames with or without WALs and Zookeeper ZNodes, the master and
 * cluster should fail respectively if there is any situation considered as not supported.
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
  private static final long TIMEOUT_MS = Duration.ofMinutes(1).toMillis();
  private static final long MASTER_INIT_TIMEOUT_MS = Duration.ofSeconds(45).toMillis();

  @Before
  public void setup() throws Exception {
    TEST_UTIL.getConfiguration().setLong("hbase.master.init.timeout.localHBaseCluster",
      MASTER_INIT_TIMEOUT_MS);
    TEST_UTIL.startMiniCluster(StartMiniClusterOption.builder().numRegionServers(NUM_RS)
      .numDataNodes(NUM_RS).createWALDir(true).build());
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testRecreateCluster_UserTableDisabled_ReuseWALsAndZNodes() throws Exception {
    validateRecreateClusterWithUserDisabled(false, false);
  }

  @Test
  public void testRecreateCluster_UserTableEnabled_ReuseWALsAndZNodes() throws Exception {
    validateRecreateClusterWithUserTableEnabled(false, false);
  }

  @Test
  public void testRecreateCluster_UserTableEnabled_CleanupZNodes() throws Exception {
    // this is no longer failing because master region stores the information the region servers
    // as long as it's gracefully flushed before shutdown
    validateRecreateClusterWithUserTableEnabled(false, true);
  }

  @Test
  public void testRecreateCluster_UserTableEnabled_CleanupWALAndZNodes() throws Exception {
    validateRecreateClusterWithUserTableEnabled(true, true);
  }

  private void validateRecreateClusterWithUserDisabled(boolean cleanupWALs, boolean cleanUpZNodes)
    throws Exception {
    TableName tableName = TableName.valueOf("t1");
    prepareDataBeforeRecreate(TEST_UTIL, tableName);
    TEST_UTIL.getAdmin().disableTable(tableName);
    TEST_UTIL.waitTableDisabled(tableName.getName());
    restartHBaseCluster(cleanupWALs, cleanUpZNodes);
    TEST_UTIL.getAdmin().enableTable(tableName);
    validateDataAfterRecreate(TEST_UTIL, tableName);
  }

  private void validateRecreateClusterWithUserTableEnabled(boolean cleanupWALs,
    boolean cleanUpZNodes) throws Exception {
    TableName tableName = TableName.valueOf("t1");
    prepareDataBeforeRecreate(TEST_UTIL, tableName);
    restartHBaseCluster(cleanupWALs, cleanUpZNodes);
    validateDataAfterRecreate(TEST_UTIL, tableName);
  }

  private void restartHBaseCluster(boolean cleanUpWALs, boolean cleanUpZnodes) throws Exception {
    // flush cache so that everything is on disk
    TEST_UTIL.getMiniHBaseCluster().flushcache(TableName.META_TABLE_NAME);
    TEST_UTIL.getMiniHBaseCluster().flushcache();

    List<ServerName> oldServers =
      TEST_UTIL.getHBaseCluster().getMaster().getServerManager().getOnlineServersList();

    // make sure there is no procedures pending
    TEST_UTIL.waitFor(TIMEOUT_MS, () -> TEST_UTIL.getHBaseCluster().getMaster().getProcedures()
      .stream().filter(p -> p.isFinished()).findAny().isPresent());

    // shutdown and delete data if needed
    Path walRootDirPath = TEST_UTIL.getMiniHBaseCluster().getMaster().getWALRootDir();
    Path rootDirPath = CommonFSUtils.getRootDir(TEST_UTIL.getConfiguration());
    TEST_UTIL.shutdownMiniHBaseCluster();

    if (cleanUpWALs) {
      TEST_UTIL.getDFSCluster().getFileSystem().delete(walRootDirPath, true);
    }

    if (cleanUpZnodes) {
      // delete all zk data
      // we cannot keep ZK data because it will hold the meta region states as open and
      // didn't submit a InitMetaProcedure
      ZKUtil.deleteChildrenRecursively(TEST_UTIL.getZooKeeperWatcher(),
        TEST_UTIL.getZooKeeperWatcher().getZNodePaths().baseZNode);
      TEST_UTIL.shutdownMiniZKCluster();
      TEST_UTIL.startMiniZKCluster();
    }

    TEST_UTIL.restartHBaseCluster(NUM_RS);
    TEST_UTIL.waitFor(TIMEOUT_MS,
      () -> TEST_UTIL.getMiniHBaseCluster().getNumLiveRegionServers() == NUM_RS);

    // make sure we have a new set of region servers with different hostnames and ports
    List<ServerName> newServers =
      TEST_UTIL.getHBaseCluster().getMaster().getServerManager().getOnlineServersList();
    assertFalse(newServers.stream().filter(newServer -> oldServers.contains(newServer)).findAny()
      .isPresent());
  }

  private void prepareDataBeforeRecreate(HBaseTestingUtility testUtil, TableName tableName)
    throws Exception {
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
      destServer.getServerName().getPort(), TIMEOUT_MS);
    // move region of userTable to a different regionserver not co-located with system table
    TEST_UTIL.moveRegionAndWait(TEST_UTIL.getAdmin().getRegions(userTable).get(0),
      destServer.getServerName());
  }

  private int getServerNumForTableWithOnlyOneRegion(TableName tableName) throws IOException {
    List<RegionInfo> tableRegionInfos = TEST_UTIL.getAdmin().getRegions(tableName);
    assertEquals(1, tableRegionInfos.size());
    return TEST_UTIL.getHBaseCluster().getServerWith(tableRegionInfos.get(0).getRegionName());
  }

  private void validateDataAfterRecreate(HBaseTestingUtility testUtil, TableName tableName)
    throws Exception {
    Table t1 = testUtil.getConnection().getTable(tableName);
    Get get = new Get(Bytes.toBytes("r1"));
    get.addColumn(Bytes.toBytes("f"), Bytes.toBytes("c"));
    Result result = t1.get(get);
    assertTrue(result.advance());
    Cell cell = result.current();
    assertEquals("v",
      Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
    assertFalse(result.advance());
  }
}
