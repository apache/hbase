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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.hbase.util.RegionSplitter.DecimalStringSplit;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test reuse storefiles within data directory when cluster failover with a set of new region
 * servers with different hostnames with or without WALs and Zookeeper ZNodes support. For any hbase
 * system table and user table can be assigned normally after cluster restart. Turns out that it's
 * difficult to write a negative test that the configuration property can be disabled because
 * MiniHBaseCluster won't fully start. In JVMClusterUtil (called by HBaseTestingUtility), we wait
 * for the active Master to complete its initialization. However, because we don't recover these
 * Regions on UNKNOWN servers (including hbase:namespace's Region), the Master will never initialize
 * on its own.
 */
@Category({ LargeTests.class })
public class TestRecreateCluster {
  private static final Logger LOG = LoggerFactory.getLogger(TestRecreateCluster.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRecreateCluster.class);

  @Rule
  public TestName name = new TestName();

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final int NUM_RS = 3;
  private static final long TIMEOUT_MS = Duration.ofMinutes(2).toMillis();

  @Before
  public void setup() {
    TEST_UTIL.getConfiguration().setBoolean(AssignmentManager.PROCESS_UNKNOWN_RS_ON_STARTUP, true);
  }

  @Test
  public void recreateDisabledTable() throws Exception {
    TEST_UTIL.startMiniCluster(NUM_RS);
    try {
      LOG.info("Creating data");
      TableName tableName = TableName.valueOf("t1");
      prepareDataBeforeRecreate(TEST_UTIL, tableName);
      LOG.info("Disabling {}", tableName);
      TEST_UTIL.getAdmin().disableTable(tableName);
      TEST_UTIL.waitTableDisabled(tableName, TIMEOUT_MS);
      LOG.info("Stopping HBase, deleting data, and starting HBase");
      restartAndCleanHBaseCluster();
      // With a disabled table, the regions should still be in meta. The table
      // should remain offline after HBase restarts, but we should be able to
      // enable the table and validate the data is present.
      LOG.info("Enabling {}", tableName);
      TEST_UTIL.getAdmin().enableTable(tableName);
      LOG.info("Valiating table is present");
      validateDataAfterRecreate(TEST_UTIL, tableName);
    } finally {
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  @Test
  public void recreateEnabledTest() throws Exception {
    try {
      TEST_UTIL.startMiniCluster(NUM_RS);
      TableName tableName = TableName.valueOf("t1");
      prepareDataBeforeRecreate(TEST_UTIL, tableName);
      restartAndCleanHBaseCluster();
      validateDataAfterRecreate(TEST_UTIL, tableName);
    } finally {
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  @Test
  public void recreateTableWithLotsOfRegions() throws Exception {
    try {
      final int numRegions = 50;
      TEST_UTIL.startMiniCluster(NUM_RS);
      TableName tableName = TableName.valueOf("t1");
      preparePresplitTable(TEST_UTIL, tableName, numRegions);
      restartAndCleanHBaseCluster();
      validatePresplitTable(TEST_UTIL, tableName, numRegions);
    } finally {
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  /**
   * Stops HBase, removes the WALs, oldWALs, and MasterProcWALs directories and HBase zookeeper
   * data. Then, HBase is restarted.
   */
  private void restartAndCleanHBaseCluster() throws Exception {
    // flush cache so that everything is on disk
    TEST_UTIL.getMiniHBaseCluster().flushcache();

    List<ServerName> oldServers =
      TEST_UTIL.getHBaseCluster().getMaster().getServerManager().getOnlineServersList();

    // make sure there is no procedures pending
    TEST_UTIL.waitFor(TIMEOUT_MS, () -> TEST_UTIL.getHBaseCluster().getMaster().getProcedures()
      .stream().filter(p -> p.isFinished()).findAny().isPresent());

    // shutdown and delete data if needed
    Path walRootDir =
      TEST_UTIL.getMiniHBaseCluster().getMaster().getMasterFileSystem().getWALRootDir();
    Path walArchiveDir = new Path(walRootDir, WALProcedureStore.MASTER_PROCEDURE_LOGDIR);
    Path walDir = new Path(walRootDir, HConstants.HREGION_LOGDIR_NAME);

    TEST_UTIL.shutdownMiniHBaseCluster();

    // Remove the WAL dirs
    LOG.info("Removing WAL archive dir {}", walArchiveDir);
    TEST_UTIL.getDFSCluster().getFileSystem().delete(walArchiveDir, true);
    LOG.info("Removing WAL dir {}", walDir);
    TEST_UTIL.getDFSCluster().getFileSystem().delete(walDir, true);

    // Remove the ZooKeeper data
    ZKUtil.deleteChildrenRecursively(TEST_UTIL.getZooKeeperWatcher(),
      TEST_UTIL.getZooKeeperWatcher().getZNodePaths().baseZNode);
    TEST_UTIL.shutdownMiniZKCluster();
    TEST_UTIL.startMiniZKCluster();

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
    Put put = new Put(Bytes.toBytes(Integer.toString(1)));
    put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("c"), Bytes.toBytes("v"));
    table.put(put);

    ensureTableNotColocatedWithSystemTable(tableName, TableName.NAMESPACE_TABLE_NAME);
  }

  private void preparePresplitTable(HBaseTestingUtility testUtil, TableName tableName,
    int numRegions) throws Exception {
    DecimalStringSplit splitter = new RegionSplitter.DecimalStringSplit();
    byte[][] splitPoints = splitter.split(numRegions);
    for (byte[] split : splitPoints) {
      LOG.error(Bytes.toString(split));
    }
    Table table = testUtil.createTable(tableName, Bytes.toBytes("f"), splitPoints);
    // Write one row per split point
    for (int i = 0; i <= numRegions; i++) {
      Put put = new Put(Bytes.toBytes(Integer.toString(i)));
      put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("c"), Bytes.toBytes("v"));
      table.put(put);
    }
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
    Get get = new Get(Bytes.toBytes(Integer.toString(1)));
    get.addColumn(Bytes.toBytes("f"), Bytes.toBytes("c"));
    Result result = t1.get(get);
    assertEquals(1, result.size());
    assertArrayEquals(Bytes.toBytes("v"), result.getValue(Bytes.toBytes("f"), Bytes.toBytes("c")));
  }

  private void validatePresplitTable(HBaseTestingUtility testUtil, TableName tableName,
    int numRegions) throws Exception {
    Table t1 = testUtil.getConnection().getTable(tableName);
    assertEquals(numRegions, testUtil.getAdmin().getRegions(tableName).size());
    for (int i = 0; i <= numRegions; i++) {
      Get get = new Get(Bytes.toBytes(Integer.toString(i)));
      get.addColumn(Bytes.toBytes("f"), Bytes.toBytes("c"));
      Result result = t1.get(get);
      assertEquals("Failed to find row for " + get, 1, result.size());
      assertArrayEquals(Bytes.toBytes("v"),
        result.getValue(Bytes.toBytes("f"), Bytes.toBytes("c")));
    }
  }
}
