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
package org.apache.hadoop.hbase.snapshot;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.SnapshotType;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.regionserver.snapshot.RegionServerSnapshotManager;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test clone/restore snapshots from the client
 *
 * TODO This is essentially a clone of TestRestoreSnapshotFromClient.  This is worth refactoring
 * this because there will be a few more flavors of snapshots that need to run these tests.
 */
@Category({RegionServerTests.class, LargeTests.class})
public class TestRestoreFlushSnapshotFromClient {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRestoreFlushSnapshotFromClient.class);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestRestoreFlushSnapshotFromClient.class);

  protected final static HBaseTestingUtility UTIL = new HBaseTestingUtility();

  protected final byte[] FAMILY = Bytes.toBytes("cf");

  protected byte[] snapshotName0;
  protected byte[] snapshotName1;
  protected byte[] snapshotName2;
  protected int snapshot0Rows;
  protected int snapshot1Rows;
  protected TableName tableName;
  protected Admin admin;

  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(3);
  }

  protected static void setupConf(Configuration conf) {
    UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
    UTIL.getConfiguration().setInt("hbase.client.pause", 250);
    UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 6);
    UTIL.getConfiguration().setBoolean(
        "hbase.master.enabletable.roundrobin", true);

    // Enable snapshot
    UTIL.getConfiguration().setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    UTIL.getConfiguration().setLong(RegionServerSnapshotManager.SNAPSHOT_TIMEOUT_MILLIS_KEY,
      RegionServerSnapshotManager.SNAPSHOT_TIMEOUT_MILLIS_DEFAULT * 2);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  protected void createTable() throws Exception {
    SnapshotTestingUtils.createTable(UTIL, tableName, FAMILY);
  }

  /**
   * Initialize the tests with a table filled with some data
   * and two snapshots (snapshotName0, snapshotName1) of different states.
   * The tableName, snapshotNames and the number of rows in the snapshot are initialized.
   */
  @Before
  public void setup() throws Exception {
    this.admin = UTIL.getAdmin();

    long tid = System.currentTimeMillis();
    tableName = TableName.valueOf("testtb-" + tid);
    snapshotName0 = Bytes.toBytes("snaptb0-" + tid);
    snapshotName1 = Bytes.toBytes("snaptb1-" + tid);
    snapshotName2 = Bytes.toBytes("snaptb2-" + tid);

    // create Table and disable it
    createTable();
    SnapshotTestingUtils.loadData(UTIL, tableName, 500, FAMILY);
    Table table = UTIL.getConnection().getTable(tableName);
    snapshot0Rows = countRows(table);
    LOG.info("=== before snapshot with 500 rows");
    logFSTree();

    // take a snapshot
    admin.snapshot(Bytes.toString(snapshotName0), tableName, SnapshotType.FLUSH);

    LOG.info("=== after snapshot with 500 rows");
    logFSTree();

    // insert more data
    SnapshotTestingUtils.loadData(UTIL, tableName, 500, FAMILY);
    snapshot1Rows = countRows(table);
    LOG.info("=== before snapshot with 1000 rows");
    logFSTree();

    // take a snapshot of the updated table
    admin.snapshot(Bytes.toString(snapshotName1), tableName, SnapshotType.FLUSH);
    LOG.info("=== after snapshot with 1000 rows");
    logFSTree();
    table.close();
  }

  @After
  public void tearDown() throws Exception {
    SnapshotTestingUtils.deleteAllSnapshots(UTIL.getAdmin());
    SnapshotTestingUtils.deleteArchiveDirectory(UTIL);
  }

  @Test
  public void testTakeFlushSnapshot() throws IOException {
    // taking happens in setup.
  }

  @Test
  public void testRestoreSnapshot() throws IOException {
    verifyRowCount(UTIL, tableName, snapshot1Rows);

    // Restore from snapshot-0
    admin.disableTable(tableName);
    admin.restoreSnapshot(snapshotName0);
    logFSTree();
    admin.enableTable(tableName);
    LOG.info("=== after restore with 500 row snapshot");
    logFSTree();
    verifyRowCount(UTIL, tableName, snapshot0Rows);

    // Restore from snapshot-1
    admin.disableTable(tableName);
    admin.restoreSnapshot(snapshotName1);
    admin.enableTable(tableName);
    verifyRowCount(UTIL, tableName, snapshot1Rows);
  }

  @Test(expected=SnapshotDoesNotExistException.class)
  public void testCloneNonExistentSnapshot() throws IOException, InterruptedException {
    String snapshotName = "random-snapshot-" + System.currentTimeMillis();
    TableName tableName = TableName.valueOf("random-table-" + System.currentTimeMillis());
    admin.cloneSnapshot(snapshotName, tableName);
  }

  @Test
  public void testCloneSnapshot() throws IOException, InterruptedException {
    TableName clonedTableName = TableName.valueOf("clonedtb-" + System.currentTimeMillis());
    testCloneSnapshot(clonedTableName, snapshotName0, snapshot0Rows);
    testCloneSnapshot(clonedTableName, snapshotName1, snapshot1Rows);
  }

  private void testCloneSnapshot(final TableName tableName, final byte[] snapshotName,
      int snapshotRows) throws IOException, InterruptedException {
    // create a new table from snapshot
    admin.cloneSnapshot(snapshotName, tableName);
    verifyRowCount(UTIL, tableName, snapshotRows);

    UTIL.deleteTable(tableName);
  }

  @Test
  public void testRestoreSnapshotOfCloned() throws IOException, InterruptedException {
    TableName clonedTableName = TableName.valueOf("clonedtb-" + System.currentTimeMillis());
    admin.cloneSnapshot(snapshotName0, clonedTableName);
    verifyRowCount(UTIL, clonedTableName, snapshot0Rows);
    admin.snapshot(Bytes.toString(snapshotName2), clonedTableName, SnapshotType.FLUSH);
    UTIL.deleteTable(clonedTableName);

    admin.cloneSnapshot(snapshotName2, clonedTableName);
    verifyRowCount(UTIL, clonedTableName, snapshot0Rows);
    UTIL.deleteTable(clonedTableName);
  }

  // ==========================================================================
  //  Helpers
  // ==========================================================================
  private void logFSTree() throws IOException {
    UTIL.getMiniHBaseCluster().getMaster().getMasterFileSystem().logFileSystemState(LOG);
  }

  protected void verifyRowCount(final HBaseTestingUtility util, final TableName tableName,
      long expectedRows) throws IOException {
    SnapshotTestingUtils.verifyRowCount(util, tableName, expectedRows);
  }

  protected int countRows(final Table table, final byte[]... families) throws IOException {
    return UTIL.countRows(table, families);
  }
}
