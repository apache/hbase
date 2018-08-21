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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.snapshot.SnapshotDoesNotExistException;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test clone snapshots from the client
 */
@Category({LargeTests.class, ClientTests.class})
public class TestCloneSnapshotFromClient {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCloneSnapshotFromClient.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestCloneSnapshotFromClient.class);

  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  protected final byte[] FAMILY = Bytes.toBytes("cf");

  protected byte[] emptySnapshot;
  protected byte[] snapshotName0;
  protected byte[] snapshotName1;
  protected byte[] snapshotName2;
  protected TableName tableName;
  protected int snapshot0Rows;
  protected int snapshot1Rows;
  protected Admin admin;

  @Rule
  public TestName name = new TestName();

  protected static void setupConfiguration() {
    TEST_UTIL.getConfiguration().setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    TEST_UTIL.getConfiguration().setInt("hbase.hstore.compactionThreshold", 10);
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 250);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 6);
    TEST_UTIL.getConfiguration().setBoolean(
        "hbase.master.enabletable.roundrobin", true);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    setupConfiguration();
    TEST_UTIL.startMiniCluster(3);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Initialize the tests with a table filled with some data
   * and two snapshots (snapshotName0, snapshotName1) of different states.
   * The tableName, snapshotNames and the number of rows in the snapshot are initialized.
   */
  @Before
  public void setup() throws Exception {
    this.admin = TEST_UTIL.getAdmin();

    long tid = System.currentTimeMillis();
    tableName = TableName.valueOf(name.getMethodName() + tid);
    emptySnapshot = Bytes.toBytes("emptySnaptb-" + tid);
    snapshotName0 = Bytes.toBytes("snaptb0-" + tid);
    snapshotName1 = Bytes.toBytes("snaptb1-" + tid);
    snapshotName2 = Bytes.toBytes("snaptb2-" + tid);

    createTableAndSnapshots();
  }

  protected void createTableAndSnapshots() throws Exception {
    // create Table and disable it
    SnapshotTestingUtils.createTable(TEST_UTIL, tableName, getNumReplicas(), FAMILY);
    admin.disableTable(tableName);

    // take an empty snapshot
    admin.snapshot(emptySnapshot, tableName);

    // enable table and insert data
    admin.enableTable(tableName);
    SnapshotTestingUtils.loadData(TEST_UTIL, tableName, 500, FAMILY);
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)){
      snapshot0Rows = TEST_UTIL.countRows(table);
    }
    admin.disableTable(tableName);

    // take a snapshot
    admin.snapshot(snapshotName0, tableName);

    // enable table and insert more data
    admin.enableTable(tableName);
    SnapshotTestingUtils.loadData(TEST_UTIL, tableName, 500, FAMILY);
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)){
      snapshot1Rows = TEST_UTIL.countRows(table);
    }
    admin.disableTable(tableName);

    // take a snapshot of the updated table
    admin.snapshot(snapshotName1, tableName);

    // re-enable table
    admin.enableTable(tableName);
  }

  protected int getNumReplicas() {
    return 1;
  }

  @After
  public void tearDown() throws Exception {
    if (admin.tableExists(tableName)) {
      TEST_UTIL.deleteTable(tableName);
    }
    SnapshotTestingUtils.deleteAllSnapshots(admin);
    SnapshotTestingUtils.deleteArchiveDirectory(TEST_UTIL);
  }

  @Test(expected=SnapshotDoesNotExistException.class)
  public void testCloneNonExistentSnapshot() throws IOException, InterruptedException {
    String snapshotName = "random-snapshot-" + System.currentTimeMillis();
    final TableName tableName = TableName.valueOf(name.getMethodName() + "-"
      + System.currentTimeMillis());
    admin.cloneSnapshot(snapshotName, tableName);
  }

  @Test(expected = NamespaceNotFoundException.class)
  public void testCloneOnMissingNamespace() throws IOException, InterruptedException {
    final TableName clonedTableName = TableName.valueOf("unknownNS:" + name.getMethodName());
    admin.cloneSnapshot(snapshotName1, clonedTableName);
  }

  @Test
  public void testCloneSnapshot() throws IOException, InterruptedException {
    final TableName clonedTableName = TableName.valueOf(name.getMethodName() + "-"
      + System.currentTimeMillis());
    testCloneSnapshot(clonedTableName, snapshotName0, snapshot0Rows);
    testCloneSnapshot(clonedTableName, snapshotName1, snapshot1Rows);
    testCloneSnapshot(clonedTableName, emptySnapshot, 0);
  }

  private void testCloneSnapshot(final TableName tableName, final byte[] snapshotName,
      int snapshotRows) throws IOException, InterruptedException {
    // create a new table from snapshot
    admin.cloneSnapshot(snapshotName, tableName);
    verifyRowCount(TEST_UTIL, tableName, snapshotRows);

    verifyReplicasCameOnline(tableName);
    TEST_UTIL.deleteTable(tableName);
  }

  protected void verifyReplicasCameOnline(TableName tableName) throws IOException {
    SnapshotTestingUtils.verifyReplicasCameOnline(tableName, admin, getNumReplicas());
  }

  @Test
  public void testCloneSnapshotCrossNamespace() throws IOException, InterruptedException {
    String nsName = "testCloneSnapshotCrossNamespace";
    admin.createNamespace(NamespaceDescriptor.create(nsName).build());
    final TableName clonedTableName = TableName.valueOf(nsName, name.getMethodName()
      + "-" + System.currentTimeMillis());
    testCloneSnapshot(clonedTableName, snapshotName0, snapshot0Rows);
    testCloneSnapshot(clonedTableName, snapshotName1, snapshot1Rows);
    testCloneSnapshot(clonedTableName, emptySnapshot, 0);
  }

  /**
   * Verify that tables created from the snapshot are still alive after source table deletion.
   */
  @Test
  public void testCloneLinksAfterDelete() throws IOException, InterruptedException {
    // Clone a table from the first snapshot
    final TableName clonedTableName = TableName.valueOf(name.getMethodName() + "1-"
      + System.currentTimeMillis());
    admin.cloneSnapshot(snapshotName0, clonedTableName);
    verifyRowCount(TEST_UTIL, clonedTableName, snapshot0Rows);

    // Take a snapshot of this cloned table.
    admin.disableTable(clonedTableName);
    admin.snapshot(snapshotName2, clonedTableName);

    // Clone the snapshot of the cloned table
    final TableName clonedTableName2 = TableName.valueOf(name.getMethodName() + "2-"
      + System.currentTimeMillis());
    admin.cloneSnapshot(snapshotName2, clonedTableName2);
    verifyRowCount(TEST_UTIL, clonedTableName2, snapshot0Rows);
    admin.disableTable(clonedTableName2);

    // Remove the original table
    TEST_UTIL.deleteTable(tableName);
    waitCleanerRun();

    // Verify the first cloned table
    admin.enableTable(clonedTableName);
    verifyRowCount(TEST_UTIL, clonedTableName, snapshot0Rows);

    // Verify the second cloned table
    admin.enableTable(clonedTableName2);
    verifyRowCount(TEST_UTIL, clonedTableName2, snapshot0Rows);
    admin.disableTable(clonedTableName2);

    // Delete the first cloned table
    TEST_UTIL.deleteTable(clonedTableName);
    waitCleanerRun();

    // Verify the second cloned table
    admin.enableTable(clonedTableName2);
    verifyRowCount(TEST_UTIL, clonedTableName2, snapshot0Rows);

    // Clone a new table from cloned
    final TableName clonedTableName3 = TableName.valueOf(name.getMethodName() + "3-"
      + System.currentTimeMillis());
    admin.cloneSnapshot(snapshotName2, clonedTableName3);
    verifyRowCount(TEST_UTIL, clonedTableName3, snapshot0Rows);

    // Delete the cloned tables
    TEST_UTIL.deleteTable(clonedTableName2);
    TEST_UTIL.deleteTable(clonedTableName3);
    admin.deleteSnapshot(snapshotName2);
  }

  @Test
  public void testCloneSnapshotAfterSplittingRegion() throws IOException, InterruptedException {
    // Turn off the CatalogJanitor
    admin.catalogJanitorSwitch(false);

    try {
      List<RegionInfo> regionInfos = admin.getRegions(tableName);
      RegionReplicaUtil.removeNonDefaultRegions(regionInfos);

      // Split the first region
      splitRegion(regionInfos.get(0));

      // Take a snapshot
      admin.snapshot(snapshotName2, tableName);

      // Clone the snapshot to another table
      TableName clonedTableName = TableName.valueOf(name.getMethodName() + "-"
        + System.currentTimeMillis());
      admin.cloneSnapshot(snapshotName2, clonedTableName);
      SnapshotTestingUtils.waitForTableToBeOnline(TEST_UTIL, clonedTableName);

      RegionStates regionStates =
        TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStates();

      // The region count of the cloned table should be the same as the one of the original table
      int openRegionCountOfOriginalTable =
        regionStates.getRegionByStateOfTable(tableName).get(RegionState.State.OPEN).size();
      int openRegionCountOfClonedTable =
        regionStates.getRegionByStateOfTable(clonedTableName).get(RegionState.State.OPEN).size();
      assertEquals(openRegionCountOfOriginalTable, openRegionCountOfClonedTable);

      int splitRegionCountOfOriginalTable =
        regionStates.getRegionByStateOfTable(tableName).get(RegionState.State.SPLIT).size();
      int splitRegionCountOfClonedTable =
        regionStates.getRegionByStateOfTable(clonedTableName).get(RegionState.State.SPLIT).size();
      assertEquals(splitRegionCountOfOriginalTable, splitRegionCountOfClonedTable);

      TEST_UTIL.deleteTable(clonedTableName);
    } finally {
      admin.catalogJanitorSwitch(true);
    }
  }

  // ==========================================================================
  //  Helpers
  // ==========================================================================

  private void waitCleanerRun() throws InterruptedException {
    TEST_UTIL.getMiniHBaseCluster().getMaster().getHFileCleaner().choreForTesting();
  }

  protected void verifyRowCount(final HBaseTestingUtility util, final TableName tableName,
      long expectedRows) throws IOException {
    SnapshotTestingUtils.verifyRowCount(util, tableName, expectedRows);
  }

  protected void splitRegion(final RegionInfo regionInfo) throws IOException {
    byte[][] splitPoints = Bytes.split(regionInfo.getStartKey(), regionInfo.getEndKey(), 1);
    admin.split(regionInfo.getTable(), splitPoints[1]);
  }
}
