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

import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.snapshot.TestRestoreFlushSnapshotFromClient;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;

/**
 * Test to verify that the cloned table is independent of the table from which it was cloned
 */
@Category({MediumTests.class, ClientTests.class})
public class TestSnapshotCloneIndependence {
  private static final Log LOG = LogFactory.getLog(TestSnapshotCloneIndependence.class);

  @Rule
  public Timeout globalTimeout = Timeout.seconds(60);

  @Rule
  public TestName testName = new TestName();

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  protected static final int NUM_RS = 2;
  private static final String STRING_TABLE_NAME = "test";
  private static final String TEST_FAM_STR = "fam";
  protected static final byte[] TEST_FAM = Bytes.toBytes(TEST_FAM_STR);
  private static final int CLEANER_INTERVAL = 100;

  private FileSystem fs;
  private Path rootDir;
  private Admin admin;
  private TableName originalTableName;
  private Table originalTable;
  private TableName cloneTableName;
  private int countOriginalTable;
  String snapshotNameAsString;
  byte[] snapshotName;

  /**
   * Setup the config for the cluster and start it
   */
  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(NUM_RS);
  }

  static void setupConf(Configuration conf) {
    // Up the handlers; this test needs more than usual.
    conf.setInt(HConstants.REGION_SERVER_HIGH_PRIORITY_HANDLER_COUNT, 15);
    // enable snapshot support
    conf.setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    // change the flush size to a small amount, regulating number of store files
    conf.setInt("hbase.hregion.memstore.flush.size", 25000);
    // so make sure we get a compaction when doing a load, but keep around
    // some files in the store
    conf.setInt("hbase.hstore.compaction.min", 10);
    conf.setInt("hbase.hstore.compactionThreshold", 10);
    // block writes if we get to 12 store files
    conf.setInt("hbase.hstore.blockingStoreFiles", 12);
    conf.setInt("hbase.regionserver.msginterval", 100);
    conf.setBoolean("hbase.master.enabletable.roundrobin", true);
    // Avoid potentially aggressive splitting which would cause snapshot to fail
    conf.set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
        ConstantSizeRegionSplitPolicy.class.getName());
    // Execute cleaner frequently to induce failures
    conf.setInt("hbase.master.cleaner.interval", CLEANER_INTERVAL);
    conf.setInt("hbase.master.hfilecleaner.plugins.snapshot.period", CLEANER_INTERVAL);
    // Effectively disable TimeToLiveHFileCleaner. Don't want to fully disable it because that
    // will even trigger races between creating the directory containing back references and
    // the back reference itself.
    conf.setInt("hbase.master.hfilecleaner.ttl", CLEANER_INTERVAL);
  }

  @Before
  public void setup() throws Exception {
    fs = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getFileSystem();
    rootDir = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();

    admin = UTIL.getHBaseAdmin();
    originalTableName = TableName.valueOf("test" + testName.getMethodName());
    cloneTableName = TableName.valueOf("test-clone-" + originalTableName);
    snapshotNameAsString = "snapshot_" + originalTableName;
    snapshotName = Bytes.toBytes(snapshotNameAsString);

    originalTable = createTable(originalTableName, TEST_FAM);
    loadData(originalTable, TEST_FAM);
    countOriginalTable = countRows(originalTable);
    System.out.println("Original table has: " + countOriginalTable + " rows");
  }

  @After
  public void tearDown() throws Exception {
    UTIL.deleteTable(originalTableName);
    UTIL.deleteTable(cloneTableName);
    SnapshotTestingUtils.deleteAllSnapshots(UTIL.getHBaseAdmin());
    SnapshotTestingUtils.deleteArchiveDirectory(UTIL);
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    try {
      UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      LOG.warn("failure shutting down cluster", e);
    }
  }

  /**
   * Verify that adding data to the cloned table will not affect the original, and vice-versa when
   * it is taken as an online snapshot.
   */
  @Test
  public void testOnlineSnapshotAppendIndependent() throws Exception {
    createAndCloneSnapshot(true);
    runTestSnapshotAppendIndependent();
  }

  /**
   * Verify that adding data to the cloned table will not affect the original, and vice-versa when
   * it is taken as an offline snapshot.
   */
  @Test
  public void testOfflineSnapshotAppendIndependent() throws Exception {
    createAndCloneSnapshot(false);
    runTestSnapshotAppendIndependent();
  }

  /**
   * Verify that adding metadata to the cloned table will not affect the original, and vice-versa
   * when it is taken as an online snapshot.
   */
  @Test
  public void testOnlineSnapshotMetadataChangesIndependent() throws Exception {
    createAndCloneSnapshot(true);
    runTestSnapshotMetadataChangesIndependent();
  }

  /**
   * Verify that adding netadata to the cloned table will not affect the original, and vice-versa
   * when is taken as an online snapshot.
   */
  @Test
  public void testOfflineSnapshotMetadataChangesIndependent() throws Exception {
    createAndCloneSnapshot(false);
    runTestSnapshotMetadataChangesIndependent();
  }

  /**
   * Verify that region operations, in this case splitting a region, are independent between the
   * cloned table and the original.
   */
  @Test
  public void testOfflineSnapshotRegionOperationsIndependent() throws Exception {
    createAndCloneSnapshot(false);
    runTestRegionOperationsIndependent();
  }

  /**
   * Verify that region operations, in this case splitting a region, are independent between the
   * cloned table and the original.
   */
  @Test
  public void testOnlineSnapshotRegionOperationsIndependent() throws Exception {
    createAndCloneSnapshot(true);
    runTestRegionOperationsIndependent();
  }

  @Test
  public void testOfflineSnapshotDeleteIndependent() throws Exception {
    createAndCloneSnapshot(false);
    runTestSnapshotDeleteIndependent();
  }

  @Test
  public void testOnlineSnapshotDeleteIndependent() throws Exception {
    createAndCloneSnapshot(true);
    runTestSnapshotDeleteIndependent();
  }

  private static void waitOnSplit(Connection c, final Table t, int originalCount) throws Exception {
    for (int i = 0; i < 200; i++) {
      Threads.sleepWithoutInterrupt(500);
      try (RegionLocator locator = c.getRegionLocator(t.getName())) {
        if (locator.getAllRegionLocations().size() > originalCount) {
          return;
        }
      }
    }
    throw new Exception("Split did not increase the number of regions");
  }

  /**
   * Takes the snapshot of originalTable and clones the snapshot to another tables.
   * If {@code online} is false, the original table is disabled during taking snapshot, so also
   * enables it again.
   * @param online - Whether the table is online or not during the snapshot
   */
  private void createAndCloneSnapshot(boolean online) throws Exception {
    SnapshotTestingUtils.createSnapshotAndValidate(admin, originalTableName, TEST_FAM_STR,
      snapshotNameAsString, rootDir, fs, online);

    // If offline, enable the table disabled by snapshot testing util.
    if (!online) {
      admin.enableTable(originalTableName);
      UTIL.waitTableAvailable(originalTableName);
    }

    admin.cloneSnapshot(snapshotName, cloneTableName);
    UTIL.waitUntilAllRegionsAssigned(cloneTableName);
  }

  /**
   * Verify that adding data to original table or clone table doesn't affect other table.
   */
  private void runTestSnapshotAppendIndependent() throws Exception {
    try (Table clonedTable = UTIL.getConnection().getTable(cloneTableName)) {
      final int clonedTableRowCount = countRows(clonedTable);

      Assert.assertEquals(
        "The line counts of original and cloned tables do not match after clone. ",
        countOriginalTable, clonedTableRowCount);

      // Attempt to add data to the test
      Put p = new Put(Bytes.toBytes("new-row-" + System.currentTimeMillis()));
      p.addColumn(TEST_FAM, Bytes.toBytes("someQualifier"), Bytes.toBytes("someString"));
      originalTable.put(p);

      // Verify that the new row is not in the restored table
      Assert.assertEquals("The row count of the original table was not modified by the put",
        countOriginalTable + 1, countRows(originalTable));
      Assert.assertEquals(
        "The row count of the cloned table changed as a result of addition to the original",
        clonedTableRowCount, countRows(clonedTable));

      Put p2 = new Put(Bytes.toBytes("new-row-" + System.currentTimeMillis()));
      p2.addColumn(TEST_FAM, Bytes.toBytes("someQualifier"), Bytes.toBytes("someString"));
      clonedTable.put(p2);

      // Verify that the row is not added to the original table.
      Assert.assertEquals(
        "The row count of the original table was modified by the put to the clone",
        countOriginalTable + 1, countRows(originalTable));
      Assert.assertEquals("The row count of the cloned table was not modified by the put",
        clonedTableRowCount + 1, countRows(clonedTable));
    }
  }

  /**
   * Do a split, and verify that this only affects one table
   */
  private void runTestRegionOperationsIndependent() throws Exception {
    // Verify that region information is the same pre-split
    ((ClusterConnection) UTIL.getConnection()).clearRegionCache();
    List<HRegionInfo> originalTableHRegions = admin.getTableRegions(originalTableName);

    final int originalRegionCount = originalTableHRegions.size();
    final int cloneTableRegionCount = admin.getTableRegions(cloneTableName).size();
    Assert.assertEquals(
      "The number of regions in the cloned table is different than in the original table.",
      originalRegionCount, cloneTableRegionCount);

    // Split a region on the parent table
    admin.splitRegion(originalTableHRegions.get(0).getRegionName());
    waitOnSplit(UTIL.getConnection(), originalTable, originalRegionCount);

    // Verify that the cloned table region is not split
    final int cloneTableRegionCount2 = admin.getTableRegions(cloneTableName).size();
    Assert.assertEquals(
      "The number of regions in the cloned table changed though none of its regions were split.",
      cloneTableRegionCount, cloneTableRegionCount2);
  }

  /**
   * Add metadata, and verify that this only affects one table
   */
  private void runTestSnapshotMetadataChangesIndependent() throws Exception {
    // Add a new column family to the original table
    byte[] TEST_FAM_2 = Bytes.toBytes("fam2");
    HColumnDescriptor hcd = new HColumnDescriptor(TEST_FAM_2);

    admin.disableTable(originalTableName);
    admin.addColumnFamily(originalTableName, hcd);

    // Verify that it is not in the snapshot
    admin.enableTable(originalTableName);
    UTIL.waitTableAvailable(originalTableName);

    // get a description of the cloned table
    // get a list of its families
    // assert that the family is there
    HTableDescriptor originalTableDescriptor = originalTable.getTableDescriptor();
    HTableDescriptor clonedTableDescriptor = admin.getTableDescriptor(cloneTableName);

    Assert.assertTrue("The original family was not found. There is something wrong. ",
      originalTableDescriptor.hasFamily(TEST_FAM));
    Assert.assertTrue("The original family was not found in the clone. There is something wrong. ",
      clonedTableDescriptor.hasFamily(TEST_FAM));

    Assert.assertTrue("The new family was not found. ",
      originalTableDescriptor.hasFamily(TEST_FAM_2));
    Assert.assertTrue("The new family was not found. ",
      !clonedTableDescriptor.hasFamily(TEST_FAM_2));
  }

  /**
   * Verify that deleting the snapshot does not affect either table.
   */
  private void runTestSnapshotDeleteIndependent() throws Exception {
    // Ensure the original table does not reference the HFiles anymore
    admin.majorCompact(originalTableName);

    // Deleting the snapshot used to break the cloned table by deleting in-use HFiles
    admin.deleteSnapshot(snapshotName);

    // Wait for cleaner run and DFS heartbeats so that anything that is deletable is fully deleted
    do {
      Thread.sleep(5000);
    } while (!admin.listSnapshots(snapshotNameAsString).isEmpty());

    try (Table original = UTIL.getConnection().getTable(originalTableName)) {
      try (Table clonedTable = UTIL.getConnection().getTable(cloneTableName)) {
        // Verify that all regions of both tables are readable
        final int origTableRowCount = countRows(original);
        final int clonedTableRowCount = countRows(clonedTable);
        Assert.assertEquals(origTableRowCount, clonedTableRowCount);
      }
    }
  }

  protected Table createTable(final TableName table, byte[] family) throws Exception {
   Table t = UTIL.createTable(table, family);
    // Wait for everything to be ready with the table
    UTIL.waitUntilAllRegionsAssigned(table);

    // At this point the table should be good to go.
    return t;
  }

  public void loadData(final Table table, byte[]... families) throws Exception {
    UTIL.loadTable(originalTable, TEST_FAM);
  }

  protected int countRows(final Table table, final byte[]... families) throws Exception {
    return UTIL.countRows(table, families);
  }
}
