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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test to verify that the cloned table is independent of the table from which it was cloned
 */
@Category(LargeTests.class)
public class TestSnapshotCloneIndependence {
  private static final Log LOG = LogFactory.getLog(TestSnapshotCloneIndependence.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final int NUM_RS = 2;
  private static final String STRING_TABLE_NAME = "test";
  private static final String TEST_FAM_STR = "fam";
  private static final byte[] TEST_FAM = Bytes.toBytes(TEST_FAM_STR);
  private static final TableName TABLE_NAME = TableName.valueOf(STRING_TABLE_NAME);

  /**
   * Setup the config for the cluster and start it
   * @throws Exception on failure
   */
  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(NUM_RS);
  }

  private static void setupConf(Configuration conf) {
    // enable snapshot support
    conf.setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    // disable the ui
    conf.setInt("hbase.regionsever.info.port", -1);
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
  }

  @Before
  public void setup() throws Exception {
    UTIL.createTable(TABLE_NAME, TEST_FAM);
  }

  @After
  public void tearDown() throws Exception {
    UTIL.deleteTable(TABLE_NAME);
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
  @Test (timeout=300000)
  public void testOnlineSnapshotAppendIndependent() throws Exception {
    runTestSnapshotAppendIndependent(true);
  }

  /**
   * Verify that adding data to the cloned table will not affect the original, and vice-versa when
   * it is taken as an offline snapshot.
   */
  @Test (timeout=300000)
  public void testOfflineSnapshotAppendIndependent() throws Exception {
    runTestSnapshotAppendIndependent(false);
  }

  /**
   * Verify that adding metadata to the cloned table will not affect the original, and vice-versa
   * when it is taken as an online snapshot.
   */
  @Test (timeout=300000)
  public void testOnlineSnapshotMetadataChangesIndependent() throws Exception {
    runTestSnapshotMetadataChangesIndependent(true);
  }

  /**
   * Verify that adding netadata to the cloned table will not affect the original, and vice-versa
   * when is taken as an online snapshot.
   */
  @Test (timeout=300000)
  public void testOfflineSnapshotMetadataChangesIndependent() throws Exception {
    runTestSnapshotMetadataChangesIndependent(false);
  }

  /**
   * Verify that region operations, in this case splitting a region, are independent between the
   * cloned table and the original.
   */
  @Test (timeout=300000)
  public void testOfflineSnapshotRegionOperationsIndependent() throws Exception {
    runTestRegionOperationsIndependent(false);
  }

  /**
   * Verify that region operations, in this case splitting a region, are independent between the
   * cloned table and the original.
   */
  @Test (timeout=300000)
  public void testOnlineSnapshotRegionOperationsIndependent() throws Exception {
    runTestRegionOperationsIndependent(true);
  }

  private static void waitOnSplit(final HTable t, int originalCount) throws Exception {
    for (int i = 0; i < 200; i++) {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        // Restore the interrupted status
        Thread.currentThread().interrupt();
      }
      if (t.getRegionLocations().size() > originalCount) {
        return;
      }
    }
    throw new Exception("Split did not increase the number of regions");
  }

  /*
   * Take a snapshot of a table, add data, and verify that this only
   * affects one table
   * @param online - Whether the table is online or not during the snapshot
   */
  private void runTestSnapshotAppendIndependent(boolean online) throws Exception {
    FileSystem fs = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getFileSystem();
    Path rootDir = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();

    Admin admin = UTIL.getHBaseAdmin();
    final long startTime = System.currentTimeMillis();
    final TableName localTableName =
        TableName.valueOf(STRING_TABLE_NAME + startTime);

    HTable original = UTIL.createTable(localTableName, TEST_FAM);
    try {

      UTIL.loadTable(original, TEST_FAM);
      final int origTableRowCount = UTIL.countRows(original);

      // Take a snapshot
      final String snapshotNameAsString = "snapshot_" + localTableName;
      byte[] snapshotName = Bytes.toBytes(snapshotNameAsString);

      SnapshotTestingUtils.createSnapshotAndValidate(admin, localTableName, TEST_FAM_STR,
        snapshotNameAsString, rootDir, fs, online);

      if (!online) {
        admin.enableTable(localTableName);
      }
      TableName cloneTableName = TableName.valueOf("test-clone-" + localTableName);
      admin.cloneSnapshot(snapshotName, cloneTableName);

      Table clonedTable = new HTable(UTIL.getConfiguration(), cloneTableName);

      try {
        final int clonedTableRowCount = UTIL.countRows(clonedTable);

        Assert.assertEquals(
          "The line counts of original and cloned tables do not match after clone. ",
          origTableRowCount, clonedTableRowCount);

        // Attempt to add data to the test
        final String rowKey = "new-row-" + System.currentTimeMillis();

        Put p = new Put(Bytes.toBytes(rowKey));
        p.add(TEST_FAM, Bytes.toBytes("someQualifier"), Bytes.toBytes("someString"));
        original.put(p);
        original.flushCommits();

        // Verify that it is not present in the original table
        Assert.assertEquals("The row count of the original table was not modified by the put",
          origTableRowCount + 1, UTIL.countRows(original));
        Assert.assertEquals(
          "The row count of the cloned table changed as a result of addition to the original",
          clonedTableRowCount, UTIL.countRows(clonedTable));

        p = new Put(Bytes.toBytes(rowKey));
        p.add(TEST_FAM, Bytes.toBytes("someQualifier"), Bytes.toBytes("someString"));
        clonedTable.put(p);
        clonedTable.flushCommits();

        // Verify that the new family is not in the restored table's description
        Assert.assertEquals(
          "The row count of the original table was modified by the put to the clone",
          origTableRowCount + 1, UTIL.countRows(original));
        Assert.assertEquals("The row count of the cloned table was not modified by the put",
          clonedTableRowCount + 1, UTIL.countRows(clonedTable));
      } finally {

        clonedTable.close();
      }
    } finally {

      original.close();
    }
  }

  /*
   * Take a snapshot of a table, do a split, and verify that this only affects one table
   * @param online - Whether the table is online or not during the snapshot
   */
  private void runTestRegionOperationsIndependent(boolean online) throws Exception {
    FileSystem fs = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getFileSystem();
    Path rootDir = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();

    // Create a table
    Admin admin = UTIL.getHBaseAdmin();
    final long startTime = System.currentTimeMillis();
    final TableName localTableName =
        TableName.valueOf(STRING_TABLE_NAME + startTime);
    HTable original = UTIL.createTable(localTableName, TEST_FAM);
    UTIL.loadTable(original, TEST_FAM);
    final int loadedTableCount = UTIL.countRows(original);
    System.out.println("Original table has: " + loadedTableCount + " rows");

    final String snapshotNameAsString = "snapshot_" + localTableName;

    // Create a snapshot
    SnapshotTestingUtils.createSnapshotAndValidate(admin, localTableName, TEST_FAM_STR,
      snapshotNameAsString, rootDir, fs, online);

    if (!online) {
      admin.enableTable(localTableName);
    }

    TableName cloneTableName = TableName.valueOf("test-clone-" + localTableName);

    // Clone the snapshot
    byte[] snapshotName = Bytes.toBytes(snapshotNameAsString);
    admin.cloneSnapshot(snapshotName, cloneTableName);

    // Verify that region information is the same pre-split
    original.clearRegionCache();
    List<HRegionInfo> originalTableHRegions = admin.getTableRegions(localTableName);

    final int originalRegionCount = originalTableHRegions.size();
    final int cloneTableRegionCount = admin.getTableRegions(cloneTableName).size();
    Assert.assertEquals(
      "The number of regions in the cloned table is different than in the original table.",
      originalRegionCount, cloneTableRegionCount);

    // Split a region on the parent table
    admin.splitRegion(originalTableHRegions.get(0).getRegionName());
    waitOnSplit(original, originalRegionCount);

    // Verify that the cloned table region is not split
    final int cloneTableRegionCount2 = admin.getTableRegions(cloneTableName).size();
    Assert.assertEquals(
      "The number of regions in the cloned table changed though none of its regions were split.",
      cloneTableRegionCount, cloneTableRegionCount2);
  }

  /*
   * Take a snapshot of a table, add metadata, and verify that this only
   * affects one table
   * @param online - Whether the table is online or not during the snapshot
   */
  private void runTestSnapshotMetadataChangesIndependent(boolean online) throws Exception {
    FileSystem fs = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getFileSystem();
    Path rootDir = UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();

    // Create a table
    Admin admin = UTIL.getHBaseAdmin();
    final long startTime = System.currentTimeMillis();
    final TableName localTableName =
        TableName.valueOf(STRING_TABLE_NAME + startTime);
    HTable original = UTIL.createTable(localTableName, TEST_FAM);
    UTIL.loadTable(original, TEST_FAM);

    final String snapshotNameAsString = "snapshot_" + localTableName;

    // Create a snapshot
    SnapshotTestingUtils.createSnapshotAndValidate(admin, localTableName, TEST_FAM_STR,
      snapshotNameAsString, rootDir, fs, online);

    if (!online) {
      admin.enableTable(localTableName);
    }
    TableName cloneTableName = TableName.valueOf("test-clone-" + localTableName);

    // Clone the snapshot
    byte[] snapshotName = Bytes.toBytes(snapshotNameAsString);
    admin.cloneSnapshot(snapshotName, cloneTableName);

    // Add a new column family to the original table
    byte[] TEST_FAM_2 = Bytes.toBytes("fam2");
    HColumnDescriptor hcd = new HColumnDescriptor(TEST_FAM_2);

    admin.disableTable(localTableName);
    admin.addColumn(localTableName, hcd);

    // Verify that it is not in the snapshot
    admin.enableTable(localTableName);

    // get a description of the cloned table
    // get a list of its families
    // assert that the family is there
    HTableDescriptor originalTableDescriptor = original.getTableDescriptor();
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
}
