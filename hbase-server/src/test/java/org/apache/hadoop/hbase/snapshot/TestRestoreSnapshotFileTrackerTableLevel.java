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
package org.apache.hadoop.hbase.snapshot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a regression test for a bug introduced by HBASE-28564, which refactored reference file
 * creation to go through the StoreFileTracker interface but created the tracker using the raw
 * Master conf instead of merging table descriptor config.
 */
@Category({ RegionServerTests.class, LargeTests.class })
public class TestRestoreSnapshotFileTrackerTableLevel {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRestoreSnapshotFileTrackerTableLevel.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestRestoreSnapshotFileTrackerTableLevel.class);

  private static HBaseTestingUtil UTIL;
  private static final byte[] CF = Bytes.toBytes("cf");
  private static final String FILELIST_DIR = ".filelist";

  @BeforeClass
  public static void setupCluster() throws Exception {
    UTIL = new HBaseTestingUtil();
    Configuration conf = UTIL.getConfiguration();
    // Do NOT set TRACKER_IMPL=FILE globally.
    // The global config must default to DEFAULT tracker.
    // FILE tracker is set ONLY at the table descriptor level.
    assertFalse("Global conf must NOT have FILE tracker for this test to be valid",
      "FILE".equalsIgnoreCase(conf.get(StoreFileTrackerFactory.TRACKER_IMPL)));
    UTIL.startMiniCluster(3);
  }

  @AfterClass
  public static void tearDownCluster() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  /**
   * Test 1: Data change + compaction restore. Reproduces the exact production scenario: 1. Create
   * table with FILE tracker at TABLE level only (not global) 2. Load data, flush, take snapshot 3.
   * Load more data, flush, major compact (creates new HFiles not in snapshot) 4. Disable table,
   * restore from snapshot 5. Enable table — regions must open successfully 6. Verify row count
   * matches the snapshot Before the fix, step 5 fails with FileNotFoundException because
   * restoreRegion() used DefaultStoreFileTracker (no-op set()) instead of
   * FileBasedStoreFileTracker, so .filelist still references archived HFiles.
   */
  @Test
  public void testRestoreAfterDataChangeAndCompaction() throws Exception {
    Admin admin = UTIL.getAdmin();
    TableName tableName = TableName.valueOf("testRestoreAfterDataChangeAndCompaction");
    String snapshotName = "snapshot-data-change";

    try {
      // Step 1: Create table with FILE tracker at TABLE level only.
      // The global config does NOT have FILE, this is the trigger for the bug.
      TableDescriptor htd = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF))
        .setValue(StoreFileTrackerFactory.TRACKER_IMPL, "FILE").build();
      UTIL.getAdmin().createTable(htd, Bytes.toBytes("row00000"), Bytes.toBytes("row99999"), 4);

      // Step 2: Load initial data and flush
      loadData(tableName, 0, 500);
      admin.flush(tableName);

      int snapshotRowCount;
      try (Table table = UTIL.getConnection().getTable(tableName)) {
        snapshotRowCount = UTIL.countRows(table);
      }
      LOG.info("Snapshot row count: {}", snapshotRowCount);
      assertTrue("Should have loaded data", snapshotRowCount > 0);

      // Take snapshot
      admin.disableTable(tableName);
      admin.snapshot(snapshotName, tableName);
      admin.enableTable(tableName);

      // Step 3: Load more data, flush, and compact.
      // This creates new HFiles that are NOT in the snapshot.
      loadData(tableName, 500, 500);
      admin.flush(tableName);
      admin.majorCompact(tableName);
      UTIL.waitFor(30000, 500, () -> admin.getCompactionState(tableName) == CompactionState.NONE);

      int postCompactCount;
      try (Table table = UTIL.getConnection().getTable(tableName)) {
        postCompactCount = UTIL.countRows(table);
      }
      LOG.info("Post-compact row count: {} (snapshot had {})", postCompactCount, snapshotRowCount);
      assertTrue("Should have more rows after loading more data",
        postCompactCount > snapshotRowCount);

      // Step 4: Disable and restore from snapshot.
      // restoreRegion() must update .filelist to point to snapshot's HFiles.
      admin.disableTable(tableName);
      admin.restoreSnapshot(snapshotName);

      // Step 5: Enable table, triggers region opens.
      admin.enableTable(tableName);

      // Step 6: Verify all regions are online and data matches snapshot.
      List<RegionInfo> regions = admin.getRegions(tableName);
      LOG.info("Regions after restore: {}", regions.size());
      assertTrue("Table should have regions", regions.size() > 0);

      int finalCount;
      try (Table table = UTIL.getConnection().getTable(tableName)) {
        finalCount = UTIL.countRows(table);
      }
      LOG.info("Final row count: {} (expected: {})", finalCount, snapshotRowCount);
      assertEquals("Row count should match snapshot after restore", snapshotRowCount, finalCount);

      // Verify .filelist exists for each region
      verifyFileListExists(tableName, CF);
      LOG.info("Test 1 PASSED: restore after data change + compaction");

    } finally {
      cleanup(admin, tableName, snapshotName);
    }
  }

  /**
   * Test 2: Restore snapshot with different column families. Exercises the "Add families not
   * present in the table" code path: 1. Create table with two families (cf, cf2) and FILE tracker
   * at table level 2. Load data into both families, flush, take snapshot 3. Remove cf2 from the
   * table 4. Restore from snapshot (which has both families) 5. Verify both families are restored
   * and data is accessible
   */
  @Test
  public void testRestoreWithDifferentColumnFamilies() throws Exception {
    Admin admin = UTIL.getAdmin();
    TableName tableName = TableName.valueOf("testRestoreWithDifferentCFs");
    String snapshotName = "snapshot-two-families";
    byte[] CF2 = Bytes.toBytes("cf2");

    try {
      // Step 1: Create table with TWO families and FILE tracker at table level.
      TableDescriptor htd = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF2))
        .setValue(StoreFileTrackerFactory.TRACKER_IMPL, "FILE").build();
      UTIL.getAdmin().createTable(htd, Bytes.toBytes("row00000"), Bytes.toBytes("row99999"), 4);

      // Step 2: Load data into BOTH families and flush
      loadDataTwoFamilies(tableName, 0, 300, CF, CF2);
      admin.flush(tableName);

      int snapshotRowCount;
      try (Table table = UTIL.getConnection().getTable(tableName)) {
        snapshotRowCount = UTIL.countRows(table);
      }
      LOG.info("Snapshot row count (both families): {}", snapshotRowCount);

      // Take snapshot with both families
      admin.disableTable(tableName);
      admin.snapshot(snapshotName, tableName);
      admin.enableTable(tableName);

      // Step 3: Remove cf2 from the table
      admin.disableTable(tableName);
      admin.deleteColumnFamily(tableName, CF2);
      admin.enableTable(tableName);

      // Verify cf2 is gone
      TableDescriptor currentHtd = admin.getDescriptor(tableName);
      assertFalse("cf2 should be removed", currentHtd.hasColumnFamily(CF2));
      LOG.info("cf2 removed from table descriptor");

      // Step 4: Restore from snapshot (which has both families).
      // This exercises the "Add families not present in the table" code path.
      admin.disableTable(tableName);
      admin.restoreSnapshot(snapshotName);

      // Step 5: Enable table
      admin.enableTable(tableName);

      // Step 6: Verify both families are restored
      TableDescriptor restoredHtd = admin.getDescriptor(tableName);
      assertTrue("cf should exist after restore", restoredHtd.hasColumnFamily(CF));
      assertTrue("cf2 should exist after restore", restoredHtd.hasColumnFamily(CF2));

      int finalCount;
      try (Table table = UTIL.getConnection().getTable(tableName)) {
        finalCount = UTIL.countRows(table);
      }
      LOG.info("Final row count: {} (expected: {})", finalCount, snapshotRowCount);
      assertEquals("Row count should match snapshot", snapshotRowCount, finalCount);

      // Verify .filelist exists
      verifyFileListExists(tableName, CF, CF2);
      LOG.info("Test 2 PASSED: restore with different column families");

    } finally {
      cleanup(admin, tableName, snapshotName);
    }
  }

  private void loadData(TableName tableName, int startRow, int numRows) throws IOException {
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = startRow; i < startRow + numRows; i++) {
        Put put = new Put(Bytes.toBytes(String.format("row%05d", i)));
        put.addColumn(CF, Bytes.toBytes("q1"), Bytes.toBytes("value_" + i));
        table.put(put);
      }
    }
    LOG.info("Loaded {} rows starting at {}", numRows, startRow);
  }

  private void loadDataTwoFamilies(TableName tableName, int startRow, int numRows, byte[] cf1,
    byte[] cf2) throws IOException {
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = startRow; i < startRow + numRows; i++) {
        Put put = new Put(Bytes.toBytes(String.format("row%05d", i)));
        put.addColumn(cf1, Bytes.toBytes("q1"), Bytes.toBytes("cf1_value_" + i));
        put.addColumn(cf2, Bytes.toBytes("q1"), Bytes.toBytes("cf2_value_" + i));
        table.put(put);
      }
    }
    LOG.info("Loaded {} rows into both families starting at {}", numRows, startRow);
  }

  private void verifyFileListExists(TableName tableName, byte[]... families) throws IOException {
    Configuration conf = UTIL.getConfiguration();
    FileSystem fs = FileSystem.get(conf);
    Path rootDir = CommonFSUtils.getRootDir(conf);
    Path tableDir = CommonFSUtils.getTableDir(rootDir, tableName);

    int regionsWithFilelist = 0;
    List<RegionInfo> regions = UTIL.getAdmin().getRegions(tableName);
    for (RegionInfo ri : regions) {
      Path regionDir = new Path(tableDir, ri.getEncodedName());
      for (byte[] family : families) {
        Path familyDir = new Path(regionDir, Bytes.toString(family));
        Path fileListDir = new Path(familyDir, FILELIST_DIR);

        if (fs.exists(familyDir)) {
          FileStatus[] storeFiles =
            fs.listStatus(familyDir, path -> !path.getName().equals(FILELIST_DIR));
          if (storeFiles != null && storeFiles.length > 0) {
            assertTrue("Expected .filelist directory for region " + ri.getEncodedName() + " family "
              + Bytes.toString(family) + " at " + fileListDir, fs.exists(fileListDir));
            FileStatus[] files = fs.listStatus(fileListDir);
            assertTrue("Expected .filelist files for region " + ri.getEncodedName() + " family "
              + Bytes.toString(family), files != null && files.length > 0);
            regionsWithFilelist++;
            LOG.info("Region {} family {} has {} .filelist files", ri.getEncodedName(),
              Bytes.toString(family), files.length);
          } else {
            LOG.info("Region {} family {} has no store files, skipping .filelist check",
              ri.getEncodedName(), Bytes.toString(family));
          }
        }
      }
    }
    assertTrue("Expected at least one region with .filelist", regionsWithFilelist > 0);
  }

  private void cleanup(Admin admin, TableName tableName, String snapshotName) throws Exception {
    try {
      if (admin.tableExists(tableName)) {
        if (!admin.isTableDisabled(tableName)) {
          admin.disableTable(tableName);
        }
        admin.deleteTable(tableName);
      }
    } catch (Exception e) {
      LOG.warn("Cleanup error for table: {}", e.getMessage());
    }
    try {
      SnapshotTestingUtils.deleteAllSnapshots(admin);
    } catch (Exception e) {
      LOG.warn("Cleanup error for snapshots: {}", e.getMessage());
    }
    try {
      SnapshotTestingUtils.deleteArchiveDirectory(UTIL);
    } catch (Exception e) {
      LOG.warn("Cleanup error for archive: {}", e.getMessage());
    }
  }
}
