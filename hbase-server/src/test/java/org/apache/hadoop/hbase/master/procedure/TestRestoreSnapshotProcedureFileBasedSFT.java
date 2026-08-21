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
package org.apache.hadoop.hbase.master.procedure;

import static org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory.TRACKER_IMPL;
import static org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory.Trackers.FILE;
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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
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
 * Integration test for RestoreSnapshotProcedure with FileBasedStoreFileTracker. Verifies the
 * end-to-end restore snapshot flow works correctly when the FILE-based StoreFileTracker is in use.
 * Uses HBaseTestingUtil to start an in-process mini HBase cluster to exercise the restoreRegion()
 * code path in RestoreSnapshotHelper.
 */
@Category({ MasterTests.class, LargeTests.class })
public class TestRestoreSnapshotProcedureFileBasedSFT {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRestoreSnapshotProcedureFileBasedSFT.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestRestoreSnapshotProcedureFileBasedSFT.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static final byte[] CF = Bytes.toBytes("cf");

  /** The .filelist directory name used by FileBasedStoreFileTracker. */
  private static final String FILELIST_DIR = ".filelist";

  @BeforeClass
  public static void setupCluster() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    // Do NOT set FILE tracker globally — the bug only manifests when
    // global is Default and table-level config is FILE.
    conf.setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, 1);
    UTIL.startMiniCluster(3);
  }

  @AfterClass
  public static void tearDownCluster() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  /**
   * Test that reproduces the scenario where restore fails with FileNotFoundException: 1. Create
   * table with FILE tracker, load data, snapshot 2. Load more data + flush (creates new HFiles not
   * in snapshot) 3. Restore from snapshot 4. Verify all regions open and data matches the snapshot
   * Before the fix, step 4 would fail with FileNotFoundException because the .filelist still
   * referenced the post-snapshot HFiles that were archived during restore.
   */
  @Test
  public void testRestoreSnapshotWithFileTrackerAfterDataChange() throws Exception {
    Admin admin = UTIL.getAdmin();
    TableName tableName = TableName.valueOf("testRestoreWithFileTracker");
    String snapshotName = "snapshot-before-change";

    try {
      // Step 1: Create table, load initial data, take snapshot
      TableDescriptor htd = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF)).setValue(TRACKER_IMPL, FILE.name())
        .build();
      UTIL.getAdmin().createTable(htd);
      SnapshotTestingUtils.loadData(UTIL, tableName, 500, CF);

      int snapshotRowCount;
      try (Table table = UTIL.getConnection().getTable(tableName)) {
        snapshotRowCount = UTIL.countRows(table);
      }
      LOG.info("Snapshot row count: {}", snapshotRowCount);
      assertTrue("Should have loaded data", snapshotRowCount > 0);

      admin.disableTable(tableName);
      admin.snapshot(snapshotName, tableName);
      admin.enableTable(tableName);

      // Step 2: Load more data and flush to create new HFiles.
      // These new HFiles will NOT be in the snapshot.
      SnapshotTestingUtils.loadData(UTIL, tableName, 500, CF);
      admin.flush(tableName);

      int postFlushRowCount;
      try (Table table = UTIL.getConnection().getTable(tableName)) {
        postFlushRowCount = UTIL.countRows(table);
      }
      LOG.info("Post-flush row count: {} (snapshot had {})", postFlushRowCount, snapshotRowCount);
      assertTrue("Should have more rows after loading more data",
        postFlushRowCount > snapshotRowCount);

      // Step 3: Disable and restore from the earlier snapshot.
      // restoreRegion() must update .filelist to point to the snapshot's HFiles.
      admin.disableTable(tableName);
      admin.restoreSnapshot(snapshotName);

      // Step 4: Enable table, triggers region opens.
      admin.enableTable(tableName);

      // Verify all regions are online
      List<RegionInfo> regions = admin.getRegions(tableName);
      LOG.info("Number of regions after restore: {}", regions.size());
      assertTrue("Table should have at least one region", regions.size() > 0);

      // Verify data matches the snapshot (not the post-flush state)
      SnapshotTestingUtils.verifyRowCount(UTIL, tableName, snapshotRowCount);
      LOG.info("Data verification passed: row count matches snapshot ({})", snapshotRowCount);

      // Verify .filelist files exist on disk for each region
      verifyFileListExists(tableName);

    } finally {
      if (admin.tableExists(tableName)) {
        if (!admin.isTableDisabled(tableName)) {
          admin.disableTable(tableName);
        }
        admin.deleteTable(tableName);
      }
      SnapshotTestingUtils.deleteAllSnapshots(admin);
      SnapshotTestingUtils.deleteArchiveDirectory(UTIL);
    }
  }

  /**
   * Test restore after compaction, compaction creates new HFiles that replace the originals. After
   * restore, the .filelist must point to the snapshot's HFiles, not the compaction output.
   */
  @Test
  public void testRestoreSnapshotAfterCompaction() throws Exception {
    Admin admin = UTIL.getAdmin();
    TableName tableName = TableName.valueOf("testRestoreAfterCompaction");
    String snapshotName = "snapshot-before-compaction";

    try {
      // Create table with multiple regions
      TableDescriptor htd = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF)).setValue(TRACKER_IMPL, FILE.name())
        .build();
      UTIL.getAdmin().createTable(htd, Bytes.toBytes("row00000"), Bytes.toBytes("row99999"), 4);

      // Load data across regions
      try (Table table = UTIL.getConnection().getTable(tableName)) {
        for (int i = 0; i < 200; i++) {
          Put put = new Put(Bytes.toBytes(String.format("row%05d", i)));
          put.addColumn(CF, Bytes.toBytes("q"), Bytes.toBytes("val" + i));
          table.put(put);
        }
      }
      admin.flush(tableName);

      int snapshotRowCount;
      try (Table table = UTIL.getConnection().getTable(tableName)) {
        snapshotRowCount = UTIL.countRows(table);
      }

      // Take snapshot
      admin.disableTable(tableName);
      admin.snapshot(snapshotName, tableName);
      admin.enableTable(tableName);

      // Load more data and trigger compaction, creates new HFiles
      SnapshotTestingUtils.loadData(UTIL, tableName, 200, CF);
      admin.flush(tableName);
      admin.majorCompact(tableName);
      // Wait for compaction to complete
      Thread.sleep(5000);

      // Restore from pre-compaction snapshot
      admin.disableTable(tableName);
      admin.restoreSnapshot(snapshotName);
      admin.enableTable(tableName);

      // Verify regions open and data is correct
      SnapshotTestingUtils.verifyRowCount(UTIL, tableName, snapshotRowCount);
      verifyFileListExists(tableName);
      LOG.info("Restore after compaction verified successfully");

    } finally {
      if (admin.tableExists(tableName)) {
        if (!admin.isTableDisabled(tableName)) {
          admin.disableTable(tableName);
        }
        admin.deleteTable(tableName);
      }
      SnapshotTestingUtils.deleteAllSnapshots(admin);
      SnapshotTestingUtils.deleteArchiveDirectory(UTIL);
    }
  }

  /**
   * Verify that .filelist files exist for each region/family of the table. This confirms that
   * FileBasedStoreFileTracker.doSetStoreFiles() was called (not the no-op
   * DefaultStoreFileTracker.doSetStoreFiles()).
   */
  private void verifyFileListExists(TableName tableName) throws IOException {
    Configuration conf = UTIL.getConfiguration();
    FileSystem fs = FileSystem.get(conf);
    Path rootDir = CommonFSUtils.getRootDir(conf);
    Path tableDir = CommonFSUtils.getTableDir(rootDir, tableName);

    int regionsWithFilelist = 0;
    List<RegionInfo> regions = UTIL.getAdmin().getRegions(tableName);
    for (RegionInfo ri : regions) {
      Path regionDir = new Path(tableDir, ri.getEncodedName());
      Path familyDir = new Path(regionDir, Bytes.toString(CF));
      Path fileListDir = new Path(familyDir, FILELIST_DIR);

      if (fs.exists(familyDir)) {
        // Only check .filelist if the family directory has store files (non-directory entries
        // excluding .filelist itself). Empty regions may not have .filelist written.
        FileStatus[] storeFiles =
          fs.listStatus(familyDir, path -> !path.getName().equals(FILELIST_DIR));
        if (storeFiles != null && storeFiles.length > 0) {
          assertTrue("Expected .filelist directory for region " + ri.getEncodedName(),
            fs.exists(fileListDir));
          FileStatus[] files = fs.listStatus(fileListDir);
          assertTrue("Expected .filelist files for region " + ri.getEncodedName(),
            files != null && files.length > 0);
          regionsWithFilelist++;
          LOG.info("Region {} has {} .filelist files", ri.getEncodedName(), files.length);
        } else {
          LOG.info("Region {} has no store files in family dir, skipping .filelist check",
            ri.getEncodedName());
        }
      }
    }
    assertTrue("Expected at least one region with .filelist", regionsWithFilelist > 0);
  }
}
