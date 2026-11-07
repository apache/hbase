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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.StoreContext;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.regionserver.StoreUtils;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTracker;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;

/**
 * Test RestoreSnapshotHelper with FileBasedStoreFileTracker (FILE SFT). Verifies that
 * restoreRegion() correctly uses the table descriptor's hbase.store.file-tracker.impl=FILE setting
 * when creating the StoreFileTracker, rather than falling back to DefaultStoreFileTracker (which is
 * a no-op for set()).
 */
@Category({ RegionServerTests.class, MediumTests.class })
public class TestRestoreSnapshotHelperWithFileBasedSFT {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRestoreSnapshotHelperWithFileBasedSFT.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestRestoreSnapshotHelperWithFileBasedSFT.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final String TEST_FAMILY = "cf";
  private static final byte[] TEST_FAMILY_BYTES = Bytes.toBytes(TEST_FAMILY);

  /** The .filelist directory name used by FileBasedStoreFileTracker. */
  private static final String FILELIST_DIR = ".filelist";

  private Configuration conf;
  private FileSystem fs;
  private Path rootDir;

  @BeforeClass
  public static void setupCluster() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDownCluster() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setup() throws Exception {
    rootDir = TEST_UTIL.getDataTestDir("testRestoreWithFileSFT");
    fs = TEST_UTIL.getTestFileSystem();
    conf = new Configuration(TEST_UTIL.getConfiguration());
    CommonFSUtils.setRootDir(conf, rootDir);
  }

  @After
  public void tearDown() throws Exception {
    fs.delete(TEST_UTIL.getDataTestDir(), true);
  }

  /**
   * Test that restoreRegion() writes .filelist when the table descriptor specifies
   * hbase.store.file-tracker.impl=FILE, even when the global Configuration does NOT have this
   * setting.
   */
  @Test
  public void testRestoreRegionWritesFileListWithFileTracker() throws IOException {
    // Ensure global conf does NOT have the FILE tracker, this simulates the Master's
    // global Configuration which defaults to DEFAULT.
    assertFalse("Global conf should not have FILE tracker set for this test",
      "FILE".equalsIgnoreCase(conf.get(StoreFileTrackerFactory.TRACKER_IMPL)));

    // Create a table descriptor WITH FILE tracker at the table level.
    // This is the modifiedTableDescriptor that RestoreSnapshotProcedure passes
    // to RestoreSnapshotHelper.
    TableName tableName = TableName.valueOf("testRestoreRegionWritesFileList");
    TableDescriptor htd = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(TEST_FAMILY_BYTES))
      .setValue(StoreFileTrackerFactory.TRACKER_IMPL, "FILE").build();

    // Manually create the table and snapshot on disk with our FILE-tracker htd.
    Path tableDir = CommonFSUtils.getTableDir(rootDir, tableName);
    new FSTableDescriptors(conf).createTableDescriptorForTableDirectory(tableDir, htd, false);

    // Create a region with HFiles
    byte[] startKey = Bytes.toBytes(0);
    byte[] endKey = Bytes.toBytes(1);
    RegionInfo hri =
      RegionInfoBuilder.newBuilder(tableName).setStartKey(startKey).setEndKey(endKey).build();
    HRegionFileSystem rfs = HRegionFileSystem.createRegionOnFileSystem(conf, fs, tableDir, hri);
    // Create store files for the region
    Path storeFile1 = createStoreFile(rfs.createTempName());
    Path committedFile1 = rfs.commitStoreFile(TEST_FAMILY, storeFile1);
    Path storeFile2 = createStoreFile(rfs.createTempName());
    Path committedFile2 = rfs.commitStoreFile(TEST_FAMILY, storeFile2);

    // Write .filelist so the FILE tracker can discover these files during snapshot creation.
    // The RegionServer writes .filelist on flush/compaction. In this unit test
    // we create files directly, so we must initialize the tracker manually.
    writeFileList(conf, htd, TEST_FAMILY_BYTES, rfs, tableDir, hri,
      new Path[] { committedFile1, committedFile2 });

    // Create the snapshot description
    SnapshotProtos.SnapshotDescription desc =
      SnapshotProtos.SnapshotDescription.newBuilder().setTable(tableName.getNameAsString())
        .setName("fileTrackerSnapshot").setVersion(SnapshotManifestV2.DESCRIPTOR_VERSION).build();

    // Create the working snapshot directory and write snapshot info
    Path workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(desc, rootDir, conf);
    SnapshotDescriptionUtils.writeSnapshotInfo(desc, workingDir, workingDir.getFileSystem(conf));

    // Build the snapshot manifest: add table descriptor, add region, then consolidate.
    // consolidate() writes the SnapshotDataManifest protobuf which contains the htd.
    ForeignExceptionDispatcher snapshotMonitor = new ForeignExceptionDispatcher(desc.getName());
    SnapshotManifest manifest =
      SnapshotManifest.create(conf, fs, workingDir, desc, snapshotMonitor);
    manifest.addTableDescriptor(htd);
    manifest.addRegion(tableDir, hri);
    manifest.consolidate();

    // Commit the snapshot (move working dir to completed dir)
    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(desc, rootDir);
    SnapshotDescriptionUtils.completeSnapshot(snapshotDir, workingDir, fs,
      workingDir.getFileSystem(conf), conf);

    // Now add extra files to the region to simulate post-snapshot changes.
    // These files are NOT in the snapshot, so restoreRegion() must remove them
    // and update the .filelist to only contain the snapshot's files.
    Path extraFile = createStoreFile(rfs.createTempName());
    rfs.commitStoreFile(TEST_FAMILY, extraFile);

    // Perform the restore using the FILE-tracker table descriptor.
    // The key assertion: conf does NOT have FILE tracker, but htd does.
    ForeignExceptionDispatcher monitor = Mockito.mock(ForeignExceptionDispatcher.class);
    MonitoredTask status = Mockito.mock(MonitoredTask.class);

    SnapshotManifest restoreManifest = SnapshotManifest.open(conf, fs, snapshotDir, desc);
    RestoreSnapshotHelper restoreHelper =
      new RestoreSnapshotHelper(conf, fs, restoreManifest, htd, rootDir, monitor, status);

    RestoreSnapshotHelper.RestoreMetaChanges metaChanges = restoreHelper.restoreHdfsRegions();
    assertNotNull("RestoreMetaChanges should not be null", metaChanges);

    // Verify that .filelist files were written for restored regions.
    // If the bug were present (DefaultStoreFileTracker used), no .filelist would exist.
    List<RegionInfo> regionsToRestore = metaChanges.getRegionsToRestore();
    if (regionsToRestore != null && !regionsToRestore.isEmpty()) {
      for (RegionInfo ri : regionsToRestore) {
        verifyFileListExists(tableDir, ri, TEST_FAMILY);
      }
      LOG.info("Verified .filelist for {} restored regions", regionsToRestore.size());
    }

    // Also check cloned regions (regionsToAdd), these use cloneRegion()
    List<RegionInfo> regionsToAdd = metaChanges.getRegionsToAdd();
    if (regionsToAdd != null && !regionsToAdd.isEmpty()) {
      for (RegionInfo ri : regionsToAdd) {
        verifyFileListExists(tableDir, ri, TEST_FAMILY);
      }
      LOG.info("Verified .filelist for {} cloned regions", regionsToAdd.size());
    }

    // At least one of the two lists should be non-empty
    assertTrue("Expected at least one restored or cloned region",
      (regionsToRestore != null && !regionsToRestore.isEmpty())
        || (regionsToAdd != null && !regionsToAdd.isEmpty()));
  }

  /**
   * Test that restoreRegion() correctly handles the case where the snapshot contains a column
   * family that does NOT exist on disk in the current table. This exercises the "Add families not
   * present in the table" code path in restoreRegion(), where the snapshot has families that the
   * on-disk region doesn't. The restore must create the family directory, restore the HFiles from
   * the snapshot, and write the .filelist using the correct FileBasedStoreFileTracker.
   */
  @Test
  public void testRestoreRegionWithNewFamilyFromSnapshot() throws IOException {
    assertFalse("Global conf should not have FILE tracker set for this test",
      "FILE".equalsIgnoreCase(conf.get(StoreFileTrackerFactory.TRACKER_IMPL)));

    TableName tableName = TableName.valueOf("testRestoreRegionWithNewFamily");
    String snapshotFamily2 = "cf2";
    byte[] snapshotFamily2Bytes = Bytes.toBytes(snapshotFamily2);

    // Table descriptor with TWO column families and FILE tracker.
    // This represents the table state at snapshot time.
    TableDescriptor htdWithBothFamilies = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(TEST_FAMILY_BYTES))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(snapshotFamily2Bytes))
      .setValue(StoreFileTrackerFactory.TRACKER_IMPL, "FILE").build();

    Path tableDir = CommonFSUtils.getTableDir(rootDir, tableName);
    new FSTableDescriptors(conf).createTableDescriptorForTableDirectory(tableDir,
      htdWithBothFamilies, false);

    // Create a region with HFiles in BOTH families
    byte[] startKey = Bytes.toBytes(0);
    byte[] endKey = Bytes.toBytes(1);
    RegionInfo hri =
      RegionInfoBuilder.newBuilder(tableName).setStartKey(startKey).setEndKey(endKey).build();
    HRegionFileSystem rfs = HRegionFileSystem.createRegionOnFileSystem(conf, fs, tableDir, hri);

    // Add files to cf (the family that will remain on disk)
    Path sf1 = createStoreFile(rfs.createTempName());
    Path committedSf1 = rfs.commitStoreFile(TEST_FAMILY, sf1);

    // Add files to cf2 (the family that will be in the snapshot but removed from disk)
    Path sf2 = createStoreFile(rfs.createTempName());
    Path committedSf2 = rfs.commitStoreFile(snapshotFamily2, sf2);
    Path sf3 = createStoreFile(rfs.createTempName());
    Path committedSf3 = rfs.commitStoreFile(snapshotFamily2, sf3);

    // Write .filelist for both families so the FILE tracker can discover files during
    // snapshot creation. Without this, addRegion() skips families with no tracked files.
    writeFileList(conf, htdWithBothFamilies, TEST_FAMILY_BYTES, rfs, tableDir, hri,
      new Path[] { committedSf1 });
    writeFileList(conf, htdWithBothFamilies, snapshotFamily2Bytes, rfs, tableDir, hri,
      new Path[] { committedSf2, committedSf3 });

    // Take snapshot with both families present
    SnapshotProtos.SnapshotDescription desc = SnapshotProtos.SnapshotDescription.newBuilder()
      .setTable(tableName.getNameAsString()).setName("snapshotWithTwoFamilies")
      .setVersion(SnapshotManifestV2.DESCRIPTOR_VERSION).build();

    Path workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(desc, rootDir, conf);
    SnapshotDescriptionUtils.writeSnapshotInfo(desc, workingDir, workingDir.getFileSystem(conf));

    ForeignExceptionDispatcher snapshotMonitor = new ForeignExceptionDispatcher(desc.getName());
    SnapshotManifest manifest =
      SnapshotManifest.create(conf, fs, workingDir, desc, snapshotMonitor);
    manifest.addTableDescriptor(htdWithBothFamilies);
    manifest.addRegion(tableDir, hri);
    manifest.consolidate();

    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(desc, rootDir);
    SnapshotDescriptionUtils.completeSnapshot(snapshotDir, workingDir, fs,
      workingDir.getFileSystem(conf), conf);

    // Now simulate the table having ONLY cf on disk (cf2 was removed after snapshot).
    // Delete cf2 directory from the region.
    Path regionDir = new Path(tableDir, hri.getEncodedName());
    Path family2Dir = new Path(regionDir, snapshotFamily2);
    assertTrue("cf2 directory should exist before deletion", fs.exists(family2Dir));
    fs.delete(family2Dir, true);
    assertFalse("cf2 directory should be gone", fs.exists(family2Dir));

    // Perform the restore. The restore should:
    // - For cf: keep existing files (should match the snapshot)
    // - For cf2: create the directory, restore HFiles from snapshot, write .filelist
    ForeignExceptionDispatcher monitor = Mockito.mock(ForeignExceptionDispatcher.class);
    MonitoredTask status = Mockito.mock(MonitoredTask.class);

    SnapshotManifest restoreManifest = SnapshotManifest.open(conf, fs, snapshotDir, desc);
    RestoreSnapshotHelper restoreHelper = new RestoreSnapshotHelper(conf, fs, restoreManifest,
      htdWithBothFamilies, rootDir, monitor, status);

    RestoreSnapshotHelper.RestoreMetaChanges metaChanges = restoreHelper.restoreHdfsRegions();
    assertNotNull("RestoreMetaChanges should not be null", metaChanges);

    // Verify .filelist was written for BOTH families
    List<RegionInfo> regionsToRestore = metaChanges.getRegionsToRestore();
    List<RegionInfo> regionsToAdd = metaChanges.getRegionsToAdd();

    // Collect all regions from both lists
    Set<String> verifiedRegions = new HashSet<>();
    if (regionsToRestore != null) {
      for (RegionInfo ri : regionsToRestore) {
        // cf should have .filelist (existing family, restored)
        verifyFileListExists(tableDir, ri, TEST_FAMILY);
        // cf2 should have .filelist (new family from snapshot)
        verifyFileListExists(tableDir, ri, snapshotFamily2);
        verifiedRegions.add(ri.getEncodedName());
        LOG.info("Verified .filelist for both families in restored region {}", ri.getEncodedName());
      }
    }
    if (regionsToAdd != null) {
      for (RegionInfo ri : regionsToAdd) {
        verifyFileListExists(tableDir, ri, TEST_FAMILY);
        verifyFileListExists(tableDir, ri, snapshotFamily2);
        verifiedRegions.add(ri.getEncodedName());
        LOG.info("Verified .filelist for both families in cloned region {}", ri.getEncodedName());
      }
    }

    assertTrue("Expected at least one restored or cloned region", !verifiedRegions.isEmpty());

    // Verify cf2 directory was re-created with files
    assertTrue("cf2 directory should be re-created after restore", fs.exists(family2Dir));
    LOG.info("Restore with new family from snapshot verified successfully");
  }

  /**
   * Verify that .filelist directory exists and contains at least one file for the given region and
   * family.
   */
  private void verifyFileListExists(Path tableDir, RegionInfo ri, String family)
    throws IOException {
    Path regionDir = new Path(tableDir, ri.getEncodedName());
    Path familyDir = new Path(regionDir, family);
    Path fileListDir = new Path(familyDir, FILELIST_DIR);
    assertTrue(
      "Expected .filelist directory for region " + ri.getEncodedName() + " at " + fileListDir,
      fs.exists(fileListDir));
    FileStatus[] fileListFiles = fs.listStatus(fileListDir);
    assertTrue("Expected at least one .filelist file for region " + ri.getEncodedName(),
      fileListFiles != null && fileListFiles.length > 0);
    LOG.info("Verified .filelist exists for region {} with {} files", ri.getEncodedName(),
      fileListFiles.length);
  }

  /**
   * Write .filelist entries for the given committed store files so that FileBasedStoreFileTracker
   * can discover them (e.g. during snapshot creation). The RegionServer writes .filelist on
   * flush/compaction; in unit tests we must do it manually.
   */
  private void writeFileList(Configuration conf, TableDescriptor htd, byte[] family,
    HRegionFileSystem regionFS, Path tableDir, RegionInfo regionInfo, Path[] committedFiles)
    throws IOException {
    Path familyDir =
      new Path(new Path(tableDir, regionInfo.getEncodedName()), Bytes.toString(family));
    Configuration sftConf =
      StoreUtils.createStoreConfiguration(conf, htd, htd.getColumnFamily(family));
    StoreFileTracker tracker = StoreFileTrackerFactory.create(sftConf, true,
      StoreContext.getBuilder().withColumnFamilyDescriptor(htd.getColumnFamily(family))
        .withFamilyStoreDirectoryPath(familyDir).withRegionFileSystem(regionFS).build());
    List<StoreFileInfo> fileInfos = new ArrayList<>();
    for (Path committedFile : committedFiles) {
      fileInfos.add(tracker.getStoreFileInfo(committedFile, true));
    }
    tracker.set(fileInfos);
    LOG.info("Wrote .filelist for family {} with {} files in region {}", Bytes.toString(family),
      committedFiles.length, regionInfo.getEncodedName());
  }

  /**
   * Create a simple store file with some content.
   */
  private Path createStoreFile(final Path storeFile) throws IOException {
    FSDataOutputStream out = fs.create(storeFile);
    try {
      out.write(Bytes.toBytes(storeFile.toString()));
    } finally {
      out.close();
    }
    return storeFile;
  }
}
