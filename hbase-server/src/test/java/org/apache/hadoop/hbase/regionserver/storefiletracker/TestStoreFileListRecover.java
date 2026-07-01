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
package org.apache.hadoop.hbase.regionserver.storefiletracker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseCommonTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.HFileTestUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import org.apache.hadoop.hbase.shaded.protobuf.generated.FSProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.StoreFileTrackerProtos.StoreFileEntry;
import org.apache.hadoop.hbase.shaded.protobuf.generated.StoreFileTrackerProtos.StoreFileList;

@Tag(RegionServerTests.TAG)
@Tag(SmallTests.TAG)
public class TestStoreFileListRecover {

  private static final HBaseCommonTestingUtil UTIL = new HBaseCommonTestingUtil();
  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");
  private static final String FAMILY_NAME = Bytes.toString(FAMILY);
  private static final TableName TABLE_NAME = TableName.valueOf("ns:tbl");

  private FileSystem fs;
  private Path rootDir;
  private Path tableDir;
  private TableDescriptor tableDescriptor;
  private ColumnFamilyDescriptor familyDescriptor;

  @BeforeEach
  public void setUp(TestInfo testInfo) throws IOException {
    fs = FileSystem.get(UTIL.getConfiguration());
    rootDir = UTIL.getDataTestDir(testInfo.getTestMethod().get().getName());
    tableDir = CommonFSUtils.getTableDir(rootDir, TABLE_NAME);
    fs.mkdirs(tableDir);
    familyDescriptor = ColumnFamilyDescriptorBuilder.of(FAMILY);
    tableDescriptor =
      TableDescriptorBuilder.newBuilder(TABLE_NAME).setColumnFamily(familyDescriptor).build();
  }

  @AfterAll
  public static void tearDown() {
    UTIL.cleanupTestDir();
  }

  @Test
  public void testCorruptedManifestIsDiagnosedAndReplaced() throws Exception {
    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(1L).build();
    HRegionFileSystem regionFs = createRegion(regionInfo);
    Path familyDir = regionFs.getStoreDir(FAMILY_NAME);
    Path hfile = new Path(familyDir, "abcdef01");
    HFileTestUtil.createHFile(UTIL.getConfiguration(), fs, hfile, FAMILY, QUALIFIER,
      Bytes.toBytes("a"), Bytes.toBytes("z"), 10);
    Path corrupt = writeCorruptTracker(regionFs, "f1.1");

    StoreFileListRecover.RecoverReport report = StoreFileListRecover.recover(
      UTIL.getConfiguration(), tableDescriptor, familyDescriptor, regionFs,
      Collections.emptyList(), false);

    // Diagnostics must mention the corrupted file.
    assertTrue(report.hasCorruption(), "expected diagnostics to surface the corrupted file");
    assertTrue(
      report.getDiagnostics().stream()
        .anyMatch(d -> d.isCorrupted() && d.getPath().getName().equals(corrupt.getName())),
      "corrupted file should be reported by name");

    assertEquals(1, report.getManifestEntries().size());
    assertNotNull(report.getWrittenManifest());

    StoreFileList recovered = StoreFileListFile.load(fs, report.getWrittenManifest());
    assertEquals(1, recovered.getStoreFileCount());
    assertEquals("abcdef01", recovered.getStoreFile(0).getName());

    // The recovered manifest must have a strictly newer seqId than the corrupted file.
    long corruptSeqId = parseSeqId(corrupt);
    long recoveredSeqId = parseSeqId(report.getWrittenManifest());
    assertTrue(recoveredSeqId > corruptSeqId,
      "recovered seqId " + recoveredSeqId + " should be > corrupted " + corruptSeqId);
  }

  @Test
  public void testNoParentsIsDiskOnly() throws Exception {
    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(2L).build();
    HRegionFileSystem regionFs = createRegion(regionInfo);
    Path hfile = new Path(regionFs.getStoreDir(FAMILY_NAME), "abcdef02");
    HFileTestUtil.createHFile(UTIL.getConfiguration(), fs, hfile, FAMILY, QUALIFIER,
      Bytes.toBytes("a"), Bytes.toBytes("z"), 10);

    StoreFileListRecover.RecoverReport report = StoreFileListRecover.recover(
      UTIL.getConfiguration(), tableDescriptor, familyDescriptor, regionFs,
      Collections.emptyList(), false);

    assertEquals(1, report.getManifestEntries().size());
    assertTrue(report.getParentContributions().isEmpty(),
      "no parents passed -> no parent assessment");
    StoreFileList recovered = StoreFileListFile.load(fs, report.getWrittenManifest());
    assertEquals(1, recovered.getStoreFileCount());
    assertEquals("abcdef02", recovered.getStoreFile(0).getName());
  }

  @Test
  public void testArchivedParentReportsNoDataLoss() throws Exception {
    RegionInfo parent = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(51L)
      .setStartKey(Bytes.toBytes("")).setEndKey(Bytes.toBytes("")).build();
    HRegionFileSystem parentFs = createRegion(parent);
    HFileTestUtil.createHFile(UTIL.getConfiguration(), fs,
      new Path(parentFs.getStoreDir(FAMILY_NAME), "abcdef50"), FAMILY, QUALIFIER,
      Bytes.toBytes("a"), Bytes.toBytes("z"), 10);

    RegionInfo topChild = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(52L)
      .setStartKey(Bytes.toBytes("m")).setEndKey(Bytes.toBytes("")).build();
    // Child has its own already-compacted-in HFile.
    HRegionFileSystem childFs = createRegion(topChild);
    HFileTestUtil.createHFile(UTIL.getConfiguration(), fs,
      new Path(childFs.getStoreDir(FAMILY_NAME), "abcdef59"), FAMILY, QUALIFIER, Bytes.toBytes("m"),
      Bytes.toBytes("z"), 10);

    // Simulate Catalog Janitor having archived (deleted) the parent's region directory.
    Path parentRegionDir = new Path(tableDir, parent.getEncodedName());
    assertTrue(fs.exists(parentRegionDir), "test setup: parent dir should exist");
    assertTrue(fs.delete(parentRegionDir, true), "delete parent dir to simulate archive");

    StoreFileListRecover.RecoverReport report = StoreFileListRecover.recover(
      UTIL.getConfiguration(), tableDescriptor, familyDescriptor, childFs,
      Collections.singletonList(parent), false);

    // Manifest is rebuilt from the child's own disk files; the parent never contributes entries.
    assertEquals(1, report.getManifestEntries().size(),
      "manifest is disk-only and must contain only the child's own HFile");
    assertEquals("abcdef59", report.getManifestEntries().get(0).getPath().getName());

    // Parent contribution is reported as ARCHIVED -> no data loss.
    assertEquals(1, report.getParentContributions().size());
    StoreFileListRecover.ParentContribution pc = report.getParentContributions().get(0);
    assertEquals(parent.getEncodedName(), pc.getParent().getEncodedName());
    assertEquals(StoreFileListRecover.ParentContribution.Status.ARCHIVED, pc.getStatus());
    assertEquals(0, pc.getUnarchivedHFileCount());
    assertTrue(report.allParentsArchived(),
      "allParentsArchived should be true when parent is archived");
    assertFalse(report.hasUnarchivedParents(),
      "hasUnarchivedParents should be false when parent is archived");
  }

  @Test
  public void testUnarchivedParentReportsPotentialDataLoss() throws Exception {
    // Split parent is still present on disk with HFiles -> potential data loss.
    RegionInfo parent = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(53L)
      .setStartKey(Bytes.toBytes("")).setEndKey(Bytes.toBytes("")).build();
    HRegionFileSystem parentFs = createRegion(parent);
    HFileTestUtil.createHFile(UTIL.getConfiguration(), fs,
      new Path(parentFs.getStoreDir(FAMILY_NAME), "abcdef55"), FAMILY, QUALIFIER,
      Bytes.toBytes("a"), Bytes.toBytes("z"), 10);

    RegionInfo topChild = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(54L)
      .setStartKey(Bytes.toBytes("m")).setEndKey(Bytes.toBytes("")).build();
    HRegionFileSystem childFs = createRegion(topChild);

    StoreFileListRecover.RecoverReport report = StoreFileListRecover.recover(
      UTIL.getConfiguration(), tableDescriptor, familyDescriptor, childFs,
      Collections.singletonList(parent), false);

    // The manifest is still disk-only: the unarchived parent does NOT inject entries.
    assertEquals(0, report.getManifestEntries().size(),
      "manifest must remain disk-only; parent files are never injected");

    // Parent contribution should be PRESENT_WITH_FILES -> potential data loss.
    assertEquals(1, report.getParentContributions().size());
    StoreFileListRecover.ParentContribution pc = report.getParentContributions().get(0);
    assertEquals(parent.getEncodedName(), pc.getParent().getEncodedName());
    assertEquals(StoreFileListRecover.ParentContribution.Status.PRESENT_WITH_FILES, pc.getStatus());
    assertTrue(pc.getUnarchivedHFileCount() > 0, "unarchived HFile count should be > 0");
    assertFalse(report.allParentsArchived(),
      "allParentsArchived should be false when parent has files");
    assertTrue(report.hasUnarchivedParents(),
      "hasUnarchivedParents should be true when parent has files");
  }

  @Test
  public void testMergeWithMixedArchiveStatus() throws Exception {
    // Two merge parents: one archived, one still present with files.
    RegionInfo mergeParentA = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(55L)
      .setStartKey(Bytes.toBytes("")).setEndKey(Bytes.toBytes("m")).build();
    RegionInfo mergeParentB = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(56L)
      .setStartKey(Bytes.toBytes("m")).setEndKey(Bytes.toBytes("")).build();
    HRegionFileSystem parentAFs = createRegion(mergeParentA);
    HRegionFileSystem parentBFs = createRegion(mergeParentB);
    HFileTestUtil.createHFile(UTIL.getConfiguration(), fs,
      new Path(parentAFs.getStoreDir(FAMILY_NAME), "abcdef56"), FAMILY, QUALIFIER,
      Bytes.toBytes("a"), Bytes.toBytes("l"), 10);
    HFileTestUtil.createHFile(UTIL.getConfiguration(), fs,
      new Path(parentBFs.getStoreDir(FAMILY_NAME), "abcdef57"), FAMILY, QUALIFIER,
      Bytes.toBytes("m"), Bytes.toBytes("z"), 10);

    // Delete parent A to simulate archival.
    Path parentADir = new Path(tableDir, mergeParentA.getEncodedName());
    assertTrue(fs.delete(parentADir, true), "delete parent A to simulate archive");

    RegionInfo mergedChild = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(57L)
      .setStartKey(Bytes.toBytes("")).setEndKey(Bytes.toBytes("")).build();
    HRegionFileSystem childFs = createRegion(mergedChild);

    StoreFileListRecover.RecoverReport report = StoreFileListRecover.recover(
      UTIL.getConfiguration(), tableDescriptor, familyDescriptor, childFs,
      Arrays.asList(mergeParentA, mergeParentB), false);

    // Two parent contributions: one ARCHIVED, one PRESENT_WITH_FILES.
    assertEquals(2, report.getParentContributions().size());
    StoreFileListRecover.ParentContribution pcA = report.getParentContributions().stream()
      .filter(pc -> pc.getParent().getEncodedName().equals(mergeParentA.getEncodedName()))
      .findFirst().orElse(null);
    StoreFileListRecover.ParentContribution pcB = report.getParentContributions().stream()
      .filter(pc -> pc.getParent().getEncodedName().equals(mergeParentB.getEncodedName()))
      .findFirst().orElse(null);
    assertNotNull(pcA, "parent A contribution must be present");
    assertNotNull(pcB, "parent B contribution must be present");
    assertEquals(StoreFileListRecover.ParentContribution.Status.ARCHIVED, pcA.getStatus());
    assertEquals(StoreFileListRecover.ParentContribution.Status.PRESENT_WITH_FILES,
      pcB.getStatus());
    assertFalse(report.allParentsArchived(), "allParentsArchived should be false (mixed status)");
    assertTrue(report.hasUnarchivedParents(),
      "hasUnarchivedParents should be true (parent B has files)");
  }

  @Test
  public void testDryRunDoesNotWriteManifest() throws Exception {
    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(8L).build();
    HRegionFileSystem regionFs = createRegion(regionInfo);
    Path hfile = new Path(regionFs.getStoreDir(FAMILY_NAME), "abcdef30");
    HFileTestUtil.createHFile(UTIL.getConfiguration(), fs, hfile, FAMILY, QUALIFIER,
      Bytes.toBytes("a"), Bytes.toBytes("z"), 10);
    Path corrupt = writeCorruptTracker(regionFs, "f1.1");

    StoreFileListRecover.RecoverReport report = StoreFileListRecover.recover(
      UTIL.getConfiguration(), tableDescriptor, familyDescriptor, regionFs,
      Collections.emptyList(), true);

    assertNull(report.getWrittenManifest(), "dry-run must not write a new manifest");
    assertTrue(fs.exists(corrupt), "corrupted tracker file must remain after dry-run");
    Path trackDir = new Path(regionFs.getStoreDir(FAMILY_NAME), StoreFileListFile.TRACK_FILE_DIR);
    // Only the corrupt file should be in the track dir, no new f1/f2 should have been created.
    int count = 0;
    for (org.apache.hadoop.fs.FileStatus s : fs.listStatus(trackDir)) {
      assertEquals(corrupt.getName(), s.getPath().getName());
      count++;
    }
    assertEquals(1, count);
  }

  @Test
  public void testNoOpWhenManifestAlreadyMatchesDisk() throws Exception {
    // First, write a healthy manifest by running recover against a non-corrupted store.
    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(9L).build();
    HRegionFileSystem regionFs = createRegion(regionInfo);
    Path hfile = new Path(regionFs.getStoreDir(FAMILY_NAME), "abcdef60");
    HFileTestUtil.createHFile(UTIL.getConfiguration(), fs, hfile, FAMILY, QUALIFIER,
      Bytes.toBytes("a"), Bytes.toBytes("z"), 10);

    StoreFileListRecover.RecoverReport first = StoreFileListRecover.recover(
      UTIL.getConfiguration(), tableDescriptor, familyDescriptor, regionFs,
      Collections.emptyList(), false);
    assertNotNull(first.getWrittenManifest());
    assertFalse(first.isNoOp());

    // Run again. There is no corruption and the manifest matches disk; should be a no-op.
    StoreFileListRecover.RecoverReport second = StoreFileListRecover.recover(
      UTIL.getConfiguration(), tableDescriptor, familyDescriptor, regionFs,
      Collections.emptyList(), false);
    assertTrue(second.isNoOp(), "second recover should be a no-op");
    assertNull(second.getWrittenManifest(), "no new manifest should have been written");
  }

  @Test
  public void testCorruptHighestSeqIdIsNotMaskedByHealthyOlderFile() throws Exception {
    // Write a healthy manifest first.
    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(10L).build();
    HRegionFileSystem regionFs = createRegion(regionInfo);
    Path hfile = new Path(regionFs.getStoreDir(FAMILY_NAME), "abcdef70");
    HFileTestUtil.createHFile(UTIL.getConfiguration(), fs, hfile, FAMILY, QUALIFIER,
      Bytes.toBytes("a"), Bytes.toBytes("z"), 10);
    StoreFileListRecover.RecoverReport first = StoreFileListRecover.recover(
      UTIL.getConfiguration(), tableDescriptor, familyDescriptor, regionFs,
      Collections.emptyList(), false);
    long healthySeqId = parseSeqId(first.getWrittenManifest());

    // Now plant a corrupted tracker file with a *higher* seqId than the healthy generation. The
    // runtime load(false) visits the highest seqId first, so this corruption would fail region open
    // even though the older healthy file matches disk. Recovery must NOT treat this as a no-op.
    long corruptSeqId = healthySeqId + 1_000_000L;
    Path corrupt = writeCorruptTracker(regionFs, "f2." + corruptSeqId);

    StoreFileListRecover.RecoverReport second = StoreFileListRecover.recover(
      UTIL.getConfiguration(), tableDescriptor, familyDescriptor, regionFs,
      Collections.emptyList(), false);

    assertTrue(second.hasCorruption(), "the higher-seqId corruption must be surfaced");
    assertFalse(second.isNoOp(),
      "recovery must not no-op when a corrupt file outranks the healthy generation");
    assertNotNull(second.getWrittenManifest(), "a fresh generation must be written");
    long recoveredSeqId = parseSeqId(second.getWrittenManifest());
    assertTrue(recoveredSeqId > corruptSeqId, "recovered seqId " + recoveredSeqId
      + " must outrank the corrupt file " + corruptSeqId);
    assertTrue(fs.exists(corrupt), "corrupt file is left in place; pruned on next load(false)");
  }

  @Test
  public void testCorruptOlderFileDoesNotBlockNoOp() throws Exception {
    // A healthy manifest plus a corrupted file with a *lower* seqId: the runtime would never reach
    // the corrupt file, so the store is effectively healthy and recovery should no-op.
    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(11L).build();
    HRegionFileSystem regionFs = createRegion(regionInfo);
    Path hfile = new Path(regionFs.getStoreDir(FAMILY_NAME), "abcdef71");
    HFileTestUtil.createHFile(UTIL.getConfiguration(), fs, hfile, FAMILY, QUALIFIER,
      Bytes.toBytes("a"), Bytes.toBytes("z"), 10);
    StoreFileListRecover.RecoverReport first = StoreFileListRecover.recover(
      UTIL.getConfiguration(), tableDescriptor, familyDescriptor, regionFs,
      Collections.emptyList(), false);
    assertNotNull(first.getWrittenManifest());

    // Plant a corrupt file whose numeric seqId is below the healthy generation's.
    writeCorruptTracker(regionFs, "f1.1");

    StoreFileListRecover.RecoverReport second = StoreFileListRecover.recover(
      UTIL.getConfiguration(), tableDescriptor, familyDescriptor, regionFs,
      Collections.emptyList(), false);

    assertTrue(second.isNoOp(),
      "a lower-seqId corrupt file the runtime never reaches must not force a rewrite");
    assertNull(second.getWrittenManifest());
  }

  @Test
  public void testReferenceFilePreservedInRecoveredManifest() throws Exception {
    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(12L).build();
    HRegionFileSystem regionFs = createRegion(regionInfo);
    Path familyDir = regionFs.getStoreDir(FAMILY_NAME);
    // A plain HFile plus a TOP split-reference file physically present on disk.
    HFileTestUtil.createHFile(UTIL.getConfiguration(), fs, new Path(familyDir, "abcdef80"), FAMILY,
      QUALIFIER, Bytes.toBytes("a"), Bytes.toBytes("z"), 10);
    byte[] splitRow = Bytes.toBytes("split-row-key");
    String refName = "abcdef81.0123456789abcdef0123456789abcde0";
    Reference original = Reference.createTopReference(splitRow);
    original.write(fs, new Path(familyDir, refName));

    StoreFileListRecover.RecoverReport report = StoreFileListRecover.recover(
      UTIL.getConfiguration(), tableDescriptor, familyDescriptor, regionFs,
      Collections.emptyList(), false);

    assertEquals(2, report.getManifestEntries().size(),
      "both the HFile and reference are recorded");
    StoreFileList recovered = StoreFileListFile.load(fs, report.getWrittenManifest());
    StoreFileEntry refEntry = recovered.getStoreFileList().stream()
      .filter(e -> e.getName().equals(refName)).findFirst().orElse(null);
    assertNotNull(refEntry, "the reference entry must be in the recovered manifest");
    assertTrue(refEntry.hasReference(), "reference entry must carry a Reference body");
    assertEquals(FSProtos.Reference.Range.TOP, refEntry.getReference().getRange());
    // The Reference body (range + encoded split key) must round-trip faithfully.
    Reference roundTripped = Reference.convert(refEntry.getReference());
    assertEquals(0, Bytes.compareTo(original.getSplitKey(), roundTripped.getSplitKey()),
      "the encoded split key must round-trip through the recovered manifest");
  }

  @Test
  public void testPresentParentWithOnlyReferenceReportsNoDataLoss() throws Exception {
    // Parent directory exists but its only store file is a reference (not unarchived parent data).
    RegionInfo parent = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(58L)
      .setStartKey(Bytes.toBytes("")).setEndKey(Bytes.toBytes("")).build();
    HRegionFileSystem parentFs = createRegion(parent);
    byte[] splitRow = Bytes.toBytes("p");
    Reference.createBottomReference(splitRow).write(fs,
      new Path(parentFs.getStoreDir(FAMILY_NAME), "abcdef90.0123456789abcdef0123456789abcde1"));

    RegionInfo child = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(59L)
      .setStartKey(Bytes.toBytes("m")).setEndKey(Bytes.toBytes("")).build();
    HRegionFileSystem childFs = createRegion(child);

    StoreFileListRecover.RecoverReport report = StoreFileListRecover.recover(
      UTIL.getConfiguration(), tableDescriptor, familyDescriptor, childFs,
      Collections.singletonList(parent), false);

    assertEquals(1, report.getParentContributions().size());
    StoreFileListRecover.ParentContribution pc = report.getParentContributions().get(0);
    assertEquals(StoreFileListRecover.ParentContribution.Status.PRESENT_NO_FILES, pc.getStatus());
    assertEquals(0, pc.getUnarchivedHFileCount(),
      "a reference file does not count as unarchived parent data");
    assertFalse(report.allParentsArchived());
    assertFalse(report.hasUnarchivedParents(),
      "present-but-no-files parent must not raise a data-loss flag");
  }

  private HRegionFileSystem createRegion(RegionInfo regionInfo) throws IOException {
    HRegionFileSystem regionFs =
      HRegionFileSystem.create(UTIL.getConfiguration(), fs, tableDir, regionInfo);
    fs.mkdirs(regionFs.getStoreDir(FAMILY_NAME));
    return regionFs;
  }

  private Path writeCorruptTracker(HRegionFileSystem regionFs, String fileName) throws IOException {
    Path trackDir = new Path(regionFs.getStoreDir(FAMILY_NAME), StoreFileListFile.TRACK_FILE_DIR);
    fs.mkdirs(trackDir);
    Path file = new Path(trackDir, fileName);
    try (FSDataOutputStream out = fs.create(file, true)) {
      // Write an inconsistent length+payload+checksum so load() throws an IOException
      // (the checksum will not match), exercising the corruption diagnostic path.
      out.writeInt(8);
      out.writeLong(1L);
      out.writeInt(0xdeadbeef);
    }
    return file;
  }

  private static long parseSeqId(Path file) {
    String n = file.getName();
    int dot = n.indexOf('.');
    return dot < 0 ? 0L : Long.parseLong(n.substring(dot + 1));
  }
}
