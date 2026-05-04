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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
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
import org.apache.hadoop.hbase.io.HFileLink;
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
public class TestStoreFileListRepair {

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

    StoreFileListRepair.RepairReport report = StoreFileListRepair.repair(UTIL.getConfiguration(),
      tableDescriptor, familyDescriptor, regionFs, StoreFileListRepair.Lineage.none(),
      StoreFileListRepair.Mode.DISK_ONLY, false);

    // Diagnostics must mention the corrupted file.
    assertTrue(report.hasCorruption(), "expected diagnostics to surface the corrupted file");
    assertTrue(
      report.getDiagnostics().stream()
        .anyMatch(d -> d.isCorrupted() && d.getPath().getName().equals(corrupt.getName())),
      "corrupted file should be reported by name");

    assertEquals(1, report.getDiskEntries().size());
    assertEquals(0, report.getLineageEntries().size());
    assertNotNull(report.getWrittenManifest());

    StoreFileList repaired = StoreFileListFile.load(fs, report.getWrittenManifest());
    assertEquals(1, repaired.getStoreFileCount());
    assertEquals("abcdef01", repaired.getStoreFile(0).getName());

    // The repaired manifest must have a strictly newer seqId than the corrupted file.
    long corruptSeqId = parseSeqId(corrupt);
    long repairedSeqId = parseSeqId(report.getWrittenManifest());
    assertTrue(repairedSeqId > corruptSeqId,
      "repaired seqId " + repairedSeqId + " should be > corrupted " + corruptSeqId);
  }

  @Test
  public void testLineageAssistedWithoutLineageFallsBackToDiskOnly() throws Exception {
    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(2L).build();
    HRegionFileSystem regionFs = createRegion(regionInfo);
    Path hfile = new Path(regionFs.getStoreDir(FAMILY_NAME), "abcdef02");
    HFileTestUtil.createHFile(UTIL.getConfiguration(), fs, hfile, FAMILY, QUALIFIER,
      Bytes.toBytes("a"), Bytes.toBytes("z"), 10);

    StoreFileListRepair.RepairReport report = StoreFileListRepair.repair(UTIL.getConfiguration(),
      tableDescriptor, familyDescriptor, regionFs, StoreFileListRepair.Lineage.none(),
      StoreFileListRepair.Mode.LINEAGE_ASSISTED, false);

    assertEquals(1, report.getDiskEntries().size());
    assertEquals(0, report.getLineageEntries().size());
    StoreFileList repaired = StoreFileListFile.load(fs, report.getWrittenManifest());
    assertEquals(1, repaired.getStoreFileCount());
    assertEquals("abcdef02", repaired.getStoreFile(0).getName());
  }

  @Test
  public void testLineageAssistedSplitRepairAddsReferencesAndLinks() throws Exception {
    RegionInfo parent = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(3L)
      .setStartKey(Bytes.toBytes("")).setEndKey(Bytes.toBytes("")).build();
    HRegionFileSystem parentFs = createRegion(parent);
    Path parentFamilyDir = parentFs.getStoreDir(FAMILY_NAME);
    HFileTestUtil.createHFile(UTIL.getConfiguration(), fs, new Path(parentFamilyDir, "abcdef10"),
      FAMILY, QUALIFIER, Bytes.toBytes("a"), Bytes.toBytes("z"), 10);
    HFileTestUtil.createHFile(UTIL.getConfiguration(), fs, new Path(parentFamilyDir, "abcdef11"),
      FAMILY, QUALIFIER, Bytes.toBytes("n"), Bytes.toBytes("z"), 10);

    RegionInfo topChild = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(4L)
      .setStartKey(Bytes.toBytes("m")).setEndKey(Bytes.toBytes("")).build();
    HRegionFileSystem childFs = createRegion(topChild);

    StoreFileListRepair.RepairReport report = StoreFileListRepair.repair(UTIL.getConfiguration(),
      tableDescriptor, familyDescriptor, childFs, StoreFileListRepair.Lineage.splitParent(parent),
      StoreFileListRepair.Mode.LINEAGE_ASSISTED, false);

    assertEquals(0, report.getDiskEntries().size());
    assertEquals(2, report.getLineageEntries().size());

    List<String> names = report.getManifestEntries().stream().map(info -> info.getPath().getName())
      .collect(Collectors.toList());
    String linkName = HFileLink.createHFileLinkName(TABLE_NAME, parent.getEncodedName(), "abcdef11");
    String refName = "abcdef10." + parent.getEncodedName();
    assertTrue(names.contains(refName), "expected a reference for abcdef10");
    assertTrue(names.contains(linkName), "expected an HFileLink for abcdef11");

    StoreFileList repaired = StoreFileListFile.load(fs, report.getWrittenManifest());
    assertEquals(2, repaired.getStoreFileCount());

    StoreFileEntry refEntry = entryByName(repaired, refName);
    assertNotNull(refEntry, "reference entry must be present");
    assertTrue(refEntry.hasReference(), "reference entry must carry a Reference body");
    FSProtos.Reference proto = refEntry.getReference();
    // Top daughter -> Reference is TOP. The encoded split key is a "first on row" cell whose
    // row component must equal the daughter's startKey ("m").
    assertEquals(FSProtos.Reference.Range.TOP, proto.getRange());
    Reference roundTripped = Reference.convert(proto);
    assertTrue(Bytes.toString(roundTripped.getSplitKey()).contains("m"),
      "encoded split key should contain the daughter's start row");

    StoreFileEntry linkEntry = entryByName(repaired, linkName);
    assertNotNull(linkEntry, "link entry must be present");
    assertFalse(linkEntry.hasReference(), "link entry must NOT carry a Reference body");

    // Verify parent contribution is tracked as PRESENT_WITH_FILES.
    assertEquals(1, report.getParentContributions().size());
    StoreFileListRepair.ParentContribution pc = report.getParentContributions().get(0);
    assertEquals(StoreFileListRepair.ParentContribution.Status.PRESENT_WITH_FILES, pc.getStatus());
    assertEquals(2, pc.getFilesContributed());
  }

  @Test
  public void testLineageAssistedSplitBottomDaughterReferenceIsBottom() throws Exception {
    RegionInfo parent = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(31L)
      .setStartKey(Bytes.toBytes("")).setEndKey(Bytes.toBytes("")).build();
    HRegionFileSystem parentFs = createRegion(parent);
    HFileTestUtil.createHFile(UTIL.getConfiguration(), fs,
      new Path(parentFs.getStoreDir(FAMILY_NAME), "abcdef12"), FAMILY, QUALIFIER,
      Bytes.toBytes("a"), Bytes.toBytes("z"), 10);

    RegionInfo bottomChild = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(32L)
      .setStartKey(Bytes.toBytes("")).setEndKey(Bytes.toBytes("m")).build();
    HRegionFileSystem childFs = createRegion(bottomChild);

    StoreFileListRepair.RepairReport report = StoreFileListRepair.repair(UTIL.getConfiguration(),
      tableDescriptor, familyDescriptor, childFs, StoreFileListRepair.Lineage.splitParent(parent),
      StoreFileListRepair.Mode.LINEAGE_ASSISTED, false);

    assertEquals(1, report.getLineageEntries().size());
    StoreFileList repaired = StoreFileListFile.load(fs, report.getWrittenManifest());
    StoreFileEntry refEntry = entryByName(repaired, "abcdef12." + parent.getEncodedName());
    assertNotNull(refEntry);
    assertTrue(refEntry.hasReference());
    assertEquals(FSProtos.Reference.Range.BOTTOM, refEntry.getReference().getRange());
    Reference roundTripped = Reference.convert(refEntry.getReference());
    assertTrue(Bytes.toString(roundTripped.getSplitKey()).contains("m"),
      "encoded split key should contain the daughter's end row");
  }

  @Test
  public void testLineageAssistedUnionPreservesOnDiskFiles() throws Exception {
    RegionInfo parent = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(41L)
      .setStartKey(Bytes.toBytes("")).setEndKey(Bytes.toBytes("")).build();
    HRegionFileSystem parentFs = createRegion(parent);
    HFileTestUtil.createHFile(UTIL.getConfiguration(), fs,
      new Path(parentFs.getStoreDir(FAMILY_NAME), "abcdef40"), FAMILY, QUALIFIER,
      Bytes.toBytes("a"), Bytes.toBytes("z"), 10);

    RegionInfo topChild = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(42L)
      .setStartKey(Bytes.toBytes("m")).setEndKey(Bytes.toBytes("")).build();
    HRegionFileSystem childFs = createRegion(topChild);
    // an existing on-disk HFile already in the child family directory
    HFileTestUtil.createHFile(UTIL.getConfiguration(), fs,
      new Path(childFs.getStoreDir(FAMILY_NAME), "abcdef41"), FAMILY, QUALIFIER,
      Bytes.toBytes("m"), Bytes.toBytes("z"), 10);

    StoreFileListRepair.RepairReport report = StoreFileListRepair.repair(UTIL.getConfiguration(),
      tableDescriptor, familyDescriptor, childFs, StoreFileListRepair.Lineage.splitParent(parent),
      StoreFileListRepair.Mode.LINEAGE_ASSISTED, false);

    List<String> names = report.getManifestEntries().stream().map(info -> info.getPath().getName())
      .collect(Collectors.toList());
    assertTrue(names.contains("abcdef41"), "union must contain the on-disk HFile");
    assertTrue(
      names.stream().anyMatch(n -> n.contains(parent.getEncodedName()) || HFileLink.isHFileLink(n)),
      "union must contain the lineage-derived link/reference");
    assertEquals(2, report.getManifestEntries().size());
  }

  @Test
  public void testLineageAssistedMergeRepairAddsReferences() throws Exception {
    RegionInfo mergeParentA = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(5L)
      .setStartKey(Bytes.toBytes("")).setEndKey(Bytes.toBytes("m")).build();
    RegionInfo mergeParentB = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(6L)
      .setStartKey(Bytes.toBytes("m")).setEndKey(Bytes.toBytes("")).build();
    HRegionFileSystem parentAFs = createRegion(mergeParentA);
    HRegionFileSystem parentBFs = createRegion(mergeParentB);
    HFileTestUtil.createHFile(UTIL.getConfiguration(), fs,
      new Path(parentAFs.getStoreDir(FAMILY_NAME), "abcdef20"), FAMILY, QUALIFIER, Bytes.toBytes("a"),
      Bytes.toBytes("l"), 10);
    HFileTestUtil.createHFile(UTIL.getConfiguration(), fs,
      new Path(parentBFs.getStoreDir(FAMILY_NAME), "abcdef21"), FAMILY, QUALIFIER, Bytes.toBytes("m"),
      Bytes.toBytes("z"), 10);

    RegionInfo mergedChild = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(7L)
      .setStartKey(Bytes.toBytes("")).setEndKey(Bytes.toBytes("")).build();
    HRegionFileSystem childFs = createRegion(mergedChild);

    StoreFileListRepair.RepairReport report = StoreFileListRepair.repair(UTIL.getConfiguration(),
      tableDescriptor, familyDescriptor, childFs,
      StoreFileListRepair.Lineage.mergeParents(Arrays.asList(mergeParentA, mergeParentB)),
      StoreFileListRepair.Mode.LINEAGE_ASSISTED, false);

    assertEquals(2, report.getLineageEntries().size());
    List<String> names = report.getManifestEntries().stream().map(info -> info.getPath().getName())
      .collect(Collectors.toList());
    assertTrue(names.contains("abcdef20." + mergeParentA.getEncodedName()));
    assertTrue(names.contains("abcdef21." + mergeParentB.getEncodedName()));
    StoreFileList repaired = StoreFileListFile.load(fs, report.getWrittenManifest());
    assertTrue(repaired.getStoreFileList().stream().allMatch(StoreFileEntry::hasReference));

    // Both merge parents should be tracked as PRESENT_WITH_FILES.
    assertEquals(2, report.getParentContributions().size());
    for (StoreFileListRepair.ParentContribution pc : report.getParentContributions()) {
      assertEquals(StoreFileListRepair.ParentContribution.Status.PRESENT_WITH_FILES, pc.getStatus());
      assertEquals(1, pc.getFilesContributed());
    }
  }

  @Test
  public void testLineageAssistedSplitWithArchivedParentProducesNoLineageEntries() throws Exception {
    RegionInfo parent = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(51L)
      .setStartKey(Bytes.toBytes("")).setEndKey(Bytes.toBytes("")).build();
    HRegionFileSystem parentFs = createRegion(parent);
    HFileTestUtil.createHFile(UTIL.getConfiguration(), fs,
      new Path(parentFs.getStoreDir(FAMILY_NAME), "abcdef50"), FAMILY, QUALIFIER, Bytes.toBytes("a"),
      Bytes.toBytes("z"), 10);

    RegionInfo topChild = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(52L)
      .setStartKey(Bytes.toBytes("m")).setEndKey(Bytes.toBytes("")).build();
    HRegionFileSystem childFs = createRegion(topChild);

    // Simulate Catalog Janitor having archived (deleted) the parent's region directory.
    Path parentRegionDir = new Path(tableDir, parent.getEncodedName());
    assertTrue(fs.exists(parentRegionDir), "test setup: parent dir should exist");
    assertTrue(fs.delete(parentRegionDir, true), "delete parent dir to simulate archive");

    StoreFileListRepair.RepairReport report = StoreFileListRepair.repair(UTIL.getConfiguration(),
      tableDescriptor, familyDescriptor, childFs, StoreFileListRepair.Lineage.splitParent(parent),
      StoreFileListRepair.Mode.LINEAGE_ASSISTED, false);

    assertEquals(Collections.emptyList(), report.getLineageEntries(),
      "no lineage entries should be synthesized when parent is archived");
    assertEquals(0, report.getManifestEntries().size(),
      "manifest should be empty since child dir is empty too");

    // Verify the parent contribution is reported as ARCHIVED.
    assertEquals(1, report.getParentContributions().size());
    StoreFileListRepair.ParentContribution pc = report.getParentContributions().get(0);
    assertEquals(parent.getEncodedName(), pc.getParent().getEncodedName());
    assertEquals(StoreFileListRepair.ParentContribution.Status.ARCHIVED, pc.getStatus());
    assertEquals(0, pc.getFilesContributed());
    assertTrue(report.allParentsArchived(),
      "allParentsArchived should be true when parent is archived");
    assertFalse(report.hasUnarchivedParents(),
      "hasUnarchivedParents should be false when parent is archived");
  }

  @Test
  public void testUnarchivedParentReportsPresentWithFiles() throws Exception {
    // Split parent is still present on disk -> report should flag PRESENT_WITH_FILES
    // and hasUnarchivedParents() should return true.
    RegionInfo parent = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(53L)
      .setStartKey(Bytes.toBytes("")).setEndKey(Bytes.toBytes("")).build();
    HRegionFileSystem parentFs = createRegion(parent);
    HFileTestUtil.createHFile(UTIL.getConfiguration(), fs,
      new Path(parentFs.getStoreDir(FAMILY_NAME), "abcdef55"), FAMILY, QUALIFIER,
      Bytes.toBytes("a"), Bytes.toBytes("z"), 10);

    RegionInfo topChild = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(54L)
      .setStartKey(Bytes.toBytes("m")).setEndKey(Bytes.toBytes("")).build();
    HRegionFileSystem childFs = createRegion(topChild);

    StoreFileListRepair.RepairReport report = StoreFileListRepair.repair(UTIL.getConfiguration(),
      tableDescriptor, familyDescriptor, childFs, StoreFileListRepair.Lineage.splitParent(parent),
      StoreFileListRepair.Mode.LINEAGE_ASSISTED, false);

    // Lineage entries should have been synthesized from the unarchived parent.
    assertTrue(report.getLineageEntries().size() > 0,
      "expected lineage entries from unarchived parent");

    // Parent contribution should be PRESENT_WITH_FILES.
    assertEquals(1, report.getParentContributions().size());
    StoreFileListRepair.ParentContribution pc = report.getParentContributions().get(0);
    assertEquals(parent.getEncodedName(), pc.getParent().getEncodedName());
    assertEquals(StoreFileListRepair.ParentContribution.Status.PRESENT_WITH_FILES, pc.getStatus());
    assertTrue(pc.getFilesContributed() > 0, "files contributed should be > 0");
    assertFalse(report.allParentsArchived(),
      "allParentsArchived should be false when parent has files");
    assertTrue(report.hasUnarchivedParents(),
      "hasUnarchivedParents should be true when parent has files");
  }

  @Test
  public void testMergeWithMixedArchiveStatus() throws Exception {
    // Two merge parents: one archived, one still present.
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

    StoreFileListRepair.RepairReport report = StoreFileListRepair.repair(UTIL.getConfiguration(),
      tableDescriptor, familyDescriptor, childFs,
      StoreFileListRepair.Lineage.mergeParents(Arrays.asList(mergeParentA, mergeParentB)),
      StoreFileListRepair.Mode.LINEAGE_ASSISTED, false);

    // Only parent B should contribute entries.
    assertEquals(1, report.getLineageEntries().size());

    // Two parent contributions: one ARCHIVED, one PRESENT_WITH_FILES.
    assertEquals(2, report.getParentContributions().size());
    StoreFileListRepair.ParentContribution pcA = report.getParentContributions().stream()
      .filter(pc -> pc.getParent().getEncodedName().equals(mergeParentA.getEncodedName()))
      .findFirst().orElse(null);
    StoreFileListRepair.ParentContribution pcB = report.getParentContributions().stream()
      .filter(pc -> pc.getParent().getEncodedName().equals(mergeParentB.getEncodedName()))
      .findFirst().orElse(null);
    assertNotNull(pcA, "parent A contribution must be present");
    assertNotNull(pcB, "parent B contribution must be present");
    assertEquals(StoreFileListRepair.ParentContribution.Status.ARCHIVED, pcA.getStatus());
    assertEquals(StoreFileListRepair.ParentContribution.Status.PRESENT_WITH_FILES, pcB.getStatus());
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

    StoreFileListRepair.RepairReport report = StoreFileListRepair.repair(UTIL.getConfiguration(),
      tableDescriptor, familyDescriptor, regionFs, StoreFileListRepair.Lineage.none(),
      StoreFileListRepair.Mode.DISK_ONLY, true);

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
    // First, write a healthy manifest by running repair against a non-corrupted store.
    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(9L).build();
    HRegionFileSystem regionFs = createRegion(regionInfo);
    Path hfile = new Path(regionFs.getStoreDir(FAMILY_NAME), "abcdef60");
    HFileTestUtil.createHFile(UTIL.getConfiguration(), fs, hfile, FAMILY, QUALIFIER,
      Bytes.toBytes("a"), Bytes.toBytes("z"), 10);

    StoreFileListRepair.RepairReport first = StoreFileListRepair.repair(UTIL.getConfiguration(),
      tableDescriptor, familyDescriptor, regionFs, StoreFileListRepair.Lineage.none(),
      StoreFileListRepair.Mode.DISK_ONLY, false);
    assertNotNull(first.getWrittenManifest());
    assertFalse(first.isNoOp());

    // Run again. There is no corruption and the manifest matches disk; should be a no-op.
    StoreFileListRepair.RepairReport second = StoreFileListRepair.repair(UTIL.getConfiguration(),
      tableDescriptor, familyDescriptor, regionFs, StoreFileListRepair.Lineage.none(),
      StoreFileListRepair.Mode.DISK_ONLY, false);
    assertTrue(second.isNoOp(), "second repair should be a no-op");
    assertNull(second.getWrittenManifest(), "no new manifest should have been written");
  }

  @Test
  public void testDecideSplitDaughterIsTopThrowsWhenNotADaughter() {
    RegionInfo parent = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(10L)
      .setStartKey(Bytes.toBytes("a")).setEndKey(Bytes.toBytes("z")).build();
    RegionInfo unrelated = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(11L)
      .setStartKey(Bytes.toBytes("p")).setEndKey(Bytes.toBytes("q")).build();
    assertThrows(IOException.class,
      () -> StoreFileListRepair.decideSplitDaughterIsTop(parent, unrelated),
      "expected IOException for non-daughter");
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

  private static StoreFileEntry entryByName(StoreFileList list, String name) {
    return list.getStoreFileList().stream().filter(e -> e.getName().equals(name)).findFirst()
      .orElse(null);
  }

  private static long parseSeqId(Path file) {
    String n = file.getName();
    int dot = n.indexOf('.');
    return dot < 0 ? 0L : Long.parseLong(n.substring(dot + 1));
  }
}
