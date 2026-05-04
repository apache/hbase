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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.hadoop.hbase.shaded.protobuf.generated.FSProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.StoreFileTrackerProtos.StoreFileEntry;
import org.apache.hadoop.hbase.shaded.protobuf.generated.StoreFileTrackerProtos.StoreFileList;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestStoreFileListRepair {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestStoreFileListRepair.class);

  private static final HBaseCommonTestingUtil UTIL = new HBaseCommonTestingUtil();
  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");
  private static final String FAMILY_NAME = Bytes.toString(FAMILY);
  private static final TableName TABLE_NAME = TableName.valueOf("ns:tbl");

  @Rule
  public TestName name = new TestName();

  private FileSystem fs;
  private Path rootDir;
  private Path tableDir;
  private TableDescriptor tableDescriptor;
  private ColumnFamilyDescriptor familyDescriptor;

  @Before
  public void setUp() throws IOException {
    fs = FileSystem.get(UTIL.getConfiguration());
    rootDir = UTIL.getDataTestDir(name.getMethodName());
    tableDir = CommonFSUtils.getTableDir(rootDir, TABLE_NAME);
    fs.mkdirs(tableDir);
    familyDescriptor = ColumnFamilyDescriptorBuilder.of(FAMILY);
    tableDescriptor =
      TableDescriptorBuilder.newBuilder(TABLE_NAME).setColumnFamily(familyDescriptor).build();
  }

  @AfterClass
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
    assertTrue("expected diagnostics to surface the corrupted file", report.hasCorruption());
    assertTrue("corrupted file should be reported by name",
      report.getDiagnostics().stream()
        .anyMatch(d -> d.isCorrupted() && d.getPath().getName().equals(corrupt.getName())));

    assertEquals(1, report.getDiskEntries().size());
    assertEquals(0, report.getLineageEntries().size());
    assertNotNull(report.getWrittenManifest());

    StoreFileList repaired = StoreFileListFile.load(fs, report.getWrittenManifest());
    assertEquals(1, repaired.getStoreFileCount());
    assertEquals("abcdef01", repaired.getStoreFile(0).getName());

    // The repaired manifest must have a strictly newer seqId than the corrupted file.
    long corruptSeqId = parseSeqId(corrupt);
    long repairedSeqId = parseSeqId(report.getWrittenManifest());
    assertTrue("repaired seqId " + repairedSeqId + " should be > corrupted " + corruptSeqId,
      repairedSeqId > corruptSeqId);
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
    assertTrue("expected a reference for abcdef10", names.contains(refName));
    assertTrue("expected an HFileLink for abcdef11", names.contains(linkName));

    StoreFileList repaired = StoreFileListFile.load(fs, report.getWrittenManifest());
    assertEquals(2, repaired.getStoreFileCount());

    StoreFileEntry refEntry = entryByName(repaired, refName);
    assertNotNull("reference entry must be present", refEntry);
    assertTrue("reference entry must carry a Reference body", refEntry.hasReference());
    FSProtos.Reference proto = refEntry.getReference();
    // Top daughter -> Reference is TOP. The encoded split key is a "first on row" cell whose
    // row component must equal the daughter's startKey ("m").
    assertEquals(FSProtos.Reference.Range.TOP, proto.getRange());
    Reference roundTripped = Reference.convert(proto);
    assertTrue("encoded split key should contain the daughter's start row",
      Bytes.toString(roundTripped.getSplitKey()).contains("m"));

    StoreFileEntry linkEntry = entryByName(repaired, linkName);
    assertNotNull("link entry must be present", linkEntry);
    assertFalse("link entry must NOT carry a Reference body", linkEntry.hasReference());

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
    assertTrue("encoded split key should contain the daughter's end row",
      Bytes.toString(roundTripped.getSplitKey()).contains("m"));
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
    assertTrue("union must contain the on-disk HFile", names.contains("abcdef41"));
    assertTrue("union must contain the lineage-derived link/reference",
      names.stream().anyMatch(n -> n.contains(parent.getEncodedName()) || HFileLink.isHFileLink(n)));
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
    assertTrue("test setup: parent dir should exist", fs.exists(parentRegionDir));
    assertTrue("delete parent dir to simulate archive", fs.delete(parentRegionDir, true));

    StoreFileListRepair.RepairReport report = StoreFileListRepair.repair(UTIL.getConfiguration(),
      tableDescriptor, familyDescriptor, childFs, StoreFileListRepair.Lineage.splitParent(parent),
      StoreFileListRepair.Mode.LINEAGE_ASSISTED, false);

    assertEquals("no lineage entries should be synthesized when parent is archived",
      Collections.emptyList(), report.getLineageEntries());
    assertEquals("manifest should be empty since child dir is empty too", 0,
      report.getManifestEntries().size());

    // Verify the parent contribution is reported as ARCHIVED.
    assertEquals(1, report.getParentContributions().size());
    StoreFileListRepair.ParentContribution pc = report.getParentContributions().get(0);
    assertEquals(parent.getEncodedName(), pc.getParent().getEncodedName());
    assertEquals(StoreFileListRepair.ParentContribution.Status.ARCHIVED, pc.getStatus());
    assertEquals(0, pc.getFilesContributed());
    assertTrue("allParentsArchived should be true when parent is archived",
      report.allParentsArchived());
    assertFalse("hasUnarchivedParents should be false when parent is archived",
      report.hasUnarchivedParents());
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
    assertTrue("expected lineage entries from unarchived parent",
      report.getLineageEntries().size() > 0);

    // Parent contribution should be PRESENT_WITH_FILES.
    assertEquals(1, report.getParentContributions().size());
    StoreFileListRepair.ParentContribution pc = report.getParentContributions().get(0);
    assertEquals(parent.getEncodedName(), pc.getParent().getEncodedName());
    assertEquals(StoreFileListRepair.ParentContribution.Status.PRESENT_WITH_FILES, pc.getStatus());
    assertTrue("files contributed should be > 0", pc.getFilesContributed() > 0);
    assertFalse("allParentsArchived should be false when parent has files",
      report.allParentsArchived());
    assertTrue("hasUnarchivedParents should be true when parent has files",
      report.hasUnarchivedParents());
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
    assertTrue("delete parent A to simulate archive", fs.delete(parentADir, true));

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
    assertNotNull("parent A contribution must be present", pcA);
    assertNotNull("parent B contribution must be present", pcB);
    assertEquals(StoreFileListRepair.ParentContribution.Status.ARCHIVED, pcA.getStatus());
    assertEquals(StoreFileListRepair.ParentContribution.Status.PRESENT_WITH_FILES, pcB.getStatus());
    assertFalse("allParentsArchived should be false (mixed status)",
      report.allParentsArchived());
    assertTrue("hasUnarchivedParents should be true (parent B has files)",
      report.hasUnarchivedParents());
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

    assertNull("dry-run must not write a new manifest", report.getWrittenManifest());
    assertTrue("corrupted tracker file must remain after dry-run", fs.exists(corrupt));
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
    assertTrue("second repair should be a no-op", second.isNoOp());
    assertNull("no new manifest should have been written", second.getWrittenManifest());
  }

  @Test
  public void testDecideSplitDaughterIsTopThrowsWhenNotADaughter() {
    RegionInfo parent = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(10L)
      .setStartKey(Bytes.toBytes("a")).setEndKey(Bytes.toBytes("z")).build();
    RegionInfo unrelated = RegionInfoBuilder.newBuilder(TABLE_NAME).setRegionId(11L)
      .setStartKey(Bytes.toBytes("p")).setEndKey(Bytes.toBytes("q")).build();
    try {
      StoreFileListRepair.decideSplitDaughterIsTop(parent, unrelated);
      fail("expected IOException for non-daughter");
    } catch (IOException e) {
      // expected
    }
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
