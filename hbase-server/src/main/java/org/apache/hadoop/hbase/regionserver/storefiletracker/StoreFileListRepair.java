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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreContext;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.regionserver.StoreUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.FSProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.StoreFileTrackerProtos.StoreFileEntry;
import org.apache.hadoop.hbase.shaded.protobuf.generated.StoreFileTrackerProtos.StoreFileList;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

/**
 * Offline helper that rebuilds the FILE store-file-tracker manifest for a single store
 * (table + region + family) when the latest manifest cannot be loaded.
 * <p>
 * See {@code dev-support/design-docs/fsft-manifest-repair.md} for the full design.
 * <p>
 * The repair never modifies the corrupted manifest in place and never deletes older generations
 * itself; it writes a brand new strictly-newer generation under {@code .filelist} via
 * {@link StoreFileListFile#writeNew(StoreFileList.Builder)}, leaving {@code load(false)} to prune
 * older files on the next region open.
 */
@InterfaceAudience.Private
public final class StoreFileListRepair {

  private static final Logger LOG = LoggerFactory.getLogger(StoreFileListRepair.class);

  public enum Mode {
    DISK_ONLY,
    LINEAGE_ASSISTED;

    static Mode valueOfOption(String value) {
      if ("disk-only".equalsIgnoreCase(value)) {
        return DISK_ONLY;
      }
      if ("lineage-assisted".equalsIgnoreCase(value)) {
        return LINEAGE_ASSISTED;
      }
      throw new IllegalArgumentException("Unknown repair mode: " + value
        + ". Expected disk-only or lineage-assisted.");
    }
  }

  public static final class Lineage {
    private final RegionInfo splitParent;
    private final List<RegionInfo> mergeParents;

    private Lineage(RegionInfo splitParent, List<RegionInfo> mergeParents) {
      this.splitParent = splitParent;
      this.mergeParents = mergeParents;
    }

    public static Lineage none() {
      return new Lineage(null, Collections.emptyList());
    }

    public static Lineage splitParent(RegionInfo parent) {
      return new Lineage(parent, Collections.emptyList());
    }

    public static Lineage mergeParents(List<RegionInfo> parents) {
      return new Lineage(null, Collections.unmodifiableList(new ArrayList<>(parents)));
    }

    Optional<RegionInfo> getSplitParent() {
      return Optional.ofNullable(splitParent);
    }

    List<RegionInfo> getMergeParents() {
      return mergeParents;
    }

    boolean isEmpty() {
      return splitParent == null && mergeParents.isEmpty();
    }
  }

  /**
   * Tracks the archive status and contribution of a single parent region during
   * lineage-assisted repair. This allows the report to distinguish between parents that
   * have been fully archived by Catalog Janitor (no data loss) and parents that still have
   * unarchived HFiles (potential data discrepancy requiring admin review).
   */
  public static final class ParentContribution {
    public enum Status {
      /** Parent region directory was not found; Catalog Janitor has archived it. */
      ARCHIVED,
      /** Parent region directory exists and contributed store file entries. */
      PRESENT_WITH_FILES,
      /** Parent region directory exists but no store file entries were derived. */
      PRESENT_NO_FILES
    }

    private final RegionInfo parent;
    private final Status status;
    private final int filesContributed;

    ParentContribution(RegionInfo parent, Status status, int filesContributed) {
      this.parent = parent;
      this.status = status;
      this.filesContributed = filesContributed;
    }

    public RegionInfo getParent() {
      return parent;
    }

    public Status getStatus() {
      return status;
    }

    public int getFilesContributed() {
      return filesContributed;
    }
  }

  public static final class TrackerFileDiagnostic {
    private final Path path;
    private final Integer storeFileCount;
    private final String error;

    TrackerFileDiagnostic(Path path, Integer storeFileCount, String error) {
      this.path = path;
      this.storeFileCount = storeFileCount;
      this.error = error;
    }

    public Path getPath() {
      return path;
    }

    public Integer getStoreFileCount() {
      return storeFileCount;
    }

    public String getError() {
      return error;
    }

    public boolean isCorrupted() {
      return error != null;
    }
  }

  /**
   * Internal bundle returned by the lineage loading methods. Carries both the derived
   * store-file entries and the per-parent contribution records for the report.
   */
  private static final class LineageResult {
    static final LineageResult EMPTY =
      new LineageResult(Collections.emptyList(), Collections.emptyList());

    private final List<StoreFileInfo> entries;
    private final List<ParentContribution> parentContributions;

    LineageResult(List<StoreFileInfo> entries, List<ParentContribution> parentContributions) {
      this.entries = entries;
      this.parentContributions = parentContributions;
    }
  }

  public static final class RepairReport {
    private final List<TrackerFileDiagnostic> diagnostics;
    private final List<StoreFileInfo> diskEntries;
    private final List<StoreFileInfo> lineageEntries;
    private final List<StoreFileInfo> manifestEntries;
    private final List<ParentContribution> parentContributions;
    private final Path writtenManifest;
    private final boolean noOp;

    RepairReport(List<TrackerFileDiagnostic> diagnostics, List<StoreFileInfo> diskEntries,
      List<StoreFileInfo> lineageEntries, List<StoreFileInfo> manifestEntries,
      List<ParentContribution> parentContributions, Path writtenManifest, boolean noOp) {
      this.diagnostics = Collections.unmodifiableList(new ArrayList<>(diagnostics));
      this.diskEntries = Collections.unmodifiableList(new ArrayList<>(diskEntries));
      this.lineageEntries = Collections.unmodifiableList(new ArrayList<>(lineageEntries));
      this.manifestEntries = Collections.unmodifiableList(new ArrayList<>(manifestEntries));
      this.parentContributions =
        Collections.unmodifiableList(new ArrayList<>(parentContributions));
      this.writtenManifest = writtenManifest;
      this.noOp = noOp;
    }

    public List<TrackerFileDiagnostic> getDiagnostics() {
      return diagnostics;
    }

    public List<StoreFileInfo> getDiskEntries() {
      return diskEntries;
    }

    public List<StoreFileInfo> getLineageEntries() {
      return lineageEntries;
    }

    public List<StoreFileInfo> getManifestEntries() {
      return manifestEntries;
    }

    public List<ParentContribution> getParentContributions() {
      return parentContributions;
    }

    public Path getWrittenManifest() {
      return writtenManifest;
    }

    public boolean isNoOp() {
      return noOp;
    }

    public boolean hasCorruption() {
      for (TrackerFileDiagnostic d : diagnostics) {
        if (d.isCorrupted()) {
          return true;
        }
      }
      return false;
    }

    /** Returns true when all parents that had lineage were already archived. */
    public boolean allParentsArchived() {
      if (parentContributions.isEmpty()) {
        return false;
      }
      for (ParentContribution pc : parentContributions) {
        if (pc.getStatus() != ParentContribution.Status.ARCHIVED) {
          return false;
        }
      }
      return true;
    }

    /** Returns true when at least one parent has unarchived HFiles on disk. */
    public boolean hasUnarchivedParents() {
      for (ParentContribution pc : parentContributions) {
        if (pc.getStatus() == ParentContribution.Status.PRESENT_WITH_FILES) {
          return true;
        }
      }
      return false;
    }
  }

  private StoreFileListRepair() {
  }

  public static RepairReport repair(Configuration conf, TableDescriptor tableDescriptor,
    ColumnFamilyDescriptor familyDescriptor, HRegionFileSystem regionFs, Lineage lineage, Mode mode,
    boolean dryRun) throws IOException {
    StoreContext storeContext = StoreContext.getBuilder()
      .withColumnFamilyDescriptor(familyDescriptor)
      .withFamilyStoreDirectoryPath(regionFs.getStoreDir(familyDescriptor.getNameAsString()))
      .withRegionFileSystem(regionFs).build();
    StoreFileListFile storeFileListFile = new StoreFileListFile(storeContext);

    List<TrackerFileDiagnostic> diagnostics =
      diagnoseTrackerFiles(storeFileListFile, regionFs, familyDescriptor);

    List<StoreFileInfo> diskEntries =
      loadStoreFilesFromDisk(conf, tableDescriptor, familyDescriptor, regionFs);

    LineageResult lineageResult = LineageResult.EMPTY;
    if (mode == Mode.LINEAGE_ASSISTED && !lineage.isEmpty()) {
      lineageResult =
        loadStoreFilesFromLineage(conf, tableDescriptor, familyDescriptor, regionFs, lineage);
    }
    List<StoreFileInfo> lineageEntries = lineageResult.entries;

    List<StoreFileInfo> manifestEntries = unionStoreFileEntries(diskEntries, lineageEntries);

    // No-op detection: if there is a healthy latest tracker file whose contents already match
    // the recomputed set by name, do not churn the seqId.
    boolean noOp = isAlreadyHealthy(diagnostics, manifestEntries, storeFileListFile);

    Path writtenManifest = null;
    if (!dryRun && !noOp) {
      writtenManifest =
        storeFileListFile.writeNew(toStoreFileListBuilder(manifestEntries));
      LOG.info("Wrote repaired FSFT manifest at {} with {} entries", writtenManifest,
        manifestEntries.size());
    }
    return new RepairReport(diagnostics, diskEntries, lineageEntries, manifestEntries,
      lineageResult.parentContributions, writtenManifest, noOp);
  }

  /**
   * Returns true when a tracker file already loaded cleanly and exposes the same store-file name
   * set as the recomputed one. This is best-effort and only avoids unnecessary seqId churn; it
   * does not relax any safety check.
   */
  private static boolean isAlreadyHealthy(List<TrackerFileDiagnostic> diagnostics,
    List<StoreFileInfo> manifestEntries, StoreFileListFile storeFileListFile) {
    if (diagnostics.isEmpty()) {
      // No tracker files at all -> not "already healthy"; we still need to write one if
      // there is at least one entry to record. If there are no entries either, treat as no-op.
      return manifestEntries.isEmpty();
    }
    TrackerFileDiagnostic newest = null;
    for (TrackerFileDiagnostic d : diagnostics) {
      if (d.isCorrupted()) {
        continue;
      }
      if (newest == null || d.getPath().getName().compareTo(newest.getPath().getName()) > 0) {
        newest = d;
      }
    }
    if (newest == null) {
      return false;
    }
    try {
      StoreFileList list = storeFileListFile.load(newest.getPath());
      if (list.getStoreFileCount() != manifestEntries.size()) {
        return false;
      }
      java.util.Set<String> expected = new java.util.HashSet<>();
      for (StoreFileInfo info : manifestEntries) {
        expected.add(info.getPath().getName());
      }
      for (StoreFileEntry entry : list.getStoreFileList()) {
        if (!expected.contains(entry.getName())) {
          return false;
        }
      }
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  private static List<TrackerFileDiagnostic> diagnoseTrackerFiles(
    StoreFileListFile storeFileListFile, HRegionFileSystem regionFs,
    ColumnFamilyDescriptor familyDescriptor) throws IOException {
    FileSystem fs = regionFs.getFileSystem();
    Path trackFileDir = new Path(regionFs.getStoreDir(familyDescriptor.getNameAsString()),
      StoreFileListFile.TRACK_FILE_DIR);
    FileStatus[] statuses;
    try {
      statuses = fs.listStatus(trackFileDir);
    } catch (FileNotFoundException e) {
      return Collections.emptyList();
    }
    if (statuses == null || statuses.length == 0) {
      return Collections.emptyList();
    }
    List<TrackerFileDiagnostic> diagnostics = new ArrayList<>();
    for (FileStatus status : statuses) {
      Path path = status.getPath();
      if (
        !status.isFile() || !StoreFileListFile.TRACK_FILE_PATTERN.matcher(path.getName()).matches()
      ) {
        continue;
      }
      try {
        StoreFileList storeFileList = storeFileListFile.load(path);
        diagnostics.add(new TrackerFileDiagnostic(path, storeFileList.getStoreFileCount(), null));
      } catch (IOException e) {
        diagnostics.add(new TrackerFileDiagnostic(path, null, e.getMessage()));
      }
    }
    return diagnostics;
  }

  private static List<StoreFileInfo> loadStoreFilesFromDisk(Configuration conf,
    TableDescriptor tableDescriptor, ColumnFamilyDescriptor familyDescriptor,
    HRegionFileSystem regionFs) throws IOException {
    Configuration storeConf =
      StoreUtils.createStoreConfiguration(conf, tableDescriptor, familyDescriptor);
    StoreContext ctx = StoreContext.getBuilder().withColumnFamilyDescriptor(familyDescriptor)
      .withFamilyStoreDirectoryPath(regionFs.getStoreDir(familyDescriptor.getNameAsString()))
      .withRegionFileSystem(regionFs).build();
    DefaultStoreFileTracker tracker = new DefaultStoreFileTracker(storeConf,
      regionFs.getRegionInfo().getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID, ctx);
    List<StoreFileInfo> files = tracker.getStoreFiles(familyDescriptor.getNameAsString());
    return files != null ? files : Collections.emptyList();
  }

  /**
   * Holds the result of loading parent HFiles, distinguishing between an archived (not found)
   * parent directory and a present one.
   */
  private static final class ParentLoadResult {
    final List<StoreFileInfo> hfiles;
    final boolean archived;

    ParentLoadResult(List<StoreFileInfo> hfiles, boolean archived) {
      this.hfiles = hfiles;
      this.archived = archived;
    }
  }

  /**
   * Returns parent store files restricted to <em>real on-disk HFiles only</em>. Reference files,
   * link files, MOB link files etc. that may be lingering inside the parent dir (e.g. from an
   * interrupted split that left artifacts behind) must NOT be used as inputs to split/merge
   * simulation, otherwise we would synthesize references-of-references.
   * <p>
   * The returned {@link ParentLoadResult#archived} flag indicates whether the parent region
   * directory was not found (i.e. Catalog Janitor archived it).
   */
  private static ParentLoadResult loadParentHFilesOnly(Configuration conf,
    TableDescriptor tableDescriptor, ColumnFamilyDescriptor familyDescriptor,
    HRegionFileSystem childRegionFs, RegionInfo parentRegion) throws IOException {
    // Explicitly check whether the parent region directory exists. openRegionFromFileSystem
    // with readOnly=true may silently succeed even for a missing directory, deferring the
    // failure to a later listStatus call that surfaces as an empty result rather than FNF.
    FileSystem fs = childRegionFs.getFileSystem();
    Path parentRegionDir = new Path(childRegionFs.getTableDir(), parentRegion.getEncodedName());
    if (!fs.exists(parentRegionDir)) {
      LOG.info("Parent region directory not found for {}; treating as archived/missing.",
        parentRegion.getEncodedName());
      return new ParentLoadResult(Collections.emptyList(), true);
    }
    HRegionFileSystem parentRegionFs;
    try {
      parentRegionFs = HRegionFileSystem.openRegionFromFileSystem(conf,
        fs, childRegionFs.getTableDir(), parentRegion, true);
    } catch (FileNotFoundException e) {
      LOG.info("Parent region directory not found for {}; treating as archived/missing.",
        parentRegion.getEncodedName());
      return new ParentLoadResult(Collections.emptyList(), true);
    } catch (IOException e) {
      LOG.warn("Failed to open parent region {}; skipping lineage contribution.",
        parentRegion.getEncodedName(), e);
      return new ParentLoadResult(Collections.emptyList(), false);
    }
    List<StoreFileInfo> all =
      loadStoreFilesFromDisk(conf, tableDescriptor, familyDescriptor, parentRegionFs);
    List<StoreFileInfo> hfilesOnly = new ArrayList<>(all.size());
    for (StoreFileInfo info : all) {
      if (info.isReference() || HFileLink.isHFileLink(info.getPath().getName())) {
        LOG.debug("Skipping non-HFile entry {} in parent {} during lineage simulation.",
          info.getPath().getName(), parentRegion.getEncodedName());
        continue;
      }
      hfilesOnly.add(info);
    }
    return new ParentLoadResult(hfilesOnly, false);
  }

  private static LineageResult loadStoreFilesFromLineage(Configuration conf,
    TableDescriptor tableDescriptor, ColumnFamilyDescriptor familyDescriptor,
    HRegionFileSystem regionFs, Lineage lineage) throws IOException {
    if (lineage.getSplitParent().isPresent()) {
      return loadStoreFilesFromSplitParent(conf, tableDescriptor, familyDescriptor, regionFs,
        lineage.getSplitParent().get());
    }
    if (!lineage.getMergeParents().isEmpty()) {
      return loadStoreFilesFromMergeParents(conf, tableDescriptor, familyDescriptor, regionFs,
        lineage.getMergeParents());
    }
    return LineageResult.EMPTY;
  }

  private static LineageResult loadStoreFilesFromSplitParent(Configuration conf,
    TableDescriptor tableDescriptor, ColumnFamilyDescriptor familyDescriptor,
    HRegionFileSystem childRegionFs, RegionInfo splitParent) throws IOException {
    RegionInfo child = childRegionFs.getRegionInfo();
    boolean top = decideSplitDaughterIsTop(splitParent, child);
    byte[] splitRow = top ? child.getStartKey() : child.getEndKey();
    if (splitRow == null || splitRow.length == 0) {
      throw new IOException("Cannot derive split row for child " + child.getEncodedName()
        + " from parent " + splitParent.getEncodedName()
        + "; refusing to synthesize references without a provable split key.");
    }
    ParentLoadResult parentLoad = loadParentHFilesOnly(conf, tableDescriptor, familyDescriptor,
      childRegionFs, splitParent);
    if (parentLoad.archived) {
      ParentContribution pc =
        new ParentContribution(splitParent, ParentContribution.Status.ARCHIVED, 0);
      return new LineageResult(Collections.emptyList(), Collections.singletonList(pc));
    }
    if (parentLoad.hfiles.isEmpty()) {
      ParentContribution pc =
        new ParentContribution(splitParent, ParentContribution.Status.PRESENT_NO_FILES, 0);
      return new LineageResult(Collections.emptyList(), Collections.singletonList(pc));
    }
    Configuration storeConf =
      StoreUtils.createStoreConfiguration(conf, tableDescriptor, familyDescriptor);
    List<StoreFileInfo> derived = new ArrayList<>();
    for (StoreFileInfo parentFile : parentLoad.hfiles) {
      StoreFileInfo storeFileInfo = simulateSplitStoreFile(storeConf, familyDescriptor,
        childRegionFs.getFileSystem(),
        childRegionFs.getStoreDir(familyDescriptor.getNameAsString()), splitParent,
        child.getTable(), splitRow, top, parentFile);
      if (storeFileInfo != null) {
        derived.add(storeFileInfo);
      }
    }
    ParentContribution pc = new ParentContribution(splitParent,
      ParentContribution.Status.PRESENT_WITH_FILES, derived.size());
    return new LineageResult(derived, Collections.singletonList(pc));
  }

  private static LineageResult loadStoreFilesFromMergeParents(Configuration conf,
    TableDescriptor tableDescriptor, ColumnFamilyDescriptor familyDescriptor,
    HRegionFileSystem childRegionFs, List<RegionInfo> mergeParents) throws IOException {
    FileSystem fs = childRegionFs.getFileSystem();
    Path childStoreDir = childRegionFs.getStoreDir(familyDescriptor.getNameAsString());
    Configuration storeConf =
      StoreUtils.createStoreConfiguration(conf, tableDescriptor, familyDescriptor);
    List<StoreFileInfo> derived = new ArrayList<>();
    List<ParentContribution> contributions = new ArrayList<>();
    for (RegionInfo mergeParent : mergeParents) {
      ParentLoadResult parentLoad = loadParentHFilesOnly(conf, tableDescriptor,
        familyDescriptor, childRegionFs, mergeParent);
      if (parentLoad.archived) {
        contributions.add(
          new ParentContribution(mergeParent, ParentContribution.Status.ARCHIVED, 0));
        continue;
      }
      if (parentLoad.hfiles.isEmpty()) {
        contributions.add(
          new ParentContribution(mergeParent, ParentContribution.Status.PRESENT_NO_FILES, 0));
        continue;
      }
      int count = 0;
      for (StoreFileInfo parentFile : parentLoad.hfiles) {
        Reference reference = Reference.createTopReference(mergeParent.getStartKey());
        Path path = new Path(childStoreDir,
          parentFile.getPath().getName() + "." + mergeParent.getEncodedName());
        derived.add(new StoreFileInfo(storeConf, fs, path, reference));
        count++;
      }
      contributions.add(
        new ParentContribution(mergeParent, ParentContribution.Status.PRESENT_WITH_FILES, count));
    }
    return new LineageResult(derived, contributions);
  }

  private static StoreFileInfo simulateSplitStoreFile(Configuration conf,
    ColumnFamilyDescriptor familyDescriptor, FileSystem fs, Path childStoreDir,
    RegionInfo splitParent, TableName childTable, byte[] splitRow, boolean top,
    StoreFileInfo parentFile) throws IOException {
    HStoreFile storeFile =
      new HStoreFile(parentFile, familyDescriptor.getBloomFilterType(), CacheConfig.DISABLED);
    boolean readerOpened = false;
    boolean createLinkFile = false;
    boolean outOfRange = false;
    try {
      storeFile.initReader();
      readerOpened = true;
      ExtendedCell splitKey = PrivateCellUtil.createFirstOnRow(splitRow);
      Optional<ExtendedCell> lastKey = storeFile.getLastKey();
      Optional<ExtendedCell> firstKey = storeFile.getFirstKey();
      if (top) {
        if (!lastKey.isPresent()) {
          outOfRange = true;
        } else if (storeFile.getComparator().compare(splitKey, lastKey.get()) > 0) {
          outOfRange = true;
        } else if (
          firstKey.isPresent() && storeFile.getComparator().compare(splitKey, firstKey.get()) <= 0
        ) {
          createLinkFile = true;
        }
      } else {
        if (!firstKey.isPresent()) {
          outOfRange = true;
        } else if (storeFile.getComparator().compare(splitKey, firstKey.get()) < 0) {
          outOfRange = true;
        } else if (
          lastKey.isPresent() && storeFile.getComparator().compare(splitKey, lastKey.get()) >= 0
        ) {
          createLinkFile = true;
        }
      }
    } catch (IOException e) {
      LOG.warn("Failed to read parent file {} during split simulation; skipping.",
        parentFile.getPath(), e);
      return null;
    } finally {
      if (readerOpened) {
        try {
          storeFile.closeStoreFile(true);
        } catch (IOException e) {
          LOG.warn("Failed to close parent file {} after split simulation.", parentFile.getPath(),
            e);
        }
      }
    }
    if (outOfRange) {
      return null;
    }
    if (createLinkFile) {
      String hfileName = parentFile.getPath().getName();
      TableName linkedTable = childTable;
      String linkedRegion = splitParent.getEncodedName();
      if (HFileLink.isHFileLink(hfileName)) {
        Matcher matcher = HFileLink.LINK_NAME_PATTERN.matcher(hfileName);
        if (!matcher.matches()) {
          throw new IOException(hfileName + " is not a valid HFileLink name");
        }
        linkedTable = TableName.valueOf(matcher.group(1), matcher.group(2));
        linkedRegion = matcher.group(3);
        hfileName = matcher.group(4);
      }
      String linkName = HFileLink.createHFileLinkName(linkedTable, linkedRegion, hfileName);
      Path linkPath = new Path(childStoreDir, linkName);
      HFileLink link = HFileLink.build(conf, linkedTable, linkedRegion,
        familyDescriptor.getNameAsString(), hfileName);
      return new StoreFileInfo(conf, fs, linkPath, link);
    }
    Reference reference =
      top ? Reference.createTopReference(splitRow) : Reference.createBottomReference(splitRow);
    Path path =
      new Path(childStoreDir, parentFile.getPath().getName() + "." + splitParent.getEncodedName());
    return new StoreFileInfo(conf, fs, path, reference);
  }

  /**
   * Decide whether a child region is the top (upper) daughter of its split parent. Falls back to
   * the bottom daughter when only the start-key boundary matches. Throws if neither boundary
   * matches the parent, because that is not a provable split daughter.
   */
  static boolean decideSplitDaughterIsTop(RegionInfo splitParent, RegionInfo child)
    throws IOException {
    boolean startMatches = Bytes.equals(child.getStartKey(), splitParent.getStartKey());
    boolean endMatches = Bytes.equals(child.getEndKey(), splitParent.getEndKey());
    if (startMatches && !endMatches) {
      return false; // bottom daughter
    }
    if (endMatches && !startMatches) {
      return true; // top daughter
    }
    if (startMatches && endMatches) {
      throw new IOException("Child region " + child.getEncodedName()
        + " has the same key range as parent " + splitParent.getEncodedName()
        + "; cannot prove which daughter half this is.");
    }
    throw new IOException("Child region " + child.getEncodedName()
      + " does not share either boundary with parent " + splitParent.getEncodedName()
      + "; lineage is not provable, refusing to synthesize references.");
  }

  /**
   * Union store-file entries from disk and lineage. Disk entries take precedence over
   * lineage-derived entries with the same file name; a collision is logged.
   */
  private static List<StoreFileInfo> unionStoreFileEntries(List<StoreFileInfo> diskEntries,
    List<StoreFileInfo> lineageEntries) {
    Map<String, StoreFileInfo> byName = new LinkedHashMap<>();
    for (StoreFileInfo entry : diskEntries) {
      byName.put(entry.getPath().getName(), entry);
    }
    for (StoreFileInfo entry : lineageEntries) {
      String name = entry.getPath().getName();
      if (byName.containsKey(name)) {
        LOG.info(
          "Lineage-derived entry {} collides with on-disk entry; preferring on-disk.", name);
        continue;
      }
      byName.put(name, entry);
    }
    return new ArrayList<>(byName.values());
  }

  private static StoreFileList.Builder toStoreFileListBuilder(Collection<StoreFileInfo> storeFiles) {
    StoreFileList.Builder builder = StoreFileList.newBuilder();
    for (StoreFileInfo info : storeFiles) {
      StoreFileEntry.Builder entry =
        StoreFileEntry.newBuilder().setName(info.getPath().getName()).setSize(info.getSize());
      if (info.isReference()) {
        FSProtos.Reference reference = FSProtos.Reference.newBuilder()
          .setSplitkey(ByteString.copyFrom(info.getReference().getSplitKey()))
          .setRange(info.getReference().convert().getRange()).build();
        entry.setReference(reference);
      }
      builder.addStoreFile(entry.build());
    }
    return builder;
  }
}
