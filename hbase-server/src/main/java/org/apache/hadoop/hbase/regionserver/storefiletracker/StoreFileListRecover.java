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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CatalogFamilyFormat;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.StoreContext;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.regionserver.StoreUtils;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.shaded.protobuf.generated.FSProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.StoreFileTrackerProtos.StoreFileEntry;
import org.apache.hadoop.hbase.shaded.protobuf.generated.StoreFileTrackerProtos.StoreFileList;

/**
 * Offline helper that rebuilds the FILE store-file-tracker manifest for a single store
 * (table + region + family) when the latest manifest cannot be loaded.
 * <p>
 * See {@code dev-support/design-docs/fsft-manifest-recover.md} for the full design.
 * <p>
 * The recovered manifest is reconstructed <em>purely from the store directory listing</em>: the
 * set of HFiles, references and links physically present under the family directory. Recovery
 * never synthesizes references/links from split/merge lineage and never modifies an existing
 * manifest in place. It writes a brand new, strictly-newer generation under {@code .filelist} via
 * {@link StoreFileListFile#writeNew(StoreFileList.Builder)}, leaving {@code load(false)} to prune
 * older files on the next region open.
 * <p>
 * For user-table regions, split/merge parents discovered from {@code hbase:meta} are consulted for
 * <em>reporting only</em>: if any parent still has unarchived HFiles on disk, the recovered store
 * may be missing data the Catalog Janitor has not yet propagated, and the report flags potential
 * data loss so an operator can decide whether a data recovery is required.
 */
@InterfaceAudience.Private
public final class StoreFileListRecover {

  private static final Logger LOG = LoggerFactory.getLogger(StoreFileListRecover.class);

  /**
   * Tracks the on-disk archive status of a single split/merge parent region. Recovery uses this to
   * distinguish parents that have been fully archived by the Catalog Janitor (no data loss) from
   * parents that still have unarchived HFiles (potential data loss requiring operator review).
   */
  public static final class ParentContribution {
    public enum Status {
      /** Parent region directory was not found; Catalog Janitor has archived it. */
      ARCHIVED,
      /** Parent region directory exists and still has unarchived HFiles. */
      PRESENT_WITH_FILES,
      /** Parent region directory exists but has no unarchived HFiles. */
      PRESENT_NO_FILES
    }

    private final RegionInfo parent;
    private final Status status;
    private final int unarchivedHFileCount;

    ParentContribution(RegionInfo parent, Status status, int unarchivedHFileCount) {
      this.parent = parent;
      this.status = status;
      this.unarchivedHFileCount = unarchivedHFileCount;
    }

    public RegionInfo getParent() {
      return parent;
    }

    public Status getStatus() {
      return status;
    }

    public int getUnarchivedHFileCount() {
      return unarchivedHFileCount;
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

  public static final class RecoverReport {
    private final List<TrackerFileDiagnostic> diagnostics;
    private final List<StoreFileInfo> manifestEntries;
    private final List<ParentContribution> parentContributions;
    private final Path writtenManifest;
    private final boolean noOp;

    RecoverReport(List<TrackerFileDiagnostic> diagnostics, List<StoreFileInfo> manifestEntries,
      List<ParentContribution> parentContributions, Path writtenManifest, boolean noOp) {
      this.diagnostics = Collections.unmodifiableList(new ArrayList<>(diagnostics));
      this.manifestEntries = Collections.unmodifiableList(new ArrayList<>(manifestEntries));
      this.parentContributions =
        Collections.unmodifiableList(new ArrayList<>(parentContributions));
      this.writtenManifest = writtenManifest;
      this.noOp = noOp;
    }

    public List<TrackerFileDiagnostic> getDiagnostics() {
      return diagnostics;
    }

    /** The store-file set reconstructed from the store directory; this is what gets written. */
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

    /** Returns true when at least one parent was assessed and all of them were already archived. */
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

    /** Returns true when at least one parent still has unarchived HFiles on disk. */
    public boolean hasUnarchivedParents() {
      for (ParentContribution pc : parentContributions) {
        if (pc.getStatus() == ParentContribution.Status.PRESENT_WITH_FILES) {
          return true;
        }
      }
      return false;
    }
  }

  private StoreFileListRecover() {
  }

  /**
   * Rebuild the FSFT manifest for a single store from its on-disk file listing.
   * @param conf             configuration
   * @param tableDescriptor  descriptor of the store's table
   * @param familyDescriptor descriptor of the target column family
   * @param regionFs         region filesystem opened read-only
   * @param parents          split/merge parent regions of this region (from {@code hbase:meta}),
   *                         consulted for data-loss reporting only; pass an empty list to skip the
   *                         assessment (e.g. for {@code hbase:meta} / {@code master:store})
   * @param dryRun           when true, compute and report but do not write a new manifest
   */
  public static RecoverReport recover(Configuration conf, TableDescriptor tableDescriptor,
    ColumnFamilyDescriptor familyDescriptor, HRegionFileSystem regionFs, List<RegionInfo> parents,
    boolean dryRun) throws IOException {
    StoreContext storeContext = StoreContext.getBuilder()
      .withColumnFamilyDescriptor(familyDescriptor)
      .withFamilyStoreDirectoryPath(regionFs.getStoreDir(familyDescriptor.getNameAsString()))
      .withRegionFileSystem(regionFs).build();
    StoreFileListFile storeFileListFile = new StoreFileListFile(storeContext);

    List<TrackerFileDiagnostic> diagnostics =
      diagnoseTrackerFiles(storeFileListFile, regionFs, familyDescriptor);

    // The manifest is reconstructed purely from the store directory listing.
    List<StoreFileInfo> manifestEntries =
      loadStoreFilesFromDisk(conf, tableDescriptor, familyDescriptor, regionFs);

    // Assess split/merge parents for data-loss reporting only. No references/links are synthesized
    // into the manifest from this.
    List<ParentContribution> parentContributions = (parents == null || parents.isEmpty())
      ? Collections.emptyList()
      : assessParents(conf, tableDescriptor, familyDescriptor, regionFs, parents);

    // No-op detection: if there is a healthy latest tracker file whose contents already match
    // the recomputed set by name, do not churn the seqId.
    boolean noOp = isAlreadyHealthy(diagnostics, manifestEntries, storeFileListFile);

    Path writtenManifest = null;
    if (!dryRun && !noOp) {
      writtenManifest = storeFileListFile.writeNew(toStoreFileListBuilder(manifestEntries));
      LOG.info("Wrote recovered FSFT manifest at {} with {} entries", writtenManifest,
        manifestEntries.size());
    }
    return new RecoverReport(diagnostics, manifestEntries, parentContributions, writtenManifest,
      noOp);
  }

  /**
   * Resolve the split/merge parent regions for a region by consulting {@code hbase:meta}. Returns
   * the merge parents recorded on the region's own row if present; otherwise scans the table's
   * regions for a split parent that references this region as a daughter. Returns an empty list if
   * the region has no recorded lineage.
   * @param conn       connection to use for meta lookups; must not be closed by this method
   * @param regionInfo the child region whose parents we want
   */
  public static List<RegionInfo> resolveParents(Connection conn, RegionInfo regionInfo)
    throws IOException {
    Result childRow = MetaTableAccessor.getRegionResult(conn, regionInfo);
    if (childRow != null && !childRow.isEmpty()) {
      List<RegionInfo> mergeParents = CatalogFamilyFormat.getMergeRegions(childRow.rawCells());
      if (mergeParents != null && !mergeParents.isEmpty()) {
        return new ArrayList<>(mergeParents);
      }
    }
    final RegionInfo[] splitParent = new RegionInfo[1];
    MetaTableAccessor.scanMetaForTableRegions(conn, result -> {
      PairOfSameType<RegionInfo> daughters = MetaTableAccessor.getDaughterRegions(result);
      if (regionInfo.equals(daughters.getFirst()) || regionInfo.equals(daughters.getSecond())) {
        splitParent[0] = CatalogFamilyFormat.getRegionInfo(result);
        return false;
      }
      return true;
    }, regionInfo.getTable());
    return splitParent[0] != null ? Collections.singletonList(splitParent[0])
      : Collections.emptyList();
  }

  /**
   * Convenience overload that opens (and closes) its own {@link Connection} from {@code conf}. Use
   * from standalone/offline contexts (the {@code sftrecover} CLI).
   */
  public static List<RegionInfo> resolveParents(Configuration conf, RegionInfo regionInfo)
    throws IOException {
    try (Connection conn = ConnectionFactory.createConnection(conf)) {
      return resolveParents(conn, regionInfo);
    }
  }

  /**
   * Returns true when the tracker generation the runtime would actually serve already exposes the
   * same store-file name set as the recomputed one, so recovery would only churn the seqId. This is
   * best-effort and only avoids unnecessary writes; it never relaxes a safety check. When in doubt
   * it returns false, because writing a fresh, strictly-newer generation is always safe.
   * <p>
   * It faithfully mirrors {@link StoreFileListFile#load(boolean)} selection: generations are
   * ordered by the <em>numeric</em> seqId parsed from the file name (not lexicographically), and
   * within the winning seqId the {@code f1}/{@code f2} rotation pair is disambiguated by the
   * internal {@link StoreFileList#getTimestamp()} exactly like {@code select(...)}. Crucially, if
   * any corrupted tracker file sits at or above the newest healthy generation, {@code load(false)}
   * would hit it first and fail region open, so this is <em>not</em> treated as a no-op.
   */
  private static boolean isAlreadyHealthy(List<TrackerFileDiagnostic> diagnostics,
    List<StoreFileInfo> manifestEntries, StoreFileListFile storeFileListFile) {
    if (diagnostics.isEmpty()) {
      // No tracker files at all -> not "already healthy"; we still need to write one if
      // there is at least one entry to record. If there are no entries either, treat as no-op.
      return manifestEntries.isEmpty();
    }
    // Highest-seqId healthy generation, by numeric seqId (mirroring StoreFileListFile.listFiles()).
    long newestHealthySeqId = -1L;
    for (TrackerFileDiagnostic d : diagnostics) {
      if (d.isCorrupted()) {
        continue;
      }
      newestHealthySeqId = Math.max(newestHealthySeqId, parseSeqId(d.getPath()));
    }
    if (newestHealthySeqId < 0) {
      // Every tracker file is corrupted; recovery is definitely needed.
      return false;
    }
    // If a corrupted tracker file has a seqId >= the newest healthy generation, the runtime
    // load(false) visits it first and a non-EOF corruption fails region open before the healthy
    // generation is ever reached. Recovery is required; do not declare a no-op.
    for (TrackerFileDiagnostic d : diagnostics) {
      if (d.isCorrupted() && parseSeqId(d.getPath()) >= newestHealthySeqId) {
        return false;
      }
    }
    // Among the healthy files sharing the newest seqId there may be an f1/f2 rotation pair carrying
    // different internal timestamps; the one with the greater timestamp is what the runtime serves.
    StoreFileList winner = null;
    for (TrackerFileDiagnostic d : diagnostics) {
      if (d.isCorrupted() || parseSeqId(d.getPath()) != newestHealthySeqId) {
        continue;
      }
      try {
        StoreFileList candidate = storeFileListFile.load(d.getPath());
        if (winner == null || candidate.getTimestamp() > winner.getTimestamp()) {
          winner = candidate;
        }
      } catch (IOException e) {
        // A file previously diagnosed as healthy now fails to load; be conservative and recover.
        return false;
      }
    }
    if (winner == null) {
      return false;
    }
    if (winner.getStoreFileCount() != manifestEntries.size()) {
      return false;
    }
    Set<String> expected = new HashSet<>();
    for (StoreFileInfo info : manifestEntries) {
      expected.add(info.getPath().getName());
    }
    for (StoreFileEntry entry : winner.getStoreFileList()) {
      if (!expected.contains(entry.getName())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Parse the numeric seqId encoded in a tracker file name ({@code f1}, {@code f1.<seqId>},
   * {@code f2.<seqId>}), mirroring {@link StoreFileListFile#listFiles()}: a missing or unparseable
   * suffix yields {@code 0}. The {@link StoreFileListFile#TRACK_FILE_PATTERN} guarantees the suffix
   * (when present) is all digits, so this never throws for valid track files.
   */
  private static long parseSeqId(Path path) {
    String name = path.getName();
    int sep = name.indexOf(StoreFileListFile.TRACK_FILE_SEPARATOR);
    if (sep < 0 || sep == name.length() - 1) {
      return 0L;
    }
    try {
      return Long.parseLong(name.substring(sep + 1));
    } catch (NumberFormatException e) {
      return 0L;
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
   * Holds the result of probing a parent region directory: the real (non-reference, non-link)
   * HFiles still present, and whether the parent directory was archived (not found).
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
   * Assess each split/merge parent's on-disk archive status for data-loss reporting. This is purely
   * diagnostic: it never contributes entries to the recovered manifest.
   */
  private static List<ParentContribution> assessParents(Configuration conf,
    TableDescriptor tableDescriptor, ColumnFamilyDescriptor familyDescriptor,
    HRegionFileSystem regionFs, List<RegionInfo> parents) throws IOException {
    List<ParentContribution> contributions = new ArrayList<>(parents.size());
    for (RegionInfo parent : parents) {
      ParentLoadResult load =
        loadParentHFilesOnly(conf, tableDescriptor, familyDescriptor, regionFs, parent);
      if (load.archived) {
        contributions.add(new ParentContribution(parent, ParentContribution.Status.ARCHIVED, 0));
      } else if (load.hfiles.isEmpty()) {
        contributions.add(
          new ParentContribution(parent, ParentContribution.Status.PRESENT_NO_FILES, 0));
      } else {
        contributions.add(new ParentContribution(parent,
          ParentContribution.Status.PRESENT_WITH_FILES, load.hfiles.size()));
      }
    }
    return contributions;
  }

  /**
   * Returns the parent region's real on-disk HFiles only (reference files, link files, MOB link
   * files etc. are excluded, as they do not represent unarchived parent data). The returned
   * {@link ParentLoadResult#archived} flag indicates whether the parent region directory was not
   * found (i.e. the Catalog Janitor archived it).
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
      parentRegionFs = HRegionFileSystem.openRegionFromFileSystem(conf, fs,
        childRegionFs.getTableDir(), parentRegion, true);
    } catch (FileNotFoundException e) {
      LOG.info("Parent region directory not found for {}; treating as archived/missing.",
        parentRegion.getEncodedName());
      return new ParentLoadResult(Collections.emptyList(), true);
    } catch (IOException e) {
      LOG.warn("Failed to open parent region {}; skipping data-loss assessment for it.",
        parentRegion.getEncodedName(), e);
      return new ParentLoadResult(Collections.emptyList(), false);
    }
    List<StoreFileInfo> all =
      loadStoreFilesFromDisk(conf, tableDescriptor, familyDescriptor, parentRegionFs);
    List<StoreFileInfo> hfilesOnly = new ArrayList<>(all.size());
    for (StoreFileInfo info : all) {
      if (info.isReference() || HFileLink.isHFileLink(info.getPath().getName())) {
        LOG.debug("Skipping non-HFile entry {} in parent {} during data-loss assessment.",
          info.getPath().getName(), parentRegion.getEncodedName());
        continue;
      }
      hfilesOnly.add(info);
    }
    return new ParentLoadResult(hfilesOnly, false);
  }

  private static StoreFileList.Builder
    toStoreFileListBuilder(Collection<StoreFileInfo> storeFiles) {
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
