/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.quotas;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.HashMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.Multimap;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotRegionManifest.FamilyFiles;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotRegionManifest.StoreFile;

/**
 * Tracks file archiving and updates the hbase quota table.
 */
@InterfaceAudience.Private
public class FileArchiverNotifierImpl implements FileArchiverNotifier {
  private static final Logger LOG = LoggerFactory.getLogger(FileArchiverNotifierImpl.class);
  private final Connection conn;
  private final Configuration conf;
  private final FileSystem fs;
  private final TableName tn;
  private final ReadLock readLock;
  private final WriteLock writeLock;
  private volatile long lastFullCompute = Long.MIN_VALUE;
  private List<String> currentSnapshots = Collections.emptyList();
  private static final Map<String,Object> NAMESPACE_LOCKS = new HashMap<>();

  /**
   * An Exception thrown when SnapshotSize updates to hbase:quota fail to be written.
   */
  @InterfaceAudience.Private
  public static class QuotaSnapshotSizeSerializationException extends IOException {
    private static final long serialVersionUID = 1L;

    public QuotaSnapshotSizeSerializationException(String msg) {
      super(msg);
    }
  }

  public FileArchiverNotifierImpl(
      Connection conn, Configuration conf, FileSystem fs, TableName tn) {
    this.conn = conn;
    this.conf = conf;
    this.fs = fs;
    this.tn = tn;
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  static synchronized Object getLockForNamespace(String namespace) {
    return NAMESPACE_LOCKS.computeIfAbsent(namespace, (ns) -> new Object());
  }

  /**
   * Returns a strictly-increasing measure of time extracted by {@link System#nanoTime()}.
   */
  long getLastFullCompute() {
    return lastFullCompute;
  }

  @Override
  public void addArchivedFiles(Set<Entry<String, Long>> fileSizes) throws IOException {
    long start = System.nanoTime();
    readLock.lock();
    try {
      // We want to catch the case where we got an archival request, but there was a full
      // re-computation in progress that was blocking us. Most likely, the full computation is going
      // to already include the changes we were going to make.
      //
      // Same as "start < lastFullCompute" but avoiding numeric overflow per the
      // System.nanoTime() javadoc
      if (lastFullCompute != Long.MIN_VALUE && start - lastFullCompute < 0) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("A full computation was performed after this request was received."
              + " Ignoring requested updates: " + fileSizes);
        }
        return;
      }

      if (LOG.isTraceEnabled()) {
        LOG.trace("currentSnapshots: " + currentSnapshots + " fileSize: "+ fileSizes);
      }

      // Write increment to quota table for the correct snapshot. Only do this if we have snapshots
      // and some files that were archived.
      if (!currentSnapshots.isEmpty() && !fileSizes.isEmpty()) {
        // We get back the files which no snapshot referenced (the files which will be deleted soon)
        groupArchivedFiledBySnapshotAndRecordSize(currentSnapshots, fileSizes);
      }
    } finally {
      readLock.unlock();
    }
  }

  /**
   * For each file in the map, this updates the first snapshot (lexicographic snapshot name) that
   * references this file. The result of this computation is serialized to the quota table.
   *
   * @param snapshots A collection of HBase snapshots to group the files into
   * @param fileSizes A map of file names to their sizes
   */
  void groupArchivedFiledBySnapshotAndRecordSize(
      List<String> snapshots, Set<Entry<String, Long>> fileSizes) throws IOException {
    // Make a copy as we'll modify it.
    final Map<String,Long> filesToUpdate = new HashMap<>(fileSizes.size());
    for (Entry<String,Long> entry : fileSizes) {
      filesToUpdate.put(entry.getKey(), entry.getValue());
    }
    // Track the change in size to each snapshot
    final Map<String,Long> snapshotSizeChanges = new HashMap<>();
    for (String snapshot : snapshots) {
      // For each file in `filesToUpdate`, check if `snapshot` refers to it.
      // If `snapshot` does, remove it from `filesToUpdate` and add it to `snapshotSizeChanges`.
      bucketFilesToSnapshot(snapshot, filesToUpdate, snapshotSizeChanges);
      if (filesToUpdate.isEmpty()) {
        // If we have no more files recently archived, we have nothing more to check
        break;
      }
    }
    // We have computed changes to the snapshot size, we need to record them.
    if (!snapshotSizeChanges.isEmpty()) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Writing snapshot size changes for: " + snapshotSizeChanges);
      }
      persistSnapshotSizeChanges(snapshotSizeChanges);
    }
  }

  /**
   * For the given snapshot, find all files which this {@code snapshotName} references. After a file
   * is found to be referenced by the snapshot, it is removed from {@code filesToUpdate} and
   * {@code snapshotSizeChanges} is updated in concert.
   *
   * @param snapshotName The snapshot to check
   * @param filesToUpdate A mapping of archived files to their size
   * @param snapshotSizeChanges A mapping of snapshots and their change in size
   */
  void bucketFilesToSnapshot(
      String snapshotName, Map<String,Long> filesToUpdate, Map<String,Long> snapshotSizeChanges)
          throws IOException {
    // A quick check to avoid doing work if the caller unnecessarily invoked this method.
    if (filesToUpdate.isEmpty()) {
      return;
    }

    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(
        snapshotName, CommonFSUtils.getRootDir(conf));
    SnapshotDescription sd = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
    SnapshotManifest manifest = SnapshotManifest.open(conf, fs, snapshotDir, sd);
    // For each region referenced by the snapshot
    for (SnapshotRegionManifest rm : manifest.getRegionManifests()) {
      // For each column family in this region
      for (FamilyFiles ff : rm.getFamilyFilesList()) {
        // And each store file in that family
        for (StoreFile sf : ff.getStoreFilesList()) {
          Long valueOrNull = filesToUpdate.remove(sf.getName());
          if (valueOrNull != null) {
            // This storefile was recently archived, we should update this snapshot with its size
            snapshotSizeChanges.merge(snapshotName, valueOrNull, Long::sum);
          }
          // Short-circuit, if we have no more files that were archived, we don't need to iterate
          // over the rest of the snapshot.
          if (filesToUpdate.isEmpty()) {
            return;
          }
        }
      }
    }
  }

  /**
   * Reads the current size for each snapshot to update, generates a new update based on that value,
   * and then writes the new update.
   *
   * @param snapshotSizeChanges A map of snapshot name to size change
   */
  void persistSnapshotSizeChanges(Map<String,Long> snapshotSizeChanges) throws IOException {
    try (Table quotaTable = conn.getTable(QuotaTableUtil.QUOTA_TABLE_NAME)) {
      // Create a list (with a more typical ordering implied)
      final List<Entry<String,Long>> snapshotSizeEntries = new ArrayList<>(
          snapshotSizeChanges.entrySet());
      // Create the Gets for each snapshot we need to update
      final List<Get> snapshotSizeGets = snapshotSizeEntries.stream()
          .map((e) -> QuotaTableUtil.makeGetForSnapshotSize(tn, e.getKey()))
          .collect(Collectors.toList());
      final Iterator<Entry<String,Long>> iterator = snapshotSizeEntries.iterator();
      // A List to store each Put we'll create from the Get's we retrieve
      final List<Put> updates = new ArrayList<>(snapshotSizeEntries.size());

      // TODO Push this down to the RegionServer with a coprocessor:
      //
      // We would really like to piggy-back on the row-lock already being grabbed
      // to handle the update of the row in the quota table. However, because the value
      // is a serialized protobuf, the standard Increment API doesn't work for us. With a CP, we
      // can just send the size deltas to the RS and atomically update the serialized PB object
      // while relying on the row-lock for synchronization.
      //
      // Synchronizing on the namespace string is a "minor smell" but passable as this is
      // only invoked via a single caller (the active Master). Using the namespace name lets us
      // have some parallelism without worry of on caller seeing stale data from the quota table.
      synchronized (getLockForNamespace(tn.getNamespaceAsString())) {
        final Result[] existingSnapshotSizes = quotaTable.get(snapshotSizeGets);
        long totalSizeChange = 0;
        // Read the current size values (if they exist) to generate the new value
        for (Result result : existingSnapshotSizes) {
          Entry<String,Long> entry = iterator.next();
          String snapshot = entry.getKey();
          Long size = entry.getValue();
          // Track the total size change for the namespace this table belongs in
          totalSizeChange += size;
          // Get the size of the previous value (or zero)
          long previousSize = getSnapshotSizeFromResult(result);
          // Create an update. A file was archived from the table, so the table's size goes
          // down, but the snapshot's size goes up.
          updates.add(QuotaTableUtil.createPutForSnapshotSize(tn, snapshot, previousSize + size));
        }

        // Create an update for the summation of all snapshots in the namespace
        if (totalSizeChange != 0) {
          long previousSize = getPreviousNamespaceSnapshotSize(
              quotaTable, tn.getNamespaceAsString());
          updates.add(QuotaTableUtil.createPutForNamespaceSnapshotSize(
              tn.getNamespaceAsString(), previousSize + totalSizeChange));
        }

        // Send all of the quota table updates in one batch.
        List<Object> failures = new ArrayList<>();
        final Object[] results = new Object[updates.size()];
        quotaTable.batch(updates, results);
        for (Object result : results) {
          // A null result is an error condition (all RPC attempts failed)
          if (!(result instanceof Result)) {
            failures.add(result);
          }
        }
        // Propagate a failure if any updates failed
        if (!failures.isEmpty()) {
          throw new QuotaSnapshotSizeSerializationException(
              "Failed to write some snapshot size updates: " + failures);
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return;
    }
  }

  /**
   * Fetches the current size of all snapshots in the given {@code namespace}.
   *
   * @param quotaTable The HBase quota table
   * @param namespace Namespace to fetch the sum of snapshot sizes for
   * @return The size of all snapshot sizes for the namespace in bytes.
   */
  long getPreviousNamespaceSnapshotSize(Table quotaTable, String namespace) throws IOException {
    // Update the size of each snapshot for all snapshots in a namespace.
    Result r = quotaTable.get(
        QuotaTableUtil.createGetNamespaceSnapshotSize(namespace));
    return getSnapshotSizeFromResult(r);
  }

  /**
   * Extracts the size component from a serialized {@link SpaceQuotaSnapshot} protobuf.
   *
   * @param r A Result containing one cell with a SpaceQuotaSnapshot protobuf
   * @return The size in bytes of the snapshot.
   */
  long getSnapshotSizeFromResult(Result r) throws InvalidProtocolBufferException {
    // Per javadoc, Result should only be null if an exception was thrown. So, if we're here,
    // we should be non-null. If we can't advance to the first cell, same as "no cell".
    if (!r.isEmpty() && r.advance()) {
      return QuotaTableUtil.parseSnapshotSize(r.current());
    }
    return 0L;
  }

  @Override
  public long computeAndStoreSnapshotSizes(
      Collection<String> currentSnapshots) throws IOException {
    // Record what the current snapshots are
    this.currentSnapshots = new ArrayList<>(currentSnapshots);
    Collections.sort(this.currentSnapshots);

    // compute new size for table + snapshots for that table
    List<SnapshotWithSize> snapshotSizes = computeSnapshotSizes(this.currentSnapshots);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Computed snapshot sizes for " + tn + " of " + snapshotSizes);
    }

    // Compute the total size of all snapshots against our table
    final long totalSnapshotSize = snapshotSizes.stream().mapToLong((sws) -> sws.getSize()).sum();

    writeLock.lock();
    try {
      // Persist the size of each snapshot
      try (Table quotaTable = conn.getTable(QuotaTableUtil.QUOTA_TABLE_NAME)) {
        persistSnapshotSizes(quotaTable, snapshotSizes);
      }

      // Report the last time we did a recomputation
      lastFullCompute = System.nanoTime();

      return totalSnapshotSize;
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName()).append("[");
    sb.append("tableName=").append(tn).append(", currentSnapshots=");
    sb.append(currentSnapshots).append(", lastFullCompute=").append(lastFullCompute);
    return sb.append("]").toString();
  }

  /**
   * Computes the size of each snapshot against the table referenced by {@code this}.
   *
   * @param snapshots A sorted list of snapshots against {@code tn}.
   * @return A list of the size for each snapshot against {@code tn}.
   */
  List<SnapshotWithSize> computeSnapshotSizes(List<String> snapshots) throws IOException {
    final List<SnapshotWithSize> snapshotSizes = new ArrayList<>(snapshots.size());
    final Path rootDir = CommonFSUtils.getRootDir(conf);

    // Get the map of store file names to store file path for this table
    final Set<String> tableReferencedStoreFiles;
    try {
      tableReferencedStoreFiles = FSUtils.getTableStoreFilePathMap(fs, rootDir).keySet();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("Paths for " + tn + ": " + tableReferencedStoreFiles);
    }

    // For each snapshot on this table, get the files which the snapshot references which
    // the table does not.
    Set<String> snapshotReferencedFiles = new HashSet<>();
    for (String snapshotName : snapshots) {
      Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
      SnapshotDescription sd = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
      SnapshotManifest manifest = SnapshotManifest.open(conf, fs, snapshotDir, sd);

      if (LOG.isTraceEnabled()) {
        LOG.trace("Files referenced by other snapshots: " + snapshotReferencedFiles);
      }

      // Get the set of files from the manifest that this snapshot references which are not also
      // referenced by the originating table.
      Set<StoreFileReference> unreferencedStoreFileNames = getStoreFilesFromSnapshot(
          manifest, (sfn) -> !tableReferencedStoreFiles.contains(sfn)
              && !snapshotReferencedFiles.contains(sfn));

      if (LOG.isTraceEnabled()) {
        LOG.trace("Snapshot " + snapshotName + " solely references the files: "
            + unreferencedStoreFileNames);
      }

      // Compute the size of the store files for this snapshot
      long size = getSizeOfStoreFiles(tn, unreferencedStoreFileNames);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Computed size of " + snapshotName + " to be " + size);
      }

      // Persist this snapshot's size into the map
      snapshotSizes.add(new SnapshotWithSize(snapshotName, size));

      // Make sure that we don't double-count the same file
      for (StoreFileReference ref : unreferencedStoreFileNames) {
        for (String fileNames : ref.getFamilyToFilesMapping().values()) {
          snapshotReferencedFiles.add(fileNames);
        }
      }
    }

    return snapshotSizes;
  }

  /**
   * Computes the size of each store file in {@code storeFileNames}
   */
  long getSizeOfStoreFiles(TableName tn, Set<StoreFileReference> storeFileNames) {
    return storeFileNames.stream()
        .collect(Collectors.summingLong((sfr) -> getSizeOfStoreFile(tn, sfr)));
  }

  /**
   * Computes the size of the store files for a single region.
   */
  long getSizeOfStoreFile(TableName tn, StoreFileReference storeFileName) {
    String regionName = storeFileName.getRegionName();
    return storeFileName.getFamilyToFilesMapping()
        .entries().stream()
        .collect(Collectors.summingLong((e) ->
            getSizeOfStoreFile(tn, regionName, e.getKey(), e.getValue())));
  }

  /**
   * Computes the size of the store file given its name, region and family name in
   * the archive directory.
   */
  long getSizeOfStoreFile(
      TableName tn, String regionName, String family, String storeFile) {
    Path familyArchivePath;
    try {
      familyArchivePath = HFileArchiveUtil.getStoreArchivePath(conf, tn, regionName, family);
    } catch (IOException e) {
      LOG.warn("Could not compute path for the archive directory for the region", e);
      return 0L;
    }
    Path fileArchivePath = new Path(familyArchivePath, storeFile);
    try {
      if (fs.exists(fileArchivePath)) {
        FileStatus[] status = fs.listStatus(fileArchivePath);
        if (1 != status.length) {
          LOG.warn("Expected " + fileArchivePath +
              " to be a file but was a directory, ignoring reference");
          return 0L;
        }
        return status[0].getLen();
      }
    } catch (IOException e) {
      LOG.warn("Could not obtain the status of " + fileArchivePath, e);
      return 0L;
    }
    LOG.warn("Expected " + fileArchivePath + " to exist but does not, ignoring reference.");
    return 0L;
  }

  /**
   * Extracts the names of the store files referenced by this snapshot which satisfy the given
   * predicate (the predicate returns {@code true}).
   */
  Set<StoreFileReference> getStoreFilesFromSnapshot(
      SnapshotManifest manifest, Predicate<String> filter) {
    Set<StoreFileReference> references = new HashSet<>();
    // For each region referenced by the snapshot
    for (SnapshotRegionManifest rm : manifest.getRegionManifests()) {
      StoreFileReference regionReference = new StoreFileReference(
          ProtobufUtil.toRegionInfo(rm.getRegionInfo()).getEncodedName());

      // For each column family in this region
      for (FamilyFiles ff : rm.getFamilyFilesList()) {
        final String familyName = ff.getFamilyName().toStringUtf8();
        // And each store file in that family
        for (StoreFile sf : ff.getStoreFilesList()) {
          String storeFileName = sf.getName();
          // A snapshot only "inherits" a files size if it uniquely refers to it (no table
          // and no other snapshot references it).
          if (filter.test(storeFileName)) {
            regionReference.addFamilyStoreFile(familyName, storeFileName);
          }
        }
      }
      // Only add this Region reference if we retained any files.
      if (!regionReference.getFamilyToFilesMapping().isEmpty()) {
        references.add(regionReference);
      }
    }
    return references;
  }

  /**
   * Writes the snapshot sizes to the provided {@code table}.
   */
  void persistSnapshotSizes(
      Table table, List<SnapshotWithSize> snapshotSizes) throws IOException {
    // Convert each entry in the map to a Put and write them to the quota table
    table.put(snapshotSizes
        .stream()
        .map(sws -> QuotaTableUtil.createPutForSnapshotSize(
            tn, sws.getName(), sws.getSize()))
        .collect(Collectors.toList()));
  }

  /**
   * A struct encapsulating the name of a snapshot and its "size" on the filesystem. This size is
   * defined as the amount of filesystem space taken by the files the snapshot refers to which
   * the originating table no longer refers to.
   */
  static class SnapshotWithSize {
    private final String name;
    private final long size;

    SnapshotWithSize(String name, long size) {
      this.name = Objects.requireNonNull(name);
      this.size = size;
    }

    String getName() {
      return name;
    }

    long getSize() {
      return size;
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder().append(name).append(size).toHashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (!(o instanceof SnapshotWithSize)) {
        return false;
      }

      SnapshotWithSize other = (SnapshotWithSize) o;
      return name.equals(other.name) && size == other.size;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder(32);
      return sb.append("SnapshotWithSize:[").append(name).append(" ")
          .append(StringUtils.byteDesc(size)).append("]").toString();
    }
  }

  /**
   * A reference to a collection of files in the archive directory for a single region.
   */
  static class StoreFileReference {
    private final String regionName;
    private final Multimap<String,String> familyToFiles;

    StoreFileReference(String regionName) {
      this.regionName = Objects.requireNonNull(regionName);
      familyToFiles = HashMultimap.create();
    }

    String getRegionName() {
      return regionName;
    }

    Multimap<String,String> getFamilyToFilesMapping() {
      return familyToFiles;
    }

    void addFamilyStoreFile(String family, String storeFileName) {
      familyToFiles.put(family, storeFileName);
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder().append(regionName).append(familyToFiles).toHashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof StoreFileReference)) {
        return false;
      }
      StoreFileReference other = (StoreFileReference) o;
      return regionName.equals(other.regionName) && familyToFiles.equals(other.familyToFiles);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      return sb.append("StoreFileReference[region=").append(regionName).append(", files=")
          .append(familyToFiles).append("]").toString();
    }
  }
}
