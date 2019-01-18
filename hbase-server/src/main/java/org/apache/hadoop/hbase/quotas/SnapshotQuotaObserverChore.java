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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MetricsMaster;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotRegionManifest.FamilyFiles;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotRegionManifest.StoreFile;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.util.StringUtils;

import org.apache.hbase.thirdparty.com.google.common.collect.HashMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.Multimap;

/**
 * A Master-invoked {@code Chore} that computes the size of each snapshot which was created from
 * a table which has a space quota.
 */
@InterfaceAudience.Private
public class SnapshotQuotaObserverChore extends ScheduledChore {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotQuotaObserverChore.class);
  static final String SNAPSHOT_QUOTA_CHORE_PERIOD_KEY =
      "hbase.master.quotas.snapshot.chore.period";
  static final int SNAPSHOT_QUOTA_CHORE_PERIOD_DEFAULT = 1000 * 60 * 5; // 5 minutes in millis

  static final String SNAPSHOT_QUOTA_CHORE_DELAY_KEY =
      "hbase.master.quotas.snapshot.chore.delay";
  static final long SNAPSHOT_QUOTA_CHORE_DELAY_DEFAULT = 1000L * 60L; // 1 minute in millis

  static final String SNAPSHOT_QUOTA_CHORE_TIMEUNIT_KEY =
      "hbase.master.quotas.snapshot.chore.timeunit";
  static final String SNAPSHOT_QUOTA_CHORE_TIMEUNIT_DEFAULT = TimeUnit.MILLISECONDS.name();

  private final Connection conn;
  private final Configuration conf;
  private final MetricsMaster metrics;
  private final FileSystem fs;

  public SnapshotQuotaObserverChore(HMaster master, MetricsMaster metrics) {
    this(
        master.getConnection(), master.getConfiguration(), master.getFileSystem(), master, metrics);
  }

  SnapshotQuotaObserverChore(
      Connection conn, Configuration conf, FileSystem fs, Stoppable stopper,
      MetricsMaster metrics) {
    super(
        QuotaObserverChore.class.getSimpleName(), stopper, getPeriod(conf),
        getInitialDelay(conf), getTimeUnit(conf));
    this.conn = conn;
    this.conf = conf;
    this.metrics = metrics;
    this.fs = fs;
  }

  @Override
  protected void chore() {
    try {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Computing sizes of snapshots for quota management.");
      }
      long start = System.nanoTime();
      _chore();
      if (null != metrics) {
        metrics.incrementSnapshotObserverTime((System.nanoTime() - start) / 1_000_000);
      }
    } catch (IOException e) {
      LOG.warn("Failed to compute the size of snapshots, will retry", e);
    }
  }

  void _chore() throws IOException {
    // Gets all tables with quotas that also have snapshots.
    // This values are all of the snapshots that we need to compute the size of.
    long start = System.nanoTime();
    Multimap<TableName,String> snapshotsToComputeSize = getSnapshotsToComputeSize();
    if (null != metrics) {
      metrics.incrementSnapshotFetchTime((System.nanoTime() - start) / 1_000_000);
    }

    // For each table, compute the size of each snapshot
    Multimap<TableName,SnapshotWithSize> snapshotsWithSize = computeSnapshotSizes(
        snapshotsToComputeSize);

    // Write the size data to the quota table.
    persistSnapshotSizes(snapshotsWithSize);
  }

  /**
   * Fetches each table with a quota (table or namespace quota), and then fetch the name of each
   * snapshot which was created from that table.
   *
   * @return A mapping of table to snapshots created from that table
   */
  Multimap<TableName,String> getSnapshotsToComputeSize() throws IOException {
    Set<TableName> tablesToFetchSnapshotsFrom = new HashSet<>();
    QuotaFilter filter = new QuotaFilter();
    filter.addTypeFilter(QuotaType.SPACE);
    try (Admin admin = conn.getAdmin()) {
      // Pull all of the tables that have quotas (direct, or from namespace)
      for (QuotaSettings qs : QuotaRetriever.open(conf, filter)) {
        if (qs.getQuotaType() == QuotaType.SPACE) {
          String ns = qs.getNamespace();
          TableName tn = qs.getTableName();
          if ((null == ns && null == tn) || (null != ns && null != tn)) {
            throw new IllegalStateException(
                "Expected either one of namespace and tablename to be null but not both");
          }
          // Collect either the table name itself, or all of the tables in the namespace
          if (null != ns) {
            tablesToFetchSnapshotsFrom.addAll(Arrays.asList(admin.listTableNamesByNamespace(ns)));
          } else {
            tablesToFetchSnapshotsFrom.add(tn);
          }
        }
      }
      // Fetch all snapshots that were created from these tables
      return getSnapshotsFromTables(admin, tablesToFetchSnapshotsFrom);
    }
  }

  /**
   * Computes a mapping of originating {@code TableName} to snapshots, when the {@code TableName}
   * exists in the provided {@code Set}.
   */
  Multimap<TableName,String> getSnapshotsFromTables(
      Admin admin, Set<TableName> tablesToFetchSnapshotsFrom) throws IOException {
    Multimap<TableName,String> snapshotsToCompute = HashMultimap.create();
    for (org.apache.hadoop.hbase.client.SnapshotDescription sd : admin.listSnapshots()) {
      TableName tn = sd.getTableName();
      if (tablesToFetchSnapshotsFrom.contains(tn)) {
        snapshotsToCompute.put(tn, sd.getName());
      }
    }
    return snapshotsToCompute;
  }

  /**
   * Computes the size of each snapshot provided given the current files referenced by the table.
   *
   * @param snapshotsToComputeSize The snapshots to compute the size of
   * @return A mapping of table to snapshot created from that table and the snapshot's size.
   */
  Multimap<TableName,SnapshotWithSize> computeSnapshotSizes(
      Multimap<TableName,String> snapshotsToComputeSize) throws IOException {
    Multimap<TableName,SnapshotWithSize> snapshotSizes = HashMultimap.create();
    for (Entry<TableName,Collection<String>> entry : snapshotsToComputeSize.asMap().entrySet()) {
      final TableName tn = entry.getKey();
      final List<String> snapshotNames = new ArrayList<>(entry.getValue());
      // Sort the snapshots so we process them in lexicographic order. This ensures that multiple
      // invocations of this Chore do not more the size ownership of some files between snapshots
      // that reference the file (prevents size ownership from moving between snapshots).
      Collections.sort(snapshotNames);
      final Path rootDir = FSUtils.getRootDir(conf);
      // Get the map of store file names to store file path for this table
      // TODO is the store-file name unique enough? Does this need to be region+family+storefile?
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
      for (String snapshotName : snapshotNames) {
        final long start = System.nanoTime();
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
        snapshotSizes.put(tn, new SnapshotWithSize(snapshotName, size));

        // Make sure that we don't double-count the same file
        for (StoreFileReference ref : unreferencedStoreFileNames) {
          for (String fileName : ref.getFamilyToFilesMapping().values()) {
            snapshotReferencedFiles.add(fileName);
          }
        }
        // Update the amount of time it took to compute the snapshot's size
        if (null != metrics) {
          metrics.incrementSnapshotSizeComputationTime((System.nanoTime() - start) / 1_000_000);
        }
      }
    }
    return snapshotSizes;
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
          HRegionInfo.convert(rm.getRegionInfo()).getEncodedName());

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
   * Calculates the directory in HDFS for a table based on the configuration.
   */
  Path getTableDir(TableName tn) throws IOException {
    Path rootDir = FSUtils.getRootDir(conf);
    return FSUtils.getTableDir(rootDir, tn);
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
   * Writes the snapshot sizes to the {@code hbase:quota} table.
   *
   * @param snapshotsWithSize The snapshot sizes to write.
   */
  void persistSnapshotSizes(
      Multimap<TableName,SnapshotWithSize> snapshotsWithSize) throws IOException {
    try (Table quotaTable = conn.getTable(QuotaTableUtil.QUOTA_TABLE_NAME)) {
      // Write each snapshot size for the table
      persistSnapshotSizes(quotaTable, snapshotsWithSize);
      // Write a size entry for all snapshots in a namespace
      persistSnapshotSizesByNS(quotaTable, snapshotsWithSize);
    }
  }

  /**
   * Writes the snapshot sizes to the provided {@code table}.
   */
  void persistSnapshotSizes(
      Table table, Multimap<TableName,SnapshotWithSize> snapshotsWithSize) throws IOException {
    // Convert each entry in the map to a Put and write them to the quota table
    table.put(snapshotsWithSize.entries()
        .stream()
        .map(e -> QuotaTableUtil.createPutForSnapshotSize(
            e.getKey(), e.getValue().getName(), e.getValue().getSize()))
        .collect(Collectors.toList()));
  }

  /**
   * Rolls up the snapshot sizes by namespace and writes a single record for each namespace
   * which is the size of all snapshots in that namespace.
   */
  void persistSnapshotSizesByNS(
      Table quotaTable, Multimap<TableName,SnapshotWithSize> snapshotsWithSize) throws IOException {
    Map<String,Long> namespaceSnapshotSizes = groupSnapshotSizesByNamespace(snapshotsWithSize);
    quotaTable.put(namespaceSnapshotSizes.entrySet().stream()
        .map(e -> QuotaTableUtil.createPutForNamespaceSnapshotSize(
            e.getKey(), e.getValue()))
        .collect(Collectors.toList()));
  }

  /**
   * Sums the snapshot sizes for each namespace.
   */
  Map<String,Long> groupSnapshotSizesByNamespace(
      Multimap<TableName,SnapshotWithSize> snapshotsWithSize) {
    return snapshotsWithSize.entries().stream()
        .collect(Collectors.groupingBy(
            // Convert TableName into the namespace string
            (e) -> e.getKey().getNamespaceAsString(),
            // Sum the values for namespace
            Collectors.mapping(
                Map.Entry::getValue, Collectors.summingLong((sws) -> sws.getSize()))));
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

  /**
   * Extracts the period for the chore from the configuration.
   *
   * @param conf The configuration object.
   * @return The configured chore period or the default value.
   */
  static int getPeriod(Configuration conf) {
    return conf.getInt(SNAPSHOT_QUOTA_CHORE_PERIOD_KEY,
        SNAPSHOT_QUOTA_CHORE_PERIOD_DEFAULT);
  }

  /**
   * Extracts the initial delay for the chore from the configuration.
   *
   * @param conf The configuration object.
   * @return The configured chore initial delay or the default value.
   */
  static long getInitialDelay(Configuration conf) {
    return conf.getLong(SNAPSHOT_QUOTA_CHORE_DELAY_KEY,
        SNAPSHOT_QUOTA_CHORE_DELAY_DEFAULT);
  }

  /**
   * Extracts the time unit for the chore period and initial delay from the configuration. The
   * configuration value for {@link #SNAPSHOT_QUOTA_CHORE_TIMEUNIT_KEY} must correspond to
   * a {@link TimeUnit} value.
   *
   * @param conf The configuration object.
   * @return The configured time unit for the chore period and initial delay or the default value.
   */
  static TimeUnit getTimeUnit(Configuration conf) {
    return TimeUnit.valueOf(conf.get(SNAPSHOT_QUOTA_CHORE_TIMEUNIT_KEY,
        SNAPSHOT_QUOTA_CHORE_TIMEUNIT_DEFAULT));
  }
}
