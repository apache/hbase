/**
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

import java.io.IOException;
import java.io.FileNotFoundException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.fs.MasterStorage;
import org.apache.hadoop.hbase.fs.StorageContext;
import org.apache.hadoop.hbase.fs.StorageIdentifier;
import org.apache.hadoop.hbase.fs.legacy.LegacyPathIdentifier;
import org.apache.hadoop.hbase.fs.legacy.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.fs.legacy.io.HFileLink;
import org.apache.hadoop.hbase.io.WALLink;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * Tool for dumping snapshot information.
 * <ol>
 * <li> Table Descriptor
 * <li> Snapshot creation time, type, format version, ...
 * <li> List of hfiles and wals
 * <li> Stats about hfiles and logs sizes, percentage of shared with the source table, ...
 * </ol>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class SnapshotInfo extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(SnapshotInfo.class);

  /**
   * Statistics about the snapshot
   * <ol>
   * <li> How many store files and logs are in the archive
   * <li> How many store files and logs are shared with the table
   * <li> Total store files and logs size and shared amount
   * </ol>
   */
  public static class SnapshotStats {
    /** Information about the file referenced by the snapshot */
    static class FileInfo {
      private final boolean corrupted;
      private final boolean inArchive;
      private final long size;

      FileInfo(final boolean inArchive, final long size, final boolean corrupted) {
        this.corrupted = corrupted;
        this.inArchive = inArchive;
        this.size = size;
      }

      /** @return true if the file is in the archive */
      public boolean inArchive() {
        return this.inArchive;
      }

      /** @return true if the file is corrupted */
      public boolean isCorrupted() {
        return this.corrupted;
      }

      /** @return true if the file is missing */
      public boolean isMissing() {
        return this.size < 0;
      }

      /** @return the file size */
      public long getSize() {
        return this.size;
      }

      String getStateToString() {
        if (isCorrupted()) return "CORRUPTED";
        if (isMissing()) return "NOT FOUND";
        if (inArchive()) return "archive";
        return null;
      }
    }

    private AtomicInteger hfilesArchiveCount = new AtomicInteger();
    private AtomicInteger hfilesCorrupted = new AtomicInteger();
    private AtomicInteger hfilesMissing = new AtomicInteger();
    private AtomicInteger hfilesCount = new AtomicInteger();
    private AtomicInteger hfilesMobCount = new AtomicInteger();
    private AtomicInteger logsMissing = new AtomicInteger();
    private AtomicInteger logsCount = new AtomicInteger();
    private AtomicLong hfilesArchiveSize = new AtomicLong();
    private AtomicLong hfilesSize = new AtomicLong();
    private AtomicLong hfilesMobSize = new AtomicLong();
    private AtomicLong nonSharedHfilesArchiveSize = new AtomicLong();
    private AtomicLong logSize = new AtomicLong();

    private final HBaseProtos.SnapshotDescription snapshot;
    private final TableName snapshotTable;
    private final MasterStorage<? extends StorageIdentifier> masterStorage;

    SnapshotStats(final MasterStorage<? extends StorageIdentifier> masterStorage,
        final SnapshotDescription snapshot)
    {
      this.snapshot = ProtobufUtil.createHBaseProtosSnapshotDesc(snapshot);
      this.snapshotTable = TableName.valueOf(snapshot.getTable());
      this.masterStorage = masterStorage;
    }

    SnapshotStats(final MasterStorage<? extends StorageIdentifier> masterStorage,
        final HBaseProtos.SnapshotDescription snapshot) {
      this.snapshot = snapshot;
      this.snapshotTable = TableName.valueOf(snapshot.getTable());
      this.masterStorage = masterStorage;
    }


    /** @return the snapshot descriptor */
    public SnapshotDescription getSnapshotDescription() {
      return new SnapshotDescription(this.snapshot.getName(), this.snapshot.getTable(),
          ProtobufUtil.createSnapshotType(this.snapshot.getType()), this.snapshot.getOwner(),
          this.snapshot.getCreationTime(), this.snapshot.getVersion());
    }

    /** @return true if the snapshot is corrupted */
    public boolean isSnapshotCorrupted() {
      return hfilesMissing.get() > 0 ||
             logsMissing.get() > 0 ||
             hfilesCorrupted.get() > 0;
    }

    /** @return the number of available store files */
    public int getStoreFilesCount() {
      return hfilesCount.get() + hfilesArchiveCount.get() + hfilesMobCount.get();
    }

    /** @return the number of available store files in the archive */
    public int getArchivedStoreFilesCount() {
      return hfilesArchiveCount.get();
    }

    /** @return the number of available store files in the mob dir */
    public int getMobStoreFilesCount() { return hfilesMobCount.get(); }

    /** @return the number of available log files */
    public int getLogsCount() {
      return logsCount.get();
    }

    /** @return the number of missing store files */
    public int getMissingStoreFilesCount() {
      return hfilesMissing.get();
    }

    /** @return the number of corrupted store files */
    public int getCorruptedStoreFilesCount() {
      return hfilesCorrupted.get();
    }

    /** @return the number of missing log files */
    public int getMissingLogsCount() {
      return logsMissing.get();
    }

    /** @return the total size of the store files referenced by the snapshot */
    public long getStoreFilesSize() {
      return hfilesSize.get() + hfilesArchiveSize.get() + hfilesMobSize.get();
    }

    /** @return the total size of the store files shared */
    public long getSharedStoreFilesSize() {
      return hfilesSize.get();
    }

    /** @return the total size of the store files in the archive */
    public long getArchivedStoreFileSize() {
      return hfilesArchiveSize.get();
    }

    /** @return the total size of the store files in the mob store*/
    public long getMobStoreFilesSize() { return hfilesMobSize.get(); }

    /** @return the total size of the store files in the archive which is not shared
     *    with other snapshots and tables
     *
     *    This is only calculated when
     *    {@link #getSnapshotStats(Configuration, HBaseProtos.SnapshotDescription, Map)}
     *    is called with a non-null Map
     */
    public long getNonSharedArchivedStoreFilesSize() {
      return nonSharedHfilesArchiveSize.get();
    }

    /** @return the percentage of the shared store files */
    public float getSharedStoreFilePercentage() {
      return ((float) hfilesSize.get() / (getStoreFilesSize())) * 100;
    }

    /** @return the percentage of the mob store files */
    public float getMobStoreFilePercentage() {
      return ((float) hfilesMobSize.get() / (getStoreFilesSize())) * 100;
    }

    /** @return the total log size */
    public long getLogsSize() {
      return logSize.get();
    }

    /** Check if for a give file in archive, if there are other snapshots/tables still
     * reference it.
     * @param filePath file path in archive
     * @param snapshotFilesMap a map for store files in snapshots about how many snapshots refer
     *                         to it.
     * @return true or false
     */
    private boolean isArchivedFileStillReferenced(final Path filePath,
        final Map<Path, Integer> snapshotFilesMap) {

      Integer c = snapshotFilesMap.get(filePath);

      // Check if there are other snapshots or table from clone_snapshot() (via back-reference)
      // still reference to it.
      if ((c != null) && (c == 1)) {
        Path parentDir = filePath.getParent();
        Path backRefDir = HFileLink.getBackReferencesDir(parentDir, filePath.getName());
        try {
          if (FSUtils.listStatus(masterStorage.getFileSystem(), backRefDir) == null) {
            return false;
          }
        } catch (IOException e) {
          // For the purpose of this function, IOException is ignored and treated as
          // the file is still being referenced.
        }
      }
      return true;
    }

    /**
     * Add the specified store file to the stats
     * @param region region encoded Name
     * @param family family name
     * @param storeFile store file name
     * @param filesMap store files map for all snapshots, it may be null
     * @return the store file information
     */
    FileInfo addStoreFile(final HRegionInfo region, final String family,
        final SnapshotRegionManifest.StoreFile storeFile,
        final Map<Path, Integer> filesMap) throws IOException {
      Configuration conf = masterStorage.getConfiguration();
      FileSystem fs = masterStorage.getFileSystem();
      HFileLink link = HFileLink.build(conf, snapshotTable, region.getEncodedName(),
              family, storeFile.getName());
      boolean isCorrupted = false;
      boolean inArchive = false;
      long size = -1;
      try {
        if ((inArchive = fs.exists(link.getArchivePath()))) {
          size = fs.getFileStatus(link.getArchivePath()).getLen();
          hfilesArchiveSize.addAndGet(size);
          hfilesArchiveCount.incrementAndGet();

          // If store file is not shared with other snapshots and tables,
          // increase nonSharedHfilesArchiveSize
          if ((filesMap != null) &&
              !isArchivedFileStillReferenced(link.getArchivePath(), filesMap)) {
            nonSharedHfilesArchiveSize.addAndGet(size);
          }
        } else if (inArchive = fs.exists(link.getMobPath())) {
          size = fs.getFileStatus(link.getMobPath()).getLen();
          hfilesMobSize.addAndGet(size);
          hfilesMobCount.incrementAndGet();
        } else {
          size = link.getFileStatus(fs).getLen();
          hfilesSize.addAndGet(size);
          hfilesCount.incrementAndGet();
        }
        isCorrupted = (storeFile.hasFileSize() && storeFile.getFileSize() != size);
        if (isCorrupted) hfilesCorrupted.incrementAndGet();
      } catch (FileNotFoundException e) {
        hfilesMissing.incrementAndGet();
      }
      return new FileInfo(inArchive, size, isCorrupted);
    }

    /**
     * Add the specified log file to the stats
     * @param server server name
     * @param logfile log file name
     * @return the log information
     */
    FileInfo addLogFile(final String server, final String logfile) throws IOException {
      WALLink logLink = new WALLink(masterStorage.getConfiguration(), server, logfile);
      long size = -1;
      try {
        size = logLink.getFileStatus(masterStorage.getFileSystem()).getLen();
        logSize.addAndGet(size);
        logsCount.incrementAndGet();
      } catch (FileNotFoundException e) {
        logsMissing.incrementAndGet();
      }
      return new FileInfo(false, size, false);
    }
  }

  private boolean printSizeInBytes = false;
  private MasterStorage<? extends StorageIdentifier> masterStorage;

  private HBaseProtos.SnapshotDescription snapshotDesc;
  private HTableDescriptor snapshotTable;

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="REC_CATCH_EXCEPTION",
    justification="Intentional")
  public int run(String[] args) throws IOException, InterruptedException {
    final Configuration conf = getConf();
    boolean listSnapshots = false;
    String snapshotName = null;
    boolean showSchema = false;
    boolean showFiles = false;
    boolean showStats = false;

    // Process command line args
    for (int i = 0; i < args.length; i++) {
      String cmd = args[i];
      try {
        if (cmd.equals("-snapshot")) {
          snapshotName = args[++i];
        } else if (cmd.equals("-files")) {
          showFiles = true;
          showStats = true;
        } else if (cmd.equals("-stats")) {
          showStats = true;
        } else if (cmd.equals("-schema")) {
          showSchema = true;
        } else if (cmd.equals("-remote-dir")) {
          Path sourceDir = new Path(args[++i]);
          URI defaultFs = sourceDir.getFileSystem(conf).getUri();
          FSUtils.setFsDefault(conf, new Path(defaultFs));
          FSUtils.setRootDir(conf, sourceDir);
        } else if (cmd.equals("-list-snapshots")) {
          listSnapshots = true;
        } else if (cmd.equals("-size-in-bytes")) {
          printSizeInBytes = true;
        } else if (cmd.equals("-h") || cmd.equals("--help")) {
          printUsageAndExit();
        } else {
          System.err.println("UNEXPECTED: " + cmd);
          printUsageAndExit();
        }
      } catch (Exception e) {
        printUsageAndExit(); // FindBugs: REC_CATCH_EXCEPTION
      }
    }

    // instantiate MasterStorage
    masterStorage = MasterStorage.open(conf, false);

    // List Available Snapshots
    if (listSnapshots) {
      SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
      System.out.printf("%-20s | %-20s | %s%n", "SNAPSHOT", "CREATION TIME", "TABLE NAME");
      for (SnapshotDescription desc: getSnapshotList(masterStorage)) {
        System.out.printf("%-20s | %20s | %s%n",
                          desc.getName(),
                          df.format(new Date(desc.getCreationTime())),
                          desc.getTable());
      }
      return 0;
    }

    if (snapshotName == null) {
      System.err.println("Missing snapshot name!");
      printUsageAndExit();
      return 1;
    }

    LOG.debug("fs=" + masterStorage.getFileSystem().getUri().toString() + " root=" +
        ((LegacyPathIdentifier)masterStorage.getRootContainer()).path);

    // Load snapshot information
    if (!loadSnapshotInfo(snapshotName)) {
      System.err.println("Snapshot '" + snapshotName + "' not found!");
      return 1;
    }

    printInfo();
    if (showSchema) printSchema();
    printFiles(showFiles, showStats);

    return 0;
  }

  /**
   * Load snapshot info and table descriptor for the specified snapshot
   * @param snapshotName name of the snapshot to load
   * @return false if snapshot is not found
   */
  private boolean loadSnapshotInfo(final String snapshotName) throws IOException {
    snapshotDesc = masterStorage.getSnapshot(snapshotName);
    snapshotTable = masterStorage.getTableDescriptorForSnapshot(snapshotDesc);
    return true;
  }

  /**
   * Dump the {@link SnapshotDescription}
   */
  private void printInfo() {
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    System.out.println("Snapshot Info");
    System.out.println("----------------------------------------");
    System.out.println("   Name: " + snapshotDesc.getName());
    System.out.println("   Type: " + snapshotDesc.getType());
    System.out.println("  Table: " + snapshotDesc.getTable());
    System.out.println(" Format: " + snapshotDesc.getVersion());
    System.out.println("Created: " + df.format(new Date(snapshotDesc.getCreationTime())));
    System.out.println("  Owner: " + snapshotDesc.getOwner());
    System.out.println();
  }

  /**
   * Dump the {@link HTableDescriptor}
   */
  private void printSchema() {
    System.out.println("Table Descriptor");
    System.out.println("----------------------------------------");
    System.out.println(snapshotTable.toString());
    System.out.println();
  }

  /**
   * Collect the hfiles and logs statistics of the snapshot and
   * dump the file list if requested and the collected information.
   */
  private void printFiles(final boolean showFiles, final boolean showStats) throws IOException {
    if (showFiles) {
      System.out.println("Snapshot Files");
      System.out.println("----------------------------------------");
    }

    // Collect information about hfiles and logs in the snapshot
    final String table = snapshotDesc.getTable();
    SnapshotDescription desc = new SnapshotDescription(snapshotDesc.getName(),
        snapshotDesc.getTable(), ProtobufUtil.createSnapshotType(snapshotDesc.getType()),
        snapshotDesc.getOwner(), snapshotDesc.getCreationTime(), snapshotDesc.getVersion());
    final SnapshotStats stats = new SnapshotStats(masterStorage, desc);
    // TODO: add concurrent API to MasterStorage
    masterStorage.visitSnapshotStoreFiles(snapshotDesc, StorageContext.DATA,
        new MasterStorage.SnapshotStoreFileVisitor() {
          @Override
          public void visitSnapshotStoreFile(HBaseProtos.SnapshotDescription snapshot,
              StorageContext ctx, HRegionInfo hri, String familyName,
              SnapshotRegionManifest.StoreFile storeFile) throws IOException {
            if (storeFile.hasReference()) return;

            SnapshotStats.FileInfo info = stats.addStoreFile(hri, familyName, storeFile, null);
            if (showFiles) {
              String state = info.getStateToString();
              System.out.printf("%8s %s/%s/%s/%s %s%n",
                  (info.isMissing() ? "-" : fileSizeToString(info.getSize())),
                  table, hri.getEncodedName(), familyName, storeFile.getName(),
                  state == null ? "" : "(" + state + ")");
            }
          }
        });

    // Dump the stats
    System.out.println();
    if (stats.isSnapshotCorrupted()) {
      System.out.println("**************************************************************");
      System.out.printf("BAD SNAPSHOT: %d hfile(s) and %d log(s) missing.%n",
        stats.getMissingStoreFilesCount(), stats.getMissingLogsCount());
      System.out.printf("              %d hfile(s) corrupted.%n",
        stats.getCorruptedStoreFilesCount());
      System.out.println("**************************************************************");
    }

    if (showStats) {
      System.out.printf("%d HFiles (%d in archive, %d in mob storage), total size %s " +
              "(%.2f%% %s shared with the source table, %.2f%% %s in mob dir)%n",
        stats.getStoreFilesCount(), stats.getArchivedStoreFilesCount(),
        stats.getMobStoreFilesCount(),
        fileSizeToString(stats.getStoreFilesSize()),
        stats.getSharedStoreFilePercentage(),
        fileSizeToString(stats.getSharedStoreFilesSize()),
        stats.getMobStoreFilePercentage(),
        fileSizeToString(stats.getMobStoreFilesSize())
      );
      System.out.printf("%d Logs, total size %s%n",
        stats.getLogsCount(), fileSizeToString(stats.getLogsSize()));
      System.out.println();
    }
  }

  private String fileSizeToString(long size) {
    return printSizeInBytes ? Long.toString(size) : StringUtils.humanReadableInt(size);
  }

  private void printUsageAndExit() {
    System.err.printf("Usage: bin/hbase snapshot info [options]%n");
    System.err.println(" where [options] are:");
    System.err.println("  -h|-help                Show this help and exit.");
    System.err.println("  -remote-dir             Root directory that contains the snapshots.");
    System.err.println("  -list-snapshots         List all the available snapshots and exit.");
    System.err.println("  -size-in-bytes          Print the size of the files in bytes.");
    System.err.println("  -snapshot NAME          Snapshot to examine.");
    System.err.println("  -files                  Files and logs list.");
    System.err.println("  -stats                  Files and logs stats.");
    System.err.println("  -schema                 Describe the snapshotted table.");
    System.err.println();
    System.err.println("Examples:");
    System.err.println("  hbase snapshot info \\");
    System.err.println("    -snapshot MySnapshot -files");
    System.exit(1);
  }

  /**
   * Returns the snapshot stats
   * @param conf the {@link Configuration} to use
   * @param snapshot {@link SnapshotDescription} to get stats from
   * @return the snapshot stats
   */
  public static SnapshotStats getSnapshotStats(final Configuration conf,
      final SnapshotDescription snapshot) throws IOException {
    HBaseProtos.SnapshotDescription snapshotDesc = ProtobufUtil.createHBaseProtosSnapshotDesc(
        snapshot);

    MasterStorage<? extends StorageIdentifier> masterStorage = MasterStorage.open(conf, false);
    return getSnapshotStats(masterStorage.getConfiguration(), snapshotDesc, null);
  }

  /**
   * Returns the snapshot stats
   * @param conf the {@link Configuration} to use
   * @param snapshotDesc  HBaseProtos.SnapshotDescription to get stats from
   * @param filesMap {@link Map} store files map for all snapshots, it may be null
   * @return the snapshot stats
   */
  public static SnapshotStats getSnapshotStats(
      final Configuration conf, final HBaseProtos.SnapshotDescription snapshotDesc,
      final Map<Path, Integer> filesMap) throws IOException {
    MasterStorage<? extends StorageIdentifier> masterStorage = MasterStorage.open(conf, false);
    final SnapshotStats stats = new SnapshotStats(masterStorage, snapshotDesc);
    masterStorage.visitSnapshotStoreFiles(snapshotDesc, StorageContext.DATA,
        new MasterStorage.SnapshotStoreFileVisitor() {
          @Override
          public void visitSnapshotStoreFile(HBaseProtos.SnapshotDescription snapshot,
              StorageContext ctx, HRegionInfo hri, String familyName,
              SnapshotRegionManifest.StoreFile storeFile) throws IOException {
            if (!storeFile.hasReference()) {
              stats.addStoreFile(hri, familyName, storeFile, filesMap);
            }
          }
        });
    return stats;
  }

  /**
   * Returns the list of available snapshots in the specified location
   * @param masterStorage the {@link MasterStorage} to use
   * @return the list of snapshots
   */
  public static List<SnapshotDescription> getSnapshotList(
      final MasterStorage<? extends StorageIdentifier> masterStorage) throws IOException {
    List<HBaseProtos.SnapshotDescription> descriptions = masterStorage.getSnapshots();
    List<SnapshotDescription> snapshotLists = new ArrayList(descriptions.size());
    for (HBaseProtos.SnapshotDescription desc: masterStorage.getSnapshots()) {
      snapshotLists.add(ProtobufUtil.createSnapshotDesc(desc));
    }
    return snapshotLists;
  }

  /**
   * Gets the store files map for snapshot
   * @param masterStorage the {@link MasterStorage} to use
   * @param snapshot {@link SnapshotDescription} to get stats from
   * @param exec the {@link ExecutorService} to use
   * @param filesMap {@link Map} the map to put the mapping entries
   * @param uniqueHFilesArchiveSize {@link AtomicLong} the accumulated store file size in archive
   * @param uniqueHFilesSize {@link AtomicLong} the accumulated store file size shared
   * @param uniqueHFilesMobSize {@link AtomicLong} the accumulated mob store file size shared
   * @return the snapshot stats
   */
  private static void getSnapshotFilesMap(
      final MasterStorage<? extends StorageIdentifier> masterStorage,
      final SnapshotDescription snapshot, final ExecutorService exec,
      final ConcurrentHashMap<Path, Integer> filesMap,
      final AtomicLong uniqueHFilesArchiveSize, final AtomicLong uniqueHFilesSize,
      final AtomicLong uniqueHFilesMobSize) throws IOException {
    HBaseProtos.SnapshotDescription snapshotDesc = ProtobufUtil.createHBaseProtosSnapshotDesc(
        snapshot);
    masterStorage.visitSnapshotStoreFiles(snapshotDesc, StorageContext.DATA,
        new MasterStorage.SnapshotStoreFileVisitor() {
          @Override
          public void visitSnapshotStoreFile(HBaseProtos.SnapshotDescription snapshot,
              StorageContext ctx, HRegionInfo hri, String familyName,
              SnapshotRegionManifest.StoreFile storeFile) throws IOException {
            Configuration conf = masterStorage.getConfiguration();
            FileSystem fs = masterStorage.getFileSystem();
            if (!storeFile.hasReference()) {
              HFileLink link = HFileLink.build(conf, TableName.valueOf(snapshot.getTable()),
                  hri.getEncodedName(), familyName, storeFile.getName());
              long size;
              Integer count;
              Path p;
              AtomicLong al;
              int c = 0;

              if (masterStorage.getFileSystem().exists(link.getArchivePath())) {
                p = link.getArchivePath();
                al = uniqueHFilesArchiveSize;
                size = fs.getFileStatus(p).getLen();
              } else if (fs.exists(link.getMobPath())) {
                p = link.getMobPath();
                al = uniqueHFilesMobSize;
                size = fs.getFileStatus(p).getLen();
              } else {
                p = link.getOriginPath();
                al = uniqueHFilesSize;
                size = link.getFileStatus(fs).getLen();
              }

              // If it has been counted, do not double count
              count = filesMap.get(p);
              if (count != null) {
                c = count.intValue();
              } else {
                al.addAndGet(size);
              }

              filesMap.put(p, ++c);
            }
          }
    });
  }

  /**
   * Returns the map of store files based on path for all snapshots
   * @param conf the {@link Configuration} to use
   * @param uniqueHFilesArchiveSize pass out the size for store files in archive
   * @param uniqueHFilesSize pass out the size for store files shared
   * @param uniqueHFilesMobSize pass out the size for mob store files shared
   * @return the map of store files
   */
  public static Map<Path, Integer> getSnapshotsFilesMap(final Configuration conf,
      AtomicLong uniqueHFilesArchiveSize, AtomicLong uniqueHFilesSize,
      AtomicLong uniqueHFilesMobSize) throws IOException {
    MasterStorage<? extends StorageIdentifier> masterStorage = MasterStorage.open(conf, false);
    List<SnapshotDescription> snapshotList = getSnapshotList(masterStorage);


    if (snapshotList.size() == 0) {
      return Collections.emptyMap();
    }

    ConcurrentHashMap<Path, Integer> fileMap = new ConcurrentHashMap<>();

    ExecutorService exec = SnapshotManifest.createExecutor(conf, "SnapshotsFilesMapping");

    try {
      for (final SnapshotDescription snapshot : snapshotList) {
        getSnapshotFilesMap(masterStorage, snapshot, exec, fileMap, uniqueHFilesArchiveSize,
            uniqueHFilesSize, uniqueHFilesMobSize);
      }
    } finally {
      exec.shutdown();
    }

    return fileMap;
  }

  /**
   * The guts of the {@link #main} method.
   * Call this method to avoid the {@link #main(String[])} System.exit.
   * @param args
   * @return errCode
   * @throws Exception
   */
  static int innerMain(final String [] args) throws Exception {
    return ToolRunner.run(HBaseConfiguration.create(), new SnapshotInfo(), args);
  }

  public static void main(String[] args) throws Exception {
     System.exit(innerMain(args));
  }
}
