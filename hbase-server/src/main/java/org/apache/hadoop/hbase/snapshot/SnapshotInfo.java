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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.WALLink;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hbase.thirdparty.org.apache.commons.cli.AlreadySelectedException;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLineParser;
import org.apache.hbase.thirdparty.org.apache.commons.cli.DefaultParser;
import org.apache.hbase.thirdparty.org.apache.commons.cli.MissingOptionException;
import org.apache.hbase.thirdparty.org.apache.commons.cli.Options;
import org.apache.hbase.thirdparty.org.apache.commons.cli.ParseException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.hbase.thirdparty.org.apache.commons.cli.Option;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;

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
public final class SnapshotInfo extends AbstractHBaseTool {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotInfo.class);

  static final class Options {
    static final Option SNAPSHOT = new Option(null, "snapshot", true,
      "The name of the snapshot to be detailed.");
    static final Option REMOTE_DIR = new Option(null, "remote-dir", true,
        "A custom root directory where snapshots are stored. "
          + "Use it together with the --snapshot option.");
    static final Option LIST_SNAPSHOTS = new Option(null, "list-snapshots", false,
        "List all the available snapshots and exit.");
    static final Option FILES = new Option(null, "files", false,
      "The list of files retained by the specified snapshot. "
        + "Use it together with the --snapshot option.");
    static final Option STATS = new Option(null, "stats", false,
      "Additional information about the specified snapshot. "
        + "Use it together with the --snapshot option.");
    static final Option SCHEMA = new Option(null, "schema", false,
        "Show the descriptor of the table for the specified snapshot. "
          + "Use it together with the --snapshot option.");
    static final Option SIZE_IN_BYTES = new Option(null, "size-in-bytes", false,
        "Print the size of the files in bytes. "
          + "Use it together with the --snapshot and --files options.");
  }

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

    private final SnapshotProtos.SnapshotDescription snapshot;
    private final TableName snapshotTable;
    private final Configuration conf;
    private final FileSystem fs;

    SnapshotStats(final Configuration conf, final FileSystem fs,
        final SnapshotDescription snapshot)
    {
      this.snapshot = ProtobufUtil.createHBaseProtosSnapshotDesc(snapshot);
      this.snapshotTable = snapshot.getTableName();
      this.conf = conf;
      this.fs = fs;
    }

    SnapshotStats(final Configuration conf, final FileSystem fs,
        final SnapshotProtos.SnapshotDescription snapshot) {
      this.snapshot = snapshot;
      this.snapshotTable = TableName.valueOf(snapshot.getTable());
      this.conf = conf;
      this.fs = fs;
    }


    /** @return the snapshot descriptor */
    public SnapshotDescription getSnapshotDescription() {
      return ProtobufUtil.createSnapshotDesc(this.snapshot);
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
     *  {@link #getSnapshotStats(Configuration, SnapshotProtos.SnapshotDescription, Map)}
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
          if (CommonFSUtils.listStatus(fs, backRefDir) == null) {
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
    FileInfo addStoreFile(final RegionInfo region, final String family,
        final SnapshotRegionManifest.StoreFile storeFile,
        final Map<Path, Integer> filesMap) throws IOException {
      HFileLink link = HFileLink.build(conf, snapshotTable, region.getEncodedName(),
              family, storeFile.getName());
      boolean isCorrupted = false;
      boolean inArchive = false;
      long size = -1;
      try {
        if (fs.exists(link.getArchivePath())) {
          inArchive = true;
          size = fs.getFileStatus(link.getArchivePath()).getLen();
          hfilesArchiveSize.addAndGet(size);
          hfilesArchiveCount.incrementAndGet();

          // If store file is not shared with other snapshots and tables,
          // increase nonSharedHfilesArchiveSize
          if ((filesMap != null) &&
              !isArchivedFileStillReferenced(link.getArchivePath(), filesMap)) {
            nonSharedHfilesArchiveSize.addAndGet(size);
          }
        } else if (fs.exists(link.getMobPath())) {
          inArchive = true;
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
      WALLink logLink = new WALLink(conf, server, logfile);
      long size = -1;
      try {
        size = logLink.getFileStatus(fs).getLen();
        logSize.addAndGet(size);
        logsCount.incrementAndGet();
      } catch (FileNotFoundException e) {
        logsMissing.incrementAndGet();
      }
      return new FileInfo(false, size, false);
    }
  }

  private FileSystem fs;
  private Path rootDir;

  private SnapshotManifest snapshotManifest;

  private boolean listSnapshots = false;
  private String snapshotName;
  private Path remoteDir;
  private boolean showSchema = false;
  private boolean showFiles = false;
  private boolean showStats = false;
  private boolean printSizeInBytes = false;

  @Override
  public int doWork() throws IOException, InterruptedException {
    if (remoteDir != null) {
      URI defaultFs = remoteDir.getFileSystem(conf).getUri();
      CommonFSUtils.setFsDefault(conf, new Path(defaultFs));
      CommonFSUtils.setRootDir(conf, remoteDir);
    }

    // List Available Snapshots
    if (listSnapshots) {
      SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
      System.out.printf("%-20s | %-20s | %-20s | %s%n", "SNAPSHOT", "CREATION TIME", "TTL IN SEC",
              "TABLE NAME");
      for (SnapshotDescription desc: getSnapshotList(conf)) {
        System.out.printf("%-20s | %20s | %20s | %s%n", desc.getName(),
                df.format(new Date(desc.getCreationTime())), desc.getTtl(),
                desc.getTableNameAsString());
      }
      return 0;
    }

    rootDir = CommonFSUtils.getRootDir(conf);
    fs = FileSystem.get(rootDir.toUri(), conf);
    LOG.debug("fs=" + fs.getUri().toString() + " root=" + rootDir);

    // Load snapshot information
    if (!loadSnapshotInfo(snapshotName)) {
      System.err.println("Snapshot '" + snapshotName + "' not found!");
      return 1;
    }

    printInfo();
    if (showSchema) {
      printSchema();
    }
    printFiles(showFiles, showStats);

    return 0;
  }

  /**
   * Load snapshot info and table descriptor for the specified snapshot
   * @param snapshotName name of the snapshot to load
   * @return false if snapshot is not found
   */
  private boolean loadSnapshotInfo(final String snapshotName) throws IOException {
    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
    if (!fs.exists(snapshotDir)) {
      LOG.warn("Snapshot '" + snapshotName + "' not found in: " + snapshotDir);
      return false;
    }

    SnapshotProtos.SnapshotDescription snapshotDesc =
        SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
    snapshotManifest = SnapshotManifest.open(getConf(), fs, snapshotDir, snapshotDesc);
    return true;
  }

  /**
   * Dump the {@link SnapshotDescription}
   */
  private void printInfo() {
    SnapshotProtos.SnapshotDescription snapshotDesc = snapshotManifest.getSnapshotDescription();
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    System.out.println("Snapshot Info");
    System.out.println("----------------------------------------");
    System.out.println("   Name: " + snapshotDesc.getName());
    System.out.println("   Type: " + snapshotDesc.getType());
    System.out.println("  Table: " + snapshotDesc.getTable());
    System.out.println(" Format: " + snapshotDesc.getVersion());
    System.out.println("Created: " + df.format(new Date(snapshotDesc.getCreationTime())));
    System.out.println("    Ttl: " + snapshotDesc.getTtl());
    System.out.println("  Owner: " + snapshotDesc.getOwner());
    System.out.println();
  }

  /**
   * Dump the {@link org.apache.hadoop.hbase.client.TableDescriptor}
   */
  private void printSchema() {
    System.out.println("Table Descriptor");
    System.out.println("----------------------------------------");
    System.out.println(snapshotManifest.getTableDescriptor().toString());
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
    final SnapshotProtos.SnapshotDescription snapshotDesc = snapshotManifest.getSnapshotDescription();
    final String table = snapshotDesc.getTable();
    final SnapshotDescription desc = ProtobufUtil.createSnapshotDesc(snapshotDesc);
    final SnapshotStats stats = new SnapshotStats(this.getConf(), this.fs, desc);
    SnapshotReferenceUtil.concurrentVisitReferencedFiles(getConf(), fs, snapshotManifest,
        "SnapshotInfo",
      new SnapshotReferenceUtil.SnapshotVisitor() {
        @Override
        public void storeFile(final RegionInfo regionInfo, final String family,
            final SnapshotRegionManifest.StoreFile storeFile) throws IOException {
          if (storeFile.hasReference()) return;

          SnapshotStats.FileInfo info = stats.addStoreFile(regionInfo, family, storeFile, null);
          if (showFiles) {
            String state = info.getStateToString();
            System.out.printf("%8s %s/%s/%s/%s %s%n",
              (info.isMissing() ? "-" : fileSizeToString(info.getSize())),
              table, regionInfo.getEncodedName(), family, storeFile.getName(),
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

  @Override
  protected void addOptions() {
    addOption(Options.SNAPSHOT);
    addOption(Options.REMOTE_DIR);
    addOption(Options.LIST_SNAPSHOTS);
    addOption(Options.FILES);
    addOption(Options.STATS);
    addOption(Options.SCHEMA);
    addOption(Options.SIZE_IN_BYTES);
  }


  @Override
  protected CommandLineParser newParser() {
    // Commons-CLI lacks the capability to handle combinations of options, so we do it ourselves
    // Validate in parse() to get helpful error messages instead of exploding in processOptions()
    return new DefaultParser() {
      @Override
      public CommandLine parse(org.apache.hbase.thirdparty.org.apache.commons.cli.Options opts, String[] args, Properties props, boolean stop)
        throws ParseException {
        CommandLine cl = super.parse(opts, args, props, stop);
        if(!cmd.hasOption(Options.LIST_SNAPSHOTS.getLongOpt()) && !cmd.hasOption(Options.SNAPSHOT.getLongOpt())) {
          throw new ParseException("Missing required snapshot option!");
        }
        return cl;
      }
    };
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    snapshotName = cmd.getOptionValue(Options.SNAPSHOT.getLongOpt());
    showFiles = cmd.hasOption(Options.FILES.getLongOpt());
    showStats = cmd.hasOption(Options.FILES.getLongOpt())
        || cmd.hasOption(Options.STATS.getLongOpt());
    showSchema = cmd.hasOption(Options.SCHEMA.getLongOpt());
    listSnapshots = cmd.hasOption(Options.LIST_SNAPSHOTS.getLongOpt());
    printSizeInBytes = cmd.hasOption(Options.SIZE_IN_BYTES.getLongOpt());
    if (cmd.hasOption(Options.REMOTE_DIR.getLongOpt())) {
      remoteDir = new Path(cmd.getOptionValue(Options.REMOTE_DIR.getLongOpt()));
    }
  }

  @Override
  protected void printUsage() {
    printUsage("hbase snapshot info [options]", "Options:", "");
    System.err.println("Examples:");
    System.err.println("  hbase snapshot info --snapshot MySnapshot --files");
  }

  /**
   * Returns the snapshot stats
   * @param conf the {@link Configuration} to use
   * @param snapshot {@link SnapshotDescription} to get stats from
   * @return the snapshot stats
   */
  public static SnapshotStats getSnapshotStats(final Configuration conf,
      final SnapshotDescription snapshot) throws IOException {
    SnapshotProtos.SnapshotDescription snapshotDesc =
      ProtobufUtil.createHBaseProtosSnapshotDesc(snapshot);
    return getSnapshotStats(conf, snapshotDesc, null);
  }

  /**
   * Returns the snapshot stats
   * @param conf the {@link Configuration} to use
   * @param snapshotDesc  HBaseProtos.SnapshotDescription to get stats from
   * @param filesMap {@link Map} store files map for all snapshots, it may be null
   * @return the snapshot stats
   */
  public static SnapshotStats getSnapshotStats(final Configuration conf,
      final SnapshotProtos.SnapshotDescription snapshotDesc,
      final Map<Path, Integer> filesMap) throws IOException {
    Path rootDir = CommonFSUtils.getRootDir(conf);
    FileSystem fs = FileSystem.get(rootDir.toUri(), conf);
    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotDesc, rootDir);
    SnapshotManifest manifest = SnapshotManifest.open(conf, fs, snapshotDir, snapshotDesc);
    final SnapshotStats stats = new SnapshotStats(conf, fs, snapshotDesc);
    SnapshotReferenceUtil.concurrentVisitReferencedFiles(conf, fs, manifest,
        "SnapshotsStatsAggregation", new SnapshotReferenceUtil.SnapshotVisitor() {
          @Override
          public void storeFile(final RegionInfo regionInfo, final String family,
              final SnapshotRegionManifest.StoreFile storeFile) throws IOException {
            if (!storeFile.hasReference()) {
              stats.addStoreFile(regionInfo, family, storeFile, filesMap);
            }
          }});
    return stats;
  }

  /**
   * Returns the list of available snapshots in the specified location
   * @param conf the {@link Configuration} to use
   * @return the list of snapshots
   */
  public static List<SnapshotDescription> getSnapshotList(final Configuration conf)
      throws IOException {
    Path rootDir = CommonFSUtils.getRootDir(conf);
    FileSystem fs = FileSystem.get(rootDir.toUri(), conf);
    Path snapshotDir = SnapshotDescriptionUtils.getSnapshotsDir(rootDir);
    FileStatus[] snapshots = fs.listStatus(snapshotDir,
        new SnapshotDescriptionUtils.CompletedSnaphotDirectoriesFilter(fs));
    List<SnapshotDescription> snapshotLists = new ArrayList<>(snapshots.length);
    for (FileStatus snapshotDirStat: snapshots) {
      SnapshotProtos.SnapshotDescription snapshotDesc =
          SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDirStat.getPath());
      snapshotLists.add(ProtobufUtil.createSnapshotDesc(snapshotDesc));
    }
    return snapshotLists;
  }

  /**
   * Gets the store files map for snapshot
   * @param conf the {@link Configuration} to use
   * @param snapshot {@link SnapshotDescription} to get stats from
   * @param exec the {@link ExecutorService} to use
   * @param filesMap {@link Map} the map to put the mapping entries
   * @param uniqueHFilesArchiveSize {@link AtomicLong} the accumulated store file size in archive
   * @param uniqueHFilesSize {@link AtomicLong} the accumulated store file size shared
   * @param uniqueHFilesMobSize {@link AtomicLong} the accumulated mob store file size shared
   */
  private static void getSnapshotFilesMap(final Configuration conf,
      final SnapshotDescription snapshot, final ExecutorService exec,
      final ConcurrentHashMap<Path, Integer> filesMap,
      final AtomicLong uniqueHFilesArchiveSize, final AtomicLong uniqueHFilesSize,
      final AtomicLong uniqueHFilesMobSize) throws IOException {
    SnapshotProtos.SnapshotDescription snapshotDesc =
        ProtobufUtil.createHBaseProtosSnapshotDesc(snapshot);
    Path rootDir = CommonFSUtils.getRootDir(conf);
    final FileSystem fs = FileSystem.get(rootDir.toUri(), conf);

    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotDesc, rootDir);
    SnapshotManifest manifest = SnapshotManifest.open(conf, fs, snapshotDir, snapshotDesc);
    SnapshotReferenceUtil.concurrentVisitReferencedFiles(conf, fs, manifest, exec,
        new SnapshotReferenceUtil.SnapshotVisitor() {
          @Override public void storeFile(final RegionInfo regionInfo, final String family,
              final SnapshotRegionManifest.StoreFile storeFile) throws IOException {
            if (!storeFile.hasReference()) {
              HFileLink link = HFileLink.build(conf, snapshot.getTableName(),
                  regionInfo.getEncodedName(), family, storeFile.getName());
              long size;
              Integer count;
              Path p;
              AtomicLong al;
              int c = 0;

              if (fs.exists(link.getArchivePath())) {
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
    List<SnapshotDescription> snapshotList = getSnapshotList(conf);


    if (snapshotList.isEmpty()) {
      return Collections.emptyMap();
    }

    ConcurrentHashMap<Path, Integer> fileMap = new ConcurrentHashMap<>();

    ExecutorService exec = SnapshotManifest.createExecutor(conf, "SnapshotsFilesMapping");

    try {
      for (final SnapshotDescription snapshot : snapshotList) {
        getSnapshotFilesMap(conf, snapshot, exec, fileMap, uniqueHFilesArchiveSize,
            uniqueHFilesSize, uniqueHFilesMobSize);
      }
    } finally {
      exec.shutdown();
    }

    return fileMap;
  }


  public static void main(String[] args) {
    new SnapshotInfo().doStaticMain(args);
  }
}
