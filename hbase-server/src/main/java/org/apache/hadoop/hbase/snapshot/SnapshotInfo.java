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
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.HLogLink;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.FSTableDescriptors;

/**
 * Tool for dumping snapshot information.
 * <ol>
 * <li> Table Descriptor
 * <li> Snapshot creation time, type, format version, ...
 * <li> List of hfiles and hlogs
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
      private final boolean inArchive;
      private final long size;

      FileInfo(final boolean inArchive, final long size) {
        this.inArchive = inArchive;
        this.size = size;
      }

      /** @return true if the file is in the archive */
      public boolean inArchive() {
        return this.inArchive;
      }

      /** @return true if the file is missing */
      public boolean isMissing() {
        return this.size < 0;
      }

      /** @return the file size */
      public long getSize() {
        return this.size;
      }
    }

    private int hfileArchiveCount = 0;
    private int hfilesMissing = 0;
    private int hfilesCount = 0;
    private int logsMissing = 0;
    private int logsCount = 0;
    private long hfileArchiveSize = 0;
    private long hfileSize = 0;
    private long logSize = 0;

    private final SnapshotDescription snapshot;
    private final TableName snapshotTable;
    private final Configuration conf;
    private final FileSystem fs;

    SnapshotStats(final Configuration conf, final FileSystem fs, final SnapshotDescription snapshot)
    {
      this.snapshot = snapshot;
      this.snapshotTable = TableName.valueOf(snapshot.getTable());
      this.conf = conf;
      this.fs = fs;
    }

    /** @return the snapshot descriptor */
    public SnapshotDescription getSnapshotDescription() {
      return this.snapshot;
    }

    /** @return true if the snapshot is corrupted */
    public boolean isSnapshotCorrupted() {
      return hfilesMissing > 0 || logsMissing > 0;
    }

    /** @return the number of available store files */
    public int getStoreFilesCount() {
      return hfilesCount + hfileArchiveCount;
    }

    /** @return the number of available store files in the archive */
    public int getArchivedStoreFilesCount() {
      return hfileArchiveCount;
    }

    /** @return the number of available log files */
    public int getLogsCount() {
      return logsCount;
    }

    /** @return the number of missing store files */
    public int getMissingStoreFilesCount() {
      return hfilesMissing;
    }

    /** @return the number of missing log files */
    public int getMissingLogsCount() {
      return logsMissing;
    }

    /** @return the total size of the store files referenced by the snapshot */
    public long getStoreFilesSize() {
      return hfileSize + hfileArchiveSize;
    }

    /** @return the total size of the store files shared */
    public long getSharedStoreFilesSize() {
      return hfileSize;
    }

    /** @return the total size of the store files in the archive */
    public long getArchivedStoreFileSize() {
      return hfileArchiveSize;
    }

    /** @return the percentage of the shared store files */
    public float getSharedStoreFilePercentage() {
      return ((float)hfileSize / (hfileSize + hfileArchiveSize)) * 100;
    }

    /** @return the total log size */
    public long getLogsSize() {
      return logSize;
    }

    /**
     * Add the specified store file to the stats
     * @param region region encoded Name
     * @param family family name
     * @param hfile store file name
     * @return the store file information
     */
    FileInfo addStoreFile(final String region, final String family, final String hfile)
          throws IOException {
      TableName table = snapshotTable;
      HFileLink link = HFileLink.create(conf, table, region, family, hfile);
      boolean inArchive = false;
      long size = -1;
      try {
        if ((inArchive = fs.exists(link.getArchivePath()))) {
          size = fs.getFileStatus(link.getArchivePath()).getLen();
          hfileArchiveSize += size;
          hfileArchiveCount++;
        } else {
          size = link.getFileStatus(fs).getLen();
          hfileSize += size;
          hfilesCount++;
        }
      } catch (FileNotFoundException e) {
        hfilesMissing++;
      }
      return new FileInfo(inArchive, size);
    }

    /**
     * Add the specified recovered.edits file to the stats
     * @param region region encoded name
     * @param logfile log file name
     * @return the recovered.edits information
     */
    FileInfo addRecoveredEdits(final String region, final String logfile) throws IOException {
      Path rootDir = FSUtils.getRootDir(conf);
      Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshot, rootDir);
      Path path = SnapshotReferenceUtil.getRecoveredEdits(snapshotDir, region, logfile);
      long size = fs.getFileStatus(path).getLen();
      logSize += size;
      logsCount++;
      return new FileInfo(true, size);
    }

    /**
     * Add the specified log file to the stats
     * @param server server name
     * @param logfile log file name
     * @return the log information
     */
    FileInfo addLogFile(final String server, final String logfile) throws IOException {
      HLogLink logLink = new HLogLink(conf, server, logfile);
      long size = -1;
      try {
        size = logLink.getFileStatus(fs).getLen();
        logSize += size;
        logsCount++;
      } catch (FileNotFoundException e) {
        logsMissing++;
      }
      return new FileInfo(false, size);
    }
  }

  private FileSystem fs;
  private Path rootDir;

  private HTableDescriptor snapshotTableDesc;
  private SnapshotDescription snapshotDesc;
  private Path snapshotDir;

  @Override
  public int run(String[] args) throws IOException, InterruptedException {
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
        } else if (cmd.equals("-h") || cmd.equals("--help")) {
          printUsageAndExit();
        } else {
          System.err.println("UNEXPECTED: " + cmd);
          printUsageAndExit();
        }
      } catch (Exception e) {
        printUsageAndExit();
      }
    }

    if (snapshotName == null) {
      System.err.println("Missing snapshot name!");
      printUsageAndExit();
      return 1;
    }

    Configuration conf = getConf();
    fs = FileSystem.get(conf);
    rootDir = FSUtils.getRootDir(conf);

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
    snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
    if (!fs.exists(snapshotDir)) {
      LOG.warn("Snapshot '" + snapshotName + "' not found in: " + snapshotDir);
      return false;
    }

    snapshotDesc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
    snapshotTableDesc = FSTableDescriptors.getTableDescriptorFromFs(fs, snapshotDir);
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
    System.out.println("  Table: " + snapshotTableDesc.getTableName().getNameAsString());
    System.out.println(" Format: " + snapshotDesc.getVersion());
    System.out.println("Created: " + df.format(new Date(snapshotDesc.getCreationTime())));
    System.out.println();
  }

  /**
   * Dump the {@link HTableDescriptor}
   */
  private void printSchema() {
    System.out.println("Table Descriptor");
    System.out.println("----------------------------------------");
    System.out.println(snapshotTableDesc.toString());
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
    final String table = snapshotTableDesc.getTableName().getNameAsString();
    final SnapshotStats stats = new SnapshotStats(this.getConf(), this.fs, this.snapshotDesc);
    SnapshotReferenceUtil.visitReferencedFiles(fs, snapshotDir,
      new SnapshotReferenceUtil.FileVisitor() {
        public void storeFile (final String region, final String family, final String hfile)
            throws IOException {
          SnapshotStats.FileInfo info = stats.addStoreFile(region, family, hfile);

          if (showFiles) {
            System.out.printf("%8s %s/%s/%s/%s %s%n",
              (info.isMissing() ? "-" : StringUtils.humanReadableInt(info.getSize())),
              table, region, family, hfile,
              (info.inArchive() ? "(archive)" : info.isMissing() ? "(NOT FOUND)" : ""));
          }
        }

        public void recoveredEdits (final String region, final String logfile)
            throws IOException {
          SnapshotStats.FileInfo info = stats.addRecoveredEdits(region, logfile);

          if (showFiles) {
            System.out.printf("%8s recovered.edits %s on region %s%n",
              StringUtils.humanReadableInt(info.getSize()), logfile, region);
          }
        }

        public void logFile (final String server, final String logfile)
            throws IOException {
          SnapshotStats.FileInfo info = stats.addLogFile(server, logfile);

          if (showFiles) {
            System.out.printf("%8s log %s on server %s %s%n",
              (info.isMissing() ? "-" : StringUtils.humanReadableInt(info.getSize())),
              logfile, server,
              (info.isMissing() ? "(NOT FOUND)" : ""));
          }
        }
    });

    // Dump the stats
    System.out.println();
    if (stats.isSnapshotCorrupted()) {
      System.out.println("**************************************************************");
      System.out.printf("BAD SNAPSHOT: %d hfile(s) and %d log(s) missing.%n",
        stats.getMissingStoreFilesCount(), stats.getMissingLogsCount());
      System.out.println("**************************************************************");
    }

    if (showStats) {
      System.out.printf("%d HFiles (%d in archive), total size %s (%.2f%% %s shared with the source table)%n",
        stats.getStoreFilesCount(), stats.getArchivedStoreFilesCount(),
        StringUtils.humanReadableInt(stats.getStoreFilesSize()),
        stats.getSharedStoreFilePercentage(),
        StringUtils.humanReadableInt(stats.getSharedStoreFilesSize())
      );
      System.out.printf("%d Logs, total size %s%n",
        stats.getLogsCount(), StringUtils.humanReadableInt(stats.getLogsSize()));
      System.out.println();
    }
  }

  private void printUsageAndExit() {
    System.err.printf("Usage: bin/hbase %s [options]%n", getClass().getName());
    System.err.println(" where [options] are:");
    System.err.println("  -h|-help                Show this help and exit.");
    System.err.println("  -snapshot NAME          Snapshot to examine.");
    System.err.println("  -files                  Files and logs list.");
    System.err.println("  -stats                  Files and logs stats.");
    System.err.println("  -schema                 Describe the snapshotted table.");
    System.err.println();
    System.err.println("Examples:");
    System.err.println("  hbase " + getClass() + " \\");
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
    Path rootDir = FSUtils.getRootDir(conf);
    FileSystem fs = FileSystem.get(conf);
    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshot, rootDir);
    final SnapshotStats stats = new SnapshotStats(conf, fs, snapshot);
    SnapshotReferenceUtil.visitReferencedFiles(fs, snapshotDir,
      new SnapshotReferenceUtil.FileVisitor() {
        public void storeFile (final String region, final String family, final String hfile)
            throws IOException {
          stats.addStoreFile(region, family, hfile);
        }

        public void recoveredEdits (final String region, final String logfile) throws IOException {
          stats.addRecoveredEdits(region, logfile);
        }

        public void logFile (final String server, final String logfile) throws IOException {
          stats.addLogFile(server, logfile);
        }
    });
    return stats;
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
