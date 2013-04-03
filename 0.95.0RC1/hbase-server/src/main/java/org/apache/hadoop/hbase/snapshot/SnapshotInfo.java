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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.HLogLink;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil;
import org.apache.hadoop.hbase.util.Bytes;
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
    if (showFiles || showStats) printFiles(showFiles);

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
    snapshotTableDesc = FSTableDescriptors.getTableDescriptor(fs, snapshotDir);
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
  private void printFiles(final boolean showFiles) throws IOException {
    final String table = snapshotDesc.getTable();
    final Configuration conf = getConf();

    if (showFiles) {
      System.out.println("Snapshot Files");
      System.out.println("----------------------------------------");
    }

    // Collect information about hfiles and logs in the snapshot
    final AtomicInteger hfileArchiveCount = new AtomicInteger();
    final AtomicInteger hfilesMissing = new AtomicInteger();
    final AtomicInteger hfilesCount = new AtomicInteger();
    final AtomicInteger logsMissing = new AtomicInteger();
    final AtomicInteger logsCount = new AtomicInteger();
    final AtomicLong hfileArchiveSize = new AtomicLong();
    final AtomicLong hfileSize = new AtomicLong();
    final AtomicLong logSize = new AtomicLong();
    SnapshotReferenceUtil.visitReferencedFiles(fs, snapshotDir,
      new SnapshotReferenceUtil.FileVisitor() {
        public void storeFile (final String region, final String family, final String hfile)
            throws IOException {
          Path path = new Path(family, HFileLink.createHFileLinkName(table, region, hfile));
          HFileLink link = new HFileLink(conf, path);
          boolean inArchive = false;
          long size = -1;
          try {
            if ((inArchive = fs.exists(link.getArchivePath()))) {
              size = fs.getFileStatus(link.getArchivePath()).getLen();
              hfileArchiveSize.addAndGet(size);
              hfileArchiveCount.addAndGet(1);
            } else {
              size = link.getFileStatus(fs).getLen();
              hfileSize.addAndGet(size);
              hfilesCount.addAndGet(1);
            }
          } catch (FileNotFoundException e) {
            hfilesMissing.addAndGet(1);
          }

          if (showFiles) {
            System.out.printf("%8s %s/%s/%s/%s %s%n",
              (size < 0 ? "-" : StringUtils.humanReadableInt(size)),
              table, region, family, hfile,
              (inArchive ? "(archive)" : (size < 0) ? "(NOT FOUND)" : ""));
          }
        }

        public void recoveredEdits (final String region, final String logfile)
            throws IOException {
          Path path = SnapshotReferenceUtil.getRecoveredEdits(snapshotDir, region, logfile);
          long size = fs.getFileStatus(path).getLen();
          logSize.addAndGet(size);
          logsCount.addAndGet(1);

          if (showFiles) {
            System.out.printf("%8s recovered.edits %s on region %s%n",
              StringUtils.humanReadableInt(size), logfile, region);
          }
        }

        public void logFile (final String server, final String logfile)
            throws IOException {
          HLogLink logLink = new HLogLink(conf, server, logfile);
          long size = -1;
          try {
            size = logLink.getFileStatus(fs).getLen();
            logSize.addAndGet(size);
            logsCount.addAndGet(1);
          } catch (FileNotFoundException e) {
            logsMissing.addAndGet(1);
          }

          if (showFiles) {
            System.out.printf("%8s log %s on server %s %s%n",
              (size < 0 ? "-" : StringUtils.humanReadableInt(size)),
              logfile, server,
              (size < 0 ? "(NOT FOUND)" : ""));
          }
        }
    });

    // Dump the stats
    System.out.println();
    if (hfilesMissing.get() > 0 || logsMissing.get() > 0) {
      System.out.println("**************************************************************");
      System.out.printf("BAD SNAPSHOT: %d hfile(s) and %d log(s) missing.%n",
        hfilesMissing.get(), logsMissing.get());
      System.out.println("**************************************************************");
    }

    System.out.printf("%d HFiles (%d in archive), total size %s (%.2f%% %s shared with the source table)%n",
      hfilesCount.get() + hfileArchiveCount.get(), hfileArchiveCount.get(),
      StringUtils.humanReadableInt(hfileSize.get() + hfileArchiveSize.get()),
      ((float)hfileSize.get() / (hfileSize.get() + hfileArchiveSize.get())) * 100,
      StringUtils.humanReadableInt(hfileSize.get())
    );
    System.out.printf("%d Logs, total size %s%n",
      logsCount.get(), StringUtils.humanReadableInt(logSize.get()));
    System.out.println();
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
