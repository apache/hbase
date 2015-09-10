/**
 *
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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.fs.layout.FsLayout;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.wal.WALSplitter;

/**
 * Utility methods for interacting with the hbase.root file system.
 */
@InterfaceAudience.Private
public final class FSVisitor {
  private static final Log LOG = LogFactory.getLog(FSVisitor.class);

  public interface RegionVisitor {
    void region(final String region) throws IOException;
  }

  public interface StoreFileVisitor {
    void storeFile(final String region, final String family, final String hfileName)
       throws IOException;
  }

  public interface RecoveredEditsVisitor {
    void recoveredEdits (final String region, final String logfile)
      throws IOException;
  }

  public interface LogFileVisitor {
    void logFile (final String server, final String logfile)
      throws IOException;
  }

  private FSVisitor() {
    // private constructor for utility class
  }

  /**
   * Iterate over the table store files
   *
   * @param fs {@link FileSystem}
   * @param tableDir {@link Path} to the table directory
   * @param visitor callback object to get the store files
   * @throws IOException if an error occurred while scanning the directory
   */
  public static void visitRegions(final FileSystem fs, final Path tableDir,
      final RegionVisitor visitor) throws IOException {
    List<FileStatus> regions = FsLayout.getRegionDirFileStats(fs, tableDir, new FSUtils.RegionDirFilter(fs));
    if (regions == null) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("No regions under directory:" + tableDir);
      }
      return;
    }

    for (FileStatus region: regions) {
      visitor.region(region.getPath().getName());
    }
  }

  /**
   * Iterate over the table store files
   *
   * @param fs {@link FileSystem}
   * @param tableDir {@link Path} to the table directory
   * @param visitor callback object to get the store files
   * @throws IOException if an error occurred while scanning the directory
   */
  public static void visitTableStoreFiles(final FileSystem fs, final Path tableDir,
      final StoreFileVisitor visitor) throws IOException {
    List<FileStatus> regions = FsLayout.getRegionDirFileStats(fs, tableDir, new FSUtils.RegionDirFilter(fs));
    if (regions == null) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("No regions under directory:" + tableDir);
      }
      return;
    }

    for (FileStatus region: regions) {
      visitRegionStoreFiles(fs, region.getPath(), visitor);
    }
  }

  /**
   * Iterate over the region store files
   *
   * @param fs {@link FileSystem}
   * @param regionDir {@link Path} to the region directory
   * @param visitor callback object to get the store files
   * @throws IOException if an error occurred while scanning the directory
   */
  public static void visitRegionStoreFiles(final FileSystem fs, final Path regionDir,
      final StoreFileVisitor visitor) throws IOException {
    FileStatus[] families = FSUtils.listStatus(fs, regionDir, new FSUtils.FamilyDirFilter(fs));
    if (families == null) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("No families under region directory:" + regionDir);
      }
      return;
    }

    PathFilter fileFilter = new FSUtils.FileFilter(fs);
    for (FileStatus family: families) {
      Path familyDir = family.getPath();
      String familyName = familyDir.getName();

      // get all the storeFiles in the family
      FileStatus[] storeFiles = FSUtils.listStatus(fs, familyDir, fileFilter);
      if (storeFiles == null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("No hfiles found for family: " + familyDir + ", skipping.");
        }
        continue;
      }

      for (FileStatus hfile: storeFiles) {
        Path hfilePath = hfile.getPath();
        visitor.storeFile(regionDir.getName(), familyName, hfilePath.getName());
      }
    }
  }

  /**
   * Iterate over each region in the table and inform about recovered.edits
   *
   * @param fs {@link FileSystem}
   * @param tableDir {@link Path} to the table directory
   * @param visitor callback object to get the recovered.edits files
   * @throws IOException if an error occurred while scanning the directory
   */
  public static void visitTableRecoveredEdits(final FileSystem fs, final Path tableDir,
      final FSVisitor.RecoveredEditsVisitor visitor) throws IOException {
    List<FileStatus> regions = FsLayout.getRegionDirFileStats(fs, tableDir, new FSUtils.RegionDirFilter(fs));
    if (regions == null) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("No recoveredEdits regions under directory:" + tableDir);
      }
      return;
    }

    for (FileStatus region: regions) {
      visitRegionRecoveredEdits(fs, region.getPath(), visitor);
    }
  }

  /**
   * Iterate over recovered.edits of the specified region
   *
   * @param fs {@link FileSystem}
   * @param regionDir {@link Path} to the Region directory
   * @param visitor callback object to get the recovered.edits files
   * @throws IOException if an error occurred while scanning the directory
   */
  public static void visitRegionRecoveredEdits(final FileSystem fs, final Path regionDir,
      final FSVisitor.RecoveredEditsVisitor visitor) throws IOException {
    NavigableSet<Path> files = WALSplitter.getSplitEditFilesSorted(fs, regionDir);
    if (files == null || files.size() == 0) return;

    for (Path source: files) {
      // check to see if the file is zero length, in which case we can skip it
      FileStatus stat = fs.getFileStatus(source);
      if (stat.getLen() <= 0) continue;

      visitor.recoveredEdits(regionDir.getName(), source.getName());
    }
  }

  /**
   * Iterate over hbase log files
   *
   * @param fs {@link FileSystem}
   * @param rootDir {@link Path} to the HBase root folder
   * @param visitor callback object to get the log files
   * @throws IOException if an error occurred while scanning the directory
   */
  public static void visitLogFiles(final FileSystem fs, final Path rootDir,
      final LogFileVisitor visitor) throws IOException {
    Path logsDir = new Path(rootDir, HConstants.HREGION_LOGDIR_NAME);
    FileStatus[] logServerDirs = FSUtils.listStatus(fs, logsDir);
    if (logServerDirs == null) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("No logs under directory:" + logsDir);
      }
      return;
    }

    for (FileStatus serverLogs: logServerDirs) {
      String serverName = serverLogs.getPath().getName();

      FileStatus[] wals = FSUtils.listStatus(fs, serverLogs.getPath());
      if (wals == null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("No wals found for server: " + serverName + ", skipping.");
        }
        continue;
      }

      for (FileStatus walRef: wals) {
        visitor.logFile(serverName, walRef.getPath().getName());
      }
    }
  }
}
