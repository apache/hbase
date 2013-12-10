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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.wal.HLogUtil;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.FSVisitor;

/**
 * Utility methods for interacting with the snapshot referenced files.
 */
@InterfaceAudience.Private
public final class SnapshotReferenceUtil {
  public interface FileVisitor extends FSVisitor.StoreFileVisitor,
    FSVisitor.RecoveredEditsVisitor, FSVisitor.LogFileVisitor {
  }

  private SnapshotReferenceUtil() {
    // private constructor for utility class
  }

  /**
   * Get log directory for a server in a snapshot.
   *
   * @param snapshotDir directory where the specific snapshot is stored
   * @param serverName name of the parent regionserver for the log files
   * @return path to the log home directory for the archive files.
   */
  public static Path getLogsDir(Path snapshotDir, String serverName) {
    return new Path(snapshotDir, HLogUtil.getHLogDirectoryName(serverName));
  }

  /**
   * Get the snapshotted recovered.edits dir for the specified region.
   *
   * @param snapshotDir directory where the specific snapshot is stored
   * @param regionName name of the region
   * @return path to the recovered.edits directory for the specified region files.
   */
  public static Path getRecoveredEditsDir(Path snapshotDir, String regionName) {
    return HLogUtil.getRegionDirRecoveredEditsDir(new Path(snapshotDir, regionName));
  }

  /**
   * Get the snapshot recovered.edits file
   *
   * @param snapshotDir directory where the specific snapshot is stored
   * @param regionName name of the region
   * @param logfile name of the edit file
   * @return full path of the log file for the specified region files.
   */
  public static Path getRecoveredEdits(Path snapshotDir, String regionName, String logfile) {
    return new Path(getRecoveredEditsDir(snapshotDir, regionName), logfile);
  }

  /**
   * Iterate over the snapshot store files, restored.edits and logs
   *
   * @param fs {@link FileSystem}
   * @param snapshotDir {@link Path} to the Snapshot directory
   * @param visitor callback object to get the referenced files
   * @throws IOException if an error occurred while scanning the directory
   */
  public static void visitReferencedFiles(final FileSystem fs, final Path snapshotDir,
      final FileVisitor visitor) throws IOException {
    visitTableStoreFiles(fs, snapshotDir, visitor);
    visitRecoveredEdits(fs, snapshotDir, visitor);
    visitLogFiles(fs, snapshotDir, visitor);
  }

  /**
   * Iterate over the snapshot store files
   *
   * @param fs {@link FileSystem}
   * @param snapshotDir {@link Path} to the Snapshot directory
   * @param visitor callback object to get the store files
   * @throws IOException if an error occurred while scanning the directory
   */
  public static void visitTableStoreFiles(final FileSystem fs, final Path snapshotDir,
      final FSVisitor.StoreFileVisitor visitor) throws IOException {
    FSVisitor.visitTableStoreFiles(fs, snapshotDir, visitor);
  }

  /**
   * Iterate over the snapshot store files in the specified region
   *
   * @param fs {@link FileSystem}
   * @param regionDir {@link Path} to the Snapshot region directory
   * @param visitor callback object to get the store files
   * @throws IOException if an error occurred while scanning the directory
   */
  public static void visitRegionStoreFiles(final FileSystem fs, final Path regionDir,
      final FSVisitor.StoreFileVisitor visitor) throws IOException {
    FSVisitor.visitRegionStoreFiles(fs, regionDir, visitor);
  }

  /**
   * Iterate over the snapshot recovered.edits
   *
   * @param fs {@link FileSystem}
   * @param snapshotDir {@link Path} to the Snapshot directory
   * @param visitor callback object to get the recovered.edits files
   * @throws IOException if an error occurred while scanning the directory
   */
  public static void visitRecoveredEdits(final FileSystem fs, final Path snapshotDir,
      final FSVisitor.RecoveredEditsVisitor visitor) throws IOException {
    FSVisitor.visitTableRecoveredEdits(fs, snapshotDir, visitor);
  }

  /**
   * Iterate over the snapshot log files
   *
   * @param fs {@link FileSystem}
   * @param snapshotDir {@link Path} to the Snapshot directory
   * @param visitor callback object to get the log files
   * @throws IOException if an error occurred while scanning the directory
   */
  public static void visitLogFiles(final FileSystem fs, final Path snapshotDir,
      final FSVisitor.LogFileVisitor visitor) throws IOException {
    FSVisitor.visitLogFiles(fs, snapshotDir, visitor);
  }

  /**
   * Verify the validity of the snapshot
   *
   * @param conf The current {@link Configuration} instance.
   * @param fs {@link FileSystem}
   * @param snapshotDir {@link Path} to the Snapshot directory of the snapshot to verify
   * @param snapshotDesc the {@link SnapshotDescription} of the snapshot to verify
   * @throws CorruptedSnapshotException if the snapshot is corrupted
   * @throws IOException if an error occurred while scanning the directory
   */
  public static void verifySnapshot(final Configuration conf, final FileSystem fs,
      final Path snapshotDir, final SnapshotDescription snapshotDesc) throws IOException {
    final TableName table = TableName.valueOf(snapshotDesc.getTable());
    visitTableStoreFiles(fs, snapshotDir, new FSVisitor.StoreFileVisitor() {
      public void storeFile (final String region, final String family, final String hfile)
          throws IOException {
        HFileLink link = HFileLink.create(conf, table, region, family, hfile);
        try {
          link.getFileStatus(fs);
        } catch (FileNotFoundException e) {
          throw new CorruptedSnapshotException("Corrupted snapshot '" + snapshotDesc + "'", e);
        }
      }
    });
  }

  /**
   * Returns the set of region names available in the snapshot.
   *
   * @param fs {@link FileSystem}
   * @param snapshotDir {@link Path} to the Snapshot directory
   * @throws IOException if an error occurred while scanning the directory
   * @return the set of the regions contained in the snapshot
   */
  public static Set<String> getSnapshotRegionNames(final FileSystem fs, final Path snapshotDir)
      throws IOException {
    FileStatus[] regionDirs = FSUtils.listStatus(fs, snapshotDir, new FSUtils.RegionDirFilter(fs));
    if (regionDirs == null) return null;

    Set<String> regions = new HashSet<String>();
    for (FileStatus regionDir: regionDirs) {
      regions.add(regionDir.getPath().getName());
    }
    return regions;
  }

  /**
   * Get the list of hfiles for the specified snapshot region.
   * NOTE: The current implementation keeps one empty file per HFile in the region.
   * The file name matches the one in the original table, and by reconstructing
   * the path you can quickly jump to the referenced file.
   *
   * @param fs {@link FileSystem}
   * @param snapshotRegionDir {@link Path} to the Snapshot region directory
   * @return Map of hfiles per family, the key is the family name and values are hfile names
   * @throws IOException if an error occurred while scanning the directory
   */
  public static Map<String, List<String>> getRegionHFileReferences(final FileSystem fs,
      final Path snapshotRegionDir) throws IOException {
    final Map<String, List<String>> familyFiles = new TreeMap<String, List<String>>();

    visitRegionStoreFiles(fs, snapshotRegionDir,
      new FSVisitor.StoreFileVisitor() {
        public void storeFile (final String region, final String family, final String hfile)
            throws IOException {
          List<String> hfiles = familyFiles.get(family);
          if (hfiles == null) {
            hfiles = new LinkedList<String>();
            familyFiles.put(family, hfiles);
          }
          hfiles.add(hfile);
        }
    });

    return familyFiles;
  }

  /**
   * Returns the store file names in the snapshot.
   *
   * @param fs {@link FileSystem}
   * @param snapshotDir {@link Path} to the Snapshot directory
   * @throws IOException if an error occurred while scanning the directory
   * @return the names of hfiles in the specified snaphot
   */
  public static Set<String> getHFileNames(final FileSystem fs, final Path snapshotDir)
      throws IOException {
    final Set<String> names = new HashSet<String>();
    visitTableStoreFiles(fs, snapshotDir, new FSVisitor.StoreFileVisitor() {
      public void storeFile (final String region, final String family, final String hfile)
          throws IOException {
        if (HFileLink.isHFileLink(hfile)) {
          names.add(HFileLink.getReferencedHFileName(hfile));
        } else {
          names.add(hfile);
        }
      }
    });
    return names;
  }

  /**
   * Returns the log file names available in the snapshot.
   *
   * @param fs {@link FileSystem}
   * @param snapshotDir {@link Path} to the Snapshot directory
   * @throws IOException if an error occurred while scanning the directory
   * @return the names of hlogs in the specified snaphot
   */
  public static Set<String> getHLogNames(final FileSystem fs, final Path snapshotDir)
      throws IOException {
    final Set<String> names = new HashSet<String>();
    visitLogFiles(fs, snapshotDir, new FSVisitor.LogFileVisitor() {
      public void logFile (final String server, final String logfile) throws IOException {
        names.add(logfile);
      }
    });
    return names;
  }
}
