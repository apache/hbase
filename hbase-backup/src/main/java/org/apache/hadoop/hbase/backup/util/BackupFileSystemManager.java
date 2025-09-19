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
package org.apache.hadoop.hbase.backup.util;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.backup.replication.Utils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Initializes and organizes backup directories for continuous Write-Ahead Logs (WALs) and
 * bulk-loaded files within the specified backup root directory.
 */
@InterfaceAudience.Private
public class BackupFileSystemManager {
  private static final Logger LOG = LoggerFactory.getLogger(BackupFileSystemManager.class);

  public static final String WALS_DIR = "WALs";
  public static final String BULKLOAD_FILES_DIR = "bulk-load-files";
  private final String peerId;
  private final FileSystem backupFs;
  private final Path backupRootDir;
  private final Path walsDir;
  private final Path bulkLoadFilesDir;

  public BackupFileSystemManager(String peerId, Configuration conf, String backupRootDirStr)
    throws IOException {
    this.peerId = peerId;
    this.backupRootDir = new Path(backupRootDirStr);
    this.backupFs = FileSystem.get(backupRootDir.toUri(), conf);
    this.walsDir = createDirectory(WALS_DIR);
    this.bulkLoadFilesDir = createDirectory(BULKLOAD_FILES_DIR);
  }

  private Path createDirectory(String dirName) throws IOException {
    Path dirPath = new Path(backupRootDir, dirName);
    backupFs.mkdirs(dirPath);
    LOG.info("{} Initialized directory: {}", Utils.logPeerId(peerId), dirPath);
    return dirPath;
  }

  public Path getWalsDir() {
    return walsDir;
  }

  public Path getBulkLoadFilesDir() {
    return bulkLoadFilesDir;
  }

  public FileSystem getBackupFs() {
    return backupFs;
  }

  public static final class WalPathInfo {
    private final Path prefixBeforeWALs;
    private final String dateSegment;

    public WalPathInfo(Path prefixBeforeWALs, String dateSegment) {
      this.prefixBeforeWALs = prefixBeforeWALs;
      this.dateSegment = dateSegment;
    }

    public Path getPrefixBeforeWALs() {
      return prefixBeforeWALs;
    }

    public String getDateSegment() {
      return dateSegment;
    }
  }

  /**
   * Validate the walPath has the expected structure: .../WALs/<date>/<wal-file> and return
   * WalPathInfo(prefixBeforeWALs, dateSegment).
   * @throws IOException if the path is not in expected format
   */
  public static WalPathInfo extractWalPathInfo(Path walPath) throws IOException {
    if (walPath == null) {
      throw new IllegalArgumentException("walPath must not be null");
    }

    Path dateDir = walPath.getParent(); // .../WALs/<date>
    if (dateDir == null) {
      throw new IOException("Invalid WAL path: missing date directory. Path: " + walPath);
    }

    Path walsDir = dateDir.getParent(); // .../WALs
    if (walsDir == null) {
      throw new IOException("Invalid WAL path: missing WALs directory. Path: " + walPath);
    }

    String walsDirName = walsDir.getName();
    if (!WALS_DIR.equals(walsDirName)) {
      throw new IOException("Invalid WAL path: expected '" + WALS_DIR + "' segment but found '"
        + walsDirName + "'. Path: " + walPath);
    }

    String dateSegment = dateDir.getName();
    if (dateSegment == null || dateSegment.isEmpty()) {
      throw new IOException("Invalid WAL path: date segment is empty. Path: " + walPath);
    }

    Path prefixBeforeWALs = walsDir.getParent(); // might be null if path is like "/WALs/..."
    return new WalPathInfo(prefixBeforeWALs, dateSegment);
  }

  /**
   * Resolve the full bulk-load file path corresponding to a relative bulk-load path referenced from
   * a WAL file path. For a WAL path like: /some/prefix/.../WALs/23-08-2025/some-wal-file and a
   * relative bulk path like: namespace/table/region/family/file, this returns:
   * /some/prefix/.../bulk-load-files/23-08-2025/namespace/table/region/family/file
   * @param walPath          the Path to the WAL file (must contain the {@link #WALS_DIR} segment
   *                         followed by date)
   * @param relativeBulkPath the relative bulk-load file Path
   * @return resolved full Path for the bulk-load file
   * @throws IOException if the WAL path does not contain the expected segments
   */
  public static Path resolveBulkLoadFullPath(Path walPath, Path relativeBulkPath)
    throws IOException {
    WalPathInfo info = extractWalPathInfo(walPath);

    Path prefixBeforeWALs = info.getPrefixBeforeWALs();
    String dateSegment = info.getDateSegment();

    Path full; // Build final path:
               // <prefixBeforeWALs>/bulk-load-files/<dateSegment>/<relativeBulkPath>
    if (prefixBeforeWALs == null || prefixBeforeWALs.toString().isEmpty()) {
      full = new Path(BULKLOAD_FILES_DIR, new Path(dateSegment, relativeBulkPath));
    } else {
      full = new Path(new Path(prefixBeforeWALs, BULKLOAD_FILES_DIR),
        new Path(dateSegment, relativeBulkPath));
    }
    return full;
  }
}
