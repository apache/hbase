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
package org.apache.hadoop.hbase.backup;

import static org.apache.hadoop.hbase.HConstants.HBASE_TEMP_DIRECTORY;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.BACKUPID_PREFIX;

import com.google.errorprone.annotations.RestrictedApi;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupManifest;
import org.apache.hadoop.hbase.backup.impl.BackupManifest.BackupImage;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * View to an on-disk Backup Image FileSytem Provides the set of methods necessary to interact with
 * the on-disk Backup Image data.
 */
@InterfaceAudience.Private
public final class HBackupFileSystem {
  public static final Logger LOG = LoggerFactory.getLogger(HBackupFileSystem.class);
  public static final String BACKUP_COMPLETE_FILE_NAME = ".backup.complete";

  /**
   * This is utility class.
   */
  private HBackupFileSystem() {
  }

  /**
   * Given the backup root dir, backup id and the table name, return the backup image location.
   * Return value look like:
   * "hdfs://backup.hbase.org:9000/user/biadmin/backup/backup_1396650096738/default/t1_dn/", where
   * "hdfs://backup.hbase.org:9000/user/biadmin/backup" is a backup root directory
   * @param backupRootDir backup root directory
   * @param backupId      backup id
   * @param tableName     table name
   * @return backupPath String for the particular table
   */
  public static String getTableBackupDir(String backupRootDir, String backupId,
    TableName tableName) {
    return backupRootDir + Path.SEPARATOR + backupId + Path.SEPARATOR
      + tableName.getNamespaceAsString() + Path.SEPARATOR + tableName.getQualifierAsString()
      + Path.SEPARATOR;
  }

  /**
   * Get backup temporary directory
   * @param backupRootDir backup root
   * @return backup tmp directory path
   */
  public static Path getBackupTmpDirPath(String backupRootDir) {
    return new Path(backupRootDir, ".tmp");
  }

  /**
   * Get backup tmp directory for backupId
   * @param backupRoot backup root
   * @param backupId   backup id
   * @return backup tmp directory path
   */
  public static Path getBackupTmpDirPathForBackupId(String backupRoot, String backupId) {
    return new Path(getBackupTmpDirPath(backupRoot), backupId);
  }

  public static Path getBackupPath(String backupRootDir, String backupId) {
    return new Path(backupRootDir + Path.SEPARATOR + backupId);
  }

  /**
   * Write a completion marker file to signal a backup has completed successfully. This is a
   * best-effort operation: if the write fails, the backup itself is still considered complete (the
   * system table is the source of truth). The marker file is intended for external consumers (e.g.,
   * another cluster) that cannot access the originating cluster's BackupSystemTable. A backup can
   * still be complete even if the completion marker file is missing, but there is no way to verify
   * this without using the originating cluster's BackupSystemTable.
   * <p>
   * The marker is always written last and deleted first: any operation that removes or modifies
   * data in a backup directory (failed-backup cleanup, backup delete, merge, removing a table from
   * a backup image when a dependent backup is deleted) deletes the marker before touching the data.
   * A backup whose image was modified this way may therefore remain COMPLETE in the system table
   * without a marker.
   * @param conf          configuration
   * @param backupRootDir backup root directory
   * @param backupId      backup id
   */
  public static void writeBackupCompleteMarker(Configuration conf, String backupRootDir,
    String backupId) {
    Path backupDir = getBackupPath(backupRootDir, backupId);
    Path markerPath = new Path(backupDir, BACKUP_COMPLETE_FILE_NAME);
    try {
      FileSystem fs = markerPath.getFileSystem(conf);
      fs.create(markerPath, true).close();
      LOG.info("Backup completion marker file written to: {}", markerPath);
    } catch (IOException | RuntimeException e) {
      LOG.warn("Failed to write {} marker file to backup directory: {}", BACKUP_COMPLETE_FILE_NAME,
        backupDir, e);
    }
  }

  /**
   * Delete the completion marker file for the given backup, if it exists. Callers must invoke this
   * before removing or modifying any data in the backup directory, so that the marker is never
   * present on a backup whose data is incomplete.
   * @param conf          configuration
   * @param backupRootDir backup root directory
   * @param backupId      backup id
   * @throws IOException if the file system cannot be accessed or the marker could not be deleted
   */
  public static void deleteBackupCompleteMarker(Configuration conf, String backupRootDir,
    String backupId) throws IOException {
    Path markerPath = new Path(getBackupPath(backupRootDir, backupId), BACKUP_COMPLETE_FILE_NAME);
    FileSystem fs = markerPath.getFileSystem(conf);
    if (fs.delete(markerPath, false)) {
      LOG.debug("Removed backup completion marker: {}", markerPath);
    } else if (fs.exists(markerPath)) {
      throw new IOException("Unable to delete backup completion marker: " + markerPath);
    }
  }

  /**
   * Check whether a backup completion marker file exists for the given backup.
   * @param conf          configuration
   * @param backupRootDir backup root directory
   * @param backupId      backup id
   * @return true if the marker file exists
   * @throws IOException if the file system cannot be accessed
   */
  public static boolean isBackupComplete(Configuration conf, String backupRootDir, String backupId)
    throws IOException {
    Path markerPath = new Path(getBackupPath(backupRootDir, backupId), BACKUP_COMPLETE_FILE_NAME);
    FileSystem fs = markerPath.getFileSystem(conf);
    return fs.exists(markerPath);
  }

  /**
   * Given the backup root dir, backup id and the table name, return the backup image location,
   * which is also where the backup manifest file is. return value look like:
   * "hdfs://backup.hbase.org:9000/user/biadmin/backup/backup_1396650096738/default/t1_dn/", where
   * "hdfs://backup.hbase.org:9000/user/biadmin/backup" is a backup root directory
   * @param backupRootPath backup root path
   * @param tableName      table name
   * @param backupId       backup Id
   * @return backupPath for the particular table
   */
  public static Path getTableBackupPath(TableName tableName, Path backupRootPath, String backupId) {
    return new Path(getTableBackupDir(backupRootPath.toString(), backupId, tableName));
  }

  private static Path getManifestPath(Configuration conf, Path backupRootPath, String backupId)
    throws IOException {
    return getManifestPath(conf, backupRootPath, backupId, true);
  }

  /* Visible for testing only */
  @RestrictedApi(explanation = "Should only be called internally or in tests", link = "",
      allowedOnPath = "(.*/src/test/.*|.*/org/apache/hadoop/hbase/backup/HBackupFileSystem.java)")
  static Path getManifestPath(Configuration conf, Path backupRootPath, String backupId,
    boolean throwIfNotFound) throws IOException {
    FileSystem fs = backupRootPath.getFileSystem(conf);
    Path manifestPath = new Path(getBackupPath(backupRootPath.toString(), backupId) + Path.SEPARATOR
      + BackupManifest.MANIFEST_FILE_NAME);
    if (throwIfNotFound && !fs.exists(manifestPath)) {
      String errorMsg = "Could not find backup manifest " + BackupManifest.MANIFEST_FILE_NAME
        + " for " + backupId + ". File " + manifestPath + " does not exists. Did " + backupId
        + " correspond to previously taken backup ?";
      throw new IOException(errorMsg);
    }
    return manifestPath;
  }

  public static Path getRootDirFromBackupPath(Path backupPath, String backupId) {
    if (backupPath.getName().equals(BackupManifest.MANIFEST_FILE_NAME)) {
      backupPath = backupPath.getParent();
    }
    Preconditions.checkArgument(backupPath.getName().equals(backupId),
      String.format("Backup path %s must end in backupId %s", backupPath, backupId));
    return backupPath.getParent();
  }

  public static BackupManifest getManifest(Configuration conf, Path backupRootPath, String backupId)
    throws IOException {
    BackupManifest manifest =
      new BackupManifest(conf, getManifestPath(conf, backupRootPath, backupId));
    return manifest;
  }

  public static List<BackupImage> getAllBackupImages(Configuration conf, Path backupRootPath)
    throws IOException {
    FileSystem fs = FileSystem.get(backupRootPath.toUri(), conf);
    RemoteIterator<LocatedFileStatus> it = fs.listLocatedStatus(backupRootPath);

    List<BackupImage> images = new ArrayList<>();

    while (it.hasNext()) {
      LocatedFileStatus lfs = it.next();
      if (!lfs.isDirectory()) {
        continue;
      }

      String backupId = lfs.getPath().getName();
      if (HBASE_TEMP_DIRECTORY.equals(backupId)) {
        continue;
      }

      try {
        BackupManifest manifest = getManifest(conf, backupRootPath, backupId);
        images.add(manifest.getBackupImage());
      } catch (IOException e) {
        LOG.error("Cannot load backup manifest from: {}", lfs.getPath(), e);
      }
    }

    // Sort images by timestamp in descending order
    images.sort(Comparator.comparingLong(m -> -getTimestamp(m.getBackupId())));

    return images;
  }

  private static long getTimestamp(String backupId) {
    return Long.parseLong(backupId.substring(BACKUPID_PREFIX.length()));
  }
}
