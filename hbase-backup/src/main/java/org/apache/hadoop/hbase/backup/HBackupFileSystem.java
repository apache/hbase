/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.backup;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupManifest;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * View to an on-disk Backup Image FileSytem Provides the set of methods necessary to interact with
 * the on-disk Backup Image data.
 */
@InterfaceAudience.Private
public final class HBackupFileSystem {
  public static final Logger LOG = LoggerFactory.getLogger(HBackupFileSystem.class);

  /**
   * This is utility class.
   */
  private HBackupFileSystem() {
  }

  /**
   * Given the backup root dir, backup id and the table name, return the backup image location,
   * which is also where the backup manifest file is. return value look like:
   * "hdfs://backup.hbase.org:9000/user/biadmin/backup/backup_1396650096738/default/t1_dn/", where
   * "hdfs://backup.hbase.org:9000/user/biadmin/backup" is a backup root directory
   * @param backupRootDir backup root directory
   * @param backupId backup id
   * @param tableName table name
   * @return backupPath String for the particular table
   */
  public static String
      getTableBackupDir(String backupRootDir, String backupId, TableName tableName) {
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
   * @param backupId backup id
   * @return backup tmp directory path
   */
  public static Path getBackupTmpDirPathForBackupId(String backupRoot, String backupId) {
    return new Path(getBackupTmpDirPath(backupRoot), backupId);
  }

  public static String getTableBackupDataDir(String backupRootDir, String backupId,
      TableName tableName) {
    return getTableBackupDir(backupRootDir, backupId, tableName) + Path.SEPARATOR + "data";
  }

  public static Path getBackupPath(String backupRootDir, String backupId) {
    return new Path(backupRootDir + Path.SEPARATOR + backupId);
  }

  /**
   * Given the backup root dir, backup id and the table name, return the backup image location,
   * which is also where the backup manifest file is. return value look like:
   * "hdfs://backup.hbase.org:9000/user/biadmin/backup/backup_1396650096738/default/t1_dn/", where
   * "hdfs://backup.hbase.org:9000/user/biadmin/backup" is a backup root directory
   * @param backupRootPath backup root path
   * @param tableName table name
   * @param backupId backup Id
   * @return backupPath for the particular table
   */
  public static Path getTableBackupPath(TableName tableName, Path backupRootPath, String backupId) {
    return new Path(getTableBackupDir(backupRootPath.toString(), backupId, tableName));
  }

  /**
   * Given the backup root dir and the backup id, return the log file location for an incremental
   * backup.
   * @param backupRootDir backup root directory
   * @param backupId backup id
   * @return logBackupDir: ".../user/biadmin/backup/WALs/backup_1396650096738"
   */
  public static String getLogBackupDir(String backupRootDir, String backupId) {
    return backupRootDir + Path.SEPARATOR + backupId + Path.SEPARATOR
        + HConstants.HREGION_LOGDIR_NAME;
  }

  public static Path getLogBackupPath(String backupRootDir, String backupId) {
    return new Path(getLogBackupDir(backupRootDir, backupId));
  }

  // TODO we do not keep WAL files anymore
  // Move manifest file to other place
  private static Path getManifestPath(Configuration conf, Path backupRootPath, String backupId)
      throws IOException {
    FileSystem fs = backupRootPath.getFileSystem(conf);
    Path manifestPath =
        new Path(getBackupPath(backupRootPath.toString(), backupId) + Path.SEPARATOR
            + BackupManifest.MANIFEST_FILE_NAME);
    if (!fs.exists(manifestPath)) {
      String errorMsg =
          "Could not find backup manifest " + BackupManifest.MANIFEST_FILE_NAME + " for "
              + backupId + ". File " + manifestPath + " does not exists. Did " + backupId
              + " correspond to previously taken backup ?";
      throw new IOException(errorMsg);
    }
    return manifestPath;
  }

  public static BackupManifest
      getManifest(Configuration conf, Path backupRootPath, String backupId) throws IOException {
    BackupManifest manifest =
        new BackupManifest(conf, getManifestPath(conf, backupRootPath, backupId));
    return manifest;
  }

  /**
   * Check whether the backup image path and there is manifest file in the path.
   * @param backupManifestMap If all the manifests are found, then they are put into this map
   * @param tableArray the tables involved
   * @throws IOException exception
   */
  public static void checkImageManifestExist(HashMap<TableName, BackupManifest> backupManifestMap,
      TableName[] tableArray, Configuration conf, Path backupRootPath, String backupId)
      throws IOException {
    for (TableName tableName : tableArray) {
      BackupManifest manifest = getManifest(conf, backupRootPath, backupId);
      backupManifestMap.put(tableName, manifest);
    }
  }
}
