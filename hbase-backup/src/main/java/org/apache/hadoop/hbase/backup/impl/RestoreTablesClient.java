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
package org.apache.hadoop.hbase.backup.impl;

import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.JOB_NAME_CONF_KEY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.backup.HBackupFileSystem;
import org.apache.hadoop.hbase.backup.RestoreRequest;
import org.apache.hadoop.hbase.backup.impl.BackupManifest.BackupImage;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.backup.util.RestoreTool;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Restore table implementation
 */
@InterfaceAudience.Private
public class RestoreTablesClient {
  private static final Logger LOG = LoggerFactory.getLogger(RestoreTablesClient.class);

  private Configuration conf;
  private Connection conn;
  private String backupId;
  private TableName[] sTableArray;
  private TableName[] tTableArray;
  private String backupRootDir;
  private Path restoreRootDir;
  private boolean isOverwrite;

  private boolean isKeepOriginalSplits;

  public RestoreTablesClient(Connection conn, RestoreRequest request) throws IOException {
    this.backupRootDir = request.getBackupRootDir();
    this.backupId = request.getBackupId();
    this.sTableArray = request.getFromTables();
    this.tTableArray = request.getToTables();
    if (tTableArray == null || tTableArray.length == 0) {
      this.tTableArray = sTableArray;
    }
    this.isOverwrite = request.isOverwrite();
    this.isKeepOriginalSplits = request.isKeepOriginalSplits();
    this.conn = conn;
    this.conf = conn.getConfiguration();
    if (request.getRestoreRootDir() != null) {
      restoreRootDir = new Path(request.getRestoreRootDir());
    } else {
      FileSystem fs = FileSystem.get(conf);
      this.restoreRootDir = BackupUtils.getTmpRestoreOutputDir(fs, conf);
    }
  }

  /**
   * Validate target tables.
   * @param tTableArray target tables
   * @param isOverwrite overwrite existing table
   * @throws IOException exception
   */
  private void checkTargetTables(TableName[] tTableArray, boolean isOverwrite) throws IOException {
    ArrayList<TableName> existTableList = new ArrayList<>();
    ArrayList<TableName> disabledTableList = new ArrayList<>();

    // check if the tables already exist
    try (Admin admin = conn.getAdmin()) {
      for (TableName tableName : tTableArray) {
        if (admin.tableExists(tableName)) {
          existTableList.add(tableName);
          if (admin.isTableDisabled(tableName)) {
            disabledTableList.add(tableName);
          }
        } else {
          LOG.info("HBase table " + tableName
            + " does not exist. It will be created during restore process");
        }
      }
    }

    if (existTableList.size() > 0) {
      if (!isOverwrite) {
        LOG.error("Existing table (" + existTableList + ") found in the restore target, please add "
          + "\"-o\" as overwrite option in the command if you mean"
          + " to restore to these existing tables");
        throw new IOException(
          "Existing table found in target while no \"-o\" " + "as overwrite option found");
      } else {
        if (disabledTableList.size() > 0) {
          LOG.error("Found offline table in the restore target, "
            + "please enable them before restore with \"-overwrite\" option");
          LOG.info("Offline table list in restore target: " + disabledTableList);
          throw new IOException(
            "Found offline table in the target when restore with \"-overwrite\" option");
        }
      }
    }
  }

  /**
   * Restore operation handle each backupImage in array.
   * @param images           array BackupImage
   * @param sTable           table to be restored
   * @param tTable           table to be restored to
   * @param truncateIfExists truncate table
   * @throws IOException exception
   */

  private void restoreImages(BackupImage[] images, TableName sTable, TableName tTable,
    boolean truncateIfExists, boolean isKeepOriginalSplits) throws IOException {
    // First image MUST be image of a FULL backup
    BackupImage image = images[0];
    String rootDir = image.getRootDir();
    String backupId = image.getBackupId();
    Path backupRoot = new Path(rootDir);
    RestoreTool restoreTool = new RestoreTool(conf, backupRoot, restoreRootDir, backupId);
    Path tableBackupPath = HBackupFileSystem.getTableBackupPath(sTable, backupRoot, backupId);
    String lastIncrBackupId = images.length == 1 ? null : images[images.length - 1].getBackupId();
    // We need hFS only for full restore (see the code)
    BackupManifest manifest = HBackupFileSystem.getManifest(conf, backupRoot, backupId);
    if (manifest.getType() == BackupType.FULL) {
      LOG.info("Restoring '" + sTable + "' to '" + tTable + "' from full" + " backup image "
        + tableBackupPath.toString());
      conf.set(JOB_NAME_CONF_KEY, "Full_Restore-" + backupId + "-" + tTable);
      restoreTool.fullRestoreTable(conn, tableBackupPath, sTable, tTable, truncateIfExists,
        isKeepOriginalSplits, lastIncrBackupId);
      conf.unset(JOB_NAME_CONF_KEY);
    } else { // incremental Backup
      throw new IOException("Unexpected backup type " + image.getType());
    }

    if (images.length == 1) {
      // full backup restore done
      return;
    }

    List<Path> dirList = new ArrayList<>();
    // add full backup path
    // full backup path comes first
    for (int i = 1; i < images.length; i++) {
      BackupImage im = images[i];
      String fileBackupDir =
        HBackupFileSystem.getTableBackupDir(im.getRootDir(), im.getBackupId(), sTable);
      List<Path> list = getFilesRecursively(fileBackupDir);
      dirList.addAll(list);

    }

    if (dirList.isEmpty()) {
      LOG.warn("Nothing has changed, so there is no need to restore '" + sTable + "'");
      return;
    }

    String dirs = StringUtils.join(dirList, ",");
    LOG.info("Restoring '" + sTable + "' to '" + tTable + "' from log dirs: " + dirs);
    Path[] paths = new Path[dirList.size()];
    dirList.toArray(paths);
    conf.set(JOB_NAME_CONF_KEY, "Incremental_Restore-" + backupId + "-" + tTable);
    restoreTool.incrementalRestoreTable(conn, tableBackupPath, paths, new TableName[] { sTable },
      new TableName[] { tTable }, lastIncrBackupId, isKeepOriginalSplits);
    LOG.info(sTable + " has been successfully restored to " + tTable);
  }

  private List<Path> getFilesRecursively(String fileBackupDir)
    throws IllegalArgumentException, IOException {
    FileSystem fs = FileSystem.get(new Path(fileBackupDir).toUri(), new Configuration());
    List<Path> list = new ArrayList<>();
    RemoteIterator<LocatedFileStatus> it = fs.listFiles(new Path(fileBackupDir), true);
    while (it.hasNext()) {
      Path p = it.next().getPath();
      if (HFile.isHFileFormat(fs, p)) {
        list.add(p);
      }
    }
    return list;
  }

  /**
   * Restore operation. Stage 2: resolved Backup Image dependency
   * @param sTableArray The array of tables to be restored
   * @param tTableArray The array of mapping tables to restore to
   * @throws IOException exception
   */
  private void restore(BackupManifest manifest, TableName[] sTableArray, TableName[] tTableArray,
    boolean isOverwrite, boolean isKeepOriginalSplits) throws IOException {
    TreeSet<BackupImage> restoreImageSet = new TreeSet<>();

    for (int i = 0; i < sTableArray.length; i++) {
      TableName table = sTableArray[i];

      // Get the image list of this backup for restore in time order from old
      // to new.
      List<BackupImage> list = new ArrayList<>();
      list.add(manifest.getBackupImage());
      TreeSet<BackupImage> set = new TreeSet<>(list);
      List<BackupImage> depList = manifest.getDependentListByTable(table);
      set.addAll(depList);
      BackupImage[] arr = new BackupImage[set.size()];
      set.toArray(arr);
      restoreImages(arr, table, tTableArray[i], isOverwrite, isKeepOriginalSplits);
      restoreImageSet.addAll(list);
      if (restoreImageSet != null && !restoreImageSet.isEmpty()) {
        LOG.info("Restore includes the following image(s):");
        for (BackupImage image : restoreImageSet) {
          LOG.info("Backup: " + image.getBackupId() + " "
            + HBackupFileSystem.getTableBackupDir(image.getRootDir(), image.getBackupId(), table));
        }
      }
    }
    LOG.debug("restoreStage finished");
  }

  static long getTsFromBackupId(String backupId) {
    if (backupId == null) {
      return 0;
    }
    return Long.parseLong(backupId.substring(backupId.lastIndexOf("_") + 1));
  }

  static boolean withinRange(long a, long lower, long upper) {
    return a >= lower && a <= upper;
  }

  public void execute() throws IOException {
    // case VALIDATION:
    // check the target tables
    checkTargetTables(tTableArray, isOverwrite);

    // case RESTORE_IMAGES:
    // check and load backup image manifest for the tables
    Path rootPath = new Path(backupRootDir);
    BackupManifest manifest = HBackupFileSystem.getManifest(conf, rootPath, backupId);

    restore(manifest, sTableArray, tTableArray, isOverwrite, isKeepOriginalSplits);
  }
}
