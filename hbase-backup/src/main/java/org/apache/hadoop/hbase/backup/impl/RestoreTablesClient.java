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

package org.apache.hadoop.hbase.backup.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.backup.HBackupFileSystem;
import org.apache.hadoop.hbase.backup.RestoreRequest;
import org.apache.hadoop.hbase.backup.impl.BackupManifest.BackupImage;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.backup.util.RestoreTool;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles.LoadQueueItem;

/**
 * Restore table implementation
 *
 */
@InterfaceAudience.Private
public class RestoreTablesClient {
  private static final Log LOG = LogFactory.getLog(RestoreTablesClient.class);

  private Configuration conf;
  private Connection conn;
  private String backupId;
  private TableName[] sTableArray;
  private TableName[] tTableArray;
  private String targetRootDir;
  private boolean isOverwrite;

  public RestoreTablesClient(Connection conn, RestoreRequest request) throws IOException {
    this.targetRootDir = request.getBackupRootDir();
    this.backupId = request.getBackupId();
    this.sTableArray = request.getFromTables();
    this.tTableArray = request.getToTables();
    if (tTableArray == null || tTableArray.length == 0) {
      this.tTableArray = sTableArray;
    }
    this.isOverwrite = request.isOverwrite();
    this.conn = conn;
    this.conf = conn.getConfiguration();

  }

  /**
   * Validate target tables
   * @param conn connection
   * @param mgr table state manager
   * @param tTableArray: target tables
   * @param isOverwrite overwrite existing table
   * @throws IOException exception
   */
  private void checkTargetTables(TableName[] tTableArray, boolean isOverwrite) throws IOException {
    ArrayList<TableName> existTableList = new ArrayList<>();
    ArrayList<TableName> disabledTableList = new ArrayList<>();

    // check if the tables already exist
    try (Admin admin = conn.getAdmin();) {
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
        LOG.error("Existing table (" + existTableList
            + ") found in the restore target, please add "
            + "\"-o\" as overwrite option in the command if you mean"
            + " to restore to these existing tables");
        throw new IOException("Existing table found in target while no \"-o\" "
            + "as overwrite option found");
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
   * Restore operation handle each backupImage in array
   * @param svc: master services
   * @param images: array BackupImage
   * @param sTable: table to be restored
   * @param tTable: table to be restored to
   * @param truncateIfExists: truncate table
   * @throws IOException exception
   */

  private void restoreImages(BackupImage[] images, TableName sTable, TableName tTable,
      boolean truncateIfExists) throws IOException {

    // First image MUST be image of a FULL backup
    BackupImage image = images[0];
    String rootDir = image.getRootDir();
    String backupId = image.getBackupId();
    Path backupRoot = new Path(rootDir);
    RestoreTool restoreTool = new RestoreTool(conf, backupRoot, backupId);
    Path tableBackupPath = HBackupFileSystem.getTableBackupPath(sTable, backupRoot, backupId);
    String lastIncrBackupId = images.length == 1 ? null : images[images.length - 1].getBackupId();
    // We need hFS only for full restore (see the code)
    BackupManifest manifest = HBackupFileSystem.getManifest(conf, backupRoot, backupId);
    if (manifest.getType() == BackupType.FULL) {
      LOG.info("Restoring '" + sTable + "' to '" + tTable + "' from full" + " backup image "
          + tableBackupPath.toString());
      restoreTool.fullRestoreTable(conn, tableBackupPath, sTable, tTable, truncateIfExists,
        lastIncrBackupId);
    } else { // incremental Backup
      throw new IOException("Unexpected backup type " + image.getType());
    }

    if (images.length == 1) {
      // full backup restore done
      return;
    }

    List<Path> dirList = new ArrayList<Path>();
    // add full backup path
    // full backup path comes first
    for (int i = 1; i < images.length; i++) {
      BackupImage im = images[i];
      String fileBackupDir =
          HBackupFileSystem.getTableBackupDataDir(im.getRootDir(), im.getBackupId(), sTable);
      dirList.add(new Path(fileBackupDir));
    }

    String dirs = StringUtils.join(dirList, ",");
    LOG.info("Restoring '" + sTable + "' to '" + tTable + "' from log dirs: " + dirs);
    Path[] paths = new Path[dirList.size()];
    dirList.toArray(paths);
    restoreTool.incrementalRestoreTable(conn, tableBackupPath, paths, new TableName[] { sTable },
      new TableName[] { tTable }, lastIncrBackupId);
    LOG.info(sTable + " has been successfully restored to " + tTable);
  }

  /**
   * Restore operation. Stage 2: resolved Backup Image dependency
   * @param backupManifestMap : tableName, Manifest
   * @param sTableArray The array of tables to be restored
   * @param tTableArray The array of mapping tables to restore to
   * @return set of BackupImages restored
   * @throws IOException exception
   */
  private void restore(HashMap<TableName, BackupManifest> backupManifestMap,
      TableName[] sTableArray, TableName[] tTableArray, boolean isOverwrite) throws IOException {
    TreeSet<BackupImage> restoreImageSet = new TreeSet<BackupImage>();
    boolean truncateIfExists = isOverwrite;
    Set<String> backupIdSet = new HashSet<>();

    for (int i = 0; i < sTableArray.length; i++) {
      TableName table = sTableArray[i];

      BackupManifest manifest = backupManifestMap.get(table);
      // Get the image list of this backup for restore in time order from old
      // to new.
      List<BackupImage> list = new ArrayList<BackupImage>();
      list.add(manifest.getBackupImage());
      TreeSet<BackupImage> set = new TreeSet<BackupImage>(list);
      List<BackupImage> depList = manifest.getDependentListByTable(table);
      set.addAll(depList);
      BackupImage[] arr = new BackupImage[set.size()];
      set.toArray(arr);
      restoreImages(arr, table, tTableArray[i], truncateIfExists);
      restoreImageSet.addAll(list);
      if (restoreImageSet != null && !restoreImageSet.isEmpty()) {
        LOG.info("Restore includes the following image(s):");
        for (BackupImage image : restoreImageSet) {
          LOG.info("Backup: " + image.getBackupId() + " "
              + HBackupFileSystem.getTableBackupDir(image.getRootDir(), image.getBackupId(), table));
          if (image.getType() == BackupType.INCREMENTAL) {
            backupIdSet.add(image.getBackupId());
            LOG.debug("adding " + image.getBackupId() + " for bulk load");
          }
        }
      }
    }
    try (BackupSystemTable table = new BackupSystemTable(conn)) {
      List<TableName> sTableList = Arrays.asList(sTableArray);
      for (String id : backupIdSet) {
        LOG.debug("restoring bulk load for " + id);
        Map<byte[], List<Path>>[] mapForSrc = table.readBulkLoadedFiles(id, sTableList);
        Map<LoadQueueItem, ByteBuffer> loaderResult;
        conf.setBoolean(LoadIncrementalHFiles.ALWAYS_COPY_FILES, true);
        LoadIncrementalHFiles loader = BackupUtils.createLoader(conf);
        for (int i = 0; i < sTableList.size(); i++) {
          if (mapForSrc[i] != null && !mapForSrc[i].isEmpty()) {
            loaderResult = loader.run(mapForSrc[i], tTableArray[i]);
            LOG.debug("bulk loading " + sTableList.get(i) + " to " + tTableArray[i]);
            if (loaderResult.isEmpty()) {
              String msg = "Couldn't bulk load for " + sTableList.get(i) + " to " + tTableArray[i];
              LOG.error(msg);
              throw new IOException(msg);
            }
          }
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
    if (a < lower || a > upper) {
      return false;
    }
    return true;
  }

  public void execute() throws IOException {

    // case VALIDATION:
    // check the target tables
    checkTargetTables(tTableArray, isOverwrite);

    // case RESTORE_IMAGES:
    HashMap<TableName, BackupManifest> backupManifestMap = new HashMap<>();
    // check and load backup image manifest for the tables
    Path rootPath = new Path(targetRootDir);
    HBackupFileSystem.checkImageManifestExist(backupManifestMap, sTableArray, conf, rootPath,
      backupId);

    restore(backupManifestMap, sTableArray, tTableArray, isOverwrite);
  }

}
