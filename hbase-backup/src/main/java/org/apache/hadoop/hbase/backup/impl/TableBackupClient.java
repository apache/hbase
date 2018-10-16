/**
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
package org.apache.hadoop.hbase.backup.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupPhase;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupState;
import org.apache.hadoop.hbase.backup.BackupRequest;
import org.apache.hadoop.hbase.backup.BackupRestoreConstants;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.backup.HBackupFileSystem;
import org.apache.hadoop.hbase.backup.impl.BackupManifest.BackupImage;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Base class for backup operation. Concrete implementation for
 * full and incremental backup are delegated to corresponding sub-classes:
 * {@link FullTableBackupClient} and {@link IncrementalTableBackupClient}
 *
 */
@InterfaceAudience.Private
public abstract class TableBackupClient {

  public static final String BACKUP_CLIENT_IMPL_CLASS = "backup.client.impl.class";

  @VisibleForTesting
  public static final String BACKUP_TEST_MODE_STAGE = "backup.test.mode.stage";

  private static final Logger LOG = LoggerFactory.getLogger(TableBackupClient.class);

  protected Configuration conf;
  protected Connection conn;
  protected String backupId;
  protected List<TableName> tableList;
  protected HashMap<String, Long> newTimestamps = null;

  protected BackupManager backupManager;
  protected BackupInfo backupInfo;
  protected FileSystem fs;

  public TableBackupClient() {
  }

  public TableBackupClient(final Connection conn, final String backupId, BackupRequest request)
      throws IOException {
    init(conn, backupId, request);
  }

  public void init(final Connection conn, final String backupId, BackupRequest request)
      throws IOException {
    if (request.getBackupType() == BackupType.FULL) {
      backupManager = new BackupManager(conn, conn.getConfiguration());
    } else {
      backupManager = new IncrementalBackupManager(conn, conn.getConfiguration());
    }
    this.backupId = backupId;
    this.tableList = request.getTableList();
    this.conn = conn;
    this.conf = conn.getConfiguration();
    this.fs = FSUtils.getCurrentFileSystem(conf);
    backupInfo =
        backupManager.createBackupInfo(backupId, request.getBackupType(), tableList,
          request.getTargetRootDir(), request.getTotalTasks(), request.getBandwidth());
    if (tableList == null || tableList.isEmpty()) {
      this.tableList = new ArrayList<>(backupInfo.getTables());
    }
    // Start new session
    backupManager.startBackupSession();
  }

  /**
   * Begin the overall backup.
   * @param backupInfo backup info
   * @throws IOException exception
   */
  protected void beginBackup(BackupManager backupManager, BackupInfo backupInfo)
      throws IOException {

    BackupSystemTable.snapshot(conn);
    backupManager.setBackupInfo(backupInfo);
    // set the start timestamp of the overall backup
    long startTs = EnvironmentEdgeManager.currentTime();
    backupInfo.setStartTs(startTs);
    // set overall backup status: ongoing
    backupInfo.setState(BackupState.RUNNING);
    backupInfo.setPhase(BackupPhase.REQUEST);
    LOG.info("Backup " + backupInfo.getBackupId() + " started at " + startTs + ".");

    backupManager.updateBackupInfo(backupInfo);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Backup session " + backupInfo.getBackupId() + " has been started.");
    }
  }

  protected String getMessage(Exception e) {
    String msg = e.getMessage();
    if (msg == null || msg.equals("")) {
      msg = e.getClass().getName();
    }
    return msg;
  }

  /**
   * Delete HBase snapshot for backup.
   * @param backupInfo backup info
   * @throws IOException exception
   */
  protected static void deleteSnapshots(final Connection conn, BackupInfo backupInfo,
      Configuration conf) throws IOException {
    LOG.debug("Trying to delete snapshot for full backup.");
    for (String snapshotName : backupInfo.getSnapshotNames()) {
      if (snapshotName == null) {
        continue;
      }
      LOG.debug("Trying to delete snapshot: " + snapshotName);

      try (Admin admin = conn.getAdmin()) {
        admin.deleteSnapshot(snapshotName);
      }
      LOG.debug("Deleting the snapshot " + snapshotName + " for backup " + backupInfo.getBackupId()
          + " succeeded.");
    }
  }

  /**
   * Clean up directories with prefix "exportSnapshot-", which are generated when exporting
   * snapshots.
   * @throws IOException exception
   */
  protected static void cleanupExportSnapshotLog(Configuration conf) throws IOException {
    FileSystem fs = FSUtils.getCurrentFileSystem(conf);
    Path stagingDir =
        new Path(conf.get(BackupRestoreConstants.CONF_STAGING_ROOT, fs.getWorkingDirectory()
            .toString()));
    FileStatus[] files = FSUtils.listStatus(fs, stagingDir);
    if (files == null) {
      return;
    }
    for (FileStatus file : files) {
      if (file.getPath().getName().startsWith("exportSnapshot-")) {
        LOG.debug("Delete log files of exporting snapshot: " + file.getPath().getName());
        if (FSUtils.delete(fs, file.getPath(), true) == false) {
          LOG.warn("Can not delete " + file.getPath());
        }
      }
    }
  }

  /**
   * Clean up the uncompleted data at target directory if the ongoing backup has already entered
   * the copy phase.
   */
  protected static void cleanupTargetDir(BackupInfo backupInfo, Configuration conf) {
    try {
      // clean up the uncompleted data at target directory if the ongoing backup has already entered
      // the copy phase
      LOG.debug("Trying to cleanup up target dir. Current backup phase: "
          + backupInfo.getPhase());
      if (backupInfo.getPhase().equals(BackupPhase.SNAPSHOTCOPY)
          || backupInfo.getPhase().equals(BackupPhase.INCREMENTAL_COPY)
          || backupInfo.getPhase().equals(BackupPhase.STORE_MANIFEST)) {
        FileSystem outputFs =
            FileSystem.get(new Path(backupInfo.getBackupRootDir()).toUri(), conf);

        // now treat one backup as a transaction, clean up data that has been partially copied at
        // table level
        for (TableName table : backupInfo.getTables()) {
          Path targetDirPath =
              new Path(HBackupFileSystem.getTableBackupDir(backupInfo.getBackupRootDir(),
                backupInfo.getBackupId(), table));
          if (outputFs.delete(targetDirPath, true)) {
            LOG.debug("Cleaning up uncompleted backup data at " + targetDirPath.toString()
                + " done.");
          } else {
            LOG.debug("No data has been copied to " + targetDirPath.toString() + ".");
          }

          Path tableDir = targetDirPath.getParent();
          FileStatus[] backups = FSUtils.listStatus(outputFs, tableDir);
          if (backups == null || backups.length == 0) {
            outputFs.delete(tableDir, true);
            LOG.debug(tableDir.toString() + " is empty, remove it.");
          }
        }
      }

    } catch (IOException e1) {
      LOG.error("Cleaning up uncompleted backup data of " + backupInfo.getBackupId() + " at "
          + backupInfo.getBackupRootDir() + " failed due to " + e1.getMessage() + ".");
    }
  }

  /**
   * Fail the overall backup.
   * @param backupInfo backup info
   * @param e exception
   * @throws IOException exception
   */
  protected void failBackup(Connection conn, BackupInfo backupInfo, BackupManager backupManager,
      Exception e, String msg, BackupType type, Configuration conf) throws IOException {
    try {
      LOG.error(msg + getMessage(e), e);
      // If this is a cancel exception, then we've already cleaned.
      // set the failure timestamp of the overall backup
      backupInfo.setCompleteTs(EnvironmentEdgeManager.currentTime());
      // set failure message
      backupInfo.setFailedMsg(e.getMessage());
      // set overall backup status: failed
      backupInfo.setState(BackupState.FAILED);
      // compose the backup failed data
      String backupFailedData =
          "BackupId=" + backupInfo.getBackupId() + ",startts=" + backupInfo.getStartTs()
              + ",failedts=" + backupInfo.getCompleteTs() + ",failedphase=" + backupInfo.getPhase()
              + ",failedmessage=" + backupInfo.getFailedMsg();
      LOG.error(backupFailedData);
      cleanupAndRestoreBackupSystem(conn, backupInfo, conf);
      // If backup session is updated to FAILED state - means we
      // processed recovery already.
      backupManager.updateBackupInfo(backupInfo);
      backupManager.finishBackupSession();
      LOG.error("Backup " + backupInfo.getBackupId() + " failed.");
    } catch (IOException ee) {
      LOG.error("Please run backup repair tool manually to restore backup system integrity");
      throw ee;
    }
  }

  public static void cleanupAndRestoreBackupSystem(Connection conn, BackupInfo backupInfo,
      Configuration conf) throws IOException {
    BackupType type = backupInfo.getType();
    // if full backup, then delete HBase snapshots if there already are snapshots taken
    // and also clean up export snapshot log files if exist
    if (type == BackupType.FULL) {
      deleteSnapshots(conn, backupInfo, conf);
      cleanupExportSnapshotLog(conf);
    }
    BackupSystemTable.restoreFromSnapshot(conn);
    BackupSystemTable.deleteSnapshot(conn);
    // clean up the uncompleted data at target directory if the ongoing backup has already entered
    // the copy phase
    // For incremental backup, DistCp logs will be cleaned with the targetDir.
    cleanupTargetDir(backupInfo, conf);
  }

  /**
   * Add manifest for the current backup. The manifest is stored within the table backup directory.
   * @param backupInfo The current backup info
   * @throws IOException exception
   */
  protected void addManifest(BackupInfo backupInfo, BackupManager backupManager, BackupType type,
      Configuration conf) throws IOException {
    // set the overall backup phase : store manifest
    backupInfo.setPhase(BackupPhase.STORE_MANIFEST);

    BackupManifest manifest;

    // Since we have each table's backup in its own directory structure,
    // we'll store its manifest with the table directory.
    for (TableName table : backupInfo.getTables()) {
      manifest = new BackupManifest(backupInfo, table);
      ArrayList<BackupImage> ancestors = backupManager.getAncestors(backupInfo, table);
      for (BackupImage image : ancestors) {
        manifest.addDependentImage(image);
      }

      if (type == BackupType.INCREMENTAL) {
        // We'll store the log timestamps for this table only in its manifest.
        HashMap<TableName, HashMap<String, Long>> tableTimestampMap = new HashMap<>();
        tableTimestampMap.put(table, backupInfo.getIncrTimestampMap().get(table));
        manifest.setIncrTimestampMap(tableTimestampMap);
        ArrayList<BackupImage> ancestorss = backupManager.getAncestors(backupInfo);
        for (BackupImage image : ancestorss) {
          manifest.addDependentImage(image);
        }
      }
      manifest.store(conf);
    }

    // For incremental backup, we store a overall manifest in
    // <backup-root-dir>/WALs/<backup-id>
    // This is used when created the next incremental backup
    if (type == BackupType.INCREMENTAL) {
      manifest = new BackupManifest(backupInfo);
      // set the table region server start and end timestamps for incremental backup
      manifest.setIncrTimestampMap(backupInfo.getIncrTimestampMap());
      ArrayList<BackupImage> ancestors = backupManager.getAncestors(backupInfo);
      for (BackupImage image : ancestors) {
        manifest.addDependentImage(image);
      }
      manifest.store(conf);
    }
  }

  /**
   * Get backup request meta data dir as string.
   * @param backupInfo backup info
   * @return meta data dir
   */
  protected String obtainBackupMetaDataStr(BackupInfo backupInfo) {
    StringBuffer sb = new StringBuffer();
    sb.append("type=" + backupInfo.getType() + ",tablelist=");
    for (TableName table : backupInfo.getTables()) {
      sb.append(table + ";");
    }
    if (sb.lastIndexOf(";") > 0) {
      sb.delete(sb.lastIndexOf(";"), sb.lastIndexOf(";") + 1);
    }
    sb.append(",targetRootDir=" + backupInfo.getBackupRootDir());

    return sb.toString();
  }

  /**
   * Clean up directories with prefix "_distcp_logs-", which are generated when DistCp copying
   * hlogs.
   * @throws IOException exception
   */
  protected void cleanupDistCpLog(BackupInfo backupInfo, Configuration conf) throws IOException {
    Path rootPath = new Path(backupInfo.getHLogTargetDir()).getParent();
    FileStatus[] files = FSUtils.listStatus(fs, rootPath);
    if (files == null) {
      return;
    }
    for (FileStatus file : files) {
      if (file.getPath().getName().startsWith("_distcp_logs")) {
        LOG.debug("Delete log files of DistCp: " + file.getPath().getName());
        FSUtils.delete(fs, file.getPath(), true);
      }
    }
  }

  /**
   * Complete the overall backup.
   * @param backupInfo backup info
   * @throws IOException exception
   */
  protected void completeBackup(final Connection conn, BackupInfo backupInfo,
      BackupManager backupManager, BackupType type, Configuration conf) throws IOException {
    // set the complete timestamp of the overall backup
    backupInfo.setCompleteTs(EnvironmentEdgeManager.currentTime());
    // set overall backup status: complete
    backupInfo.setState(BackupState.COMPLETE);
    backupInfo.setProgress(100);
    // add and store the manifest for the backup
    addManifest(backupInfo, backupManager, type, conf);

    // compose the backup complete data
    String backupCompleteData =
        obtainBackupMetaDataStr(backupInfo) + ",startts=" + backupInfo.getStartTs()
            + ",completets=" + backupInfo.getCompleteTs() + ",bytescopied="
            + backupInfo.getTotalBytesCopied();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Backup " + backupInfo.getBackupId() + " finished: " + backupCompleteData);
    }

    // when full backup is done:
    // - delete HBase snapshot
    // - clean up directories with prefix "exportSnapshot-", which are generated when exporting
    // snapshots
    if (type == BackupType.FULL) {
      deleteSnapshots(conn, backupInfo, conf);
      cleanupExportSnapshotLog(conf);
    } else if (type == BackupType.INCREMENTAL) {
      cleanupDistCpLog(backupInfo, conf);
    }
    BackupSystemTable.deleteSnapshot(conn);
    backupManager.updateBackupInfo(backupInfo);

    // Finish active session
    backupManager.finishBackupSession();

    LOG.info("Backup " + backupInfo.getBackupId() + " completed.");
  }

  /**
   * Backup request execution.
   *
   * @throws IOException if the execution of the backup fails
   */
  public abstract void execute() throws IOException;

  @VisibleForTesting
  protected Stage getTestStage() {
    return Stage.valueOf("stage_"+ conf.getInt(BACKUP_TEST_MODE_STAGE, 0));
  }

  @VisibleForTesting
  protected void failStageIf(Stage stage) throws IOException {
    Stage current = getTestStage();
    if (current == stage) {
      throw new IOException("Failed stage " + stage+" in testing");
    }
  }

  public enum Stage {
    stage_0, stage_1, stage_2, stage_3, stage_4
  }
}
