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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for backup operation. Concrete implementation for full and incremental backup are
 * delegated to corresponding sub-classes: {@link FullTableBackupClient} and
 * {@link IncrementalTableBackupClient}
 */
@InterfaceAudience.Private
public abstract class TableBackupClient {

  public static final String BACKUP_CLIENT_IMPL_CLASS = "backup.client.impl.class";

  public static final String BACKUP_TEST_MODE_STAGE = "backup.test.mode.stage";

  private static final Logger LOG = LoggerFactory.getLogger(TableBackupClient.class);

  protected Configuration conf;
  protected Connection conn;
  protected String backupId;
  protected List<TableName> tableList;
  protected Map<String, Long> newTimestamps = null;

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
    this.fs = CommonFSUtils.getCurrentFileSystem(conf);
    backupInfo = backupManager.createBackupInfo(backupId, request.getBackupType(), tableList,
      request.getTargetRootDir(), request.getTotalTasks(), request.getBandwidth(),
      request.getNoChecksumVerify(), request.isContinuousBackupEnabled());
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
    FileSystem fs = CommonFSUtils.getCurrentFileSystem(conf);
    Path stagingDir = new Path(
      conf.get(BackupRestoreConstants.CONF_STAGING_ROOT, fs.getWorkingDirectory().toString()));
    FileStatus[] files = CommonFSUtils.listStatus(fs, stagingDir);
    if (files == null) {
      return;
    }
    for (FileStatus file : files) {
      if (file.getPath().getName().startsWith("exportSnapshot-")) {
        LOG.debug("Delete log files of exporting snapshot: " + file.getPath().getName());
        if (CommonFSUtils.delete(fs, file.getPath(), true) == false) {
          LOG.warn("Can not delete " + file.getPath());
        }
      }
    }
  }

  /**
   * Clean up the uncompleted data at target directory if the ongoing backup has already entered the
   * copy phase.
   */
  protected static void cleanupTargetDir(BackupInfo backupInfo, Configuration conf) {
    try {
      // clean up the uncompleted data at target directory if the ongoing backup has already entered
      // the copy phase
      LOG.debug("Trying to cleanup up target dir. Current backup phase: " + backupInfo.getPhase());
      if (
        backupInfo.getPhase().equals(BackupPhase.SNAPSHOTCOPY)
          || backupInfo.getPhase().equals(BackupPhase.INCREMENTAL_COPY)
          || backupInfo.getPhase().equals(BackupPhase.STORE_MANIFEST)
      ) {
        FileSystem outputFs = FileSystem.get(new Path(backupInfo.getBackupRootDir()).toUri(), conf);

        // now treat one backup as a transaction, clean up data that has been partially copied at
        // table level
        for (TableName table : backupInfo.getTables()) {
          Path targetDirPath = new Path(HBackupFileSystem
            .getTableBackupDir(backupInfo.getBackupRootDir(), backupInfo.getBackupId(), table));
          if (outputFs.delete(targetDirPath, true)) {
            LOG.debug(
              "Cleaning up uncompleted backup data at " + targetDirPath.toString() + " done.");
          } else {
            LOG.debug("No data has been copied to " + targetDirPath.toString() + ".");
          }

          Path tableDir = targetDirPath.getParent();
          FileStatus[] backups = CommonFSUtils.listStatus(outputFs, tableDir);
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
   * @param e          exception
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
      String backupFailedData = "BackupId=" + backupInfo.getBackupId() + ",startts="
        + backupInfo.getStartTs() + ",failedts=" + backupInfo.getCompleteTs() + ",failedphase="
        + backupInfo.getPhase() + ",failedmessage=" + backupInfo.getFailedMsg();
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
   * Creates a manifest based on the provided info, and store it in the backup-specific directory.
   * @param backupInfo The current backup info
   * @throws IOException exception
   */
  protected void addManifest(BackupInfo backupInfo, BackupType type, Configuration conf)
    throws IOException {
    // set the overall backup phase : store manifest
    backupInfo.setPhase(BackupPhase.STORE_MANIFEST);

    BackupManifest manifest = new BackupManifest(backupInfo);
    if (type == BackupType.INCREMENTAL) {
      // set the table region server start and end timestamps for incremental backup
      manifest.setIncrTimestampMap(backupInfo.getIncrTimestampMap());
    }
    List<BackupImage> ancestors = getAncestors(backupInfo);
    for (BackupImage image : ancestors) {
      manifest.addDependentImage(image);
    }
    manifest.store(conf);
  }

  /**
   * Gets the direct ancestors of the currently being created backup.
   * @param backupInfo The backup info for the backup being created
   */
  protected List<BackupImage> getAncestors(BackupInfo backupInfo) throws IOException {
    LOG.debug("Getting the direct ancestors of the current backup {}", backupInfo.getBackupId());

    // Full backups do not have ancestors
    if (backupInfo.getType() == BackupType.FULL) {
      LOG.debug("Current backup is a full backup, no direct ancestor for it.");
      return Collections.emptyList();
    }

    List<BackupImage> ancestors = new ArrayList<>();
    Set<TableName> tablesToCover = new HashSet<>(backupInfo.getTables());

    // Go over the backup history list from newest to oldest
    List<BackupInfo> allHistoryList = backupManager.getBackupHistory(true);
    for (BackupInfo backup : allHistoryList) {
      // If the image has a different rootDir, it cannot be an ancestor.
      if (!Objects.equals(backup.getBackupRootDir(), backupInfo.getBackupRootDir())) {
        continue;
      }

      BackupImage.Builder builder = BackupImage.newBuilder();
      BackupImage image = builder.withBackupId(backup.getBackupId()).withType(backup.getType())
        .withRootDir(backup.getBackupRootDir()).withTableList(backup.getTableNames())
        .withStartTime(backup.getStartTs()).withCompleteTime(backup.getCompleteTs()).build();

      // The ancestors consist of the most recent FULL backups that cover the list of tables
      // required in the new backup and all INCREMENTAL backups that came after one of those FULL
      // backups.
      if (backup.getType().equals(BackupType.INCREMENTAL)) {
        ancestors.add(image);
        LOG.debug("Dependent incremental backup image: {BackupID={}}", image.getBackupId());
      } else {
        if (tablesToCover.removeAll(new HashSet<>(image.getTableNames()))) {
          ancestors.add(image);
          LOG.debug("Dependent full backup image: {BackupID={}}", image.getBackupId());

          if (tablesToCover.isEmpty()) {
            LOG.debug("Got {} ancestors for the current backup.", ancestors.size());
            return Collections.unmodifiableList(ancestors);
          }
        }
      }
    }

    throw new IllegalStateException(
      "Unable to find full backup that contains tables: " + tablesToCover);
  }

  /**
   * Get backup request meta data dir as string.
   * @param backupInfo backup info
   * @return meta data dir
   */
  protected String obtainBackupMetaDataStr(BackupInfo backupInfo) {
    StringBuilder sb = new StringBuilder();
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
   * Complete the overall backup.
   * @param backupInfo backup info
   * @throws IOException exception
   */
  protected void completeBackup(final Connection conn, BackupInfo backupInfo, BackupType type,
    Configuration conf) throws IOException {
    // set the complete timestamp of the overall backup
    backupInfo.setCompleteTs(EnvironmentEdgeManager.currentTime());
    // set overall backup status: complete
    backupInfo.setState(BackupState.COMPLETE);
    backupInfo.setProgress(100);
    // add and store the manifest for the backup
    addManifest(backupInfo, type, conf);

    // compose the backup complete data
    String backupCompleteData =
      obtainBackupMetaDataStr(backupInfo) + ",startts=" + backupInfo.getStartTs() + ",completets="
        + backupInfo.getCompleteTs() + ",bytescopied=" + backupInfo.getTotalBytesCopied();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Backup " + backupInfo.getBackupId() + " finished: " + backupCompleteData);
    }

    // when full backup is done:
    // - delete HBase snapshot
    // - clean up directories with prefix "exportSnapshot-", which are generated when exporting
    // snapshots
    // incremental backups use distcp, which handles cleaning up its own directories
    if (type == BackupType.FULL) {
      deleteSnapshots(conn, backupInfo, conf);
      cleanupExportSnapshotLog(conf);
    }
    BackupSystemTable.deleteSnapshot(conn);
    backupManager.updateBackupInfo(backupInfo);

    // Finish active session
    backupManager.finishBackupSession();

    LOG.info("Backup " + backupInfo.getBackupId() + " completed.");
  }

  /**
   * Backup request execution.
   * @throws IOException if the execution of the backup fails
   */
  public abstract void execute() throws IOException;

  protected Stage getTestStage() {
    return Stage.valueOf("stage_" + conf.getInt(BACKUP_TEST_MODE_STAGE, 0));
  }

  protected void failStageIf(Stage stage) throws IOException {
    Stage current = getTestStage();
    if (current == stage) {
      throw new IOException("Failed stage " + stage + " in testing");
    }
  }

  public enum Stage {
    stage_0,
    stage_1,
    stage_2,
    stage_3,
    stage_4
  }
}
