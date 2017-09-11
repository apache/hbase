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

import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.BACKUP_ATTEMPTS_PAUSE_MS_KEY;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.BACKUP_MAX_ATTEMPTS_KEY;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.DEFAULT_BACKUP_ATTEMPTS_PAUSE_MS;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.DEFAULT_BACKUP_MAX_ATTEMPTS;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupCopyJob;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupPhase;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupState;
import org.apache.hadoop.hbase.backup.BackupRequest;
import org.apache.hadoop.hbase.backup.BackupRestoreFactory;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.backup.master.LogRollMasterProcedureManager;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * Full table backup implementation
 *
 */
@InterfaceAudience.Private
public class FullTableBackupClient extends TableBackupClient {
  private static final Log LOG = LogFactory.getLog(FullTableBackupClient.class);

  public FullTableBackupClient() {
  }

  public FullTableBackupClient(final Connection conn, final String backupId, BackupRequest request)
      throws IOException {
    super(conn, backupId, request);
  }

  /**
   * Do snapshot copy.
   * @param backupInfo backup info
   * @throws Exception exception
   */
  protected void snapshotCopy(BackupInfo backupInfo) throws Exception {
    LOG.info("Snapshot copy is starting.");

    // set overall backup phase: snapshot_copy
    backupInfo.setPhase(BackupPhase.SNAPSHOTCOPY);

    // call ExportSnapshot to copy files based on hbase snapshot for backup
    // ExportSnapshot only support single snapshot export, need loop for multiple tables case
    BackupCopyJob copyService = BackupRestoreFactory.getBackupCopyJob(conf);

    // number of snapshots matches number of tables
    float numOfSnapshots = backupInfo.getSnapshotNames().size();

    LOG.debug("There are " + (int) numOfSnapshots + " snapshots to be copied.");

    for (TableName table : backupInfo.getTables()) {
      // Currently we simply set the sub copy tasks by counting the table snapshot number, we can
      // calculate the real files' size for the percentage in the future.
      // backupCopier.setSubTaskPercntgInWholeTask(1f / numOfSnapshots);
      int res = 0;
      String[] args = new String[4];
      args[0] = "-snapshot";
      args[1] = backupInfo.getSnapshotName(table);
      args[2] = "-copy-to";
      args[3] = backupInfo.getTableBackupDir(table);

      LOG.debug("Copy snapshot " + args[1] + " to " + args[3]);
      res = copyService.copy(backupInfo, backupManager, conf, BackupType.FULL, args);
      // if one snapshot export failed, do not continue for remained snapshots
      if (res != 0) {
        LOG.error("Exporting Snapshot " + args[1] + " failed with return code: " + res + ".");

        throw new IOException("Failed of exporting snapshot " + args[1] + " to " + args[3]
            + " with reason code " + res);
      }
      LOG.info("Snapshot copy " + args[1] + " finished.");
    }
  }

  /**
   * Backup request execution
   * @throws IOException
   */
  @Override
  public void execute() throws IOException {
    try (Admin admin = conn.getAdmin();) {

      // Begin BACKUP
      beginBackup(backupManager, backupInfo);
      String savedStartCode = null;
      boolean firstBackup = false;
      // do snapshot for full table backup

      savedStartCode = backupManager.readBackupStartCode();
      firstBackup = savedStartCode == null || Long.parseLong(savedStartCode) == 0L;
      if (firstBackup) {
        // This is our first backup. Let's put some marker to system table so that we can hold the logs
        // while we do the backup.
        backupManager.writeBackupStartCode(0L);
      }
      // We roll log here before we do the snapshot. It is possible there is duplicate data
      // in the log that is already in the snapshot. But if we do it after the snapshot, we
      // could have data loss.
      // A better approach is to do the roll log on each RS in the same global procedure as
      // the snapshot.
      LOG.info("Execute roll log procedure for full backup ...");

      Map<String, String> props = new HashMap<String, String>();
      props.put("backupRoot", backupInfo.getBackupRootDir());
      admin.execProcedure(LogRollMasterProcedureManager.ROLLLOG_PROCEDURE_SIGNATURE,
        LogRollMasterProcedureManager.ROLLLOG_PROCEDURE_NAME, props);

      newTimestamps = backupManager.readRegionServerLastLogRollResult();
      if (firstBackup) {
        // Updates registered log files
        // We record ALL old WAL files as registered, because
        // this is a first full backup in the system and these
        // files are not needed for next incremental backup
        List<String> logFiles = BackupUtils.getWALFilesOlderThan(conf, newTimestamps);
        backupManager.recordWALFiles(logFiles);
      }

      // SNAPSHOT_TABLES:
      backupInfo.setPhase(BackupPhase.SNAPSHOT);
      for (TableName tableName : tableList) {
        String snapshotName =
            "snapshot_" + Long.toString(EnvironmentEdgeManager.currentTime()) + "_"
                + tableName.getNamespaceAsString() + "_" + tableName.getQualifierAsString();

        snapshotTable(admin, tableName, snapshotName);
        backupInfo.setSnapshotName(tableName, snapshotName);
      }

      // SNAPSHOT_COPY:
      // do snapshot copy
      LOG.debug("snapshot copy for " + backupId);
      snapshotCopy(backupInfo);
      // Updates incremental backup table set
      backupManager.addIncrementalBackupTableSet(backupInfo.getTables());

      // BACKUP_COMPLETE:
      // set overall backup status: complete. Here we make sure to complete the backup.
      // After this checkpoint, even if entering cancel process, will let the backup finished
      backupInfo.setState(BackupState.COMPLETE);
      // The table list in backupInfo is good for both full backup and incremental backup.
      // For incremental backup, it contains the incremental backup table set.
      backupManager.writeRegionServerLogTimestamp(backupInfo.getTables(), newTimestamps);

      HashMap<TableName, HashMap<String, Long>> newTableSetTimestampMap =
          backupManager.readLogTimestampMap();

      Long newStartCode =
          BackupUtils.getMinValue(BackupUtils
              .getRSLogTimestampMins(newTableSetTimestampMap));
      backupManager.writeBackupStartCode(newStartCode);

      // backup complete
      completeBackup(conn, backupInfo, backupManager, BackupType.FULL, conf);
    } catch (Exception e) {
      failBackup(conn, backupInfo, backupManager, e, "Unexpected BackupException : ",
        BackupType.FULL, conf);
      throw new IOException(e);
    }

  }


  protected void snapshotTable(Admin admin, TableName tableName, String snapshotName)
      throws IOException {

    int maxAttempts =
        conf.getInt(BACKUP_MAX_ATTEMPTS_KEY, DEFAULT_BACKUP_MAX_ATTEMPTS);
    int pause =
        conf.getInt(BACKUP_ATTEMPTS_PAUSE_MS_KEY, DEFAULT_BACKUP_ATTEMPTS_PAUSE_MS);
    int attempts = 0;

    while (attempts++ < maxAttempts) {
      try {
        admin.snapshot(snapshotName, tableName);
        return;
      } catch (IOException ee) {
        LOG.warn("Snapshot attempt " + attempts + " failed for table " + tableName
            + ", sleeping for " + pause + "ms", ee);
        if (attempts < maxAttempts) {
          try {
            Thread.sleep(pause);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      }
    }
    throw new IOException("Failed to snapshot table "+ tableName);
  }
}
