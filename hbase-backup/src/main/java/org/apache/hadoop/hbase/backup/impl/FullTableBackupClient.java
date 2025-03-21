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

import static org.apache.hadoop.hbase.HConstants.REPLICATION_SCOPE_GLOBAL;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.BACKUP_ATTEMPTS_PAUSE_MS_KEY;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.BACKUP_MAX_ATTEMPTS_KEY;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.CONF_CONTINUOUS_BACKUP_WAL_DIR;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.CONTINUOUS_BACKUP_REPLICATION_PEER;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.DEFAULT_BACKUP_ATTEMPTS_PAUSE_MS;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.DEFAULT_BACKUP_MAX_ATTEMPTS;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.DEFAULT_CONTINUOUS_BACKUP_REPLICATION_ENDPOINT;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.JOB_NAME_CONF_KEY;
import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupReplicationEndpoint.CONF_BACKUP_ROOT_DIR;
import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupReplicationEndpoint.CONF_PEER_UUID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
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
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Full table backup implementation
 */
@InterfaceAudience.Private
public class FullTableBackupClient extends TableBackupClient {
  private static final Logger LOG = LoggerFactory.getLogger(FullTableBackupClient.class);

  public FullTableBackupClient() {
  }

  public FullTableBackupClient(final Connection conn, final String backupId, BackupRequest request)
    throws IOException {
    super(conn, backupId, request);
  }

  /**
   * Do snapshot copy.
   * @param backupInfo backup info
   * @throws IOException exception
   */
  protected void snapshotCopy(BackupInfo backupInfo) throws IOException {
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
      int res;
      ArrayList<String> argsList = new ArrayList<>();
      argsList.add("-snapshot");
      argsList.add(backupInfo.getSnapshotName(table));
      argsList.add("-copy-to");
      argsList.add(backupInfo.getTableBackupDir(table));
      if (backupInfo.getBandwidth() > -1) {
        argsList.add("-bandwidth");
        argsList.add(String.valueOf(backupInfo.getBandwidth()));
      }
      if (backupInfo.getWorkers() > -1) {
        argsList.add("-mappers");
        argsList.add(String.valueOf(backupInfo.getWorkers()));
      }
      if (backupInfo.getNoChecksumVerify()) {
        argsList.add("-no-checksum-verify");
      }

      String[] args = argsList.toArray(new String[0]);

      String jobname = "Full-Backup_" + backupInfo.getBackupId() + "_" + table.getNameAsString();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Setting snapshot copy job name to : " + jobname);
      }
      conf.set(JOB_NAME_CONF_KEY, jobname);

      LOG.debug("Copy snapshot " + args[1] + " to " + args[3]);
      res = copyService.copy(backupInfo, backupManager, conf, BackupType.FULL, args);

      // if one snapshot export failed, do not continue for remained snapshots
      if (res != 0) {
        LOG.error("Exporting Snapshot " + args[1] + " failed with return code: " + res + ".");

        throw new IOException("Failed of exporting snapshot " + args[1] + " to " + args[3]
          + " with reason code " + res);
      }

      conf.unset(JOB_NAME_CONF_KEY);
      LOG.info("Snapshot copy " + args[1] + " finished.");
    }
  }

  /**
   * Backup request execution.
   * @throws IOException if the execution of the backup fails
   */
  @Override
  public void execute() throws IOException {
    try (Admin admin = conn.getAdmin()) {
      beginBackup(backupManager, backupInfo);

      if (backupInfo.isContinuousBackupEnabled()) {
        handleContinuousBackup(admin);
      } else {
        handleNonContinuousBackup(admin);
      }

      completeBackup(conn, backupInfo, BackupType.FULL, conf);
    } catch (Exception e) {
      failBackup(conn, backupInfo, backupManager, e, "Unexpected BackupException : ",
        BackupType.FULL, conf);
      throw new IOException(e);
    }
  }

  private void handleContinuousBackup(Admin admin) throws IOException {
    backupInfo.setPhase(BackupInfo.BackupPhase.SETUP_WAL_REPLICATION);
    long startTimestamp = startContinuousWALBackup(admin);
    backupManager.addContinuousBackupTableSet(backupInfo.getTables(), startTimestamp);

    // Updating the start time of this backup to reflect the actual beginning of the full backup.
    // So far, we have only set up continuous WAL replication, but the full backup has not yet
    // started.
    // Setting the correct start time is crucial for Point-In-Time Recovery (PITR).
    // When selecting a backup for PITR, we must ensure that the backup started **on or after** the
    // starting time of the WALs. If WAL streaming began later, we couldn't guarantee that WALs
    // exist for the entire period between the backup's start time and the desired PITR timestamp.
    backupInfo.setStartTs(startTimestamp);

    performBackupSnapshots(admin);

    // set overall backup status: complete. Here we make sure to complete the backup.
    // After this checkpoint, even if entering cancel process, will let the backup finished
    backupInfo.setState(BackupState.COMPLETE);

    if (!conf.getBoolean("hbase.replication.bulkload.enabled", false)) {
      System.out.println("NOTE: Bulkload replication is not enabled. "
        + "Bulk loaded files will not be backed up as part of continuous backup. "
        + "To ensure bulk loaded files are included in the backup, please enable bulkload replication "
        + "(hbase.replication.bulkload.enabled=true) and configure other necessary settings "
        + "to properly enable bulkload replication.");
    }
  }

  private void handleNonContinuousBackup(Admin admin) throws IOException {
    initializeBackupStartCode(backupManager);
    performLogRoll(admin);
    performBackupSnapshots(admin);
    backupManager.addIncrementalBackupTableSet(backupInfo.getTables());

    // set overall backup status: complete. Here we make sure to complete the backup.
    // After this checkpoint, even if entering cancel process, will let the backup finished
    backupInfo.setState(BackupState.COMPLETE);

    updateBackupMetadata();
  }

  private void initializeBackupStartCode(BackupManager backupManager) throws IOException {
    String savedStartCode;
    boolean firstBackup;
    // do snapshot for full table backup
    savedStartCode = backupManager.readBackupStartCode();
    firstBackup = savedStartCode == null || Long.parseLong(savedStartCode) == 0L;
    if (firstBackup) {
      // This is our first backup. Let's put some marker to system table so that we can hold the
      // logs while we do the backup.
      backupManager.writeBackupStartCode(0L);
    }
  }

  private void performLogRoll(Admin admin) throws IOException {
    // We roll log here before we do the snapshot. It is possible there is duplicate data
    // in the log that is already in the snapshot. But if we do it after the snapshot, we
    // could have data loss.
    // A better approach is to do the roll log on each RS in the same global procedure as
    // the snapshot.
    LOG.info("Execute roll log procedure for full backup ...");
    Map<String, String> props = new HashMap<>();
    props.put("backupRoot", backupInfo.getBackupRootDir());
    admin.execProcedure(LogRollMasterProcedureManager.ROLLLOG_PROCEDURE_SIGNATURE,
      LogRollMasterProcedureManager.ROLLLOG_PROCEDURE_NAME, props);

    newTimestamps = backupManager.readRegionServerLastLogRollResult();
  }

  private void performBackupSnapshots(Admin admin) throws IOException {
    backupInfo.setPhase(BackupPhase.SNAPSHOT);
    performSnapshots(admin);
    LOG.debug("Performing snapshot copy for backup ID: {}", backupInfo.getBackupId());
    snapshotCopy(backupInfo);
  }

  private void performSnapshots(Admin admin) throws IOException {
    backupInfo.setPhase(BackupPhase.SNAPSHOT);

    for (TableName tableName : tableList) {
      String snapshotName = String.format("snapshot_%d_%s_%s", EnvironmentEdgeManager.currentTime(),
        tableName.getNamespaceAsString(), tableName.getQualifierAsString());
      snapshotTable(admin, tableName, snapshotName);
      backupInfo.setSnapshotName(tableName, snapshotName);
    }
  }

  private void updateBackupMetadata() throws IOException {
    // The table list in backupInfo is good for both full backup and incremental backup.
    // For incremental backup, it contains the incremental backup table set.
    backupManager.writeRegionServerLogTimestamp(backupInfo.getTables(), newTimestamps);
    Map<TableName, Map<String, Long>> timestampMap = backupManager.readLogTimestampMap();
    backupInfo.setTableSetTimestampMap(timestampMap);
    Long newStartCode = BackupUtils.getMinValue(BackupUtils.getRSLogTimestampMins(timestampMap));
    backupManager.writeBackupStartCode(newStartCode);
  }

  private long startContinuousWALBackup(Admin admin) throws IOException {
    enableTableReplication(admin);
    if (continuousBackupReplicationPeerExists(admin)) {
      updateContinuousBackupReplicationPeer(admin);
    } else {
      addContinuousBackupReplicationPeer(admin);
    }
    LOG.info("Continuous WAL Backup setup completed.");
    return EnvironmentEdgeManager.getDelegate().currentTime();
  }

  private void enableTableReplication(Admin admin) throws IOException {
    for (TableName table : tableList) {
      TableDescriptor tableDescriptor = admin.getDescriptor(table);
      TableDescriptorBuilder tableDescriptorBuilder =
        TableDescriptorBuilder.newBuilder(tableDescriptor);

      for (ColumnFamilyDescriptor cfDescriptor : tableDescriptor.getColumnFamilies()) {
        if (cfDescriptor.getScope() != REPLICATION_SCOPE_GLOBAL) {
          ColumnFamilyDescriptor newCfDescriptor = ColumnFamilyDescriptorBuilder
            .newBuilder(cfDescriptor).setScope(REPLICATION_SCOPE_GLOBAL).build();

          tableDescriptorBuilder.modifyColumnFamily(newCfDescriptor);
        }
      }

      admin.modifyTable(tableDescriptorBuilder.build());
      LOG.info("Enabled Global replication scope for table: {}", table);
    }
  }

  private void updateContinuousBackupReplicationPeer(Admin admin) throws IOException {
    Map<TableName, List<String>> tableMap = tableList.stream()
      .collect(Collectors.toMap(tableName -> tableName, tableName -> new ArrayList<>()));

    try {
      admin.appendReplicationPeerTableCFs(CONTINUOUS_BACKUP_REPLICATION_PEER, tableMap);
      LOG.info("Updated replication peer {} with table and column family map.",
        CONTINUOUS_BACKUP_REPLICATION_PEER);
    } catch (ReplicationException e) {
      LOG.error("Error while updating the replication peer: {}. Error: {}",
        CONTINUOUS_BACKUP_REPLICATION_PEER, e.getMessage(), e);
      throw new IOException("Error while updating the continuous backup replication peer.", e);
    }
  }

  private void addContinuousBackupReplicationPeer(Admin admin) throws IOException {
    String backupWalDir = conf.get(CONF_CONTINUOUS_BACKUP_WAL_DIR);

    if (backupWalDir == null || backupWalDir.isEmpty()) {
      String errorMsg = "WAL Directory is not specified for continuous backup.";
      LOG.error(errorMsg);
      throw new IOException(errorMsg);
    }

    Map<String, String> additionalArgs = new HashMap<>();
    additionalArgs.put(CONF_PEER_UUID, UUID.randomUUID().toString());
    additionalArgs.put(CONF_BACKUP_ROOT_DIR, backupWalDir);

    Map<TableName, List<String>> tableMap = tableList.stream()
      .collect(Collectors.toMap(tableName -> tableName, tableName -> new ArrayList<>()));

    ReplicationPeerConfig peerConfig = ReplicationPeerConfig.newBuilder()
      .setReplicationEndpointImpl(DEFAULT_CONTINUOUS_BACKUP_REPLICATION_ENDPOINT)
      .setReplicateAllUserTables(false).setTableCFsMap(tableMap).putAllConfiguration(additionalArgs)
      .build();

    try {
      admin.addReplicationPeer(CONTINUOUS_BACKUP_REPLICATION_PEER, peerConfig, true);
      LOG.info("Successfully added replication peer with ID: {}",
        CONTINUOUS_BACKUP_REPLICATION_PEER);
    } catch (IOException e) {
      LOG.error("Failed to add replication peer with ID: {}. Error: {}",
        CONTINUOUS_BACKUP_REPLICATION_PEER, e.getMessage(), e);
      throw e;
    }
  }

  private boolean continuousBackupReplicationPeerExists(Admin admin) throws IOException {
    return admin.listReplicationPeers().stream()
      .anyMatch(peer -> peer.getPeerId().equals(CONTINUOUS_BACKUP_REPLICATION_PEER));
  }

  protected void snapshotTable(Admin admin, TableName tableName, String snapshotName)
    throws IOException {
    int maxAttempts = conf.getInt(BACKUP_MAX_ATTEMPTS_KEY, DEFAULT_BACKUP_MAX_ATTEMPTS);
    int pause = conf.getInt(BACKUP_ATTEMPTS_PAUSE_MS_KEY, DEFAULT_BACKUP_ATTEMPTS_PAUSE_MS);
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
    throw new IOException("Failed to snapshot table " + tableName);
  }
}
