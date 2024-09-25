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
package org.apache.hadoop.hbase.backup.master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupRestoreConstants;
import org.apache.hadoop.hbase.backup.impl.BackupManager;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.cleaner.BaseLogCleanerDelegate;
import org.apache.hadoop.hbase.master.region.MasterRegionFactory;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.org.apache.commons.collections4.IterableUtils;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.MapUtils;

/**
 * Implementation of a log cleaner that checks if a log is still scheduled for incremental backup
 * before deleting it when its TTL is over.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class BackupLogCleaner extends BaseLogCleanerDelegate {
  private static final Logger LOG = LoggerFactory.getLogger(BackupLogCleaner.class);

  private boolean stopped = false;
  private Connection conn;

  public BackupLogCleaner() {
  }

  @Override
  public void init(Map<String, Object> params) {
    MasterServices master = (MasterServices) MapUtils.getObject(params, HMaster.MASTER);
    if (master != null) {
      conn = master.getConnection();
      if (getConf() == null) {
        super.setConf(conn.getConfiguration());
      }
    }
    if (conn == null) {
      try {
        conn = ConnectionFactory.createConnection(getConf());
      } catch (IOException ioe) {
        throw new RuntimeException("Failed to create connection", ioe);
      }
    }
  }

  /**
   * Calculates the timestamp boundary up to which all backup roots have already included the WAL.
   * I.e. WALs with a lower (= older) or equal timestamp are no longer needed for future incremental
   * backups.
   */
  private Map<Address, Long> serverToPreservationBoundaryTs(List<BackupInfo> backups)
    throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(
        "Cleaning WALs if they are older than the WAL cleanup time-boundary. "
          + "Checking WALs against {} backups: {}",
        backups.size(),
        backups.stream().map(BackupInfo::getBackupId).sorted().collect(Collectors.joining(", ")));
    }

    // This map tracks, for every backup root, the most recent created backup (= highest timestamp)
    Map<String, BackupInfo> newestBackupPerRootDir = new HashMap<>();
    for (BackupInfo backup : backups) {
      BackupInfo existingEntry = newestBackupPerRootDir.get(backup.getBackupRootDir());
      if (existingEntry == null || existingEntry.getStartTs() < backup.getStartTs()) {
        newestBackupPerRootDir.put(backup.getBackupRootDir(), backup);
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("WAL cleanup time-boundary using info from: {}. ",
        newestBackupPerRootDir.entrySet().stream()
          .map(e -> "Backup root " + e.getKey() + ": " + e.getValue().getBackupId()).sorted()
          .collect(Collectors.joining(", ")));
    }

    // This map tracks, for every RegionServer, the least recent (= oldest / lowest timestamp)
    // inclusion in any backup. In other words, it is the timestamp boundary up to which all backup
    // roots have included the WAL in their backup.
    Map<Address, Long> boundaries = new HashMap<>();
    for (BackupInfo backupInfo : newestBackupPerRootDir.values()) {
      // Iterate over all tables in the timestamp map, which contains all tables covered in the
      // backup root, not just the tables included in that specific backup (which could be a subset)
      for (TableName table : backupInfo.getTableSetTimestampMap().keySet()) {
        for (Map.Entry<String, Long> entry : backupInfo.getTableSetTimestampMap().get(table)
          .entrySet()) {
          Address address = Address.fromString(entry.getKey());
          Long storedTs = boundaries.get(address);
          if (storedTs == null || entry.getValue() < storedTs) {
            boundaries.put(address, entry.getValue());
          }
        }
      }
    }

    if (LOG.isDebugEnabled()) {
      for (Map.Entry<Address, Long> entry : boundaries.entrySet()) {
        LOG.debug("Server: {}, WAL cleanup boundary: {}", entry.getKey().getHostName(),
          entry.getValue());
      }
    }

    return boundaries;
  }

  @Override
  public Iterable<FileStatus> getDeletableFiles(Iterable<FileStatus> files) {
    List<FileStatus> filteredFiles = new ArrayList<>();

    // all members of this class are null if backup is disabled,
    // so we cannot filter the files
    if (this.getConf() == null || !BackupManager.isBackupEnabled(getConf())) {
      LOG.debug("Backup is not enabled. Check your {} setting",
        BackupRestoreConstants.BACKUP_ENABLE_KEY);
      return files;
    }

    Map<Address, Long> serverToPreservationBoundaryTs;
    try {
      try (BackupManager backupManager = new BackupManager(conn, getConf())) {
        serverToPreservationBoundaryTs =
          serverToPreservationBoundaryTs(backupManager.getBackupHistory(true));
      }
    } catch (IOException ex) {
      LOG.error("Failed to analyse backup history with exception: {}. Retaining all logs",
        ex.getMessage(), ex);
      return Collections.emptyList();
    }
    for (FileStatus file : files) {
      if (canDeleteFile(serverToPreservationBoundaryTs, file.getPath())) {
        filteredFiles.add(file);
      }
    }

    LOG.info("Total files: {}, Filtered Files: {}", IterableUtils.size(files),
      filteredFiles.size());
    return filteredFiles;
  }

  @Override
  public void setConf(Configuration config) {
    // If backup is disabled, keep all members null
    super.setConf(config);
    if (
      !config.getBoolean(BackupRestoreConstants.BACKUP_ENABLE_KEY,
        BackupRestoreConstants.BACKUP_ENABLE_DEFAULT)
    ) {
      LOG.warn("Backup is disabled - allowing all wals to be deleted");
    }
  }

  @Override
  public void stop(String why) {
    if (!this.stopped) {
      this.stopped = true;
      LOG.info("Stopping BackupLogCleaner");
    }
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }

  protected static boolean canDeleteFile(Map<Address, Long> addressToBoundaryTs, Path path) {
    if (isHMasterWAL(path)) {
      return true;
    }

    try {
      String hostname = BackupUtils.parseHostNameFromLogFile(path);
      if (hostname == null) {
        LOG.warn(
          "Cannot parse hostname from RegionServer WAL file: {}. Ignoring cleanup of this log",
          path);
        return false;
      }
      Address walServerAddress = Address.fromString(hostname);
      long walTimestamp = AbstractFSWALProvider.getTimestamp(path.getName());

      if (!addressToBoundaryTs.containsKey(walServerAddress)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("No cleanup WAL time-boundary found for server: {}. Ok to delete file: {}",
            walServerAddress.getHostName(), path);
        }
        return true;
      }

      Long backupBoundary = addressToBoundaryTs.get(walServerAddress);
      if (backupBoundary >= walTimestamp) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(
            "WAL cleanup time-boundary found for server {}: {}. Ok to delete older file: {}",
            walServerAddress.getHostName(), backupBoundary, path);
        }
        return true;
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("WAL cleanup time-boundary found for server {}: {}. Keeping younger file: {}",
          walServerAddress.getHostName(), backupBoundary, path);
      }
    } catch (Exception ex) {
      LOG.warn("Error occurred while filtering file: {}. Ignoring cleanup of this log", path, ex);
      return false;
    }
    return false;
  }

  private static boolean isHMasterWAL(Path path) {
    String fn = path.getName();
    return fn.startsWith(WALProcedureStore.LOG_PREFIX)
      || fn.endsWith(MasterRegionFactory.ARCHIVED_WAL_SUFFIX);
  }
}
