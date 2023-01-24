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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
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
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore;
import org.apache.hadoop.hbase.wal.WAL;
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

  private Map<Address, Long> getServersToOldestBackupMapping(List<BackupInfo> backups)
    throws IOException {
    Map<Address, Long> serverAddressToLastBackupMap = new HashMap<>();

    Map<TableName, Long> tableNameBackupInfoMap = new HashMap<>();
    for (BackupInfo backupInfo : backups) {
      for (TableName table : backupInfo.getTables()) {
        tableNameBackupInfoMap.putIfAbsent(table, backupInfo.getStartTs());
        if (tableNameBackupInfoMap.get(table) <= backupInfo.getStartTs()) {
          tableNameBackupInfoMap.put(table, backupInfo.getStartTs());
          for (Map.Entry<String, Long> entry : backupInfo.getTableSetTimestampMap().get(table)
            .entrySet()) {
            serverAddressToLastBackupMap.put(Address.fromString(entry.getKey()), entry.getValue());
          }
        }
      }
    }

    return serverAddressToLastBackupMap;
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

    Map<Address, Long> addressToLastBackupMap;
    try {
      try (BackupManager backupManager = new BackupManager(conn, getConf())) {
        addressToLastBackupMap =
          getServersToOldestBackupMapping(backupManager.getBackupHistory(true));
      }
    } catch (IOException ex) {
      LOG.error("Failed to analyse backup history with exception: {}. Retaining all logs",
        ex.getMessage(), ex);
      return Collections.emptyList();
    }
    for (FileStatus file : files) {
      String fn = file.getPath().getName();
      if (fn.startsWith(WALProcedureStore.LOG_PREFIX)) {
        filteredFiles.add(file);
        continue;
      }

      try {
        Address walServerAddress =
          Address.fromString(BackupUtils.parseHostNameFromLogFile(file.getPath()));
        long walTimestamp = WAL.getTimestamp(file.getPath().getName());

        if (
          !addressToLastBackupMap.containsKey(walServerAddress)
            || addressToLastBackupMap.get(walServerAddress) >= walTimestamp
        ) {
          filteredFiles.add(file);
        }
      } catch (Exception ex) {
        LOG.warn(
          "Error occurred while filtering file: {} with error: {}. Ignoring cleanup of this log",
          file.getPath(), ex.getMessage());
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
}
