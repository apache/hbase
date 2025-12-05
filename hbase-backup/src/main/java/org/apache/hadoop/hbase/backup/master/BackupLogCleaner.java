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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupRestoreConstants;
import org.apache.hadoop.hbase.backup.impl.BackupManager;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.cleaner.BaseLogCleanerDelegate;
import org.apache.hadoop.hbase.master.region.MasterRegionFactory;
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
  // Adds a buffer to give some allowance when cleaning up WAL files
  public static final String TS_BUFFER_KEY = "hbase.backup.log.cleaner.timestamp.buffer.ms";
  private static final long TS_BUFFER_DEFAULT = Duration.ofHours(1).toMillis();

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

    long oldestStartCode = Long.MAX_VALUE;
    try {
      try (BackupSystemTable sysTable = new BackupSystemTable(conn)) {
        Set<String> roots = sysTable.getBackupHistory(true).stream()
          .map(BackupInfo::getBackupRootDir).collect(Collectors.toSet());
        if (roots.isEmpty()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("No backups found, can delete all files");
          }
          return files;
        }

        for (String root : roots) {
          long startCode = Long.parseLong(sysTable.readBackupStartCode(root));
          if (startCode < oldestStartCode) {
            oldestStartCode = startCode;
          }
        }
      }
    } catch (IOException ex) {
      LOG.error("Failed to analyse backup history with exception: {}. Retaining all logs",
        ex.getMessage(), ex);
      return Collections.emptyList();
    }

    oldestStartCode -= getConf().getLong(TS_BUFFER_KEY, TS_BUFFER_DEFAULT);
    LOG.info("Allowing file deletes for any wal file older than {}", oldestStartCode);

    for (FileStatus file : files) {
      if (canDeleteFile(oldestStartCode, file.getPath())) {
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

  protected static boolean canDeleteFile(long oldestStartCode, Path path) {
    if (isHMasterWAL(path)) {
      return true;
    }

    try {
      long walTimestamp = AbstractFSWALProvider.getTimestamp(path.getName());
      if (walTimestamp <= oldestStartCode) {
        return true;
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
      || fn.endsWith(MasterRegionFactory.ARCHIVED_WAL_SUFFIX)
      || path.toString().contains(MasterRegionFactory.MASTER_STORE_DIR);
  }
}
