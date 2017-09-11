/**
 *
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
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.backup.BackupRestoreConstants;
import org.apache.hadoop.hbase.backup.impl.BackupManager;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.cleaner.BaseLogCleanerDelegate;

/**
 * Implementation of a log cleaner that checks if a log is still scheduled for incremental backup
 * before deleting it when its TTL is over.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class BackupLogCleaner extends BaseLogCleanerDelegate {
  private static final Log LOG = LogFactory.getLog(BackupLogCleaner.class);

  private boolean stopped = false;
  private Connection conn;

  public BackupLogCleaner() {
  }

  @Override
  public void init(Map<String, Object> params) {
    if (params != null && params.containsKey(HMaster.MASTER)) {
      MasterServices master = (MasterServices) params.get(HMaster.MASTER);
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
    // all members of this class are null if backup is disabled,
    // so we cannot filter the files
    if (this.getConf() == null || !BackupManager.isBackupEnabled(getConf())) {
      LOG.warn("Backup is not enabled. Check your " + BackupRestoreConstants.BACKUP_ENABLE_KEY
          + " setting");
      return files;
    }

    List<FileStatus> list = new ArrayList<FileStatus>();
    try (final BackupSystemTable table = new BackupSystemTable(conn)) {
      // If we do not have recorded backup sessions
      try {
        if (!table.hasBackupSessions()) {
          LOG.trace("BackupLogCleaner has no backup sessions");
          return files;
        }
      } catch (TableNotFoundException tnfe) {
        LOG.warn("backup system table is not available" + tnfe.getMessage());
        return files;
      }

      for (FileStatus file : files) {
        String wal = file.getPath().toString();
        boolean logInSystemTable = table.isWALFileDeletable(wal);
        if (LOG.isDebugEnabled()) {
          if (logInSystemTable) {
            LOG.debug("Found log file in backup system table, deleting: " + wal);
            list.add(file);
          } else {
            LOG.debug("Didn't find this log in backup system table, keeping: " + wal);
          }
        }
      }
      return list;
    } catch (IOException e) {
      LOG.error("Failed to get backup system table table, therefore will keep all files", e);
      // nothing to delete
      return new ArrayList<FileStatus>();
    }
  }

  @Override
  public void setConf(Configuration config) {
    // If backup is disabled, keep all members null
    if (!config.getBoolean(BackupRestoreConstants.BACKUP_ENABLE_KEY,
      BackupRestoreConstants.BACKUP_ENABLE_DEFAULT)) {
      LOG.warn("Backup is disabled - allowing all wals to be deleted");
      return;
    }
    super.setConf(config);
  }

  @Override
  public void stop(String why) {
    if (this.stopped) {
      return;
    }
    this.stopped = true;
    LOG.info("Stopping BackupLogCleaner");
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }

}
