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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.backup.BackupSystemTable;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.master.cleaner.BaseLogCleanerDelegate;




/**
 * Implementation of a log cleaner that checks if a log is still scheduled for
 * incremental backup before deleting it when its TTL is over.
 */
@InterfaceStability.Evolving
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class BackupLogCleaner extends BaseLogCleanerDelegate {
  private static final Log LOG = LogFactory.getLog(BackupLogCleaner.class);

  private boolean stopped = false;

  public BackupLogCleaner() {
  }

  @Override
  public Iterable<FileStatus> getDeletableFiles(Iterable<FileStatus> files) {
    // all members of this class are null if backup is disabled,
    // so we cannot filter the files
    if (this.getConf() == null) {
      return files;
    }

    try {
      final BackupSystemTable table = BackupSystemTable.getTable(getConf());
      // If we do not have recorded backup sessions
      if (table.hasBackupSessions() == false) {
        return files;
      }
      return Iterables.filter(files, new Predicate<FileStatus>() {
        @Override
        public boolean apply(FileStatus file) {
          try {
            String wal = file.getPath().toString();
            boolean logInSystemTable = table.checkWALFile(wal);
            if (LOG.isDebugEnabled()) {
              if (logInSystemTable) {
                LOG.debug("Found log file in hbase:backup, deleting: " + wal);
              } else {
                LOG.debug("Didn't find this log in hbase:backup, keeping: " + wal);
              }
            }
            return logInSystemTable;
          } catch (IOException e) {
            LOG.error(e);
            return false;// keep file for a while, HBase failed
          }
        }
      });
    } catch (IOException e) {
      LOG.error("Failed to get hbase:backup table, therefore will keep all files", e);
      // nothing to delete
      return new ArrayList<FileStatus>();
    }

  }

  @Override
  public void setConf(Configuration config) {
    // If backup is disabled, keep all members null
    if (!config.getBoolean(HConstants.BACKUP_ENABLE_KEY, HConstants.BACKUP_ENABLE_DEFAULT)) {
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
