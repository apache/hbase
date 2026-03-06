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
package org.apache.hadoop.hbase.backup;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.backup.impl.BulkLoad;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.master.cleaner.BaseHFileCleanerDelegate;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;

/**
 * File cleaner that prevents deletion of HFiles that are still required by future incremental
 * backups.
 * <p>
 * Bulk loaded HFiles that are needed by future updates are stored in the backup system table.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class BackupHFileCleaner extends BaseHFileCleanerDelegate implements Abortable {
  private static final Logger LOG = LoggerFactory.getLogger(BackupHFileCleaner.class);

  private boolean stopped = false;
  private boolean aborted = false;
  private Connection connection;
  private volatile long previousCleaningCompletionTimestamp = 0;
  private volatile Set<String> bulkLoadedHFilesNeedingBackup = null;

  @Override
  public void preClean() {
    if (stopped) {
      return;
    }
    try (BackupSystemTable tbl = new BackupSystemTable(connection)) {
      Set<TableName> tablesIncludedInBackups = fetchFullyBackedUpTables(tbl);
      Set<String> hfileFilenames = new HashSet<>();
      for (BulkLoad bulkLoad : tbl.readBulkloadRows(tablesIncludedInBackups)) {
        String path = bulkLoad.getHfilePath();
        hfileFilenames.add(path.substring(path.lastIndexOf('/') + 1));
      }
      bulkLoadedHFilesNeedingBackup = hfileFilenames;
      LOG.debug("Cached {} unique HFile filenames registered as bulk loads.",
        hfileFilenames.size());
    } catch (IOException ioe) {
      LOG.error(
        "Failed to read registered bulk load references from backup system table, "
          + "marking all files as non-deletable.",
        ioe);
      bulkLoadedHFilesNeedingBackup = null;
    }
  }

  @Override
  public void postClean() {
    previousCleaningCompletionTimestamp = EnvironmentEdgeManager.currentTime();
    bulkLoadedHFilesNeedingBackup = null;
  }

  @Override
  public Iterable<FileStatus> getDeletableFiles(Iterable<FileStatus> files) {
    if (stopped) {
      return Collections.emptyList();
    }

    final Set<String> hfilesNeedingBackup = bulkLoadedHFilesNeedingBackup;
    if (hfilesNeedingBackup == null) {
      return Collections.emptyList();
    }

    final long recentFileThreshold = previousCleaningCompletionTimestamp;

    return Iterables.filter(files, file -> {
      if (file.getModificationTime() > recentFileThreshold) {
        LOG.debug("Preventing deletion due to timestamp: {}", file.getPath().toString());
        return false;
      }
      String hfile = file.getPath().getName();
      if (hfilesNeedingBackup.contains(hfile)) {
        LOG.debug("Preventing deletion due to bulk load registration in backup system table: {}",
          file.getPath().toString());
        return false;
      } else {
        LOG.debug("OK to delete: {}", file.getPath().toString());
        return true;
      }
    });
  }

  protected Set<TableName> fetchFullyBackedUpTables(BackupSystemTable tbl) throws IOException {
    return tbl.getTablesIncludedInBackups();
  }

  @Override
  public boolean isFileDeletable(FileStatus fStat) {
    throw new IllegalStateException("This method should not be called");
  }

  @Override
  public void setConf(Configuration config) {
    this.connection = null;
    try {
      this.connection = ConnectionFactory.createConnection(config);
    } catch (IOException ioe) {
      LOG.error("Couldn't establish connection", ioe);
    }
  }

  @Override
  public void stop(String why) {
    if (this.stopped) {
      return;
    }
    if (this.connection != null) {
      try {
        this.connection.close();
      } catch (IOException ioe) {
        LOG.debug("Got IOException when closing connection", ioe);
      }
    }
    this.stopped = true;
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }

  @Override
  public void abort(String why, Throwable e) {
    LOG.warn("Aborting ReplicationHFileCleaner because {}", why, e);
    this.aborted = true;
    stop(why);
  }

  @Override
  public boolean isAborted() {
    return this.aborted;
  }
}
