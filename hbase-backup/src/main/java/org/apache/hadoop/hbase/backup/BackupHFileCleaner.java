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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.master.cleaner.BaseHFileCleanerDelegate;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;

/**
 * Implementation of a file cleaner that checks if an hfile is still referenced by backup before
 * deleting it from hfile archive directory.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class BackupHFileCleaner extends BaseHFileCleanerDelegate implements Abortable {
  private static final Logger LOG = LoggerFactory.getLogger(BackupHFileCleaner.class);
  private boolean stopped = false;
  private boolean aborted;
  private Configuration conf;
  private Connection connection;
  private long prevReadFromBackupTbl = 0, // timestamp of most recent read from backup:system table
      secondPrevReadFromBackupTbl = 0; // timestamp of 2nd most recent read from backup:system table
  // used by unit test to skip reading backup:system
  private boolean checkForFullyBackedUpTables = true;
  private List<TableName> fullyBackedUpTables = null;

  private Set<String> getFilenameFromBulkLoad(Map<byte[], List<Path>>[] maps) {
    Set<String> filenames = new HashSet<>();
    for (Map<byte[], List<Path>> map : maps) {
      if (map == null) {
        continue;
      }

      for (List<Path> paths : map.values()) {
        for (Path p : paths) {
          filenames.add(p.getName());
        }
      }
    }
    return filenames;
  }

  private Set<String> loadHFileRefs(List<TableName> tableList) throws IOException {
    if (connection == null) {
      connection = ConnectionFactory.createConnection(conf);
    }
    try (BackupSystemTable tbl = new BackupSystemTable(connection)) {
      Map<byte[], List<Path>>[] res = tbl.readBulkLoadedFiles(null, tableList);
      secondPrevReadFromBackupTbl = prevReadFromBackupTbl;
      prevReadFromBackupTbl = EnvironmentEdgeManager.currentTime();
      return getFilenameFromBulkLoad(res);
    }
  }

  @InterfaceAudience.Private
  void setCheckForFullyBackedUpTables(boolean b) {
    checkForFullyBackedUpTables = b;
  }

  @Override
  public Iterable<FileStatus> getDeletableFiles(Iterable<FileStatus> files) {
    if (conf == null) {
      return files;
    }
    // obtain the Set of TableName's which have been fully backed up
    // so that we filter BulkLoad to be returned from server
    if (checkForFullyBackedUpTables) {
      if (connection == null) {
        return files;
      }

      try (BackupSystemTable tbl = new BackupSystemTable(connection)) {
        fullyBackedUpTables = new ArrayList<>(tbl.getTablesIncludedInBackups());
      } catch (IOException ioe) {
        LOG.error("Failed to get tables which have been fully backed up, skipping checking", ioe);
        return Collections.emptyList();
      }
      Collections.sort(fullyBackedUpTables);
    }
    final Set<String> hfileRefs;
    try {
      hfileRefs = loadHFileRefs(fullyBackedUpTables);
    } catch (IOException ioe) {
      LOG.error("Failed to read hfile references, skipping checking deletable files", ioe);
      return Collections.emptyList();
    }
    Iterable<FileStatus> deletables = Iterables.filter(files, file -> {
      // If the file is recent, be conservative and wait for one more scan of backup:system table
      if (file.getModificationTime() > secondPrevReadFromBackupTbl) {
        return false;
      }
      String hfile = file.getPath().getName();
      boolean foundHFileRef = hfileRefs.contains(hfile);
      return !foundHFileRef;
    });
    return deletables;
  }

  @Override
  public boolean isFileDeletable(FileStatus fStat) {
    // work is done in getDeletableFiles()
    return true;
  }

  @Override
  public void setConf(Configuration config) {
    this.conf = config;
    this.connection = null;
    try {
      this.connection = ConnectionFactory.createConnection(conf);
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
        LOG.debug("Got " + ioe + " when closing connection");
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
    LOG.warn("Aborting ReplicationHFileCleaner because " + why, e);
    this.aborted = true;
    stop(why);
  }

  @Override
  public boolean isAborted() {
    return this.aborted;
  }
}
