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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.backup.master.LogRollMasterProcedureManager;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * After a full backup was created, the incremental backup will only store the changes made after
 * the last full or incremental backup. Creating the backup copies the logfiles in .logs and
 * .oldlogs since the last backup timestamp.
 */
@InterfaceAudience.Private
public class IncrementalBackupManager extends BackupManager {
  public static final Logger LOG = LoggerFactory.getLogger(IncrementalBackupManager.class);

  public IncrementalBackupManager(Connection conn, Configuration conf) throws IOException {
    super(conn, conf);
  }

  /**
   * Obtain the list of logs that need to be copied out for this incremental backup. The list is set
   * in BackupInfo.
   * @return The new map of RS log time stamps for this incremental backup, as computed by
   *         {@link BackupUtils#computeLogBoundaries}: the included logs are covered, the logs held
   *         back for a later backup (for example, logs still being split) are pending, and a host
   *         that still has logs keeps its previous boundary if this backup gives it no new one.
   * @throws IOException exception
   */
  public Map<String, Long> getIncrBackupLogFileMap() throws IOException {
    List<String> logList;
    Map<String, Long> previousTimestampMins =
      BackupUtils.getRSLogTimestampMins(readLogTimestampMap());

    // get all new log files from .logs and .oldlogs after last TS and before new timestamp
    if (previousTimestampMins.isEmpty()) {
      throw new IOException("Cannot read any previous back up timestamps from backup system table. "
        + "In order to create an incremental backup, at least one full backup is needed.");
    }

    Map<String, Long> previousLogRollByHost = readRegionServerLastLogRollResult();
    LOG.info("Execute roll log procedure for incremental backup ...");
    HashMap<String, String> props = new HashMap<>();
    props.put("backupRoot", backupInfo.getBackupRootDir());

    try (Admin admin = conn.getAdmin()) {
      admin.execProcedure(LogRollMasterProcedureManager.ROLLLOG_PROCEDURE_SIGNATURE,
        LogRollMasterProcedureManager.ROLLLOG_PROCEDURE_NAME, props);
    }

    Map<String, Long> rolledHosts =
      BackupUtils.getRolledHosts(previousLogRollByHost, readRegionServerLastLogRollResult());

    LogFileSelection selection = getLogFilesForNewBackup(previousTimestampMins, rolledHosts, conf);
    logList = excludeProcV2WALs(selection.included());
    backupInfo.setIncrBackupFileList(logList);

    Map<String, Long> newTimestamps = BackupUtils.computeLogBoundaries(rolledHosts,
      previousTimestampMins, selection.hostsWithLogs(), logList, selection.heldBack());
    LOG.debug("Log boundaries for incremental backup {}: {}", backupInfo.getBackupId(),
      newTimestamps);
    return newTimestamps;
  }

  private static final class LogFileSelection {
    private final List<String> included;
    private final List<String> heldBack;
    private final Set<String> hostsWithLogs;

    LogFileSelection(List<String> included, List<String> heldBack, Set<String> hostsWithLogs) {
      this.included = included;
      this.heldBack = heldBack;
      this.hostsWithLogs = hostsWithLogs;
    }

    List<String> included() {
      return included;
    }

    List<String> heldBack() {
      return heldBack;
    }

    Set<String> hostsWithLogs() {
      return hostsWithLogs;
    }
  }

  private List<String> excludeProcV2WALs(List<String> logList) {
    List<String> list = new ArrayList<>();
    for (int i = 0; i < logList.size(); i++) {
      Path p = new Path(logList.get(i));
      String name = p.getName();

      if (name.startsWith(WALProcedureStore.LOG_PREFIX)) {
        continue;
      }

      list.add(logList.get(i));
    }
    return list;
  }

  /**
   * Gather all log files that either: 1) are newer than the older timestamps, but not newer than
   * the newest timestamps, or 2) are archived logs whose host name does not occur in the newest
   * timestamps.
   * @param olderTimestamps  the timestamp for each region server of the last backup.
   * @param newestTimestamps the timestamp for each region server that the backup should lead to.
   * @param conf             the Hadoop and Hbase configuration
   * @return the log files to be backed up, the log files held back for a later backup, and the
   *         hosts that have any log files, including ones not backed up
   * @throws IOException exception
   */
  private LogFileSelection getLogFilesForNewBackup(Map<String, Long> olderTimestamps,
    Map<String, Long> newestTimestamps, Configuration conf) throws IOException {
    LOG.debug("In getLogFilesForNewBackup()\n" + "olderTimestamps: " + olderTimestamps
      + "\n newestTimestamps: " + newestTimestamps);

    long prevBackupStartTs = Collections.min(olderTimestamps.values());
    Path walRootDir = CommonFSUtils.getWALRootDir(conf);
    Path logDir = new Path(walRootDir, HConstants.HREGION_LOGDIR_NAME);
    Path oldLogDir = new Path(walRootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    FileSystem fs = walRootDir.getFileSystem(conf);
    NewestLogFilter pathFilter = new NewestLogFilter();

    List<String> resultLogFiles = new ArrayList<>();
    List<String> newestLogs = new ArrayList<>();
    Set<String> hostsWithLogs = new HashSet<>();

    /*
     * The old region servers and timestamps info we kept in backup system table may be out of sync
     * if new region server is added or existing one lost. We'll deal with it here when processing
     * the logs. If data in backup system table has more hosts, just ignore it. If the .logs
     * directory includes more hosts, the additional hosts will not have old timestamps to compare
     * with. We'll just use all the logs in that directory. We always write up-to-date region server
     * and timestamp info to backup system table at the end of successful backup.
     */
    FileStatus[] rss;
    Path p;
    String host;
    Long oldTimeStamp;
    String currentLogFile;
    long currentLogTS;

    // Get the files in .logs.
    rss = fs.listStatus(logDir);
    for (FileStatus rs : rss) {
      p = rs.getPath();
      host = BackupUtils.parseHostNameFromLogFile(p);
      if (host == null) {
        continue;
      }
      hostsWithLogs.add(host);
      FileStatus[] logs;
      oldTimeStamp = olderTimestamps.get(host);
      // It is possible that there is no old timestamp in backup system table for this host if
      // this region server is newly added after our last backup.
      if (oldTimeStamp == null) {
        logs = fs.listStatus(p);
      } else {
        pathFilter.setLastBackupTS(oldTimeStamp);
        logs = fs.listStatus(p, pathFilter);
      }
      for (FileStatus log : logs) {
        LOG.debug("currentLogFile: " + log.getPath().toString());
        if (AbstractFSWALProvider.isMetaFile(log.getPath())) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Skip hbase:meta log file: " + log.getPath().getName());
          }
          continue;
        }
        currentLogFile = log.getPath().toString();
        resultLogFiles.add(currentLogFile);
        currentLogTS = BackupUtils.getCreationTime(log.getPath());

        // A host without a roll result did not take part in this backup's log roll: it is offline
        // (for example a dead server whose logs are still being split), or it started after the
        // roll. Its logs are held back, like logs newer than the roll result of a host that took
        // part. BackupUtils#computeLogBoundaries keeps the boundary of the host below held-back
        // logs, so a later backup includes them.
        Long ts = newestTimestamps.get(host);
        if (ts == null) {
          LOG.debug("Holding back log {} of host {}, which did not take part in the log roll",
            log.getPath(), host);
        }
        if (ts == null || currentLogTS > ts) {
          newestLogs.add(currentLogFile);
        }
      }
    }

    // Include the .oldlogs files too.
    FileStatus[] oldlogs = fs.listStatus(oldLogDir);
    for (FileStatus oldlog : oldlogs) {
      p = oldlog.getPath();
      currentLogFile = p.toString();
      if (AbstractFSWALProvider.isMetaFile(p)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skip .meta log file: " + currentLogFile);
        }
        continue;
      }
      host = BackupUtils.parseHostFromOldLog(p);
      if (host == null) {
        continue;
      }
      hostsWithLogs.add(host);
      currentLogTS = BackupUtils.getCreationTime(p);
      oldTimeStamp = olderTimestamps.get(host);
      /*
       * It is possible that there is no old timestamp in backup system table for this host. At the
       * time of our last backup operation, this rs did not exist. The reason can be one of the two:
       * 1. The rs already left/crashed. Its logs were moved to .oldlogs. 2. The rs was added after
       * our last backup.
       */
      if (oldTimeStamp == null) {
        if (currentLogTS < prevBackupStartTs) {
          // This log file is really old, its region server was before our last backup.
          continue;
        } else {
          resultLogFiles.add(currentLogFile);
        }
      } else if (currentLogTS > oldTimeStamp) {
        resultLogFiles.add(currentLogFile);
      }

      Long newTimestamp = newestTimestamps.get(host);
      if (newTimestamp != null && currentLogTS > newTimestamp) {
        newestLogs.add(currentLogFile);
      }
    }
    // Held-back logs are newer than the roll result of their host, or belong to a host that did not
    // take part in the log roll. A later backup includes them.
    resultLogFiles.removeAll(newestLogs);
    return new LogFileSelection(resultLogFiles, newestLogs, hostsWithLogs);
  }

  static class NewestLogFilter implements PathFilter {
    private Long lastBackupTS = 0L;

    public NewestLogFilter() {
    }

    protected void setLastBackupTS(Long ts) {
      this.lastBackupTS = ts;
    }

    @Override
    public boolean accept(Path path) {
      // skip meta table log -- ts.meta file
      if (AbstractFSWALProvider.isMetaFile(path)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skip .meta log file: " + path.getName());
        }
        return false;
      }
      long timestamp;
      try {
        timestamp = BackupUtils.getCreationTime(path);
        return timestamp > lastBackupTS;
      } catch (Exception e) {
        LOG.warn("Cannot read timestamp of log file " + path);
        return false;
      }
    }
  }
}
