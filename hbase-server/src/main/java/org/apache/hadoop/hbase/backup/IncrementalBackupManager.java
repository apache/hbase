/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.backup.master.LogRollMasterProcedureManager;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.DefaultWALProvider;

/**
 * After a full backup was created, the incremental backup will only store the changes made
 * after the last full or incremental backup.
 *
 * Creating the backup copies the logfiles in .logs and .oldlogs since the last backup timestamp.
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class IncrementalBackupManager {
  // parent manager
  private BackupManager backupManager;

  public static final Log LOG = LogFactory.getLog(IncrementalBackupManager.class);

  public IncrementalBackupManager(BackupManager bm) {
    this.backupManager = bm;
  }

  /**
   * Obtain the list of logs that need to be copied out for this incremental backup. The list is set
   * in BackupContext.
   * @param backupContext backup context
   * @return The new HashMap of RS log timestamps after the log roll for this incremental backup.
   * @throws IOException exception
   */
  public HashMap<String, String> getIncrBackupLogFileList(BackupContext backupContext)
      throws IOException {
    List<String> logList;
    HashMap<String, String> newTimestamps;
    HashMap<String, String> previousTimestampMins;

    Configuration conf = BackupUtil.getConf();
    String savedStartCode = backupManager.readBackupStartCode();

    // key: tableName
    // value: <RegionServer,PreviousTimeStamp>
    HashMap<String, HashMap<String, String>> previousTimestampMap =
        backupManager.readLogTimestampMap();

    previousTimestampMins = BackupUtil.getRSLogTimestampMins(previousTimestampMap);

    LOG.debug("StartCode " + savedStartCode + "for backupID " + backupContext.getBackupId());
    LOG.debug("Timestamps " + previousTimestampMap);
    // get all new log files from .logs and .oldlogs after last TS and before new timestamp
    if (savedStartCode == null || 
        previousTimestampMins == null || 
          previousTimestampMins.isEmpty()) {
      throw new IOException("Cannot read any previous back up timestamps from hbase:backup. "
          + "In order to create an incremental backup, at least one full backup is needed.");
    }

    HBaseAdmin hbadmin = null;
    Connection conn = null;
    try {
      LOG.info("Execute roll log procedure for incremental backup ...");
      conn = ConnectionFactory.createConnection(conf);
      hbadmin = (HBaseAdmin) conn.getAdmin();
      hbadmin.execProcedure(LogRollMasterProcedureManager.ROLLLOG_PROCEDURE_SIGNATURE,
        LogRollMasterProcedureManager.ROLLLOG_PROCEDURE_NAME, new HashMap<String, String>());
    } finally {
      if (hbadmin != null) {
        hbadmin.close();
      }
      if(conn != null){
        conn.close();
      }
    }

    newTimestamps = backupManager.readRegionServerLastLogRollResult();

    logList = getLogFilesForNewBackup(previousTimestampMins, newTimestamps, conf, savedStartCode);

    backupContext.setIncrBackupFileList(logList);

    return newTimestamps;
  }

  /**
   * For each region server: get all log files newer than the last timestamps but not newer than the
   * newest timestamps.
   * @param olderTimestamps the timestamp for each region server of the last backup.
   * @param newestTimestamps the timestamp for each region server that the backup should lead to.
   * @param conf the Hadoop and Hbase configuration
   * @param savedStartCode the startcode (timestamp) of last successful backup.
   * @return a list of log files to be backed up
   * @throws IOException exception
   */
  private List<String> getLogFilesForNewBackup(HashMap<String, String> olderTimestamps,
    HashMap<String, String> newestTimestamps, Configuration conf, String savedStartCode)
        throws IOException {
    LOG.debug("In getLogFilesForNewBackup()\n" + "olderTimestamps: " + olderTimestamps
      + "\n newestTimestamps: " + newestTimestamps);
    Path rootdir = FSUtils.getRootDir(conf);
    Path logDir = new Path(rootdir, HConstants.HREGION_LOGDIR_NAME);
    Path oldLogDir = new Path(rootdir, HConstants.HREGION_OLDLOGDIR_NAME);
    FileSystem fs = rootdir.getFileSystem(conf);
    NewestLogFilter pathFilter = new NewestLogFilter(conf);

    List<String> resultLogFiles = new ArrayList<String>();
    List<String> newestLogs = new ArrayList<String>();

    /*
     * The old region servers and timestamps info we kept in hbase:backup may be out of sync if new
     * region server is added or existing one lost. We'll deal with it here when processing the
     * logs. If data in hbase:backup has more hosts, just ignore it. If the .logs directory includes
     * more hosts, the additional hosts will not have old timestamps to compare with. We'll just use
     * all the logs in that directory. We always write up-to-date region server and timestamp info
     * to hbase:backup at the end of successful backup.
     */

    FileStatus[] rss;
    Path p;
    String host;
    String oldTimeStamp;
    String currentLogFile;
    String currentLogTS;

    // Get the files in .logs.
    rss = fs.listStatus(logDir);
    for (FileStatus rs : rss) {
      p = rs.getPath();
      host = DefaultWALProvider.getServerNameFromWALDirectoryName(p).getHostname();
      FileStatus[] logs;
      oldTimeStamp = olderTimestamps.get(host);
      // It is possible that there is no old timestamp in hbase:backup for this host if
      // this region server is newly added after our last backup.
      if (oldTimeStamp == null) {
        logs = fs.listStatus(p);
      } else {
        pathFilter.setLastBackupTS(oldTimeStamp);
        logs = fs.listStatus(p, pathFilter);
      }
      for (FileStatus log : logs) {
        LOG.debug("currentLogFile: " + log.getPath().toString());
        if (DefaultWALProvider.isMetaFile(log.getPath())) {
          LOG.debug("Skip hbase:meta log file: " + log.getPath().getName());
          continue;
        }
        currentLogFile = log.getPath().toString();
        resultLogFiles.add(currentLogFile);
        currentLogTS = BackupUtil.getCreationTime(log.getPath(), conf);
        // newestTimestamps is up-to-date with the current list of hosts
        // so newestTimestamps.get(host) will not be null.
        if (Long.valueOf(currentLogTS) > Long.valueOf(newestTimestamps.get(host))) {
          newestLogs.add(currentLogFile);
        }
      }
    }

    // Include the .oldlogs files too.
    FileStatus[] oldlogs = fs.listStatus(oldLogDir);
    for (FileStatus oldlog : oldlogs) {
      p = oldlog.getPath();
      currentLogFile = p.toString();
      if (DefaultWALProvider.isMetaFile(p)) {
        LOG.debug("Skip .meta log file: " + currentLogFile);
        continue;
      }
      host = BackupUtil.parseHostFromOldLog(p);
      currentLogTS = BackupUtil.getCreationTime(p, conf);
      oldTimeStamp = olderTimestamps.get(host);
      /*
       * It is possible that there is no old timestamp in hbase:backup for this host. At the time of
       * our last backup operation, this rs did not exist. The reason can be one of the two: 1. The
       * rs already left/crashed. Its logs were moved to .oldlogs. 2. The rs was added after our
       * last backup.
       */
      if (oldTimeStamp == null) {
        if (Long.valueOf(currentLogTS) < Long.valueOf(savedStartCode)) {
          // This log file is really old, its region server was before our last backup.
          continue;
        } else {
          resultLogFiles.add(currentLogFile);
        }
      } else if (Long.valueOf(currentLogTS) > Long.valueOf(oldTimeStamp)) {
        resultLogFiles.add(currentLogFile);
      }

      LOG.debug("resultLogFiles before removal of newestLogs: " + resultLogFiles);
      // It is possible that a host in .oldlogs is an obsolete region server
      // so newestTimestamps.get(host) here can be null.
      // Even if these logs belong to a obsolete region server, we still need
      // to include they to avoid loss of edits for backup.
      String newTimestamp = newestTimestamps.get(host);
      if (newTimestamp != null && Long.valueOf(currentLogTS) > Long.valueOf(newTimestamp)) {
        newestLogs.add(currentLogFile);
      }
    }
    LOG.debug("newestLogs: " + newestLogs);
    // remove newest log per host because they are still in use
    resultLogFiles.removeAll(newestLogs);
    LOG.debug("resultLogFiles after removal of newestLogs: " + resultLogFiles);
    return resultLogFiles;
  }

  class NewestLogFilter implements PathFilter {
    private String lastBackupTS = "0";
    final private Configuration conf;

    public NewestLogFilter(Configuration conf) {
      this.conf = conf;
    }

    protected void setLastBackupTS(String ts) {
      this.lastBackupTS = ts;
    }

    @Override
    public boolean accept(Path path) {
      // skip meta table log -- ts.meta file
      if (DefaultWALProvider.isMetaFile(path)) {
        LOG.debug("Skip .meta log file: " + path.getName());
        return false;
      }
      String timestamp;
      try {
        timestamp = BackupUtil.getCreationTime(path, conf);
        return Long.valueOf(timestamp) > Long.valueOf(lastBackupTS);
      } catch (IOException e) {
        LOG.warn("Cannot read timestamp of log file " + path);
        return false;
      }
    }
  }

}
