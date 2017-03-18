/**
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
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupCopyJob;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupPhase;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupState;
import org.apache.hadoop.hbase.backup.BackupRequest;
import org.apache.hadoop.hbase.backup.BackupRestoreFactory;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Connection;

/**
 * Incremental backup implementation.
 * See the {@link #execute() execute} method.
 *
 */
@InterfaceAudience.Private
public class IncrementalTableBackupClient extends TableBackupClient {
  private static final Log LOG = LogFactory.getLog(IncrementalTableBackupClient.class);

  public IncrementalTableBackupClient(final Connection conn, final String backupId,
      BackupRequest request) throws IOException {
    super(conn, backupId, request);
  }

  private List<String> filterMissingFiles(List<String> incrBackupFileList) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    List<String> list = new ArrayList<String>();
    for (String file : incrBackupFileList) {
      if (fs.exists(new Path(file))) {
        list.add(file);
      } else {
        LOG.warn("Can't find file: " + file);
      }
    }
    return list;
  }

  private List<String> getMissingFiles(List<String> incrBackupFileList) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    List<String> list = new ArrayList<String>();
    for (String file : incrBackupFileList) {
      if (!fs.exists(new Path(file))) {
        list.add(file);
      }
    }
    return list;

  }

  /**
   * Do incremental copy.
   * @param backupInfo backup info
   */
  private void incrementalCopy(BackupInfo backupInfo) throws Exception {

    LOG.info("Incremental copy is starting.");
    // set overall backup phase: incremental_copy
    backupInfo.setPhase(BackupPhase.INCREMENTAL_COPY);
    // get incremental backup file list and prepare parms for DistCp
    List<String> incrBackupFileList = backupInfo.getIncrBackupFileList();
    // filter missing files out (they have been copied by previous backups)
    incrBackupFileList = filterMissingFiles(incrBackupFileList);
    String[] strArr = incrBackupFileList.toArray(new String[incrBackupFileList.size() + 1]);
    strArr[strArr.length - 1] = backupInfo.getHLogTargetDir();

    BackupCopyJob copyService = BackupRestoreFactory.getBackupCopyJob(conf);
    int counter = 0;
    int MAX_ITERAIONS = 2;
    while (counter++ < MAX_ITERAIONS) {
      // We run DistCp maximum 2 times
      // If it fails on a second time, we throw Exception
      int res =
          copyService.copy(backupInfo, backupManager, conf, BackupType.INCREMENTAL, strArr);

      if (res != 0) {
        LOG.error("Copy incremental log files failed with return code: " + res + ".");
        throw new IOException("Failed of Hadoop Distributed Copy from "
            + StringUtils.join(incrBackupFileList, ",") + " to "
            + backupInfo.getHLogTargetDir());
      }
      List<String> missingFiles = getMissingFiles(incrBackupFileList);

      if (missingFiles.isEmpty()) {
        break;
      } else {
        // Repeat DistCp, some files have been moved from WALs to oldWALs during previous run
        // update backupInfo and strAttr
        if (counter == MAX_ITERAIONS) {
          String msg =
              "DistCp could not finish the following files: " + StringUtils.join(missingFiles, ",");
          LOG.error(msg);
          throw new IOException(msg);
        }
        List<String> converted = convertFilesFromWALtoOldWAL(missingFiles);
        incrBackupFileList.removeAll(missingFiles);
        incrBackupFileList.addAll(converted);
        backupInfo.setIncrBackupFileList(incrBackupFileList);

        // Run DistCp only for missing files (which have been moved from WALs to oldWALs
        // during previous run)
        strArr = converted.toArray(new String[converted.size() + 1]);
        strArr[strArr.length - 1] = backupInfo.getHLogTargetDir();
      }
    }

    LOG.info("Incremental copy from " + StringUtils.join(incrBackupFileList, ",") + " to "
        + backupInfo.getHLogTargetDir() + " finished.");
  }

  private List<String> convertFilesFromWALtoOldWAL(List<String> missingFiles) throws IOException {
    List<String> list = new ArrayList<String>();
    for (String path : missingFiles) {
      if (path.indexOf(Path.SEPARATOR + HConstants.HREGION_LOGDIR_NAME) < 0) {
        LOG.error("Copy incremental log files failed, file is missing : " + path);
        throw new IOException("Failed of Hadoop Distributed Copy to "
            + backupInfo.getHLogTargetDir() + ", file is missing " + path);
      }
      list.add(path.replace(Path.SEPARATOR + HConstants.HREGION_LOGDIR_NAME, Path.SEPARATOR
          + HConstants.HREGION_OLDLOGDIR_NAME));
    }
    return list;
  }

  @Override
  public void execute() throws IOException {

    // case PREPARE_INCREMENTAL:
    beginBackup(backupManager, backupInfo);
    backupInfo.setPhase(BackupPhase.PREPARE_INCREMENTAL);
    LOG.debug("For incremental backup, current table set is "
        + backupManager.getIncrementalBackupTableSet());
    try {
      newTimestamps =
          ((IncrementalBackupManager) backupManager).getIncrBackupLogFileList(conn, backupInfo);
    } catch (Exception e) {
      // fail the overall backup and return
      failBackup(conn, backupInfo, backupManager, e, "Unexpected Exception : ",
        BackupType.INCREMENTAL, conf);
    }

    // case INCREMENTAL_COPY:
    try {
      // copy out the table and region info files for each table
      BackupUtils.copyTableRegionInfo(conn, backupInfo, conf);
      incrementalCopy(backupInfo);
      // Save list of WAL files copied
      backupManager.recordWALFiles(backupInfo.getIncrBackupFileList());
    } catch (Exception e) {
      String msg = "Unexpected exception in incremental-backup: incremental copy " + backupId;
      // fail the overall backup and return
      failBackup(conn, backupInfo, backupManager, e, msg, BackupType.INCREMENTAL, conf);
    }
    // case INCR_BACKUP_COMPLETE:
    // set overall backup status: complete. Here we make sure to complete the backup.
    // After this checkpoint, even if entering cancel process, will let the backup finished
    try {
      backupInfo.setState(BackupState.COMPLETE);
      // Set the previousTimestampMap which is before this current log roll to the manifest.
      HashMap<TableName, HashMap<String, Long>> previousTimestampMap =
          backupManager.readLogTimestampMap();
      backupInfo.setIncrTimestampMap(previousTimestampMap);

      // The table list in backupInfo is good for both full backup and incremental backup.
      // For incremental backup, it contains the incremental backup table set.
      backupManager.writeRegionServerLogTimestamp(backupInfo.getTables(), newTimestamps);

      HashMap<TableName, HashMap<String, Long>> newTableSetTimestampMap =
          backupManager.readLogTimestampMap();

      Long newStartCode =
          BackupUtils.getMinValue(BackupUtils
              .getRSLogTimestampMins(newTableSetTimestampMap));
      backupManager.writeBackupStartCode(newStartCode);
      // backup complete
      completeBackup(conn, backupInfo, backupManager, BackupType.INCREMENTAL, conf);

    } catch (IOException e) {
      failBackup(conn, backupInfo, backupManager, e, "Unexpected Exception : ",
        BackupType.INCREMENTAL, conf);
    }
  }

}
