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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupAdmin;
import org.apache.hadoop.hbase.backup.BackupClientFactory;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupState;
import org.apache.hadoop.hbase.backup.BackupMergeJob;
import org.apache.hadoop.hbase.backup.BackupRequest;
import org.apache.hadoop.hbase.backup.BackupRestoreConstants;
import org.apache.hadoop.hbase.backup.BackupRestoreFactory;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.backup.HBackupFileSystem;
import org.apache.hadoop.hbase.backup.RestoreRequest;
import org.apache.hadoop.hbase.backup.util.BackupSet;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@InterfaceAudience.Private
public class BackupAdminImpl implements BackupAdmin {
  public final static String CHECK_OK = "Checking backup images: OK";
  public final static String CHECK_FAILED =
      "Checking backup images: Failed. Some dependencies are missing for restore";
  private static final Logger LOG = LoggerFactory.getLogger(BackupAdminImpl.class);

  private final Connection conn;

  public BackupAdminImpl(Connection conn) {
    this.conn = conn;
  }

  @Override
  public void close() {
  }

  @Override
  public BackupInfo getBackupInfo(String backupId) throws IOException {
    BackupInfo backupInfo;
    try (final BackupSystemTable table = new BackupSystemTable(conn)) {
      if (backupId == null) {
        ArrayList<BackupInfo> recentSessions = table.getBackupInfos(BackupState.RUNNING);
        if (recentSessions.isEmpty()) {
          LOG.warn("No ongoing sessions found.");
          return null;
        }
        // else show status for ongoing session
        // must be one maximum
        return recentSessions.get(0);
      } else {
        backupInfo = table.readBackupInfo(backupId);
        return backupInfo;
      }
    }
  }

  @Override
  public int deleteBackups(String[] backupIds) throws IOException {

    int totalDeleted = 0;
    Map<String, HashSet<TableName>> allTablesMap = new HashMap<>();

    boolean deleteSessionStarted;
    boolean snapshotDone;
    try (final BackupSystemTable sysTable = new BackupSystemTable(conn)) {
      // Step 1: Make sure there is no active session
      // is running by using startBackupSession API
      // If there is an active session in progress, exception will be thrown
      try {
        sysTable.startBackupExclusiveOperation();
        deleteSessionStarted = true;
      } catch (IOException e) {
        LOG.warn("You can not run delete command while active backup session is in progress. \n"
            + "If there is no active backup session running, run backup repair utility to "
            + "restore \nbackup system integrity.");
        return -1;
      }

      // Step 2: Make sure there is no failed session
      List<BackupInfo> list = sysTable.getBackupInfos(BackupState.RUNNING);
      if (list.size() != 0) {
        // ailed sessions found
        LOG.warn("Failed backup session found. Run backup repair tool first.");
        return -1;
      }

      // Step 3: Record delete session
      sysTable.startDeleteOperation(backupIds);
      // Step 4: Snapshot backup system table
      if (!BackupSystemTable.snapshotExists(conn)) {
        BackupSystemTable.snapshot(conn);
      } else {
        LOG.warn("Backup system table snapshot exists");
      }
      snapshotDone = true;
      try {
        for (int i = 0; i < backupIds.length; i++) {
          BackupInfo info = sysTable.readBackupInfo(backupIds[i]);
          if (info != null) {
            String rootDir = info.getBackupRootDir();
            HashSet<TableName> allTables = allTablesMap.get(rootDir);
            if (allTables == null) {
              allTables = new HashSet<>();
              allTablesMap.put(rootDir, allTables);
            }
            allTables.addAll(info.getTableNames());
            totalDeleted += deleteBackup(backupIds[i], sysTable);
          }
        }
        finalizeDelete(allTablesMap, sysTable);
        // Finish
        sysTable.finishDeleteOperation();
        // delete snapshot
        BackupSystemTable.deleteSnapshot(conn);
      } catch (IOException e) {
        // Fail delete operation
        // Step 1
        if (snapshotDone) {
          if (BackupSystemTable.snapshotExists(conn)) {
            BackupSystemTable.restoreFromSnapshot(conn);
            // delete snapshot
            BackupSystemTable.deleteSnapshot(conn);
            // We still have record with unfinished delete operation
            LOG.error("Delete operation failed, please run backup repair utility to restore "
                + "backup system integrity", e);
            throw e;
          } else {
            LOG.warn("Delete operation succeeded, there were some errors: ", e);
          }
        }

      } finally {
        if (deleteSessionStarted) {
          sysTable.finishBackupExclusiveOperation();
        }
      }
    }
    return totalDeleted;
  }

  /**
   * Updates incremental backup set for every backupRoot
   * @param tablesMap map [backupRoot: {@code Set<TableName>}]
   * @param table backup system table
   * @throws IOException if a table operation fails
   */
  private void finalizeDelete(Map<String, HashSet<TableName>> tablesMap, BackupSystemTable table)
      throws IOException {
    for (String backupRoot : tablesMap.keySet()) {
      Set<TableName> incrTableSet = table.getIncrementalBackupTableSet(backupRoot);
      Map<TableName, ArrayList<BackupInfo>> tableMap =
          table.getBackupHistoryForTableSet(incrTableSet, backupRoot);
      for (Map.Entry<TableName, ArrayList<BackupInfo>> entry : tableMap.entrySet()) {
        if (entry.getValue() == null) {
          // No more backups for a table
          incrTableSet.remove(entry.getKey());
        }
      }
      if (!incrTableSet.isEmpty()) {
        table.addIncrementalBackupTableSet(incrTableSet, backupRoot);
      } else { // empty
        table.deleteIncrementalBackupTableSet(backupRoot);
      }
    }
  }

  /**
   * Delete single backup and all related backups <br>
   * Algorithm:<br>
   * Backup type: FULL or INCREMENTAL <br>
   * Is this last backup session for table T: YES or NO <br>
   * For every table T from table list 'tables':<br>
   * if(FULL, YES) deletes only physical data (PD) <br>
   * if(FULL, NO), deletes PD, scans all newer backups and removes T from backupInfo,<br>
   * until we either reach the most recent backup for T in the system or FULL backup<br>
   * which includes T<br>
   * if(INCREMENTAL, YES) deletes only physical data (PD) if(INCREMENTAL, NO) deletes physical data
   * and for table T scans all backup images between last<br>
   * FULL backup, which is older than the backup being deleted and the next FULL backup (if exists)
   * <br>
   * or last one for a particular table T and removes T from list of backup tables.
   * @param backupId backup id
   * @param sysTable backup system table
   * @return total number of deleted backup images
   * @throws IOException if deleting the backup fails
   */
  private int deleteBackup(String backupId, BackupSystemTable sysTable) throws IOException {
    BackupInfo backupInfo = sysTable.readBackupInfo(backupId);

    int totalDeleted = 0;
    if (backupInfo != null) {
      LOG.info("Deleting backup " + backupInfo.getBackupId() + " ...");
      // Step 1: clean up data for backup session (idempotent)
      BackupUtils.cleanupBackupData(backupInfo, conn.getConfiguration());
      // List of tables in this backup;
      List<TableName> tables = backupInfo.getTableNames();
      long startTime = backupInfo.getStartTs();
      for (TableName tn : tables) {
        boolean isLastBackupSession = isLastBackupSession(sysTable, tn, startTime);
        if (isLastBackupSession) {
          continue;
        }
        // else
        List<BackupInfo> affectedBackups = getAffectedBackupSessions(backupInfo, tn, sysTable);
        for (BackupInfo info : affectedBackups) {
          if (info.equals(backupInfo)) {
            continue;
          }
          removeTableFromBackupImage(info, tn, sysTable);
        }
      }
      Map<byte[], String> map = sysTable.readBulkLoadedFiles(backupId);
      FileSystem fs = FileSystem.get(conn.getConfiguration());
      boolean success = true;
      int numDeleted = 0;
      for (String f : map.values()) {
        Path p = new Path(f);
        try {
          LOG.debug("Delete backup info " + p + " for " + backupInfo.getBackupId());
          if (!fs.delete(p)) {
            if (fs.exists(p)) {
              LOG.warn(f + " was not deleted");
              success = false;
            }
          } else {
            numDeleted++;
          }
        } catch (IOException ioe) {
          LOG.warn(f + " was not deleted", ioe);
          success = false;
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug(numDeleted + " bulk loaded files out of " + map.size() + " were deleted");
      }
      if (success) {
        sysTable.deleteBulkLoadedRows(new ArrayList<>(map.keySet()));
      }

      sysTable.deleteBackupInfo(backupInfo.getBackupId());
      LOG.info("Delete backup " + backupInfo.getBackupId() + " completed.");
      totalDeleted++;
    } else {
      LOG.warn("Delete backup failed: no information found for backupID=" + backupId);
    }
    return totalDeleted;
  }

  private void removeTableFromBackupImage(BackupInfo info, TableName tn, BackupSystemTable sysTable)
          throws IOException {
    List<TableName> tables = info.getTableNames();
    LOG.debug("Remove " + tn + " from " + info.getBackupId() + " tables="
        + info.getTableListAsString());
    if (tables.contains(tn)) {
      tables.remove(tn);

      if (tables.isEmpty()) {
        LOG.debug("Delete backup info " + info.getBackupId());

        sysTable.deleteBackupInfo(info.getBackupId());
        // Idempotent operation
        BackupUtils.cleanupBackupData(info, conn.getConfiguration());
      } else {
        info.setTables(tables);
        sysTable.updateBackupInfo(info);
        // Now, clean up directory for table (idempotent)
        cleanupBackupDir(info, tn, conn.getConfiguration());
      }
    }
  }

  private List<BackupInfo> getAffectedBackupSessions(BackupInfo backupInfo, TableName tn,
      BackupSystemTable table) throws IOException {
    LOG.debug("GetAffectedBackupInfos for: " + backupInfo.getBackupId() + " table=" + tn);
    long ts = backupInfo.getStartTs();
    List<BackupInfo> list = new ArrayList<>();
    List<BackupInfo> history = table.getBackupHistory(backupInfo.getBackupRootDir());
    // Scan from most recent to backupInfo
    // break when backupInfo reached
    for (BackupInfo info : history) {
      if (info.getStartTs() == ts) {
        break;
      }
      List<TableName> tables = info.getTableNames();
      if (tables.contains(tn)) {
        BackupType bt = info.getType();
        if (bt == BackupType.FULL) {
          // Clear list if we encounter FULL backup
          list.clear();
        } else {
          LOG.debug("GetAffectedBackupInfos for: " + backupInfo.getBackupId() + " table=" + tn
              + " added " + info.getBackupId() + " tables=" + info.getTableListAsString());
          list.add(info);
        }
      }
    }
    return list;
  }

  /**
   * Clean up the data at target directory
   * @throws IOException if cleaning up the backup directory fails
   */
  private void cleanupBackupDir(BackupInfo backupInfo, TableName table, Configuration conf)
      throws IOException {
    try {
      // clean up the data at target directory
      String targetDir = backupInfo.getBackupRootDir();
      if (targetDir == null) {
        LOG.warn("No target directory specified for " + backupInfo.getBackupId());
        return;
      }

      FileSystem outputFs = FileSystem.get(new Path(backupInfo.getBackupRootDir()).toUri(), conf);

      Path targetDirPath =
          new Path(BackupUtils.getTableBackupDir(backupInfo.getBackupRootDir(),
            backupInfo.getBackupId(), table));
      if (outputFs.delete(targetDirPath, true)) {
        LOG.info("Cleaning up backup data at " + targetDirPath.toString() + " done.");
      } else {
        LOG.info("No data has been found in " + targetDirPath.toString() + ".");
      }
    } catch (IOException e1) {
      LOG.error("Cleaning up backup data of " + backupInfo.getBackupId() + " for table " + table
          + "at " + backupInfo.getBackupRootDir() + " failed due to " + e1.getMessage() + ".");
      throw e1;
    }
  }

  private boolean isLastBackupSession(BackupSystemTable table, TableName tn, long startTime)
      throws IOException {
    List<BackupInfo> history = table.getBackupHistory();
    for (BackupInfo info : history) {
      List<TableName> tables = info.getTableNames();
      if (!tables.contains(tn)) {
        continue;
      }
      return info.getStartTs() <= startTime;
    }
    return false;
  }

  @Override
  public List<BackupInfo> getHistory(int n) throws IOException {
    try (final BackupSystemTable table = new BackupSystemTable(conn)) {
      List<BackupInfo> history = table.getBackupHistory();

      if (history.size() <= n) {
        return history;
      }

      List<BackupInfo> list = new ArrayList<>();
      for (int i = 0; i < n; i++) {
        list.add(history.get(i));
      }
      return list;
    }
  }

  @Override
  public List<BackupInfo> getHistory(int n, BackupInfo.Filter... filters) throws IOException {
    if (filters.length == 0) {
      return getHistory(n);
    }

    try (final BackupSystemTable table = new BackupSystemTable(conn)) {
      List<BackupInfo> history = table.getBackupHistory();
      List<BackupInfo> result = new ArrayList<>();
      for (BackupInfo bi : history) {
        if (result.size() == n) {
          break;
        }

        boolean passed = true;
        for (int i = 0; i < filters.length; i++) {
          if (!filters[i].apply(bi)) {
            passed = false;
            break;
          }
        }
        if (passed) {
          result.add(bi);
        }
      }
      return result;
    }
  }

  @Override
  public List<BackupSet> listBackupSets() throws IOException {
    try (final BackupSystemTable table = new BackupSystemTable(conn)) {
      List<String> list = table.listBackupSets();
      List<BackupSet> bslist = new ArrayList<>();
      for (String s : list) {
        List<TableName> tables = table.describeBackupSet(s);
        if (tables != null) {
          bslist.add(new BackupSet(s, tables));
        }
      }
      return bslist;
    }
  }

  @Override
  public BackupSet getBackupSet(String name) throws IOException {
    try (final BackupSystemTable table = new BackupSystemTable(conn)) {
      List<TableName> list = table.describeBackupSet(name);

      if (list == null) {
        return null;
      }

      return new BackupSet(name, list);
    }
  }

  @Override
  public boolean deleteBackupSet(String name) throws IOException {
    try (final BackupSystemTable table = new BackupSystemTable(conn)) {
      if (table.describeBackupSet(name) == null) {
        return false;
      }
      table.deleteBackupSet(name);
      return true;
    }
  }

  @Override
  public void addToBackupSet(String name, TableName[] tables) throws IOException {
    String[] tableNames = new String[tables.length];
    try (final BackupSystemTable table = new BackupSystemTable(conn);
        final Admin admin = conn.getAdmin()) {
      for (int i = 0; i < tables.length; i++) {
        tableNames[i] = tables[i].getNameAsString();
        if (!admin.tableExists(TableName.valueOf(tableNames[i]))) {
          throw new IOException("Cannot add " + tableNames[i] + " because it doesn't exist");
        }
      }
      table.addToBackupSet(name, tableNames);
      LOG.info("Added tables [" + StringUtils.join(tableNames, " ") + "] to '" + name
          + "' backup set");
    }
  }

  @Override
  public void removeFromBackupSet(String name, TableName[] tables) throws IOException {
    LOG.info("Removing tables [" + StringUtils.join(tables, " ") + "] from '" + name + "'");
    try (final BackupSystemTable table = new BackupSystemTable(conn)) {
      table.removeFromBackupSet(name, toStringArray(tables));
      LOG.info("Removing tables [" + StringUtils.join(tables, " ") + "] from '" + name
          + "' completed.");
    }
  }

  private String[] toStringArray(TableName[] list) {
    String[] arr = new String[list.length];
    for (int i = 0; i < list.length; i++) {
      arr[i] = list[i].toString();
    }
    return arr;
  }

  @Override
  public void restore(RestoreRequest request) throws IOException {
    if (request.isCheck()) {
      HashMap<TableName, BackupManifest> backupManifestMap = new HashMap<>();
      // check and load backup image manifest for the tables
      Path rootPath = new Path(request.getBackupRootDir());
      String backupId = request.getBackupId();
      TableName[] sTableArray = request.getFromTables();
      HBackupFileSystem.checkImageManifestExist(backupManifestMap, sTableArray,
        conn.getConfiguration(), rootPath, backupId);

      // Check and validate the backup image and its dependencies
      if (BackupUtils.validate(backupManifestMap, conn.getConfiguration())) {
        LOG.info(CHECK_OK);
      } else {
        LOG.error(CHECK_FAILED);
      }
      return;
    }
    // Execute restore request
    new RestoreTablesClient(conn, request).execute();
  }

  @Override
  public String backupTables(BackupRequest request) throws IOException {
    BackupType type = request.getBackupType();
    String targetRootDir = request.getTargetRootDir();
    List<TableName> tableList = request.getTableList();

    String backupId = BackupRestoreConstants.BACKUPID_PREFIX + EnvironmentEdgeManager.currentTime();
    if (type == BackupType.INCREMENTAL) {
      Set<TableName> incrTableSet;
      try (BackupSystemTable table = new BackupSystemTable(conn)) {
        incrTableSet = table.getIncrementalBackupTableSet(targetRootDir);
      }

      if (incrTableSet.isEmpty()) {
        String msg = "Incremental backup table set contains no tables. "
                + "You need to run full backup first "
                + (tableList != null ? "on " + StringUtils.join(tableList, ",") : "");

        throw new IOException(msg);
      }
      if (tableList != null) {
        tableList.removeAll(incrTableSet);
        if (!tableList.isEmpty()) {
          String extraTables = StringUtils.join(tableList, ",");
          String msg = "Some tables (" + extraTables + ") haven't gone through full backup. "
                  + "Perform full backup on " + extraTables + " first, " + "then retry the command";
          throw new IOException(msg);
        }
      }
      tableList = Lists.newArrayList(incrTableSet);
    }
    if (tableList != null && !tableList.isEmpty()) {
      for (TableName table : tableList) {
        String targetTableBackupDir =
            HBackupFileSystem.getTableBackupDir(targetRootDir, backupId, table);
        Path targetTableBackupDirPath = new Path(targetTableBackupDir);
        FileSystem outputFs =
            FileSystem.get(targetTableBackupDirPath.toUri(), conn.getConfiguration());
        if (outputFs.exists(targetTableBackupDirPath)) {
          throw new IOException("Target backup directory " + targetTableBackupDir
              + " exists already.");
        }
        outputFs.mkdirs(targetTableBackupDirPath);
      }
      ArrayList<TableName> nonExistingTableList = null;
      try (Admin admin = conn.getAdmin()) {
        for (TableName tableName : tableList) {
          if (!admin.tableExists(tableName)) {
            if (nonExistingTableList == null) {
              nonExistingTableList = new ArrayList<>();
            }
            nonExistingTableList.add(tableName);
          }
        }
      }
      if (nonExistingTableList != null) {
        if (type == BackupType.INCREMENTAL) {
          // Update incremental backup set
          tableList = excludeNonExistingTables(tableList, nonExistingTableList);
        } else {
          // Throw exception only in full mode - we try to backup non-existing table
          throw new IOException("Non-existing tables found in the table list: "
              + nonExistingTableList);
        }
      }
    }

    // update table list
    BackupRequest.Builder builder = new BackupRequest.Builder();
    request = builder.withBackupType(request.getBackupType()).withTableList(tableList)
            .withTargetRootDir(request.getTargetRootDir())
            .withBackupSetName(request.getBackupSetName()).withTotalTasks(request.getTotalTasks())
            .withBandwidthPerTasks((int) request.getBandwidth()).build();

    TableBackupClient client;
    try {
      client = BackupClientFactory.create(conn, backupId, request);
    } catch (IOException e) {
      LOG.error("There is an active session already running");
      throw e;
    }

    client.execute();

    return backupId;
  }

  private List<TableName> excludeNonExistingTables(List<TableName> tableList,
      List<TableName> nonExistingTableList) {
    for (TableName table : nonExistingTableList) {
      tableList.remove(table);
    }
    return tableList;
  }

  @Override
  public void mergeBackups(String[] backupIds) throws IOException {
    try (final BackupSystemTable sysTable = new BackupSystemTable(conn)) {
      checkIfValidForMerge(backupIds, sysTable);
      //TODO run job on remote cluster
      BackupMergeJob job = BackupRestoreFactory.getBackupMergeJob(conn.getConfiguration());
      job.run(backupIds);
    }
  }

  /**
   * Verifies that backup images are valid for merge.
   *
   * <ul>
   * <li>All backups MUST be in the same destination
   * <li>No FULL backups are allowed - only INCREMENTAL
   * <li>All backups must be in COMPLETE state
   * <li>No holes in backup list are allowed
   * </ul>
   * <p>
   * @param backupIds list of backup ids
   * @param table backup system table
   * @throws IOException if the backup image is not valid for merge
   */
  private void checkIfValidForMerge(String[] backupIds, BackupSystemTable table)
          throws IOException {
    String backupRoot = null;

    final Set<TableName> allTables = new HashSet<>();
    final Set<String> allBackups = new HashSet<>();
    long minTime = Long.MAX_VALUE, maxTime = Long.MIN_VALUE;
    for (String backupId : backupIds) {
      BackupInfo bInfo = table.readBackupInfo(backupId);
      if (bInfo == null) {
        String msg = "Backup session " + backupId + " not found";
        throw new IOException(msg);
      }
      if (backupRoot == null) {
        backupRoot = bInfo.getBackupRootDir();
      } else if (!bInfo.getBackupRootDir().equals(backupRoot)) {
        throw new IOException("Found different backup destinations in a list of a backup sessions "
                + "\n1. " + backupRoot + "\n" + "2. " + bInfo.getBackupRootDir());
      }
      if (bInfo.getType() == BackupType.FULL) {
        throw new IOException("FULL backup image can not be merged for: \n" + bInfo);
      }

      if (bInfo.getState() != BackupState.COMPLETE) {
        throw new IOException("Backup image " + backupId
            + " can not be merged becuase of its state: " + bInfo.getState());
      }
      allBackups.add(backupId);
      allTables.addAll(bInfo.getTableNames());
      long time = bInfo.getStartTs();
      if (time < minTime) {
        minTime = time;
      }
      if (time > maxTime) {
        maxTime = time;
      }
    }

    final long startRangeTime  = minTime;
    final long endRangeTime = maxTime;
    final String backupDest = backupRoot;
    // Check we have no 'holes' in backup id list
    // Filter 1 : backupRoot
    // Filter 2 : time range filter
    // Filter 3 : table filter
    BackupInfo.Filter destinationFilter = info -> info.getBackupRootDir().equals(backupDest);

    BackupInfo.Filter timeRangeFilter = info -> {
      long time = info.getStartTs();
      return time >= startRangeTime && time <= endRangeTime ;
    };

    BackupInfo.Filter tableFilter = info -> {
      List<TableName> tables = info.getTableNames();
      return !Collections.disjoint(allTables, tables);
    };

    BackupInfo.Filter typeFilter = info -> info.getType() == BackupType.INCREMENTAL;
    BackupInfo.Filter stateFilter = info -> info.getState() == BackupState.COMPLETE;

    List<BackupInfo> allInfos = table.getBackupHistory(-1, destinationFilter,
          timeRangeFilter, tableFilter, typeFilter, stateFilter);
    if (allInfos.size() != allBackups.size()) {
      // Yes we have at least one  hole in backup image sequence
      List<String> missingIds = new ArrayList<>();
      for(BackupInfo info: allInfos) {
        if(allBackups.contains(info.getBackupId())) {
          continue;
        }
        missingIds.add(info.getBackupId());
      }
      String errMsg =
          "Sequence of backup ids has 'holes'. The following backup images must be added:" +
           org.apache.hadoop.util.StringUtils.join(",", missingIds);
      throw new IOException(errMsg);
    }
  }
}
