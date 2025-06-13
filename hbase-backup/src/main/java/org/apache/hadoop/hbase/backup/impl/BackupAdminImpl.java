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
import java.util.Arrays;
import java.util.Collections;
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
        // failed sessions found
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
        List<String> affectedBackupRootDirs = new ArrayList<>();
        for (int i = 0; i < backupIds.length; i++) {
          BackupInfo info = sysTable.readBackupInfo(backupIds[i]);
          if (info == null) {
            LOG.warn("Delete backup failed: no information found for backupID={}", backupIds[i]);
            continue;
          }
          affectedBackupRootDirs.add(info.getBackupRootDir());
          totalDeleted += deleteBackup(info, sysTable);
        }
        finalizeDelete(affectedBackupRootDirs, sysTable);
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
   * @param backupRoots backupRoots for which to revise the incremental backup set
   * @param table       backup system table
   * @throws IOException if a table operation fails
   */
  private void finalizeDelete(List<String> backupRoots, BackupSystemTable table)
    throws IOException {
    for (String backupRoot : backupRoots) {
      Set<TableName> incrTableSet = table.getIncrementalBackupTableSet(backupRoot);
      Map<TableName, List<BackupInfo>> tableMap =
        table.getBackupHistoryForTableSet(incrTableSet, backupRoot);

      // Keep only the tables that are present in other backups
      incrTableSet.retainAll(tableMap.keySet());

      table.deleteIncrementalBackupTableSet(backupRoot);
      if (!incrTableSet.isEmpty()) {
        table.addIncrementalBackupTableSet(incrTableSet, backupRoot);
      }
    }
  }

  /**
   * Deletes a single backup, updating the metadata of any dependent backups as necessary. If
   * dependent backups no longer contain any tables, they are deleted as well.
   */
  private int deleteBackup(BackupInfo backupToDelete, BackupSystemTable sysTable)
    throws IOException {
    int totalDeleted = 1;
    LOG.info("Deleting backup {} ...", backupToDelete.getBackupId());
    // Clean up data for backup session (idempotent)
    BackupUtils.cleanupBackupData(backupToDelete, conn.getConfiguration());

    List<BackupInfo> history = sysTable.getBackupHistory(backupToDelete.getBackupRootDir());
    for (TableName tn : backupToDelete.getTableNames()) {
      List<BackupInfo> affectedBackups = getAffectedBackupSessions(backupToDelete, tn, history);
      for (BackupInfo info : affectedBackups) {
        if (removeTableFromBackupImage(info, tn, sysTable)) {
          totalDeleted++;
        }
      }
    }
    sysTable.deleteBackupInfo(backupToDelete.getBackupId());
    LOG.info("Deletion of backup {} completed.", backupToDelete.getBackupId());

    // If there are no more backups, clear the bulk loaded rows.
    // HBASE-28706: this should be done on a per backup-root basis.
    if (sysTable.getBackupHistory().isEmpty()) {
      List<BulkLoad> bulkLoads = sysTable.readBulkloadRows();
      List<byte[]> bulkLoadedRows = Lists.transform(bulkLoads, BulkLoad::getRowKey);
      sysTable.deleteBulkLoadedRows(bulkLoadedRows);
    }
    return totalDeleted;
  }

  /**
   * Adjusts the metadata of the given backup to no longer include the given table. If the backup no
   * longer contains any tables, the backup is deleted.
   * @return true if the backup is deleted
   */
  private boolean removeTableFromBackupImage(BackupInfo info, TableName tn,
    BackupSystemTable sysTable) throws IOException {
    List<TableName> tables = info.getTableNames();
    LOG.debug("Removing table {} from backup {} (tables={})", tn, info.getBackupId(),
      info.getTableListAsString());

    if (tables.remove(tn)) {
      if (tables.isEmpty()) {
        LOG.debug("Deleting backup info {}", info.getBackupId());

        sysTable.deleteBackupInfo(info.getBackupId());
        // Idempotent operation
        BackupUtils.cleanupBackupData(info, conn.getConfiguration());
        return true;
      } else {
        info.setTables(tables);
        sysTable.updateBackupInfo(info);
        // Now, clean up directory for table (idempotent)
        cleanupBackupDir(info, tn, conn.getConfiguration());
        return false;
      }
    }
    return false;
  }

  /**
   * Collects all backups whose metadata would need to be updated due to a given backup being
   * removed, for a given table (from that backup being removed).
   * <p>
   * This is the list of all backups that are newer than the backup being removed, up to (and
   * excluding) the first FULL backup containing the given table. If no newer FULL backup exists,
   * this means all newer backups.
   * @param backupBeingRemoved the backup being removed
   * @param tn                 the name of a table present in {@code backupBeingRemoved}
   * @param history            the backup history corresponding to the backup root of
   *                           {@code backupBeingRemoved}, from newest to oldest
   * @return the list of backups whose metadata would need to be updated
   */
  private List<BackupInfo> getAffectedBackupSessions(BackupInfo backupBeingRemoved, TableName tn,
    List<BackupInfo> history) {
    LOG.debug("GetAffectedBackupInfos for: {} table={}", backupBeingRemoved.getBackupId(), tn);
    List<BackupInfo> result = new ArrayList<>();

    for (BackupInfo historicBackup : history) {
      if (historicBackup.equals(backupBeingRemoved)) {
        break;
      }
      Set<TableName> tables = historicBackup.getTables();
      if (tables.contains(tn)) {
        if (historicBackup.getType() == BackupType.FULL) {
          result.clear();
        } else {
          LOG.debug("GetAffectedBackupInfos for: {} table={} added {} tables={}",
            backupBeingRemoved.getBackupId(), tn, historicBackup.getBackupId(),
            historicBackup.getTableListAsString());
          result.add(historicBackup);
        }
      }
    }
    return result;
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

      Path targetDirPath = new Path(BackupUtils.getTableBackupDir(backupInfo.getBackupRootDir(),
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
      LOG.info(
        "Added tables [" + StringUtils.join(tableNames, " ") + "] to '" + name + "' backup set");
    }
  }

  @Override
  public void removeFromBackupSet(String name, TableName[] tables) throws IOException {
    LOG.info("Removing tables [" + StringUtils.join(tables, " ") + "] from '" + name + "'");
    try (final BackupSystemTable table = new BackupSystemTable(conn)) {
      table.removeFromBackupSet(name, toStringArray(tables));
      LOG.info(
        "Removing tables [" + StringUtils.join(tables, " ") + "] from '" + name + "' completed.");
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
      // check and load backup image manifest for the tables
      Path rootPath = new Path(request.getBackupRootDir());
      String backupId = request.getBackupId();
      TableName[] sTableArray = request.getFromTables();
      BackupManifest manifest =
        HBackupFileSystem.getManifest(conn.getConfiguration(), rootPath, backupId);

      // Check and validate the backup image and its dependencies
      if (BackupUtils.validate(Arrays.asList(sTableArray), manifest, conn.getConfiguration())) {
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
        String msg =
          "Incremental backup table set contains no tables. " + "You need to run full backup first "
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
          throw new IOException(
            "Target backup directory " + targetTableBackupDir + " exists already.");
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
          throw new IOException(
            "Non-existing tables found in the table list: " + nonExistingTableList);
        }
      }
    }

    // update table list
    BackupRequest.Builder builder = new BackupRequest.Builder();
    request = builder.withBackupType(request.getBackupType()).withTableList(tableList)
      .withTargetRootDir(request.getTargetRootDir()).withBackupSetName(request.getBackupSetName())
      .withTotalTasks(request.getTotalTasks()).withBandwidthPerTasks((int) request.getBandwidth())
      .withNoChecksumVerify(request.getNoChecksumVerify()).build();

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
      // TODO run job on remote cluster
      BackupMergeJob job = BackupRestoreFactory.getBackupMergeJob(conn.getConfiguration());
      job.run(backupIds);
    }
  }

  /**
   * Verifies that backup images are valid for merge.
   * <ul>
   * <li>All backups MUST be in the same destination
   * <li>No FULL backups are allowed - only INCREMENTAL
   * <li>All backups must be in COMPLETE state
   * <li>No holes in backup list are allowed
   * </ul>
   * <p>
   * @param backupIds list of backup ids
   * @param table     backup system table
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

    final long startRangeTime = minTime;
    final long endRangeTime = maxTime;
    final String backupDest = backupRoot;
    // Check we have no 'holes' in backup id list
    // Filter 1 : backupRoot
    // Filter 2 : time range filter
    // Filter 3 : table filter
    BackupInfo.Filter destinationFilter = info -> info.getBackupRootDir().equals(backupDest);

    BackupInfo.Filter timeRangeFilter = info -> {
      long time = info.getStartTs();
      return time >= startRangeTime && time <= endRangeTime;
    };

    BackupInfo.Filter tableFilter = info -> {
      List<TableName> tables = info.getTableNames();
      return !Collections.disjoint(allTables, tables);
    };

    BackupInfo.Filter typeFilter = info -> info.getType() == BackupType.INCREMENTAL;
    BackupInfo.Filter stateFilter = info -> info.getState() == BackupState.COMPLETE;

    List<BackupInfo> allInfos = table.getBackupHistory(-1, destinationFilter, timeRangeFilter,
      tableFilter, typeFilter, stateFilter);
    if (allInfos.size() != allBackups.size()) {
      // Yes we have at least one hole in backup image sequence
      List<String> missingIds = new ArrayList<>();
      for (BackupInfo info : allInfos) {
        if (allBackups.contains(info.getBackupId())) {
          continue;
        }
        missingIds.add(info.getBackupId());
      }
      String errMsg =
        "Sequence of backup ids has 'holes'. The following backup images must be added:"
          + org.apache.hadoop.util.StringUtils.join(",", missingIds);
      throw new IOException(errMsg);
    }
  }
}
