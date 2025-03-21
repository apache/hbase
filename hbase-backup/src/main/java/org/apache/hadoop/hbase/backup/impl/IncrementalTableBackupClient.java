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

import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.JOB_NAME_CONF_KEY;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupCopyJob;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupPhase;
import org.apache.hadoop.hbase.backup.BackupRequest;
import org.apache.hadoop.hbase.backup.BackupRestoreFactory;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.backup.HBackupFileSystem;
import org.apache.hadoop.hbase.backup.mapreduce.MapReduceBackupCopyJob;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.mapreduce.WALPlayer;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.util.Tool;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;

/**
 * Incremental backup implementation. See the {@link #execute() execute} method.
 */
@InterfaceAudience.Private
public class IncrementalTableBackupClient extends TableBackupClient {
  private static final Logger LOG = LoggerFactory.getLogger(IncrementalTableBackupClient.class);

  protected IncrementalTableBackupClient() {
  }

  public IncrementalTableBackupClient(final Connection conn, final String backupId,
    BackupRequest request) throws IOException {
    super(conn, backupId, request);
  }

  protected List<String> filterMissingFiles(List<String> incrBackupFileList) throws IOException {
    List<String> list = new ArrayList<>();
    for (String file : incrBackupFileList) {
      Path p = new Path(file);
      if (fs.exists(p) || isActiveWalPath(p)) {
        list.add(file);
      } else {
        LOG.warn("Can't find file: " + file);
      }
    }
    return list;
  }

  /**
   * Check if a given path is belongs to active WAL directory
   * @param p path
   * @return true, if yes
   */
  protected boolean isActiveWalPath(Path p) {
    return !AbstractFSWALProvider.isArchivedLogFile(p);
  }

  protected static int getIndex(TableName tbl, List<TableName> sTableList) {
    if (sTableList == null) {
      return 0;
    }

    for (int i = 0; i < sTableList.size(); i++) {
      if (tbl.equals(sTableList.get(i))) {
        return i;
      }
    }
    return -1;
  }

  /*
   * Reads bulk load records from backup table, iterates through the records and forms the paths for
   * bulk loaded hfiles. Copies the bulk loaded hfiles to backup destination. This method does NOT
   * clean up the entries in the bulk load system table. Those entries should not be cleaned until
   * the backup is marked as complete.
   * @param sTableList list of tables to be backed up
   * @return the rowkeys of bulk loaded files
   */
  @SuppressWarnings("unchecked")
  protected List<byte[]> handleBulkLoad(List<TableName> sTableList) throws IOException {
    Map<byte[], List<Path>>[] mapForSrc = new Map[sTableList.size()];
    List<String> activeFiles = new ArrayList<>();
    List<String> archiveFiles = new ArrayList<>();
    Pair<Map<TableName, Map<String, Map<String, List<Pair<String, Boolean>>>>>, List<byte[]>> pair =
      backupManager.readBulkloadRows(sTableList);
    Map<TableName, Map<String, Map<String, List<Pair<String, Boolean>>>>> map = pair.getFirst();
    FileSystem tgtFs;
    try {
      tgtFs = FileSystem.get(new URI(backupInfo.getBackupRootDir()), conf);
    } catch (URISyntaxException use) {
      throw new IOException("Unable to get FileSystem", use);
    }
    Path rootdir = CommonFSUtils.getRootDir(conf);
    Path tgtRoot = new Path(new Path(backupInfo.getBackupRootDir()), backupId);

    for (Map.Entry<TableName, Map<String, Map<String, List<Pair<String, Boolean>>>>> tblEntry : map
      .entrySet()) {
      TableName srcTable = tblEntry.getKey();

      int srcIdx = getIndex(srcTable, sTableList);
      if (srcIdx < 0) {
        LOG.warn("Couldn't find " + srcTable + " in source table List");
        continue;
      }
      if (mapForSrc[srcIdx] == null) {
        mapForSrc[srcIdx] = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      }
      Path tblDir = CommonFSUtils.getTableDir(rootdir, srcTable);
      Path tgtTable = new Path(new Path(tgtRoot, srcTable.getNamespaceAsString()),
        srcTable.getQualifierAsString());
      for (Map.Entry<String, Map<String, List<Pair<String, Boolean>>>> regionEntry : tblEntry
        .getValue().entrySet()) {
        String regionName = regionEntry.getKey();
        Path regionDir = new Path(tblDir, regionName);
        // map from family to List of hfiles
        for (Map.Entry<String, List<Pair<String, Boolean>>> famEntry : regionEntry.getValue()
          .entrySet()) {
          String fam = famEntry.getKey();
          Path famDir = new Path(regionDir, fam);
          List<Path> files;
          if (!mapForSrc[srcIdx].containsKey(Bytes.toBytes(fam))) {
            files = new ArrayList<>();
            mapForSrc[srcIdx].put(Bytes.toBytes(fam), files);
          } else {
            files = mapForSrc[srcIdx].get(Bytes.toBytes(fam));
          }
          Path archiveDir = HFileArchiveUtil.getStoreArchivePath(conf, srcTable, regionName, fam);
          String tblName = srcTable.getQualifierAsString();
          Path tgtFam = new Path(new Path(tgtTable, regionName), fam);
          if (!tgtFs.mkdirs(tgtFam)) {
            throw new IOException("couldn't create " + tgtFam);
          }
          for (Pair<String, Boolean> fileWithState : famEntry.getValue()) {
            String file = fileWithState.getFirst();
            int idx = file.lastIndexOf("/");
            String filename = file;
            if (idx > 0) {
              filename = file.substring(idx + 1);
            }
            Path p = new Path(famDir, filename);
            Path tgt = new Path(tgtFam, filename);
            Path archive = new Path(archiveDir, filename);
            if (fs.exists(p)) {
              if (LOG.isTraceEnabled()) {
                LOG.trace("found bulk hfile " + file + " in " + famDir + " for " + tblName);
              }
              if (LOG.isTraceEnabled()) {
                LOG.trace("copying " + p + " to " + tgt);
              }
              activeFiles.add(p.toString());
            } else if (fs.exists(archive)) {
              LOG.debug("copying archive " + archive + " to " + tgt);
              archiveFiles.add(archive.toString());
            }
            files.add(tgt);
          }
        }
      }
    }

    copyBulkLoadedFiles(activeFiles, archiveFiles);

    return pair.getSecond();
  }

  private void copyBulkLoadedFiles(List<String> activeFiles, List<String> archiveFiles)
    throws IOException {
    try {
      // Enable special mode of BackupDistCp
      conf.setInt(MapReduceBackupCopyJob.NUMBER_OF_LEVELS_TO_PRESERVE_KEY, 5);
      // Copy active files
      String tgtDest = backupInfo.getBackupRootDir() + Path.SEPARATOR + backupInfo.getBackupId();
      int attempt = 1;
      while (activeFiles.size() > 0) {
        LOG.info("Copy " + activeFiles.size() + " active bulk loaded files. Attempt =" + attempt++);
        String[] toCopy = new String[activeFiles.size()];
        activeFiles.toArray(toCopy);
        // Active file can be archived during copy operation,
        // we need to handle this properly
        try {
          incrementalCopyHFiles(toCopy, tgtDest);
          break;
        } catch (IOException e) {
          // Check if some files got archived
          // Update active and archived lists
          // When file is being moved from active to archive
          // directory, the number of active files decreases
          int numOfActive = activeFiles.size();
          updateFileLists(activeFiles, archiveFiles);
          if (activeFiles.size() < numOfActive) {
            continue;
          }
          // if not - throw exception
          throw e;
        }
      }
      // If incremental copy will fail for archived files
      // we will have partially loaded files in backup destination (only files from active data
      // directory). It is OK, because the backup will marked as FAILED and data will be cleaned up
      if (archiveFiles.size() > 0) {
        String[] toCopy = new String[archiveFiles.size()];
        archiveFiles.toArray(toCopy);
        incrementalCopyHFiles(toCopy, tgtDest);
      }
    } finally {
      // Disable special mode of BackupDistCp
      conf.unset(MapReduceBackupCopyJob.NUMBER_OF_LEVELS_TO_PRESERVE_KEY);
    }
  }

  private void updateFileLists(List<String> activeFiles, List<String> archiveFiles)
    throws IOException {
    List<String> newlyArchived = new ArrayList<>();

    for (String spath : activeFiles) {
      if (!fs.exists(new Path(spath))) {
        newlyArchived.add(spath);
      }
    }

    if (newlyArchived.size() > 0) {
      activeFiles.removeAll(newlyArchived);
      archiveFiles.addAll(newlyArchived);
    }

    LOG.debug(newlyArchived.size() + " files have been archived.");
  }

  /**
   * @throws IOException                   If the execution of the backup fails
   * @throws ColumnFamilyMismatchException If the column families of the current table do not match
   *                                       the column families for the last full backup. In which
   *                                       case, a full backup should be taken
   */
  @Override
  public void execute() throws IOException, ColumnFamilyMismatchException {
    try {
      Map<TableName, String> tablesToFullBackupIds = getFullBackupIds();
      verifyCfCompatibility(backupInfo.getTables(), tablesToFullBackupIds);

      // case PREPARE_INCREMENTAL:
      beginBackup(backupManager, backupInfo);
      backupInfo.setPhase(BackupPhase.PREPARE_INCREMENTAL);
      LOG.debug("For incremental backup, current table set is "
        + backupManager.getIncrementalBackupTableSet());
      newTimestamps = ((IncrementalBackupManager) backupManager).getIncrBackupLogFileMap();
    } catch (Exception e) {
      // fail the overall backup and return
      failBackup(conn, backupInfo, backupManager, e, "Unexpected Exception : ",
        BackupType.INCREMENTAL, conf);
      throw new IOException(e);
    }

    // case INCREMENTAL_COPY:
    try {
      // copy out the table and region info files for each table
      BackupUtils.copyTableRegionInfo(conn, backupInfo, conf);
      // convert WAL to HFiles and copy them to .tmp under BACKUP_ROOT
      convertWALsToHFiles();
      incrementalCopyHFiles(new String[] { getBulkOutputDir().toString() },
        backupInfo.getBackupRootDir());
    } catch (Exception e) {
      String msg = "Unexpected exception in incremental-backup: incremental copy " + backupId;
      // fail the overall backup and return
      failBackup(conn, backupInfo, backupManager, e, msg, BackupType.INCREMENTAL, conf);
      throw new IOException(e);
    }
    // case INCR_BACKUP_COMPLETE:
    // set overall backup status: complete. Here we make sure to complete the backup.
    // After this checkpoint, even if entering cancel process, will let the backup finished
    try {
      // Set the previousTimestampMap which is before this current log roll to the manifest.
      Map<TableName, Map<String, Long>> previousTimestampMap = backupManager.readLogTimestampMap();
      backupInfo.setIncrTimestampMap(previousTimestampMap);

      // The table list in backupInfo is good for both full backup and incremental backup.
      // For incremental backup, it contains the incremental backup table set.
      backupManager.writeRegionServerLogTimestamp(backupInfo.getTables(), newTimestamps);

      Map<TableName, Map<String, Long>> newTableSetTimestampMap =
        backupManager.readLogTimestampMap();

      backupInfo.setTableSetTimestampMap(newTableSetTimestampMap);
      Long newStartCode =
        BackupUtils.getMinValue(BackupUtils.getRSLogTimestampMins(newTableSetTimestampMap));
      backupManager.writeBackupStartCode(newStartCode);

      List<byte[]> bulkLoadedRows = handleBulkLoad(backupInfo.getTableNames());

      // backup complete
      completeBackup(conn, backupInfo, BackupType.INCREMENTAL, conf);

      backupManager.deleteBulkLoadedRows(bulkLoadedRows);
    } catch (IOException e) {
      failBackup(conn, backupInfo, backupManager, e, "Unexpected Exception : ",
        BackupType.INCREMENTAL, conf);
      throw new IOException(e);
    }
  }

  protected void incrementalCopyHFiles(String[] files, String backupDest) throws IOException {
    try {
      LOG.debug("Incremental copy HFiles is starting. dest=" + backupDest);
      // set overall backup phase: incremental_copy
      backupInfo.setPhase(BackupPhase.INCREMENTAL_COPY);
      // get incremental backup file list and prepare parms for DistCp
      String[] strArr = new String[files.length + 1];
      System.arraycopy(files, 0, strArr, 0, files.length);
      strArr[strArr.length - 1] = backupDest;

      String jobname = "Incremental_Backup-HFileCopy-" + backupInfo.getBackupId();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Setting incremental copy HFiles job name to : " + jobname);
      }
      conf.set(JOB_NAME_CONF_KEY, jobname);

      BackupCopyJob copyService = BackupRestoreFactory.getBackupCopyJob(conf);
      int res = copyService.copy(backupInfo, backupManager, conf, BackupType.INCREMENTAL, strArr);
      if (res != 0) {
        LOG.error("Copy incremental HFile files failed with return code: " + res + ".");
        throw new IOException(
          "Failed copy from " + StringUtils.join(files, ',') + " to " + backupDest);
      }
      LOG.debug("Incremental copy HFiles from " + StringUtils.join(files, ',') + " to " + backupDest
        + " finished.");
    } finally {
      deleteBulkLoadDirectory();
    }
  }

  protected void deleteBulkLoadDirectory() throws IOException {
    // delete original bulk load directory on method exit
    Path path = getBulkOutputDir();
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    boolean result = fs.delete(path, true);
    if (!result) {
      LOG.warn("Could not delete " + path);
    }
  }

  protected void convertWALsToHFiles() throws IOException {
    // get incremental backup file list and prepare parameters for DistCp
    List<String> incrBackupFileList = backupInfo.getIncrBackupFileList();
    // Get list of tables in incremental backup set
    Set<TableName> tableSet = backupManager.getIncrementalBackupTableSet();
    if (backupInfo.isContinuousBackupEnabled()) {
      tableSet = backupManager.getContinuousBackupTableSet().keySet();
    }
    // filter missing files out (they have been copied by previous backups)
    incrBackupFileList = filterMissingFiles(incrBackupFileList);
    List<String> tableList = new ArrayList<String>();
    for (TableName table : tableSet) {
      // Check if table exists
      if (tableExists(table, conn)) {
        tableList.add(table.getNameAsString());
      } else {
        LOG.warn("Table " + table + " does not exists. Skipping in WAL converter");
      }
    }
    walToHFiles(incrBackupFileList, tableList);
  }

  protected boolean tableExists(TableName table, Connection conn) throws IOException {
    try (Admin admin = conn.getAdmin()) {
      return admin.tableExists(table);
    }
  }

  protected void walToHFiles(List<String> dirPaths, List<String> tableList) throws IOException {
    Tool player = new WALPlayer();

    // Player reads all files in arbitrary directory structure and creates
    // a Map task for each file. We use ';' as separator
    // because WAL file names contains ','
    String dirs = StringUtils.join(dirPaths, ';');
    String jobname = "Incremental_Backup-" + backupId;

    Path bulkOutputPath = getBulkOutputDir();
    conf.set(WALPlayer.BULK_OUTPUT_CONF_KEY, bulkOutputPath.toString());
    conf.set(WALPlayer.INPUT_FILES_SEPARATOR_KEY, ";");
    conf.setBoolean(WALPlayer.MULTI_TABLES_SUPPORT, true);
    conf.set(JOB_NAME_CONF_KEY, jobname);
    String[] playerArgs = { dirs, StringUtils.join(tableList, ",") };

    try {
      player.setConf(conf);
      int result = player.run(playerArgs);
      if (result != 0) {
        throw new IOException("WAL Player failed");
      }
      conf.unset(WALPlayer.INPUT_FILES_SEPARATOR_KEY);
      conf.unset(JOB_NAME_CONF_KEY);
    } catch (IOException e) {
      throw e;
    } catch (Exception ee) {
      throw new IOException("Can not convert from directory " + dirs
        + " (check Hadoop, HBase and WALPlayer M/R job logs) ", ee);
    }
  }

  protected Path getBulkOutputDirForTable(TableName table) {
    Path tablePath = getBulkOutputDir();
    tablePath = new Path(tablePath, table.getNamespaceAsString());
    tablePath = new Path(tablePath, table.getQualifierAsString());
    return new Path(tablePath, "data");
  }

  protected Path getBulkOutputDir() {
    String backupId = backupInfo.getBackupId();
    Path path = new Path(backupInfo.getBackupRootDir());
    path = new Path(path, ".tmp");
    path = new Path(path, backupId);
    return path;
  }

  private Map<TableName, String> getFullBackupIds() throws IOException {
    // Ancestors are stored from newest to oldest, so we can iterate backwards
    // in order to populate our backupId map with the most recent full backup
    // for a given table
    List<BackupManifest.BackupImage> images = getAncestors(backupInfo);
    Map<TableName, String> results = new HashMap<>();
    for (int i = images.size() - 1; i >= 0; i--) {
      BackupManifest.BackupImage image = images.get(i);
      if (image.getType() != BackupType.FULL) {
        continue;
      }

      for (TableName tn : image.getTableNames()) {
        results.put(tn, image.getBackupId());
      }
    }
    return results;
  }

  /**
   * Verifies that the current table descriptor CFs matches the descriptor CFs of the last full
   * backup for the tables. This ensures CF compatibility across incremental backups. If a mismatch
   * is detected, a full table backup should be taken, rather than an incremental one
   */
  private void verifyCfCompatibility(Set<TableName> tables,
    Map<TableName, String> tablesToFullBackupId) throws IOException, ColumnFamilyMismatchException {
    ColumnFamilyMismatchException.ColumnFamilyMismatchExceptionBuilder exBuilder =
      ColumnFamilyMismatchException.newBuilder();
    try (Admin admin = conn.getAdmin(); BackupAdminImpl backupAdmin = new BackupAdminImpl(conn)) {
      for (TableName tn : tables) {
        String backupId = tablesToFullBackupId.get(tn);
        BackupInfo fullBackupInfo = backupAdmin.getBackupInfo(backupId);

        ColumnFamilyDescriptor[] currentCfs = admin.getDescriptor(tn).getColumnFamilies();
        String snapshotName = fullBackupInfo.getSnapshotName(tn);
        Path root = HBackupFileSystem.getTableBackupPath(tn,
          new Path(fullBackupInfo.getBackupRootDir()), fullBackupInfo.getBackupId());
        Path manifestDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, root);

        FileSystem fs;
        try {
          fs = FileSystem.get(new URI(fullBackupInfo.getBackupRootDir()), conf);
        } catch (URISyntaxException e) {
          throw new IOException("Unable to get fs for backup " + fullBackupInfo.getBackupId(), e);
        }

        SnapshotProtos.SnapshotDescription snapshotDescription =
          SnapshotDescriptionUtils.readSnapshotInfo(fs, manifestDir);
        SnapshotManifest manifest =
          SnapshotManifest.open(conf, fs, manifestDir, snapshotDescription);

        ColumnFamilyDescriptor[] backupCfs = manifest.getTableDescriptor().getColumnFamilies();
        if (!areCfsCompatible(currentCfs, backupCfs)) {
          exBuilder.addMismatchedTable(tn, currentCfs, backupCfs);
        }
      }
    }

    ColumnFamilyMismatchException ex = exBuilder.build();
    if (!ex.getMismatchedTables().isEmpty()) {
      throw ex;
    }
  }

  private static boolean areCfsCompatible(ColumnFamilyDescriptor[] currentCfs,
    ColumnFamilyDescriptor[] backupCfs) {
    if (currentCfs.length != backupCfs.length) {
      return false;
    }

    for (int i = 0; i < backupCfs.length; i++) {
      String currentCf = currentCfs[i].getNameAsString();
      String backupCf = backupCfs[i].getNameAsString();

      if (!currentCf.equals(backupCf)) {
        return false;
      }
    }

    return true;
  }
}
