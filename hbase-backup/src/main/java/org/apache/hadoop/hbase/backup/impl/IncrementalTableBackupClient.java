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

import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.CONF_CONTINUOUS_BACKUP_WAL_DIR;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.JOB_NAME_CONF_KEY;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupCopyJob;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupPhase;
import org.apache.hadoop.hbase.backup.BackupRequest;
import org.apache.hadoop.hbase.backup.BackupRestoreFactory;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.backup.HBackupFileSystem;
import org.apache.hadoop.hbase.backup.mapreduce.MapReduceBackupCopyJob;
import org.apache.hadoop.hbase.backup.mapreduce.MapReduceHFileSplitterJob;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.WALInputFormat;
import org.apache.hadoop.hbase.mapreduce.WALPlayer;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.snapshot.SnapshotRegionLocator;
import org.apache.hadoop.hbase.snapshot.SnapshotTTLExpiredException;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.util.Tool;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Strings;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;

/**
 * Incremental backup implementation. See the {@link #execute() execute} method.
 */
@InterfaceAudience.Private
public class IncrementalTableBackupClient extends TableBackupClient {
  private static final Logger LOG = LoggerFactory.getLogger(IncrementalTableBackupClient.class);
  private static final String BULKLOAD_COLLECTOR_OUTPUT = "bulkload-collector-output";

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
   * Check if a given path belongs to active WAL directory
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

  /**
   * Reads bulk load records from backup table, iterates through the records and forms the paths for
   * bulk loaded hfiles. Copies the bulk loaded hfiles to the backup destination. This method does
   * NOT clean up the entries in the bulk load system table. Those entries should not be cleaned
   * until the backup is marked as complete.
   * @param tablesToBackup list of tables to be backed up
   */
  protected List<BulkLoad> handleBulkLoad(List<TableName> tablesToBackup,
    Map<TableName, List<String>> tablesToWALFileList, Map<TableName, Long> tablesToPrevBackupTs)
    throws IOException {
    Map<TableName, MergeSplitBulkloadInfo> toBulkload = new HashMap<>();
    List<BulkLoad> bulkLoads = new ArrayList<>();

    FileSystem tgtFs;
    try {
      tgtFs = FileSystem.get(new URI(backupInfo.getBackupRootDir()), conf);
    } catch (URISyntaxException use) {
      throw new IOException("Unable to get FileSystem", use);
    }

    Path rootdir = CommonFSUtils.getRootDir(conf);
    Path tgtRoot = new Path(new Path(backupInfo.getBackupRootDir()), backupId);

    if (!backupInfo.isContinuousBackupEnabled()) {
      bulkLoads = backupManager.readBulkloadRows(tablesToBackup);
      for (BulkLoad bulkLoad : bulkLoads) {
        TableName srcTable = bulkLoad.getTableName();
        if (!tablesToBackup.contains(srcTable)) {
          LOG.debug("Skipping {} since it is not in tablesToBackup", srcTable);
          continue;
        }

        MergeSplitBulkloadInfo bulkloadInfo =
          toBulkload.computeIfAbsent(srcTable, MergeSplitBulkloadInfo::new);
        String regionName = bulkLoad.getRegion();
        String fam = bulkLoad.getColumnFamily();
        String filename = FilenameUtils.getName(bulkLoad.getHfilePath());
        Path tblDir = CommonFSUtils.getTableDir(rootdir, srcTable);
        Path p = new Path(tblDir, regionName + Path.SEPARATOR + fam + Path.SEPARATOR + filename);
        String srcTableQualifier = srcTable.getQualifierAsString();
        String srcTableNs = srcTable.getNamespaceAsString();
        Path tgtFam = new Path(tgtRoot, srcTableNs + Path.SEPARATOR + srcTableQualifier
          + Path.SEPARATOR + regionName + Path.SEPARATOR + fam);
        if (!tgtFs.mkdirs(tgtFam)) {
          throw new IOException("couldn't create " + tgtFam);
        }

        Path tgt = new Path(tgtFam, filename);
        Path archiveDir = HFileArchiveUtil.getStoreArchivePath(conf, srcTable, regionName, fam);
        Path archive = new Path(archiveDir, filename);

        if (fs.exists(p)) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("found bulk hfile {} in {} for {}", bulkLoad.getHfilePath(), p.getParent(),
              srcTableQualifier);
            LOG.trace("copying {} to {}", p, tgt);
          }
          bulkloadInfo.addActiveFile(p.toString());
        } else if (fs.exists(archive)) {
          LOG.debug("copying archive {} to {}", archive, tgt);
          bulkloadInfo.addArchiveFiles(archive.toString());
        }
      }

      for (MergeSplitBulkloadInfo bulkloadInfo : toBulkload.values()) {
        mergeSplitAndCopyBulkloadedHFiles(bulkloadInfo.getActiveFiles(),
          bulkloadInfo.getArchiveFiles(), bulkloadInfo.getSrcTable(), tgtFs);
      }
    } else {
      // Continuous incremental backup: run BulkLoadCollectorJob over backed-up WALs
      Path collectorOutput = new Path(getBulkOutputDir(), BULKLOAD_COLLECTOR_OUTPUT);
      for (TableName table : tablesToBackup) {
        long startTs = tablesToPrevBackupTs.getOrDefault(table, 0L);
        long endTs = backupInfo.getIncrCommittedWalTs();
        List<String> walDirs = tablesToWALFileList.getOrDefault(table, new ArrayList<String>());

        List<Path> bulkloadPaths = BackupUtils.collectBulkFiles(conn, table, table, startTs, endTs,
          collectorOutput, walDirs);

        List<String> bulkLoadFiles =
          bulkloadPaths.stream().map(Path::toString).collect(Collectors.toList());

        if (bulkLoadFiles.isEmpty()) {
          LOG.info("No bulk-load files found for table {}", table);
          continue;
        }

        mergeSplitAndCopyBulkloadedHFiles(bulkLoadFiles, table, tgtFs);
      }
    }
    return bulkLoads;
  }

  private void mergeSplitAndCopyBulkloadedHFiles(List<String> activeFiles,
    List<String> archiveFiles, TableName tn, FileSystem tgtFs) throws IOException {
    int attempt = 1;

    while (!activeFiles.isEmpty()) {
      LOG.info("MergeSplit {} active bulk loaded files. Attempt={}", activeFiles.size(), attempt++);
      // Active file can be archived during copy operation,
      // we need to handle this properly
      try {
        mergeSplitAndCopyBulkloadedHFiles(activeFiles, tn, tgtFs);
        break;
      } catch (IOException e) {
        int numActiveFiles = activeFiles.size();
        updateFileLists(activeFiles, archiveFiles);
        if (activeFiles.size() < numActiveFiles) {
          // We've archived some files, delete bulkloads directory
          // and re-try
          deleteBulkLoadDirectory();
          continue;
        }

        throw e;
      }
    }

    if (!archiveFiles.isEmpty()) {
      mergeSplitAndCopyBulkloadedHFiles(archiveFiles, tn, tgtFs);
    }
  }

  private void mergeSplitAndCopyBulkloadedHFiles(List<String> files, TableName tn, FileSystem tgtFs)
    throws IOException {
    MapReduceHFileSplitterJob player = new MapReduceHFileSplitterJob();
    conf.set(MapReduceHFileSplitterJob.BULK_OUTPUT_CONF_KEY,
      getBulkOutputDirForTable(tn).toString());
    player.setConf(conf);

    String inputDirs = StringUtils.join(files, ",");
    String[] args = { inputDirs, tn.getNameWithNamespaceInclAsString() };

    int result;

    try {
      result = player.run(args);
    } catch (Exception e) {
      LOG.error("Failed to run MapReduceHFileSplitterJob", e);
      // Delete the bulkload directory if we fail to run the HFile splitter job for any reason
      // as it might be re-tried
      deleteBulkLoadDirectory();
      throw new IOException(e);
    }

    if (result != 0) {
      throw new IOException(
        "Failed to run MapReduceHFileSplitterJob with invalid result: " + result);
    }

    incrementalCopyBulkloadHFiles(tgtFs, tn);
  }

  public void updateFileLists(List<String> activeFiles, List<String> archiveFiles)
    throws IOException {
    List<String> newlyArchived = new ArrayList<>();

    for (String spath : activeFiles) {
      if (!fs.exists(new Path(spath))) {
        newlyArchived.add(spath);
      }
    }

    if (!newlyArchived.isEmpty()) {
      String rootDir = CommonFSUtils.getRootDir(conf).toString();

      activeFiles.removeAll(newlyArchived);
      for (String file : newlyArchived) {
        String archivedFile = file.substring(rootDir.length() + 1);
        Path archivedFilePath = new Path(HFileArchiveUtil.getArchivePath(conf), archivedFile);
        archivedFile = archivedFilePath.toString();

        if (!fs.exists(archivedFilePath)) {
          throw new IOException(String.format(
            "File %s no longer exists, and no archived file %s exists for it", file, archivedFile));
        }

        LOG.debug("Archived file {} has been updated", archivedFile);
        archiveFiles.add(archivedFile);
      }
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
    // tablesToWALFileList and tablesToPrevBackupTs are needed for "continuous" Incremental backup
    Map<TableName, List<String>> tablesToWALFileList = new HashMap<>();
    Map<TableName, Long> tablesToPrevBackupTs = new HashMap<>();
    try {
      Map<TableName, String> tablesToFullBackupIds = getFullBackupIds();
      verifyCfCompatibility(backupInfo.getTables(), tablesToFullBackupIds);

      // case PREPARE_INCREMENTAL:
      if (backupInfo.isContinuousBackupEnabled()) {
        // committedWALsTs is needed only for Incremental backups with continuous backup
        // since these do not depend on log roll ts
        long committedWALsTs = BackupUtils.getReplicationCheckpoint(conn);
        backupInfo.setIncrCommittedWalTs(committedWALsTs);
      }
      beginBackup(backupManager, backupInfo);
      backupInfo.setPhase(BackupPhase.PREPARE_INCREMENTAL);
      // Non-continuous Backup incremental backup is controlled by 'incremental backup table set'
      // and not by user provided backup table list. This is an optimization to avoid copying
      // the same set of WALs for incremental backups of different tables at different times
      // HBASE-14038
      // Continuous-incremental backup backs up user provided table list/set
      Set<TableName> currentTableSet;
      if (backupInfo.isContinuousBackupEnabled()) {
        currentTableSet = backupInfo.getTables();
      } else {
        currentTableSet = backupManager.getIncrementalBackupTableSet();
        newTimestamps = ((IncrementalBackupManager) backupManager).getIncrBackupLogFileMap();
      }
      LOG.debug("For incremental backup, the current table set is {}", currentTableSet);
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
      setupRegionLocator();
      // convert WAL to HFiles and copy them to .tmp under BACKUP_ROOT
      convertWALsToHFiles(tablesToWALFileList, tablesToPrevBackupTs);
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
      if (!backupInfo.isContinuousBackupEnabled()) {
        // Set the previousTimestampMap which is before this current log roll to the manifest.
        Map<TableName, Map<String, Long>> previousTimestampMap =
          backupManager.readLogTimestampMap();
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
      }

      List<BulkLoad> bulkLoads =
        handleBulkLoad(backupInfo.getTableNames(), tablesToWALFileList, tablesToPrevBackupTs);

      // backup complete
      completeBackup(conn, backupInfo, BackupType.INCREMENTAL, conf);

      List<byte[]> bulkLoadedRows = Lists.transform(bulkLoads, BulkLoad::getRowKey);
      backupManager.deleteBulkLoadedRows(bulkLoadedRows);
    } catch (IOException e) {
      failBackup(conn, backupInfo, backupManager, e, "Unexpected Exception : ",
        BackupType.INCREMENTAL, conf);
      throw new IOException(e);
    } finally {
      if (backupInfo.isContinuousBackupEnabled()) {
        deleteBulkLoadDirectory();
      }
    }
  }

  protected void incrementalCopyHFiles(String[] files, String backupDest) throws IOException {
    boolean diskBasedSortingOriginalValue = HFileOutputFormat2.diskBasedSortingEnabled(conf);
    try {
      LOG.debug("Incremental copy HFiles is starting. dest={}", backupDest);
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
      conf.setBoolean(HFileOutputFormat2.DISK_BASED_SORTING_ENABLED_KEY, true);

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
      conf.setBoolean(HFileOutputFormat2.DISK_BASED_SORTING_ENABLED_KEY,
        diskBasedSortingOriginalValue);
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

  protected void convertWALsToHFiles(Map<TableName, List<String>> tablesToWALFileList,
    Map<TableName, Long> tablesToPrevBackupTs) throws IOException {
    long previousBackupTs = 0L;
    long currentBackupTs = 0L;
    if (backupInfo.isContinuousBackupEnabled()) {
      String walBackupDir = conf.get(CONF_CONTINUOUS_BACKUP_WAL_DIR);
      if (Strings.isNullOrEmpty(walBackupDir)) {
        throw new IOException(
          "Incremental backup requires the WAL backup directory " + CONF_CONTINUOUS_BACKUP_WAL_DIR);
      }
      Path walBackupPath = new Path(walBackupDir);
      Set<TableName> tableSet = backupInfo.getTables();
      currentBackupTs = backupInfo.getIncrCommittedWalTs();
      List<BackupInfo> backupInfos = backupManager.getBackupHistory(true);
      for (TableName table : tableSet) {
        for (BackupInfo backup : backupInfos) {
          // find previous backup for this table
          if (backup.getTables().contains(table)) {
            LOG.info("Found previous backup of type {} with id {} for table {}", backup.getType(),
              backup.getBackupId(), table.getNameAsString());
            List<String> walBackupFileList;
            if (backup.getType() == BackupType.FULL) {
              previousBackupTs = backup.getStartTs();
            } else {
              previousBackupTs = backup.getIncrCommittedWalTs();
            }
            walBackupFileList =
              BackupUtils.getValidWalDirs(conf, walBackupPath, previousBackupTs, currentBackupTs);
            tablesToWALFileList.put(table, walBackupFileList);
            tablesToPrevBackupTs.put(table, previousBackupTs);
            walToHFiles(walBackupFileList, Arrays.asList(table.getNameAsString()),
              previousBackupTs);
            break;
          }
        }
      }
    } else {
      // get incremental backup file list and prepare parameters for DistCp
      List<String> incrBackupFileList = backupInfo.getIncrBackupFileList();
      // Get list of tables in incremental backup set
      Set<TableName> tableSet = backupManager.getIncrementalBackupTableSet();
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
      walToHFiles(incrBackupFileList, tableList, previousBackupTs);
    }
  }

  protected boolean tableExists(TableName table, Connection conn) throws IOException {
    try (Admin admin = conn.getAdmin()) {
      return admin.tableExists(table);
    }
  }

  protected void walToHFiles(List<String> dirPaths, List<String> tableList, long previousBackupTs)
    throws IOException {
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
    boolean diskBasedSortingEnabledOriginalValue = HFileOutputFormat2.diskBasedSortingEnabled(conf);
    conf.setBoolean(HFileOutputFormat2.DISK_BASED_SORTING_ENABLED_KEY, true);
    if (backupInfo.isContinuousBackupEnabled()) {
      conf.set(WALInputFormat.START_TIME_KEY, Long.toString(previousBackupTs));
      conf.set(WALInputFormat.END_TIME_KEY, Long.toString(backupInfo.getIncrCommittedWalTs()));
    }
    String[] playerArgs = { dirs, StringUtils.join(tableList, ",") };

    try {
      player.setConf(conf);
      int result = player.run(playerArgs);
      if (result != 0) {
        throw new IOException("WAL Player failed");
      }
    } catch (IOException e) {
      throw e;
    } catch (Exception ee) {
      throw new IOException("Can not convert from directory " + dirs
        + " (check Hadoop, HBase and WALPlayer M/R job logs) ", ee);
    } finally {
      conf.setBoolean(HFileOutputFormat2.DISK_BASED_SORTING_ENABLED_KEY,
        diskBasedSortingEnabledOriginalValue);
      conf.unset(WALPlayer.INPUT_FILES_SEPARATOR_KEY);
      conf.unset(JOB_NAME_CONF_KEY);
    }
  }

  private void incrementalCopyBulkloadHFiles(FileSystem tgtFs, TableName tn) throws IOException {
    Path bulkOutDir = getBulkOutputDirForTable(tn);

    if (tgtFs.exists(bulkOutDir)) {
      conf.setInt(MapReduceBackupCopyJob.NUMBER_OF_LEVELS_TO_PRESERVE_KEY, 2);
      Path tgtPath = getTargetDirForTable(tn);
      try {
        RemoteIterator<LocatedFileStatus> locatedFiles = tgtFs.listFiles(bulkOutDir, true);
        List<String> files = new ArrayList<>();
        while (locatedFiles.hasNext()) {
          LocatedFileStatus file = locatedFiles.next();
          if (file.isFile() && HFile.isHFileFormat(tgtFs, file.getPath())) {
            files.add(file.getPath().toString());
          }
        }
        incrementalCopyHFiles(files.toArray(files.toArray(new String[0])), tgtPath.toString());
      } finally {
        conf.unset(MapReduceBackupCopyJob.NUMBER_OF_LEVELS_TO_PRESERVE_KEY);
      }
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

  private Path getTargetDirForTable(TableName table) {
    Path path = new Path(backupInfo.getBackupRootDir() + Path.SEPARATOR + backupInfo.getBackupId());
    path = new Path(path, table.getNamespaceAsString());
    path = new Path(path, table.getQualifierAsString());
    return path;
  }

  private void setupRegionLocator() throws IOException {
    Map<TableName, String> fullBackupIds = getFullBackupIds();
    try (BackupAdminImpl backupAdmin = new BackupAdminImpl(conn)) {

      for (TableName tableName : backupInfo.getTables()) {
        String fullBackupId = fullBackupIds.get(tableName);
        BackupInfo fullBackupInfo = backupAdmin.getBackupInfo(fullBackupId);
        String snapshotName = fullBackupInfo.getSnapshotName(tableName);
        Path root = HBackupFileSystem.getTableBackupPath(tableName,
          new Path(fullBackupInfo.getBackupRootDir()), fullBackupId);
        String manifestDir =
          SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, root).toString();
        SnapshotRegionLocator.setSnapshotManifestDir(conf, manifestDir, tableName);
      }
    }
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
        if (
          SnapshotDescriptionUtils.isExpiredSnapshot(snapshotDescription.getTtl(),
            snapshotDescription.getCreationTime(), EnvironmentEdgeManager.currentTime())
        ) {
          throw new SnapshotTTLExpiredException(
            ProtobufUtil.createSnapshotDesc(snapshotDescription));
        }

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
