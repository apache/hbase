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

import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.CONF_CONTINUOUS_BACKUP_PITR_WINDOW_DAYS;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.CONF_CONTINUOUS_BACKUP_WAL_DIR;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.DEFAULT_CONTINUOUS_BACKUP_PITR_WINDOW_DAYS;
import static org.apache.hadoop.hbase.backup.replication.BackupFileSystemManager.WALS_DIR;
import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupReplicationEndpoint.ONE_DAY_IN_MILLISECONDS;
import static org.apache.hadoop.hbase.backup.util.BackupUtils.DATE_FORMAT;
import static org.apache.hadoop.hbase.mapreduce.WALPlayer.IGNORE_EMPTY_FILES;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.backup.PointInTimeRestoreRequest;
import org.apache.hadoop.hbase.backup.RestoreRequest;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.mapreduce.WALInputFormat;
import org.apache.hadoop.hbase.mapreduce.WALPlayer;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.Tool;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for handling Point-In-Time Restore (PITR).
 * <p>
 * Defines the common PITR algorithm using the Template Method Pattern. Subclasses provide the
 * metadata source (e.g., backup system table or a custom backup location).
 * <p>
 * The PITR flow includes:
 * <ul>
 * <li>Validating recovery time within the PITR window</li>
 * <li>Checking for continuous backup and valid backup availability</li>
 * <li>Restoring the backup</li>
 * <li>Replaying WALs to bring tables to the target state</li>
 * </ul>
 * <p>
 * Subclasses must implement {@link #getBackupMetadata(PointInTimeRestoreRequest)} to supply the
 * list of completed backups.
 */
@InterfaceAudience.Private
public abstract class AbstractPitrRestoreHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractPitrRestoreHandler.class);

  protected final Connection conn;
  protected final BackupAdminImpl backupAdmin;
  protected final PointInTimeRestoreRequest request;

  AbstractPitrRestoreHandler(Connection conn, PointInTimeRestoreRequest request) {
    this.conn = conn;
    this.backupAdmin = new BackupAdminImpl(conn);
    this.request = request;
  }

  /**
   * Validates the PITR request and performs the restore if valid. This is the main entry point for
   * the PITR process and should be called by clients.
   */
  public final void validateAndRestore() throws IOException {
    long endTime = request.getToDateTime();
    validateRequestToTime(endTime);

    TableName[] sourceTableArray = request.getFromTables();
    TableName[] targetTableArray = resolveTargetTables(sourceTableArray, request.getToTables());

    // Validate PITR requirements
    validatePitr(endTime, sourceTableArray, targetTableArray);

    // If only validation is required, log and return
    if (request.isCheck()) {
      LOG.info("PITR can be successfully executed");
      return;
    }

    // Execute PITR process
    try (BackupSystemTable table = new BackupSystemTable(conn)) {
      Map<TableName, Long> continuousBackupTables = table.getContinuousBackupTableSet();
      List<PitrBackupMetadata> backupMetadataList = getBackupMetadata(request);

      for (int i = 0; i < sourceTableArray.length; i++) {
        restoreTableWithWalReplay(sourceTableArray[i], targetTableArray[i], endTime,
          continuousBackupTables, backupMetadataList, request);
      }
    }
  }

  /**
   * Validates whether the requested end time falls within the allowed PITR recovery window.
   * @param endTime The target recovery time.
   * @throws IOException If the requested recovery time is outside the allowed window.
   */
  private void validateRequestToTime(long endTime) throws IOException {
    long pitrWindowDays = conn.getConfiguration().getLong(CONF_CONTINUOUS_BACKUP_PITR_WINDOW_DAYS,
      DEFAULT_CONTINUOUS_BACKUP_PITR_WINDOW_DAYS);
    long currentTime = EnvironmentEdgeManager.getDelegate().currentTime();
    long pitrMaxStartTime = currentTime - TimeUnit.DAYS.toMillis(pitrWindowDays);

    if (endTime < pitrMaxStartTime) {
      String errorMsg = String.format(
        "Requested recovery time (%d) is out of the allowed PITR window (last %d days).", endTime,
        pitrWindowDays);
      LOG.error(errorMsg);
      throw new IOException(errorMsg);
    }

    if (endTime > currentTime) {
      String errorMsg = String.format(
        "Requested recovery time (%d) is in the future. Current time: %d.", endTime, currentTime);
      LOG.error(errorMsg);
      throw new IOException(errorMsg);
    }
  }

  /**
   * Resolves the target table array. If null or empty, defaults to the source table array.
   */
  private TableName[] resolveTargetTables(TableName[] sourceTables, TableName[] targetTables) {
    return (targetTables == null || targetTables.length == 0) ? sourceTables : targetTables;
  }

  /**
   * Validates whether Point-In-Time Recovery (PITR) is possible for the given tables at the
   * specified time.
   * <p>
   * PITR requires:
   * <ul>
   * <li>Continuous backup to be enabled for the source tables.</li>
   * <li>A valid backup image and corresponding WALs to be available.</li>
   * </ul>
   * @param endTime     The target recovery time.
   * @param sTableArray The source tables to restore.
   * @param tTableArray The target tables where the restore will be performed.
   * @throws IOException If PITR is not possible due to missing continuous backup or backup images.
   */
  private void validatePitr(long endTime, TableName[] sTableArray, TableName[] tTableArray)
    throws IOException {
    try (BackupSystemTable table = new BackupSystemTable(conn)) {
      // Retrieve the set of tables with continuous backup enabled
      Map<TableName, Long> continuousBackupTables = table.getContinuousBackupTableSet();

      // Ensure all source tables have continuous backup enabled
      validateContinuousBackup(sTableArray, continuousBackupTables);

      // Fetch completed backup information
      List<PitrBackupMetadata> backupMetadataList = getBackupMetadata(request);

      // Ensure a valid backup and WALs exist for PITR
      validateBackupAvailability(sTableArray, tTableArray, endTime, continuousBackupTables,
        backupMetadataList);
    }
  }

  /**
   * Ensures that all source tables have continuous backup enabled.
   */
  private void validateContinuousBackup(TableName[] tables,
    Map<TableName, Long> continuousBackupTables) throws IOException {
    List<TableName> missingTables =
      Arrays.stream(tables).filter(table -> !continuousBackupTables.containsKey(table)).toList();

    if (!missingTables.isEmpty()) {
      String errorMsg = "Continuous Backup is not enabled for the following tables: "
        + missingTables.stream().map(TableName::getNameAsString).collect(Collectors.joining(", "));
      LOG.error(errorMsg);
      throw new IOException(errorMsg);
    }
  }

  /**
   * Ensures that a valid backup and corresponding WALs exist for PITR for each source table. PITR
   * requires: 1. A valid backup available before the end time. 2. Write-Ahead Logs (WALs) covering
   * the remaining duration up to the end time.
   */
  private void validateBackupAvailability(TableName[] sTableArray, TableName[] tTableArray,
    long endTime, Map<TableName, Long> continuousBackupTables, List<PitrBackupMetadata> backups)
    throws IOException {
    for (int i = 0; i < sTableArray.length; i++) {
      if (
        !canPerformPitr(sTableArray[i], tTableArray[i], endTime, continuousBackupTables, backups)
      ) {
        String errorMsg = String.format(
          "PITR failed: No valid backup/WALs found for source table %s (target: %s) before time %d",
          sTableArray[i].getNameAsString(), tTableArray[i].getNameAsString(), endTime);
        LOG.error(errorMsg);
        throw new IOException(errorMsg);
      }
    }
  }

  /**
   * Checks whether PITR can be performed for a given source-target table pair.
   */
  private boolean canPerformPitr(TableName stableName, TableName tTableName, long endTime,
    Map<TableName, Long> continuousBackupTables, List<PitrBackupMetadata> backups) {
    return getValidBackup(stableName, tTableName, endTime, continuousBackupTables, backups) != null;
  }

  /**
   * Finds and returns the first valid backup metadata entry that can be used to restore the given
   * source table up to the specified end time. A backup is considered valid if:
   * <ul>
   * <li>It contains the source table</li>
   * <li>It was completed before the requested end time</li>
   * <li>Its start time is after the table's continuous backup start time</li>
   * <li>It passes the restore request validation</li>
   * </ul>
   */
  private PitrBackupMetadata getValidBackup(TableName sTableName, TableName tTablename,
    long endTime, Map<TableName, Long> continuousBackupTables, List<PitrBackupMetadata> backups) {
    for (PitrBackupMetadata backup : backups) {
      if (isValidBackupForPitr(backup, sTableName, endTime, continuousBackupTables)) {

        RestoreRequest restoreRequest =
          BackupUtils.createRestoreRequest(backup.getRootDir(), backup.getBackupId(), true,
            new TableName[] { sTableName }, new TableName[] { tTablename }, false);

        try {
          if (backupAdmin.validateRequest(restoreRequest)) {
            // check if any bulkload entry exists post this backup time and before "endtime"
            checkBulkLoadAfterBackup(conn, sTableName, backup, endTime);
            return backup;
          }
        } catch (IOException e) {
          LOG.warn("Exception occurred while testing the backup : {} for restore ",
            backup.getBackupId(), e);
        }
      }
    }
    return null;
  }

  /**
   * Checks if any bulk load operation occurred for the specified table post last successful backup
   * and before restore time.
   * @param conn       Active HBase connection
   * @param sTableName Table for which to check bulk load history
   * @param backup     Last successful backup before the target recovery time
   * @param endTime    Target recovery time
   * @throws IOException if a bulkload entry is found in between backup time and endtime
   */
  private void checkBulkLoadAfterBackup(Connection conn, TableName sTableName,
    PitrBackupMetadata backup, long endTime) throws IOException {
    try (BackupSystemTable backupSystemTable = new BackupSystemTable(conn)) {
      List<BulkLoad> bulkLoads = backupSystemTable.readBulkloadRows(List.of(sTableName));
      for (BulkLoad load : bulkLoads) {
        long lastBackupTs = (backup.getType() == BackupType.FULL)
          ? backup.getStartTs()
          : backup.getIncrCommittedWalTs();
        if (lastBackupTs < load.getTimestamp() && load.getTimestamp() < endTime) {
          throw new IOException("Bulk load operation detected after last successful backup for "
            + "table: " + sTableName);
        }
      }
    }
  }

  /**
   * Determines if the given backup is valid for PITR.
   * <p>
   * A backup is valid if:
   * <ul>
   * <li>It contains the source table.</li>
   * <li>It was completed before the end time.</li>
   * <li>The start timestamp of the backup is after the continuous backup start time for the
   * table.</li>
   * </ul>
   * @param backupMetadata         Backup information object.
   * @param tableName              Table to check.
   * @param endTime                The target recovery time.
   * @param continuousBackupTables Map of tables with continuous backup enabled.
   * @return true if the backup is valid for PITR, false otherwise.
   */
  private boolean isValidBackupForPitr(PitrBackupMetadata backupMetadata, TableName tableName,
    long endTime, Map<TableName, Long> continuousBackupTables) {
    return backupMetadata.getTableNames().contains(tableName)
      && backupMetadata.getCompleteTs() <= endTime
      && continuousBackupTables.getOrDefault(tableName, 0L) <= backupMetadata.getStartTs();
  }

  /**
   * Restores the table using the selected backup and replays WALs from the backup start time to the
   * requested end time.
   * @throws IOException if no valid backup is found or WAL replay fails
   */
  private void restoreTableWithWalReplay(TableName sourceTable, TableName targetTable, long endTime,
    Map<TableName, Long> continuousBackupTables, List<PitrBackupMetadata> backupMetadataList,
    PointInTimeRestoreRequest request) throws IOException {
    PitrBackupMetadata backupMetadata =
      getValidBackup(sourceTable, targetTable, endTime, continuousBackupTables, backupMetadataList);
    if (backupMetadata == null) {
      String errorMsg = "Could not find a valid backup and WALs for PITR for table: "
        + sourceTable.getNameAsString();
      LOG.error(errorMsg);
      throw new IOException(errorMsg);
    }

    RestoreRequest restoreRequest = BackupUtils.createRestoreRequest(backupMetadata.getRootDir(),
      backupMetadata.getBackupId(), false, new TableName[] { sourceTable },
      new TableName[] { targetTable }, request.isOverwrite());

    backupAdmin.restore(restoreRequest);
    replayWal(sourceTable, targetTable, backupMetadata.getStartTs(), endTime);
  }

  /**
   * Replays WALs to bring the table to the desired state.
   */
  private void replayWal(TableName sourceTable, TableName targetTable, long startTime, long endTime)
    throws IOException {
    String walBackupDir = conn.getConfiguration().get(CONF_CONTINUOUS_BACKUP_WAL_DIR);
    Path walDirPath = new Path(walBackupDir);
    LOG.info(
      "Starting WAL replay for source: {}, target: {}, time range: {} - {}, WAL backup dir: {}",
      sourceTable, targetTable, startTime, endTime, walDirPath);

    List<String> validDirs =
      getValidWalDirs(conn.getConfiguration(), walDirPath, startTime, endTime);
    if (validDirs.isEmpty()) {
      LOG.warn("No valid WAL directories found for range {} - {}. Skipping WAL replay.", startTime,
        endTime);
      return;
    }

    executeWalReplay(validDirs, sourceTable, targetTable, startTime, endTime);
  }

  /**
   * Fetches valid WAL directories based on the given time range.
   */
  private List<String> getValidWalDirs(Configuration conf, Path walBackupDir, long startTime,
    long endTime) throws IOException {
    FileSystem backupFs = FileSystem.get(walBackupDir.toUri(), conf);
    FileStatus[] dayDirs = backupFs.listStatus(new Path(walBackupDir, WALS_DIR));

    List<String> validDirs = new ArrayList<>();
    SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);

    for (FileStatus dayDir : dayDirs) {
      if (!dayDir.isDirectory()) {
        continue; // Skip files, only process directories
      }

      String dirName = dayDir.getPath().getName();
      try {
        Date dirDate = dateFormat.parse(dirName);
        long dirStartTime = dirDate.getTime(); // Start of that day (00:00:00)
        long dirEndTime = dirStartTime + ONE_DAY_IN_MILLISECONDS - 1; // End time of day (23:59:59)

        // Check if this day's WAL files overlap with the required time range
        if (dirEndTime >= startTime && dirStartTime <= endTime) {
          validDirs.add(dayDir.getPath().toString());
        }
      } catch (ParseException e) {
        LOG.warn("Skipping invalid directory name: " + dirName, e);
      }
    }
    return validDirs;
  }

  /**
   * Executes WAL replay using WALPlayer.
   */
  private void executeWalReplay(List<String> walDirs, TableName sourceTable, TableName targetTable,
    long startTime, long endTime) throws IOException {
    Tool walPlayer = initializeWalPlayer(startTime, endTime);
    String[] args =
      { String.join(",", walDirs), sourceTable.getNameAsString(), targetTable.getNameAsString() };

    try {
      LOG.info("Executing WALPlayer with args: {}", Arrays.toString(args));
      int exitCode = walPlayer.run(args);
      if (exitCode == 0) {
        LOG.info("WAL replay completed successfully for {}", targetTable);
      } else {
        throw new IOException("WAL replay failed with exit code: " + exitCode);
      }
    } catch (Exception e) {
      LOG.error("Error during WAL replay for {}: {}", targetTable, e.getMessage(), e);
      throw new IOException("Exception during WAL replay", e);
    }
  }

  /**
   * Initializes and configures WALPlayer.
   */
  private Tool initializeWalPlayer(long startTime, long endTime) {
    Configuration conf = HBaseConfiguration.create(conn.getConfiguration());
    conf.setLong(WALInputFormat.START_TIME_KEY, startTime);
    conf.setLong(WALInputFormat.END_TIME_KEY, endTime);
    conf.setBoolean(IGNORE_EMPTY_FILES, true);
    Tool walPlayer = new WALPlayer();
    walPlayer.setConf(conf);
    return walPlayer;
  }

  protected abstract List<PitrBackupMetadata> getBackupMetadata(PointInTimeRestoreRequest request)
    throws IOException;
}
