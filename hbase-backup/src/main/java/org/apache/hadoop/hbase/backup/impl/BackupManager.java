/**
 *
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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupState;
import org.apache.hadoop.hbase.backup.BackupObserver;
import org.apache.hadoop.hbase.backup.BackupRestoreConstants;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.backup.HBackupFileSystem;
import org.apache.hadoop.hbase.backup.impl.BackupManifest.BackupImage;
import org.apache.hadoop.hbase.backup.master.BackupLogCleaner;
import org.apache.hadoop.hbase.backup.master.LogRollMasterProcedureManager;
import org.apache.hadoop.hbase.backup.regionserver.LogRollRegionServerProcedureManager;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.procedure.ProcedureManagerHost;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles backup requests, creates backup info records in backup system table to keep track of
 * backup sessions, dispatches backup request.
 */
@InterfaceAudience.Private
public class BackupManager implements Closeable {
  // in seconds
  public final static String BACKUP_EXCLUSIVE_OPERATION_TIMEOUT_SECONDS_KEY =
      "hbase.backup.exclusive.op.timeout.seconds";
  // In seconds
  private final static int DEFAULT_BACKUP_EXCLUSIVE_OPERATION_TIMEOUT = 3600;
  private static final Logger LOG = LoggerFactory.getLogger(BackupManager.class);

  protected Configuration conf = null;
  protected BackupInfo backupInfo = null;
  protected BackupSystemTable systemTable;
  protected final Connection conn;

  /**
   * Backup manager constructor.
   * @param conn connection
   * @param conf configuration
   * @throws IOException exception
   */
  public BackupManager(Connection conn, Configuration conf) throws IOException {
    if (!conf.getBoolean(BackupRestoreConstants.BACKUP_ENABLE_KEY,
      BackupRestoreConstants.BACKUP_ENABLE_DEFAULT)) {
      throw new BackupException("HBase backup is not enabled. Check your "
          + BackupRestoreConstants.BACKUP_ENABLE_KEY + " setting.");
    }
    this.conf = conf;
    this.conn = conn;
    this.systemTable = new BackupSystemTable(conn);
  }

  /**
   * Returns backup info
   */
  protected BackupInfo getBackupInfo() {
    return backupInfo;
  }

  /**
   * This method modifies the master's configuration in order to inject backup-related features
   * (TESTs only)
   * @param conf configuration
   */
  @VisibleForTesting
  public static void decorateMasterConfiguration(Configuration conf) {
    if (!isBackupEnabled(conf)) {
      return;
    }
    // Add WAL archive cleaner plug-in
    String plugins = conf.get(HConstants.HBASE_MASTER_LOGCLEANER_PLUGINS);
    String cleanerClass = BackupLogCleaner.class.getCanonicalName();
    if (!plugins.contains(cleanerClass)) {
      conf.set(HConstants.HBASE_MASTER_LOGCLEANER_PLUGINS, plugins + "," + cleanerClass);
    }

    String classes = conf.get(ProcedureManagerHost.MASTER_PROCEDURE_CONF_KEY);
    String masterProcedureClass = LogRollMasterProcedureManager.class.getName();
    if (classes == null) {
      conf.set(ProcedureManagerHost.MASTER_PROCEDURE_CONF_KEY, masterProcedureClass);
    } else if (!classes.contains(masterProcedureClass)) {
      conf.set(ProcedureManagerHost.MASTER_PROCEDURE_CONF_KEY,
        classes + "," + masterProcedureClass);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Added log cleaner: " + cleanerClass + "\n" + "Added master procedure manager: "
          + masterProcedureClass);
    }
  }

  /**
   * This method modifies the Region Server configuration in order to inject backup-related features
   * TESTs only.
   * @param conf configuration
   */
  @VisibleForTesting
  public static void decorateRegionServerConfiguration(Configuration conf) {
    if (!isBackupEnabled(conf)) {
      return;
    }

    String classes = conf.get(ProcedureManagerHost.REGIONSERVER_PROCEDURE_CONF_KEY);
    String regionProcedureClass = LogRollRegionServerProcedureManager.class.getName();
    if (classes == null) {
      conf.set(ProcedureManagerHost.REGIONSERVER_PROCEDURE_CONF_KEY, regionProcedureClass);
    } else if (!classes.contains(regionProcedureClass)) {
      conf.set(ProcedureManagerHost.REGIONSERVER_PROCEDURE_CONF_KEY,
        classes + "," + regionProcedureClass);
    }
    String coproc = conf.get(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY);
    String regionObserverClass = BackupObserver.class.getName();
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      (coproc == null ? "" : coproc + ",") + regionObserverClass);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Added region procedure manager: " + regionProcedureClass
        + ". Added region observer: " + regionObserverClass);
    }
  }

  public static boolean isBackupEnabled(Configuration conf) {
    return conf.getBoolean(BackupRestoreConstants.BACKUP_ENABLE_KEY,
      BackupRestoreConstants.BACKUP_ENABLE_DEFAULT);
  }

  /**
   * Get configuration
   * @return configuration
   */
  Configuration getConf() {
    return conf;
  }

  /**
   * Stop all the work of backup.
   */
  @Override
  public void close() {
    if (systemTable != null) {
      try {
        systemTable.close();
      } catch (Exception e) {
        LOG.error(e.toString(), e);
      }
    }
  }

  /**
   * Creates a backup info based on input backup request.
   * @param backupId backup id
   * @param type type
   * @param tableList table list
   * @param targetRootDir root dir
   * @param workers number of parallel workers
   * @param bandwidth bandwidth per worker in MB per sec
   * @return BackupInfo
   * @throws BackupException exception
   */
  public BackupInfo createBackupInfo(String backupId, BackupType type, List<TableName> tableList,
      String targetRootDir, int workers, long bandwidth) throws BackupException {
    if (targetRootDir == null) {
      throw new BackupException("Wrong backup request parameter: target backup root directory");
    }

    if (type == BackupType.FULL && (tableList == null || tableList.isEmpty())) {
      // If table list is null for full backup, which means backup all tables. Then fill the table
      // list with all user tables from meta. It no table available, throw the request exception.
      HTableDescriptor[] htds = null;
      try (Admin admin = conn.getAdmin()) {
        htds = admin.listTables();
      } catch (Exception e) {
        throw new BackupException(e);
      }

      if (htds == null) {
        throw new BackupException("No table exists for full backup of all tables.");
      } else {
        tableList = new ArrayList<>();
        for (HTableDescriptor hTableDescriptor : htds) {
          TableName tn = hTableDescriptor.getTableName();
          if (tn.equals(BackupSystemTable.getTableName(conf))) {
            // skip backup system table
            continue;
          }
          tableList.add(hTableDescriptor.getTableName());
        }

        LOG.info("Full backup all the tables available in the cluster: " + tableList);
      }
    }

    // there are one or more tables in the table list
    backupInfo = new BackupInfo(backupId, type, tableList.toArray(new TableName[tableList.size()]),
      targetRootDir);
    backupInfo.setBandwidth(bandwidth);
    backupInfo.setWorkers(workers);
    return backupInfo;
  }

  /**
   * Check if any ongoing backup. Currently, we only reply on checking status in backup system
   * table. We need to consider to handle the case of orphan records in the future. Otherwise, all
   * the coming request will fail.
   * @return the ongoing backup id if on going backup exists, otherwise null
   * @throws IOException exception
   */
  private String getOngoingBackupId() throws IOException {
    ArrayList<BackupInfo> sessions = systemTable.getBackupInfos(BackupState.RUNNING);
    if (sessions.size() == 0) {
      return null;
    }
    return sessions.get(0).getBackupId();
  }

  /**
   * Start the backup manager service.
   * @throws IOException exception
   */
  public void initialize() throws IOException {
    String ongoingBackupId = this.getOngoingBackupId();
    if (ongoingBackupId != null) {
      LOG.info("There is a ongoing backup " + ongoingBackupId
        + ". Can not launch new backup until no ongoing backup remains.");
      throw new BackupException("There is ongoing backup.");
    }
  }

  public void setBackupInfo(BackupInfo backupInfo) {
    this.backupInfo = backupInfo;
  }

  /**
   * Get direct ancestors of the current backup.
   * @param backupInfo The backup info for the current backup
   * @return The ancestors for the current backup
   * @throws IOException exception
   */
  public ArrayList<BackupImage> getAncestors(BackupInfo backupInfo) throws IOException {
    LOG.debug("Getting the direct ancestors of the current backup " + backupInfo.getBackupId());

    ArrayList<BackupImage> ancestors = new ArrayList<>();

    // full backup does not have ancestor
    if (backupInfo.getType() == BackupType.FULL) {
      LOG.debug("Current backup is a full backup, no direct ancestor for it.");
      return ancestors;
    }

    // get all backup history list in descending order
    ArrayList<BackupInfo> allHistoryList = getBackupHistory(true);
    for (BackupInfo backup : allHistoryList) {

      BackupImage.Builder builder = BackupImage.newBuilder();

      BackupImage image = builder.withBackupId(backup.getBackupId()).withType(backup.getType())
          .withRootDir(backup.getBackupRootDir()).withTableList(backup.getTableNames())
          .withStartTime(backup.getStartTs()).withCompleteTime(backup.getCompleteTs()).build();

      // add the full backup image as an ancestor until the last incremental backup
      if (backup.getType().equals(BackupType.FULL)) {
        // check the backup image coverage, if previous image could be covered by the newer ones,
        // then no need to add
        if (!BackupManifest.canCoverImage(ancestors, image)) {
          ancestors.add(image);
        }
      } else {
        // found last incremental backup, if previously added full backup ancestor images can cover
        // it, then this incremental ancestor is not the dependent of the current incremental
        // backup, that is to say, this is the backup scope boundary of current table set.
        // Otherwise, this incremental backup ancestor is the dependent ancestor of the ongoing
        // incremental backup
        if (BackupManifest.canCoverImage(ancestors, image)) {
          LOG.debug("Met the backup boundary of the current table set:");
          for (BackupImage image1 : ancestors) {
            LOG.debug("  BackupID=" + image1.getBackupId() + ", BackupDir=" + image1.getRootDir());
          }
        } else {
          Path logBackupPath =
              HBackupFileSystem.getBackupPath(backup.getBackupRootDir(), backup.getBackupId());
          LOG.debug("Current backup has an incremental backup ancestor, "
              + "touching its image manifest in " + logBackupPath.toString()
              + " to construct the dependency.");
          BackupManifest lastIncrImgManifest = new BackupManifest(conf, logBackupPath);
          BackupImage lastIncrImage = lastIncrImgManifest.getBackupImage();
          ancestors.add(lastIncrImage);

          LOG.debug(
            "Last dependent incremental backup image: " + "{BackupID=" + lastIncrImage.getBackupId()
            + "," + "BackupDir=" + lastIncrImage.getRootDir() + "}");
        }
      }
    }
    LOG.debug("Got " + ancestors.size() + " ancestors for the current backup.");
    return ancestors;
  }

  /**
   * Get the direct ancestors of this backup for one table involved.
   * @param backupInfo backup info
   * @param table table
   * @return backupImages on the dependency list
   * @throws IOException exception
   */
  public ArrayList<BackupImage> getAncestors(BackupInfo backupInfo, TableName table)
      throws IOException {
    ArrayList<BackupImage> ancestors = getAncestors(backupInfo);
    ArrayList<BackupImage> tableAncestors = new ArrayList<>();
    for (BackupImage image : ancestors) {
      if (image.hasTable(table)) {
        tableAncestors.add(image);
        if (image.getType() == BackupType.FULL) {
          break;
        }
      }
    }
    return tableAncestors;
  }

  /*
   * backup system table operations
   */

  /**
   * Updates status (state) of a backup session in a persistent store
   * @param context context
   * @throws IOException exception
   */
  public void updateBackupInfo(BackupInfo context) throws IOException {
    systemTable.updateBackupInfo(context);
  }

  /**
   * Starts new backup session
   * @throws IOException if active session already exists
   */
  public void startBackupSession() throws IOException {
    long startTime = System.currentTimeMillis();
    long timeout = conf.getInt(BACKUP_EXCLUSIVE_OPERATION_TIMEOUT_SECONDS_KEY,
      DEFAULT_BACKUP_EXCLUSIVE_OPERATION_TIMEOUT) * 1000L;
    long lastWarningOutputTime = 0;
    while (System.currentTimeMillis() - startTime < timeout) {
      try {
        systemTable.startBackupExclusiveOperation();
        return;
      } catch (IOException e) {
        if (e instanceof ExclusiveOperationException) {
          // sleep, then repeat
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e1) {
            // Restore the interrupted status
            Thread.currentThread().interrupt();
          }
          if (lastWarningOutputTime == 0
              || (System.currentTimeMillis() - lastWarningOutputTime) > 60000) {
            lastWarningOutputTime = System.currentTimeMillis();
            LOG.warn("Waiting to acquire backup exclusive lock for "
                + (lastWarningOutputTime - startTime) / 1000 + "s");
          }
        } else {
          throw e;
        }
      }
    }
    throw new IOException(
      "Failed to acquire backup system table exclusive lock after " + timeout / 1000 + "s");
  }

  /**
   * Finishes active backup session
   * @throws IOException if no active session
   */
  public void finishBackupSession() throws IOException {
    systemTable.finishBackupExclusiveOperation();
  }

  /**
   * Read the last backup start code (timestamp) of last successful backup. Will return null if
   * there is no startcode stored in backup system table or the value is of length 0. These two
   * cases indicate there is no successful backup completed so far.
   * @return the timestamp of a last successful backup
   * @throws IOException exception
   */
  public String readBackupStartCode() throws IOException {
    return systemTable.readBackupStartCode(backupInfo.getBackupRootDir());
  }

  /**
   * Write the start code (timestamp) to backup system table. If passed in null, then write 0 byte.
   * @param startCode start code
   * @throws IOException exception
   */
  public void writeBackupStartCode(Long startCode) throws IOException {
    systemTable.writeBackupStartCode(startCode, backupInfo.getBackupRootDir());
  }

  /**
   * Get the RS log information after the last log roll from backup system table.
   * @return RS log info
   * @throws IOException exception
   */
  public HashMap<String, Long> readRegionServerLastLogRollResult() throws IOException {
    return systemTable.readRegionServerLastLogRollResult(backupInfo.getBackupRootDir());
  }

  public Pair<Map<TableName, Map<String, Map<String, List<Pair<String, Boolean>>>>>, List<byte[]>>
    readBulkloadRows(List<TableName> tableList) throws IOException {
    return systemTable.readBulkloadRows(tableList);
  }

  public void deleteBulkLoadedRows(List<byte[]> rows) throws IOException {
    systemTable.deleteBulkLoadedRows(rows);
  }

  /**
   * Get all completed backup information (in desc order by time)
   * @return history info of BackupCompleteData
   * @throws IOException exception
   */
  public List<BackupInfo> getBackupHistory() throws IOException {
    return systemTable.getBackupHistory();
  }

  public ArrayList<BackupInfo> getBackupHistory(boolean completed) throws IOException {
    return systemTable.getBackupHistory(completed);
  }

  /**
   * Write the current timestamps for each regionserver to backup system table after a successful
   * full or incremental backup. Each table may have a different set of log timestamps. The saved
   * timestamp is of the last log file that was backed up already.
   * @param tables tables
   * @throws IOException exception
   */
  public void writeRegionServerLogTimestamp(Set<TableName> tables,
      HashMap<String, Long> newTimestamps) throws IOException {
    systemTable.writeRegionServerLogTimestamp(tables, newTimestamps, backupInfo.getBackupRootDir());
  }

  /**
   * Read the timestamp for each region server log after the last successful backup. Each table has
   * its own set of the timestamps.
   * @return the timestamp for each region server. key: tableName value:
   *         RegionServer,PreviousTimeStamp
   * @throws IOException exception
   */
  public HashMap<TableName, HashMap<String, Long>> readLogTimestampMap() throws IOException {
    return systemTable.readLogTimestampMap(backupInfo.getBackupRootDir());
  }

  /**
   * Return the current tables covered by incremental backup.
   * @return set of tableNames
   * @throws IOException exception
   */
  public Set<TableName> getIncrementalBackupTableSet() throws IOException {
    return systemTable.getIncrementalBackupTableSet(backupInfo.getBackupRootDir());
  }

  /**
   * Adds set of tables to overall incremental backup table set
   * @param tables tables
   * @throws IOException exception
   */
  public void addIncrementalBackupTableSet(Set<TableName> tables) throws IOException {
    systemTable.addIncrementalBackupTableSet(tables, backupInfo.getBackupRootDir());
  }

  /**
   * Saves list of WAL files after incremental backup operation. These files will be stored until
   * TTL expiration and are used by Backup Log Cleaner plug-in to determine which WAL files can be
   * safely purged.
   */
  public void recordWALFiles(List<String> files) throws IOException {
    systemTable.addWALFiles(files, backupInfo.getBackupId(), backupInfo.getBackupRootDir());
  }

  /**
   * Get WAL files iterator.
   * @return WAL files iterator from backup system table
   * @throws IOException if getting the WAL files iterator fails
   */
  public Iterator<BackupSystemTable.WALItem> getWALFilesFromBackupSystem() throws IOException {
    return systemTable.getWALFilesIterator(backupInfo.getBackupRootDir());
  }

  public Connection getConnection() {
    return conn;
  }
}
