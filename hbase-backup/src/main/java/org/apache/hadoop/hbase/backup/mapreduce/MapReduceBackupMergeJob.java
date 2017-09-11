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
package org.apache.hadoop.hbase.backup.mapreduce;

import static org.apache.hadoop.hbase.backup.util.BackupUtils.succeeded;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupMergeJob;
import org.apache.hadoop.hbase.backup.BackupRestoreConstants;
import org.apache.hadoop.hbase.backup.HBackupFileSystem;
import org.apache.hadoop.hbase.backup.impl.BackupManifest;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.Tool;

/**
 * MapReduce implementation of {@link BackupMergeJob}
 * Must be initialized with configuration of a backup destination cluster
 *
 */

@InterfaceAudience.Private
public class MapReduceBackupMergeJob implements BackupMergeJob {
  public static final Log LOG = LogFactory.getLog(MapReduceBackupMergeJob.class);

  protected Tool player;
  protected Configuration conf;

  public MapReduceBackupMergeJob() {
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void run(String[] backupIds) throws IOException {
    String bulkOutputConfKey;

    // TODO : run player on remote cluster
    player = new MapReduceHFileSplitterJob();
    bulkOutputConfKey = MapReduceHFileSplitterJob.BULK_OUTPUT_CONF_KEY;
    // Player reads all files in arbitrary directory structure and creates
    // a Map task for each file
    String bids = StringUtils.join(backupIds, ",");

    if (LOG.isDebugEnabled()) {
      LOG.debug("Merge backup images " + bids);
    }

    List<Pair<TableName, Path>> processedTableList = new ArrayList<Pair<TableName, Path>>();
    boolean finishedTables = false;
    Connection conn = ConnectionFactory.createConnection(getConf());
    BackupSystemTable table = new BackupSystemTable(conn);
    FileSystem fs = FileSystem.get(getConf());

    try {

      // Get exclusive lock on backup system
      table.startBackupExclusiveOperation();
      // Start merge operation
      table.startMergeOperation(backupIds);

      // Select most recent backup id
      String mergedBackupId = findMostRecentBackupId(backupIds);

      TableName[] tableNames = getTableNamesInBackupImages(backupIds);
      String backupRoot = null;

      BackupInfo bInfo = table.readBackupInfo(backupIds[0]);
      backupRoot = bInfo.getBackupRootDir();

      for (int i = 0; i < tableNames.length; i++) {

        LOG.info("Merge backup images for " + tableNames[i]);

        // Find input directories for table

        Path[] dirPaths = findInputDirectories(fs, backupRoot, tableNames[i], backupIds);
        String dirs = StringUtils.join(dirPaths, ",");
        Path bulkOutputPath =
            BackupUtils.getBulkOutputDir(BackupUtils.getFileNameCompatibleString(tableNames[i]),
              getConf(), false);
        // Delete content if exists
        if (fs.exists(bulkOutputPath)) {
          if (!fs.delete(bulkOutputPath, true)) {
            LOG.warn("Can not delete: " + bulkOutputPath);
          }
        }
        Configuration conf = getConf();
        conf.set(bulkOutputConfKey, bulkOutputPath.toString());
        String[] playerArgs = { dirs, tableNames[i].getNameAsString() };

        int result = 0;

        player.setConf(getConf());
        result = player.run(playerArgs);
        if (!succeeded(result)) {
          throw new IOException("Can not merge backup images for " + dirs
              + " (check Hadoop/MR and HBase logs). Player return code =" + result);
        }
        // Add to processed table list
        processedTableList.add(new Pair<TableName, Path>(tableNames[i], bulkOutputPath));
        LOG.debug("Merge Job finished:" + result);
      }
      List<TableName> tableList = toTableNameList(processedTableList);
      table.updateProcessedTablesForMerge(tableList);
      finishedTables = true;

      // Move data
      for (Pair<TableName, Path> tn : processedTableList) {
        moveData(fs, backupRoot, tn.getSecond(), tn.getFirst(), mergedBackupId);
      }

      // Delete old data and update manifest
      List<String> backupsToDelete = getBackupIdsToDelete(backupIds, mergedBackupId);
      deleteBackupImages(backupsToDelete, conn, fs, backupRoot);
      updateBackupManifest(backupRoot, mergedBackupId, backupsToDelete);
      // Finish merge session
      table.finishMergeOperation();
      // Release lock
      table.finishBackupExclusiveOperation();
    } catch (RuntimeException e) {

      throw e;
    } catch (Exception e) {
      LOG.error(e);
      if (!finishedTables) {
        // cleanup bulk directories and finish merge
        // merge MUST be repeated (no need for repair)
        cleanupBulkLoadDirs(fs, toPathList(processedTableList));
        table.finishMergeOperation();
        table.finishBackupExclusiveOperation();
        throw new IOException("Backup merge operation failed, you should try it again", e);
      } else {
        // backup repair must be run
        throw new IOException(
            "Backup merge operation failed, run backup repair tool to restore system's integrity",
            e);
      }
    } finally {
      table.close();
      conn.close();
    }
  }

  protected List<Path> toPathList(List<Pair<TableName, Path>> processedTableList) {
    ArrayList<Path> list = new ArrayList<Path>();
    for (Pair<TableName, Path> p : processedTableList) {
      list.add(p.getSecond());
    }
    return list;
  }

  protected List<TableName> toTableNameList(List<Pair<TableName, Path>> processedTableList) {
    ArrayList<TableName> list = new ArrayList<TableName>();
    for (Pair<TableName, Path> p : processedTableList) {
      list.add(p.getFirst());
    }
    return list;
  }

  protected void cleanupBulkLoadDirs(FileSystem fs, List<Path> pathList) throws IOException {
    for (Path path : pathList) {

      if (!fs.delete(path, true)) {
        LOG.warn("Can't delete " + path);
      }
    }
  }

  protected void updateBackupManifest(String backupRoot, String mergedBackupId,
      List<String> backupsToDelete) throws IllegalArgumentException, IOException {

    BackupManifest manifest =
        HBackupFileSystem.getManifest(conf, new Path(backupRoot), mergedBackupId);
    manifest.getBackupImage().removeAncestors(backupsToDelete);
    // save back
    manifest.store(conf);

  }

  protected void deleteBackupImages(List<String> backupIds, Connection conn, FileSystem fs,
      String backupRoot) throws IOException {

    // Delete from backup system table
    try (BackupSystemTable table = new BackupSystemTable(conn);) {
      for (String backupId : backupIds) {
        table.deleteBackupInfo(backupId);
      }
    }

    // Delete from file system
    for (String backupId : backupIds) {
      Path backupDirPath = HBackupFileSystem.getBackupPath(backupRoot, backupId);

      if (!fs.delete(backupDirPath, true)) {
        LOG.warn("Could not delete " + backupDirPath);
      }
    }
  }

  protected List<String> getBackupIdsToDelete(String[] backupIds, String mergedBackupId) {
    List<String> list = new ArrayList<String>();
    for (String id : backupIds) {
      if (id.equals(mergedBackupId)) {
        continue;
      }
      list.add(id);
    }
    return list;
  }

  protected void moveData(FileSystem fs, String backupRoot, Path bulkOutputPath, TableName tableName,
      String mergedBackupId) throws IllegalArgumentException, IOException {

    Path dest =
        new Path(HBackupFileSystem.getTableBackupDataDir(backupRoot, mergedBackupId, tableName));

    // Delete all in dest
    if (!fs.delete(dest, true)) {
      throw new IOException("Could not delete " + dest);
    }

    FileStatus[] fsts = fs.listStatus(bulkOutputPath);
    for (FileStatus fst : fsts) {
      if (fst.isDirectory()) {
        fs.rename(fst.getPath().getParent(), dest);
      }
    }

  }

  protected String findMostRecentBackupId(String[] backupIds) {
    long recentTimestamp = Long.MIN_VALUE;
    for (String backupId : backupIds) {
      long ts = Long.parseLong(backupId.split("_")[1]);
      if (ts > recentTimestamp) {
        recentTimestamp = ts;
      }
    }
    return BackupRestoreConstants.BACKUPID_PREFIX + recentTimestamp;
  }

  protected TableName[] getTableNamesInBackupImages(String[] backupIds) throws IOException {

    Set<TableName> allSet = new HashSet<TableName>();

    try (Connection conn = ConnectionFactory.createConnection(conf);
        BackupSystemTable table = new BackupSystemTable(conn);) {
      for (String backupId : backupIds) {
        BackupInfo bInfo = table.readBackupInfo(backupId);

        allSet.addAll(bInfo.getTableNames());
      }
    }

    TableName[] ret = new TableName[allSet.size()];
    return allSet.toArray(ret);
  }

  protected Path[] findInputDirectories(FileSystem fs, String backupRoot, TableName tableName,
      String[] backupIds) throws IOException {

    List<Path> dirs = new ArrayList<Path>();

    for (String backupId : backupIds) {
      Path fileBackupDirPath =
          new Path(HBackupFileSystem.getTableBackupDataDir(backupRoot, backupId, tableName));
      if (fs.exists(fileBackupDirPath)) {
        dirs.add(fileBackupDirPath);
      } else {
        if (LOG.isTraceEnabled()) {
          LOG.trace("File: " + fileBackupDirPath + " does not exist.");
        }
      }
    }
    Path[] ret = new Path[dirs.size()];
    return dirs.toArray(ret);
  }

}
