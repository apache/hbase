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
package org.apache.hadoop.hbase.backup.mapreduce;

import static org.apache.hadoop.hbase.backup.util.BackupUtils.succeeded;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupMergeJob;
import org.apache.hadoop.hbase.backup.HBackupFileSystem;
import org.apache.hadoop.hbase.backup.impl.BackupManifest;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.Tool;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MapReduce implementation of {@link BackupMergeJob} Must be initialized with configuration of a
 * backup destination cluster
 */
@InterfaceAudience.Private
public class MapReduceBackupMergeJob implements BackupMergeJob {
  public static final Logger LOG = LoggerFactory.getLogger(MapReduceBackupMergeJob.class);

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

    List<Pair<TableName, Path>> processedTableList = new ArrayList<>();
    boolean finishedTables = false;
    Connection conn = ConnectionFactory.createConnection(getConf());
    BackupSystemTable table = new BackupSystemTable(conn);
    FileSystem fs = null;

    try {
      List<BackupInfo> backupInfos = new ArrayList<>();
      for (String backupId : backupIds) {
        BackupInfo bInfo = table.readBackupInfo(backupId);
        if (bInfo != null) {
          backupInfos.add(bInfo);
        }
      }

      // Get exclusive lock on backup system
      table.startBackupExclusiveOperation(backupInfos);
      // Start merge operation
      table.startMergeOperation(backupIds);

      // Select most recent backup id
      String mergedBackupId = BackupUtils.findMostRecentBackupId(backupIds);

      TableName[] tableNames = getTableNamesInBackupImages(backupIds);

      BackupInfo bInfo = table.readBackupInfo(backupIds[0]);
      String backupRoot = bInfo.getBackupRootDir();
      Path backupRootPath = new Path(backupRoot);
      fs = backupRootPath.getFileSystem(conf);

      for (int i = 0; i < tableNames.length; i++) {
        LOG.info("Merge backup images for " + tableNames[i]);

        // Find input directories for table
        Path[] dirPaths = findInputDirectories(fs, backupRoot, tableNames[i], backupIds);
        String dirs = StringUtils.join(dirPaths, ",");

        // bulkOutputPath should be on the same filesystem as backupRoot
        Path tmpRestoreOutputDir = HBackupFileSystem.getBackupTmpDirPath(backupRoot);
        Path bulkOutputPath = BackupUtils.getBulkOutputDir(tmpRestoreOutputDir,
          BackupUtils.getFileNameCompatibleString(tableNames[i]), getConf(), false);
        // Delete content if exists
        if (fs.exists(bulkOutputPath)) {
          if (!fs.delete(bulkOutputPath, true)) {
            LOG.warn("Can not delete: " + bulkOutputPath);
          }
        }
        Configuration conf = getConf();
        conf.set(bulkOutputConfKey, bulkOutputPath.toString());
        String[] playerArgs = { dirs, tableNames[i].getNameAsString() };

        player.setConf(getConf());
        int result = player.run(playerArgs);
        if (!succeeded(result)) {
          throw new IOException("Can not merge backup images for " + dirs
            + " (check Hadoop/MR and HBase logs). Player return code =" + result);
        }
        // Add to processed table list
        processedTableList.add(new Pair<>(tableNames[i], bulkOutputPath));
        LOG.debug("Merge Job finished:" + result);
      }
      List<TableName> tableList = toTableNameList(processedTableList);
      table.updateProcessedTablesForMerge(tableList);
      finishedTables = true;

      // PHASE 2 (modification of a backup file system)
      // Move existing mergedBackupId data into tmp directory
      // we will need it later in case of a failure
      Path tmpBackupDir =
        HBackupFileSystem.getBackupTmpDirPathForBackupId(backupRoot, mergedBackupId);
      Path backupDirPath = HBackupFileSystem.getBackupPath(backupRoot, mergedBackupId);

      if (!fs.rename(backupDirPath, tmpBackupDir)) {
        throw new IOException("Failed to rename " + backupDirPath + " to " + tmpBackupDir);
      } else {
        LOG.debug("Renamed " + backupDirPath + " to " + tmpBackupDir);
      }
      // Move new data into backup dest
      for (Pair<TableName, Path> tn : processedTableList) {
        moveData(fs, backupRoot, tn.getSecond(), tn.getFirst(), mergedBackupId);
      }
      // Update backup manifest
      List<String> backupsToDelete = getBackupIdsToDelete(backupIds, mergedBackupId);
      updateBackupManifest(tmpBackupDir.getParent().toString(), mergedBackupId, backupsToDelete);
      // Copy meta files back from tmp to backup dir
      copyMetaData(fs, tmpBackupDir, backupDirPath);
      // Delete tmp dir (Rename back during repair)
      if (!fs.delete(tmpBackupDir, true)) {
        // WARN and ignore
        LOG.warn("Could not delete tmp dir: " + tmpBackupDir);
      }
      // Delete old data
      deleteBackupImages(backupsToDelete, conn, fs, backupRoot);
      // Finish merge session
      table.finishMergeOperation();
      // Release lock
      table.finishBackupExclusiveOperation(Arrays.asList(backupIds));
    } catch (RuntimeException e) {

      throw e;
    } catch (Exception e) {
      LOG.error(e.toString(), e);
      if (!finishedTables) {
        // cleanup bulk directories and finish merge
        // merge MUST be repeated (no need for repair)
        if (fs != null) {
          cleanupBulkLoadDirs(fs, toPathList(processedTableList));
        }
        table.finishMergeOperation();
        table.finishBackupExclusiveOperation(Arrays.asList(backupIds));
        throw new IOException("Backup merge operation failed, you should try it again", e);
      } else {
        // backup repair must be run
        throw new IOException(
          "Backup merge operation failed, run backup repair tool to restore system's integrity", e);
      }
    } finally {
      table.close();
      conn.close();
    }
  }

  /**
   * Copy meta data to of a backup session
   * @param fs            file system
   * @param tmpBackupDir  temp backup directory, where meta is locaed
   * @param backupDirPath new path for backup
   * @throws IOException exception
   */
  protected void copyMetaData(FileSystem fs, Path tmpBackupDir, Path backupDirPath)
    throws IOException {
    RemoteIterator<LocatedFileStatus> it = fs.listFiles(tmpBackupDir, true);
    List<Path> toKeep = new ArrayList<Path>();
    while (it.hasNext()) {
      Path p = it.next().getPath();
      if (fs.isDirectory(p)) {
        continue;
      }
      // Keep meta
      String fileName = p.toString();
      if (
        fileName.indexOf(FSTableDescriptors.TABLEINFO_DIR) > 0
          || fileName.indexOf(HRegionFileSystem.REGION_INFO_FILE) > 0
          || fileName.indexOf(BackupManifest.MANIFEST_FILE_NAME) > 0
      ) {
        toKeep.add(p);
      }
    }
    // Copy meta to destination
    for (Path p : toKeep) {
      Path newPath = convertToDest(p, backupDirPath);
      LOG.info("Copying tmp metadata from {} to {}", p, newPath);
      copyFile(fs, p, newPath);
    }
  }

  /**
   * Copy file in DFS from p to newPath
   * @param fs      file system
   * @param p       old path
   * @param newPath new path
   * @throws IOException exception
   */
  protected void copyFile(FileSystem fs, Path p, Path newPath) throws IOException {
    try (InputStream in = fs.open(p); OutputStream out = fs.create(newPath, true)) {
      IOUtils.copy(in, out);
    }
    boolean exists = fs.exists(newPath);
    if (!exists) {
      throw new IOException("Failed to copy meta file to: " + newPath);
    }
  }

  /**
   * Converts path before copying
   * @param p             path
   * @param backupDirPath backup root
   * @return converted path
   */
  protected Path convertToDest(Path p, Path backupDirPath) {
    String backupId = backupDirPath.getName();
    Deque<String> stack = new ArrayDeque<String>();
    String name = null;
    while (true) {
      name = p.getName();
      if (!name.equals(backupId)) {
        stack.push(name);
        p = p.getParent();
      } else {
        break;
      }
    }
    Path newPath = new Path(backupDirPath.toString());
    while (!stack.isEmpty()) {
      newPath = new Path(newPath, stack.pop());
    }
    return newPath;
  }

  protected List<Path> toPathList(List<Pair<TableName, Path>> processedTableList) {
    ArrayList<Path> list = new ArrayList<>();
    for (Pair<TableName, Path> p : processedTableList) {
      list.add(p.getSecond());
    }
    return list;
  }

  protected List<TableName> toTableNameList(List<Pair<TableName, Path>> processedTableList) {
    ArrayList<TableName> list = new ArrayList<>();
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
    LOG.info("Removing ancestors from merged backup {} : {}", mergedBackupId, backupsToDelete);
    manifest.getBackupImage().removeAncestors(backupsToDelete);
    // save back
    LOG.info("Creating new manifest file for merged backup {} at root {}", mergedBackupId,
      backupRoot);
    manifest.store(conf);
  }

  protected void deleteBackupImages(List<String> backupIds, Connection conn, FileSystem fs,
    String backupRoot) throws IOException {
    // Delete from backup system table
    try (BackupSystemTable table = new BackupSystemTable(conn)) {
      for (String backupId : backupIds) {
        LOG.info("Removing metadata for backup {}", backupId);
        table.deleteBackupInfo(backupId);
      }
    }

    // Delete from file system
    for (String backupId : backupIds) {
      LOG.info("Purging backup {} from FileSystem", backupId);
      Path backupDirPath = HBackupFileSystem.getBackupPath(backupRoot, backupId);

      if (!fs.delete(backupDirPath, true)) {
        LOG.warn("Could not delete " + backupDirPath);
      }
    }
  }

  protected List<String> getBackupIdsToDelete(String[] backupIds, String mergedBackupId) {
    List<String> list = new ArrayList<>();
    for (String id : backupIds) {
      if (id.equals(mergedBackupId)) {
        continue;
      }
      list.add(id);
    }
    return list;
  }

  protected void moveData(FileSystem fs, String backupRoot, Path bulkOutputPath,
    TableName tableName, String mergedBackupId) throws IllegalArgumentException, IOException {
    Path dest =
      new Path(HBackupFileSystem.getTableBackupDir(backupRoot, mergedBackupId, tableName));

    FileStatus[] fsts = fs.listStatus(bulkOutputPath);
    for (FileStatus fst : fsts) {
      if (fst.isDirectory()) {
        String family = fst.getPath().getName();
        Path newDst = new Path(dest, family);
        if (fs.exists(newDst)) {
          if (!fs.delete(newDst, true)) {
            throw new IOException("failed to delete :" + newDst);
          }
        } else {
          fs.mkdirs(dest);
        }
        boolean result = fs.rename(fst.getPath(), dest);
        LOG.debug("MoveData from " + fst.getPath() + " to " + dest + " result=" + result);
      }
    }
  }

  protected TableName[] getTableNamesInBackupImages(String[] backupIds) throws IOException {
    Set<TableName> allSet = new HashSet<>();

    try (Connection conn = ConnectionFactory.createConnection(conf);
      BackupSystemTable table = new BackupSystemTable(conn)) {
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
    List<Path> dirs = new ArrayList<>();

    for (String backupId : backupIds) {
      Path fileBackupDirPath =
        new Path(HBackupFileSystem.getTableBackupDir(backupRoot, backupId, tableName));
      if (fs.exists(fileBackupDirPath)) {
        dirs.add(fileBackupDirPath);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("File: " + fileBackupDirPath + " does not exist.");
        }
      }
    }
    Path[] ret = new Path[dirs.size()];
    return dirs.toArray(ret);
  }
}
