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

package org.apache.hadoop.hbase.backup.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupRestoreFactory;
import org.apache.hadoop.hbase.backup.HBackupFileSystem;
import org.apache.hadoop.hbase.backup.RestoreJob;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.tool.BulkLoadHFilesTool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;

/**
 * A collection for methods used by multiple classes to restore HBase tables.
 */
@InterfaceAudience.Private
public class RestoreTool {
  public static final Logger LOG = LoggerFactory.getLogger(BackupUtils.class);
  private final static long TABLE_AVAILABILITY_WAIT_TIME = 180000;

  private final String[] ignoreDirs = { HConstants.RECOVERED_EDITS_DIR };
  protected Configuration conf;
  protected Path backupRootPath;
  protected String backupId;
  protected FileSystem fs;

  // store table name and snapshot dir mapping
  private final HashMap<TableName, Path> snapshotMap = new HashMap<>();

  public RestoreTool(Configuration conf, final Path backupRootPath, final String backupId)
      throws IOException {
    this.conf = conf;
    this.backupRootPath = backupRootPath;
    this.backupId = backupId;
    this.fs = backupRootPath.getFileSystem(conf);
  }

  /**
   * return value represent path for:
   * ".../user/biadmin/backup1/default/t1_dn/backup_1396650096738/archive/data/default/t1_dn"
   * @param tableName table name
   * @return path to table archive
   * @throws IOException exception
   */
  Path getTableArchivePath(TableName tableName) throws IOException {
    Path baseDir =
        new Path(HBackupFileSystem.getTableBackupPath(tableName, backupRootPath, backupId),
            HConstants.HFILE_ARCHIVE_DIRECTORY);
    Path dataDir = new Path(baseDir, HConstants.BASE_NAMESPACE_DIR);
    Path archivePath = new Path(dataDir, tableName.getNamespaceAsString());
    Path tableArchivePath = new Path(archivePath, tableName.getQualifierAsString());
    if (!fs.exists(tableArchivePath) || !fs.getFileStatus(tableArchivePath).isDirectory()) {
      LOG.debug("Folder tableArchivePath: " + tableArchivePath.toString() + " does not exists");
      tableArchivePath = null; // empty table has no archive
    }
    return tableArchivePath;
  }

  /**
   * Gets region list
   * @param tableName table name
   * @return RegionList region list
   * @throws IOException exception
   */
  ArrayList<Path> getRegionList(TableName tableName) throws IOException {
    Path tableArchivePath = getTableArchivePath(tableName);
    ArrayList<Path> regionDirList = new ArrayList<>();
    FileStatus[] children = fs.listStatus(tableArchivePath);
    for (FileStatus childStatus : children) {
      // here child refer to each region(Name)
      Path child = childStatus.getPath();
      regionDirList.add(child);
    }
    return regionDirList;
  }

  void modifyTableSync(Connection conn, TableDescriptor desc) throws IOException {
    try (Admin admin = conn.getAdmin()) {
      admin.modifyTable(desc);
      int attempt = 0;
      int maxAttempts = 600;
      while (!admin.isTableAvailable(desc.getTableName())) {
        Thread.sleep(100);
        attempt++;
        if (attempt++ > maxAttempts) {
          throw new IOException("Timeout expired " + (maxAttempts * 100) + "ms");
        }
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * During incremental backup operation. Call WalPlayer to replay WAL in backup image Currently
   * tableNames and newTablesNames only contain single table, will be expanded to multiple tables in
   * the future
   * @param conn HBase connection
   * @param tableBackupPath backup path
   * @param logDirs : incremental backup folders, which contains WAL
   * @param tableNames : source tableNames(table names were backuped)
   * @param newTableNames : target tableNames(table names to be restored to)
   * @param incrBackupId incremental backup Id
   * @throws IOException exception
   */
  public void incrementalRestoreTable(Connection conn, Path tableBackupPath, Path[] logDirs,
      TableName[] tableNames, TableName[] newTableNames, String incrBackupId) throws IOException {
    try (Admin admin = conn.getAdmin()) {
      if (tableNames.length != newTableNames.length) {
        throw new IOException("Number of source tables and target tables does not match!");
      }
      FileSystem fileSys = tableBackupPath.getFileSystem(this.conf);

      // for incremental backup image, expect the table already created either by user or previous
      // full backup. Here, check that all new tables exists
      for (TableName tableName : newTableNames) {
        if (!admin.tableExists(tableName)) {
          throw new IOException("HBase table " + tableName
              + " does not exist. Create the table first, e.g. by restoring a full backup.");
        }
      }
      // adjust table schema
      for (int i = 0; i < tableNames.length; i++) {
        TableName tableName = tableNames[i];
        TableDescriptor tableDescriptor = getTableDescriptor(fileSys, tableName, incrBackupId);
        LOG.debug("Found descriptor " + tableDescriptor + " through " + incrBackupId);

        TableName newTableName = newTableNames[i];
        TableDescriptor newTableDescriptor = admin.getDescriptor(newTableName);
        List<ColumnFamilyDescriptor> families = Arrays.asList(tableDescriptor.getColumnFamilies());
        List<ColumnFamilyDescriptor> existingFamilies =
            Arrays.asList(newTableDescriptor.getColumnFamilies());
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(newTableDescriptor);
        boolean schemaChangeNeeded = false;
        for (ColumnFamilyDescriptor family : families) {
          if (!existingFamilies.contains(family)) {
            builder.setColumnFamily(family);
            schemaChangeNeeded = true;
          }
        }
        for (ColumnFamilyDescriptor family : existingFamilies) {
          if (!families.contains(family)) {
            builder.removeColumnFamily(family.getName());
            schemaChangeNeeded = true;
          }
        }
        if (schemaChangeNeeded) {
          modifyTableSync(conn, builder.build());
          LOG.info("Changed " + newTableDescriptor.getTableName() + " to: " + newTableDescriptor);
        }
      }
      RestoreJob restoreService = BackupRestoreFactory.getRestoreJob(conf);

      restoreService.run(logDirs, tableNames, newTableNames, false);
    }
  }

  public void fullRestoreTable(Connection conn, Path tableBackupPath, TableName tableName,
      TableName newTableName, boolean truncateIfExists, String lastIncrBackupId)
          throws IOException {
    createAndRestoreTable(conn, tableName, newTableName, tableBackupPath, truncateIfExists,
      lastIncrBackupId);
  }

  /**
   * Returns value represent path for path to backup table snapshot directory:
   * "/$USER/SBACKUP_ROOT/backup_id/namespace/table/.hbase-snapshot"
   * @param backupRootPath backup root path
   * @param tableName table name
   * @param backupId backup Id
   * @return path for snapshot
   */
  Path getTableSnapshotPath(Path backupRootPath, TableName tableName, String backupId) {
    return new Path(HBackupFileSystem.getTableBackupPath(tableName, backupRootPath, backupId),
        HConstants.SNAPSHOT_DIR_NAME);
  }

  /**
   * Returns value represent path for:
   * ""/$USER/SBACKUP_ROOT/backup_id/namespace/table/.hbase-snapshot/
   *    snapshot_1396650097621_namespace_table"
   * this path contains .snapshotinfo, .tabledesc (0.96 and 0.98) this path contains .snapshotinfo,
   * .data.manifest (trunk)
   * @param tableName table name
   * @return path to table info
   * @throws IOException exception
   */
  Path getTableInfoPath(TableName tableName) throws IOException {
    Path tableSnapShotPath = getTableSnapshotPath(backupRootPath, tableName, backupId);
    Path tableInfoPath = null;

    // can't build the path directly as the timestamp values are different
    FileStatus[] snapshots = fs.listStatus(tableSnapShotPath,
        new SnapshotDescriptionUtils.CompletedSnaphotDirectoriesFilter(fs));
    for (FileStatus snapshot : snapshots) {
      tableInfoPath = snapshot.getPath();
      // SnapshotManifest.DATA_MANIFEST_NAME = "data.manifest";
      if (tableInfoPath.getName().endsWith("data.manifest")) {
        break;
      }
    }
    return tableInfoPath;
  }

  /**
   * Get table descriptor
   * @param tableName is the table backed up
   * @return {@link TableDescriptor} saved in backup image of the table
   */
  TableDescriptor getTableDesc(TableName tableName) throws IOException {
    Path tableInfoPath = this.getTableInfoPath(tableName);
    SnapshotDescription desc = SnapshotDescriptionUtils.readSnapshotInfo(fs, tableInfoPath);
    SnapshotManifest manifest = SnapshotManifest.open(conf, fs, tableInfoPath, desc);
    TableDescriptor tableDescriptor = manifest.getTableDescriptor();
    if (!tableDescriptor.getTableName().equals(tableName)) {
      LOG.error("couldn't find Table Desc for table: " + tableName + " under tableInfoPath: "
              + tableInfoPath.toString());
      LOG.error("tableDescriptor.getNameAsString() = "
              + tableDescriptor.getTableName().getNameAsString());
      throw new FileNotFoundException("couldn't find Table Desc for table: " + tableName
          + " under tableInfoPath: " + tableInfoPath.toString());
    }
    return tableDescriptor;
  }

  private TableDescriptor getTableDescriptor(FileSystem fileSys, TableName tableName,
      String lastIncrBackupId) throws IOException {
    if (lastIncrBackupId != null) {
      String target =
          BackupUtils.getTableBackupDir(backupRootPath.toString(),
            lastIncrBackupId, tableName);
      return FSTableDescriptors.getTableDescriptorFromFs(fileSys, new Path(target));
    }
    return null;
  }

  private void createAndRestoreTable(Connection conn, TableName tableName, TableName newTableName,
      Path tableBackupPath, boolean truncateIfExists, String lastIncrBackupId) throws IOException {
    if (newTableName == null) {
      newTableName = tableName;
    }
    FileSystem fileSys = tableBackupPath.getFileSystem(this.conf);

    // get table descriptor first
    TableDescriptor tableDescriptor = getTableDescriptor(fileSys, tableName, lastIncrBackupId);
    if (tableDescriptor != null) {
      LOG.debug("Retrieved descriptor: " + tableDescriptor + " thru " + lastIncrBackupId);
    }

    if (tableDescriptor == null) {
      Path tableSnapshotPath = getTableSnapshotPath(backupRootPath, tableName, backupId);
      if (fileSys.exists(tableSnapshotPath)) {
        // snapshot path exist means the backup path is in HDFS
        // check whether snapshot dir already recorded for target table
        if (snapshotMap.get(tableName) != null) {
          SnapshotDescription desc =
              SnapshotDescriptionUtils.readSnapshotInfo(fileSys, tableSnapshotPath);
          SnapshotManifest manifest = SnapshotManifest.open(conf, fileSys, tableSnapshotPath, desc);
          tableDescriptor = manifest.getTableDescriptor();
        } else {
          tableDescriptor = getTableDesc(tableName);
          snapshotMap.put(tableName, getTableInfoPath(tableName));
        }
        if (tableDescriptor == null) {
          LOG.debug("Found no table descriptor in the snapshot dir, previous schema would be lost");
        }
      } else {
        throw new IOException("Table snapshot directory: " +
            tableSnapshotPath + " does not exist.");
      }
    }

    Path tableArchivePath = getTableArchivePath(tableName);
    if (tableArchivePath == null) {
      if (tableDescriptor != null) {
        // find table descriptor but no archive dir means the table is empty, create table and exit
        if (LOG.isDebugEnabled()) {
          LOG.debug("find table descriptor but no archive dir for table " + tableName
              + ", will only create table");
        }
        tableDescriptor = TableDescriptorBuilder.copy(newTableName, tableDescriptor);
        checkAndCreateTable(conn, tableBackupPath, tableName, newTableName, null, tableDescriptor,
          truncateIfExists);
        return;
      } else {
        throw new IllegalStateException("Cannot restore hbase table because directory '"
            + " tableArchivePath is null.");
      }
    }

    if (tableDescriptor == null) {
      tableDescriptor = TableDescriptorBuilder.newBuilder(newTableName).build();
    } else {
      tableDescriptor = TableDescriptorBuilder.copy(newTableName, tableDescriptor);
    }

    // record all region dirs:
    // load all files in dir
    try {
      ArrayList<Path> regionPathList = getRegionList(tableName);

      // should only try to create the table with all region informations, so we could pre-split
      // the regions in fine grain
      checkAndCreateTable(conn, tableBackupPath, tableName, newTableName, regionPathList,
        tableDescriptor, truncateIfExists);
      RestoreJob restoreService = BackupRestoreFactory.getRestoreJob(conf);
      Path[] paths = new Path[regionPathList.size()];
      regionPathList.toArray(paths);
      restoreService.run(paths, new TableName[]{tableName}, new TableName[] {newTableName}, true);

    } catch (Exception e) {
      LOG.error(e.toString(), e);
      throw new IllegalStateException("Cannot restore hbase table", e);
    }
  }

  /**
   * Gets region list
   * @param tableArchivePath table archive path
   * @return RegionList region list
   * @throws IOException exception
   */
  ArrayList<Path> getRegionList(Path tableArchivePath) throws IOException {
    ArrayList<Path> regionDirList = new ArrayList<>();
    FileStatus[] children = fs.listStatus(tableArchivePath);
    for (FileStatus childStatus : children) {
      // here child refer to each region(Name)
      Path child = childStatus.getPath();
      regionDirList.add(child);
    }
    return regionDirList;
  }

  /**
   * Calculate region boundaries and add all the column families to the table descriptor
   * @param regionDirList region dir list
   * @return a set of keys to store the boundaries
   */
  byte[][] generateBoundaryKeys(ArrayList<Path> regionDirList) throws IOException {
    TreeMap<byte[], Integer> map = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    // Build a set of keys to store the boundaries
    // calculate region boundaries and add all the column families to the table descriptor
    for (Path regionDir : regionDirList) {
      LOG.debug("Parsing region dir: " + regionDir);
      Path hfofDir = regionDir;

      if (!fs.exists(hfofDir)) {
        LOG.warn("HFileOutputFormat dir " + hfofDir + " not found");
      }

      FileStatus[] familyDirStatuses = fs.listStatus(hfofDir);
      if (familyDirStatuses == null) {
        throw new IOException("No families found in " + hfofDir);
      }

      for (FileStatus stat : familyDirStatuses) {
        if (!stat.isDirectory()) {
          LOG.warn("Skipping non-directory " + stat.getPath());
          continue;
        }
        boolean isIgnore = false;
        String pathName = stat.getPath().getName();
        for (String ignore : ignoreDirs) {
          if (pathName.contains(ignore)) {
            LOG.warn("Skipping non-family directory" + pathName);
            isIgnore = true;
            break;
          }
        }
        if (isIgnore) {
          continue;
        }
        Path familyDir = stat.getPath();
        LOG.debug("Parsing family dir [" + familyDir.toString() + " in region [" + regionDir + "]");
        // Skip _logs, etc
        if (familyDir.getName().startsWith("_") || familyDir.getName().startsWith(".")) {
          continue;
        }

        // start to parse hfile inside one family dir
        Path[] hfiles = FileUtil.stat2Paths(fs.listStatus(familyDir));
        for (Path hfile : hfiles) {
          if (hfile.getName().startsWith("_") || hfile.getName().startsWith(".")
              || StoreFileInfo.isReference(hfile.getName())
              || HFileLink.isHFileLink(hfile.getName())) {
            continue;
          }
          HFile.Reader reader = HFile.createReader(fs, hfile, conf);
          final byte[] first, last;
          try {
            reader.loadFileInfo();
            first = reader.getFirstRowKey().get();
            last = reader.getLastRowKey().get();
            LOG.debug("Trying to figure out region boundaries hfile=" + hfile + " first="
                + Bytes.toStringBinary(first) + " last=" + Bytes.toStringBinary(last));

            // To eventually infer start key-end key boundaries
            Integer value = map.containsKey(first) ? (Integer) map.get(first) : 0;
            map.put(first, value + 1);
            value = map.containsKey(last) ? (Integer) map.get(last) : 0;
            map.put(last, value - 1);
          } finally {
            reader.close();
          }
        }
      }
    }
    return BulkLoadHFilesTool.inferBoundaries(map);
  }

  /**
   * Prepare the table for bulkload, most codes copied from {@code createTable} method in
   * {@code BulkLoadHFilesTool}.
   * @param conn connection
   * @param tableBackupPath path
   * @param tableName table name
   * @param targetTableName target table name
   * @param regionDirList region directory list
   * @param htd table descriptor
   * @param truncateIfExists truncates table if exists
   * @throws IOException exception
   */
  private void checkAndCreateTable(Connection conn, Path tableBackupPath, TableName tableName,
      TableName targetTableName, ArrayList<Path> regionDirList, TableDescriptor htd,
      boolean truncateIfExists) throws IOException {
    try (Admin admin = conn.getAdmin()) {
      boolean createNew = false;
      if (admin.tableExists(targetTableName)) {
        if (truncateIfExists) {
          LOG.info("Truncating exising target table '" + targetTableName
              + "', preserving region splits");
          admin.disableTable(targetTableName);
          admin.truncateTable(targetTableName, true);
        } else {
          LOG.info("Using exising target table '" + targetTableName + "'");
        }
      } else {
        createNew = true;
      }
      if (createNew) {
        LOG.info("Creating target table '" + targetTableName + "'");
        byte[][] keys;
        if (regionDirList == null || regionDirList.size() == 0) {
          admin.createTable(htd, null);
        } else {
          keys = generateBoundaryKeys(regionDirList);
          // create table using table descriptor and region boundaries
          admin.createTable(htd, keys);
        }

      }
      long startTime = EnvironmentEdgeManager.currentTime();
      while (!admin.isTableAvailable(targetTableName)) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
        if (EnvironmentEdgeManager.currentTime() - startTime > TABLE_AVAILABILITY_WAIT_TIME) {
          throw new IOException("Time out " + TABLE_AVAILABILITY_WAIT_TIME + "ms expired, table "
              + targetTableName + " is still not available");
        }
      }
    }
  }
}
