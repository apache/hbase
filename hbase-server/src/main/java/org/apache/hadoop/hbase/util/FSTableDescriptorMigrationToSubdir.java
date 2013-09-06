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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;

/**
 * A class to migrate table descriptor files to a dedicated subdir.
 * Invoked by HMaster.finishInitialization before accessing table descriptors.
 * Migrates snapshots, user tables, and system tables.
 * 
 * @deprecated will be removed for the major release after 0.96.
 */
@Deprecated
public class FSTableDescriptorMigrationToSubdir {
  
  private static final Log LOG = LogFactory.getLog(FSTableDescriptorMigrationToSubdir.class);

  public static void migrateFSTableDescriptorsIfNecessary(FileSystem fs, Path rootDir)
  throws IOException {
    if (needsMigration(fs, rootDir)) {
      migrateFsTableDescriptors(fs, rootDir);
      LOG.info("Migration complete.");
    }
  }

  /**
   * Determines if migration is required by checking to see whether the hbase:meta table has been
   * migrated.
   */
  private static boolean needsMigration(FileSystem fs, Path rootDir) throws IOException {
    Path metaTableDir = FSUtils.getTableDir(rootDir,
      TableName.META_TABLE_NAME);
    FileStatus metaTableInfoStatus =
      FSTableDescriptors.getTableInfoPath(fs, metaTableDir);
    return metaTableInfoStatus == null;
  }
  
  /**
   * Migrates all snapshots, user tables and system tables that require migration.
   * First migrates snapshots.
   * Then migrates each user table in order,
   * then attempts ROOT (should be gone)
   * Migrates hbase:meta last to indicate migration is complete.
   */
  private static void migrateFsTableDescriptors(FileSystem fs, Path rootDir) throws IOException {
    // First migrate snapshots - will migrate any snapshot dir that contains a table info file
    Path snapshotsDir = SnapshotDescriptionUtils.getSnapshotsDir(rootDir);
    if (fs.exists(snapshotsDir)) {
      LOG.info("Migrating snapshots");
      FileStatus[] snapshots = fs.listStatus(snapshotsDir,
          new SnapshotDescriptionUtils.CompletedSnaphotDirectoriesFilter(fs));
      for (FileStatus snapshot : snapshots) {
        migrateTable(fs, snapshot.getPath());
      }
    }
    
    LOG.info("Migrating user tables");
    List<Path> userTableDirs = FSUtils.getTableDirs(fs, rootDir);
    for (Path userTableDir : userTableDirs) {
      migrateTable(fs, userTableDir);
    }
    
    LOG.info("Migrating system tables");
    // migrate meta last because that's what we check to see if migration is complete
    migrateTableIfExists(fs, rootDir, TableName.META_TABLE_NAME);
  }

  private static void migrateTableIfExists(FileSystem fs, Path rootDir, TableName tableName)
  throws IOException {
    Path tableDir = FSUtils.getTableDir(rootDir, tableName);
    if (fs.exists(tableDir)) {
      migrateTable(fs, tableDir);
    }
  }

  /**
   * Migrates table info files.
   * Moves the latest table info file (is present) from the table dir to the table info subdir.
   * Removes any older table info files from the table dir and any existing table info subdir.
   */
  private static void migrateTable(FileSystem fs, Path tableDir) throws IOException {
    FileStatus oldTableStatus = FSTableDescriptors.getCurrentTableInfoStatus(fs,  tableDir, true);
    if (oldTableStatus == null) {
      LOG.debug("No table info file to migrate for " + tableDir);
      return;
    }
    
    Path tableInfoDir = new Path(tableDir, FSTableDescriptors.TABLEINFO_DIR);
    // remove table info subdir if it already exists
    boolean removedExistingSubdir = FSUtils.deleteDirectory(fs, tableInfoDir);
    if (removedExistingSubdir) {
      LOG.info("Removed existing subdir at: " + tableInfoDir);
    }
    boolean createdSubdir = fs.mkdirs(tableInfoDir);
    if (!createdSubdir) {
      throw new IOException("Unable to create new table info directory: " + tableInfoDir);
    }
    
    Path oldTableInfoPath = oldTableStatus.getPath();
    Path newTableInfoPath = new Path(tableInfoDir, oldTableInfoPath.getName());
    boolean renamedInfoFile = fs.rename(oldTableInfoPath, newTableInfoPath);
    if (!renamedInfoFile) {
      throw new IOException("Failed to move table info file from old location: "
        + oldTableInfoPath + " to new location: " + newTableInfoPath);
    }
   
    LOG.info("Migrated table info from: " + oldTableInfoPath
      + " to new location: " + newTableInfoPath);
  }

}
