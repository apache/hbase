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
package org.apache.hadoop.hbase.backup;

import static org.apache.hadoop.hbase.backup.util.BackupUtils.succeeded;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupAdminImpl;
import org.apache.hadoop.hbase.backup.impl.BackupCommands;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.backup.mapreduce.MapReduceBackupMergeJob;
import org.apache.hadoop.hbase.backup.mapreduce.MapReduceHFileSplitterJob;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@Category(LargeTests.class)
public class TestIncrementalBackupMergeWithFailures extends TestBackupBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestIncrementalBackupMergeWithFailures.class);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestIncrementalBackupMergeWithFailures.class);

  enum FailurePhase {
    PHASE1, PHASE2, PHASE3, PHASE4
  }

  public final static String FAILURE_PHASE_KEY = "failurePhase";

  static class BackupMergeJobWithFailures extends MapReduceBackupMergeJob {
    FailurePhase failurePhase;

    @Override
    public void setConf(Configuration conf) {
      super.setConf(conf);
      String val = conf.get(FAILURE_PHASE_KEY);
      if (val != null) {
        failurePhase = FailurePhase.valueOf(val);
      } else {
        Assert.fail("Failure phase is not set");
      }
    }

    /**
     * This is the exact copy of parent's run() with injections
     * of different types of failures
     */
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
      FileSystem fs = FileSystem.get(getConf());

      try {
        // Start backup exclusive operation
        table.startBackupExclusiveOperation();
        // Start merge operation
        table.startMergeOperation(backupIds);

        // Select most recent backup id
        String mergedBackupId = BackupUtils.findMostRecentBackupId(backupIds);

        TableName[] tableNames = getTableNamesInBackupImages(backupIds);

        BackupInfo bInfo = table.readBackupInfo(backupIds[0]);
        String backupRoot = bInfo.getBackupRootDir();
        // PHASE 1
        checkFailure(FailurePhase.PHASE1);

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

          // PHASE 2
          checkFailure(FailurePhase.PHASE2);
          player.setConf(getConf());
          int result = player.run(playerArgs);
          if (succeeded(result)) {
            // Add to processed table list
            processedTableList.add(new Pair<>(tableNames[i], bulkOutputPath));
          } else {
            throw new IOException("Can not merge backup images for " + dirs
                + " (check Hadoop/MR and HBase logs). Player return code =" + result);
          }
          LOG.debug("Merge Job finished:" + result);
        }
        List<TableName> tableList = toTableNameList(processedTableList);
        // PHASE 3
        checkFailure(FailurePhase.PHASE3);
        table.updateProcessedTablesForMerge(tableList);
        finishedTables = true;

        // (modification of a backup file system)
        // Move existing mergedBackupId data into tmp directory
        // we will need it later in case of a failure
        Path tmpBackupDir =  HBackupFileSystem.getBackupTmpDirPathForBackupId(backupRoot,
          mergedBackupId);
        Path backupDirPath = HBackupFileSystem.getBackupPath(backupRoot, mergedBackupId);
        if (!fs.rename(backupDirPath, tmpBackupDir)) {
          throw new IOException("Failed to rename "+ backupDirPath +" to "+tmpBackupDir);
        } else {
          LOG.debug("Renamed "+ backupDirPath +" to "+ tmpBackupDir);
        }
        // Move new data into backup dest
        for (Pair<TableName, Path> tn : processedTableList) {
          moveData(fs, backupRoot, tn.getSecond(), tn.getFirst(), mergedBackupId);
        }
        checkFailure(FailurePhase.PHASE4);
        // Update backup manifest
        List<String> backupsToDelete = getBackupIdsToDelete(backupIds, mergedBackupId);
        updateBackupManifest(tmpBackupDir.getParent().toString(), mergedBackupId, backupsToDelete);
        // Copy meta files back from tmp to backup dir
        copyMetaData(fs, tmpBackupDir, backupDirPath);
        // Delete tmp dir (Rename back during repair)
        if (!fs.delete(tmpBackupDir, true)) {
          // WARN and ignore
          LOG.warn("Could not delete tmp dir: "+ tmpBackupDir);
        }
        // Delete old data
        deleteBackupImages(backupsToDelete, conn, fs, backupRoot);
        // Finish merge session
        table.finishMergeOperation();
        // Release lock
        table.finishBackupExclusiveOperation();
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        LOG.error(e.toString(), e);
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

    private void checkFailure(FailurePhase phase) throws IOException {
      if (failurePhase != null && failurePhase == phase) {
        throw new IOException(phase.toString());
      }
    }
  }

  @Test
  public void TestIncBackupMergeRestore() throws Exception {
    int ADD_ROWS = 99;
    // #1 - create full backup for all tables
    LOG.info("create full backup image for all tables");

    List<TableName> tables = Lists.newArrayList(table1, table2);
    // Set custom Merge Job implementation
    conf1.setClass(BackupRestoreFactory.HBASE_BACKUP_MERGE_IMPL_CLASS,
      BackupMergeJobWithFailures.class, BackupMergeJob.class);

    Connection conn = ConnectionFactory.createConnection(conf1);

    HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
    BackupAdminImpl client = new BackupAdminImpl(conn);

    BackupRequest request = createBackupRequest(BackupType.FULL, tables, BACKUP_ROOT_DIR);
    String backupIdFull = client.backupTables(request);

    assertTrue(checkSucceeded(backupIdFull));

    // #2 - insert some data to table1
    Table t1 = insertIntoTable(conn, table1, famName, 1, ADD_ROWS);
    LOG.debug("writing " + ADD_ROWS + " rows to " + table1);

    Assert.assertEquals(TEST_UTIL.countRows(t1), NB_ROWS_IN_BATCH + ADD_ROWS);
    t1.close();
    LOG.debug("written " + ADD_ROWS + " rows to " + table1);

    Table t2 = insertIntoTable(conn, table2, famName, 1, ADD_ROWS);

    Assert.assertEquals(TEST_UTIL.countRows(t2), NB_ROWS_IN_BATCH + ADD_ROWS);
    t2.close();
    LOG.debug("written " + ADD_ROWS + " rows to " + table2);

    // #3 - incremental backup for multiple tables
    tables = Lists.newArrayList(table1, table2);
    request = createBackupRequest(BackupType.INCREMENTAL, tables, BACKUP_ROOT_DIR);
    String backupIdIncMultiple = client.backupTables(request);

    assertTrue(checkSucceeded(backupIdIncMultiple));

    t1 = insertIntoTable(conn, table1, famName, 2, ADD_ROWS);
    t1.close();

    t2 = insertIntoTable(conn, table2, famName, 2, ADD_ROWS);
    t2.close();

    // #3 - incremental backup for multiple tables
    request = createBackupRequest(BackupType.INCREMENTAL, tables, BACKUP_ROOT_DIR);
    String backupIdIncMultiple2 = client.backupTables(request);
    assertTrue(checkSucceeded(backupIdIncMultiple2));
        // #4 Merge backup images with failures

    for (FailurePhase phase : FailurePhase.values()) {
      Configuration conf = conn.getConfiguration();

      conf.set(FAILURE_PHASE_KEY, phase.toString());

      try (BackupAdmin bAdmin = new BackupAdminImpl(conn)) {
        String[] backups = new String[] { backupIdIncMultiple, backupIdIncMultiple2 };
        bAdmin.mergeBackups(backups);
        Assert.fail("Expected IOException");
      } catch (IOException e) {
        BackupSystemTable table = new BackupSystemTable(conn);
        if(phase.ordinal() < FailurePhase.PHASE4.ordinal()) {
          // No need to repair:
          // Both Merge and backup exclusive operations are finished
          assertFalse(table.isMergeInProgress());
          try {
            table.finishBackupExclusiveOperation();
            Assert.fail("IOException is expected");
          } catch(IOException ee) {
            // Expected
          }
        } else {
          // Repair is required
          assertTrue(table.isMergeInProgress());
          try {
            table.startBackupExclusiveOperation();
            Assert.fail("IOException is expected");
          } catch(IOException ee) {
            // Expected - clean up before proceeding
            //table.finishMergeOperation();
            //table.finishBackupExclusiveOperation();
          }
        }
        table.close();
        LOG.debug("Expected :"+ e.getMessage());
      }
    }
    // Now merge w/o failures
    Configuration conf = conn.getConfiguration();
    conf.unset(FAILURE_PHASE_KEY);
    conf.unset(BackupRestoreFactory.HBASE_BACKUP_MERGE_IMPL_CLASS);
    // Now run repair
    BackupSystemTable sysTable = new BackupSystemTable(conn);
    BackupCommands.RepairCommand.repairFailedBackupMergeIfAny(conn, sysTable);
    // Now repeat merge
    try (BackupAdmin bAdmin = new BackupAdminImpl(conn)) {
      String[] backups = new String[] { backupIdIncMultiple, backupIdIncMultiple2 };
      bAdmin.mergeBackups(backups);
    }

    // #6 - restore incremental backup for multiple tables, with overwrite
    TableName[] tablesRestoreIncMultiple = new TableName[] { table1, table2 };
    TableName[] tablesMapIncMultiple = new TableName[] { table1_restore, table2_restore };
    client.restore(BackupUtils.createRestoreRequest(BACKUP_ROOT_DIR, backupIdIncMultiple2, false,
      tablesRestoreIncMultiple, tablesMapIncMultiple, true));

    Table hTable = conn.getTable(table1_restore);
    LOG.debug("After incremental restore: " + hTable.getDescriptor());
    LOG.debug("f1 has " + TEST_UTIL.countRows(hTable, famName) + " rows");
    Assert.assertEquals(TEST_UTIL.countRows(hTable, famName), NB_ROWS_IN_BATCH + 2 * ADD_ROWS);

    hTable.close();

    hTable = conn.getTable(table2_restore);
    Assert.assertEquals(TEST_UTIL.countRows(hTable), NB_ROWS_IN_BATCH + 2 * ADD_ROWS);
    hTable.close();

    admin.close();
    conn.close();
  }
}
