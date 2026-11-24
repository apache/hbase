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
package org.apache.hadoop.hbase.backup;

import static org.apache.hadoop.hbase.backup.BackupInfo.withState;
import static org.apache.hadoop.hbase.backup.TestBackupUtils.assertBackupCompleteFileDoesNotExist;
import static org.apache.hadoop.hbase.backup.TestBackupUtils.assertBackupCompleteFileExists;
import static org.apache.hadoop.hbase.backup.impl.BackupSystemTable.Order.NEW_TO_OLD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupState;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.backup.impl.ExclusiveOperationException;
import org.apache.hadoop.hbase.backup.impl.TableBackupClient;
import org.apache.hadoop.hbase.backup.impl.TableBackupClient.Stage;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.util.ToolRunner;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(LargeTests.TAG)
public class TestBackupRepair extends TestBackupBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestBackupRepair.class);

  @Test
  public void testFullBackupWithFailuresAndRestore() throws Exception {

    autoRestoreOnFailure = false;

    conf1.set(TableBackupClient.BACKUP_CLIENT_IMPL_CLASS,
      FullTableBackupClientForTest.class.getName());
    int maxStage = Stage.values().length - 1;
    // Fail stage in loop between 0 and 4 inclusive
    for (int stage = 0; stage < maxStage; stage++) {
      LOG.info("Running stage " + stage);
      runBackupAndFailAtStageWithRestore(stage);
    }
  }

  public void runBackupAndFailAtStageWithRestore(int stage) throws Exception {

    conf1.setInt(FullTableBackupClientForTest.BACKUP_TEST_MODE_STAGE, stage);
    try (BackupSystemTable table = new BackupSystemTable(TEST_UTIL.getConnection())) {
      int before = table.getBackupHistory().size();
      String[] args = new String[] { "create", "full", BACKUP_ROOT_DIR, "-t",
        table1.getNameAsString() + "," + table2.getNameAsString() };
      // Run backup
      int ret = ToolRunner.run(conf1, new BackupDriver(), args);
      assertNotEquals(0, ret);

      // Create a stale .backup.complete marker and verify repair removes it
      List<BackupInfo> runningBackups =
        table.getBackupHistory(NEW_TO_OLD, 1, withState(BackupState.RUNNING));
      assertEquals(1, runningBackups.size());
      String failedBackupId = runningBackups.get(0).getBackupId();
      HBackupFileSystem.writeBackupCompleteMarker(conf1, BACKUP_ROOT_DIR, failedBackupId);
      assertBackupCompleteFileExists(conf1, BACKUP_ROOT_DIR, failedBackupId);

      // Now run restore
      args = new String[] { "repair" };

      ret = ToolRunner.run(conf1, new BackupDriver(), args);
      assertEquals(0, ret);

      List<BackupInfo> backups = table.getBackupHistory();
      int after = table.getBackupHistory().size();

      assertEquals(before + 1, after);
      for (BackupInfo data : backups) {
        String backupId = data.getBackupId();
        assertFalse(checkSucceeded(backupId));
        assertBackupCompleteFileDoesNotExist(conf1, BACKUP_ROOT_DIR, backupId);
      }
      Set<TableName> tables = table.getIncrementalBackupTableSet(BACKUP_ROOT_DIR);
      assertEquals(0, tables.size());
    }
  }

  // Verifies that if repair fails because the .backup.complete marker cannot be deleted, then it
  // fails before any destructive recovery step. The failed backup stays discoverable, and a later
  // repair can finish the recovery and release the backup exclusive lock.
  @Test
  public void testRepairRetriesAfterMarkerDeleteFailure() throws Exception {
    autoRestoreOnFailure = false;
    conf1.set(TableBackupClient.BACKUP_CLIENT_IMPL_CLASS,
      FullTableBackupClientForTest.class.getName());
    // Fail after the table snapshots are taken, leaving the backup RUNNING with the lock held
    conf1.setInt(FullTableBackupClientForTest.BACKUP_TEST_MODE_STAGE, Stage.stage_3.ordinal());

    Connection conn = TEST_UTIL.getConnection();
    try (BackupSystemTable table = new BackupSystemTable(conn)) {
      String[] args = new String[] { "create", "full", BACKUP_ROOT_DIR, "-t",
        table1.getNameAsString() + "," + table2.getNameAsString() };
      assertNotEquals(0, ToolRunner.run(conf1, new BackupDriver(), args));

      String failedBackupId = assertSingleRunningBackup(table);
      // Create a stale .backup.complete marker that repair has to remove
      HBackupFileSystem.writeBackupCompleteMarker(conf1, BACKUP_ROOT_DIR, failedBackupId);
      assertBackupCompleteFileExists(conf1, BACKUP_ROOT_DIR, failedBackupId);

      // Run repair with a file system that cannot delete the marker
      Configuration markerFailConf = BackupTestUtil.createMarkerDeletionFailConf(conf1);
      args = new String[] { "repair" };
      assertNotEquals(0, ToolRunner.run(markerFailConf, new BackupDriver(), args));

      // Nothing should have been rolled back: the backup is still RUNNING, the backup system table
      // snapshot still exists, the marker is still present, and the lock is still held
      assertEquals(failedBackupId, assertSingleRunningBackup(table));
      assertTrue(BackupSystemTable.snapshotExists(conn));
      assertBackupCompleteFileExists(conf1, BACKUP_ROOT_DIR, failedBackupId);
      assertThrows(ExclusiveOperationException.class, table::startBackupExclusiveOperation);

      // Clear the failure and repair again
      assertEquals(0, ToolRunner.run(conf1, new BackupDriver(), args));

      assertFalse(checkSucceeded(failedBackupId));
      assertEquals(0, table.getBackupHistory(NEW_TO_OLD, 1, withState(BackupState.RUNNING)).size());
      assertFalse(BackupSystemTable.snapshotExists(conn));
      assertBackupCompleteFileDoesNotExist(conf1, BACKUP_ROOT_DIR, failedBackupId);
      // The lock must have been released
      table.startBackupExclusiveOperation();
      table.finishBackupExclusiveOperation();
    } finally {
      conf1.unset(TableBackupClient.BACKUP_CLIENT_IMPL_CLASS);
      conf1.unset(FullTableBackupClientForTest.BACKUP_TEST_MODE_STAGE);
    }
  }

  private static String assertSingleRunningBackup(BackupSystemTable table) throws IOException {
    List<BackupInfo> runningBackups =
      table.getBackupHistory(NEW_TO_OLD, 1, withState(BackupState.RUNNING));
    assertEquals(1, runningBackups.size());
    return runningBackups.get(0).getBackupId();
  }
}
