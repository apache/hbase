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
import static org.apache.hadoop.hbase.backup.TestBackupDeleteWithCleanup.logDirectoryStructure;
import static org.apache.hadoop.hbase.backup.TestBackupDeleteWithCleanup.setupBackupFolders;
import static org.apache.hadoop.hbase.backup.replication.BackupFileSystemManager.BULKLOAD_FILES_DIR;
import static org.apache.hadoop.hbase.backup.replication.BackupFileSystemManager.WALS_DIR;
import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupReplicationEndpoint.DATE_FORMAT;
import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupReplicationEndpoint.ONE_DAY_IN_MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.backup.TestBackupBase;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestBackupCommands extends TestBackupBase {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBackupCommands.class);

  String backupWalDirName = "TestBackupWalDir";

  /**
   * Tests whether determineWALCleanupCutoffTime returns the correct FULL backup start timestamp.
   */
  @Test
  public void testDetermineWALCleanupCutoffTimeOfCleanupCommand() throws IOException {
    // GIVEN
    BackupSystemTable sysTable = mock(BackupSystemTable.class);

    BackupInfo full1 = new BackupInfo();
    full1.setType(BackupType.FULL);
    full1.setStartTs(1111L);
    full1.setState(BackupInfo.BackupState.COMPLETE);

    BackupInfo inc = new BackupInfo();
    inc.setType(BackupType.INCREMENTAL);
    inc.setStartTs(2222L);
    inc.setState(BackupInfo.BackupState.COMPLETE);

    BackupInfo full2 = new BackupInfo();
    full2.setType(BackupType.FULL);
    full2.setStartTs(3333L);
    full2.setState(BackupInfo.BackupState.COMPLETE);

    // Ordered as newest to oldest, will be reversed in the method
    List<BackupInfo> backupInfos = List.of(full2, inc, full1);
    when(sysTable.getBackupInfos(BackupInfo.BackupState.COMPLETE))
      .thenReturn(new ArrayList<>(backupInfos));

    // WHEN
    BackupCommands.DeleteCommand command = new BackupCommands.DeleteCommand(conf1, null);
    long cutoff = command.determineWALCleanupCutoffTime(sysTable);

    // THEN
    assertEquals("Expected oldest FULL backup timestamp", 1111L, cutoff);
  }

  @Test
  public void testUpdateBackupTableStartTimesOfCleanupCommand() throws IOException {
    // GIVEN
    BackupSystemTable mockSysTable = mock(BackupSystemTable.class);

    TableName tableA = TableName.valueOf("ns", "tableA");
    TableName tableB = TableName.valueOf("ns", "tableB");
    TableName tableC = TableName.valueOf("ns", "tableC");

    long cutoffTimestamp = 1_000_000L;

    // Simulate current table start times
    Map<TableName, Long> tableSet = Map.of(tableA, 900_000L, // Before cutoff → should be updated
      tableB, 1_100_000L, // After cutoff → should NOT be updated
      tableC, 800_000L // Before cutoff → should be updated
    );

    when(mockSysTable.getContinuousBackupTableSet()).thenReturn(tableSet);

    // WHEN
    BackupCommands.DeleteCommand command = new BackupCommands.DeleteCommand(conf1, null);
    command.updateBackupTableStartTimes(mockSysTable, cutoffTimestamp);

    // THEN
    Set<TableName> expectedUpdated = Set.of(tableA, tableC);
    verify(mockSysTable).updateContinuousBackupTableSet(expectedUpdated, cutoffTimestamp);
  }

  @Test
  public void testDeleteOldWALFilesOfCleanupCommand() throws IOException {
    // GIVEN
    Path root = TEST_UTIL.getDataTestDirOnTestFS();
    Path backupWalDir = new Path(root, backupWalDirName);
    conf1.set(CONF_CONTINUOUS_BACKUP_WAL_DIR, backupWalDir.toString());

    FileSystem fs = FileSystem.get(conf1);
    fs.mkdirs(backupWalDir);

    long currentTime = EnvironmentEdgeManager.getDelegate().currentTime();
    setupBackupFolders(fs, backupWalDir, currentTime); // Create 5 days of WAL/bulk folders

    logDirectoryStructure(fs, backupWalDir, "Before cleanup:");

    // Delete files older than 2 days from current time
    long cutoffTime = currentTime - (2 * ONE_DAY_IN_MILLISECONDS);

    // WHEN
    BackupCommands.DeleteCommand command = new BackupCommands.DeleteCommand(conf1, null);
    command.deleteOldWALFiles(conf1, backupWalDir.toString(), cutoffTime);

    logDirectoryStructure(fs, backupWalDir, "After cleanup:");

    // THEN
    verifyCleanupOutcome(fs, backupWalDir, currentTime, cutoffTime);
  }

  private static void verifyCleanupOutcome(FileSystem fs, Path backupWalDir, long currentTime,
    long cutoffTime) throws IOException {
    Path walsDir = new Path(backupWalDir, WALS_DIR);
    Path bulkLoadDir = new Path(backupWalDir, BULKLOAD_FILES_DIR);
    SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

    for (int i = 0; i < 5; i++) {
      long dayTime = currentTime - (i * ONE_DAY_IN_MILLISECONDS);
      String dayDir = dateFormat.format(new Date(dayTime));
      Path walPath = new Path(walsDir, dayDir);
      Path bulkPath = new Path(bulkLoadDir, dayDir);

      if (dayTime + ONE_DAY_IN_MILLISECONDS - 1 < cutoffTime) {
        assertFalse("Old WAL dir should be deleted: " + walPath, fs.exists(walPath));
        assertFalse("Old BulkLoad dir should be deleted: " + bulkPath, fs.exists(bulkPath));
      } else {
        assertTrue("Recent WAL dir should exist: " + walPath, fs.exists(walPath));
        assertTrue("Recent BulkLoad dir should exist: " + bulkPath, fs.exists(bulkPath));
      }
    }
  }
}
