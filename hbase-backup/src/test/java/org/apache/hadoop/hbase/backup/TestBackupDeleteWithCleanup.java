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

import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.CONF_CONTINUOUS_BACKUP_WAL_DIR;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.CONTINUOUS_BACKUP_REPLICATION_PEER;
import static org.apache.hadoop.hbase.backup.replication.BackupFileSystemManager.BULKLOAD_FILES_DIR;
import static org.apache.hadoop.hbase.backup.replication.BackupFileSystemManager.WALS_DIR;
import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupReplicationEndpoint.ONE_DAY_IN_MILLISECONDS;
import static org.apache.hadoop.hbase.backup.util.BackupUtils.DATE_FORMAT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupAdminImpl;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.backup.replication.BackupFileSystemManager;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@Category(LargeTests.class)
public class TestBackupDeleteWithCleanup extends TestBackupBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBackupDeleteWithCleanup.class);

  String backupWalDirName = "TestBackupDeleteWithCleanup";

  private FileSystem fs;
  private Path backupWalDir;
  private BackupSystemTable backupSystemTable;

  @Before
  public void setUpTest() throws Exception {
    Path root = TEST_UTIL.getDataTestDirOnTestFS();
    backupWalDir = new Path(root, backupWalDirName);
    conf1.set(CONF_CONTINUOUS_BACKUP_WAL_DIR, backupWalDir.toString());
    fs = FileSystem.get(conf1);
    fs.mkdirs(backupWalDir);
    backupSystemTable = new BackupSystemTable(TEST_UTIL.getConnection());
  }

  @After
  public void tearDownTest() throws Exception {
    if (backupSystemTable != null) {
      backupSystemTable.close();
    }
    if (fs != null && backupWalDir != null) {
      fs.delete(backupWalDir, true);
    }

    EnvironmentEdgeManager.reset();
  }

  @Test
  public void testBackupDeleteWithCleanupLogic() throws Exception {
    // Step 1: Setup Backup Folders
    long currentTime = EnvironmentEdgeManager.getDelegate().currentTime();
    setupBackupFolders(currentTime);

    // Log the directory structure before cleanup
    logDirectoryStructure(fs, backupWalDir, "Directory structure BEFORE cleanup:");

    // Step 2: Simulate Backup Creation
    backupSystemTable.addContinuousBackupTableSet(Set.of(table1),
      currentTime - (2 * ONE_DAY_IN_MILLISECONDS));

    EnvironmentEdgeManager
      .injectEdge(() -> System.currentTimeMillis() - (2 * ONE_DAY_IN_MILLISECONDS));

    String backupId = fullTableBackup(Lists.newArrayList(table1));
    assertTrue(checkSucceeded(backupId));
    String anotherBackupId = fullTableBackup(Lists.newArrayList(table1));
    assertTrue(checkSucceeded(anotherBackupId));

    // Step 3: Run Delete Command
    deleteBackup(backupId);

    // Log the directory structure after cleanup
    logDirectoryStructure(fs, backupWalDir, "Directory structure AFTER cleanup:");

    // Step 4: Verify Cleanup
    verifyBackupCleanup(fs, backupWalDir, currentTime);

    // Step 5: Verify System Table Update
    verifySystemTableUpdate(backupSystemTable, currentTime);

    // Cleanup
    deleteBackup(anotherBackupId);
  }

  @Test
  public void testSingleBackupForceDelete() throws Exception {
    // Step 1: Setup Backup Folders
    long currentTime = EnvironmentEdgeManager.getDelegate().currentTime();
    setupBackupFolders(currentTime);

    // Log the directory structure before cleanup
    logDirectoryStructure(fs, backupWalDir, "Directory structure BEFORE cleanup:");

    // Step 2: Simulate Backup Creation
    backupSystemTable.addContinuousBackupTableSet(Set.of(table1),
      currentTime - (2 * ONE_DAY_IN_MILLISECONDS));

    EnvironmentEdgeManager
      .injectEdge(() -> System.currentTimeMillis() - (2 * ONE_DAY_IN_MILLISECONDS));

    String backupId = fullTableBackupWithContinuous(Lists.newArrayList(table1));
    assertTrue(checkSucceeded(backupId));

    assertTrue("Backup replication peer should be enabled after the backup",
      continuousBackupReplicationPeerExistsAndEnabled());

    // Step 3: Run Delete Command
    deleteBackup(backupId);

    // Log the directory structure after cleanup
    logDirectoryStructure(fs, backupWalDir, "Directory structure AFTER cleanup:");

    // Step 4: Verify CONTINUOUS_BACKUP_REPLICATION_PEER is disabled
    assertFalse("Backup replication peer should be disabled or removed",
      continuousBackupReplicationPeerExistsAndEnabled());

    // Step 5: Verify that system table is updated to remove all the tables
    Set<TableName> remainingTables = backupSystemTable.getContinuousBackupTableSet().keySet();
    assertTrue("System table should have no tables after all full backups are clear",
      remainingTables.isEmpty());

    // Step 6: Verify that the backup WAL directory is empty
    assertTrue("WAL backup directory should be empty after force delete",
      areWalAndBulkloadDirsEmpty(conf1, backupWalDir.toString()));

    // Step 7: Take new full backup with continuous backup enabled
    String backupIdContinuous = fullTableBackupWithContinuous(Lists.newArrayList(table1));

    // Step 8: Verify CONTINUOUS_BACKUP_REPLICATION_PEER is enabled again
    assertTrue("Backup replication peer should be re-enabled after new backup",
      continuousBackupReplicationPeerExistsAndEnabled());

    // And system table has new entry
    Set<TableName> newTables = backupSystemTable.getContinuousBackupTableSet().keySet();
    assertTrue("System table should contain the table after new backup",
      newTables.contains(table1));

    // Cleanup
    deleteBackup(backupIdContinuous);
  }

  private void setupBackupFolders(long currentTime) throws IOException {
    setupBackupFolders(fs, backupWalDir, currentTime);
  }

  public static void setupBackupFolders(FileSystem fs, Path backupWalDir, long currentTime)
    throws IOException {
    Path walsDir = new Path(backupWalDir, WALS_DIR);
    Path bulkLoadDir = new Path(backupWalDir, BULKLOAD_FILES_DIR);

    fs.mkdirs(walsDir);
    fs.mkdirs(bulkLoadDir);

    SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);

    for (int i = 0; i < 5; i++) {
      String dateStr = dateFormat.format(new Date(currentTime - (i * ONE_DAY_IN_MILLISECONDS)));
      fs.mkdirs(new Path(walsDir, dateStr));
      fs.mkdirs(new Path(bulkLoadDir, dateStr));
    }
  }

  private static void verifyBackupCleanup(FileSystem fs, Path backupWalDir, long currentTime)
    throws IOException {
    Path walsDir = new Path(backupWalDir, WALS_DIR);
    Path bulkLoadDir = new Path(backupWalDir, BULKLOAD_FILES_DIR);
    SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);

    // Expect folders older than 3 days to be deleted
    for (int i = 3; i < 5; i++) {
      String oldDateStr = dateFormat.format(new Date(currentTime - (i * ONE_DAY_IN_MILLISECONDS)));
      Path walPath = new Path(walsDir, oldDateStr);
      Path bulkLoadPath = new Path(bulkLoadDir, oldDateStr);
      assertFalse("Old WAL directory (" + walPath + ") should be deleted, but it exists!",
        fs.exists(walPath));
      assertFalse("Old BulkLoad directory (" + bulkLoadPath + ") should be deleted, but it exists!",
        fs.exists(bulkLoadPath));
    }

    // Expect folders within the last 3 days to exist
    for (int i = 0; i < 3; i++) {
      String recentDateStr =
        dateFormat.format(new Date(currentTime - (i * ONE_DAY_IN_MILLISECONDS)));
      Path walPath = new Path(walsDir, recentDateStr);
      Path bulkLoadPath = new Path(bulkLoadDir, recentDateStr);

      assertTrue("Recent WAL directory (" + walPath + ") should exist, but it is missing!",
        fs.exists(walPath));
      assertTrue(
        "Recent BulkLoad directory (" + bulkLoadPath + ") should exist, but it is missing!",
        fs.exists(bulkLoadPath));
    }
  }

  private void verifySystemTableUpdate(BackupSystemTable backupSystemTable, long currentTime)
    throws IOException {
    Map<TableName, Long> updatedTables = backupSystemTable.getContinuousBackupTableSet();

    for (Map.Entry<TableName, Long> entry : updatedTables.entrySet()) {
      long updatedStartTime = entry.getValue();

      // Ensure that the updated start time is not earlier than the expected cutoff time
      assertTrue("System table update failed!",
        updatedStartTime >= (currentTime - (3 * ONE_DAY_IN_MILLISECONDS)));
    }
  }

  public static void logDirectoryStructure(FileSystem fs, Path dir, String message)
    throws IOException {
    System.out.println(message);
    listDirectory(fs, dir, "  ");
  }

  public static void listDirectory(FileSystem fs, Path dir, String indent) throws IOException {
    if (!fs.exists(dir)) {
      System.out.println(indent + "[Missing] " + dir);
      return;
    }
    FileStatus[] files = fs.listStatus(dir);
    System.out.println(indent + dir);
    for (FileStatus file : files) {
      if (file.isDirectory()) {
        listDirectory(fs, file.getPath(), indent + "  ");
      } else {
        System.out.println(indent + "  " + file.getPath());
      }
    }
  }

  private boolean continuousBackupReplicationPeerExistsAndEnabled() throws IOException {
    return TEST_UTIL.getAdmin().listReplicationPeers().stream().anyMatch(
      peer -> peer.getPeerId().equals(CONTINUOUS_BACKUP_REPLICATION_PEER) && peer.isEnabled());
  }

  private static boolean areWalAndBulkloadDirsEmpty(Configuration conf, String backupWalDir)
    throws IOException {
    BackupFileSystemManager manager =
      new BackupFileSystemManager(CONTINUOUS_BACKUP_REPLICATION_PEER, conf, backupWalDir);

    FileSystem fs = manager.getBackupFs();
    Path walDir = manager.getWalsDir();
    Path bulkloadDir = manager.getBulkLoadFilesDir();

    return isDirectoryEmpty(fs, walDir) && isDirectoryEmpty(fs, bulkloadDir);
  }

  private static boolean isDirectoryEmpty(FileSystem fs, Path dirPath) throws IOException {
    if (!fs.exists(dirPath)) {
      // Directory doesn't exist — treat as empty
      return true;
    }
    FileStatus[] entries = fs.listStatus(dirPath);
    return entries == null || entries.length == 0;
  }

  private static void deleteBackup(String backupId) throws Exception {
    int ret =
      ToolRunner.run(conf1, new BackupDriver(), new String[] { "delete", "-l", backupId, "-fd" });
    assertEquals(0, ret);
  }

  private String fullTableBackupWithContinuous(List<TableName> tables) throws IOException {
    try (BackupAdmin admin = new BackupAdminImpl(TEST_UTIL.getConnection())) {
      BackupRequest request =
        createBackupRequest(BackupType.FULL, new ArrayList<>(tables), BACKUP_ROOT_DIR, false, true);
      return admin.backupTables(request);
    }
  }

}
