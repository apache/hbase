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
import static org.apache.hadoop.hbase.backup.replication.BackupFileSystemManager.BULKLOAD_FILES_DIR;
import static org.apache.hadoop.hbase.backup.replication.BackupFileSystemManager.WALS_DIR;
import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupReplicationEndpoint.DATE_FORMAT;
import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupReplicationEndpoint.ONE_DAY_IN_MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.ToolRunner;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@Category(LargeTests.class)
public class TestBackupCleanupCommand extends TestBackupBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBackupDelete.class);

  String backupWalDirName = "TestContinuousBackupWalDir";

  @Test
  public void testCleanupCommand() throws Exception {
    Path root = TEST_UTIL.getDataTestDirOnTestFS();
    Path backupWalDir = new Path(root, backupWalDirName);
    conf1.set(CONF_CONTINUOUS_BACKUP_WAL_DIR, backupWalDir.toString());
    FileSystem fs = FileSystem.get(conf1);
    fs.mkdirs(backupWalDir);

    // Step 1: Setup Backup Folders
    long currentTime = EnvironmentEdgeManager.getDelegate().currentTime();
    setupBackupFolders(fs, backupWalDir, currentTime);

    // Log the directory structure before cleanup
    logDirectoryStructure(fs, backupWalDir, "Directory structure BEFORE cleanup:");

    // Step 2: Simulate Backup Creation
    BackupSystemTable backupSystemTable = new BackupSystemTable(TEST_UTIL.getConnection());
    backupSystemTable.addContinuousBackupTableSet(Set.of(table1),
      currentTime - (2 * ONE_DAY_IN_MILLISECONDS));

    EnvironmentEdgeManager
      .injectEdge(() -> System.currentTimeMillis() - (2 * ONE_DAY_IN_MILLISECONDS));
    String backupId1 = fullTableBackup(Lists.newArrayList(table1));
    assertTrue(checkSucceeded(backupId1));

    // Step 3: Run Cleanup Command
    int ret = ToolRunner.run(conf1, new BackupDriver(), new String[] { "cleanup" });
    assertEquals(0, ret);

    // Log the directory structure after cleanup
    logDirectoryStructure(fs, backupWalDir, "Directory structure AFTER cleanup:");

    // Step 4: Verify Cleanup
    verifyBackupCleanup(fs, backupWalDir, currentTime);
  }

  private static void setupBackupFolders(FileSystem fs, Path backupWalDir, long currentTime)
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

  private static void logDirectoryStructure(FileSystem fs, Path dir, String message)
    throws IOException {
    System.out.println(message);
    listDirectory(fs, dir, "  ");
  }

  private static void listDirectory(FileSystem fs, Path dir, String indent) throws IOException {
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
}
