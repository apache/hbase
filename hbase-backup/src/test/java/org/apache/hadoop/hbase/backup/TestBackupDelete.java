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

import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.CONF_CONTINUOUS_BACKUP_PITR_WINDOW_DAYS;
import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupReplicationEndpoint.ONE_DAY_IN_MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.ToolRunner;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

@Category(LargeTests.class)
public class TestBackupDelete extends TestBackupBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBackupDelete.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestBackupDelete.class);

  /**
   * Verify that full backup is created on a single table with data correctly. Verify that history
   * works as expected.
   * @throws Exception if doing the backup or an operation on the tables fails
   */
  @Test
  public void testBackupDelete() throws Exception {
    LOG.info("test backup delete on a single table with data");
    List<TableName> tableList = Lists.newArrayList(table1);
    String backupId = fullTableBackup(tableList);
    assertTrue(checkSucceeded(backupId));
    LOG.info("backup complete");
    String[] backupIds = new String[] { backupId };
    BackupSystemTable table = new BackupSystemTable(TEST_UTIL.getConnection());
    BackupInfo info = table.readBackupInfo(backupId);
    Path path = new Path(info.getBackupRootDir(), backupId);
    FileSystem fs = FileSystem.get(path.toUri(), conf1);
    assertTrue(fs.exists(path));
    int deleted = getBackupAdmin().deleteBackups(backupIds);

    assertTrue(!fs.exists(path));
    assertTrue(fs.exists(new Path(info.getBackupRootDir())));
    assertTrue(1 == deleted);
    table.close();
    LOG.info("delete_backup");
  }

  /**
   * Verify that full backup is created on a single table with data correctly. Verify that history
   * works as expected.
   * @throws Exception if doing the backup or an operation on the tables fails
   */
  @Test
  public void testBackupDeleteCommand() throws Exception {
    LOG.info("test backup delete on a single table with data: command-line");
    List<TableName> tableList = Lists.newArrayList(table1);
    String backupId = fullTableBackup(tableList);
    assertTrue(checkSucceeded(backupId));
    LOG.info("backup complete");
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));

    String[] args = new String[] { "delete", "-l", backupId };
    // Run backup

    try {
      int ret = ToolRunner.run(conf1, new BackupDriver(), args);
      assertTrue(ret == 0);
    } catch (Exception e) {
      LOG.error("failed", e);
    }
    LOG.info("delete_backup");
    String output = baos.toString();
    LOG.info(baos.toString());
    assertTrue(output.indexOf("Deleted 1 backups") >= 0);
  }

  @Test
  public void testBackupPurgeOldBackupsCommand() throws Exception {
    LOG.info("test backup delete (purge old backups) on a single table with data: command-line");
    List<TableName> tableList = Lists.newArrayList(table1);
    EnvironmentEdgeManager.injectEdge(new EnvironmentEdge() {
      // time - 2 days
      @Override
      public long currentTime() {
        return System.currentTimeMillis() - 2 * 24 * 3600 * 1000;
      }
    });
    String backupId = fullTableBackup(tableList);
    assertTrue(checkSucceeded(backupId));

    EnvironmentEdgeManager.reset();

    LOG.info("backup complete");
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));

    // Purge all backups which are older than 3 days
    // Must return 0 (no backups were purged)
    String[] args = new String[] { "delete", "-k", "3" };
    // Run backup

    try {
      int ret = ToolRunner.run(conf1, new BackupDriver(), args);
      assertTrue(ret == 0);
    } catch (Exception e) {
      LOG.error("failed", e);
      fail(e.getMessage());
    }
    String output = baos.toString();
    LOG.info(baos.toString());
    assertTrue(output.indexOf("Deleted 0 backups") >= 0);

    // Purge all backups which are older than 1 days
    // Must return 1 deleted backup
    args = new String[] { "delete", "-k", "1" };
    // Run backup
    baos.reset();
    try {
      int ret = ToolRunner.run(conf1, new BackupDriver(), args);
      assertTrue(ret == 0);
    } catch (Exception e) {
      LOG.error("failed", e);
      fail(e.getMessage());
    }
    output = baos.toString();
    LOG.info(baos.toString());
    assertTrue(output.indexOf("Deleted 1 backups") >= 0);
  }

  /**
   * Verify that backup deletion updates the incremental-backup-set.
   */
  @Test
  public void testBackupDeleteUpdatesIncrementalBackupSet() throws Exception {
    LOG.info("Test backup delete updates the incremental backup set");
    BackupSystemTable backupSystemTable = new BackupSystemTable(TEST_UTIL.getConnection());

    String backupId1 = fullTableBackup(Lists.newArrayList(table1, table2));
    assertTrue(checkSucceeded(backupId1));
    assertEquals(Sets.newHashSet(table1, table2),
      backupSystemTable.getIncrementalBackupTableSet(BACKUP_ROOT_DIR));

    String backupId2 = fullTableBackup(Lists.newArrayList(table3));
    assertTrue(checkSucceeded(backupId2));
    assertEquals(Sets.newHashSet(table1, table2, table3),
      backupSystemTable.getIncrementalBackupTableSet(BACKUP_ROOT_DIR));

    getBackupAdmin().deleteBackups(new String[] { backupId1 });
    assertEquals(Sets.newHashSet(table3),
      backupSystemTable.getIncrementalBackupTableSet(BACKUP_ROOT_DIR));
  }

  @Test
  public void testPITRBackupDeletion() throws Exception {
    conf1.setLong(CONF_CONTINUOUS_BACKUP_PITR_WINDOW_DAYS, 30);
    BackupSystemTable backupSystemTable = new BackupSystemTable(TEST_UTIL.getConnection());
    long currentTime = System.currentTimeMillis();

    // Set up a continuous backup state for table2 that started 40 days before
    long backupStartTime = currentTime - 40 * ONE_DAY_IN_MILLISECONDS;
    backupSystemTable.addContinuousBackupTableSet(Set.of(table2), backupStartTime);

    String backupId1 = fullTableBackup(Lists.newArrayList(table1));
    assertTrue(checkSucceeded(backupId1));

    // 31 days back
    EnvironmentEdgeManager
      .injectEdge(() -> System.currentTimeMillis() - 31 * ONE_DAY_IN_MILLISECONDS);
    String backupId2 = fullTableBackup(Lists.newArrayList(table2));
    assertTrue(checkSucceeded(backupId2));

    // 32 days back
    EnvironmentEdgeManager
      .injectEdge(() -> System.currentTimeMillis() - 32 * ONE_DAY_IN_MILLISECONDS);
    String backupId3 = fullTableBackup(Lists.newArrayList(table2));
    assertTrue(checkSucceeded(backupId3));

    // 15 days back
    EnvironmentEdgeManager
      .injectEdge(() -> System.currentTimeMillis() - 15 * ONE_DAY_IN_MILLISECONDS);
    String backupId4 = fullTableBackup(Lists.newArrayList(table2));
    assertTrue(checkSucceeded(backupId4));

    // Reset time mocking
    EnvironmentEdgeManager.reset();

    String backupId5 = incrementalTableBackup(Lists.newArrayList(table1));
    assertTrue(checkSucceeded(backupId5));

    // Validate deletion scenarios
    // Incremental backup deletion allowed
    assertDeletionSucceeds(backupSystemTable, backupId5, false);
    // Full backup for non-continuous table allowed
    assertDeletionSucceeds(backupSystemTable, backupId1, false);
    // PITR-incomplete backup deletion allowed
    assertDeletionSucceeds(backupSystemTable, backupId4, false);
    // Deleting a valid backup with another present
    assertDeletionSucceeds(backupSystemTable, backupId2, false);
    // Only valid backup deletion should fail
    assertDeletionFails(backupSystemTable, backupId3, false);
    // Force delete should work
    assertDeletionSucceeds(backupSystemTable, backupId3, true);
  }

  private void assertDeletionSucceeds(BackupSystemTable table, String backupId,
    boolean isForceDelete) throws Exception {
    int ret = deleteBackup(backupId, isForceDelete);
    assertEquals(0, ret);
    assertFalse("Backup should be deleted but still exists!", backupExists(table, backupId));
  }

  private void assertDeletionFails(BackupSystemTable table, String backupId, boolean isForceDelete)
    throws Exception {
    int ret = deleteBackup(backupId, isForceDelete);
    assertNotEquals(0, ret);
    assertTrue("Backup should still exist after failed deletion!", backupExists(table, backupId));
  }

  private boolean backupExists(BackupSystemTable table, String backupId) throws Exception {
    return table.getBackupHistory().stream()
      .anyMatch(backup -> backup.getBackupId().equals(backupId));
  }

  private int deleteBackup(String backupId, boolean isForceDelete) throws Exception {
    String[] args = buildBackupDeleteArgs(backupId, isForceDelete);
    return ToolRunner.run(conf1, new BackupDriver(), args);
  }

  private String[] buildBackupDeleteArgs(String backupId, boolean isForceDelete) {
    return isForceDelete
      ? new String[] { "delete", "-l", backupId, "-fd" }
      : new String[] { "delete", "-l", backupId };
  }
}
