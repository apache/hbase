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
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_ENABLE_CONTINUOUS_BACKUP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupManifest;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

@Category(LargeTests.class)
public class TestContinuousBackup extends TestBackupBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestContinuousBackup.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestContinuousBackup.class);

  String backupWalDirName = "TestContinuousBackupWalDir";

  @Before
  public void beforeTest() throws IOException {
    Path root = TEST_UTIL.getDataTestDirOnTestFS();
    Path backupWalDir = new Path(root, backupWalDirName);
    FileSystem fs = FileSystem.get(conf1);
    fs.mkdirs(backupWalDir);
    conf1.set(CONF_CONTINUOUS_BACKUP_WAL_DIR, backupWalDir.toString());
  }

  @After
  public void afterTest() throws IOException {
    Path root = TEST_UTIL.getDataTestDirOnTestFS();
    Path backupWalDir = new Path(root, backupWalDirName);
    FileSystem fs = FileSystem.get(conf1);

    if (fs.exists(backupWalDir)) {
      fs.delete(backupWalDir, true);
    }

    conf1.unset(CONF_CONTINUOUS_BACKUP_WAL_DIR);
    deleteContinuousBackupReplicationPeerIfExists(TEST_UTIL.getAdmin());
  }

  @Test
  public void testContinuousBackupWithFullBackup() throws Exception {
    LOG.info("Testing successful continuous backup with full backup");
    String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
    TableName tableName = TableName.valueOf("table_" + methodName);
    TEST_UTIL.createTable(tableName, "cf");

    try (BackupSystemTable table = new BackupSystemTable(TEST_UTIL.getConnection())) {
      int before = table.getBackupHistory().size();

      // Run backup
      String[] args = buildBackupArgs("full", new TableName[] { tableName }, true);
      int ret = ToolRunner.run(conf1, new BackupDriver(), args);
      assertEquals("Backup should succeed", 0, ret);

      // Verify backup history increased and all the backups are succeeded
      List<BackupInfo> backups = table.getBackupHistory();
      assertEquals("Backup history should increase", before + 1, backups.size());
      for (BackupInfo data : List.of(backups.get(0))) {
        String backupId = data.getBackupId();
        assertTrue(checkSucceeded(backupId));
      }

      // Verify backup manifest contains the correct tables
      BackupManifest manifest = getLatestBackupManifest(backups);
      assertEquals("Backup should contain the expected tables", Sets.newHashSet(tableName),
        new HashSet<>(manifest.getTableList()));
    }

    // Verify replication peer subscription
    BackupTestUtil.verifyReplicationPeerSubscription(TEST_UTIL, tableName);

    // Verify table is registered in Backup System Table
    verifyTableInBackupSystemTable(tableName);
  }

  @Test
  public void testContinuousBackupForMultipleTables() throws Exception {
    LOG.info("Test continuous backup for multiple tables");
    String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
    TableName tableName1 = TableName.valueOf("table_" + methodName);
    TEST_UTIL.createTable(tableName1, "cf");
    TableName tableName2 = TableName.valueOf("table_" + methodName + "2");
    TEST_UTIL.createTable(tableName2, "cf");

    try (BackupSystemTable table = new BackupSystemTable(TEST_UTIL.getConnection())) {
      int before = table.getBackupHistory().size();

      // Create full backup for table1
      String[] args = buildBackupArgs("full", new TableName[] { tableName1 }, true);
      int ret = ToolRunner.run(conf1, new BackupDriver(), args);
      assertEquals("Backup should succeed", 0, ret);

      // Create full backup for table2
      args = buildBackupArgs("full", new TableName[] { tableName2 }, true);
      ret = ToolRunner.run(conf1, new BackupDriver(), args);
      assertEquals("Backup should succeed", 0, ret);

      // Verify backup history increased and all the backups are succeeded
      List<BackupInfo> backups = table.getBackupHistory();
      assertEquals("Backup history should increase", before + 2, backups.size());
      for (BackupInfo data : List.of(backups.get(0), backups.get(1))) {
        String backupId = data.getBackupId();
        assertTrue(checkSucceeded(backupId));
      }

      // Verify backup manifest contains the correct tables
      BackupManifest manifest = getLatestBackupManifest(backups);
      assertEquals("Backup should contain the expected tables", Sets.newHashSet(tableName2),
        new HashSet<>(manifest.getTableList()));
    }

    // Verify replication peer subscription for each table
    BackupTestUtil.verifyReplicationPeerSubscription(TEST_UTIL, tableName1);
    BackupTestUtil.verifyReplicationPeerSubscription(TEST_UTIL, tableName2);

    // Verify tables are registered in Backup System Table
    verifyTableInBackupSystemTable(tableName1);
    verifyTableInBackupSystemTable(tableName2);
  }

  @Test
  public void testInvalidBackupScenarioWithContinuousEnabled() throws Exception {
    LOG.info("Testing invalid backup scenario with continuous backup enabled");
    String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
    TableName tableName1 = TableName.valueOf("table_" + methodName);
    TEST_UTIL.createTable(tableName1, "cf");
    TableName tableName2 = TableName.valueOf("table_" + methodName + "2");
    TEST_UTIL.createTable(tableName2, "cf");

    try (BackupSystemTable table = new BackupSystemTable(TEST_UTIL.getConnection())) {
      int before = table.getBackupHistory().size();

      // Create full backup for table1 with continuous backup enabled
      String[] args = buildBackupArgs("full", new TableName[] { tableName1 }, true);
      int ret = ToolRunner.run(conf1, new BackupDriver(), args);
      assertEquals("Backup should succeed", 0, ret);

      // Create full backup for table2 without continuous backup enabled
      args = buildBackupArgs("full", new TableName[] { tableName2 }, false);
      ret = ToolRunner.run(conf1, new BackupDriver(), args);
      assertEquals("Backup should succeed", 0, ret);

      // Attempt full backup for both tables without continuous backup enabled (should fail)
      args = buildBackupArgs("full", new TableName[] { tableName1, tableName2 }, false);
      ret = ToolRunner.run(conf1, new BackupDriver(), args);
      assertTrue("Backup should fail due to mismatch in continuous backup settings", ret != 0);

      // Verify backup history size is unchanged after the failed backup
      int after = table.getBackupHistory().size();
      assertEquals("Backup history should remain unchanged on failure", before + 2, after);
    }
  }

  @Test
  public void testContinuousBackupWithWALDirNotSpecified() throws Exception {
    LOG.info("Testing that continuous backup fails when WAL directory is not specified");
    String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
    TableName tableName = TableName.valueOf("table_" + methodName);
    TEST_UTIL.createTable(tableName, "cf");

    conf1.unset(CONF_CONTINUOUS_BACKUP_WAL_DIR);
    LOG.info("CONF_CONTINUOUS_BACKUP_WAL_DIR: {}", conf1.get(CONF_CONTINUOUS_BACKUP_WAL_DIR));

    try (BackupSystemTable table = new BackupSystemTable(TEST_UTIL.getConnection())) {
      int before = table.getBackupHistory().size();

      // Run full backup without specifying WAL directory (invalid scenario)
      String[] args = buildBackupArgs("full", new TableName[] { tableName }, true);
      int ret = ToolRunner.run(conf1, new BackupDriver(), args);

      assertTrue("Backup should fail when WAL directory is not specified", ret != 0);

      List<BackupInfo> backups = table.getBackupHistory();
      int after = backups.size();
      assertEquals("Backup history should increase", before + 1, after);

      // last backup should be a failure
      assertFalse(checkSucceeded(backups.get(0).getBackupId()));
    }
  }

  @Test
  public void testContinuousBackupWithIncrementalBackup() throws Exception {
    LOG.info("Testing that continuous backup cannot be enabled with incremental backup");
    String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
    TableName tableName = TableName.valueOf("table_" + methodName);
    TEST_UTIL.createTable(tableName, "cf");

    try (BackupSystemTable table = new BackupSystemTable(TEST_UTIL.getConnection())) {
      int before = table.getBackupHistory().size();

      // Run incremental backup with continuous backup flag (invalid scenario)
      String[] args = buildBackupArgs("incremental", new TableName[] { tableName }, true);
      int ret = ToolRunner.run(conf1, new BackupDriver(), args);

      assertTrue("Backup should fail when using continuous backup with incremental mode", ret != 0);

      // Backup history should remain unchanged
      int after = table.getBackupHistory().size();
      assertEquals("Backup history should remain unchanged on failure", before, after);
    }
  }

  String[] buildBackupArgs(String backupType, TableName[] tables, boolean continuousEnabled) {
    String tableNames =
      Arrays.stream(tables).map(TableName::getNameAsString).collect(Collectors.joining(","));

    if (continuousEnabled) {
      return new String[] { "create", backupType, BACKUP_ROOT_DIR, "-t", tableNames,
        "-" + OPTION_ENABLE_CONTINUOUS_BACKUP };
    } else {
      return new String[] { "create", backupType, BACKUP_ROOT_DIR, "-t", tableNames };
    }
  }

  private void verifyTableInBackupSystemTable(TableName table) throws IOException {
    try (BackupSystemTable backupTable = new BackupSystemTable(TEST_UTIL.getConnection())) {
      Map<TableName, Long> tableBackupMap = backupTable.getContinuousBackupTableSet();

      assertTrue("Table is missing in the continuous backup table set",
        tableBackupMap.containsKey(table));

      assertTrue("Timestamp for table should be greater than 0", tableBackupMap.get(table) > 0);
    }
  }

}
