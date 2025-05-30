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
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_TABLE;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_TABLE_MAPPING;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_TO_DATETIME;
import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupReplicationEndpoint.ONE_DAY_IN_MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(LargeTests.class)
public class TestPointInTimeRestore extends TestBackupBase {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestPointInTimeRestore.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestPointInTimeRestore.class);

  private static final String backupWalDirName = "TestPointInTimeRestoreWalDir";
  private static final int WAIT_FOR_REPLICATION_MS = 30_000;
  static Path backupWalDir;
  static FileSystem fs;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Path root = TEST_UTIL.getDataTestDirOnTestFS();
    backupWalDir = new Path(root, backupWalDirName);
    fs = FileSystem.get(conf1);
    fs.mkdirs(backupWalDir);
    conf1.set(CONF_CONTINUOUS_BACKUP_WAL_DIR, backupWalDir.toString());

    setUpBackups();
  }

  /**
   * Sets up multiple backups at different timestamps by: 1. Adjusting the system time to simulate
   * past backup points. 2. Loading data into tables to create meaningful snapshots. 3. Running full
   * backups with or without continuous backup enabled. 4. Ensuring replication is complete before
   * proceeding.
   */
  private static void setUpBackups() throws Exception {
    // Simulate a backup taken 20 days ago
    EnvironmentEdgeManager
      .injectEdge(() -> System.currentTimeMillis() - 20 * ONE_DAY_IN_MILLISECONDS);
    loadRandomData(table1, 1000); // Insert initial data into table1

    // Perform a full backup for table1 with continuous backup enabled
    String[] args = buildBackupArgs("full", new TableName[] { table1 }, true);
    int ret = ToolRunner.run(conf1, new BackupDriver(), args);
    assertEquals("Backup should succeed", 0, ret);

    // Move time forward to simulate 15 days ago
    EnvironmentEdgeManager
      .injectEdge(() -> System.currentTimeMillis() - 15 * ONE_DAY_IN_MILLISECONDS);
    loadRandomData(table1, 1000); // Add more data to table1
    loadRandomData(table2, 500); // Insert data into table2

    waitForReplication(); // Ensure replication is complete

    // Perform a full backup for table2 with continuous backup enabled
    args = buildBackupArgs("full", new TableName[] { table2 }, true);
    ret = ToolRunner.run(conf1, new BackupDriver(), args);
    assertEquals("Backup should succeed", 0, ret);

    // Move time forward to simulate 10 days ago
    EnvironmentEdgeManager
      .injectEdge(() -> System.currentTimeMillis() - 10 * ONE_DAY_IN_MILLISECONDS);
    loadRandomData(table2, 500); // Add more data to table2
    loadRandomData(table3, 500); // Insert data into table3

    // Perform a full backup for table3 and table4 (without continuous backup)
    args = buildBackupArgs("full", new TableName[] { table3, table4 }, false);
    ret = ToolRunner.run(conf1, new BackupDriver(), args);
    assertEquals("Backup should succeed", 0, ret);

    waitForReplication(); // Ensure replication is complete before concluding setup

    // Reset time mocking to avoid affecting other tests
    EnvironmentEdgeManager.reset();
  }

  @AfterClass
  public static void setupAfterClass() throws IOException {
    Path root = TEST_UTIL.getDataTestDirOnTestFS();
    Path backupWalDir = new Path(root, backupWalDirName);
    FileSystem fs = FileSystem.get(conf1);

    if (fs.exists(backupWalDir)) {
      fs.delete(backupWalDir, true);
    }

    conf1.unset(CONF_CONTINUOUS_BACKUP_WAL_DIR);
  }

  /**
   * Verifies that PITR (Point-in-Time Restore) fails when the requested restore time is either in
   * the future or outside the allowed retention window.
   */
  @Test
  public void testPITR_FailsOutsideWindow() throws Exception {
    // Case 1: Requested restore time is in the future (should fail)
    String[] args = buildPITRArgs(new TableName[] { table1 },
      new TableName[] { TableName.valueOf("restoredTable1") },
      EnvironmentEdgeManager.currentTime() + ONE_DAY_IN_MILLISECONDS);

    int ret = ToolRunner.run(conf1, new PointInTimeRestoreDriver(), args);
    assertNotEquals("Restore should fail since the requested restore time is in the future", 0,
      ret);

    // Case 2: Requested restore time is too old (beyond the retention window, should fail)
    args = buildPITRArgs(new TableName[] { table1 },
      new TableName[] { TableName.valueOf("restoredTable1") },
      EnvironmentEdgeManager.currentTime() - 40 * ONE_DAY_IN_MILLISECONDS);

    ret = ToolRunner.run(conf1, new PointInTimeRestoreDriver(), args);
    assertNotEquals(
      "Restore should fail since the requested restore time is outside the retention window", 0,
      ret);
  }

  /**
   * Ensures that PITR fails when attempting to restore tables where continuous backup was not
   * enabled.
   */
  @Test
  public void testPointInTimeRestore_ContinuousBackupNotEnabledTables() throws Exception {
    String[] args = buildPITRArgs(new TableName[] { table3 },
      new TableName[] { TableName.valueOf("restoredTable1") },
      EnvironmentEdgeManager.currentTime() - 10 * ONE_DAY_IN_MILLISECONDS);

    int ret = ToolRunner.run(conf1, new PointInTimeRestoreDriver(), args);
    assertNotEquals("Restore should fail since continuous backup is not enabled for the table", 0,
      ret);
  }

  /**
   * Ensures that PITR fails when trying to restore from a point before continuous backup started.
   */
  @Test
  public void testPointInTimeRestore_TablesWithNoProperBackupOrWals() throws Exception {
    String[] args = buildPITRArgs(new TableName[] { table2 },
      new TableName[] { TableName.valueOf("restoredTable1") },
      EnvironmentEdgeManager.currentTime() - 16 * ONE_DAY_IN_MILLISECONDS);

    int ret = ToolRunner.run(conf1, new PointInTimeRestoreDriver(), args);
    assertNotEquals(
      "Restore should fail since the requested restore point is before the start of continuous backup",
      0, ret);
  }

  /**
   * Verifies that PITR successfully restores data for a single table.
   */
  @Test
  public void testPointInTimeRestore_SuccessfulRestoreForOneTable() throws Exception {
    TableName restoredTable = TableName.valueOf("restoredTable");

    // Perform restore operation
    String[] args = buildPITRArgs(new TableName[] { table1 }, new TableName[] { restoredTable },
      EnvironmentEdgeManager.currentTime() - 5 * ONE_DAY_IN_MILLISECONDS);

    int ret = ToolRunner.run(conf1, new PointInTimeRestoreDriver(), args);
    assertEquals("Restore should succeed", 0, ret);

    // Validate that the restored table contains the same number of rows as the original table
    assertEquals("Restored table should have the same row count as the original",
      getRowCount(table1), getRowCount(restoredTable));
  }

  /**
   * Verifies that PITR successfully restores multiple tables at once.
   */
  @Test
  public void testPointInTimeRestore_SuccessfulRestoreForMultipleTables() throws Exception {
    TableName restoredTable1 = TableName.valueOf("restoredTable1");
    TableName restoredTable2 = TableName.valueOf("restoredTable2");

    // Perform restore operation for multiple tables
    String[] args = buildPITRArgs(new TableName[] { table1, table2 },
      new TableName[] { restoredTable1, restoredTable2 },
      EnvironmentEdgeManager.currentTime() - 5 * ONE_DAY_IN_MILLISECONDS);

    int ret = ToolRunner.run(conf1, new PointInTimeRestoreDriver(), args);
    assertEquals("Restore should succeed", 0, ret);

    // Validate that the restored tables contain the same number of rows as the originals
    assertEquals("Restored table1 should have the same row count as the original",
      getRowCount(table1), getRowCount(restoredTable1));
    assertEquals("Restored table2 should have the same row count as the original",
      getRowCount(table2), getRowCount(restoredTable2));
  }

  private String[] buildPITRArgs(TableName[] sourceTables, TableName[] targetTables, long endTime) {
    String sourceTableNames =
      Arrays.stream(sourceTables).map(TableName::getNameAsString).collect(Collectors.joining(","));

    String targetTableNames =
      Arrays.stream(targetTables).map(TableName::getNameAsString).collect(Collectors.joining(","));

    return new String[] { "-" + OPTION_TABLE, sourceTableNames, "-" + OPTION_TABLE_MAPPING,
      targetTableNames, "-" + OPTION_TO_DATETIME, String.valueOf(endTime) };
  }

  private static String[] buildBackupArgs(String backupType, TableName[] tables,
    boolean continuousEnabled) {
    String tableNames =
      Arrays.stream(tables).map(TableName::getNameAsString).collect(Collectors.joining(","));

    if (continuousEnabled) {
      return new String[] { "create", backupType, BACKUP_ROOT_DIR, "-" + OPTION_TABLE, tableNames,
        "-" + OPTION_ENABLE_CONTINUOUS_BACKUP };
    } else {
      return new String[] { "create", backupType, BACKUP_ROOT_DIR, "-" + OPTION_TABLE, tableNames };
    }
  }

  private static void loadRandomData(TableName tableName, int totalRows) throws IOException {
    int rowSize = 32;
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      TEST_UTIL.loadRandomRows(table, famName, rowSize, totalRows);
    }
  }

  private static void waitForReplication() {
    LOG.info("Waiting for replication to complete for {} ms", WAIT_FOR_REPLICATION_MS);
    try {
      Thread.sleep(WAIT_FOR_REPLICATION_MS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Thread was interrupted while waiting", e);
    }
  }

  private int getRowCount(TableName tableName) throws IOException {
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      return HBaseTestingUtil.countRows(table);
    }
  }
}
