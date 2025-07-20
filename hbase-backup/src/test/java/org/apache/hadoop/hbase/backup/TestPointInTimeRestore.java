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
import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupReplicationEndpoint.ONE_DAY_IN_MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestPointInTimeRestore extends TestBackupBase {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestPointInTimeRestore.class);

  private static final String backupWalDirName = "TestPointInTimeRestoreWalDir";
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
    PITRTestUtil.loadRandomData(TEST_UTIL, table1, famName, 1000); // Insert initial data into
    // table1

    // Perform a full backup for table1 with continuous backup enabled
    String[] args =
      PITRTestUtil.buildBackupArgs("full", new TableName[] { table1 }, true, BACKUP_ROOT_DIR);
    int ret = ToolRunner.run(conf1, new BackupDriver(), args);
    assertEquals("Backup should succeed", 0, ret);

    // Move time forward to simulate 15 days ago
    EnvironmentEdgeManager
      .injectEdge(() -> System.currentTimeMillis() - 15 * ONE_DAY_IN_MILLISECONDS);
    PITRTestUtil.loadRandomData(TEST_UTIL, table1, famName, 1000); // Add more data to table1
    PITRTestUtil.loadRandomData(TEST_UTIL, table2, famName, 500); // Insert data into table2

    PITRTestUtil.waitForReplication(); // Ensure replication is complete

    // Perform a full backup for table2 with continuous backup enabled
    args = PITRTestUtil.buildBackupArgs("full", new TableName[] { table2 }, true, BACKUP_ROOT_DIR);
    ret = ToolRunner.run(conf1, new BackupDriver(), args);
    assertEquals("Backup should succeed", 0, ret);

    // Move time forward to simulate 10 days ago
    EnvironmentEdgeManager
      .injectEdge(() -> System.currentTimeMillis() - 10 * ONE_DAY_IN_MILLISECONDS);
    PITRTestUtil.loadRandomData(TEST_UTIL, table2, famName, 500); // Add more data to table2
    PITRTestUtil.loadRandomData(TEST_UTIL, table3, famName, 500); // Insert data into table3

    // Perform a full backup for table3 and table4 (without continuous backup)
    args = PITRTestUtil.buildBackupArgs("full", new TableName[] { table3, table4 }, false,
      BACKUP_ROOT_DIR);
    ret = ToolRunner.run(conf1, new BackupDriver(), args);
    assertEquals("Backup should succeed", 0, ret);

    PITRTestUtil.waitForReplication(); // Ensure replication is complete before concluding setup

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
    String[] args = PITRTestUtil.buildPITRArgs(new TableName[] { table1 },
      new TableName[] { TableName.valueOf("restoredTable1") },
      EnvironmentEdgeManager.currentTime() + ONE_DAY_IN_MILLISECONDS, null);

    int ret = ToolRunner.run(conf1, new PointInTimeRestoreDriver(), args);
    assertNotEquals("Restore should fail since the requested restore time is in the future", 0,
      ret);

    // Case 2: Requested restore time is too old (beyond the retention window, should fail)
    args = PITRTestUtil.buildPITRArgs(new TableName[] { table1 },
      new TableName[] { TableName.valueOf("restoredTable1") },
      EnvironmentEdgeManager.currentTime() - 40 * ONE_DAY_IN_MILLISECONDS, null);

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
    String[] args = PITRTestUtil.buildPITRArgs(new TableName[] { table3 },
      new TableName[] { TableName.valueOf("restoredTable1") },
      EnvironmentEdgeManager.currentTime() - 10 * ONE_DAY_IN_MILLISECONDS, null);

    int ret = ToolRunner.run(conf1, new PointInTimeRestoreDriver(), args);
    assertNotEquals("Restore should fail since continuous backup is not enabled for the table", 0,
      ret);
  }

  /**
   * Ensures that PITR fails when trying to restore from a point before continuous backup started.
   */
  @Test
  public void testPointInTimeRestore_TablesWithNoProperBackupOrWals() throws Exception {
    String[] args = PITRTestUtil.buildPITRArgs(new TableName[] { table2 },
      new TableName[] { TableName.valueOf("restoredTable1") },
      EnvironmentEdgeManager.currentTime() - 16 * ONE_DAY_IN_MILLISECONDS, null);

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
    String[] args =
      PITRTestUtil.buildPITRArgs(new TableName[] { table1 }, new TableName[] { restoredTable },
        EnvironmentEdgeManager.currentTime() - 5 * ONE_DAY_IN_MILLISECONDS, null);

    int ret = ToolRunner.run(conf1, new PointInTimeRestoreDriver(), args);
    assertEquals("Restore should succeed", 0, ret);

    // Validate that the restored table contains the same number of rows as the original table
    assertEquals("Restored table should have the same row count as the original",
      PITRTestUtil.getRowCount(TEST_UTIL, table1),
      PITRTestUtil.getRowCount(TEST_UTIL, restoredTable));
  }

  /**
   * Verifies that PITR successfully restores multiple tables at once.
   */
  @Test
  public void testPointInTimeRestore_SuccessfulRestoreForMultipleTables() throws Exception {
    TableName restoredTable1 = TableName.valueOf("restoredTable1");
    TableName restoredTable2 = TableName.valueOf("restoredTable2");

    // Perform restore operation for multiple tables
    String[] args = PITRTestUtil.buildPITRArgs(new TableName[] { table1, table2 },
      new TableName[] { restoredTable1, restoredTable2 },
      EnvironmentEdgeManager.currentTime() - 5 * ONE_DAY_IN_MILLISECONDS, null);

    int ret = ToolRunner.run(conf1, new PointInTimeRestoreDriver(), args);
    assertEquals("Restore should succeed", 0, ret);

    // Validate that the restored tables contain the same number of rows as the originals
    assertEquals("Restored table1 should have the same row count as the original",
      PITRTestUtil.getRowCount(TEST_UTIL, table1),
      PITRTestUtil.getRowCount(TEST_UTIL, restoredTable1));
    assertEquals("Restored table2 should have the same row count as the original",
      PITRTestUtil.getRowCount(TEST_UTIL, table2),
      PITRTestUtil.getRowCount(TEST_UTIL, restoredTable2));
  }
}
