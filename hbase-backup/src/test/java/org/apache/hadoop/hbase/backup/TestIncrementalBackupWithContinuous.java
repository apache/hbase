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
import static org.apache.hadoop.hbase.replication.regionserver.ReplicationMarkerChore.REPLICATION_MARKER_ENABLED_DEFAULT;
import static org.apache.hadoop.hbase.replication.regionserver.ReplicationMarkerChore.REPLICATION_MARKER_ENABLED_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupAdminImpl;
import org.apache.hadoop.hbase.backup.impl.BackupManifest;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.HFileTestUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

@Tag(LargeTests.TAG)
public class TestIncrementalBackupWithContinuous extends TestBackupBase {

  private static final Logger LOG =
    LoggerFactory.getLogger(TestIncrementalBackupWithContinuous.class);

  private static final int ROWS_IN_BULK_LOAD = 100;
  private static final String backupWalDirName = "TestContinuousBackupWalDir";

  private FileSystem fs;

  @BeforeEach
  public void beforeTest() throws IOException {
    Path root = TEST_UTIL.getDataTestDirOnTestFS();
    Path backupWalDir = new Path(root, backupWalDirName);
    conf1.set(CONF_CONTINUOUS_BACKUP_WAL_DIR, backupWalDir.toString());
    conf1.setBoolean(REPLICATION_MARKER_ENABLED_KEY, true);
    fs = FileSystem.get(conf1);
  }

  @AfterEach
  public void afterTest() throws IOException {
    Path root = TEST_UTIL.getDataTestDirOnTestFS();
    Path backupWalDir = new Path(root, backupWalDirName);
    if (fs.exists(backupWalDir)) {
      fs.delete(backupWalDir, true);
    }
    conf1.unset(CONF_CONTINUOUS_BACKUP_WAL_DIR);
    conf1.setBoolean(REPLICATION_MARKER_ENABLED_KEY, REPLICATION_MARKER_ENABLED_DEFAULT);
    deleteContinuousBackupReplicationPeerIfExists(TEST_UTIL.getAdmin());
  }

  @Test
  public void testContinuousBackupWithIncrementalBackupSuccess() throws Exception {
    String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
    TableName tableName = TableName.valueOf("table_" + methodName);
    Table t1 = TEST_UTIL.createTable(tableName, famName);

    try (BackupSystemTable backupSystemTable = new BackupSystemTable(TEST_UTIL.getConnection())) {
      int before = backupSystemTable.getBackupHistory().size();

      // Run continuous backup
      LOG.info("Running full backup with continuous backup enabled on table: {}", tableName);
      String backup1 = backupTables(BackupType.FULL, List.of(tableName), BACKUP_ROOT_DIR, true);
      LOG.info("Full backup complete with ID {} for table: {}", backup1, tableName);
      assertTrue(checkSucceeded(backup1));

      // Verify backup history increased and all the backups are succeeded
      LOG.info("Verify backup history increased and all the backups are succeeded");
      List<BackupInfo> backups = backupSystemTable.getBackupHistory();
      assertEquals(before + 1, backups.size(), "Backup history should increase");

      // Verify backup manifest contains the correct tables
      LOG.info("Verify backup manifest contains the correct tables");
      BackupManifest manifest = getLatestBackupManifest(backups);
      assertEquals(Sets.newHashSet(tableName), new HashSet<>(manifest.getTableList()),
        "Backup should contain the expected tables");

      loadTable(t1);
      Thread.sleep(10000);

      // Run incremental backup
      LOG.info("Run incremental backup now on table: {}", tableName);
      before = backupSystemTable.getBackupHistory().size();
      String backup2 =
        backupTables(BackupType.INCREMENTAL, List.of(tableName), BACKUP_ROOT_DIR, true);
      assertTrue(checkSucceeded(backup2));
      LOG.info("Incremental backup completed for table: {}", tableName);

      // Verify the temporary backup directory was deleted
      Path backupTmpDir = new Path(BACKUP_ROOT_DIR, ".tmp");
      Path bulkLoadOutputDir = new Path(backupTmpDir, backup2);
      assertFalse(fs.exists(bulkLoadOutputDir),
        "Bulk load output directory " + bulkLoadOutputDir + " should have been deleted");

      // Verify backup history increased and all the backups are succeeded
      backups = backupSystemTable.getBackupHistory();
      assertEquals(before + 1, backups.size(), "Backup history should increase");

      String originalTableChecksum = TEST_UTIL.checksumRows(t1);

      LOG.info("Truncating table: {}", tableName);
      TEST_UTIL.truncateTable(tableName);

      // Restore incremental backup
      TableName[] tables = new TableName[] { tableName };
      BackupAdminImpl client = new BackupAdminImpl(TEST_UTIL.getConnection());
      LOG.info("Restoring table: {}", tableName);
      // In the restore request, the original table is both the "from table" and the "to table".
      // This means the table is being restored "into itself". It is not being restored into
      // separate table.
      client.restore(
        BackupUtils.createRestoreRequest(BACKUP_ROOT_DIR, backup2, false, tables, tables, true));

      LOG.info("Verifying data integrity for restored table: {}", tableName);
      verifyRestoredTableDataIntegrity(tables[0], originalTableChecksum, NB_ROWS_IN_BATCH);
    }
  }

  @Test
  public void testMultiTableContinuousBackupWithIncrementalBackupSuccess() throws Exception {
    String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
    List<Table> tables = new ArrayList<>();
    List<TableName> tableNames = new ArrayList<>();
    tableNames.add(TableName.valueOf("table_" + methodName + "_0"));
    tableNames.add(TableName.valueOf("table_" + methodName + "_1"));
    tableNames.add(TableName.valueOf("ns1", "ns1_table_" + methodName + "_0"));
    tableNames.add(TableName.valueOf("ns1", "ns1_table_" + methodName + "_1"));
    tableNames.add(TableName.valueOf("sameTableNameDifferentNamespace"));
    tableNames.add(TableName.valueOf("ns3", "sameTableNameDifferentNamespace"));

    for (TableName table : tableNames) {
      LOG.info("Creating table: {}", table);
      tables.add(TEST_UTIL.createTable(table, famName));
    }

    try (BackupSystemTable backupSystemTable = new BackupSystemTable(TEST_UTIL.getConnection())) {
      int before = backupSystemTable.getBackupHistory().size();

      // Run continuous backup on multiple tables
      LOG.info("Running full backup with continuous backup enabled on tables: {}", tableNames);
      String backup1 = backupTables(BackupType.FULL, tableNames, BACKUP_ROOT_DIR, true);
      LOG.info("Full backup complete with ID {} for tables: {}", backup1, tableNames);
      assertTrue(checkSucceeded(backup1));

      // Verify backup history increased and all backups have succeeded
      LOG.info("Verify backup history increased and all backups have succeeded");
      List<BackupInfo> backups = backupSystemTable.getBackupHistory();
      assertEquals(before + 1, backups.size(), "Backup history should increase");

      // Verify backup manifest contains the correct tables
      LOG.info("Verify backup manifest contains the correct tables");
      BackupManifest manifest = getLatestBackupManifest(backups);
      assertEquals(Sets.newHashSet(tableNames), new HashSet<>(manifest.getTableList()),
        "Backup should contain the expected tables");

      loadTables(tables);
      Thread.sleep(10000);

      // Run incremental backup
      LOG.info("Running incremental backup on tables: {}", tableNames);
      before = backupSystemTable.getBackupHistory().size();
      String backup2 = backupTables(BackupType.INCREMENTAL, tableNames, BACKUP_ROOT_DIR, true);
      assertTrue(checkSucceeded(backup2));
      LOG.info("Incremental backup completed with ID {} for tables: {}", backup2, tableNames);

      // Verify backup history increased and all the backups are succeeded
      backups = backupSystemTable.getBackupHistory();
      assertEquals(before + 1, backups.size(), "Backup history should increase");

      // We need to get each table's original row checksum before truncating each table
      LinkedHashMap<TableName, String> originalTableChecksums = new LinkedHashMap<>();
      for (Table table : tables) {
        LOG.info("Getting row checksum for table: {}", table);
        originalTableChecksums.put(table.getName(), TEST_UTIL.checksumRows(table));
      }

      for (TableName tableName : tableNames) {
        LOG.info("Truncating table: {}", tableName);
        TEST_UTIL.truncateTable(tableName);
      }

      // Restore incremental backup
      TableName[] tableNamesArray = tableNames.toArray(new TableName[0]);
      BackupAdminImpl client = new BackupAdminImpl(TEST_UTIL.getConnection());
      LOG.info("Restoring tables: {}", tableNames);
      // In the restore request, the original tables are both the list of "from tables" and the
      // list of "to tables". This means the tables are being restored "into themselves". They are
      // not being restored into separate tables.
      client.restore(BackupUtils.createRestoreRequest(BACKUP_ROOT_DIR, backup2, false,
        tableNamesArray, tableNamesArray, true));

      for (TableName tableName : originalTableChecksums.keySet()) {
        LOG.info("Verifying data integrity for restored table: {}", tableName);
        verifyRestoredTableDataIntegrity(tableName, originalTableChecksums.get(tableName),
          NB_ROWS_IN_BATCH);
      }
    }
  }

  @Test
  public void testIncrementalBackupCopyingBulkloadTillIncrCommittedWalTs() throws Exception {
    String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
    TableName tableName1 = TableName.valueOf("table_" + methodName);
    TEST_UTIL.createTable(tableName1, famName);

    try (BackupSystemTable systemTable = new BackupSystemTable(TEST_UTIL.getConnection())) {
      // The test starts with no data, and no bulk loaded rows.
      int expectedRowCount = 0;
      assertEquals(expectedRowCount, TEST_UTIL.countRows(tableName1));
      assertTrue(systemTable.readBulkloadRows(List.of(tableName1)).isEmpty());

      // Create continuous backup, bulk loads are now being tracked
      String backup1 = backupTables(BackupType.FULL, List.of(tableName1), BACKUP_ROOT_DIR, true);
      assertTrue(checkSucceeded(backup1));

      loadTable(TEST_UTIL.getConnection().getTable(tableName1));
      expectedRowCount = expectedRowCount + NB_ROWS_IN_BATCH;
      performBulkLoad("bulkPreIncr", methodName, tableName1);
      expectedRowCount += ROWS_IN_BULK_LOAD;
      assertEquals(expectedRowCount, TEST_UTIL.countRows(tableName1));
      assertTrue(systemTable.readBulkloadRows(List.of(tableName1)).isEmpty());
      loadTable(TEST_UTIL.getConnection().getTable(tableName1));
      Thread.sleep(15000);

      performBulkLoad("bulkPostIncr", methodName, tableName1);
      assertTrue(systemTable.readBulkloadRows(List.of(tableName1)).isEmpty());

      // Incremental backup
      String backup2 =
        backupTables(BackupType.INCREMENTAL, List.of(tableName1), BACKUP_ROOT_DIR, true);
      assertTrue(checkSucceeded(backup2));
      assertTrue(systemTable.readBulkloadRows(List.of(tableName1)).isEmpty());

      TEST_UTIL.truncateTable(tableName1);
      // Restore incremental backup
      TableName[] tables = new TableName[] { tableName1 };
      BackupAdminImpl client = new BackupAdminImpl(TEST_UTIL.getConnection());
      client.restore(
        BackupUtils.createRestoreRequest(BACKUP_ROOT_DIR, backup2, false, tables, tables, true));
      assertEquals(expectedRowCount, TEST_UTIL.countRows(tableName1));
    }
  }

  private void performBulkLoad(String keyPrefix, String testDir, TableName tableName)
    throws IOException {
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path baseDirectory = TEST_UTIL.getDataTestDirOnTestFS(testDir);
    Path hfilePath =
      new Path(baseDirectory, Bytes.toString(famName) + Path.SEPARATOR + "hfile_" + keyPrefix);

    HFileTestUtil.createHFile(TEST_UTIL.getConfiguration(), fs, hfilePath, famName, qualName,
      Bytes.toBytes(keyPrefix), Bytes.toBytes(keyPrefix + "z"), ROWS_IN_BULK_LOAD);

    listFiles(fs, baseDirectory, baseDirectory);

    Map<BulkLoadHFiles.LoadQueueItem, ByteBuffer> result =
      BulkLoadHFiles.create(TEST_UTIL.getConfiguration()).bulkLoad(tableName, baseDirectory);
    assertFalse(result.isEmpty());
  }

  private static Set<String> listFiles(final FileSystem fs, final Path root, final Path dir)
    throws IOException {
    Set<String> files = new HashSet<>();
    FileStatus[] list = CommonFSUtils.listStatus(fs, dir);
    if (list != null) {
      for (FileStatus fstat : list) {
        if (fstat.isDirectory()) {
          LOG.info("Found directory {}", Objects.toString(fstat.getPath()));
          files.addAll(listFiles(fs, root, fstat.getPath()));
        } else {
          LOG.info("Found file {}", Objects.toString(fstat.getPath()));
          String file = fstat.getPath().makeQualified(fs).toString();
          files.add(file);
        }
      }
    }
    return files;
  }

  protected static void loadTable(Table table) throws Exception {
    Put p; // 100 + 1 row to t1_syncup
    for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
      p = new Put(Bytes.toBytes("rowLoad" + i));
      p.addColumn(famName, qualName, Bytes.toBytes("val" + i));
      table.put(p);
    }
  }

  protected static void loadTables(List<Table> tables) throws Exception {
    for (Table table : tables) {
      LOG.info("Loading data into table: {}", table);
      loadTable(table);
    }
  }

  private void verifyRestoredTableDataIntegrity(TableName restoredTableName,
    String originalTableChecksum, int expectedRowCount) throws Exception {
    try (Table restoredTable = TEST_UTIL.getConnection().getTable(restoredTableName);
      ResultScanner scanner = restoredTable.getScanner(new Scan())) {

      // Verify the checksum for the original table (before it was truncated) matches the checksum
      // of the restored table.
      String restoredTableChecksum = TEST_UTIL.checksumRows(restoredTable);
      assertEquals(originalTableChecksum, restoredTableChecksum,
        "The restored table's row checksum did not match the original table's checksum");

      // Verify the data in the restored table is the same as when it was originally loaded
      // into the table.
      int count = 0;
      for (Result result : scanner) {
        // The data has a numerical match between its row key and value (such as rowLoad1 and
        // value1)
        // We can use this to ensure a row key has the expected value.
        String rowKey = Bytes.toString(result.getRow());
        int index = Integer.parseInt(rowKey.replace("rowLoad", ""));

        // Verify the Value
        byte[] actualValue = result.getValue(famName, qualName);
        assertNotNull(actualValue, "Value missing for row key: " + rowKey);
        String expectedValue = "val" + index;
        assertEquals(expectedValue, Bytes.toString(actualValue),
          "Value mismatch for row key: " + rowKey);

        count++;
      }
      assertEquals(expectedRowCount, count);
    }
  }
}
