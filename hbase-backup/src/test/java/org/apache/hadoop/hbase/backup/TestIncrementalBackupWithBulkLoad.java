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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.backup.impl.BulkLoad;
import org.apache.hadoop.hbase.backup.impl.FullTableBackupClient;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.util.HFileTestUtil;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * This test checks whether backups properly track & manage bulk files loads.
 */
@Tag(LargeTests.TAG)
public class TestIncrementalBackupWithBulkLoad extends TestBackupBase {

  private static final String TEST_NAME = TestIncrementalBackupWithBulkLoad.class.getSimpleName();
  private static final int ROWS_IN_BULK_LOAD = 100;

  // implement all test cases in 1 test since incremental backup/restore has dependencies
  @Test
  public void TestIncBackupDeleteTable() throws Exception {
    try (BackupSystemTable systemTable = new BackupSystemTable(TEST_UTIL.getConnection())) {
      // The test starts with some data, and no bulk loaded rows.
      int expectedRowCount = NB_ROWS_IN_BATCH;
      assertEquals(expectedRowCount, TEST_UTIL.countRows(table1));
      assertTrue(systemTable.readBulkloadRows(List.of(table1)).isEmpty());

      // Bulk loads aren't tracked if the table isn't backed up yet
      performBulkLoad("bulk1");
      expectedRowCount += ROWS_IN_BULK_LOAD;
      assertEquals(expectedRowCount, TEST_UTIL.countRows(table1));
      assertEquals(0, systemTable.readBulkloadRows(List.of(table1)).size());

      // Create a backup, bulk loads are now being tracked
      String backup1 = backupTables(BackupType.FULL, List.of(table1), BACKUP_ROOT_DIR);
      assertTrue(checkSucceeded(backup1));
      performBulkLoad("bulk2");
      expectedRowCount += ROWS_IN_BULK_LOAD;
      assertEquals(expectedRowCount, TEST_UTIL.countRows(table1));
      assertEquals(1, systemTable.readBulkloadRows(List.of(table1)).size());

      // Truncating or deleting a table clears the tracked bulk loads (and all rows)
      TEST_UTIL.truncateTable(table1).close();
      expectedRowCount = 0;
      assertEquals(expectedRowCount, TEST_UTIL.countRows(table1));
      assertEquals(0, systemTable.readBulkloadRows(List.of(table1)).size());

      // Creating a full backup clears the bulk loads (since they are captured in the snapshot)
      performBulkLoad("bulk3");
      expectedRowCount = ROWS_IN_BULK_LOAD;
      assertEquals(expectedRowCount, TEST_UTIL.countRows(table1));
      assertEquals(1, systemTable.readBulkloadRows(List.of(table1)).size());
      String backup2 = backupTables(BackupType.FULL, List.of(table1), BACKUP_ROOT_DIR);
      assertTrue(checkSucceeded(backup2));
      assertEquals(expectedRowCount, TEST_UTIL.countRows(table1));
      assertEquals(0, systemTable.readBulkloadRows(List.of(table1)).size());

      // Creating an incremental backup clears the bulk loads
      performBulkLoad("bulk4");
      performBulkLoad("bulk5");
      performBulkLoad("bulk6");
      expectedRowCount += 3 * ROWS_IN_BULK_LOAD;
      assertEquals(expectedRowCount, TEST_UTIL.countRows(table1));
      assertEquals(3, systemTable.readBulkloadRows(List.of(table1)).size());
      String backup3 = backupTables(BackupType.INCREMENTAL, List.of(table1), BACKUP_ROOT_DIR);
      assertTrue(checkSucceeded(backup3));
      assertEquals(expectedRowCount, TEST_UTIL.countRows(table1));
      assertEquals(0, systemTable.readBulkloadRows(List.of(table1)).size());
      int rowCountAfterBackup3 = expectedRowCount;

      // Doing another bulk load, to check that this data will disappear after a restore operation
      performBulkLoad("bulk7");
      expectedRowCount += ROWS_IN_BULK_LOAD;
      assertEquals(expectedRowCount, TEST_UTIL.countRows(table1));
      List<BulkLoad> bulkloadsTemp = systemTable.readBulkloadRows(List.of(table1));
      assertEquals(1, bulkloadsTemp.size());
      BulkLoad bulk7 = bulkloadsTemp.get(0);

      // Doing a restore. Overwriting the table implies clearing the bulk loads,
      // but the loading of restored data involves loading bulk data, we expect 2 bulk loads
      // associated with backup 3 (loading of full backup, loading of incremental backup).
      BackupAdmin client = getBackupAdmin();
      client.restore(BackupUtils.createRestoreRequest(BACKUP_ROOT_DIR, backup3, false,
        new TableName[] { table1 }, new TableName[] { table1 }, true));
      assertEquals(rowCountAfterBackup3, TEST_UTIL.countRows(table1));
      List<BulkLoad> bulkLoads = systemTable.readBulkloadRows(List.of(table1));
      assertEquals(2, bulkLoads.size());
      assertFalse(bulkLoads.contains(bulk7));

      // Check that we have data of all expected bulk loads
      try (Table restoredTable = TEST_UTIL.getConnection().getTable(table1)) {
        assertFalse(containsRowWithKey(restoredTable, "bulk1"));
        assertFalse(containsRowWithKey(restoredTable, "bulk2"));
        assertTrue(containsRowWithKey(restoredTable, "bulk3"));
        assertTrue(containsRowWithKey(restoredTable, "bulk4"));
        assertTrue(containsRowWithKey(restoredTable, "bulk5"));
        assertTrue(containsRowWithKey(restoredTable, "bulk6"));
        assertFalse(containsRowWithKey(restoredTable, "bulk7"));
      }
    }
  }

  private boolean containsRowWithKey(Table table, String rowKey) throws IOException {
    byte[] data = Bytes.toBytes(rowKey);
    Get get = new Get(data);
    Result result = table.get(get);
    return result.containsColumn(famName, qualName);
  }

  @Test
  public void testUpdateFileListsRaceCondition() throws Exception {
    try (BackupSystemTable systemTable = new BackupSystemTable(TEST_UTIL.getConnection())) {
      // Test the race condition where files are archived during incremental backup
      FileSystem fs = TEST_UTIL.getTestFileSystem();

      String regionName = "region1";
      String columnFamily = "cf";
      String filename1 = "hfile1";
      String filename2 = "hfile2";

      Path rootDir = CommonFSUtils.getRootDir(TEST_UTIL.getConfiguration());
      Path tableDir = CommonFSUtils.getTableDir(rootDir, table1);
      Path activeFile1 =
        new Path(tableDir, regionName + Path.SEPARATOR + columnFamily + Path.SEPARATOR + filename1);
      Path activeFile2 =
        new Path(tableDir, regionName + Path.SEPARATOR + columnFamily + Path.SEPARATOR + filename2);

      fs.mkdirs(activeFile1.getParent());
      fs.create(activeFile1).close();
      fs.create(activeFile2).close();

      List<String> activeFiles = new ArrayList<>();
      activeFiles.add(activeFile1.toString());
      activeFiles.add(activeFile2.toString());
      List<String> archiveFiles = new ArrayList<>();

      Path archiveDir = HFileArchiveUtil.getStoreArchivePath(TEST_UTIL.getConfiguration(), table1,
        regionName, columnFamily);
      Path archivedFile1 = new Path(archiveDir, filename1);
      fs.mkdirs(archiveDir);
      assertTrue(fs.rename(activeFile1, archivedFile1), "File should be moved to archive");

      TestBackupBase.IncrementalTableBackupClientForTest client =
        new TestBackupBase.IncrementalTableBackupClientForTest(TEST_UTIL.getConnection(),
          "test_backup_id",
          createBackupRequest(BackupType.INCREMENTAL, List.of(table1), BACKUP_ROOT_DIR));

      client.updateFileLists(activeFiles, archiveFiles);

      assertEquals(1, activeFiles.size(), "Only one file should remain in active files");
      assertEquals(activeFile2.toString(), activeFiles.get(0),
        "File2 should still be in active files");
      assertEquals(1, archiveFiles.size(), "One file should be added to archive files");
      assertEquals(archivedFile1.toString(), archiveFiles.get(0),
        "Archived file should have correct path");
      systemTable.finishBackupExclusiveOperation();
    }

  }

  @Test
  public void testUpdateFileListsMissingArchivedFile() throws Exception {
    try (BackupSystemTable systemTable = new BackupSystemTable(TEST_UTIL.getConnection())) {
      // Test that IOException is thrown when file doesn't exist in archive location
      FileSystem fs = TEST_UTIL.getTestFileSystem();

      String regionName = "region2";
      String columnFamily = "cf";
      String filename = "missing_file";

      Path rootDir = CommonFSUtils.getRootDir(TEST_UTIL.getConfiguration());
      Path tableDir = CommonFSUtils.getTableDir(rootDir, table1);
      Path activeFile =
        new Path(tableDir, regionName + Path.SEPARATOR + columnFamily + Path.SEPARATOR + filename);

      fs.mkdirs(activeFile.getParent());
      fs.create(activeFile).close();

      List<String> activeFiles = new ArrayList<>();
      activeFiles.add(activeFile.toString());
      List<String> archiveFiles = new ArrayList<>();

      // Delete the file but don't create it in archive location
      fs.delete(activeFile, false);

      TestBackupBase.IncrementalTableBackupClientForTest client =
        new TestBackupBase.IncrementalTableBackupClientForTest(TEST_UTIL.getConnection(),
          "test_backup_id",
          createBackupRequest(BackupType.INCREMENTAL, List.of(table1), BACKUP_ROOT_DIR));

      // This should throw IOException since file doesn't exist in archive
      try {
        client.updateFileLists(activeFiles, archiveFiles);
        fail("Expected IOException to be thrown");
      } catch (IOException e) {
        // Expected
      }
      systemTable.finishBackupExclusiveOperation();
    }
  }

  @Test
  public void testFailedFullBackupKeepsTrackedBulkLoads() throws Exception {
    TableName tableName = TableName.valueOf("bulk-load-full-failure-" + System.nanoTime());
    TEST_UTIL.createTable(tableName, famName).close();

    try (BackupSystemTable systemTable = new BackupSystemTable(TEST_UTIL.getConnection())) {
      String backupId = backupTables(BackupType.FULL, List.of(tableName), BACKUP_ROOT_DIR);
      assertTrue(checkSucceeded(backupId));

      String keyPrefix = "full-backup-failure";
      FileSystem fs = TEST_UTIL.getTestFileSystem();
      Path baseDirectory = new Path(TEST_UTIL.getDataTestDirOnTestFS(TEST_NAME),
        tableName.getNameWithNamespaceInclAsString().replace(':', '_') + "_" + keyPrefix);
      Path hfilePath =
        new Path(baseDirectory, Bytes.toString(famName) + Path.SEPARATOR + "hfile_" + keyPrefix);

      HFileTestUtil.createHFile(TEST_UTIL.getConfiguration(), fs, hfilePath, famName, qualName,
        Bytes.toBytes(keyPrefix), Bytes.toBytes(keyPrefix + "z"), ROWS_IN_BULK_LOAD);

      Map<BulkLoadHFiles.LoadQueueItem, ByteBuffer> bulkLoadResult =
        BulkLoadHFiles.create(TEST_UTIL.getConfiguration()).bulkLoad(tableName, baseDirectory);
      assertFalse(bulkLoadResult.isEmpty());
      assertEquals(1, systemTable.readBulkloadRows(List.of(tableName)).size());

      BackupRequest request = createBackupRequest(BackupType.FULL, List.of(tableName),
        BACKUP_ROOT_DIR);
      long failingBackupTs = EnvironmentEdgeManager.currentTime();
      String failingBackupId = BackupRestoreConstants.BACKUPID_PREFIX + failingBackupTs;
      while (failingBackupId.equals(backupId)) {
        failingBackupId = BackupRestoreConstants.BACKUPID_PREFIX + (++failingBackupTs);
      }
      Path targetTableBackupDir =
        new Path(BackupUtils.getTableBackupDir(BACKUP_ROOT_DIR, failingBackupId, tableName));
      FileSystem.get(targetTableBackupDir.toUri(), conf1).mkdirs(targetTableBackupDir);

      try (Connection conn = ConnectionFactory.createConnection(conf1)) {
        FullTableBackupClient failingClient =
          new FullTableBackupClient(conn, failingBackupId, request) {
            @Override
            protected void completeBackup(final Connection conn, BackupInfo backupInfo,
              BackupType type, Configuration conf) throws IOException {
              throw new IOException("Injected failure before completing backup");
            }
          };

        assertThrows(IOException.class, failingClient::execute);
      }
      assertTrue(checkFailed(failingBackupId));
      assertEquals(1, systemTable.readBulkloadRows(List.of(tableName)).size());
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }

  private void performBulkLoad(String keyPrefix) throws IOException {
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path baseDirectory = TEST_UTIL.getDataTestDirOnTestFS(TEST_NAME);
    Path hfilePath =
      new Path(baseDirectory, Bytes.toString(famName) + Path.SEPARATOR + "hfile_" + keyPrefix);

    HFileTestUtil.createHFile(TEST_UTIL.getConfiguration(), fs, hfilePath, famName, qualName,
      Bytes.toBytes(keyPrefix), Bytes.toBytes(keyPrefix + "z"), ROWS_IN_BULK_LOAD);

    Map<BulkLoadHFiles.LoadQueueItem, ByteBuffer> result =
      BulkLoadHFiles.create(TEST_UTIL.getConfiguration()).bulkLoad(table1, baseDirectory);
    assertFalse(result.isEmpty());
  }
}
