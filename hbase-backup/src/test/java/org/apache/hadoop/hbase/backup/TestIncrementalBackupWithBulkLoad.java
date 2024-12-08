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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.backup.impl.BulkLoad;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HFileTestUtil;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * This test checks whether backups properly track & manage bulk files loads.
 */
@Category(LargeTests.class)
public class TestIncrementalBackupWithBulkLoad extends TestBackupBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestIncrementalBackupWithBulkLoad.class);

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
