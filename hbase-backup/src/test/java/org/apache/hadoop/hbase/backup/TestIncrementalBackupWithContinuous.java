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

import static org.apache.hadoop.hbase.replication.regionserver.ReplicationMarkerChore.REPLICATION_MARKER_ENABLED_DEFAULT;
import static org.apache.hadoop.hbase.replication.regionserver.ReplicationMarkerChore.REPLICATION_MARKER_ENABLED_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupAdminImpl;
import org.apache.hadoop.hbase.backup.impl.BackupManifest;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.HFileTestUtil;
import org.apache.hadoop.util.ToolRunner;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

@Category(LargeTests.class)
public class TestIncrementalBackupWithContinuous extends TestContinuousBackup {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestIncrementalBackupWithContinuous.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestIncrementalBackupWithContinuous.class);

  private byte[] ROW = Bytes.toBytes("row1");
  private final byte[] COLUMN = Bytes.toBytes("col");
  private static final int ROWS_IN_BULK_LOAD = 100;

  @Test
  public void testContinuousBackupWithIncrementalBackupSuccess() throws Exception {
    LOG.info("Testing incremental backup with continuous backup");
    conf1.setBoolean(REPLICATION_MARKER_ENABLED_KEY, true);
    String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
    TableName tableName = TableName.valueOf("table_" + methodName);
    Table t1 = TEST_UTIL.createTable(tableName, famName);

    try (BackupSystemTable table = new BackupSystemTable(TEST_UTIL.getConnection())) {
      int before = table.getBackupHistory().size();

      // Run continuous backup
      String[] args = buildBackupArgs("full", new TableName[] { tableName }, true);
      int ret = ToolRunner.run(conf1, new BackupDriver(), args);
      assertEquals("Full Backup should succeed", 0, ret);

      // Verify backup history increased and all the backups are succeeded
      LOG.info("Verify backup history increased and all the backups are succeeded");
      List<BackupInfo> backups = table.getBackupHistory();
      assertEquals("Backup history should increase", before + 1, backups.size());
      for (BackupInfo data : List.of(backups.get(0))) {
        String backupId = data.getBackupId();
        assertTrue(checkSucceeded(backupId));
      }

      // Verify backup manifest contains the correct tables
      LOG.info("Verify backup manifest contains the correct tables");
      BackupManifest manifest = getLatestBackupManifest(backups);
      assertEquals("Backup should contain the expected tables", Sets.newHashSet(tableName),
        new HashSet<>(manifest.getTableList()));

      loadTable(t1);
      Thread.sleep(10000);

      // Run incremental backup
      LOG.info("Run incremental backup now");
      before = table.getBackupHistory().size();
      args = buildBackupArgs("incremental", new TableName[] { tableName }, false);
      ret = ToolRunner.run(conf1, new BackupDriver(), args);
      assertEquals("Incremental Backup should succeed", 0, ret);
      LOG.info("Incremental backup completed");

      // Verify backup history increased and all the backups are succeeded
      backups = table.getBackupHistory();
      String incrementalBackupid = null;
      assertEquals("Backup history should increase", before + 1, backups.size());
      for (BackupInfo data : List.of(backups.get(0))) {
        String backupId = data.getBackupId();
        incrementalBackupid = backupId;
        assertTrue(checkSucceeded(backupId));
      }

      TEST_UTIL.truncateTable(tableName);
      // Restore incremental backup
      TableName[] tables = new TableName[] { tableName };
      BackupAdminImpl client = new BackupAdminImpl(TEST_UTIL.getConnection());
      client.restore(BackupUtils.createRestoreRequest(BACKUP_ROOT_DIR, incrementalBackupid, false,
        tables, tables, true));

      assertEquals(NB_ROWS_IN_BATCH, TEST_UTIL.countRows(tableName));
    } finally {
      conf1.setBoolean(REPLICATION_MARKER_ENABLED_KEY, REPLICATION_MARKER_ENABLED_DEFAULT);
    }
  }

  @Test
  public void testIncrementalBackupCopyingBulkloadTillIncrCommittedWalTs() throws Exception {
    conf1.setBoolean(REPLICATION_MARKER_ENABLED_KEY, true);
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
      assertEquals(1, systemTable.readBulkloadRows(List.of(tableName1)).size());
      loadTable(TEST_UTIL.getConnection().getTable(tableName1));
      Thread.sleep(10000);

      performBulkLoad("bulkPostIncr", methodName, tableName1);
      assertEquals(2, systemTable.readBulkloadRows(List.of(tableName1)).size());

      // Incremental backup
      String backup2 =
        backupTables(BackupType.INCREMENTAL, List.of(tableName1), BACKUP_ROOT_DIR, true);
      assertTrue(checkSucceeded(backup2));

      // bulkPostIncr Bulkload entry should not be deleted post incremental backup
      assertEquals(1, systemTable.readBulkloadRows(List.of(tableName1)).size());

      TEST_UTIL.truncateTable(tableName1);
      // Restore incremental backup
      TableName[] tables = new TableName[] { tableName1 };
      BackupAdminImpl client = new BackupAdminImpl(TEST_UTIL.getConnection());
      client.restore(
        BackupUtils.createRestoreRequest(BACKUP_ROOT_DIR, backup2, false, tables, tables, true));
      assertEquals(expectedRowCount, TEST_UTIL.countRows(tableName1));
    } finally {
      conf1.setBoolean(REPLICATION_MARKER_ENABLED_KEY, REPLICATION_MARKER_ENABLED_DEFAULT);
    }
  }

  private void verifyTable(Table t1) throws IOException {
    Get g = new Get(ROW);
    Result r = t1.get(g);
    assertEquals(1, r.size());
    assertTrue(CellUtil.matchingQualifier(r.rawCells()[0], COLUMN));
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
}
