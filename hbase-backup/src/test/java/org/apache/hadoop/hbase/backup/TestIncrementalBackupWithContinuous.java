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

import static org.apache.hadoop.hbase.HConstants.REPLICATION_BULKLOAD_ENABLE_KEY;
import static org.apache.hadoop.hbase.HConstants.REPLICATION_CLUSTER_ID;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.CONF_CONTINUOUS_BACKUP_WAL_DIR;
import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupReplicationEndpoint.CONF_BACKUP_ROOT_DIR;
import static org.apache.hadoop.hbase.replication.regionserver.ReplicationMarkerChore.REPLICATION_MARKER_ENABLED_DEFAULT;
import static org.apache.hadoop.hbase.replication.regionserver.ReplicationMarkerChore.REPLICATION_MARKER_ENABLED_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupAdminImpl;
import org.apache.hadoop.hbase.backup.impl.BackupManifest;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.hbase.tool.BulkLoadHFilesTool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.HFileTestUtil;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

@Category(LargeTests.class)
public class TestIncrementalBackupWithContinuous extends TestBackupBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestIncrementalBackupWithContinuous.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestIncrementalBackupWithContinuous.class);

  private static final int ROWS_IN_BULK_LOAD = 100;
  private String backupWalDirName = "TestContinuousBackupWalDir";

  /*
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Set the configuration properties as required
    //conf1.setBoolean(REPLICATION_BULKLOAD_ENABLE_KEY, true);
    //conf1.set(REPLICATION_CLUSTER_ID, "clusterId1");

    // TEST_UTIL.startMiniZKCluster();
    // TEST_UTIL.startMiniCluster(3);
  }
   */

  @Before
  public void beforeTest() throws IOException {
    Path root = TEST_UTIL.getDataTestDirOnTestFS();
    Path backupWalDir = new Path(root, backupWalDirName);
    FileSystem fs = FileSystem.get(conf1);
    fs.mkdirs(backupWalDir);
    conf1.set(CONF_CONTINUOUS_BACKUP_WAL_DIR, backupWalDir.toString());
    conf1.setBoolean(REPLICATION_BULKLOAD_ENABLE_KEY, true);
    conf1.set(REPLICATION_CLUSTER_ID, "clusterId1");
    conf1.setBoolean(BulkLoadHFilesTool.BULK_LOAD_HFILES_BY_FAMILY, true);
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
    // deleteContinuousBackupReplicationPeerIfExists(TEST_UTIL.getAdmin());
  }

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
      String backup1 = backupTables(BackupType.FULL, List.of(tableName), BACKUP_ROOT_DIR, true);
      assertTrue(checkSucceeded(backup1));

      // Verify backup history increased and all the backups are succeeded
      LOG.info("Verify backup history increased and all the backups are succeeded");
      List<BackupInfo> backups = table.getBackupHistory();
      assertEquals("Backup history should increase", before + 1, backups.size());

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
      String backup2 =
        backupTables(BackupType.INCREMENTAL, List.of(tableName), BACKUP_ROOT_DIR, true);
      assertTrue(checkSucceeded(backup2));
      LOG.info("Incremental backup completed");

      // Verify backup history increased and all the backups are succeeded
      backups = table.getBackupHistory();
      assertEquals("Backup history should increase", before + 1, backups.size());

      TEST_UTIL.truncateTable(tableName);

      // Restore incremental backup
      TableName[] tables = new TableName[] { tableName };
      BackupAdminImpl client = new BackupAdminImpl(TEST_UTIL.getConnection());
      client.restore(
        BackupUtils.createRestoreRequest(BACKUP_ROOT_DIR, backup2, false, tables, tables, true));

      assertEquals(NB_ROWS_IN_BATCH, TEST_UTIL.countRows(tableName));
    } finally {
      conf1.setBoolean(REPLICATION_MARKER_ENABLED_KEY, REPLICATION_MARKER_ENABLED_DEFAULT);
    }
  }

  @Test
  public void testIncrementalBackupCopyingBulkloadTillIncrCommittedWalTs() throws Exception {
    conf1.setBoolean(REPLICATION_MARKER_ENABLED_KEY, true);
    // conf1.set(CONF_BACKUP_ROOT_DIR, BACKUP_ROOT_DIR);
    String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
    TableName tableName1 = TableName.valueOf("table_" + methodName);
    //String peerId = "peerId";
    TEST_UTIL.createTable(tableName1, famName);
    //Path backupRootDir = new Path(BACKUP_ROOT_DIR, methodName);
    //TEST_UTIL.getTestFileSystem().mkdirs(backupRootDir);
    //Path backupRootDir = new Path(TEST_UTIL.getDataTestDirOnTestFS(), methodName);
    //conf1.set(CONF_BACKUP_ROOT_DIR, backupRootDir);

    //Map<TableName, List<String>> tableMap = new HashMap<>();
    //tableMap.put(tableName1, new ArrayList<>());
    //addReplicationPeer(peerId, backupRootDir, tableMap, TEST_UTIL.getAdmin());

    try (BackupSystemTable systemTable = new BackupSystemTable(TEST_UTIL.getConnection())) {

      // The test starts with no data, and no bulk loaded rows.
      int expectedRowCount = 0;
      assertEquals(expectedRowCount, TEST_UTIL.countRows(tableName1));
      assertTrue(systemTable.readBulkloadRows(List.of(tableName1)).isEmpty());

      // Create continuous backup, bulk loads are now being tracked
      String backup1 = backupTables(BackupType.FULL, List.of(tableName1), BACKUP_ROOT_DIR, true);
      assertTrue(checkSucceeded(backup1));

      boolean rep = conf1.getBoolean(REPLICATION_BULKLOAD_ENABLE_KEY, false);
      String[] clusterid = conf1.getStrings(REPLICATION_CLUSTER_ID);
      loadTable(TEST_UTIL.getConnection().getTable(tableName1));
      expectedRowCount = expectedRowCount + NB_ROWS_IN_BATCH;
      performBulkLoad("bulkPreIncr", methodName, tableName1);
      expectedRowCount += ROWS_IN_BULK_LOAD;
      assertEquals(expectedRowCount, TEST_UTIL.countRows(tableName1));
      assertEquals(1, systemTable.readBulkloadRows(List.of(tableName1)).size());
      loadTable(TEST_UTIL.getConnection().getTable(tableName1));
      Thread.sleep(15000);

      performBulkLoad("bulkPostIncr", methodName, tableName1);
      assertEquals(2, systemTable.readBulkloadRows(List.of(tableName1)).size());

      // Incremental backup
      String backup2 =
        backupTables(BackupType.INCREMENTAL, List.of(tableName1), BACKUP_ROOT_DIR, true);
      assertTrue(checkSucceeded(backup2));

      // bulkPostIncr Bulkload entry should not be deleted post incremental backup
      // assertEquals(1, systemTable.readBulkloadRows(List.of(tableName1)).size());

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

  @Test
  public void testPitrFailureDueToMissingBackupPostBulkload() throws Exception {
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
      Thread.sleep(5000);

      // Incremental backup
      String backup2 =
        backupTables(BackupType.INCREMENTAL, List.of(tableName1), BACKUP_ROOT_DIR, true);
      assertTrue(checkSucceeded(backup2));
      assertEquals(0, systemTable.readBulkloadRows(List.of(tableName1)).size());

      performBulkLoad("bulkPostIncr", methodName, tableName1);
      assertEquals(1, systemTable.readBulkloadRows(List.of(tableName1)).size());

      loadTable(TEST_UTIL.getConnection().getTable(tableName1));
      Thread.sleep(10000);
      long restoreTs = BackupUtils.getReplicationCheckpoint(TEST_UTIL.getConnection());

      // expect restore failure due to no backup post bulkPostIncr bulkload
      TableName restoredTable = TableName.valueOf("restoredTable");
      String[] args = PITRTestUtil.buildPITRArgs(new TableName[] { tableName1 },
        new TableName[] { restoredTable }, restoreTs, null);
      int ret = ToolRunner.run(conf1, new PointInTimeRestoreDriver(), args);
      assertNotEquals("Restore should fail since there is one bulkload without any backup", 0, ret);
    } finally {
      conf1.setBoolean(REPLICATION_MARKER_ENABLED_KEY, REPLICATION_MARKER_ENABLED_DEFAULT);
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
}
