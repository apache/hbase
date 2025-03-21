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

import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.*;
import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupReplicationEndpoint.CONF_STAGED_WAL_FLUSH_INITIAL_DELAY;
import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupReplicationEndpoint.CONF_STAGED_WAL_FLUSH_INTERVAL;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupAdminImpl;
import org.apache.hadoop.hbase.backup.impl.BackupManifest;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
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
public class TestIncrementalBackupWithContinuous extends TestContinuousBackup {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestIncrementalBackupWithContinuous.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestIncrementalBackupWithContinuous.class);

  private byte[] ROW = Bytes.toBytes("row1");
  private final byte[] FAMILY = Bytes.toBytes("family");
  private final byte[] COLUMN = Bytes.toBytes("col");
  String backupWalDirName = "TestContinuousBackupWalDir";

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL = new HBaseTestingUtil();
    conf1 = TEST_UTIL.getConfiguration();
    autoRestoreOnFailure = true;
    useSecondCluster = false;
    conf1.setInt(CONF_STAGED_WAL_FLUSH_INTERVAL, 1);
    conf1.setInt(CONF_STAGED_WAL_FLUSH_INITIAL_DELAY, 1);
    setUpHelper();
  }

  @Before
  public void beforeTest() throws IOException {
    super.beforeTest();
  }

  @After
  public void afterTest() throws IOException {
    super.afterTest();
  }

  @Test
  public void testContinuousBackupWithIncrementalBackupSuccess() throws Exception {
    LOG.info("Testing incremental backup with continuous backup");
    String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
    TableName tableName = TableName.valueOf("table_" + methodName);
    Table t1 = TEST_UTIL.createTable(tableName, FAMILY);

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

      Put p = new Put(ROW);
      p.addColumn(FAMILY, COLUMN, COLUMN);
      t1.put(p);
      // Thread.sleep(5000);

      // Run incremental backup
      LOG.info("Run incremental backup now");
      before = table.getBackupHistory().size();
      args = buildBackupArgs("incremental", new TableName[] { tableName }, false);
      ret = ToolRunner.run(conf1, new BackupDriver(), args);
      assertEquals("Incremental Backup should succeed", 0, ret);

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

      verifyTable(t1);
    }
  }

  private void verifyTable(Table t1) throws IOException {
    Get g = new Get(ROW);
    Result r = t1.get(g);
    assertEquals(1, r.size());
    assertTrue(CellUtil.matchingQualifier(r.rawCells()[0], COLUMN));
  }
}
