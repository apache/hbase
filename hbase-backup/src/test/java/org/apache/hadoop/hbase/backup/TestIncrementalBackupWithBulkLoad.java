/**
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

import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupAdminImpl;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.tool.TestBulkLoadHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * 1. Create table t1
 * 2. Load data to t1
 * 3 Full backup t1
 * 4 Load data to t1
 * 5 bulk load into t1
 * 6 Incremental backup t1
 */
@Category(LargeTests.class)
public class TestIncrementalBackupWithBulkLoad extends TestBackupBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestIncrementalBackupWithBulkLoad.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestIncrementalBackupDeleteTable.class);

  // implement all test cases in 1 test since incremental backup/restore has dependencies
  @Test
  public void TestIncBackupDeleteTable() throws Exception {
    String testName = "TestIncBackupDeleteTable";
    // #1 - create full backup for all tables
    LOG.info("create full backup image for all tables");

    List<TableName> tables = Lists.newArrayList(table1);
    Connection conn = ConnectionFactory.createConnection(conf1);
    HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
    BackupAdminImpl client = new BackupAdminImpl(conn);

    BackupRequest request = createBackupRequest(BackupType.FULL, tables, BACKUP_ROOT_DIR);
    String backupIdFull = client.backupTables(request);

    assertTrue(checkSucceeded(backupIdFull));

    // #2 - insert some data to table table1
    HTable t1 = (HTable) conn.getTable(table1);
    Put p1;
    for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
      p1 = new Put(Bytes.toBytes("row-t1" + i));
      p1.addColumn(famName, qualName, Bytes.toBytes("val" + i));
      t1.put(p1);
    }

    Assert.assertEquals(TEST_UTIL.countRows(t1), NB_ROWS_IN_BATCH * 2);
    t1.close();

    int NB_ROWS2 = 20;
    LOG.debug("bulk loading into " + testName);
    int actual = TestBulkLoadHFiles.loadHFiles(testName, table1Desc, TEST_UTIL, famName,
        qualName, false, null, new byte[][][] {
          new byte[][]{ Bytes.toBytes("aaaa"), Bytes.toBytes("cccc") },
          new byte[][]{ Bytes.toBytes("ddd"), Bytes.toBytes("ooo") },
        }, true, false, true, NB_ROWS_IN_BATCH*2, NB_ROWS2);

    // #3 - incremental backup for table1
    tables = Lists.newArrayList(table1);
    request = createBackupRequest(BackupType.INCREMENTAL, tables, BACKUP_ROOT_DIR);
    String backupIdIncMultiple = client.backupTables(request);
    assertTrue(checkSucceeded(backupIdIncMultiple));
    // #4 bulk load again
    LOG.debug("bulk loading into " + testName);
    int actual1 = TestBulkLoadHFiles.loadHFiles(testName, table1Desc, TEST_UTIL, famName,
      qualName, false, null,
      new byte[][][] { new byte[][] { Bytes.toBytes("ppp"), Bytes.toBytes("qqq") },
        new byte[][] { Bytes.toBytes("rrr"), Bytes.toBytes("sss") }, },
      true, false, true, NB_ROWS_IN_BATCH * 2 + actual, NB_ROWS2);

    // #5 - incremental backup for table1
    tables = Lists.newArrayList(table1);
    request = createBackupRequest(BackupType.INCREMENTAL, tables, BACKUP_ROOT_DIR);
    String backupIdIncMultiple1 = client.backupTables(request);
    assertTrue(checkSucceeded(backupIdIncMultiple1));
    // Delete all data in table1
    TEST_UTIL.deleteTableData(table1);
    // #5.1 - check tables for full restore */
    HBaseAdmin hAdmin = TEST_UTIL.getHBaseAdmin();

    // #6 - restore incremental backup for table1
    TableName[] tablesRestoreIncMultiple = new TableName[] { table1 };
    //TableName[] tablesMapIncMultiple = new TableName[] { table1_restore };
    client.restore(BackupUtils.createRestoreRequest(BACKUP_ROOT_DIR, backupIdIncMultiple1,
      false, tablesRestoreIncMultiple, tablesRestoreIncMultiple, true));

    HTable hTable = (HTable) conn.getTable(table1);
    Assert.assertEquals(TEST_UTIL.countRows(hTable), NB_ROWS_IN_BATCH * 2 + actual + actual1);
    request = createBackupRequest(BackupType.FULL, tables, BACKUP_ROOT_DIR);

    backupIdFull = client.backupTables(request);
    try (final BackupSystemTable table = new BackupSystemTable(conn)) {
      Pair<Map<TableName, Map<String, Map<String, List<Pair<String, Boolean>>>>>, List<byte[]>> pair
        = table.readBulkloadRows(tables);
      assertTrue("map still has " + pair.getSecond().size() + " entries",
          pair.getSecond().isEmpty());
    }
    assertTrue(checkSucceeded(backupIdFull));

    hTable.close();
    admin.close();
    conn.close();
  }
}
