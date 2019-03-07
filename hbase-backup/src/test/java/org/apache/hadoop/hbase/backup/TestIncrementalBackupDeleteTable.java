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
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupAdminImpl;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * 1. Create table t1, t2
 * 2. Load data to t1, t2
 * 3 Full backup t1, t2
 * 4 Delete t2
 * 5 Load data to t1
 * 6 Incremental backup t1
 */
@Category(LargeTests.class)
public class TestIncrementalBackupDeleteTable extends TestBackupBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestIncrementalBackupDeleteTable.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestIncrementalBackupDeleteTable.class);

  // implement all test cases in 1 test since incremental backup/restore has dependencies
  @Test
  public void testIncBackupDeleteTable() throws Exception {
    // #1 - create full backup for all tables
    LOG.info("create full backup image for all tables");

    List<TableName> tables = Lists.newArrayList(table1, table2);
    HBaseAdmin admin = null;
    Connection conn = ConnectionFactory.createConnection(conf1);
    admin = (HBaseAdmin) conn.getAdmin();
    BackupAdminImpl client = new BackupAdminImpl(conn);

    BackupRequest request = createBackupRequest(BackupType.FULL, tables, BACKUP_ROOT_DIR);
    String backupIdFull = client.backupTables(request);

    assertTrue(checkSucceeded(backupIdFull));

    // #2 - insert some data to table table1
    Table t1 = conn.getTable(table1);
    Put p1;
    for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
      p1 = new Put(Bytes.toBytes("row-t1" + i));
      p1.addColumn(famName, qualName, Bytes.toBytes("val" + i));
      t1.put(p1);
    }

    Assert.assertEquals(TEST_UTIL.countRows(t1), NB_ROWS_IN_BATCH * 2);
    t1.close();

    // Delete table table2
    admin.disableTable(table2);
    admin.deleteTable(table2);

    // #3 - incremental backup for table1
    tables = Lists.newArrayList(table1);
    request = createBackupRequest(BackupType.INCREMENTAL, tables, BACKUP_ROOT_DIR);
    String backupIdIncMultiple = client.backupTables(request);
    assertTrue(checkSucceeded(backupIdIncMultiple));

    // #4 - restore full backup for all tables, without overwrite
    TableName[] tablesRestoreFull = new TableName[] { table1, table2 };

    TableName[] tablesMapFull = new TableName[] { table1_restore, table2_restore };

    client.restore(BackupUtils.createRestoreRequest(BACKUP_ROOT_DIR, backupIdFull, false,
      tablesRestoreFull, tablesMapFull, false));

    // #5.1 - check tables for full restore
    HBaseAdmin hAdmin = TEST_UTIL.getHBaseAdmin();
    assertTrue(hAdmin.tableExists(table1_restore));
    assertTrue(hAdmin.tableExists(table2_restore));

    // #5.2 - checking row count of tables for full restore
    Table hTable = conn.getTable(table1_restore);
    Assert.assertEquals(TEST_UTIL.countRows(hTable), NB_ROWS_IN_BATCH);
    hTable.close();

    hTable = conn.getTable(table2_restore);
    Assert.assertEquals(TEST_UTIL.countRows(hTable), NB_ROWS_IN_BATCH);
    hTable.close();

    // #6 - restore incremental backup for table1
    TableName[] tablesRestoreIncMultiple = new TableName[] { table1 };
    TableName[] tablesMapIncMultiple = new TableName[] { table1_restore };
    client.restore(BackupUtils.createRestoreRequest(BACKUP_ROOT_DIR, backupIdIncMultiple,
      false, tablesRestoreIncMultiple, tablesMapIncMultiple, true));

    hTable = conn.getTable(table1_restore);
    Assert.assertEquals(TEST_UTIL.countRows(hTable), NB_ROWS_IN_BATCH * 2);
    hTable.close();
    admin.close();
    conn.close();
  }

}
