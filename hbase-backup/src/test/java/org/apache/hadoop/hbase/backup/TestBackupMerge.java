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
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@Category(LargeTests.class)
public class TestBackupMerge extends TestBackupBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestBackupMerge.class);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestBackupMerge.class);



  @Test
  public void TestIncBackupMergeRestore() throws Exception {
    int ADD_ROWS = 99;
    // #1 - create full backup for all tables
    LOG.info("create full backup image for all tables");

    List<TableName> tables = Lists.newArrayList(table1, table2);
    // Set custom Merge Job implementation


    Connection conn = ConnectionFactory.createConnection(conf1);

    HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
    BackupAdminImpl client = new BackupAdminImpl(conn);

    BackupRequest request = createBackupRequest(BackupType.FULL, tables, BACKUP_ROOT_DIR);
    String backupIdFull = client.backupTables(request);

    assertTrue(checkSucceeded(backupIdFull));

    // #2 - insert some data to table1
    Table t1 = insertIntoTable(conn, table1, famName, 1, ADD_ROWS);
    LOG.debug("writing " + ADD_ROWS + " rows to " + table1);

    Assert.assertEquals(TEST_UTIL.countRows(t1), NB_ROWS_IN_BATCH + ADD_ROWS);
    t1.close();
    LOG.debug("written " + ADD_ROWS + " rows to " + table1);

    Table t2 = insertIntoTable(conn, table2, famName, 1, ADD_ROWS);

    Assert.assertEquals(TEST_UTIL.countRows(t2), NB_ROWS_IN_BATCH + ADD_ROWS);
    t2.close();
    LOG.debug("written " + ADD_ROWS + " rows to " + table2);

    // #3 - incremental backup for multiple tables
    tables = Lists.newArrayList(table1, table2);
    request = createBackupRequest(BackupType.INCREMENTAL, tables, BACKUP_ROOT_DIR);
    String backupIdIncMultiple = client.backupTables(request);

    assertTrue(checkSucceeded(backupIdIncMultiple));

    t1 = insertIntoTable(conn, table1, famName, 2, ADD_ROWS);
    t1.close();

    t2 = insertIntoTable(conn, table2, famName, 2, ADD_ROWS);
    t2.close();

    // #3 - incremental backup for multiple tables
    request = createBackupRequest(BackupType.INCREMENTAL, tables, BACKUP_ROOT_DIR);
    String backupIdIncMultiple2 = client.backupTables(request);
    assertTrue(checkSucceeded(backupIdIncMultiple2));

    try (BackupAdmin bAdmin = new BackupAdminImpl(conn)) {
      String[] backups = new String[] { backupIdIncMultiple, backupIdIncMultiple2 };
      bAdmin.mergeBackups(backups);
    }

    // #6 - restore incremental backup for multiple tables, with overwrite
    TableName[] tablesRestoreIncMultiple = new TableName[] { table1, table2 };
    TableName[] tablesMapIncMultiple = new TableName[] { table1_restore, table2_restore };
    client.restore(BackupUtils.createRestoreRequest(BACKUP_ROOT_DIR, backupIdIncMultiple2, false,
      tablesRestoreIncMultiple, tablesMapIncMultiple, true));

    Table hTable = conn.getTable(table1_restore);
    LOG.debug("After incremental restore: " + hTable.getDescriptor());
    int countRows = TEST_UTIL.countRows(hTable, famName);
    LOG.debug("f1 has " + countRows + " rows");
    Assert.assertEquals(NB_ROWS_IN_BATCH + 2 * ADD_ROWS, countRows);

    hTable.close();

    hTable = conn.getTable(table2_restore);
    Assert.assertEquals(TEST_UTIL.countRows(hTable), NB_ROWS_IN_BATCH + 2 * ADD_ROWS);
    hTable.close();

    admin.close();
    conn.close();
  }
}
