/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.backup;

import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.backup.HBackupFileSystem;
import org.apache.hadoop.fs.Path;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestIncrementalBackup extends TestBackupBase {
  private static final Log LOG = LogFactory.getLog(TestIncrementalBackup.class);
  //implement all testcases in 1 test since incremental backup/restore has dependencies
  @Test
  public void TestIncBackupRestore() throws Exception {
    HBackupFileSystem hbfs;

    // #1 - create full backup for all tables
    LOG.info("create full backup image for all tables");
    String tablesetFull =
        table1.getNameAsString() + BackupRestoreConstants.TABLENAME_DELIMITER_IN_COMMAND
            + table2.getNameAsString() + BackupRestoreConstants.TABLENAME_DELIMITER_IN_COMMAND
            + table3.getNameAsString() + BackupRestoreConstants.TABLENAME_DELIMITER_IN_COMMAND
            + table4.getNameAsString();

    String backupIdFull =
        BackupClient.create("full", BACKUP_ROOT_DIR, tablesetFull, null);
    assertTrue(checkSucceeded(backupIdFull));

    Connection conn = ConnectionFactory.createConnection(conf1); 
    // #2 - insert some data to table
    HTable t1 = (HTable) conn.getTable(table1);
    Put p1;
    for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
      p1 = new Put(Bytes.toBytes("row-t1" + i));
      p1.addColumn(famName, qualName, Bytes.toBytes("val" + i));
      t1.put(p1);
    }

    Assert.assertThat(TEST_UTIL.countRows(t1), CoreMatchers.equalTo(NB_ROWS_IN_BATCH * 2));
    t1.close();

    HTable t2 =  (HTable) conn.getTable(table2);
    Put p2;
    for (int i = 0; i < 5; i++) {
      p2 = new Put(Bytes.toBytes("row-t2" + i));
      p2.addColumn(famName, qualName, Bytes.toBytes("val" + i));
      t2.put(p2);
    }

    Assert.assertThat(TEST_UTIL.countRows(t2), CoreMatchers.equalTo(NB_ROWS_IN_BATCH + 5));
    t2.close();

    // #3 - incremental backup for multiple tables
    String tablesetIncMultiple =
        table1.getNameAsString() + BackupRestoreConstants.TABLENAME_DELIMITER_IN_COMMAND
            + table2.getNameAsString() + BackupRestoreConstants.TABLENAME_DELIMITER_IN_COMMAND
            + table3.getNameAsString();

    String backupIdIncMultiple = BackupClient.create("incremental", BACKUP_ROOT_DIR, 
      tablesetIncMultiple, null);
    assertTrue(checkSucceeded(backupIdIncMultiple));


    // #4 - restore full backup for all tables, without overwrite
    String[] tablesRestoreFull =
        new String[] { table1.getNameAsString(), table2.getNameAsString(),
            table3.getNameAsString(), table4.getNameAsString() };

    String[] tablesMapFull =
        new String[] { table1_restore, table2_restore, table3_restore, table4_restore };

    hbfs = new HBackupFileSystem(conf1, new Path(BACKUP_ROOT_DIR), backupIdFull);
    RestoreClient.restore_stage1(hbfs, BACKUP_ROOT_DIR, backupIdFull, false, false,
      tablesRestoreFull,
      tablesMapFull, false);

    // #5.1 - check tables for full restore
    HBaseAdmin hAdmin = TEST_UTIL.getHBaseAdmin();
    assertTrue(hAdmin.tableExists(TableName.valueOf(table1_restore)));
    assertTrue(hAdmin.tableExists(TableName.valueOf(table2_restore)));
    assertTrue(hAdmin.tableExists(TableName.valueOf(table3_restore)));
    assertTrue(hAdmin.tableExists(TableName.valueOf(table4_restore)));

    hAdmin.close();

    // #5.2 - checking row count of tables for full restore
    HTable hTable = (HTable) conn.getTable(TableName.valueOf(table1_restore));
    Assert.assertThat(TEST_UTIL.countRows(hTable), CoreMatchers.equalTo(NB_ROWS_IN_BATCH));
    hTable.close();

    hTable = (HTable) conn.getTable(TableName.valueOf(table2_restore));
    Assert.assertThat(TEST_UTIL.countRows(hTable), CoreMatchers.equalTo(NB_ROWS_IN_BATCH));
    hTable.close();

    hTable = (HTable) conn.getTable(TableName.valueOf(table3_restore));
    Assert.assertThat(TEST_UTIL.countRows(hTable), CoreMatchers.equalTo(0));
    hTable.close();

    hTable = (HTable) conn.getTable(TableName.valueOf(table4_restore));
    Assert.assertThat(TEST_UTIL.countRows(hTable), CoreMatchers.equalTo(0));
    hTable.close();

    // #6 - restore incremental backup for multiple tables, with overwrite
    String[] tablesRestoreIncMultiple =
        new String[]
        { table1.getNameAsString(), table2.getNameAsString(), table3.getNameAsString() };
    String[] tablesMapIncMultiple =
        new String[] { table1_restore, table2_restore, table3_restore };
    hbfs = new HBackupFileSystem(conf1, new Path(BACKUP_ROOT_DIR), backupIdIncMultiple);
    RestoreClient.restore_stage1(hbfs, BACKUP_ROOT_DIR, backupIdIncMultiple, false, false,
      tablesRestoreIncMultiple, tablesMapIncMultiple, true);

    hTable = (HTable) conn.getTable(TableName.valueOf(table1_restore));
    Assert.assertThat(TEST_UTIL.countRows(hTable), CoreMatchers.equalTo(NB_ROWS_IN_BATCH * 2));
    hTable.close();

    hTable = (HTable) conn.getTable(TableName.valueOf(table2_restore));
    Assert.assertThat(TEST_UTIL.countRows(hTable), CoreMatchers.equalTo(NB_ROWS_IN_BATCH + 5));
    hTable.close();

    hTable = (HTable) conn.getTable(TableName.valueOf(table3_restore));
    Assert.assertThat(TEST_UTIL.countRows(hTable), CoreMatchers.equalTo(0));
    hTable.close();

    // #7 - incremental backup for single, empty table

    String tablesetIncEmpty = table4.getNameAsString();
    String backupIdIncEmpty =
        BackupClient.create("incremental", BACKUP_ROOT_DIR, tablesetIncEmpty, null);
    assertTrue(checkSucceeded(backupIdIncEmpty));


    // #8 - restore incremental backup for single empty table, with overwrite
    String[] tablesRestoreIncEmpty = new String[] { table4.getNameAsString() };
    String[] tablesMapIncEmpty = new String[] { table4_restore };
    hbfs = new HBackupFileSystem(conf1, new Path(BACKUP_ROOT_DIR), backupIdIncEmpty);
    RestoreClient.restore_stage1(hbfs, BACKUP_ROOT_DIR, backupIdIncEmpty, false, false,
      tablesRestoreIncEmpty,
      tablesMapIncEmpty, true);

    hTable = (HTable) conn.getTable(TableName.valueOf(table4_restore));
    Assert.assertThat(TEST_UTIL.countRows(hTable), CoreMatchers.equalTo(0));
    hTable.close();
    conn.close();

  }

}
