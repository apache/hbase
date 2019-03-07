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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupAdminImpl;
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
 * Create multiple backups for two tables: table1, table2 then perform 1 delete
 */
@Category(LargeTests.class)
public class TestBackupMultipleDeletes extends TestBackupBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestBackupMultipleDeletes.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestBackupMultipleDeletes.class);

  @Test
  public void testBackupMultipleDeletes() throws Exception {
    // #1 - create full backup for all tables
    LOG.info("create full backup image for all tables");
    List<TableName> tables = Lists.newArrayList(table1, table2);
    HBaseAdmin admin = null;
    Connection conn = ConnectionFactory.createConnection(conf1);
    admin = (HBaseAdmin) conn.getAdmin();
    BackupAdmin client = new BackupAdminImpl(conn);
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
    // #3 - incremental backup for table1
    tables = Lists.newArrayList(table1);
    request = createBackupRequest(BackupType.INCREMENTAL, tables, BACKUP_ROOT_DIR);
    String backupIdInc1 = client.backupTables(request);
    assertTrue(checkSucceeded(backupIdInc1));
    // #4 - insert some data to table table2
    Table t2 = conn.getTable(table2);
    Put p2 = null;
    for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
      p2 = new Put(Bytes.toBytes("row-t2" + i));
      p2.addColumn(famName, qualName, Bytes.toBytes("val" + i));
      t2.put(p2);
    }
    // #5 - incremental backup for table1, table2
    tables = Lists.newArrayList(table1, table2);
    request = createBackupRequest(BackupType.INCREMENTAL, tables, BACKUP_ROOT_DIR);
    String backupIdInc2 = client.backupTables(request);
    assertTrue(checkSucceeded(backupIdInc2));
    // #6 - insert some data to table table1
    t1 = conn.getTable(table1);
    for (int i = NB_ROWS_IN_BATCH; i < 2 * NB_ROWS_IN_BATCH; i++) {
      p1 = new Put(Bytes.toBytes("row-t1" + i));
      p1.addColumn(famName, qualName, Bytes.toBytes("val" + i));
      t1.put(p1);
    }
    // #7 - incremental backup for table1
    tables = Lists.newArrayList(table1);
    request = createBackupRequest(BackupType.INCREMENTAL, tables, BACKUP_ROOT_DIR);
    String backupIdInc3 = client.backupTables(request);
    assertTrue(checkSucceeded(backupIdInc3));
    // #8 - insert some data to table table2
    t2 = conn.getTable(table2);
    for (int i = NB_ROWS_IN_BATCH; i < 2 * NB_ROWS_IN_BATCH; i++) {
      p2 = new Put(Bytes.toBytes("row-t1" + i));
      p2.addColumn(famName, qualName, Bytes.toBytes("val" + i));
      t2.put(p2);
    }
    // #9 - incremental backup for table1, table2
    tables = Lists.newArrayList(table1, table2);
    request = createBackupRequest(BackupType.INCREMENTAL, tables, BACKUP_ROOT_DIR);
    String backupIdInc4 = client.backupTables(request);
    assertTrue(checkSucceeded(backupIdInc4));
    // #10 full backup for table3
    tables = Lists.newArrayList(table3);
    request = createBackupRequest(BackupType.FULL, tables, BACKUP_ROOT_DIR);
    String backupIdFull2 = client.backupTables(request);
    assertTrue(checkSucceeded(backupIdFull2));
    // #11 - incremental backup for table3
    tables = Lists.newArrayList(table3);
    request = createBackupRequest(BackupType.INCREMENTAL, tables, BACKUP_ROOT_DIR);
    String backupIdInc5 = client.backupTables(request);
    assertTrue(checkSucceeded(backupIdInc5));
    LOG.error("Delete backupIdInc2");
    client.deleteBackups(new String[] { backupIdInc2 });
    LOG.error("Delete backupIdInc2 done");
    List<BackupInfo> list = client.getHistory(100);
    // First check number of backup images before and after
    assertEquals(4, list.size());
    // then verify that no backupIdInc2,3,4
    Set<String> ids = new HashSet<String>();
    ids.add(backupIdInc2);
    ids.add(backupIdInc3);
    ids.add(backupIdInc4);
    for (BackupInfo info : list) {
      String backupId = info.getBackupId();
      if (ids.contains(backupId)) {
        assertTrue(false);
      }
    }
    // Verify that backupInc5 contains only table3
    boolean found = false;
    for (BackupInfo info : list) {
      String backupId = info.getBackupId();
      if (backupId.equals(backupIdInc5)) {
        assertTrue(info.getTables().size() == 1);
        assertEquals(table3, info.getTableNames().get(0));
        found = true;
      }
    }
    assertTrue(found);
    admin.close();
    conn.close();
  }

}
