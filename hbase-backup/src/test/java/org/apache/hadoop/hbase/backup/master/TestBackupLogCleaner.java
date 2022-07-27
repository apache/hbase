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
package org.apache.hadoop.hbase.backup.master;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupType;
import org.apache.hadoop.hbase.backup.TestBackupBase;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@Category(LargeTests.class)
public class TestBackupLogCleaner extends TestBackupBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBackupLogCleaner.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestBackupLogCleaner.class);

  // implements all test cases in 1 test since incremental full backup/
  // incremental backup has dependencies

  @Test
  public void testBackupLogCleaner() throws Exception {

    // #1 - create full backup for all tables
    LOG.info("create full backup image for all tables");

    List<TableName> tableSetFullList = Lists.newArrayList(table1, table2, table3, table4);

    try (BackupSystemTable systemTable = new BackupSystemTable(TEST_UTIL.getConnection())) {
      // Verify that we have no backup sessions yet
      assertFalse(systemTable.hasBackupSessions());

      List<FileStatus> walFiles = getListOfWALFiles(TEST_UTIL.getConfiguration());
      BackupLogCleaner cleaner = new BackupLogCleaner();
      cleaner.setConf(TEST_UTIL.getConfiguration());
      Map<String, Object> params = new HashMap<>();
      params.put(HMaster.MASTER, TEST_UTIL.getHBaseCluster().getMaster());
      cleaner.init(params);
      cleaner.setConf(TEST_UTIL.getConfiguration());

      Iterable<FileStatus> deletable = cleaner.getDeletableFiles(walFiles);
      int size = Iterables.size(deletable);

      // We can delete all files because we do not have yet recorded backup sessions
      assertTrue(size == walFiles.size());

      String backupIdFull = fullTableBackup(tableSetFullList);
      assertTrue(checkSucceeded(backupIdFull));
      // Check one more time
      deletable = cleaner.getDeletableFiles(walFiles);
      // We can delete wal files because they were saved into backup system table table
      size = Iterables.size(deletable);
      assertTrue(size == walFiles.size());

      List<FileStatus> newWalFiles = getListOfWALFiles(TEST_UTIL.getConfiguration());
      LOG.debug("WAL list after full backup");

      // New list of wal files is greater than the previous one,
      // because new wal per RS have been opened after full backup
      assertTrue(walFiles.size() < newWalFiles.size());
      Connection conn = ConnectionFactory.createConnection(conf1);
      // #2 - insert some data to table
      Table t1 = conn.getTable(table1);
      Put p1;
      for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
        p1 = new Put(Bytes.toBytes("row-t1" + i));
        p1.addColumn(famName, qualName, Bytes.toBytes("val" + i));
        t1.put(p1);
      }

      t1.close();

      Table t2 = conn.getTable(table2);
      Put p2;
      for (int i = 0; i < 5; i++) {
        p2 = new Put(Bytes.toBytes("row-t2" + i));
        p2.addColumn(famName, qualName, Bytes.toBytes("val" + i));
        t2.put(p2);
      }

      t2.close();

      // #3 - incremental backup for multiple tables

      List<TableName> tableSetIncList = Lists.newArrayList(table1, table2, table3);
      String backupIdIncMultiple =
        backupTables(BackupType.INCREMENTAL, tableSetIncList, BACKUP_ROOT_DIR);
      assertTrue(checkSucceeded(backupIdIncMultiple));
      deletable = cleaner.getDeletableFiles(newWalFiles);

      assertTrue(Iterables.size(deletable) == newWalFiles.size());

      conn.close();
    }
  }
}
