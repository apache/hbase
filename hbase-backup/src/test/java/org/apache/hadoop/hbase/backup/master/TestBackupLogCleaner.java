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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
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
    Path backupRoot1 = new Path(BACKUP_ROOT_DIR, "root1");
    Path backupRoot2 = new Path(BACKUP_ROOT_DIR, "root2");

    // Create full backup for all tables
    LOG.info("create full backup image for all tables");

    List<TableName> tableSetFullList = Lists.newArrayList(table1, table2, table3, table4);

    try (BackupSystemTable systemTable = new BackupSystemTable(TEST_UTIL.getConnection())) {
      // Verify that we have no backup sessions yet
      assertFalse(systemTable.hasBackupSessions());

      List<FileStatus> walFilesBeforeBackup = getListOfWALFiles(TEST_UTIL.getConfiguration());
      BackupLogCleaner cleaner = new BackupLogCleaner();
      cleaner.setConf(TEST_UTIL.getConfiguration());
      Map<String, Object> params = new HashMap<>();
      params.put(HMaster.MASTER, TEST_UTIL.getHBaseCluster().getMaster());
      cleaner.init(params);
      cleaner.setConf(TEST_UTIL.getConfiguration());

      // We can delete all files because we do not have yet recorded backup sessions
      Iterable<FileStatus> deletable = cleaner.getDeletableFiles(walFilesBeforeBackup);
      int size = Iterables.size(deletable);
      assertEquals(walFilesBeforeBackup.size(), size);

      // Create a FULL backup (backupRoot 1)
      String backupIdFull = backupTables(BackupType.FULL, tableSetFullList, backupRoot1.toString());
      assertTrue(checkSucceeded(backupIdFull));

      // New list of WAL files is greater than the previous one,
      // because new WAL per RS have been opened after full backup
      Set<FileStatus> walFilesAfterFullBackup =
        mergeAsSet(walFilesBeforeBackup, getListOfWALFiles(TEST_UTIL.getConfiguration()));
      assertTrue(walFilesBeforeBackup.size() < walFilesAfterFullBackup.size());

      // We can only delete the WALs preceding the FULL backup
      deletable = cleaner.getDeletableFiles(walFilesAfterFullBackup);
      size = Iterables.size(deletable);
      assertEquals(walFilesBeforeBackup.size(), size);

      // Insert some data
      Connection conn = ConnectionFactory.createConnection(conf1);
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

      // Create an INCREMENTAL backup (backupRoot 1)
      List<TableName> tableSetIncList = Lists.newArrayList(table1, table2, table3);
      String backupIdIncMultiple =
        backupTables(BackupType.INCREMENTAL, tableSetIncList, backupRoot1.toString());
      assertTrue(checkSucceeded(backupIdIncMultiple));

      // There should be more WALs due to the rolling of Region Servers
      Set<FileStatus> walFilesAfterIncBackup =
        mergeAsSet(walFilesAfterFullBackup, getListOfWALFiles(TEST_UTIL.getConfiguration()));
      assertTrue(walFilesAfterFullBackup.size() < walFilesAfterIncBackup.size());

      // We can only delete the WALs preceding the INCREMENTAL backup
      deletable = cleaner.getDeletableFiles(walFilesAfterIncBackup);
      size = Iterables.size(deletable);
      assertEquals(walFilesAfterFullBackup.size(), size);

      // Create a FULL backup (backupRoot 2)
      String backupIdFull2 = backupTables(BackupType.FULL, tableSetIncList, backupRoot2.toString());
      assertTrue(checkSucceeded(backupIdFull2));

      // There should be more WALs due to the rolling of Region Servers
      Set<FileStatus> walFilesAfterFullBackup2 =
        mergeAsSet(walFilesAfterFullBackup, getListOfWALFiles(TEST_UTIL.getConfiguration()));
      assertTrue(walFilesAfterIncBackup.size() < walFilesAfterFullBackup2.size());

      // We created a backup in a different root, so the WAL dependencies of the first root did not
      // change. I.e. the same files should be deletable as after the incremental backup.
      deletable = cleaner.getDeletableFiles(walFilesAfterFullBackup2);
      size = Iterables.size(deletable);
      assertEquals(walFilesAfterFullBackup.size(), size);

      conn.close();
    }
  }

  private Set<FileStatus> mergeAsSet(Collection<FileStatus> toCopy, Collection<FileStatus> toAdd) {
    Set<FileStatus> result = new HashSet<>(toCopy);
    result.addAll(toAdd);
    return result;
  }

  @Test
  public void testCleansUpHMasterWal() {
    Path path = new Path("/hbase/MasterData/WALs/hmaster,60000,1718808578163");
    assertTrue(BackupLogCleaner.canDeleteFile(Collections.emptyMap(), path));
  }

  @Test
  public void testCleansUpArchivedHMasterWal() {
    Path normalPath =
      new Path("/hbase/oldWALs/hmaster%2C60000%2C1716224062663.1716247552189$masterlocalwal$");
    assertTrue(BackupLogCleaner.canDeleteFile(Collections.emptyMap(), normalPath));

    Path masterPath = new Path(
      "/hbase/MasterData/oldWALs/hmaster%2C60000%2C1716224062663.1716247552189$masterlocalwal$");
    assertTrue(BackupLogCleaner.canDeleteFile(Collections.emptyMap(), masterPath));
  }
}
