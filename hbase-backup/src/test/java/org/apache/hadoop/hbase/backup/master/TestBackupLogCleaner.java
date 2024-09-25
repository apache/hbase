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
import java.util.LinkedHashSet;
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

    List<TableName> tableSetFull = List.of(table1, table2, table3, table4);
    List<TableName> tableSet14 = List.of(table1, table4);
    List<TableName> tableSet23 = List.of(table2, table3);

    try (BackupSystemTable systemTable = new BackupSystemTable(TEST_UTIL.getConnection())) {
      // Verify that we have no backup sessions yet
      assertFalse(systemTable.hasBackupSessions());

      BackupLogCleaner cleaner = new BackupLogCleaner();
      cleaner.setConf(TEST_UTIL.getConfiguration());
      cleaner.init(Map.of(HMaster.MASTER, TEST_UTIL.getHBaseCluster().getMaster()));

      // All WAL files can be deleted because we do not have backups
      List<FileStatus> walFilesBeforeBackup = getListOfWALFiles(TEST_UTIL.getConfiguration());
      Iterable<FileStatus> deletable = cleaner.getDeletableFiles(walFilesBeforeBackup);
      assertEquals(walFilesBeforeBackup, deletable);

      // Create a FULL backup B1 in backupRoot R1, containing all tables
      String backupIdB1 = backupTables(BackupType.FULL, tableSetFull, backupRoot1.toString());
      assertTrue(checkSucceeded(backupIdB1));

      // As part of a backup, WALs are rolled, so we expect a new WAL file
      Set<FileStatus> walFilesAfterB1 =
        mergeAsSet(walFilesBeforeBackup, getListOfWALFiles(TEST_UTIL.getConfiguration()));
      assertTrue(walFilesBeforeBackup.size() < walFilesAfterB1.size());

      // Currently, we only have backup B1, so we can delete any WAL preceding B1
      deletable = cleaner.getDeletableFiles(walFilesAfterB1);
      assertEquals(toSet(walFilesBeforeBackup), toSet(deletable));

      // Insert some data
      Connection conn = TEST_UTIL.getConnection();
      try (Table t1 = conn.getTable(table1)) {
        Put p1;
        for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
          p1 = new Put(Bytes.toBytes("row-t1" + i));
          p1.addColumn(famName, qualName, Bytes.toBytes("val" + i));
          t1.put(p1);
        }
      }

      try (Table t2 = conn.getTable(table2)) {
        Put p2;
        for (int i = 0; i < 5; i++) {
          p2 = new Put(Bytes.toBytes("row-t2" + i));
          p2.addColumn(famName, qualName, Bytes.toBytes("val" + i));
          t2.put(p2);
        }
      }

      // Create an INCREMENTAL backup B2 in backupRoot R1, requesting tables 1 & 4.
      // Note that incremental tables always include all tables already included in the backup root,
      // i.e. the backup will contain all tables (1, 2, 3, 4), ignoring what we specify here.
      LOG.debug("Creating B2");
      String backupIdB2 = backupTables(BackupType.INCREMENTAL, tableSet14, backupRoot1.toString());
      assertTrue(checkSucceeded(backupIdB2));

      // As part of a backup, WALs are rolled, so we expect a new WAL file
      Set<FileStatus> walFilesAfterB2 =
        mergeAsSet(walFilesAfterB1, getListOfWALFiles(TEST_UTIL.getConfiguration()));
      assertTrue(walFilesAfterB1.size() < walFilesAfterB2.size());

      // At this point, we have backups in root R1: B1 and B2.
      // We only consider the most recent backup (B2) to determine which WALs can be deleted:
      // all WALs preceding B2
      deletable = cleaner.getDeletableFiles(walFilesAfterB2);
      assertEquals(toSet(walFilesAfterB1), toSet(deletable));

      // Create a FULL backup B3 in backupRoot R2, containing tables 1 & 4
      LOG.debug("Creating B3");
      String backupIdB3 = backupTables(BackupType.FULL, tableSetFull, backupRoot2.toString());
      assertTrue(checkSucceeded(backupIdB3));

      // As part of a backup, WALs are rolled, so we expect a new WAL file
      Set<FileStatus> walFilesAfterB3 =
        mergeAsSet(walFilesAfterB2, getListOfWALFiles(TEST_UTIL.getConfiguration()));
      assertTrue(walFilesAfterB2.size() < walFilesAfterB3.size());

      // At this point, we have backups in:
      // root R1: B1 (timestamp=0, all tables), B2 (TS=1, all tables)
      // root R2: B3 (TS=2, [T1, T4])
      //
      // To determine the WAL-deletion boundary, we only consider the most recent backup per root,
      // so [B2, B3]. From these, we take the least recent as WAL-deletion boundary: B2, it contains
      // all tables, so acts as the deletion boundary. I.e. only WALs preceding B2 are deletable.
      deletable = cleaner.getDeletableFiles(walFilesAfterB3);
      assertEquals(toSet(walFilesAfterB1), toSet(deletable));

      // Create a FULL backup B4 in backupRoot R1, with a subset of tables
      LOG.debug("Creating B4");
      String backupIdB4 = backupTables(BackupType.FULL, tableSet14, backupRoot1.toString());
      assertTrue(checkSucceeded(backupIdB4));

      // As part of a backup, WALs are rolled, so we expect a new WAL file
      Set<FileStatus> walFilesAfterB4 =
        mergeAsSet(walFilesAfterB3, getListOfWALFiles(TEST_UTIL.getConfiguration()));
      assertTrue(walFilesAfterB3.size() < walFilesAfterB4.size());

      // At this point, we have backups in:
      // root R1: B1 (timestamp=0, all tables), B2 (TS=1, all tables), B4 (TS=3, [T1, T4])
      // root R2: B3 (TS=2, [T1, T4])
      //
      // To determine the WAL-deletion boundary, we only consider the most recent backup per root,
      // so [B4, B3]. They contain the following timestamp boundaries per table:
      // B4: { T1: 3, T2: 1, T3: 1, T4: 3 }
      // B3: { T1: 2, T4: 2 }
      // Taking the minimum timestamp (= 1), this means all WALs preceding B2 can be deleted.
      deletable = cleaner.getDeletableFiles(walFilesAfterB4);
      assertEquals(toSet(walFilesAfterB1), toSet(deletable));

      // Create a FULL backup B5 in backupRoot R1, for tables 2 & 3
      String backupIdB5 = backupTables(BackupType.FULL, tableSet23, backupRoot1.toString());
      assertTrue(checkSucceeded(backupIdB5));

      // As part of a backup, WALs are rolled, so we expect a new WAL file
      Set<FileStatus> walFilesAfterB5 =
        mergeAsSet(walFilesAfterB4, getListOfWALFiles(TEST_UTIL.getConfiguration()));
      assertTrue(walFilesAfterB4.size() < walFilesAfterB5.size());

      // At this point, we have backups in:
      // root R1: ..., B2 (TS=1, all tables), B4 (TS=3, [T1, T4]), B5 (TS=4, [T2, T3])
      // root R2: B3 (TS=2, [T1, T4])
      //
      // To determine the WAL-deletion boundary, we only consider the most recent backup per root,
      // so [B5, B3]. They contain the following timestamp boundaries per table:
      // B4: { T1: 3, T2: 4, T3: 4, T4: 3 }
      // B3: { T1: 2, T4: 2 }
      // Taking the minimum timestamp (= 2), this means all WALs preceding B3 can be deleted.
      deletable = cleaner.getDeletableFiles(walFilesAfterB5);
      assertEquals(toSet(walFilesAfterB2), toSet(deletable));
    }
  }

  private Set<FileStatus> mergeAsSet(Collection<FileStatus> toCopy, Collection<FileStatus> toAdd) {
    Set<FileStatus> result = new LinkedHashSet<>(toCopy);
    result.addAll(toAdd);
    return result;
  }

  private <T> Set<T> toSet(Iterable<T> iterable) {
    Set<T> result = new LinkedHashSet<>();
    iterable.forEach(result::add);
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
