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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupAdminImpl;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@Tag(LargeTests.TAG)
public class TestIncrementalBackupRestoreWithOriginalSplits
  extends IncrementalBackupRestoreTestBase {

  @Test
  public void testIncBackupRestoreWithOriginalSplits() throws Exception {
    byte[] mobFam = Bytes.toBytes("mob");

    List<TableName> tables = Lists.newArrayList(table1);
    TableDescriptor newTable1Desc =
      TableDescriptorBuilder.newBuilder(table1Desc).setColumnFamily(ColumnFamilyDescriptorBuilder
        .newBuilder(mobFam).setMobEnabled(true).setMobThreshold(5L).build()).build();
    TEST_UTIL.getAdmin().modifyTable(newTable1Desc);

    try (Connection conn = TEST_UTIL.getConnection();
      BackupAdminImpl backupAdmin = new BackupAdminImpl(conn); Admin admin = conn.getAdmin()) {
      BackupRequest request = createBackupRequest(BackupType.FULL, tables, BACKUP_ROOT_DIR);
      String fullBackupId = backupAdmin.backupTables(request);
      assertTrue(checkSucceeded(fullBackupId));

      TableName[] fromTables = new TableName[] { table1 };
      TableName[] toTables = new TableName[] { table1_restore };

      List<LocatedFileStatus> preRestoreBackupFiles = getBackupFiles();
      backupAdmin.restore(BackupUtils.createRestoreRequest(BACKUP_ROOT_DIR, fullBackupId, false,
        fromTables, toTables, true, true));
      List<LocatedFileStatus> postRestoreBackupFiles = getBackupFiles();

      // Check that the backup files are the same before and after the restore process
      assertEquals(postRestoreBackupFiles, preRestoreBackupFiles);
      assertEquals(TEST_UTIL.countRows(table1_restore), NB_ROWS_IN_BATCH);

      int ROWS_TO_ADD = 1_000;
      // different IDs so that rows don't overlap
      insertIntoTable(conn, table1, famName, 3, ROWS_TO_ADD);
      insertIntoTable(conn, table1, mobFam, 4, ROWS_TO_ADD);
      List<HRegion> currentRegions = TEST_UTIL.getHBaseCluster().getRegions(table1);
      for (HRegion region : currentRegions) {
        byte[] name = region.getRegionInfo().getEncodedNameAsBytes();
        admin.splitRegionAsync(name).get();
      }

      TEST_UTIL.waitTableAvailable(table1);

      // Make sure we've split regions
      assertNotEquals(currentRegions, TEST_UTIL.getHBaseCluster().getRegions(table1));

      request = createBackupRequest(BackupType.INCREMENTAL, tables, BACKUP_ROOT_DIR);
      String incrementalBackupId = backupAdmin.backupTables(request);
      assertTrue(checkSucceeded(incrementalBackupId));
      preRestoreBackupFiles = getBackupFiles();
      backupAdmin.restore(BackupUtils.createRestoreRequest(BACKUP_ROOT_DIR, incrementalBackupId,
        false, fromTables, toTables, true, true));
      postRestoreBackupFiles = getBackupFiles();
      assertEquals(postRestoreBackupFiles, preRestoreBackupFiles);
      assertEquals(NB_ROWS_IN_BATCH + ROWS_TO_ADD + ROWS_TO_ADD,
        TEST_UTIL.countRows(table1_restore));

      // test bulkloads
      HRegion regionToBulkload = TEST_UTIL.getHBaseCluster().getRegions(table1).get(0);
      String regionName = regionToBulkload.getRegionInfo().getEncodedName();

      insertIntoTable(conn, table1, famName, 5, ROWS_TO_ADD);
      insertIntoTable(conn, table1, mobFam, 6, ROWS_TO_ADD);

      doBulkload(table1, regionName, famName, mobFam);

      // we need to major compact the regions to make sure there are no references
      // and the regions are once again splittable
      TEST_UTIL.compact(true);
      TEST_UTIL.flush();
      TEST_UTIL.waitTableAvailable(table1);

      for (HRegion region : TEST_UTIL.getHBaseCluster().getRegions(table1)) {
        if (region.isSplittable()) {
          admin.splitRegionAsync(region.getRegionInfo().getEncodedNameAsBytes()).get();
        }
      }

      request = createBackupRequest(BackupType.INCREMENTAL, tables, BACKUP_ROOT_DIR);
      incrementalBackupId = backupAdmin.backupTables(request);
      assertTrue(checkSucceeded(incrementalBackupId));

      preRestoreBackupFiles = getBackupFiles();
      backupAdmin.restore(BackupUtils.createRestoreRequest(BACKUP_ROOT_DIR, incrementalBackupId,
        false, fromTables, toTables, true, true));
      postRestoreBackupFiles = getBackupFiles();

      assertEquals(postRestoreBackupFiles, preRestoreBackupFiles);

      int rowsExpected = TEST_UTIL.countRows(table1);
      int rowsActual = TEST_UTIL.countRows(table1_restore);

      assertEquals(rowsExpected, rowsActual);
    }
  }
}
