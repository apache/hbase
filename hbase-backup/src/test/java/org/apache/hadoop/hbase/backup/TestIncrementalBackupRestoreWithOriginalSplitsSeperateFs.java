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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.List;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupAdminImpl;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@Tag(LargeTests.TAG)
public class TestIncrementalBackupRestoreWithOriginalSplitsSeperateFs
  extends IncrementalBackupRestoreTestBase {

  @Test
  public void testIncBackupRestoreWithOriginalSplitsSeperateFs() throws Exception {
    // prepare BACKUP_ROOT_DIR on a different filesystem from HBase.
    try (Connection conn = ConnectionFactory.createConnection(conf1);
      BackupAdminImpl admin = new BackupAdminImpl(conn)) {
      String backupTargetDir = TEST_UTIL.getDataTestDir("backupTarget").toString();
      BACKUP_ROOT_DIR = new File(backupTargetDir).toURI().toString();

      List<TableName> tables = Lists.newArrayList(table1);

      insertIntoTable(conn, table1, famName, 3, 100);
      String fullBackupId = takeFullBackup(tables, admin, true);
      assertTrue(checkSucceeded(fullBackupId));

      insertIntoTable(conn, table1, famName, 4, 100);

      HRegion regionToBulkload = TEST_UTIL.getHBaseCluster().getRegions(table1).get(0);
      String regionName = regionToBulkload.getRegionInfo().getEncodedName();
      doBulkload(table1, regionName, famName);

      BackupRequest request =
        createBackupRequest(BackupType.INCREMENTAL, tables, BACKUP_ROOT_DIR, true);
      String incrementalBackupId = admin.backupTables(request);
      assertTrue(checkSucceeded(incrementalBackupId));

      TableName[] fromTable = new TableName[] { table1 };
      TableName[] toTable = new TableName[] { table1_restore };

      // Using original splits
      admin.restore(BackupUtils.createRestoreRequest(BACKUP_ROOT_DIR, incrementalBackupId, false,
        fromTable, toTable, true, true));

      int actualRowCount = TEST_UTIL.countRows(table1_restore);
      int expectedRowCount = TEST_UTIL.countRows(table1);
      assertEquals(expectedRowCount, actualRowCount);

      // Using new splits
      admin.restore(BackupUtils.createRestoreRequest(BACKUP_ROOT_DIR, incrementalBackupId, false,
        fromTable, toTable, true, false));

      expectedRowCount = TEST_UTIL.countRows(table1);
      assertEquals(expectedRowCount, actualRowCount);
    }
  }
}
