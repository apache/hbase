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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupState;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Verify that deleting a FAILED backup does not cascade-delete subsequent COMPLETE backups. A
 * FAILED backup's state was rolled back via snapshot restore, so no subsequent backup depends on
 * it.
 */
@Category(LargeTests.class)
public class TestDeleteFailedBackupDoesNotCascade extends TestBackupBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestDeleteFailedBackupDoesNotCascade.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestDeleteFailedBackupDoesNotCascade.class);

  @Test
  public void testDeleteFailedBackupDoesNotCascadeToCompletedIncremental() throws Exception {
    LOG.info("Test that deleting a FAILED backup does not cascade to COMPLETE incrementals");

    List<TableName> tableList = Lists.newArrayList(table1);

    // Step 1: Create a full backup
    String fullBackupId = fullTableBackup(tableList);
    assertTrue(checkSucceeded(fullBackupId));
    LOG.info("Full backup {} succeeded", fullBackupId);

    // Step 2: Insert data so the incremental has something to back up
    try (Connection conn = ConnectionFactory.createConnection(conf1);
      Table t1 = conn.getTable(table1)) {
      for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
        Put p = new Put(Bytes.toBytes("row-incr1-" + i));
        p.addColumn(famName, qualName, Bytes.toBytes("val" + i));
        t1.put(p);
      }
    }

    // Step 3: Create a successful incremental backup
    String successIncrBackupId = incrementalTableBackup(tableList);
    assertTrue(checkSucceeded(successIncrBackupId));
    LOG.info("Successful incremental backup: {}", successIncrBackupId);

    // Step 4: Directly insert a FAILED backup record into the system table.
    // This simulates a backup that failed and was recorded as FAILED. Its timestamp
    // is between the full and the successful incremental, so the cascade logic
    // would find the successful incremental as "affected" if not guarded.
    BackupInfo fullInfo = getBackupInfo(fullBackupId);
    BackupInfo successInfo = getBackupInfo(successIncrBackupId);
    long failedTs = (fullInfo.getStartTs() + successInfo.getStartTs()) / 2;

    String failedBackupId = "backup_" + failedTs;
    BackupInfo failedInfo = new BackupInfo(failedBackupId, BackupType.INCREMENTAL,
      tableList.toArray(new TableName[0]), BACKUP_ROOT_DIR);
    failedInfo.setStartTs(failedTs);
    failedInfo.setCompleteTs(failedTs + 1);
    failedInfo.setState(BackupState.FAILED);
    failedInfo.setFailedMsg("Simulated failure for test");

    try (BackupSystemTable sysTable = new BackupSystemTable(TEST_UTIL.getConnection())) {
      sysTable.updateBackupInfo(failedInfo);
    }
    assertTrue("FAILED backup should exist", checkFailed(failedBackupId));
    LOG.info("Inserted FAILED backup record: {}", failedBackupId);

    // Step 5: Delete the FAILED backup
    int deleted = getBackupAdmin().deleteBackups(new String[] { failedBackupId });
    assertEquals(1, deleted);
    LOG.info("Deleted FAILED backup {}", failedBackupId);

    // Step 6: Verify the FAILED backup is gone
    assertNull("FAILED backup should be deleted", getBackupInfo(failedBackupId));

    // Step 7: Verify the successful incremental is still present
    successInfo = getBackupInfo(successIncrBackupId);
    assertNotNull("COMPLETE incremental backup should NOT be cascade-deleted", successInfo);
    assertEquals(BackupState.COMPLETE, successInfo.getState());

    // Step 8: Verify the full backup is still present
    fullInfo = getBackupInfo(fullBackupId);
    assertNotNull("Full backup should still exist", fullInfo);
    assertEquals(BackupState.COMPLETE, fullInfo.getState());

    LOG.info("Test passed: deleting FAILED backup did not cascade to COMPLETE incremental");
  }

  private BackupInfo getBackupInfo(String backupId) throws Exception {
    try (BackupSystemTable table = new BackupSystemTable(TEST_UTIL.getConnection())) {
      return table.readBackupInfo(backupId);
    }
  }
}
