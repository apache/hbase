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

import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.CONF_CONTINUOUS_BACKUP_PITR_WINDOW_DAYS;
import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupReplicationEndpoint.ONE_DAY_IN_MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Set;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.util.ToolRunner;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Tests the deletion of HBase backups under continuous backup and PITR settings.
 * <p>
 * Terminology:
 * <ul>
 * <li><b>ct (current time)</b>: Current timestamp</li>
 * <li><b>maxAllowedPITRTime (mapt)</b>: Maximum allowed time range for PITR, typically a
 * cluster-level config (e.g., 30 days ago)</li>
 * <li><b>cst (continuousBackupStartTime)</b>: Earliest time from which continuous backup is
 * available</li>
 * <li><b>fs</b>: Full backup start time (not reliably usable)</li>
 * <li><b>fm</b>: Time when snapshot (logical freeze) was taken (we don't have this)</li>
 * <li><b>fe</b>: Full backup end time (used as conservative proxy for fm)</li>
 * </ul>
 */
@Category(LargeTests.class)
public class TestBackupDeleteWithContinuousBackupAndPITR extends TestBackupBase {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBackupDeleteWithContinuousBackupAndPITR.class);

  private BackupSystemTable backupSystemTable;

  /**
   * Configures continuous backup with the specified CST (continuous backup start time).
   */
  private void configureContinuousBackup(long cstTimestamp) throws IOException {
    conf1.setLong(CONF_CONTINUOUS_BACKUP_PITR_WINDOW_DAYS, 30);
    backupSystemTable = new BackupSystemTable(TEST_UTIL.getConnection());

    backupSystemTable.addContinuousBackupTableSet(Set.of(table1), cstTimestamp);
  }

  private void cleanupContinuousBackup() throws IOException {
    backupSystemTable.removeContinuousBackupTableSet(Set.of(table1));
  }

  /**
   * Main Case: continuousBackupStartTime < maxAllowedPITRTime
   * <p>
   * Sub Case: fe < cst
   */
  @Test
  public void testDeletionWhenBackupCompletesBeforeCST() throws Exception {
    long now = System.currentTimeMillis();
    long cst = now - 40 * ONE_DAY_IN_MILLISECONDS; // CST = 40 days ago
    configureContinuousBackup(cst);

    String backupId =
      createAndUpdateBackup(cst - ONE_DAY_IN_MILLISECONDS, cst - ONE_DAY_IN_MILLISECONDS + 1000);
    assertDeletionSucceeds(backupSystemTable, backupId, false);

    cleanupContinuousBackup();
  }

  /**
   * Main Case: continuousBackupStartTime < maxAllowedPITRTime
   * <p>
   * Sub Case: fs < cst < fe
   */
  @Test
  public void testDeletionWhenBackupStraddlesCST() throws Exception {
    long now = System.currentTimeMillis();
    long cst = now - 40 * ONE_DAY_IN_MILLISECONDS; // CST = 40 days ago
    configureContinuousBackup(cst);

    String backupId = createAndUpdateBackup(cst - 1000, cst + 1000);
    assertDeletionSucceeds(backupSystemTable, backupId, false);

    cleanupContinuousBackup();
  }

  /**
   * Main Case: continuousBackupStartTime < maxAllowedPITRTime
   * <p>
   * Sub Case: fs >= cst && fe < mapt
   */
  @Test
  public void testDeletionWhenBackupWithinCSTToMAPTRangeAndUncovered() throws Exception {
    long now = System.currentTimeMillis();
    long cst = now - 40 * ONE_DAY_IN_MILLISECONDS;
    long mapt = now - 30 * ONE_DAY_IN_MILLISECONDS;
    configureContinuousBackup(cst);

    String backupId = createAndUpdateBackup(cst, mapt - 1000);
    assertDeletionFails(backupSystemTable, backupId);

    // Cover the backup with another backup
    String coverId = createAndUpdateBackup(cst, mapt - 1000);

    // Now, deletion should succeed because the backup is covered by the new one
    assertDeletionSucceeds(backupSystemTable, backupId, false);
    assertDeletionSucceeds(backupSystemTable, coverId, true);

    cleanupContinuousBackup();
  }

  /**
   * Main Case: continuousBackupStartTime < maxAllowedPITRTime
   * <p>
   * Sub Case: fs >= cst && fe >= mapt
   */
  @Test
  public void testDeletionWhenBackupExtendsBeyondMAPTAndUncovered() throws Exception {
    long now = System.currentTimeMillis();
    long cst = now - 40 * ONE_DAY_IN_MILLISECONDS;
    long mapt = now - 30 * ONE_DAY_IN_MILLISECONDS;
    configureContinuousBackup(cst);

    String backupId = createAndUpdateBackup(cst + 1000, mapt + 1000);
    assertDeletionFails(backupSystemTable, backupId);

    // Cover the backup with another backup
    String coverId = createAndUpdateBackup(cst + 1000, mapt + 1000);

    // Now, deletion should succeed because the backup is covered by the new one
    assertDeletionSucceeds(backupSystemTable, backupId, false);
    assertDeletionSucceeds(backupSystemTable, coverId, true);

    cleanupContinuousBackup();
  }

  /**
   * Main Case: continuousBackupStartTime >= maxAllowedPITRTime
   * <p>
   * Sub Case: fs < cst
   */
  @Test
  public void testDeletionWhenBackupBeforeCST_ShouldSucceed() throws Exception {
    long now = System.currentTimeMillis();
    long cst = now - 20 * ONE_DAY_IN_MILLISECONDS;
    configureContinuousBackup(cst);

    String backupId = createAndUpdateBackup(cst - 1000, cst + 1000);
    assertDeletionSucceeds(backupSystemTable, backupId, false);

    cleanupContinuousBackup();
  }

  /**
   * Main Case: continuousBackupStartTime >= maxAllowedPITRTime
   * <p>
   * Sub Case: fs >= cst
   */
  @Test
  public void testDeletionWhenBackupAfterCST_ShouldFailUnlessCovered() throws Exception {
    long now = System.currentTimeMillis();
    long cst = now - 20 * ONE_DAY_IN_MILLISECONDS;
    configureContinuousBackup(cst);

    String backupId = createAndUpdateBackup(cst + 1000, cst + 2000);
    assertDeletionFails(backupSystemTable, backupId);

    // Cover the backup with another backup
    String coverId = createAndUpdateBackup(cst + 1000, cst + 2000);

    assertDeletionSucceeds(backupSystemTable, backupId, false);
    assertDeletionSucceeds(backupSystemTable, coverId, true);

    cleanupContinuousBackup();
  }

  @Test
  public void testDeleteIncrementalBackup() throws Exception {
    long now = System.currentTimeMillis();
    long cst = now - 20 * ONE_DAY_IN_MILLISECONDS;
    configureContinuousBackup(cst);

    String fullBackupId = fullTableBackup(Lists.newArrayList(table1));
    String incrementalTableBackupId = incrementalTableBackup(Lists.newArrayList(table1));
    assertDeletionSucceeds(backupSystemTable, incrementalTableBackupId, false);

    assertDeletionSucceeds(backupSystemTable, fullBackupId, true);
  }

  @Test
  public void testDeleteFullBackupNonContinuousTable() throws Exception {
    conf1.setLong(CONF_CONTINUOUS_BACKUP_PITR_WINDOW_DAYS, 30);
    backupSystemTable = new BackupSystemTable(TEST_UTIL.getConnection());

    long now = System.currentTimeMillis();
    String backupId =
      createAndUpdateBackup(now - ONE_DAY_IN_MILLISECONDS, now - ONE_DAY_IN_MILLISECONDS + 1000);
    assertDeletionSucceeds(backupSystemTable, backupId, false);
  }

  /**
   * Creates a full backup and updates its timestamps.
   */
  private String createAndUpdateBackup(long startTs, long completeTs) throws Exception {
    String backupId = fullTableBackup(Lists.newArrayList(table1));
    assertTrue(checkSucceeded(backupId));

    BackupInfo backupInfo = getBackupInfoById(backupId);
    backupInfo.setStartTs(startTs);
    backupInfo.setCompleteTs(completeTs);
    backupSystemTable.updateBackupInfo(backupInfo);

    return backupId;
  }

  private void assertDeletionSucceeds(BackupSystemTable table, String backupId,
    boolean isForceDelete) throws Exception {
    int ret = deleteBackup(backupId, isForceDelete);
    assertEquals(0, ret);
    assertFalse("Backup should be deleted but still exists!", backupExists(table, backupId));
  }

  private void assertDeletionFails(BackupSystemTable table, String backupId) throws Exception {
    int ret = deleteBackup(backupId, false);
    assertNotEquals(0, ret);
    assertTrue("Backup should still exist after failed deletion!", backupExists(table, backupId));
  }

  private boolean backupExists(BackupSystemTable table, String backupId) throws Exception {
    return table.getBackupHistory().stream()
      .anyMatch(backup -> backup.getBackupId().equals(backupId));
  }

  private int deleteBackup(String backupId, boolean isForceDelete) throws Exception {
    String[] args = buildBackupDeleteArgs(backupId, isForceDelete);
    return ToolRunner.run(conf1, new BackupDriver(), args);
  }

  private String[] buildBackupDeleteArgs(String backupId, boolean isForceDelete) {
    return isForceDelete
      ? new String[] { "delete", "-l", backupId, "-fd" }
      : new String[] { "delete", "-l", backupId };
  }

  private BackupInfo getBackupInfoById(String backupId) throws IOException {
    return backupSystemTable.getBackupInfos(BackupInfo.BackupState.COMPLETE).stream()
      .filter(b -> b.getBackupId().equals(backupId)).findFirst()
      .orElseThrow(() -> new IllegalStateException("Backup should exist: " + backupId));
  }
}
