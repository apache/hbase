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

import java.util.List;
import java.util.Set;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@Category(LargeTests.class)
public class TestBackupDeleteWithContinuousBackupAndPITR extends TestBackupBase {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBackupDeleteWithContinuousBackupAndPITR.class);

  private BackupSystemTable backupSystemTable;
  private String backupId1;
  private String backupId2;
  private String backupId3;
  private String backupId4;
  private String backupId5;

  /**
   * Sets up the backup environment before each test.
   * <p>
   * This includes:
   * <ul>
   * <li>Setting a 30-day PITR (Point-In-Time Recovery) window</li>
   * <li>Registering table2 as a continuous backup table starting 40 days ago</li>
   * <li>Creating a mix of full and incremental backups at specific time offsets (using
   * EnvironmentEdge injection) to simulate scenarios like: - backups outside PITR window - valid
   * PITR backups - incomplete PITR chains</li>
   * <li>Resetting the system clock after time manipulation</li>
   * </ul>
   * This setup enables tests to evaluate deletion behavior of backups based on age, table type, and
   * PITR chain requirements.
   */
  @Before
  public void setup() throws Exception {
    conf1.setLong(CONF_CONTINUOUS_BACKUP_PITR_WINDOW_DAYS, 30);
    backupSystemTable = new BackupSystemTable(TEST_UTIL.getConnection());

    long currentTime = System.currentTimeMillis();
    long backupStartTime = currentTime - 40 * ONE_DAY_IN_MILLISECONDS;
    backupSystemTable.addContinuousBackupTableSet(Set.of(table2), backupStartTime);

    backupId1 = fullTableBackup(Lists.newArrayList(table1));
    assertTrue(checkSucceeded(backupId1));

    // 31 days back
    EnvironmentEdgeManager
      .injectEdge(() -> System.currentTimeMillis() - 31 * ONE_DAY_IN_MILLISECONDS);
    backupId2 = fullTableBackup(Lists.newArrayList(table2));
    assertTrue(checkSucceeded(backupId2));

    // 32 days back
    EnvironmentEdgeManager
      .injectEdge(() -> System.currentTimeMillis() - 32 * ONE_DAY_IN_MILLISECONDS);
    backupId3 = fullTableBackup(Lists.newArrayList(table2));
    assertTrue(checkSucceeded(backupId3));

    // 15 days back
    EnvironmentEdgeManager
      .injectEdge(() -> System.currentTimeMillis() - 15 * ONE_DAY_IN_MILLISECONDS);
    backupId4 = fullTableBackup(Lists.newArrayList(table2));
    assertTrue(checkSucceeded(backupId4));

    // Reset clock
    EnvironmentEdgeManager.reset();

    backupId5 = incrementalTableBackup(Lists.newArrayList(table1));
    assertTrue(checkSucceeded(backupId5));
  }

  @After
  public void teardown() throws Exception {
    EnvironmentEdgeManager.reset();
    // Try to delete all backups forcefully if they exist
    for (String id : List.of(backupId1, backupId2, backupId3, backupId4, backupId5)) {
      try {
        deleteBackup(id, true);
      } catch (Exception ignored) {
      }
    }
  }

  @Test
  public void testDeleteIncrementalBackup() throws Exception {
    assertDeletionSucceeds(backupSystemTable, backupId5, false);
  }

  @Test
  public void testDeleteFullBackupNonContinuousTable() throws Exception {
    assertDeletionSucceeds(backupSystemTable, backupId1, false);
  }

  @Test
  public void testDeletePITRIncompleteBackup() throws Exception {
    assertDeletionSucceeds(backupSystemTable, backupId4, false);
  }

  @Test
  public void testDeleteValidPITRBackupWithAnotherPresent() throws Exception {
    assertDeletionSucceeds(backupSystemTable, backupId2, false);
  }

  @Test
  public void testDeleteOnlyValidPITRBackupFails() throws Exception {
    // Delete backupId2 (31 days ago) — this should succeed
    assertDeletionSucceeds(backupSystemTable, backupId2, false);

    // Now backupId3 (32 days ago) is the only remaining PITR backup — deletion should fail
    assertDeletionFails(backupSystemTable, backupId3, false);
  }

  @Test
  public void testForceDeleteOnlyValidPITRBackup() throws Exception {
    // Delete backupId2 (31 days ago)
    assertDeletionSucceeds(backupSystemTable, backupId2, false);

    // Force delete backupId3 — should succeed despite PITR constraints
    assertDeletionSucceeds(backupSystemTable, backupId3, true);
  }

  private void assertDeletionSucceeds(BackupSystemTable table, String backupId,
    boolean isForceDelete) throws Exception {
    int ret = deleteBackup(backupId, isForceDelete);
    assertEquals(0, ret);
    assertFalse("Backup should be deleted but still exists!", backupExists(table, backupId));
  }

  private void assertDeletionFails(BackupSystemTable table, String backupId, boolean isForceDelete)
    throws Exception {
    int ret = deleteBackup(backupId, isForceDelete);
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
}
