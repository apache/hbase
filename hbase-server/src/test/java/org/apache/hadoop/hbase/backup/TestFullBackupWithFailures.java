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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupPhase;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupState;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.backup.impl.FullTableBackupClient;
import org.apache.hadoop.hbase.backup.impl.TableBackupClient;
import org.apache.hadoop.hbase.backup.master.LogRollMasterProcedureManager;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.annotations.VisibleForTesting;

@Category(LargeTests.class)
public class TestFullBackupWithFailures extends TestBackupBase {

  private static final Log LOG = LogFactory.getLog(TestFullBackupWithFailures.class);

  static class FullTableBackupClientForTest extends FullTableBackupClient
  {
    public static final String BACKUP_TEST_MODE_STAGE = "backup.test.mode.stage";

    public FullTableBackupClientForTest() {
    }

    public FullTableBackupClientForTest(Connection conn, String backupId, BackupRequest request)
        throws IOException {
      super(conn, backupId, request);
    }

    @Override
    public void execute() throws IOException
    {
      // Get the stage ID to fail on
      try (Admin admin = conn.getAdmin();) {
        // Begin BACKUP
        beginBackup(backupManager, backupInfo);
        failStageIf(0);
        String savedStartCode = null;
        boolean firstBackup = false;
        // do snapshot for full table backup
        savedStartCode = backupManager.readBackupStartCode();
        firstBackup = savedStartCode == null || Long.parseLong(savedStartCode) == 0L;
        if (firstBackup) {
          // This is our first backup. Let's put some marker to system table so that we can hold the logs
          // while we do the backup.
          backupManager.writeBackupStartCode(0L);
        }
        failStageIf(1);
        // We roll log here before we do the snapshot. It is possible there is duplicate data
        // in the log that is already in the snapshot. But if we do it after the snapshot, we
        // could have data loss.
        // A better approach is to do the roll log on each RS in the same global procedure as
        // the snapshot.
        LOG.info("Execute roll log procedure for full backup ...");

        Map<String, String> props = new HashMap<String, String>();
        props.put("backupRoot", backupInfo.getBackupRootDir());
        admin.execProcedure(LogRollMasterProcedureManager.ROLLLOG_PROCEDURE_SIGNATURE,
          LogRollMasterProcedureManager.ROLLLOG_PROCEDURE_NAME, props);
        failStageIf(2);
        newTimestamps = backupManager.readRegionServerLastLogRollResult();
        if (firstBackup) {
          // Updates registered log files
          // We record ALL old WAL files as registered, because
          // this is a first full backup in the system and these
          // files are not needed for next incremental backup
          List<String> logFiles = BackupUtils.getWALFilesOlderThan(conf, newTimestamps);
          backupManager.recordWALFiles(logFiles);
        }

        // SNAPSHOT_TABLES:
        backupInfo.setPhase(BackupPhase.SNAPSHOT);
        for (TableName tableName : tableList) {
          String snapshotName =
              "snapshot_" + Long.toString(EnvironmentEdgeManager.currentTime()) + "_"
                  + tableName.getNamespaceAsString() + "_" + tableName.getQualifierAsString();

          snapshotTable(admin, tableName, snapshotName);
          backupInfo.setSnapshotName(tableName, snapshotName);
        }
        failStageIf(3);
        // SNAPSHOT_COPY:
        // do snapshot copy
        LOG.debug("snapshot copy for " + backupId);
        snapshotCopy(backupInfo);
        // Updates incremental backup table set
        backupManager.addIncrementalBackupTableSet(backupInfo.getTables());

        // BACKUP_COMPLETE:
        // set overall backup status: complete. Here we make sure to complete the backup.
        // After this checkpoint, even if entering cancel process, will let the backup finished
        backupInfo.setState(BackupState.COMPLETE);
        // The table list in backupInfo is good for both full backup and incremental backup.
        // For incremental backup, it contains the incremental backup table set.
        backupManager.writeRegionServerLogTimestamp(backupInfo.getTables(), newTimestamps);

        HashMap<TableName, HashMap<String, Long>> newTableSetTimestampMap =
            backupManager.readLogTimestampMap();

        Long newStartCode =
            BackupUtils.getMinValue(BackupUtils
                .getRSLogTimestampMins(newTableSetTimestampMap));
        backupManager.writeBackupStartCode(newStartCode);
        failStageIf(4);
        // backup complete
        completeBackup(conn, backupInfo, backupManager, BackupType.FULL, conf);

      } catch (Exception e) {
        failBackup(conn, backupInfo, backupManager, e, "Unexpected BackupException : ",
          BackupType.FULL, conf);
        throw new IOException(e);
      }

    }



    @VisibleForTesting
    protected int getTestStageId() {
      return conf.getInt(BACKUP_TEST_MODE_STAGE, 0);
    }

    @VisibleForTesting

    protected void failStageIf(int stage) throws IOException {
      int current = getTestStageId();
      if (current == stage) {
        throw new IOException("Failed stage " + stage+" in testing");
      }
    }

  }

  @Test
  public void testFullBackupWithFailures() throws Exception {
    conf1.set(TableBackupClient.BACKUP_CLIENT_IMPL_CLASS,
      FullTableBackupClientForTest.class.getName());
    int stage = (new Random()).nextInt(5);
    // Fail random stage between 0 and 4 inclusive
    LOG.info("Running stage " + stage);
    runBackupAndFailAtStage(stage);
  }

  public void runBackupAndFailAtStage(int stage) throws Exception {

    conf1.setInt(FullTableBackupClientForTest.BACKUP_TEST_MODE_STAGE, stage);
    try (BackupSystemTable table = new BackupSystemTable(TEST_UTIL.getConnection())) {
      int before = table.getBackupHistory().size();
      String[] args =
          new String[] { "create", "full", BACKUP_ROOT_DIR, "-t",
              table1.getNameAsString() + "," + table2.getNameAsString() };
      // Run backup
      int ret = ToolRunner.run(conf1, new BackupDriver(), args);
      assertFalse(ret == 0);
      List<BackupInfo> backups = table.getBackupHistory();
      int after = table.getBackupHistory().size();

      assertTrue(after ==  before +1);
      for (BackupInfo data : backups) {
        String backupId = data.getBackupId();
        assertFalse(checkSucceeded(backupId));
      }
      Set<TableName> tables = table.getIncrementalBackupTableSet(BACKUP_ROOT_DIR);
      assertTrue(tables.size() == 0);
    }
  }


}