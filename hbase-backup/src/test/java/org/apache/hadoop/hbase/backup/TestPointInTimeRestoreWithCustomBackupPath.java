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

import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.CONF_CONTINUOUS_BACKUP_WAL_DIR;
import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupReplicationEndpoint.ONE_DAY_IN_MILLISECONDS;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestPointInTimeRestoreWithCustomBackupPath extends TestBackupBase {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestPointInTimeRestoreWithCustomBackupPath.class);

  private static final String backupWalDirName = "TestCustomBackupWalDir";
  private static final String customBackupDirName = "CustomBackupRoot";

  private static Path backupWalDir;
  private static Path customBackupDir;
  private static FileSystem fs;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Path root = TEST_UTIL.getDataTestDirOnTestFS();
    backupWalDir = new Path(root, backupWalDirName);
    customBackupDir = new Path(root, customBackupDirName);

    fs = FileSystem.get(conf1);
    fs.mkdirs(backupWalDir);
    fs.mkdirs(customBackupDir);

    conf1.set(CONF_CONTINUOUS_BACKUP_WAL_DIR, backupWalDir.toString());

    createAndCopyBackupData();
  }

  private static void createAndCopyBackupData() throws Exception {
    // Simulate time 10 days ago
    EnvironmentEdgeManager
      .injectEdge(() -> System.currentTimeMillis() - 10 * ONE_DAY_IN_MILLISECONDS);
    PITRTestUtil.loadRandomData(TEST_UTIL, table1, famName, 1000);

    // Perform backup with continuous backup enabled
    String[] args =
      PITRTestUtil.buildBackupArgs("full", new TableName[] { table1 }, true, BACKUP_ROOT_DIR);
    int ret = ToolRunner.run(conf1, new BackupDriver(), args);
    assertEquals("Backup should succeed", 0, ret);

    PITRTestUtil.waitForReplication();

    // Copy the contents of BACKUP_ROOT_DIR to the new customBackupDir
    Path defaultBackupDir = new Path(BACKUP_ROOT_DIR);
    for (FileStatus status : fs.listStatus(defaultBackupDir)) {
      Path dst = new Path(customBackupDir, status.getPath().getName());
      FileUtil.copy(fs, status, fs, dst, true, false, conf1);
    }

    EnvironmentEdgeManager.reset();
  }

  @AfterClass
  public static void cleanupAfterClass() throws IOException {
    if (fs.exists(backupWalDir)) {
      fs.delete(backupWalDir, true);
    }
    if (fs.exists(customBackupDir)) {
      fs.delete(customBackupDir, true);
    }

    conf1.unset(CONF_CONTINUOUS_BACKUP_WAL_DIR);
  }

  @Test
  public void testPITR_FromCustomBackupRootDir() throws Exception {
    TableName restoredTable = TableName.valueOf("restoredTableCustomPath");

    long restoreTime = EnvironmentEdgeManager.currentTime() - 2 * ONE_DAY_IN_MILLISECONDS;

    String[] args = PITRTestUtil.buildPITRArgs(new TableName[] { table1 },
      new TableName[] { restoredTable }, restoreTime, customBackupDir.toString());

    int ret = ToolRunner.run(conf1, new PointInTimeRestoreDriver(), args);
    assertEquals("PITR should succeed with custom backup root dir", 0, ret);

    // Validate that the restored table has same row count
    assertEquals("Restored table should match row count",
      PITRTestUtil.getRowCount(TEST_UTIL, table1),
      PITRTestUtil.getRowCount(TEST_UTIL, restoredTable));
  }
}
