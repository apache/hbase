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
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Random;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.backup.impl.BackupManager;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.util.ToolRunner;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(LargeTests.class)

public class TestParallelBackup extends TestBackupBase {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestParallelBackup.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestParallelBackup.class);

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL = new HBaseTestingUtil();
    conf1 = TEST_UTIL.getConfiguration();
    conf1.setInt(BackupManager.BACKUP_EXCLUSIVE_OPERATION_TIMEOUT_SECONDS_KEY, 3);
    setUpHelper();
  }

  @Test
  public void testParallelFullBackupOnDifferentTable() throws Exception {
    int before;
    String tableName1 = table1.getNameAsString();
    String tableName2 = table2.getNameAsString();
    try (BackupSystemTable table = new BackupSystemTable(TEST_UTIL.getConnection())) {
      before = table.getBackupHistory().size();
    }

    Thread backupThread1 = new Thread(new RunFullBackup(tableName1));
    Thread backupThread2 = new Thread(new RunFullBackup(tableName2));

    backupThread1.start();
    backupThread2.start();

    backupThread1.join();
    backupThread2.join();

    try (BackupSystemTable table = new BackupSystemTable(TEST_UTIL.getConnection())) {
      List<BackupInfo> backups = table.getBackupHistory();
      int after = table.getBackupHistory().size();
      assertEquals(after, before + 2);
      for (BackupInfo data : backups) {
        String backupId = data.getBackupId();
        assertTrue(checkSucceeded(backupId));
      }
      LOG.info("backup complete");
    }
  }

  @Test
  public void testParallelFullBackupOnSameTable() throws Exception {
    int before;
    String tableNames = table1.getNameAsString() + "," + table2.getNameAsString();
    try (BackupSystemTable table = new BackupSystemTable(TEST_UTIL.getConnection())) {
      before = table.getBackupHistory().size();
    }

    Thread backupThread1 = new Thread(new RunFullBackup(tableNames));
    Thread backupThread2 = new Thread(new RunFullBackup(tableNames));

    backupThread1.start();
    backupThread2.start();

    backupThread1.join();
    backupThread2.join();

    try (BackupSystemTable table = new BackupSystemTable(TEST_UTIL.getConnection())) {
      List<BackupInfo> backups = table.getBackupHistory();
      int after = table.getBackupHistory().size();
      assertEquals(after, before + 1);
      for (BackupInfo data : backups) {
        String backupId = data.getBackupId();
        assertTrue(checkSucceeded(backupId));
      }
      LOG.info("backup complete");
    }
  }

  @Test
  public void testParallelFullBackupOnMultipleTable() throws Exception {
    int before;
    String tableNames1 = table1.getNameAsString() + "," + table2.getNameAsString();
    String tableNames2 = table1.getNameAsString();
    try (BackupSystemTable table = new BackupSystemTable(TEST_UTIL.getConnection())) {
      before = table.getBackupHistory().size();
    }

    Thread backupThread1 = new Thread(new RunFullBackup(tableNames1));
    Thread backupThread2 = new Thread(new RunFullBackup(tableNames2));

    backupThread1.start();
    backupThread2.start();

    backupThread1.join();
    backupThread2.join();

    try (BackupSystemTable table = new BackupSystemTable(TEST_UTIL.getConnection())) {
      List<BackupInfo> backups = table.getBackupHistory();
      int after = table.getBackupHistory().size();
      assertEquals(after, before + 1);
      for (BackupInfo data : backups) {
        String backupId = data.getBackupId();
        assertTrue(checkSucceeded(backupId));
      }
      LOG.info("backup complete");
    }
  }

  static class RunFullBackup implements Runnable {
    String tableNames;

    RunFullBackup(String tableNames) {
      this.tableNames = tableNames;
    }

    @Override
    public void run() {
      try {
        String[] args = new String[] { "create", "full", BACKUP_ROOT_DIR, "-t", tableNames };
        Threads.sleep(new Random().nextInt(500));
        int ret = ToolRunner.run(conf1, new BackupDriver(), args);
        assertTrue(ret == 0);
      } catch (Exception e) {
        LOG.error("Failure with exception: " + e.getMessage(), e);
      }
    }
  }
}
