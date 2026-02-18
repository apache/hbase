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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.util.ToolRunner;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@Category(LargeTests.class)
public class TestBackupShowHistory extends TestBackupBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBackupShowHistory.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestBackupShowHistory.class);

  /**
   * Verify that backup history retrieval works as expected.
   */
  @Test
  public void testBackupHistory() throws Exception {
    LOG.info("test backup history on a single table with data");

    // Test without backups present
    List<BackupInfo> history = getBackupAdmin().getHistory(10);
    assertEquals(0, history.size());

    history = BackupUtils.getHistory(conf1, 10, new Path(BACKUP_ROOT_DIR));
    assertEquals(0, history.size());

    // Create first backup
    String backupId = fullTableBackup(Lists.newArrayList(table1));
    assertTrue(checkSucceeded(backupId));
    LOG.info("backup complete");

    // Tests with one backup present
    history = getBackupAdmin().getHistory(10);
    assertEquals(1, history.size());
    assertEquals(backupId, history.get(0).getBackupId());

    history = BackupUtils.getHistory(conf1, 10, new Path(BACKUP_ROOT_DIR));
    assertEquals(1, history.size());
    assertEquals(backupId, history.get(0).getBackupId());

    String output = runHistoryCommand(10);
    assertTrue(output.indexOf(backupId) > 0);

    // Create second backup
    String backupId2 = fullTableBackup(Lists.newArrayList(table2));
    assertTrue(checkSucceeded(backupId2));
    LOG.info("backup complete: " + table2);

    // Test with multiple backups
    history = getBackupAdmin().getHistory(10);
    assertEquals(2, history.size());
    assertEquals(backupId2, history.get(0).getBackupId());
    assertEquals(backupId, history.get(1).getBackupId());

    history = BackupUtils.getHistory(conf1, 10, new Path(BACKUP_ROOT_DIR));
    assertEquals(2, history.size());
    assertEquals(backupId2, history.get(0).getBackupId());
    assertEquals(backupId, history.get(1).getBackupId());

    output = runHistoryCommand(10);
    int idx1 = output.indexOf(backupId);
    int idx2 = output.indexOf(backupId2);
    assertTrue(idx1 >= 0); // Backup 1 is listed
    assertTrue(idx2 >= 0); // Backup 2 is listed
    assertTrue(idx2 < idx1); // Newest backup (Backup 2) comes first

    // Test with multiple backups & n == 1
    history = getBackupAdmin().getHistory(1);
    assertEquals(1, history.size());
    assertEquals(backupId2, history.get(0).getBackupId());

    history = BackupUtils.getHistory(conf1, 1, new Path(BACKUP_ROOT_DIR));
    assertEquals(1, history.size());
    assertEquals(backupId2, history.get(0).getBackupId());

    output = runHistoryCommand(1);
    idx1 = output.indexOf(backupId);
    idx2 = output.indexOf(backupId2);
    assertTrue(idx2 > 0); // most recent backup is listed
    assertEquals(-1, idx1); // second most recent backup isn't listed

    // Test with multiple backups & filtering
    BackupInfo.Filter tableNameFilter = i -> i.getTableNames().contains(table1);

    history = getBackupAdmin().getHistory(10, tableNameFilter);
    assertEquals(1, history.size());
    assertEquals(backupId, history.get(0).getBackupId());

    history = BackupUtils.getHistory(conf1, 10, new Path(BACKUP_ROOT_DIR), tableNameFilter);
    assertEquals(1, history.size());
    assertEquals(backupId, history.get(0).getBackupId());

  }

  private String runHistoryCommand(int n) throws Exception {
    String[] args = new String[] { "history", "-n", String.valueOf(n), "-p", BACKUP_ROOT_DIR };
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));

    LOG.info("Running history command");
    int ret = ToolRunner.run(conf1, new BackupDriver(), args);
    assertEquals(0, ret);

    String output = baos.toString();
    LOG.info(output);
    baos.close();
    return output;
  }
}
