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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Set;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.backup.impl.TableBackupClient;
import org.apache.hadoop.hbase.backup.impl.TableBackupClient.Stage;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.util.ToolRunner;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(LargeTests.class)
public class TestFullBackupWithFailures extends TestBackupBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestFullBackupWithFailures.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestFullBackupWithFailures.class);

  @Test
  public void testFullBackupWithFailures() throws Exception {
    conf1.set(TableBackupClient.BACKUP_CLIENT_IMPL_CLASS,
      FullTableBackupClientForTest.class.getName());
    int maxStage = Stage.values().length - 1;
    // Fail stages between 0 and 4 inclusive
    for (int stage = 0; stage <= maxStage; stage++) {
      LOG.info("Running stage " + stage);
      runBackupAndFailAtStage(stage);
    }
  }

  public void runBackupAndFailAtStage(int stage) throws Exception {

    conf1.setInt(FullTableBackupClientForTest.BACKUP_TEST_MODE_STAGE, stage);
    try (BackupSystemTable table = new BackupSystemTable(TEST_UTIL.getConnection())) {
      int before = table.getBackupHistory().size();
      String[] args = new String[] { "create", "full", BACKUP_ROOT_DIR, "-t",
        table1.getNameAsString() + "," + table2.getNameAsString() };
      // Run backup
      int ret = ToolRunner.run(conf1, new BackupDriver(), args);
      assertFalse(ret == 0);
      List<BackupInfo> backups = table.getBackupHistory();
      int after = table.getBackupHistory().size();

      assertTrue(after == before + 1);
      for (BackupInfo data : backups) {
        String backupId = data.getBackupId();
        assertFalse(checkSucceeded(backupId));
      }
      Set<TableName> tables = table.getIncrementalBackupTableSet(BACKUP_ROOT_DIR);
      assertEquals((Stage.stage_4.ordinal() == stage) ? 2 : 0, tables.size());
    }
  }

}
