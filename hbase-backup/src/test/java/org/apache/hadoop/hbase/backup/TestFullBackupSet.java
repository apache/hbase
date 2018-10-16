/**
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
import static org.junit.Assert.assertTrue;

import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.util.ToolRunner;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(LargeTests.class)
public class TestFullBackupSet extends TestBackupBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestFullBackupSet.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestFullBackupSet.class);

  /**
   * Verify that full backup is created on a single table with data correctly.
   *
   * @throws Exception if doing the backup or an operation on the tables fails
   */
  @Test
  public void testFullBackupSetExist() throws Exception {
    LOG.info("Test full backup, backup set exists");

    // Create set
    try (BackupSystemTable table = new BackupSystemTable(TEST_UTIL.getConnection())) {
      String name = "name";
      table.addToBackupSet(name, new String[] { table1.getNameAsString() });
      List<TableName> names = table.describeBackupSet(name);

      assertNotNull(names);
      assertTrue(names.size() == 1);
      assertTrue(names.get(0).equals(table1));

      String[] args = new String[] { "create", "full", BACKUP_ROOT_DIR, "-s", name };
      // Run backup
      int ret = ToolRunner.run(conf1, new BackupDriver(), args);
      assertTrue(ret == 0);
      List<BackupInfo> backups = table.getBackupHistory();
      assertTrue(backups.size() == 1);
      String backupId = backups.get(0).getBackupId();
      assertTrue(checkSucceeded(backupId));

      LOG.info("backup complete");

      // Restore from set into other table
      args =
          new String[] { BACKUP_ROOT_DIR, backupId, "-s", name, "-m",
              table1_restore.getNameAsString(), "-o" };
      // Run backup
      ret = ToolRunner.run(conf1, new RestoreDriver(), args);
      assertTrue(ret == 0);
      HBaseAdmin hba = TEST_UTIL.getHBaseAdmin();
      assertTrue(hba.tableExists(table1_restore));
      // Verify number of rows in both tables
      assertEquals(TEST_UTIL.countRows(table1), TEST_UTIL.countRows(table1_restore));
      TEST_UTIL.deleteTable(table1_restore);
      LOG.info("restore into other table is complete");
      hba.close();
    }
  }

  @Test
  public void testFullBackupSetDoesNotExist() throws Exception {
    LOG.info("test full backup, backup set does not exist");
    String name = "name1";
    String[] args = new String[] { "create", "full", BACKUP_ROOT_DIR, "-s", name };
    // Run backup
    int ret = ToolRunner.run(conf1, new BackupDriver(), args);
    assertTrue(ret != 0);
  }
}
