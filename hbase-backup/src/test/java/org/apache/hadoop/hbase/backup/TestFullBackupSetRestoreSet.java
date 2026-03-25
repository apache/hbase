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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.util.ToolRunner;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(LargeTests.TAG)
public class TestFullBackupSetRestoreSet extends TestBackupBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestFullBackupSetRestoreSet.class);

  @Test
  public void testFullRestoreSetToOtherTable() throws Exception {

    LOG.info("Test full restore set");

    // Create set
    try (BackupSystemTable table = new BackupSystemTable(TEST_UTIL.getConnection())) {
      String name = "name";
      table.addToBackupSet(name, new String[] { table1.getNameAsString() });
      List<TableName> names = table.describeBackupSet(name);

      assertNotNull(names);
      assertEquals(1, names.size());
      assertTrue(names.get(0).equals(table1));

      String[] args = new String[] { "create", "full", BACKUP_ROOT_DIR, "-s", name };
      // Run backup
      int ret = ToolRunner.run(conf1, new BackupDriver(), args);
      assertEquals(0, ret);
      List<BackupInfo> backups = table.getBackupHistory();
      assertEquals(1, backups.size());
      String backupId = backups.get(0).getBackupId();
      assertTrue(checkSucceeded(backupId));

      LOG.info("backup complete");

      // Restore from set into other table
      args = new String[] { BACKUP_ROOT_DIR, backupId, "-s", name, "-m",
        table1_restore.getNameAsString(), "-o" };
      // Run backup
      ret = ToolRunner.run(conf1, new RestoreDriver(), args);
      assertEquals(0, ret);
      Admin hba = TEST_UTIL.getAdmin();
      assertTrue(hba.tableExists(table1_restore));
      // Verify number of rows in both tables
      assertEquals(TEST_UTIL.countRows(table1), TEST_UTIL.countRows(table1_restore));
      TEST_UTIL.deleteTable(table1_restore);
      LOG.info("restore into other table is complete");
      hba.close();
    }
  }

  @Test
  public void testFullRestoreSetToSameTable() throws Exception {

    LOG.info("Test full restore set to same table");

    // Create set
    try (BackupSystemTable table = new BackupSystemTable(TEST_UTIL.getConnection())) {
      String name = "name1";
      table.addToBackupSet(name, new String[] { table1.getNameAsString() });
      List<TableName> names = table.describeBackupSet(name);

      assertNotNull(names);
      assertEquals(1, names.size());
      assertTrue(names.get(0).equals(table1));

      String[] args = new String[] { "create", "full", BACKUP_ROOT_DIR, "-s", name };
      // Run backup
      int ret = ToolRunner.run(conf1, new BackupDriver(), args);
      assertEquals(0, ret);
      List<BackupInfo> backups = table.getBackupHistory();
      String backupId = backups.get(0).getBackupId();
      assertTrue(checkSucceeded(backupId));

      LOG.info("backup complete");
      int count = TEST_UTIL.countRows(table1);
      TEST_UTIL.deleteTable(table1);

      // Restore from set into other table
      args = new String[] { BACKUP_ROOT_DIR, backupId, "-s", name, "-o" };
      // Run backup
      ret = ToolRunner.run(conf1, new RestoreDriver(), args);
      assertEquals(0, ret);
      Admin hba = TEST_UTIL.getAdmin();
      assertTrue(hba.tableExists(table1));
      // Verify number of rows in both tables
      assertEquals(count, TEST_UTIL.countRows(table1));
      LOG.info("restore into same table is complete");
      hba.close();

    }

  }

}
