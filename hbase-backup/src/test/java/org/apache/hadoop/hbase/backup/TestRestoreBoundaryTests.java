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

import static org.junit.Assert.assertTrue;

import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(LargeTests.class)
public class TestRestoreBoundaryTests extends TestBackupBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRestoreBoundaryTests.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRestoreBoundaryTests.class);

  /**
   * Verify that a single empty table is restored to a new table.
   *
   * @throws Exception if doing the backup or an operation on the tables fails
   */
  @Test
  public void testFullRestoreSingleEmpty() throws Exception {
    LOG.info("test full restore on a single table empty table");
    String backupId = fullTableBackup(toList(table1.getNameAsString()));
    LOG.info("backup complete");
    TableName[] tableset = new TableName[] { table1 };
    TableName[] tablemap = new TableName[] { table1_restore };
    getBackupAdmin().restore(
      BackupUtils.createRestoreRequest(BACKUP_ROOT_DIR, backupId, false, tableset, tablemap,
        false));
    Admin hba = TEST_UTIL.getAdmin();
    assertTrue(hba.tableExists(table1_restore));
    TEST_UTIL.deleteTable(table1_restore);
  }

  /**
   * Verify that multiple tables are restored to new tables.
   *
   * @throws Exception if doing the backup or an operation on the tables fails
   */
  @Test
  public void testFullRestoreMultipleEmpty() throws Exception {
    LOG.info("create full backup image on multiple tables");

    List<TableName> tables = toList(table2.getNameAsString(), table3.getNameAsString());
    String backupId = fullTableBackup(tables);
    TableName[] restore_tableset = new TableName[] { table2, table3 };
    TableName[] tablemap = new TableName[] { table2_restore, table3_restore };
    getBackupAdmin().restore(
      BackupUtils.createRestoreRequest(BACKUP_ROOT_DIR, backupId, false, restore_tableset,
        tablemap, false));
    Admin hba = TEST_UTIL.getAdmin();
    assertTrue(hba.tableExists(table2_restore));
    assertTrue(hba.tableExists(table3_restore));
    TEST_UTIL.deleteTable(table2_restore);
    TEST_UTIL.deleteTable(table3_restore);
    hba.close();
  }
}
