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
import static org.junit.Assert.assertTrue;

import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@Category(MediumTests.class)
public class TestBackupDeleteRestore extends TestBackupBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestBackupDeleteRestore.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestBackupDeleteRestore.class);

  /**
   * Verify that load data- backup - delete some data - restore works as expected - deleted data get
   * restored.
   *
   * @throws Exception if doing the backup or an operation on the tables fails
   */
  @Test
  public void testBackupDeleteRestore() throws Exception {
    LOG.info("test full restore on a single table empty table");

    List<TableName> tables = Lists.newArrayList(table1);
    String backupId = fullTableBackup(tables);
    assertTrue(checkSucceeded(backupId));
    LOG.info("backup complete");
    int numRows = TEST_UTIL.countRows(table1);
    Admin hba = TEST_UTIL.getAdmin();
    // delete row
    try (Table table = TEST_UTIL.getConnection().getTable(table1)) {
      Delete delete = new Delete(Bytes.toBytes("row0"));
      table.delete(delete);
      hba.flush(table1);
    }

    TableName[] tableset = new TableName[] { table1 };
    TableName[] tablemap = null;// new TableName[] { table1_restore };
    BackupAdmin client = getBackupAdmin();
    client.restore(BackupUtils.createRestoreRequest(BACKUP_ROOT_DIR, backupId, false,
      tableset, tablemap, true));

    int numRowsAfterRestore = TEST_UTIL.countRows(table1);
    assertEquals(numRows, numRowsAfterRestore);
    hba.close();
  }
}
