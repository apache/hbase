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

import static org.junit.Assert.assertTrue;

import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupAdminImpl;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@Category(LargeTests.class)
public class TestIncrementalBackupWithDataLoss extends TestBackupBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestIncrementalBackupWithDataLoss.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestIncrementalBackupWithDataLoss.class);

  @Test
  public void testFullBackupBreaksDependencyOnOlderBackups() throws Exception {
    LOG.info("test creation of backups after backup data was lost");

    try (Connection conn = ConnectionFactory.createConnection(conf1)) {
      BackupAdminImpl client = new BackupAdminImpl(conn);
      List<TableName> tables = Lists.newArrayList(table1);

      insertIntoTable(conn, table1, famName, 1, 1).close();
      String backup1 =
        client.backupTables(createBackupRequest(BackupType.FULL, tables, BACKUP_ROOT_DIR));
      insertIntoTable(conn, table1, famName, 2, 1).close();
      String backup2 =
        client.backupTables(createBackupRequest(BackupType.INCREMENTAL, tables, BACKUP_ROOT_DIR));

      assertTrue(checkSucceeded(backup1));
      assertTrue(checkSucceeded(backup2));

      // Simulate data loss on the backup storage
      TEST_UTIL.getTestFileSystem().delete(new Path(BACKUP_ROOT_DIR, backup2), true);

      insertIntoTable(conn, table1, famName, 4, 1).close();
      String backup4 =
        client.backupTables(createBackupRequest(BackupType.FULL, tables, BACKUP_ROOT_DIR));
      insertIntoTable(conn, table1, famName, 5, 1).close();
      String backup5 =
        client.backupTables(createBackupRequest(BackupType.INCREMENTAL, tables, BACKUP_ROOT_DIR));
      insertIntoTable(conn, table1, famName, 6, 1).close();
      String backup6 =
        client.backupTables(createBackupRequest(BackupType.INCREMENTAL, tables, BACKUP_ROOT_DIR));

      assertTrue(checkSucceeded(backup4));
      assertTrue(checkSucceeded(backup5));
      assertTrue(checkSucceeded(backup6));
    }
  }

}
