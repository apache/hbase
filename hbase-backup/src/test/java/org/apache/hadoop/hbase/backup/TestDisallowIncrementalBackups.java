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
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupAdminImpl;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.backup.impl.FullTableBackupClient;
import org.apache.hadoop.hbase.backup.impl.IncrementalBackupsDisallowedException;
import org.apache.hadoop.hbase.backup.impl.IncrementalTableBackupClient;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@Category(LargeTests.class)
@RunWith(Parameterized.class)
public class TestDisallowIncrementalBackups extends TestBackupBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestDisallowIncrementalBackups.class);

  public TestDisallowIncrementalBackups(Boolean b) {
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    provider = "multiwal";
    List<Object[]> params = new ArrayList<>();
    params.add(new Object[] { Boolean.TRUE });
    return params;
  }

  @Test
  public void TestItDisallowsIncrementalBackups() throws IOException {
    List<TableName> tables = Lists.newArrayList(table1, table2);

    try (Connection conn = ConnectionFactory.createConnection(conf1);
      BackupSystemTable backupSystemTable = new BackupSystemTable(conn)) {
      BackupAdminImpl client = new BackupAdminImpl(conn);

      insertIntoTable(conn, table1, famName, 1, 10);
      BackupRequest req = createBackupRequest(BackupType.FULL, tables, BACKUP_ROOT_DIR);
      checkSucceeded(client.backupTables(req));
      // Invalidate the backup
      conn.getAdmin().disableTable(table1);
      conn.getAdmin().truncateTable(table1, true);
      req = createBackupRequest(BackupType.INCREMENTAL, tables, BACKUP_ROOT_DIR);

      // Should fall back to full table backup class
      assertEquals(BackupClientFactory.create(conn, "backupId", req).getClass(),
        FullTableBackupClient.class);

      // Release the lock created by initializing the TableBackupClient. This lock is taken by
      // instantiating the TableBackupClient when we call BackupClientFactory#create, and is never
      // released because we don't call TableBackupClient#execute
      backupSystemTable.finishBackupExclusiveOperation();

      // should throw error
      final BackupRequest failOnDisallowIncrementalsReq =
        new BackupRequest.Builder().withBackupType(BackupType.INCREMENTAL).withTableList(tables)
          .withTargetRootDir(BACKUP_ROOT_DIR).withNoChecksumVerify(true)
          .withFailOnDisallowedIncrementals(true).build();
      assertThrows(IncrementalBackupsDisallowedException.class,
        () -> BackupClientFactory.create(conn, "backupId", failOnDisallowIncrementalsReq));

      req = createBackupRequest(BackupType.FULL, tables, BACKUP_ROOT_DIR);
      String fullBackupId = client.backupTables(req);
      checkSucceeded(fullBackupId);

      // Check that the backup line allows for incremental backups
      assertEquals(
        BackupClientFactory.create(conn, "backupId", failOnDisallowIncrementalsReq).getClass(),
        IncrementalTableBackupClient.class);
    }
  }
}
