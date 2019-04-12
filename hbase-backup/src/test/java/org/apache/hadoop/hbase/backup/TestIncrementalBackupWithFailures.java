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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupState;
import org.apache.hadoop.hbase.backup.impl.BackupAdminImpl;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.backup.impl.TableBackupClient;
import org.apache.hadoop.hbase.backup.impl.TableBackupClient.Stage;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@Category(LargeTests.class)
@RunWith(Parameterized.class)
public class TestIncrementalBackupWithFailures extends TestBackupBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestIncrementalBackupWithFailures.class);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestIncrementalBackupWithFailures.class);

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    provider = "multiwal";
    List<Object[]> params = new ArrayList<Object[]>();
    params.add(new Object[] { Boolean.TRUE });
    return params;
  }

  public TestIncrementalBackupWithFailures(Boolean b) {
  }

  // implement all test cases in 1 test since incremental backup/restore has dependencies
  @Test
  public void testIncBackupRestore() throws Exception {

    int ADD_ROWS = 99;
    // #1 - create full backup for all tables
    LOG.info("create full backup image for all tables");

    List<TableName> tables = Lists.newArrayList(table1, table2);
    final byte[] fam3Name = Bytes.toBytes("f3");
    table1Desc.addFamily(new HColumnDescriptor(fam3Name));
    HBaseTestingUtility.modifyTableSync(TEST_UTIL.getAdmin(), table1Desc);

    Connection conn = ConnectionFactory.createConnection(conf1);
    int NB_ROWS_FAM3 = 6;
    insertIntoTable(conn, table1, fam3Name, 3, NB_ROWS_FAM3).close();

    Admin admin = conn.getAdmin();
    BackupAdminImpl client = new BackupAdminImpl(conn);

    BackupRequest request = createBackupRequest(BackupType.FULL, tables, BACKUP_ROOT_DIR);
    String backupIdFull = client.backupTables(request);

    assertTrue(checkSucceeded(backupIdFull));

    // #2 - insert some data to table
    Table t1 = insertIntoTable(conn, table1, famName, 1, ADD_ROWS);
    LOG.debug("writing " + ADD_ROWS + " rows to " + table1);

    Assert.assertEquals(TEST_UTIL.countRows(t1), NB_ROWS_IN_BATCH + ADD_ROWS + NB_ROWS_FAM3);
    t1.close();
    LOG.debug("written " + ADD_ROWS + " rows to " + table1);

    Table t2 = conn.getTable(table2);
    Put p2;
    for (int i = 0; i < 5; i++) {
      p2 = new Put(Bytes.toBytes("row-t2" + i));
      p2.addColumn(famName, qualName, Bytes.toBytes("val" + i));
      t2.put(p2);
    }

    Assert.assertEquals(TEST_UTIL.countRows(t2), NB_ROWS_IN_BATCH + 5);
    t2.close();
    LOG.debug("written " + 5 + " rows to " + table2);

    // #3 - incremental backup for multiple tables
    incrementalBackupWithFailures();

    admin.close();
    conn.close();

  }


  private void incrementalBackupWithFailures() throws Exception {
    conf1.set(TableBackupClient.BACKUP_CLIENT_IMPL_CLASS,
      IncrementalTableBackupClientForTest.class.getName());
    int maxStage = Stage.values().length -1;
    // Fail stages between 0 and 4 inclusive
    for (int stage = 0; stage <= maxStage; stage++) {
      LOG.info("Running stage " + stage);
      runBackupAndFailAtStage(stage);
    }
  }

  private void runBackupAndFailAtStage(int stage) throws Exception {

    conf1.setInt(FullTableBackupClientForTest.BACKUP_TEST_MODE_STAGE, stage);
    try (BackupSystemTable table = new BackupSystemTable(TEST_UTIL.getConnection())) {
      int before = table.getBackupHistory().size();
      String[] args =
          new String[] { "create", "incremental", BACKUP_ROOT_DIR, "-t",
              table1.getNameAsString() + "," + table2.getNameAsString() };
      // Run backup
      int ret = ToolRunner.run(conf1, new BackupDriver(), args);
      assertFalse(ret == 0);
      List<BackupInfo> backups = table.getBackupHistory();
      int after = table.getBackupHistory().size();

      assertTrue(after ==  before +1);
      for (BackupInfo data : backups) {
        if(data.getType() == BackupType.FULL) {
          assertTrue(data.getState() == BackupState.COMPLETE);
        } else {
          assertTrue(data.getState() == BackupState.FAILED);
        }
      }
    }
  }

}
