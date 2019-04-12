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
public class TestRemoteRestore extends TestBackupBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRemoteRestore.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRemoteRestore.class);

  @Override
  public void setUp() throws Exception {
    useSecondCluster = true;
    super.setUp();
  }

  /**
   * Verify that a remote restore on a single table is successful.
   *
   * @throws Exception if doing the backup or an operation on the tables fails
   */
  @Test
  public void testFullRestoreRemote() throws Exception {
    LOG.info("test remote full backup on a single table");
    String backupId =
        backupTables(BackupType.FULL, toList(table1.getNameAsString()), BACKUP_REMOTE_ROOT_DIR);
    LOG.info("backup complete");
    TableName[] tableset = new TableName[] { table1 };
    TableName[] tablemap = new TableName[] { table1_restore };
    getBackupAdmin().restore(
      BackupUtils.createRestoreRequest(BACKUP_REMOTE_ROOT_DIR, backupId, false, tableset,
        tablemap, false));
    Admin hba = TEST_UTIL.getAdmin();
    assertTrue(hba.tableExists(table1_restore));
    TEST_UTIL.deleteTable(table1_restore);
    hba.close();
  }
}
