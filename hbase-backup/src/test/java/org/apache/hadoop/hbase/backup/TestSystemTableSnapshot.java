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

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(LargeTests.class)
public class TestSystemTableSnapshot extends TestBackupBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSystemTableSnapshot.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestSystemTableSnapshot.class);

  /**
   * Verify backup system table snapshot.
   *
   * @throws Exception if an operation on the table fails
   */
 // @Test
  public void _testBackupRestoreSystemTable() throws Exception {
    LOG.info("test snapshot system table");

    TableName backupSystem = BackupSystemTable.getTableName(conf1);

    Admin hba = TEST_UTIL.getAdmin();
    String snapshotName = "sysTable";
    hba.snapshot(snapshotName, backupSystem);

    hba.disableTable(backupSystem);
    hba.restoreSnapshot(snapshotName);
    hba.enableTable(backupSystem);
    hba.close();
  }
}
