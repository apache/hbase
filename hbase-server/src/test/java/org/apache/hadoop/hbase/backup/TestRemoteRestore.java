/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.hbase.backup;

import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestRemoteRestore extends TestBackupBase {

  private static final Log LOG = LogFactory.getLog(TestRemoteRestore.class);

  /**
   * Verify that a remote restore on a single table is successful.
   * @throws Exception
   */
  @Test
  public void testFullRestoreRemote() throws Exception {

    LOG.info("test remote full backup on a single table");
    String backupId =
        BackupClient.create("full", BACKUP_REMOTE_ROOT_DIR, table1.getNameAsString(), null);
    LOG.info("backup complete");
    assertTrue(checkSucceeded(backupId));
    String[] tableset = new String[] { table1.getNameAsString() };
    String[] tablemap = new String[] { table1_restore };
    Path path = new Path(BACKUP_REMOTE_ROOT_DIR);
    HBackupFileSystem hbfs = new HBackupFileSystem(conf1, path, backupId);
    RestoreClient.restore_stage1(hbfs, BACKUP_REMOTE_ROOT_DIR, backupId, false, false, tableset,
      tablemap, false);
    HBaseAdmin hba = TEST_UTIL.getHBaseAdmin();
    assertTrue(hba.tableExists(TableName.valueOf(table1_restore)));
    TEST_UTIL.deleteTable(TableName.valueOf(table1_restore));
    hba.close();
  }

}