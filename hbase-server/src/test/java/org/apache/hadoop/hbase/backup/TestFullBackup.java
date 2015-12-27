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
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestFullBackup extends TestBackupBase {

  private static final Log LOG = LogFactory.getLog(TestFullBackup.class);

  /**
   * Verify that full backup is created on a single table with data correctly.
   * @throws Exception
   */
  @Test
  public void testFullBackupSingle() throws Exception {

    LOG.info("test full backup on a single table with data");
    String backupId =
        BackupClient.create("full", BACKUP_ROOT_DIR, table1.getNameAsString(), null);
    LOG.info("backup complete");
    assertTrue(checkSucceeded(backupId));
  }

  /**
   * Verify that full backup is created on multiple tables correctly.
   * @throws Exception
   */
  @Test
  public void testFullBackupMultiple() throws Exception {
    LOG.info("create full backup image on multiple tables with data");
    String tableset =
        table1.getNameAsString() + BackupRestoreConstants.TABLENAME_DELIMITER_IN_COMMAND
            + table2.getNameAsString();
    String backupId = BackupClient.create("full", BACKUP_ROOT_DIR, tableset, null);
    assertTrue(checkSucceeded(backupId));

  }

  /**
   * Verify that full backup is created on all tables correctly.
   * @throws Exception
   */
  @Test
  public void testFullBackupAll() throws Exception {
    LOG.info("create full backup image on all tables");
    String backupId = BackupClient.create("full", BACKUP_ROOT_DIR, null, null);
    assertTrue(checkSucceeded(backupId));

  }

  /**
   * Verify that full backup is created on a table correctly using a snapshot.
   * @throws Exception
   */
  //@Test
  //public void testFullBackupUsingSnapshot() throws Exception {
   // HBaseAdmin hba = new HBaseAdmin(conf1);
    //String snapshot = "snapshot";
    //hba.snapshot(snapshot, table1);
    //LOG.info("create full backup image on a table using snapshot");
    //String backupId =
    //    BackupClient.create("full", BACKUP_ROOT_DIR, table1.getNameAsString(),
    //      snapshot);
  // }

}