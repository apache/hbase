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
public class TestRemoteBackup extends TestBackupBase {

  private static final Log LOG = LogFactory.getLog(TestRemoteBackup.class);

  /**
   * Verify that a remote full backup is created on a single table with data correctly.
   * @throws Exception
   */
  @Test
  public void testFullBackupRemote() throws Exception {

    LOG.info("test remote full backup on a single table");

    // String rootdir = TEST_UTIL2.getDefaultRootDirPath() + BACKUP_ROOT_DIR;
    // LOG.info("ROOTDIR " + rootdir);
    String backupId =
        BackupClient.create("full", BACKUP_REMOTE_ROOT_DIR, table1.getNameAsString(), null);
    LOG.info("backup complete");
    assertTrue(checkSucceeded(backupId));
  }

}