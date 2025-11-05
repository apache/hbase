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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.backup.impl.BackupManager;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An integration test to detect regressions in HBASE-7912. Create a table with many regions, load
 * data, perform series backup/load operations, then restore and verify data
 * @see <a href="https://issues.apache.org/jira/browse/HBASE-7912">HBASE-7912</a>
 * @see <a href="https://issues.apache.org/jira/browse/HBASE-14123">HBASE-14123</a>
 */
@Category(IntegrationTests.class)
public class IntegrationTestBackupRestore extends IntegrationTestBackupRestoreBase {
  private static final String CLASS_NAME = IntegrationTestBackupRestore.class.getSimpleName();
  protected static final Logger LOG = LoggerFactory.getLogger(IntegrationTestBackupRestore.class);

  @Override
  @Before
  public void setUp() throws Exception {
    util = new IntegrationTestingUtility();
    conf = util.getConfiguration();
    BackupTestUtil.enableBackup(conf);
    LOG.info("Initializing cluster with {} region servers.", regionServerCount);
    util.initializeCluster(regionServerCount);
    LOG.info("Cluster initialized and ready");

    backupRootDir = util.getDataTestDirOnTestFS() + Path.SEPARATOR + DEFAULT_BACKUP_ROOT_DIR;
    LOG.info("The backup root directory is: {}", backupRootDir);
    fs = FileSystem.get(conf);
  }

  @Test
  public void testBackupRestore() throws Exception {
    LOG.info("Running backup and restore integration test with continuous backup disabled");
    createTables(CLASS_NAME);
    runTestMulti(false);
  }

  /** Returns status of CLI execution */
  @Override
  public int runTestFromCommandLine() throws Exception {
    // Check if backup is enabled
    if (!BackupManager.isBackupEnabled(getConf())) {
      System.err.println(BackupRestoreConstants.ENABLE_BACKUP);
      return -1;
    }
    System.out.println(BackupRestoreConstants.VERIFY_BACKUP);
    testBackupRestore();
    return 0;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    int status = ToolRunner.run(conf, new IntegrationTestBackupRestore(), args);
    System.exit(status);
  }
}
