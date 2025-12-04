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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupAdminImpl;
import org.apache.hadoop.hbase.backup.mapreduce.MapReduceHFileSplitterJob;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.BeforeClass;
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

  /**
   * Setup Cluster with appropriate configurations before running tests.
   * @throws Exception if starting the mini cluster or setting up the tables fails
   */
  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    conf1 = TEST_UTIL.getConfiguration();
    useSecondCluster = true;
    setUpHelper();
  }

  /**
   * Verify that a remote restore on a single table is successful.
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
    getBackupAdmin().restore(BackupUtils.createRestoreRequest(BACKUP_REMOTE_ROOT_DIR, backupId,
      false, tableset, tablemap, false));
    Admin hba = TEST_UTIL.getAdmin();
    assertTrue(hba.tableExists(table1_restore));
    TEST_UTIL.deleteTable(table1_restore);
    hba.close();
  }

  /**
   * Verify that restore jobs can be run on a standalone mapreduce cluster. Ensures hfiles output
   * via {@link MapReduceHFileSplitterJob} exist on correct filesystem.
   * @throws Exception if doing the backup or an operation on the tables fails
   */
  @Test
  public void testFullRestoreRemoteWithAlternateRestoreOutputDir() throws Exception {
    LOG.info("test remote full backup on a single table with alternate restore output dir");
    String backupId =
      backupTables(BackupType.FULL, toList(table1.getNameAsString()), BACKUP_REMOTE_ROOT_DIR);
    LOG.info("backup complete");
    TableName[] tableset = new TableName[] { table1 };
    TableName[] tablemap = new TableName[] { table1_restore };

    HBaseTestingUtility mrTestUtil = new HBaseTestingUtility();
    mrTestUtil.setZkCluster(TEST_UTIL.getZkCluster());
    mrTestUtil.startMiniDFSCluster(3);
    mrTestUtil.startMiniMapReduceCluster();

    Configuration testUtilConf = TEST_UTIL.getConnection().getConfiguration();
    Configuration conf = new Configuration(mrTestUtil.getConfiguration());
    conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT,
      testUtilConf.get(HConstants.ZOOKEEPER_ZNODE_PARENT));
    conf.set(HConstants.MASTER_ADDRS_KEY, testUtilConf.get(HConstants.MASTER_ADDRS_KEY));

    new BackupAdminImpl(ConnectionFactory.createConnection(conf))
      .restore(new RestoreRequest.Builder().withBackupRootDir(BACKUP_REMOTE_ROOT_DIR)
        .withRestoreRootDir(BACKUP_ROOT_DIR).withBackupId(backupId).withCheck(false)
        .withFromTables(tableset).withToTables(tablemap).withOverwrite(false).build());

    Path hfileOutputPath = new Path(
      new Path(conf.get(MapReduceHFileSplitterJob.BULK_OUTPUT_CONF_KEY)).toUri().getPath());

    // files exist on hbase cluster
    FileSystem fileSystem = FileSystem.get(TEST_UTIL.getConfiguration());
    assertTrue(fileSystem.exists(hfileOutputPath));

    // files don't exist on MR cluster
    fileSystem = FileSystem.get(conf);
    assertFalse(fileSystem.exists(hfileOutputPath));

    Admin hba = TEST_UTIL.getAdmin();
    assertTrue(hba.tableExists(table1_restore));
    TEST_UTIL.deleteTable(table1_restore);
    hba.close();
  }
}
