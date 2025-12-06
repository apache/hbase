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

import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.CONF_CONTINUOUS_BACKUP_WAL_DIR;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.OPTION_ENABLE_CONTINUOUS_BACKUP;
import static org.apache.hadoop.hbase.replication.regionserver.ReplicationMarkerChore.REPLICATION_MARKER_ENABLED_DEFAULT;
import static org.apache.hadoop.hbase.replication.regionserver.ReplicationMarkerChore.REPLICATION_MARKER_ENABLED_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupState;
import org.apache.hadoop.hbase.backup.impl.BackupCommands;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ToolRunner;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@Category(LargeTests.class)
public class TestBackupDescribe extends TestBackupBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBackupDescribe.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestBackupDescribe.class);

  /**
   * Verify that describe works as expected if incorrect backup Id is supplied.
   * @throws Exception if creating the {@link BackupDriver} fails
   */
  @Test
  public void testBackupDescribe() throws Exception {
    LOG.info("test backup describe on a single table with data");

    String[] args = new String[] { "describe", "backup_2" };
    int ret = ToolRunner.run(conf1, new BackupDriver(), args);
    assertTrue(ret < 0);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    System.setErr(new PrintStream(baos));
    args = new String[] { "progress" };
    ToolRunner.run(TEST_UTIL.getConfiguration(), new BackupDriver(), args);

    String output = baos.toString();
    LOG.info("Output from progress: " + output);
    assertTrue(output.indexOf(BackupCommands.NO_ACTIVE_SESSION_FOUND) >= 0);
  }

  @Test
  public void testBackupSetCommandWithNonExistentTable() throws Exception {
    String[] args = new String[] { "set", "add", "some_set", "table" };
    // Run backup
    int ret = ToolRunner.run(conf1, new BackupDriver(), args);
    assertNotEquals(ret, 0);
  }

  @Test
  public void testBackupDescribeCommand() throws Exception {
    LOG.info("test backup describe on a single table with data: command-line");

    List<TableName> tableList = Lists.newArrayList(table1);
    String backupId = fullTableBackup(tableList);

    LOG.info("backup complete");
    assertTrue(checkSucceeded(backupId));

    BackupInfo info = getBackupAdmin().getBackupInfo(backupId);
    assertTrue(info.getState() == BackupState.COMPLETE);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));

    String[] args = new String[] { "describe", backupId };

    // Run backup
    int ret = ToolRunner.run(conf1, new BackupDriver(), args);
    assertTrue(ret == 0);
    String response = baos.toString();
    assertTrue(response.indexOf(backupId) > 0);
    assertTrue(response.indexOf("COMPLETE") > 0);
    assertTrue(response.contains("IsContinuous=false"));

    BackupSystemTable table = new BackupSystemTable(TEST_UTIL.getConnection());
    BackupInfo status = table.readBackupInfo(backupId);
    String desc = status.getShortDescription();
    table.close();
    assertTrue(response.indexOf(desc) >= 0);
  }

  @Test
  public void testBackupDescribeCommandForContinuousBackup() throws Exception {
    LOG.info("test backup describe on a single table with data: command-line");
    Path root = TEST_UTIL.getDataTestDirOnTestFS();
    Path backupWalDir = new Path(root, "testBackupDescribeCommand");
    FileSystem fs = FileSystem.get(conf1);
    fs.mkdirs(backupWalDir);
    conf1.set(CONF_CONTINUOUS_BACKUP_WAL_DIR, backupWalDir.toString());
    conf1.setBoolean(REPLICATION_MARKER_ENABLED_KEY, true);

    try (BackupSystemTable table = new BackupSystemTable(TEST_UTIL.getConnection())) {
      // Continuous backup
      String[] backupArgs = new String[] { "create", BackupType.FULL.name(), BACKUP_ROOT_DIR, "-t",
        table1.getNameAsString(), "-" + OPTION_ENABLE_CONTINUOUS_BACKUP };
      int ret = ToolRunner.run(conf1, new BackupDriver(), backupArgs);
      assertEquals("Backup should succeed", 0, ret);
      List<BackupInfo> backups = table.getBackupHistory();
      String backupId = backups.get(0).getBackupId();
      assertTrue(checkSucceeded(backupId));
      LOG.info("backup complete");

      BackupInfo info = getBackupAdmin().getBackupInfo(backupId);
      assertTrue(info.getState() == BackupState.COMPLETE);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      System.setOut(new PrintStream(baos));

      // Run backup describe
      String[] args = new String[] { "describe", backupId };
      ret = ToolRunner.run(conf1, new BackupDriver(), args);
      assertTrue(ret == 0);
      String response = baos.toString();
      assertTrue(response.contains(backupId));
      assertTrue(response.contains("COMPLETE"));
      assertTrue(response.contains("IsContinuous=true"));
      BackupInfo status = table.readBackupInfo(backupId);
      String desc = status.getShortDescription();
      assertTrue(response.contains(desc));

      // load table
      Put p;
      for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
        p = new Put(Bytes.toBytes("row" + i));
        p.addColumn(famName, qualName, Bytes.toBytes("val" + i));
        TEST_UTIL.getConnection().getTable(table1).put(p);
      }
      Thread.sleep(5000);

      // Incremental backup
      backupArgs = new String[] { "create", BackupType.INCREMENTAL.name(), BACKUP_ROOT_DIR, "-t",
        table1.getNameAsString() };
      ret = ToolRunner.run(conf1, new BackupDriver(), backupArgs);
      assertEquals("Incremental Backup should succeed", 0, ret);
      backups = table.getBackupHistory();
      String incrBackupId = backups.get(0).getBackupId();
      assertTrue(checkSucceeded(incrBackupId));
      LOG.info("Incremental backup complete");

      // Run backup describe
      args = new String[] { "describe", incrBackupId };
      ret = ToolRunner.run(conf1, new BackupDriver(), args);
      assertTrue(ret == 0);
      response = baos.toString();
      assertTrue(response.contains(incrBackupId));
      assertTrue(response.contains("COMPLETE"));
      assertTrue(response.contains("Committed WAL time for incremental backup="));
      status = table.readBackupInfo(incrBackupId);
      desc = status.getShortDescription();
      assertTrue(response.contains(desc));
    } finally {
      if (fs.exists(backupWalDir)) {
        fs.delete(backupWalDir, true);
      }
      conf1.unset(CONF_CONTINUOUS_BACKUP_WAL_DIR);
      conf1.setBoolean(REPLICATION_MARKER_ENABLED_KEY, REPLICATION_MARKER_ENABLED_DEFAULT);
    }
  }
}
