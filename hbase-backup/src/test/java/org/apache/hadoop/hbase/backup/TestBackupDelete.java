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
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HFileTestUtil;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

@Category(LargeTests.class)
public class TestBackupDelete extends TestBackupBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBackupDelete.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestBackupDelete.class);
  private static final byte[] QUALIFIER = Bytes.toBytes("myqual");
  private static final byte[] FAMILY = Bytes.toBytes("f");

  /**
   * Test basic deletion of a backup when only a single on is present.
   * @throws Exception if doing the backup or an operation on the tables fails
   */
  @Test
  public void testBackupDelete() throws Exception {
    try (BackupSystemTable table = new BackupSystemTable(TEST_UTIL.getConnection())) {
      LOG.info("Test backup delete on a single table with data");
      String backupId = fullTableBackup(List.of(table1));
      assertTrue(checkSucceeded(backupId));
      LOG.info("Backup complete");

      // Verify the backup exists
      BackupInfo info = table.readBackupInfo(backupId);
      Path path = new Path(info.getBackupRootDir(), backupId);
      FileSystem fs = FileSystem.get(path.toUri(), conf1);
      assertTrue(fs.exists(path));

      // Perform a bulk load
      Path dir = TEST_UTIL.getDataTestDirOnTestFS("bulkload");
      Path familyDir = new Path(dir, Bytes.toString(FAMILY));
      byte[] from = Bytes.toBytes("begin");
      byte[] to = Bytes.toBytes("end");
      HFileTestUtil.createHFile(TEST_UTIL.getConfiguration(), fs, new Path(familyDir, "hfile"),
        FAMILY, QUALIFIER, from, to, 5);
      BulkLoadHFiles.create(TEST_UTIL.getConfiguration()).bulkLoad(table1, dir);
      assertEquals(1, table.readBulkloadRows().size());

      // Delete the backup
      int deleted = getBackupAdmin().deleteBackups(new String[] { backupId });
      assertTrue(!fs.exists(path));
      assertTrue(fs.exists(new Path(info.getBackupRootDir())));
      assertEquals(1, deleted);
      assertEquals(0, table.readBulkloadRows().size());
    }
  }

  /**
   * Verify that full backup is created on a single table with data correctly. Verify that history
   * works as expected.
   * @throws Exception if doing the backup or an operation on the tables fails
   */
  @Test
  public void testBackupDeleteCommand() throws Exception {
    LOG.info("test backup delete on a single table with data: command-line");
    List<TableName> tableList = Lists.newArrayList(table1);
    String backupId = fullTableBackup(tableList);
    assertTrue(checkSucceeded(backupId));
    LOG.info("backup complete");
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));

    String[] args = new String[] { "delete", "-l", backupId };
    // Run backup

    try {
      int ret = ToolRunner.run(conf1, new BackupDriver(), args);
      assertTrue(ret == 0);
    } catch (Exception e) {
      LOG.error("failed", e);
    }
    LOG.info("delete_backup");
    String output = baos.toString();
    LOG.info(baos.toString());
    assertTrue(output.indexOf("Deleted 1 backups") >= 0);
  }

  @Test
  public void testBackupPurgeOldBackupsCommand() throws Exception {
    LOG.info("test backup delete (purge old backups) on a single table with data: command-line");
    List<TableName> tableList = Lists.newArrayList(table1);
    EnvironmentEdgeManager.injectEdge(new EnvironmentEdge() {
      // time - 2 days
      @Override
      public long currentTime() {
        return System.currentTimeMillis() - 2 * 24 * 3600 * 1000;
      }
    });
    String backupId = fullTableBackup(tableList);
    assertTrue(checkSucceeded(backupId));

    EnvironmentEdgeManager.reset();

    LOG.info("backup complete");
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));

    // Purge all backups which are older than 3 days
    // Must return 0 (no backups were purged)
    String[] args = new String[] { "delete", "-k", "3" };
    // Run backup

    try {
      int ret = ToolRunner.run(conf1, new BackupDriver(), args);
      assertTrue(ret == 0);
    } catch (Exception e) {
      LOG.error("failed", e);
      Assert.fail(e.getMessage());
    }
    String output = baos.toString();
    LOG.info(baos.toString());
    assertTrue(output.indexOf("Deleted 0 backups") >= 0);

    // Purge all backups which are older than 1 days
    // Must return 1 deleted backup
    args = new String[] { "delete", "-k", "1" };
    // Run backup
    baos.reset();
    try {
      int ret = ToolRunner.run(conf1, new BackupDriver(), args);
      assertTrue(ret == 0);
    } catch (Exception e) {
      LOG.error("failed", e);
      Assert.fail(e.getMessage());
    }
    output = baos.toString();
    LOG.info(baos.toString());
    assertTrue(output.indexOf("Deleted 1 backups") >= 0);
  }

  /**
   * Verify that backup deletion updates the incremental-backup-set.
   */
  @Test
  public void testBackupDeleteUpdatesIncrementalBackupSet() throws Exception {
    LOG.info("Test backup delete updates the incremental backup set");
    BackupSystemTable backupSystemTable = new BackupSystemTable(TEST_UTIL.getConnection());

    String backupId1 = fullTableBackup(Lists.newArrayList(table1, table2));
    assertTrue(checkSucceeded(backupId1));
    assertEquals(Sets.newHashSet(table1, table2),
      backupSystemTable.getIncrementalBackupTableSet(BACKUP_ROOT_DIR));

    String backupId2 = fullTableBackup(Lists.newArrayList(table3));
    assertTrue(checkSucceeded(backupId2));
    assertEquals(Sets.newHashSet(table1, table2, table3),
      backupSystemTable.getIncrementalBackupTableSet(BACKUP_ROOT_DIR));

    getBackupAdmin().deleteBackups(new String[] { backupId1 });
    assertEquals(Sets.newHashSet(table3),
      backupSystemTable.getIncrementalBackupTableSet(BACKUP_ROOT_DIR));
  }
}
