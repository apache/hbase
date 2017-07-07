/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.backup;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.com.google.common.collect.Lists;

@Category(LargeTests.class)
public class TestBackupDelete extends TestBackupBase {

  private static final Log LOG = LogFactory.getLog(TestBackupDelete.class);

  /**
   * Verify that full backup is created on a single table with data correctly. Verify that history
   * works as expected
   * @throws Exception
   */
  @Test
  public void testBackupDelete() throws Exception {
    LOG.info("test backup delete on a single table with data");
    List<TableName> tableList = Lists.newArrayList(table1);
    String backupId = fullTableBackup(tableList);
    assertTrue(checkSucceeded(backupId));
    LOG.info("backup complete");
    String[] backupIds = new String[] { backupId };
    BackupSystemTable table = new BackupSystemTable(TEST_UTIL.getConnection());
    BackupInfo info = table.readBackupInfo(backupId);
    Path path = new Path(info.getBackupRootDir(), backupId);
    FileSystem fs = FileSystem.get(path.toUri(), conf1);
    assertTrue(fs.exists(path));
    int deleted = getBackupAdmin().deleteBackups(backupIds);

    assertTrue(!fs.exists(path));
    assertTrue(fs.exists(new Path(info.getBackupRootDir())));
    assertTrue(1 == deleted);
    table.close();
    LOG.info("delete_backup");
  }

  /**
   * Verify that full backup is created on a single table with data correctly. Verify that history
   * works as expected
   * @throws Exception
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

    String[] args = new String[] { "delete", backupId };
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

}
