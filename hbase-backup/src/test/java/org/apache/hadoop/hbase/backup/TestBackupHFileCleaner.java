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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MasterTests.class, SmallTests.class })
public class TestBackupHFileCleaner {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBackupHFileCleaner.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestBackupHFileCleaner.class);
  private final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private final static Configuration conf = TEST_UTIL.getConfiguration();
  private final static TableName tableNameWithBackup = TableName.valueOf("backup.hfile.cleaner");
  private final static TableName tableNameWithoutBackup =
    TableName.valueOf("backup.hfile.cleaner2");

  private static FileSystem fs = null;

  private Path root;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf.setBoolean(BackupRestoreConstants.BACKUP_ENABLE_KEY, true);
    TEST_UTIL.startMiniCluster(1);
    fs = FileSystem.get(conf);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setup() throws IOException {
    root = TEST_UTIL.getDataTestDirOnTestFS();
  }

  @After
  public void cleanup() {
    try {
      fs.delete(root, true);
    } catch (IOException e) {
      LOG.warn("Failed to delete files recursively from path " + root);
    }
  }

  @Test
  public void testGetDeletableFiles() throws IOException {
    FileStatus file1 = createFile("file1");
    FileStatus file1Archived = createFile("archived/file1");
    FileStatus file2 = createFile("file2");
    FileStatus file3 = createFile("file3");

    BackupHFileCleaner cleaner = new BackupHFileCleaner() {
      @Override
      protected Set<TableName> fetchFullyBackedUpTables(BackupSystemTable tbl) {
        return Set.of(tableNameWithBackup);
      }
    };
    cleaner.setConf(conf);

    Iterable<FileStatus> deletable;

    // The first call will not allow any deletions because of the timestamp mechanism.
    deletable = cleaner.getDeletableFiles(List.of(file1, file1Archived, file2, file3));
    assertEquals(Set.of(), Sets.newHashSet(deletable));

    // No bulk loads registered, so all files can be deleted.
    deletable = cleaner.getDeletableFiles(List.of(file1, file1Archived, file2, file3));
    assertEquals(Set.of(file1, file1Archived, file2, file3), Sets.newHashSet(deletable));

    // Register some bulk loads.
    try (BackupSystemTable backupSystem = new BackupSystemTable(TEST_UTIL.getConnection())) {
      byte[] unused = new byte[] { 0 };
      backupSystem.registerBulkLoad(tableNameWithBackup, unused,
        Map.of(unused, List.of(file1.getPath())));
      backupSystem.registerBulkLoad(tableNameWithoutBackup, unused,
        Map.of(unused, List.of(file2.getPath())));
    }

    // File 1 can no longer be deleted, because it is registered as a bulk load.
    deletable = cleaner.getDeletableFiles(List.of(file1, file1Archived, file2, file3));
    assertEquals(Set.of(file2, file3), Sets.newHashSet(deletable));
  }

  private FileStatus createFile(String fileName) throws IOException {
    Path file = new Path(root, fileName);
    fs.createNewFile(file);
    assertTrue("Test file not created!", fs.exists(file));
    return fs.getFileStatus(file);
  }
}
