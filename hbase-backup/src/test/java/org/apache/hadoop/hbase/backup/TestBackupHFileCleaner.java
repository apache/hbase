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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
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
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf = TEST_UTIL.getConfiguration();
  private static TableName tableName = TableName.valueOf("backup.hfile.cleaner");
  private static String famName = "fam";
  static FileSystem fs = null;
  Path root;

  /**
   * @throws Exception if starting the mini cluster or getting the filesystem fails
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf.setBoolean(BackupRestoreConstants.BACKUP_ENABLE_KEY, true);
    TEST_UTIL.startMiniZKCluster();
    TEST_UTIL.startMiniCluster(1);
    fs = FileSystem.get(conf);
  }

  /**
   * @throws Exception if closing the filesystem or shutting down the mini cluster fails
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (fs != null) {
      fs.close();
    }
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
    // 1. Create a file
    Path file = new Path(root, "testIsFileDeletableWithNoHFileRefs");
    fs.createNewFile(file);
    // 2. Assert file is successfully created
    assertTrue("Test file not created!", fs.exists(file));
    BackupHFileCleaner cleaner = new BackupHFileCleaner();
    cleaner.setConf(conf);
    cleaner.setCheckForFullyBackedUpTables(false);
    // 3. Assert that file as is should be deletable
    List<FileStatus> stats = new ArrayList<>();
    FileStatus stat = fs.getFileStatus(file);
    stats.add(stat);
    Iterable<FileStatus> deletable = cleaner.getDeletableFiles(stats);
    deletable = cleaner.getDeletableFiles(stats);
    boolean found = false;
    for (FileStatus stat1 : deletable) {
      if (stat.equals(stat1)) {
        found = true;
      }
    }
    assertTrue("Cleaner should allow to delete this file as there is no hfile reference "
        + "for it.", found);

    // 4. Add the file as bulk load
    List<Path> list = new ArrayList<>(1);
    list.add(file);
    try (Connection conn = ConnectionFactory.createConnection(conf);
        BackupSystemTable sysTbl = new BackupSystemTable(conn)) {
      List<TableName> sTableList = new ArrayList<>();
      sTableList.add(tableName);
      Map<byte[], List<Path>>[] maps = new Map[1];
      maps[0] = new HashMap<>();
      maps[0].put(famName.getBytes(), list);
      sysTbl.writeBulkLoadedFiles(sTableList, maps, "1");
    }

    // 5. Assert file should not be deletable
    deletable = cleaner.getDeletableFiles(stats);
    deletable = cleaner.getDeletableFiles(stats);
    found = false;
    for (FileStatus stat1 : deletable) {
      if (stat.equals(stat1)) {
        found = true;
      }
    }
    assertFalse("Cleaner should not allow to delete this file as there is a hfile reference "
        + "for it.", found);
  }
}
