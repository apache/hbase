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
package org.apache.hadoop.hbase.backup.replication;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
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

@Category({ MasterTests.class, SmallTests.class })
public class TestContinuousBackupStagedHFileCleaner {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestContinuousBackupStagedHFileCleaner.class);

  private final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final Configuration conf = TEST_UTIL.getConfiguration();
  static FileSystem fs = null;
  Path root;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    fs = FileSystem.get(conf);
  }

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
  public void cleanup() throws IOException {
    fs.delete(root, true);
  }

  @Test
  public void testGetDeletableFiles() throws IOException {
    // 1. Create a file
    Path file = new Path(root, "testIsFileDeletableWithNoHFileRefs");
    fs.createNewFile(file);

    // 2. Assert file is successfully created
    assertTrue("Test file not created!", fs.exists(file));

    ContinuousBackupStagedHFileCleaner cleaner = new ContinuousBackupStagedHFileCleaner();
    cleaner.setConf(conf);

    List<FileStatus> stats = new ArrayList<>();
    // Prime the cleaner
    stats.add(fs.getFileStatus(file));
    Iterable<FileStatus> deletable = cleaner.getDeletableFiles(stats);

    // 3. Assert that file as is, should be deletable
    boolean found = false;
    for (FileStatus stat1 : deletable) {
      if (stat1.equals(fs.getFileStatus(file))) {
        found = true;
        break;
      }
    }
    assertTrue("File should be deletable as it has no HFile references.", found);

    // 4. Add the file as bulk load
    List<Path> list = new ArrayList<>(1);
    list.add(file);
    String peerId = "peerId";
    Connection conn = ConnectionFactory.createConnection(conf);
    StagedBulkloadFileRegistry stagedBulkloadFileRegistry =
      new StagedBulkloadFileRegistry(conn, peerId);
    stagedBulkloadFileRegistry.addStagedFiles(list);

    // 5. Assert file should not be deletable
    deletable = cleaner.getDeletableFiles(stats);
    found = false;
    for (FileStatus stat1 : deletable) {
      if (stat1.equals(fs.getFileStatus(file))) {
        found = true;
      }
    }

    assertFalse("File should not be deletable as it has an HFile reference.", found);
  }
}
