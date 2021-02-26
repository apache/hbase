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
package org.apache.hadoop.hbase.snapshot;

import static org.junit.Assert.assertFalse;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.VerySlowMapReduceTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests that are adjunct to {@link TestExportSnapshot}. They used to be in same test suite but
 * the test suite ran too close to the maximum time limit so we split these out. Uses
 * facility from TestExportSnapshot where possible.
 * @see TestExportSnapshot
 */
@Ignore // HBASE-24493
@Category({VerySlowMapReduceTests.class, LargeTests.class})
public class TestExportSnapshotAdjunct {
  private static final Logger LOG = LoggerFactory.getLogger(TestExportSnapshotAdjunct.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestExportSnapshotAdjunct.class);
  @Rule
  public final TestName testName = new TestName();

  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  protected TableName tableName;
  private String emptySnapshotName;
  private String snapshotName;
  private int tableNumFiles;
  private Admin admin;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TestExportSnapshot.setUpBaseConf(TEST_UTIL.getConfiguration());
    TEST_UTIL.startMiniCluster(3);
    TEST_UTIL.startMiniMapReduceCluster();
  }

  /**
   * Check for references to '/tmp'. We are trying to avoid having references to outside of the
   * test data dir when running tests. References outside of the test dir makes it so concurrent
   * tests can stamp on each other by mistake. This check is for references to the 'tmp'.
   *
   * This is a strange place for this test but I want somewhere where the configuration is
   * full -- filed w/ hdfs and mapreduce configurations.
   */
  private void checkForReferencesToTmpDir() {
    Configuration conf = TEST_UTIL.getConfiguration();
    for (Iterator<Map.Entry<String, String>> i = conf.iterator(); i.hasNext();) {
      Map.Entry<String, String> e = i.next();
      if (e.getKey().contains("original.hbase.dir")) {
        continue;
      }
      if (e.getValue().contains("java.io.tmpdir")) {
        continue;
      }
      if (e.getValue().contains("hadoop.tmp.dir")) {
        continue;
      }
      if (e.getValue().contains("hbase.tmp.dir")) {
        continue;
      }
      assertFalse(e.getKey() + " " + e.getValue(), e.getValue().contains("tmp"));
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniMapReduceCluster();
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Create a table and take a snapshot of the table used by the export test.
   */
  @Before
  public void setUp() throws Exception {
    this.admin = TEST_UTIL.getAdmin();

    tableName = TableName.valueOf("testtb-" + testName.getMethodName());
    snapshotName = "snaptb0-" + testName.getMethodName();
    emptySnapshotName = "emptySnaptb0-" + testName.getMethodName();

    // Create Table
    SnapshotTestingUtils.createPreSplitTable(TEST_UTIL, tableName, 2, TestExportSnapshot.FAMILY);

    // Take an empty snapshot
    admin.snapshot(emptySnapshotName, tableName);

    // Add some rows
    SnapshotTestingUtils.loadData(TEST_UTIL, tableName, 50,
      TestExportSnapshot.FAMILY);
    tableNumFiles = admin.getRegions(tableName).size();

    // take a snapshot
    admin.snapshot(snapshotName, tableName);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.deleteTable(tableName);
    SnapshotTestingUtils.deleteAllSnapshots(TEST_UTIL.getAdmin());
    SnapshotTestingUtils.deleteArchiveDirectory(TEST_UTIL);
  }

  /**
   * Check that ExportSnapshot will succeed if something fails but the retry succeed.
   */
  @Test
  public void testExportRetry() throws Exception {
    Path copyDir = TestExportSnapshot.getLocalDestinationDir(TEST_UTIL);
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.setBoolean(ExportSnapshot.Testing.CONF_TEST_FAILURE, true);
    conf.setInt(ExportSnapshot.Testing.CONF_TEST_FAILURE_COUNT, 2);
    conf.setInt("mapreduce.map.maxattempts", 3);
    TestExportSnapshot.testExportFileSystemState(conf, tableName,
      Bytes.toBytes(snapshotName), Bytes.toBytes(snapshotName),
      tableNumFiles, TEST_UTIL.getDefaultRootDirPath(), copyDir, true,
      null, true);
  }

  /**
   * Check that ExportSnapshot will fail if we inject failure more times than MR will retry.
   */
  @Test
  public void testExportFailure() throws Exception {
    Path copyDir = TestExportSnapshot.getLocalDestinationDir(TEST_UTIL);
    FileSystem fs = FileSystem.get(copyDir.toUri(), new Configuration());
    copyDir = copyDir.makeQualified(fs.getUri(), fs.getWorkingDirectory());
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.setBoolean(ExportSnapshot.Testing.CONF_TEST_FAILURE, true);
    conf.setInt(ExportSnapshot.Testing.CONF_TEST_FAILURE_COUNT, 4);
    conf.setInt("mapreduce.map.maxattempts", 3);
    TestExportSnapshot.testExportFileSystemState(conf, tableName,
      Bytes.toBytes(snapshotName), Bytes.toBytes(snapshotName),
      tableNumFiles, TEST_UTIL.getDefaultRootDirPath(), copyDir, true, null, false);
  }
}
