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

package org.apache.hadoop.hbase.snapshot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CategoryBasedTimeout;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils.SnapshotMock;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;

/**
 * Test Export Snapshot Tool
 */
@Category({MapReduceTests.class, MediumTests.class})
public class TestExportSnapshotNoCluster {
  @Rule public final TestRule timeout = CategoryBasedTimeout.builder().
      withTimeout(this.getClass()).withLookingForStuckThread(true).build();
  private static final Log LOG = LogFactory.getLog(TestExportSnapshotNoCluster.class);

  protected final static HBaseCommonTestingUtility TEST_UTIL = new HBaseCommonTestingUtility();

  private static FileSystem fs;
  private static Path testDir;

  public static void setUpBaseConf(Configuration conf) {
    conf.setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    conf.setInt("hbase.regionserver.msginterval", 100);
    conf.setInt("hbase.client.pause", 250);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 6);
    conf.setBoolean("hbase.master.enabletable.roundrobin", true);
    conf.setInt("mapreduce.map.maxattempts", 10);
    conf.set(HConstants.HBASE_DIR, testDir.toString());
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    testDir = TEST_UTIL.getDataTestDir();
    fs = testDir.getFileSystem(TEST_UTIL.getConfiguration());

    setUpBaseConf(TEST_UTIL.getConfiguration());
  }

  /**
   * Mock a snapshot with files in the archive dir,
   * two regions, and one reference file.
   */
  @Test
  public void testSnapshotWithRefsExportFileSystemState() throws Exception {
    SnapshotMock snapshotMock = new SnapshotMock(TEST_UTIL.getConfiguration(), fs, testDir);
    SnapshotMock.SnapshotBuilder builder = snapshotMock.createSnapshotV2("tableWithRefsV1",
      "tableWithRefsV1");
    testSnapshotWithRefsExportFileSystemState(builder);

    snapshotMock = new SnapshotMock(TEST_UTIL.getConfiguration(), fs, testDir);
    builder = snapshotMock.createSnapshotV2("tableWithRefsV2", "tableWithRefsV2");
    testSnapshotWithRefsExportFileSystemState(builder);
  }

  /**
   * Generates a couple of regions for the specified SnapshotMock,
   * and then it will run the export and verification.
   */
  private void testSnapshotWithRefsExportFileSystemState(SnapshotMock.SnapshotBuilder builder)
      throws Exception {
    Path[] r1Files = builder.addRegion();
    Path[] r2Files = builder.addRegion();
    builder.commit();
    int snapshotFilesCount = r1Files.length + r2Files.length;

    byte[] snapshotName = Bytes.toBytes(builder.getSnapshotDescription().getName());
    TableName tableName = builder.getTableDescriptor().getTableName();
    TestExportSnapshot.testExportFileSystemState(TEST_UTIL.getConfiguration(),
      tableName, snapshotName, snapshotName, snapshotFilesCount,
      testDir, getDestinationDir(), false, null);
  }

  private Path getDestinationDir() {
    Path path = new Path(new Path(testDir, "export-test"), "export-" + System.currentTimeMillis());
    LOG.info("HDFS export destination path: " + path);
    return path;
  }
}
