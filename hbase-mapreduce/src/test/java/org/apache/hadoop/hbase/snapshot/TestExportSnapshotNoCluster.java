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

import static org.junit.Assert.assertTrue;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils.SnapshotMock;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Export Snapshot Tool
 */
@Category({MapReduceTests.class, MediumTests.class})
public class TestExportSnapshotNoCluster {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestExportSnapshotNoCluster.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestExportSnapshotNoCluster.class);

  private final HBaseCommonTestingUtility testUtil = new HBaseCommonTestingUtility();
  private FileSystem fs;
  private Path testDir;

  public void setUpBaseConf(Configuration conf) {
    conf.setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    conf.setInt("hbase.regionserver.msginterval", 100);
    conf.setInt("hbase.client.pause", 250);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 6);
    conf.setBoolean("hbase.master.enabletable.roundrobin", true);
    conf.setInt("mapreduce.map.maxattempts", 10);
    conf.set(HConstants.HBASE_DIR, testDir.toString());
  }

  @Before
  public void setUpBefore() throws Exception {
    // Make sure testDir is on LocalFileSystem
    testDir = testUtil.getDataTestDir().makeQualified(URI.create("file:///"), new Path("/"));
    fs = testDir.getFileSystem(testUtil.getConfiguration());
    assertTrue("FileSystem '" + fs + "' is not local", fs instanceof LocalFileSystem);

    setUpBaseConf(testUtil.getConfiguration());
  }

  @Test
  public void testSnapshotV1WithRefsExportFileSystemState() throws Exception {
    final SnapshotMock snapshotMock = new SnapshotMock(testUtil.getConfiguration(), fs, testDir);
    final SnapshotMock.SnapshotBuilder builder = snapshotMock.createSnapshotV1("tableWithRefsV1",
      "tableWithRefsV1");
    testSnapshotWithRefsExportFileSystemState(builder);
  }

  @Test
  public void testSnapshotV2WithRefsExportFileSystemState() throws Exception {
    final SnapshotMock snapshotMock = new SnapshotMock(testUtil.getConfiguration(), fs, testDir);
    final SnapshotMock.SnapshotBuilder builder = snapshotMock.createSnapshotV2("tableWithRefsV2",
      "tableWithRefsV2");
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

    String snapshotName = builder.getSnapshotDescription().getName();
    TableName tableName = builder.getTableDescriptor().getTableName();
    TestExportSnapshot.testExportFileSystemState(testUtil.getConfiguration(),
      tableName, snapshotName, snapshotName, snapshotFilesCount,
      testDir, getDestinationDir(), false, null, true);
  }

  private Path getDestinationDir() {
    Path path = new Path(new Path(testDir, "export-test"), "export-" + System.currentTimeMillis());
    LOG.info("HDFS export destination path: " + path);
    return path;
  }
}
