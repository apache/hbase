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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils.SnapshotMock;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Export Snapshot Tool
 * Tests V1 snapshots only. Used to ALSO test v2 but strange failure so separate the tests.
 * See companion file for test of v2 snapshot.
 * @see TestExportSnapshotV2NoCluster
 */
@Category({MapReduceTests.class, MediumTests.class})
public class TestExportSnapshotV1NoCluster {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestExportSnapshotV1NoCluster.class);
  private static final Logger LOG = LoggerFactory.getLogger(TestExportSnapshotV1NoCluster.class);

  private HBaseCommonTestingUtility testUtil = new HBaseCommonTestingUtility();
  private Path testDir;
  private FileSystem fs;

  @Before
  public void setUpBefore() throws Exception {
    // Make sure testDir is on LocalFileSystem
    this.fs = FileSystem.getLocal(this.testUtil.getConfiguration());
    this.testDir = setup(fs, this.testUtil);
    LOG.info("fs={}, fsuri={}, fswd={}, testDir={}", this.fs, this.fs.getUri(),
      this.fs.getWorkingDirectory(), this.testDir);
    assertTrue("FileSystem '" + fs + "' is not local", fs instanceof LocalFileSystem);
  }

  /**
   * Setup for test. Returns path to test data dir. Sets configuration into the passed
   * hctu.getConfiguration.
   */
  static Path setup(FileSystem fs, HBaseCommonTestingUtility hctu) throws IOException {
    Path testDir = hctu.getDataTestDir().makeQualified(fs.getUri(), fs.getWorkingDirectory());
    hctu.getConfiguration().setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    hctu.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
    hctu.getConfiguration().setInt("hbase.client.pause", 250);
    hctu.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 6);
    hctu.getConfiguration().setBoolean("hbase.master.enabletable.roundrobin", true);
    hctu.getConfiguration().setInt("mapreduce.map.maxattempts", 10);
    hctu.getConfiguration().set(HConstants.HBASE_DIR, testDir.toString());
    return testDir.makeQualified(fs.getUri(), fs.getWorkingDirectory());
  }

  /**
   * V1 snapshot test
   */
  @Test
  public void testSnapshotWithRefsExportFileSystemState() throws Exception {
    final SnapshotMock snapshotMock = new SnapshotMock(testUtil.getConfiguration(),
      this.fs, testDir);
    final SnapshotMock.SnapshotBuilder builder = snapshotMock.createSnapshotV1("tableWithRefsV1",
      "tableWithRefsV1");
    testSnapshotWithRefsExportFileSystemState(this.fs, builder, testUtil, testDir);
  }

  /**
   * Generates a couple of regions for the specified SnapshotMock,
   * and then it will run the export and verification.
   */
  static void testSnapshotWithRefsExportFileSystemState(FileSystem fs,
     SnapshotMock.SnapshotBuilder builder, HBaseCommonTestingUtility testUtil, Path testDir)
        throws Exception {
    Path[] r1Files = builder.addRegion();
    Path[] r2Files = builder.addRegion();
    builder.commit();
    // remove references, only keep data files
    Set<String> dataFiles = new HashSet<>();
    for (Path[] files: new Path[][]{r1Files, r2Files}) {
      for (Path file : files) {
        if (StoreFileInfo.isReference(file.getName())) {
          Pair<String, String> referredToRegionAndFile =
            StoreFileInfo.getReferredToRegionAndFile(file.getName());
          dataFiles.add(referredToRegionAndFile.getSecond());
        } else {
          dataFiles.add(file.getName());
        }
      }
    }
    int snapshotFilesCount = dataFiles.size();
    byte[] snapshotName = Bytes.toBytes(builder.getSnapshotDescription().getName());
    TableName tableName = builder.getTableDescriptor().getTableName();
    TestExportSnapshot.testExportFileSystemState(testUtil.getConfiguration(),
      tableName, snapshotName, snapshotName, snapshotFilesCount,
      testDir, getDestinationDir(fs, testUtil, testDir), false, null, true);
  }

  static Path getDestinationDir(FileSystem fs, HBaseCommonTestingUtility hctu, Path testDir)
      throws IOException {
    Path path = new Path(new Path(testDir, "export-test"),
      "export-" + System.currentTimeMillis()).makeQualified(fs.getUri(),
      fs.getWorkingDirectory());
    LOG.info("Export destination={}, fs={}, fsurl={}, fswd={}, testDir={}", path, fs, fs.getUri(),
      fs.getWorkingDirectory(), testDir);
    return path;
  }
}
