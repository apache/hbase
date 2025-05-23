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
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;

@Category(SmallTests.class)
public class TestSnapshotRegionSizeCalculator {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSnapshotRegionSizeCalculator.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static FileSystem fs;
  private static Path rootDir;
  private static Path snapshotDir;
  private static SnapshotProtos.SnapshotDescription snapshotDesc;
  private static SnapshotManifest manifest;

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    fs = TEST_UTIL.getTestFileSystem();
    rootDir = TEST_UTIL.getDataTestDir("TestSnapshotRegionSizeCalculator");
    CommonFSUtils.setRootDir(conf, rootDir);

    // Create a mock snapshot with a region and store files
    SnapshotTestingUtils.SnapshotMock snapshotMock =
      new SnapshotTestingUtils.SnapshotMock(conf, fs, rootDir);
    SnapshotTestingUtils.SnapshotMock.SnapshotBuilder builder =
      snapshotMock.createSnapshotV2("snapshot", "testTable", 4);
    builder.addRegion();
    builder.addRegion();
    builder.addRegion();
    builder.addRegion();
    snapshotDir = builder.commit();
    snapshotDesc = builder.getSnapshotDescription();
    manifest = SnapshotManifest.open(conf, fs, snapshotDir, snapshotDesc);

  }

  @AfterClass
  public static void tearDown() throws Exception {
    fs.delete(rootDir, true);
  }

  @Test
  public void testCalculateRegionSizes() throws IOException {
    SnapshotRegionSizeCalculator calculator =
      new SnapshotRegionSizeCalculator(TEST_UTIL.getConfiguration(), manifest);
    Map<String, Long> regionSizes = calculator.calculateRegionSizes();

    // Verify that the region sizes are calculated correctly
    assertTrue("No regions found in the snapshot", regionSizes.size() == 4);
    for (Map.Entry<String, Long> entry : regionSizes.entrySet()) {
      assertTrue("Region size should be non-negative", entry.getValue() > 0);
    }
  }
}
