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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
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
 * Test Export Snapshot Tool; tests v2 snapshots.
 * @see TestExportSnapshotV1NoCluster
 */
@Category({MapReduceTests.class, MediumTests.class})
public class TestExportSnapshotV2NoCluster {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestExportSnapshotV2NoCluster.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestExportSnapshotV2NoCluster.class);

  private HBaseCommonTestingUtility testUtil = new HBaseCommonTestingUtility();
  private Path testDir;

  @Before
  public void before() throws Exception {
    this.testDir = TestExportSnapshotV1NoCluster.setup(this.testUtil);
  }

  @Test
  public void testSnapshotWithRefsExportFileSystemState() throws Exception {
    final SnapshotMock snapshotMock = new SnapshotMock(testUtil.getConfiguration(),
      testDir.getFileSystem(testUtil.getConfiguration()), testDir);
    final SnapshotMock.SnapshotBuilder builder = snapshotMock.createSnapshotV2("tableWithRefsV2",
      "tableWithRefsV2");
    TestExportSnapshotV1NoCluster.testSnapshotWithRefsExportFileSystemState(builder, this.testUtil,
      this.testDir);
  }
}
