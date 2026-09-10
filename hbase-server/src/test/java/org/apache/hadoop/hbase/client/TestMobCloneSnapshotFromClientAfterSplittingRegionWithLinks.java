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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.snapshot.MobSnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

@Tag(LargeTests.TAG)
@Tag(ClientTests.TAG)
@HBaseParameterizedTestTemplate(name = "{index}: regionReplication={0}")
public class TestMobCloneSnapshotFromClientAfterSplittingRegionWithLinks
  extends CloneSnapshotFromClientAfterSplittingRegionWithLinksTestBase {

  public TestMobCloneSnapshotFromClientAfterSplittingRegionWithLinks(int numReplicas) {
    super(numReplicas);
  }

  protected static void setupConfiguration() {
    CloneSnapshotFromClientAfterSplittingRegionWithLinksTestBase.setupConfiguration();
    TEST_UTIL.getConfiguration().setInt(MobConstants.MOB_FILE_CACHE_SIZE_KEY, 0);
  }

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    setupConfiguration();
    TEST_UTIL.startMiniCluster(3);
  }

  @Override
  protected void createTable() throws IOException, InterruptedException {
    MobSnapshotTestingUtils.createMobTable(TEST_UTIL, tableName, new byte[0][], numReplicas,
      FAMILY);
  }

  @Override
  protected int countRows(Table table) throws IOException {
    return MobSnapshotTestingUtils.countMobRows(table);
  }

  @Override
  protected void verifyRowCount(final HBaseTestingUtil util, final TableName tableName,
    long expectedRows) throws IOException {
    // Read every cell value so the MOB files are actually resolved: the point of this test is that
    // the MOB data is still readable after the source table and snapshot are removed.
    MobSnapshotTestingUtils.verifyMobRowCount(util, tableName, expectedRows);
  }
}
