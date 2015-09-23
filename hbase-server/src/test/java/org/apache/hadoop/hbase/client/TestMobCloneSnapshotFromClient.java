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
package org.apache.hadoop.hbase.client;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.snapshot.MobSnapshotTestingUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

/**
 * Test clone snapshots from the client
 */
@Category({LargeTests.class, ClientTests.class})
public class TestMobCloneSnapshotFromClient extends TestCloneSnapshotFromClient {
  private static final Log LOG = LogFactory.getLog(TestMobCloneSnapshotFromClient.class);

  protected static void setupConfiguration() {
    TestCloneSnapshotFromClient.setupConfiguration();
    TEST_UTIL.getConfiguration().setInt(MobConstants.MOB_FILE_CACHE_SIZE_KEY, 0);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    setupConfiguration();
    TEST_UTIL.startMiniCluster(3);
  }

  @Override
  protected void createTableAndSnapshots() throws Exception {
    // create Table and disable it
    MobSnapshotTestingUtils.createMobTable(TEST_UTIL, tableName, getNumReplicas(), FAMILY);
    admin.disableTable(tableName);

    // take an empty snapshot
    admin.snapshot(emptySnapshot, tableName);

    Connection c = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
    Table table = c.getTable(tableName);
    try {
      // enable table and insert data
      admin.enableTable(tableName);
      SnapshotTestingUtils.loadData(TEST_UTIL, tableName, 500, FAMILY);
      snapshot0Rows = MobSnapshotTestingUtils.countMobRows(table);
      admin.disableTable(tableName);

      // take a snapshot
      admin.snapshot(snapshotName0, tableName);

      // enable table and insert more data
      admin.enableTable(tableName);
      SnapshotTestingUtils.loadData(TEST_UTIL, tableName, 500, FAMILY);
      snapshot1Rows = MobSnapshotTestingUtils.countMobRows(table);
      admin.disableTable(tableName);

      // take a snapshot of the updated table
      admin.snapshot(snapshotName1, tableName);

      // re-enable table
      admin.enableTable(tableName);
    } finally {
      table.close();
    }
  }

  @Override
  protected void verifyRowCount(final HBaseTestingUtility util, final TableName tableName,
      long expectedRows) throws IOException {
    MobSnapshotTestingUtils.verifyMobRowCount(util, tableName, expectedRows);
  }
}
