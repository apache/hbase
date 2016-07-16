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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CategoryBasedTimeout;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;

/**
 * Test creating/using/deleting snapshots from the client
 * <p>
 * This is an end-to-end test for the snapshot utility
 *
 * TODO This is essentially a clone of TestSnapshotFromClient.  This is worth refactoring this
 * because there will be a few more flavors of snapshots that need to run these tests.
 */
@Category({ClientTests.class, LargeTests.class})
public class TestMobFlushSnapshotFromClient extends TestFlushSnapshotFromClient {
  private static final Log LOG = LogFactory.getLog(TestFlushSnapshotFromClient.class);

  @ClassRule
  public static final TestRule timeout =
      CategoryBasedTimeout.forClass(TestMobFlushSnapshotFromClient.class);

  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(3);
  }

  protected static void setupConf(Configuration conf) {
    TestFlushSnapshotFromClient.setupConf(conf);
    UTIL.getConfiguration().setInt(MobConstants.MOB_FILE_CACHE_SIZE_KEY, 0);
  }

  @Override
  protected void createTable() throws Exception {
    MobSnapshotTestingUtils.createMobTable(UTIL, TABLE_NAME, 1, TEST_FAM);
  }

  @Override
  protected void verifyRowCount(final HBaseTestingUtility util, final TableName tableName,
      long expectedRows) throws IOException {
    MobSnapshotTestingUtils.verifyMobRowCount(util, tableName, expectedRows);
  }

  @Override
  protected int countRows(final Table table, final byte[]... families) throws IOException {
    return MobSnapshotTestingUtils.countMobRows(table, families);
  }
}