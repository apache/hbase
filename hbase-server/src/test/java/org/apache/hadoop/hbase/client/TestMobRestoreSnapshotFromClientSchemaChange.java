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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.snapshot.MobSnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

@Category({ LargeTests.class, ClientTests.class })
public class TestMobRestoreSnapshotFromClientSchemaChange
    extends RestoreSnapshotFromClientSchemaChangeTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMobRestoreSnapshotFromClientSchemaChange.class);

  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(TEST_UTIL.getConfiguration());
    TEST_UTIL.startMiniCluster(3);
  }

  protected static void setupConf(Configuration conf) {
    RestoreSnapshotFromClientTestBase.setupConf(conf);
    TEST_UTIL.getConfiguration().setInt(MobConstants.MOB_FILE_CACHE_SIZE_KEY, 0);
  }

  @Override
  protected void createTable() throws Exception {
    MobSnapshotTestingUtils.createMobTable(TEST_UTIL, tableName, getNumReplicas(), FAMILY);
  }

  @Override
  protected void verifyRowCount(HBaseTestingUtility util, TableName tableName, long expectedRows)
      throws IOException {
    MobSnapshotTestingUtils.verifyMobRowCount(util, tableName, expectedRows);
  }

  @Override
  protected int countRows(Table table, byte[]... families) throws IOException {
    return MobSnapshotTestingUtils.countMobRows(table, families);
  }

  @Override
  protected ColumnFamilyDescriptor getTestRestoreSchemaChangeHCD() {
    return ColumnFamilyDescriptorBuilder.newBuilder(TEST_FAMILY2).setMobEnabled(true)
      .setMobThreshold(3L).build();
  }
}
