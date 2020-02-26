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

package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.cleaner.TimeToLiveHFileCleaner;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({LargeTests.class})
public class TestCleanupCompactedWithForceArchive {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCleanupCompactedWithForceArchive.class);

  private static HBaseTestingUtility TEST_UTIL;
  private static Admin admin;

  private static TableName TABLE_NAME = TableName.valueOf("TestCleanupCompactedFileAfterFailover");
  private static byte[] FAMILY = Bytes.toBytes("cf");
  private static final int RS_NUMBER = 5;

  @BeforeClass
  public static void beforeClass() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 1200000);
    TEST_UTIL.getConfiguration()
      .setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_KEY, 100);
    TEST_UTIL.getConfiguration().set("dfs.blocksize", "64000");
    TEST_UTIL.getConfiguration().set("dfs.namenode.fs-limits.min-block-size", "1024");
    TEST_UTIL.getConfiguration().set(TimeToLiveHFileCleaner.TTL_CONF_KEY, "0");
    TEST_UTIL.getConfiguration().setBoolean(HStore.FORCE_ARCHIVAL_AFTER_RESET, true);
    TEST_UTIL.startMiniCluster(RS_NUMBER);
    admin = TEST_UTIL.getAdmin();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void before() throws Exception {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TABLE_NAME);
    builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY));
    admin.createTable(builder.build());
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
  }

  @After
  public void after() throws Exception {
    admin.disableTable(TABLE_NAME);
    admin.deleteTable(TABLE_NAME);
  }

  @Test
  public void testForceArchiveWithCompactOnce() throws Exception {
    TestCleanupCompactedFileAfterFailover.testCleanupAfterFailover(1, true, TEST_UTIL);
  }

  @Test
  public void testForceArchiveWithCompactTwice() throws Exception {
    TestCleanupCompactedFileAfterFailover.testCleanupAfterFailover(2, true, TEST_UTIL);
  }

  @Test
  public void testForceArchiveWithCompactThreeTimes() throws Exception {
    TestCleanupCompactedFileAfterFailover.testCleanupAfterFailover(3, true, TEST_UTIL);
  }

}
