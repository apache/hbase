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
package org.apache.hadoop.hbase.quotas;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestDefaultHandlerUsageQuota {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestDefaultHandlerUsageQuota.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final TableName TABLE_NAME = TableName.valueOf(UUID.randomUUID().toString());
  private static final int REFRESH_TIME = 5;
  private static final byte[] FAMILY = Bytes.toBytes("cf");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");

  @AfterClass
  public static void tearDown() throws Exception {
    ThrottleQuotaTestUtil.clearQuotaCache(TEST_UTIL);
    EnvironmentEdgeManager.reset();
    TEST_UTIL.deleteTable(TABLE_NAME);
    TEST_UTIL.shutdownMiniCluster();
  }

  @BeforeClass
  public static void setUp() throws Exception {
    // quotas enabled, using block bytes scanned
    TEST_UTIL.getConfiguration().setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    TEST_UTIL.getConfiguration().setInt(QuotaCache.REFRESH_CONF_KEY, REFRESH_TIME);
    // Set default to very strict
    TEST_UTIL.getConfiguration()
      .setInt(QuotaUtil.QUOTA_DEFAULT_USER_MACHINE_REQUEST_HANDLER_USAGE_MS, 10);

    // don't cache blocks to make IO predictable
    TEST_UTIL.getConfiguration().setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.0f);

    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.waitTableAvailable(QuotaTableUtil.QUOTA_TABLE_NAME);
    TEST_UTIL.createTable(TABLE_NAME, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
    TEST_UTIL.flush(TABLE_NAME);
  }

  @Test
  public void testDefaultHandlerUsageLimits() throws Exception {
    // Should have a strict throttle by default
    TEST_UTIL.waitFor(60_000, () -> runPutTest(100) < 100);

    // Add big quota and should be effectively unlimited
    configureLenientThrottle();
    // Should run without error
    TEST_UTIL.waitFor(60_000, () -> runPutTest(100) == 100);

    // Remove all the limits, and should revert to strict default
    unsetQuota();
    TEST_UTIL.waitFor(60_000, () -> runPutTest(100) < 100);
  }

  private void configureLenientThrottle() throws IOException {
    try (Admin admin = TEST_UTIL.getAdmin()) {
      admin.setQuota(QuotaSettingsFactory.throttleUser(getUserName(),
        ThrottleType.REQUEST_HANDLER_USAGE_MS, 100_000, TimeUnit.SECONDS));
    }
  }

  private static String getUserName() throws IOException {
    return User.getCurrent().getShortName();
  }

  private void unsetQuota() throws Exception {
    try (Admin admin = TEST_UTIL.getAdmin()) {
      admin.setQuota(QuotaSettingsFactory.unthrottleUser(getUserName()));
    }
  }

  private long runPutTest(int attempts) throws Exception {
    try (Table table = getTable()) {
      return ThrottleQuotaTestUtil.doPuts(attempts, FAMILY, QUALIFIER, table);
    }
  }

  private Table getTable() throws IOException {
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 100);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    return TEST_UTIL.getConnection().getTableBuilder(TABLE_NAME, null).setOperationTimeout(250)
      .build();
  }
}
