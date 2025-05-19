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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
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
public class TestThreadHandlerUsageQuota {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestThreadHandlerUsageQuota.class);
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final TableName TABLE_NAME = TableName.valueOf(UUID.randomUUID().toString());
  private static final int REFRESH_TIME = 5;
  private static final byte[] FAMILY = Bytes.toBytes("cf");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");
  private static final int MAX_OPS = 1000;

  @AfterClass
  public static void tearDown() throws Exception {
    ThrottleQuotaTestUtil.clearQuotaCache(TEST_UTIL);
    EnvironmentEdgeManager.reset();
    TEST_UTIL.deleteTable(TABLE_NAME);
    TEST_UTIL.shutdownMiniCluster();
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Enable quotas
    TEST_UTIL.getConfiguration().setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    TEST_UTIL.getConfiguration().setInt(QuotaCache.REFRESH_CONF_KEY, REFRESH_TIME);

    // Don't cache blocks to make IO predictable
    TEST_UTIL.getConfiguration().setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.0f);

    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.waitTableAvailable(QuotaTableUtil.QUOTA_TABLE_NAME);

    TEST_UTIL.createTable(TABLE_NAME, FAMILY);

    TEST_UTIL.waitTableAvailable(TABLE_NAME);
    QuotaCache.TEST_FORCE_REFRESH = true;
    TEST_UTIL.flush(TABLE_NAME);
  }

  @Test
  public void testHandlerUsageThrottleForReads() throws Exception {
    try (Table table = getTable()) {
      unthrottleUser();
      long unthrottledAttempts = ThrottleQuotaTestUtil.doGets(MAX_OPS, FAMILY, QUALIFIER, table);

      configureThrottle();
      long throttledAttempts = ThrottleQuotaTestUtil.doGets(MAX_OPS, FAMILY, QUALIFIER, table);
      assertTrue("Throttled attempts should be less than unthrottled attempts",
        throttledAttempts < unthrottledAttempts);
    }
  }

  @Test
  public void testHandlerUsageThrottleForWrites() throws Exception {
    try (Table table = getTable()) {
      unthrottleUser();
      long unthrottledAttempts = ThrottleQuotaTestUtil.doPuts(MAX_OPS, FAMILY, QUALIFIER, table);

      configureThrottle();
      long throttledAttempts = ThrottleQuotaTestUtil.doPuts(MAX_OPS, FAMILY, QUALIFIER, table);
      assertTrue("Throttled attempts should be less than unthrottled attempts",
        throttledAttempts < unthrottledAttempts);
    }
  }

  private void configureThrottle() throws IOException {
    try (Admin admin = TEST_UTIL.getAdmin()) {
      admin.setQuota(QuotaSettingsFactory.throttleUser(getUserName(),
        ThrottleType.REQUEST_HANDLER_USAGE_MS, 10000, TimeUnit.SECONDS));
    }
  }

  private void unthrottleUser() throws Exception {
    try (Admin admin = TEST_UTIL.getAdmin()) {
      admin.setQuota(QuotaSettingsFactory.unthrottleUserByThrottleType(getUserName(),
        ThrottleType.REQUEST_HANDLER_USAGE_MS));
    }
  }

  private static String getUserName() throws IOException {
    return User.getCurrent().getShortName();
  }

  private Table getTable() throws IOException {
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 100);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    return TEST_UTIL.getConnection().getTableBuilder(TABLE_NAME, null).setOperationTimeout(250)
      .build();
  }
}
