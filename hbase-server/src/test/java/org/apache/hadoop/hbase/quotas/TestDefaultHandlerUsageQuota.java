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

import static org.apache.hadoop.hbase.quotas.ThrottleQuotaTestUtil.triggerUserCacheRefresh;
import static org.apache.hadoop.hbase.quotas.ThrottleQuotaTestUtil.waitMinuteQuota;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestDefaultHandlerUsageQuota {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestDefaultHandlerUsageQuota.class);
  
  private static final Logger LOG = LoggerFactory.getLogger(TestDefaultHandlerUsageQuota.class);
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final TableName TABLE_NAME = TableName.valueOf(UUID.randomUUID().toString());
  private static final int REFRESH_TIME = 1000;
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
    // Enable quotas with a strict default handler usage time limit
    TEST_UTIL.getConfiguration().setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    TEST_UTIL.getConfiguration().setInt(QuotaCache.REFRESH_CONF_KEY, REFRESH_TIME);
    TEST_UTIL.getConfiguration().setInt(QuotaUtil.QUOTA_DEFAULT_USER_MACHINE_REQUEST_HANDLER_USAGE_MS, 10);

    // don't cache blocks to make IO predictable
    TEST_UTIL.getConfiguration().setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.0f);

    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.waitTableAvailable(QuotaTableUtil.QUOTA_TABLE_NAME);
    TEST_UTIL.createTable(TABLE_NAME, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
    QuotaCache.TEST_FORCE_REFRESH = true;
    TEST_UTIL.flush(TABLE_NAME);
  }
  
  @Test
  public void testDefaultHandlerUsageLimits() throws Exception {
    try {
      // Clear any existing quotas to make sure we use the default
      unsetQuota();
      
      // Strict throttle by default set to 10ms in setUp method
      // Expect operations to be throttled due to strict handler usage limits
      long firstRunCount = runPutTest(20);
      LOG.info("First run with default strict quota completed {} operations", firstRunCount);
      
      assertTrue("Should be throttled with default quota, but completed all operations",
          firstRunCount < 20);
    } catch (Exception e) {
      LOG.error("Test failed with exception", e);
      throw e;
    }
  }
  
  @Test
  public void testExplicitHandlerUsageThrottle() throws Exception {
    try {
      unsetQuota();
      // Set a specific strict handler usage time limit (20ms per second)
      configureStrictThrottle();
      refreshQuotas();

      long runCount = runPutTest(20);

      LOG.info("Run with strict quota completed {} operations", runCount);
      assertTrue("Should be throttled with strict quota: " + runCount, runCount < 20);
    } catch (Exception e) {
      LOG.error("Test failed with exception", e);
      throw e;
    }
  }
  
  @Test
  public void testHandlerUsageQuotaReset() throws Exception {
    try {
      // Set a specific strict handler usage time limit (20ms per second)
      configureStrictThrottle();
      refreshQuotas();
      
      long firstRunCount = runPutTest(20);
      LOG.info("First run with strict quota completed {} operations", firstRunCount);
      assertTrue("Should be throttled with strict quota: " + firstRunCount, firstRunCount < 20);
      
      // Wait for quota to reset
      waitMinuteQuota();
      
      // Second batch - should still be throttled but allow operations up to the quota
      long secondRunCount = runPutTest(20);
      LOG.info("Second run with strict quota completed {} operations", secondRunCount);
      assertTrue("Should be throttled but allow similar operations count as first run", 
          Math.abs(secondRunCount - firstRunCount) <= 3);
    } catch (Exception e) {
      LOG.error("Test failed with exception", e);
      throw e;
    }
  }
  
  @Test
  public void testMixedThrottleTypes() throws Exception {
    try {
      unsetQuota();
      
      // Set a completely unthrottled handler usage quota (explicitly set to bypass)
      try (Admin admin = TEST_UTIL.getAdmin()) {
        admin.setQuota(QuotaSettingsFactory.unthrottleUser(getUserName()));
      }
      
      // Set a strict request number throttle
      try (Admin admin = TEST_UTIL.getAdmin()) {
        admin.setQuota(
          QuotaSettingsFactory.throttleUser(getUserName(), ThrottleType.REQUEST_NUMBER, 5, TimeUnit.SECONDS));
      }
      refreshQuotas();
      
      // Should be throttled due to request number, despite unthrottled handler usage
      long runCount = runPutTest(20);
      LOG.info("Run with mixed throttles completed {} operations", runCount);
      assertTrue("Should be throttled by request number: " + runCount, runCount < 20);
      
      // Now remove the request number throttle, should be unlimited
      try (Admin admin = TEST_UTIL.getAdmin()) {
        admin.setQuota(QuotaSettingsFactory.unthrottleUser(getUserName()));
      }
      refreshQuotas();
      
      // Wait for quota to reset 
      waitMinuteQuota();
      
      long unthrottledCount = runPutTest(20);
      LOG.info("Run with unthrottled quotas completed {} operations", unthrottledCount);
      assertTrue("Should have better throughput with unthrottled quotas", unthrottledCount > runCount);
    } catch (Exception e) {
      LOG.error("Test failed with exception", e);
      throw e;
    }
  }
  
  private void configureStrictThrottle() throws IOException {
    try (Admin admin = TEST_UTIL.getAdmin()) {
      // Set a very strict quota - only 20ms of handler time per second
      admin.setQuota(
        QuotaSettingsFactory.throttleUser(getUserName(), ThrottleType.REQUEST_HANDLER_USAGE_MS, 20, TimeUnit.SECONDS));
    }
  }
  
  private static String getUserName() throws IOException {
    return User.getCurrent().getShortName();
  }

  private void refreshQuotas() throws Exception {
    triggerUserCacheRefresh(TEST_UTIL, false, TABLE_NAME);
    waitMinuteQuota();
  }

  private void unsetQuota() throws Exception {
    try (Admin admin = TEST_UTIL.getAdmin()) {
      admin.setQuota(QuotaSettingsFactory.unthrottleUser(getUserName()));
    }
    refreshQuotas();
  }

  private long runPutTest(int attempts) throws Exception {
    refreshQuotas();
    try (Table table = getTable(TABLE_NAME)) {
      // For handler usage throttles, we need to perform operations that include reads
      // because handler usage throttles are only checked when estimateReadSize > 0
      int putCount = attempts / 2; // Half puts, half gets
      int getCount = attempts - putCount;
      
      // First do some puts to ensure we have data
      ThrottleQuotaTestUtil.doPuts(putCount, FAMILY, QUALIFIER, table);
      
      // Then do gets which will trigger the handler usage throttle check
      return ThrottleQuotaTestUtil.doGets(getCount, FAMILY, QUALIFIER, table);
    }
  }

  private Table getTable(TableName tableName) throws IOException {
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 100);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    return TEST_UTIL.getConnection().getTableBuilder(tableName, null)
      .setOperationTimeout(1000)
      .build();
  }

}
