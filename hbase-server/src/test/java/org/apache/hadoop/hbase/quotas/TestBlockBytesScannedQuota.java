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

import static org.apache.hadoop.hbase.quotas.ThrottleQuotaTestUtil.doGets;
import static org.apache.hadoop.hbase.quotas.ThrottleQuotaTestUtil.doMultiGets;
import static org.apache.hadoop.hbase.quotas.ThrottleQuotaTestUtil.doPuts;
import static org.apache.hadoop.hbase.quotas.ThrottleQuotaTestUtil.doScans;
import static org.apache.hadoop.hbase.quotas.ThrottleQuotaTestUtil.triggerUserCacheRefresh;
import static org.apache.hadoop.hbase.quotas.ThrottleQuotaTestUtil.waitMinuteQuota;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Callable;
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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestBlockBytesScannedQuota {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBlockBytesScannedQuota.class);

  private final static Logger LOG = LoggerFactory.getLogger(TestBlockBytesScannedQuota.class);

  private static final int REFRESH_TIME = 5000;
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] FAMILY = Bytes.toBytes("cf");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");

  private static final TableName TABLE_NAME = TableName.valueOf("BlockBytesScannedQuotaTest");
  private static final long MAX_SCANNER_RESULT_SIZE = 100 * 1024 * 1024;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // client should fail fast
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 10);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    TEST_UTIL.getConfiguration().setLong(HConstants.HBASE_SERVER_SCANNER_MAX_RESULT_SIZE_KEY,
      MAX_SCANNER_RESULT_SIZE);
    TEST_UTIL.getConfiguration().setClass(RateLimiter.QUOTA_RATE_LIMITER_CONF_KEY,
      AverageIntervalRateLimiter.class, RateLimiter.class);

    // quotas enabled, using block bytes scanned
    TEST_UTIL.getConfiguration().setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    TEST_UTIL.getConfiguration().setInt(QuotaCache.REFRESH_CONF_KEY, REFRESH_TIME);

    // don't cache blocks to make IO predictable
    TEST_UTIL.getConfiguration().setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.0f);

    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.waitTableAvailable(QuotaTableUtil.QUOTA_TABLE_NAME);
    TEST_UTIL.createTable(TABLE_NAME, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    EnvironmentEdgeManager.reset();
    TEST_UTIL.deleteTable(TABLE_NAME);
    TEST_UTIL.shutdownMiniCluster();
  }

  @After
  public void tearDown() throws Exception {
    ThrottleQuotaTestUtil.clearQuotaCache(TEST_UTIL);
  }

  @Test
  public void testBBSGet() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();
    final String userName = User.getCurrent().getShortName();
    int blockSize = admin.getDescriptor(TABLE_NAME).getColumnFamily(FAMILY).getBlocksize();
    Table table = admin.getConnection().getTable(TABLE_NAME);

    doPuts(10_000, FAMILY, QUALIFIER, table);
    TEST_UTIL.flush(TABLE_NAME);

    // Add ~10 block/sec limit
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, ThrottleType.READ_SIZE,
      Math.round(10.1 * blockSize), TimeUnit.SECONDS));
    triggerUserCacheRefresh(TEST_UTIL, false, TABLE_NAME);

    // should execute at max 10 requests
    testTraffic(() -> doGets(20, FAMILY, QUALIFIER, table), 10, 1);

    // wait a minute and you should get another 10 requests executed
    waitMinuteQuota();
    testTraffic(() -> doGets(20, FAMILY, QUALIFIER, table), 10, 1);

    // Remove all the limits
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName));
    triggerUserCacheRefresh(TEST_UTIL, true, TABLE_NAME);
    testTraffic(() -> doGets(100, FAMILY, QUALIFIER, table), 100, 0);
    testTraffic(() -> doGets(100, FAMILY, QUALIFIER, table), 100, 0);
  }

  @Test
  public void testBBSScan() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();
    final String userName = User.getCurrent().getShortName();
    int blockSize = admin.getDescriptor(TABLE_NAME).getColumnFamily(FAMILY).getBlocksize();
    Table table = admin.getConnection().getTable(TABLE_NAME);

    doPuts(10_000, FAMILY, QUALIFIER, table);
    TEST_UTIL.flush(TABLE_NAME);

    // Add 1 block/sec limit.
    // This should only allow 1 scan per minute, because we estimate 1 block per scan
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, ThrottleType.REQUEST_SIZE, blockSize,
      TimeUnit.SECONDS));
    triggerUserCacheRefresh(TEST_UTIL, false, TABLE_NAME);
    waitMinuteQuota();

    // should execute 1 request
    testTraffic(() -> doScans(5, table, 1), 1, 0);

    // Remove all the limits
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName));
    triggerUserCacheRefresh(TEST_UTIL, true, TABLE_NAME);
    testTraffic(() -> doScans(100, table, 1), 100, 0);
    testTraffic(() -> doScans(100, table, 1), 100, 0);

    // Add ~3 block/sec limit. This should support >1 scans
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, ThrottleType.REQUEST_SIZE,
      Math.round(3.1 * blockSize), TimeUnit.SECONDS));
    triggerUserCacheRefresh(TEST_UTIL, false, TABLE_NAME);
    waitMinuteQuota();

    // Add 50 block/sec limit. This should support >1 scans
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, ThrottleType.REQUEST_SIZE,
      Math.round(50.1 * blockSize), TimeUnit.SECONDS));
    triggerUserCacheRefresh(TEST_UTIL, false, TABLE_NAME);
    waitMinuteQuota();

    // This will produce some throttling exceptions, but all/most should succeed within the timeout
    testTraffic(() -> doScans(100, table, 1), 75, 25);
    triggerUserCacheRefresh(TEST_UTIL, false, TABLE_NAME);
    waitMinuteQuota();

    // With large caching, a big scan should succeed
    testTraffic(() -> doScans(10_000, table, 10_000), 10_000, 0);
    triggerUserCacheRefresh(TEST_UTIL, false, TABLE_NAME);
    waitMinuteQuota();

    // Remove all the limits
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName));
    triggerUserCacheRefresh(TEST_UTIL, true, TABLE_NAME);
    testTraffic(() -> doScans(100, table, 1), 100, 0);
    testTraffic(() -> doScans(100, table, 1), 100, 0);
  }

  @Test
  public void testSmallScanNeverBlockedByLargeEstimate() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();
    final String userName = User.getCurrent().getShortName();
    Table table = admin.getConnection().getTable(TABLE_NAME);

    doPuts(10_000, FAMILY, QUALIFIER, table);
    TEST_UTIL.flush(TABLE_NAME);

    // Add 99MB/sec limit.
    // This should never be blocked, but with a sequence number approaching 10k, without
    // other intervention, we would estimate a scan workload approaching 625MB or the
    // maxScannerResultSize (both larger than the 90MB limit). This test ensures that all
    // requests succeed, so the estimate never becomes large enough to cause read downtime
    long limit = 99 * 1024 * 1024;
    assertTrue(limit <= MAX_SCANNER_RESULT_SIZE); // always true, but protecting against code
                                                  // changes
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, ThrottleType.REQUEST_SIZE, limit,
      TimeUnit.SECONDS));
    triggerUserCacheRefresh(TEST_UTIL, false, TABLE_NAME);
    waitMinuteQuota();

    // should execute all requests
    testTraffic(() -> doScans(10_000, table, 1), 10_000, 0);
    triggerUserCacheRefresh(TEST_UTIL, false, TABLE_NAME);
    waitMinuteQuota();

    // Remove all the limits
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName));
    triggerUserCacheRefresh(TEST_UTIL, true, TABLE_NAME);
    testTraffic(() -> doScans(100, table, 1), 100, 0);
    testTraffic(() -> doScans(100, table, 1), 100, 0);
  }

  @Test
  public void testBBSMultiGet() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();
    final String userName = User.getCurrent().getShortName();
    int blockSize = admin.getDescriptor(TABLE_NAME).getColumnFamily(FAMILY).getBlocksize();
    Table table = admin.getConnection().getTable(TABLE_NAME);
    int rowCount = 10_000;

    doPuts(rowCount, FAMILY, QUALIFIER, table);
    TEST_UTIL.flush(TABLE_NAME);

    // Add 1 block/sec limit.
    // This should only allow 1 multiget per minute, because we estimate 1 block per multiget
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, ThrottleType.REQUEST_SIZE, blockSize,
      TimeUnit.SECONDS));
    triggerUserCacheRefresh(TEST_UTIL, false, TABLE_NAME);
    waitMinuteQuota();

    // should execute 1 request
    testTraffic(() -> doMultiGets(10, 10, rowCount, FAMILY, QUALIFIER, table), 1, 1);

    // Remove all the limits
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName));
    triggerUserCacheRefresh(TEST_UTIL, true, TABLE_NAME);
    testTraffic(() -> doMultiGets(100, 10, rowCount, FAMILY, QUALIFIER, table), 100, 0);
    testTraffic(() -> doMultiGets(100, 10, rowCount, FAMILY, QUALIFIER, table), 100, 0);

    // Add ~100 block/sec limit
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, ThrottleType.REQUEST_SIZE,
      Math.round(100.1 * blockSize), TimeUnit.SECONDS));
    triggerUserCacheRefresh(TEST_UTIL, false, TABLE_NAME);

    // should execute approximately 10 batches of 10 requests
    testTraffic(() -> doMultiGets(20, 10, rowCount, FAMILY, QUALIFIER, table), 10, 1);

    // wait a minute and you should get another ~10 batches of 10 requests
    waitMinuteQuota();
    testTraffic(() -> doMultiGets(20, 10, rowCount, FAMILY, QUALIFIER, table), 10, 1);

    // Remove all the limits
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName));
    triggerUserCacheRefresh(TEST_UTIL, true, TABLE_NAME);
    testTraffic(() -> doMultiGets(100, 10, rowCount, FAMILY, QUALIFIER, table), 100, 0);
    testTraffic(() -> doMultiGets(100, 10, rowCount, FAMILY, QUALIFIER, table), 100, 0);
  }

  private void testTraffic(Callable<Long> trafficCallable, long expectedSuccess, long marginOfError)
    throws Exception {
    TEST_UTIL.waitFor(5_000, () -> {
      long actualSuccess;
      try {
        actualSuccess = trafficCallable.call();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      LOG.info("Traffic test yielded {} successful requests. Expected {} +/- {}", actualSuccess,
        expectedSuccess, marginOfError);
      boolean success = (actualSuccess >= expectedSuccess - marginOfError)
        && (actualSuccess <= expectedSuccess + marginOfError);
      if (!success) {
        triggerUserCacheRefresh(TEST_UTIL, false, TABLE_NAME);
        waitMinuteQuota();
      }
      return success;
    });
  }
}
