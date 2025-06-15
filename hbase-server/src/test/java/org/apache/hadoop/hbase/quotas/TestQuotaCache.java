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

import static org.apache.hadoop.hbase.quotas.ThrottleQuotaTestUtil.waitMinuteQuota;
import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestQuotaCache {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestQuotaCache.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final int REFRESH_TIME_MS = 1000;

  @After
  public void tearDown() throws Exception {
    ThrottleQuotaTestUtil.clearQuotaCache(TEST_UTIL);
    EnvironmentEdgeManager.reset();
    TEST_UTIL.shutdownMiniCluster();
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    TEST_UTIL.getConfiguration().setInt(QuotaCache.REFRESH_CONF_KEY, REFRESH_TIME_MS);
    TEST_UTIL.getConfiguration().setInt(QuotaUtil.QUOTA_DEFAULT_USER_MACHINE_READ_NUM, 1000);

    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.waitTableAvailable(QuotaTableUtil.QUOTA_TABLE_NAME);
  }

  @Test
  public void testDefaultUserRefreshFrequency() throws Exception {
    QuotaCache.TEST_BLOCK_REFRESH = true;

    QuotaCache quotaCache =
      ThrottleQuotaTestUtil.getQuotaCaches(TEST_UTIL).stream().findAny().get();
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

    UserQuotaState userQuotaState = quotaCache.getUserQuotaState(ugi);
    assertEquals(userQuotaState.getLastUpdate(), 0);

    QuotaCache.TEST_BLOCK_REFRESH = false;
    // new user should have refreshed immediately
    TEST_UTIL.waitFor(5_000, () -> userQuotaState.getLastUpdate() != 0);
    long lastUpdate = userQuotaState.getLastUpdate();

    // refresh should not apply to recently refreshed quota
    quotaCache.triggerCacheRefresh();
    Thread.sleep(250);
    long newLastUpdate = userQuotaState.getLastUpdate();
    assertEquals(lastUpdate, newLastUpdate);

    quotaCache.triggerCacheRefresh();
    waitMinuteQuota();
    // should refresh after time has passed
    TEST_UTIL.waitFor(5_000, () -> lastUpdate != userQuotaState.getLastUpdate());
  }
}
