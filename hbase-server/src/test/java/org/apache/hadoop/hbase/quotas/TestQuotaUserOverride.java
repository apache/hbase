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

import static org.apache.hadoop.hbase.quotas.ThrottleQuotaTestUtil.doPuts;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
public class TestQuotaUserOverride {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestQuotaUserOverride.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final byte[] FAMILY = Bytes.toBytes("cf");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");
  private static final int NUM_SERVERS = 1;
  private static final String CUSTOM_OVERRIDE_KEY = "foo";

  private static final TableName TABLE_NAME = TableName.valueOf("TestQuotaUserOverride");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    TEST_UTIL.getConfiguration().setInt(QuotaCache.REFRESH_CONF_KEY, 1_000);
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 1);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 0);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 500);
    TEST_UTIL.getConfiguration().set(QuotaCache.QUOTA_USER_REQUEST_ATTRIBUTE_OVERRIDE_KEY,
      CUSTOM_OVERRIDE_KEY);
    TEST_UTIL.startMiniCluster(NUM_SERVERS);
    TEST_UTIL.waitTableAvailable(QuotaTableUtil.QUOTA_TABLE_NAME);
    TEST_UTIL.createTable(TABLE_NAME, FAMILY);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    EnvironmentEdgeManager.reset();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testUserGlobalThrottleWithCustomOverride() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();
    final String userOverrideWithQuota = User.getCurrent().getShortName();

    // Add 6req/min limit
    admin.setQuota(QuotaSettingsFactory.throttleUser(userOverrideWithQuota,
      ThrottleType.REQUEST_NUMBER, 6, TimeUnit.MINUTES));
    ThrottleQuotaTestUtil.triggerUserCacheRefresh(TEST_UTIL, false, TABLE_NAME);

    Table tableWithThrottle = TEST_UTIL.getConnection().getTableBuilder(TABLE_NAME, null)
      .setRequestAttribute(CUSTOM_OVERRIDE_KEY, Bytes.toBytes(userOverrideWithQuota)).build();
    Table tableWithoutThrottle = TEST_UTIL.getConnection().getTableBuilder(TABLE_NAME, null)
      .setRequestAttribute(CUSTOM_OVERRIDE_KEY, Bytes.toBytes("anotherUser")).build();

    // warm things up
    doPuts(10, FAMILY, QUALIFIER, tableWithThrottle);
    doPuts(10, FAMILY, QUALIFIER, tableWithoutThrottle);

    // should reject some requests
    assertTrue(10 > doPuts(10, FAMILY, QUALIFIER, tableWithThrottle));
    // should accept all puts
    assertEquals(10, doPuts(10, FAMILY, QUALIFIER, tableWithoutThrottle));

    // Remove all the limits
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userOverrideWithQuota));
    ThrottleQuotaTestUtil.triggerUserCacheRefresh(TEST_UTIL, true, TABLE_NAME);
    assertEquals(10, doPuts(10, FAMILY, QUALIFIER, tableWithThrottle));
    assertEquals(10, doPuts(10, FAMILY, QUALIFIER, tableWithoutThrottle));
  }

}
