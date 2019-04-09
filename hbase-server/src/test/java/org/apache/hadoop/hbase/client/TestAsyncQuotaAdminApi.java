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

import static org.apache.hadoop.hbase.client.AsyncProcess.START_LOG_ERRORS_AFTER_COUNT_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.quotas.QuotaCache;
import org.apache.hadoop.hbase.quotas.QuotaFilter;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.quotas.QuotaSettingsFactory;
import org.apache.hadoop.hbase.quotas.QuotaTableUtil;
import org.apache.hadoop.hbase.quotas.QuotaUtil;
import org.apache.hadoop.hbase.quotas.ThrottleType;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Category({ ClientTests.class, MediumTests.class })
public class TestAsyncQuotaAdminApi extends TestAsyncAdminBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAsyncQuotaAdminApi.class);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    TEST_UTIL.getConfiguration().setInt(QuotaCache.REFRESH_CONF_KEY, 2000);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 60000);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 120000);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    TEST_UTIL.getConfiguration().setInt(START_LOG_ERRORS_AFTER_COUNT_KEY, 0);
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.waitTableAvailable(QuotaTableUtil.QUOTA_TABLE_NAME);
    ASYNC_CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();
  }

  @Test
  public void testThrottleType() throws Exception {
    String userName = User.getCurrent().getShortName();

    admin.setQuota(
      QuotaSettingsFactory.throttleUser(userName, ThrottleType.READ_NUMBER, 6, TimeUnit.MINUTES))
        .get();
    admin.setQuota(
      QuotaSettingsFactory.throttleUser(userName, ThrottleType.WRITE_NUMBER, 12, TimeUnit.MINUTES))
        .get();
    admin.setQuota(QuotaSettingsFactory.bypassGlobals(userName, true)).get();

    int countThrottle = 0;
    int countGlobalBypass = 0;
    for (QuotaSettings settings : admin.getQuota(null).get()) {
      switch (settings.getQuotaType()) {
      case THROTTLE:
        countThrottle++;
        break;
      case GLOBAL_BYPASS:
        countGlobalBypass++;
        break;
      default:
        fail("unexpected settings type: " + settings.getQuotaType());
      }
    }
    assertEquals(2, countThrottle);
    assertEquals(1, countGlobalBypass);

    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName)).get();
    assertNumResults(1, null);
    admin.setQuota(QuotaSettingsFactory.bypassGlobals(userName, false)).get();
    assertNumResults(0, null);
  }

  @Test
  public void testQuotaRetrieverFilter() throws Exception {
    TableName[] tables = new TableName[] { TableName.valueOf("T0"), TableName.valueOf("T01"),
        TableName.valueOf("NS0:T2"), };
    String[] namespaces = new String[] { "NS0", "NS01", "NS2" };
    String[] users = new String[] { "User0", "User01", "User2" };

    for (String user : users) {
      admin.setQuota(
        QuotaSettingsFactory.throttleUser(user, ThrottleType.REQUEST_NUMBER, 1, TimeUnit.MINUTES))
          .get();

      for (TableName table : tables) {
        admin.setQuota(QuotaSettingsFactory.throttleUser(user, table, ThrottleType.REQUEST_NUMBER,
          2, TimeUnit.MINUTES)).get();
      }

      for (String ns : namespaces) {
        admin.setQuota(QuotaSettingsFactory.throttleUser(user, ns, ThrottleType.REQUEST_NUMBER, 3,
          TimeUnit.MINUTES)).get();
      }
    }
    assertNumResults(21, null);

    for (TableName table : tables) {
      admin.setQuota(
        QuotaSettingsFactory.throttleTable(table, ThrottleType.REQUEST_NUMBER, 4, TimeUnit.MINUTES))
          .get();
    }
    assertNumResults(24, null);

    for (String ns : namespaces) {
      admin.setQuota(QuotaSettingsFactory.throttleNamespace(ns, ThrottleType.REQUEST_NUMBER, 5,
        TimeUnit.MINUTES)).get();
    }
    assertNumResults(27, null);

    assertNumResults(7, new QuotaFilter().setUserFilter("User0"));
    assertNumResults(0, new QuotaFilter().setUserFilter("User"));
    assertNumResults(21, new QuotaFilter().setUserFilter("User.*"));
    assertNumResults(3, new QuotaFilter().setUserFilter("User.*").setTableFilter("T0"));
    assertNumResults(3, new QuotaFilter().setUserFilter("User.*").setTableFilter("NS.*"));
    assertNumResults(0, new QuotaFilter().setUserFilter("User.*").setTableFilter("T"));
    assertNumResults(6, new QuotaFilter().setUserFilter("User.*").setTableFilter("T.*"));
    assertNumResults(3, new QuotaFilter().setUserFilter("User.*").setNamespaceFilter("NS0"));
    assertNumResults(0, new QuotaFilter().setUserFilter("User.*").setNamespaceFilter("NS"));
    assertNumResults(9, new QuotaFilter().setUserFilter("User.*").setNamespaceFilter("NS.*"));
    assertNumResults(6,
      new QuotaFilter().setUserFilter("User.*").setTableFilter("T0").setNamespaceFilter("NS0"));
    assertNumResults(1, new QuotaFilter().setTableFilter("T0"));
    assertNumResults(0, new QuotaFilter().setTableFilter("T"));
    assertNumResults(2, new QuotaFilter().setTableFilter("T.*"));
    assertNumResults(3, new QuotaFilter().setTableFilter(".*T.*"));
    assertNumResults(1, new QuotaFilter().setNamespaceFilter("NS0"));
    assertNumResults(0, new QuotaFilter().setNamespaceFilter("NS"));
    assertNumResults(3, new QuotaFilter().setNamespaceFilter("NS.*"));

    for (String user : users) {
      admin.setQuota(QuotaSettingsFactory.unthrottleUser(user)).get();
      for (TableName table : tables) {
        admin.setQuota(QuotaSettingsFactory.unthrottleUser(user, table)).get();
      }
      for (String ns : namespaces) {
        admin.setQuota(QuotaSettingsFactory.unthrottleUser(user, ns)).get();
      }
    }
    assertNumResults(6, null);

    for (TableName table : tables) {
      admin.setQuota(QuotaSettingsFactory.unthrottleTable(table)).get();
    }
    assertNumResults(3, null);

    for (String ns : namespaces) {
      admin.setQuota(QuotaSettingsFactory.unthrottleNamespace(ns)).get();
    }
    assertNumResults(0, null);
  }

  @Test
  public void testSwitchRpcThrottle() throws Exception {
    CompletableFuture<Boolean> future1 = ASYNC_CONN.getAdmin().switchRpcThrottle(true);
    assertEquals(true, future1.get().booleanValue());
    CompletableFuture<Boolean> future2 = ASYNC_CONN.getAdmin().isRpcThrottleEnabled();
    assertEquals(true, future2.get().booleanValue());
  }

  @Test
  public void testSwitchExceedThrottleQuota() throws Exception {
    AsyncAdmin admin = ASYNC_CONN.getAdmin();
    assertEquals(false, admin.exceedThrottleQuotaSwitch(false).get().booleanValue());
  }

  private void assertNumResults(int expected, final QuotaFilter filter) throws Exception {
    assertEquals(expected, countResults(filter));
  }

  private int countResults(final QuotaFilter filter) throws Exception {
    int count = 0;
    for (QuotaSettings settings : admin.getQuota(filter).get()) {
      LOG.debug(Objects.toString(settings));
      count++;
    }
    return count;
  }
}
