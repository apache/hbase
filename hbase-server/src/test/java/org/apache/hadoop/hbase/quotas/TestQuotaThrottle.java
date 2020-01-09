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
package org.apache.hadoop.hbase.quotas;

import static org.apache.hadoop.hbase.quotas.ThrottleQuotaTestUtil.doGets;
import static org.apache.hadoop.hbase.quotas.ThrottleQuotaTestUtil.doPuts;
import static org.apache.hadoop.hbase.quotas.ThrottleQuotaTestUtil.triggerExceedThrottleQuotaCacheRefresh;
import static org.apache.hadoop.hbase.quotas.ThrottleQuotaTestUtil.triggerNamespaceCacheRefresh;
import static org.apache.hadoop.hbase.quotas.ThrottleQuotaTestUtil.triggerRegionServerCacheRefresh;
import static org.apache.hadoop.hbase.quotas.ThrottleQuotaTestUtil.triggerTableCacheRefresh;
import static org.apache.hadoop.hbase.quotas.ThrottleQuotaTestUtil.triggerUserCacheRefresh;
import static org.apache.hadoop.hbase.quotas.ThrottleQuotaTestUtil.waitMinuteQuota;
import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
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
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Ignore // Disabled because flakey. Fails ~30% on a resource constrained GCE though not on Apache.
@Category({RegionServerTests.class, MediumTests.class})
public class TestQuotaThrottle {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestQuotaThrottle.class);

  private final static Logger LOG = LoggerFactory.getLogger(TestQuotaThrottle.class);

  private final static int REFRESH_TIME = 30 * 60000;

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static byte[] FAMILY = Bytes.toBytes("cf");
  private final static byte[] QUALIFIER = Bytes.toBytes("q");

  private final static TableName[] TABLE_NAMES = new TableName[] {
    TableName.valueOf("TestQuotaAdmin0"),
    TableName.valueOf("TestQuotaAdmin1"),
    TableName.valueOf("TestQuotaAdmin2")
  };

  private static Table[] tables;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    TEST_UTIL.getConfiguration().setInt(QuotaCache.REFRESH_CONF_KEY, REFRESH_TIME);
    TEST_UTIL.getConfiguration().setInt("hbase.hstore.compactionThreshold", 10);
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 250);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 6);
    TEST_UTIL.getConfiguration().setBoolean("hbase.master.enabletable.roundrobin", true);
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.waitTableAvailable(QuotaTableUtil.QUOTA_TABLE_NAME);
    QuotaCache.TEST_FORCE_REFRESH = true;

    tables = new Table[TABLE_NAMES.length];
    for (int i = 0; i < TABLE_NAMES.length; ++i) {
      tables[i] = TEST_UTIL.createTable(TABLE_NAMES[i], FAMILY);
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    EnvironmentEdgeManager.reset();
    for (int i = 0; i < tables.length; ++i) {
      if (tables[i] != null) {
        tables[i].close();
        TEST_UTIL.deleteTable(TABLE_NAMES[i]);
      }
    }

    TEST_UTIL.shutdownMiniCluster();
  }

  @After
  public void tearDown() throws Exception {
    ThrottleQuotaTestUtil.clearQuotaCache(TEST_UTIL);
  }

  @Test
  public void testUserGlobalThrottle() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();
    final String userName = User.getCurrent().getShortName();

    // Add 6req/min limit
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, ThrottleType.REQUEST_NUMBER, 6,
      TimeUnit.MINUTES));
    triggerUserCacheRefresh(TEST_UTIL, false, TABLE_NAMES);

    // should execute at max 6 requests
    assertEquals(6, doPuts(100, FAMILY, QUALIFIER, tables));

    // wait a minute and you should get other 6 requests executed
    waitMinuteQuota();
    assertEquals(6, doPuts(100, FAMILY, QUALIFIER, tables));

    // Remove all the limits
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName));
    triggerUserCacheRefresh(TEST_UTIL, true, TABLE_NAMES);
    assertEquals(60, doPuts(60, FAMILY, QUALIFIER, tables));
    assertEquals(60, doGets(60, tables));
  }

  @Test
  public void testUserGlobalReadAndWriteThrottle() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();
    final String userName = User.getCurrent().getShortName();

    // Add 6req/min limit for read request
    admin.setQuota(
      QuotaSettingsFactory.throttleUser(userName, ThrottleType.READ_NUMBER, 6, TimeUnit.MINUTES));
    triggerUserCacheRefresh(TEST_UTIL, false, TABLE_NAMES);

    // not limit for write request and should execute at max 6 read requests
    assertEquals(60, doPuts(60, FAMILY, QUALIFIER, tables));
    assertEquals(6, doGets(100, tables));

    waitMinuteQuota();

    // Add 6req/min limit for write request
    admin.setQuota(
      QuotaSettingsFactory.throttleUser(userName, ThrottleType.WRITE_NUMBER, 6, TimeUnit.MINUTES));
    triggerUserCacheRefresh(TEST_UTIL, false, TABLE_NAMES);

    // should execute at max 6 read requests and at max 6 write write requests
    assertEquals(6, doGets(100, tables));
    assertEquals(6, doPuts(60, FAMILY, QUALIFIER, tables));

    // Remove all the limits
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName));
    triggerUserCacheRefresh(TEST_UTIL, true, TABLE_NAMES);
    assertEquals(60, doPuts(60, FAMILY, QUALIFIER, tables));
    assertEquals(60, doGets(60, tables));
  }

  @Test
  public void testUserTableThrottle() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();
    final String userName = User.getCurrent().getShortName();

    // Add 6req/min limit
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[0],
      ThrottleType.REQUEST_NUMBER, 6, TimeUnit.MINUTES));
    triggerUserCacheRefresh(TEST_UTIL, false, TABLE_NAMES[0]);

    // should execute at max 6 requests on tables[0] and have no limit on tables[1]
    assertEquals(6, doPuts(100, FAMILY, QUALIFIER, tables[0]));
    assertEquals(30, doPuts(30, FAMILY, QUALIFIER, tables[1]));

    // wait a minute and you should get other 6 requests executed
    waitMinuteQuota();
    assertEquals(6, doPuts(100, FAMILY, QUALIFIER, tables[0]));

    // Remove all the limits
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName, TABLE_NAMES[0]));
    triggerUserCacheRefresh(TEST_UTIL, true, TABLE_NAMES);
    assertEquals(60, doPuts(60, FAMILY, QUALIFIER, tables));
    assertEquals(60, doGets(60, tables));
  }

  @Test
  public void testUserTableReadAndWriteThrottle() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();
    final String userName = User.getCurrent().getShortName();

    // Add 6req/min limit for write request on tables[0]
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[0],
      ThrottleType.WRITE_NUMBER, 6, TimeUnit.MINUTES));
    triggerUserCacheRefresh(TEST_UTIL, false, TABLE_NAMES[0]);

    // should execute at max 6 write requests and have no limit for read request
    assertEquals(6, doPuts(100, FAMILY, QUALIFIER, tables[0]));
    assertEquals(60, doGets(60, tables[0]));

    // no limit on tables[1]
    assertEquals(60, doPuts(60, FAMILY, QUALIFIER, tables[1]));
    assertEquals(60, doGets(60, tables[1]));

    // wait a minute and you should get other 6 write requests executed
    waitMinuteQuota();

    // Add 6req/min limit for read request on tables[0]
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[0],
      ThrottleType.READ_NUMBER, 6, TimeUnit.MINUTES));
    triggerUserCacheRefresh(TEST_UTIL, false, TABLE_NAMES[0]);

    // should execute at max 6 read requests and at max 6 write requests
    assertEquals(6, doPuts(100, FAMILY, QUALIFIER, tables[0]));
    assertEquals(6, doGets(60, tables[0]));

    // no limit on tables[1]
    assertEquals(30, doPuts(30, FAMILY, QUALIFIER, tables[1]));
    assertEquals(30, doGets(30, tables[1]));

    // Remove all the limits
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName, TABLE_NAMES[0]));
    triggerUserCacheRefresh(TEST_UTIL, true, TABLE_NAMES);
    assertEquals(60, doPuts(60, FAMILY, QUALIFIER, tables));
    assertEquals(60, doGets(60, tables));
  }

  @Test
  public void testUserNamespaceThrottle() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();
    final String userName = User.getCurrent().getShortName();
    final String NAMESPACE = "default";

    // Add 6req/min limit
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, NAMESPACE,
      ThrottleType.REQUEST_NUMBER, 6, TimeUnit.MINUTES));
    triggerUserCacheRefresh(TEST_UTIL, false, TABLE_NAMES[0]);

    // should execute at max 6 requests on tables[0] and have no limit on tables[1]
    assertEquals(6, doPuts(100, FAMILY, QUALIFIER, tables[0]));

    // wait a minute and you should get other 6 requests executed
    waitMinuteQuota();
    assertEquals(6, doPuts(100, FAMILY, QUALIFIER, tables[1]));

    // Remove all the limits
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName, NAMESPACE));
    triggerUserCacheRefresh(TEST_UTIL, true, TABLE_NAMES);
    assertEquals(60, doPuts(60, FAMILY, QUALIFIER, tables));
    assertEquals(60, doGets(60, tables));
  }

  @Test
  public void testUserNamespaceReadAndWriteThrottle() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();
    final String userName = User.getCurrent().getShortName();
    final String NAMESPACE = "default";

    // Add 6req/min limit for read request
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, NAMESPACE, ThrottleType.READ_NUMBER,
      6, TimeUnit.MINUTES));
    triggerUserCacheRefresh(TEST_UTIL, false, TABLE_NAMES[0]);

    // should execute at max 6 read requests and have no limit for write request
    assertEquals(6, doGets(60, tables[0]));
    assertEquals(60, doPuts(60, FAMILY, QUALIFIER, tables[0]));

    waitMinuteQuota();

    // Add 6req/min limit for write request, too
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, NAMESPACE, ThrottleType.WRITE_NUMBER,
      6, TimeUnit.MINUTES));
    triggerUserCacheRefresh(TEST_UTIL, false, TABLE_NAMES[0]);

    // should execute at max 6 read requests and at max 6 write requests
    assertEquals(6, doGets(60, tables[0]));
    assertEquals(6, doPuts(60, FAMILY, QUALIFIER, tables[0]));

    // Remove all the limits
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName, NAMESPACE));
    triggerUserCacheRefresh(TEST_UTIL, true, TABLE_NAMES);
    assertEquals(60, doPuts(60, FAMILY, QUALIFIER, tables));
    assertEquals(60, doGets(60, tables));
  }

  @Test
  public void testTableGlobalThrottle() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();

    // Add 6req/min limit
    admin.setQuota(QuotaSettingsFactory.throttleTable(TABLE_NAMES[0], ThrottleType.REQUEST_NUMBER,
      6, TimeUnit.MINUTES));
    triggerTableCacheRefresh(TEST_UTIL, false, TABLE_NAMES[0]);

    // should execute at max 6 requests
    assertEquals(6, doPuts(100, FAMILY, QUALIFIER, tables[0]));
    // should have no limits
    assertEquals(30, doPuts(30, FAMILY, QUALIFIER, tables[1]));

    // wait a minute and you should get other 6 requests executed
    waitMinuteQuota();
    assertEquals(6, doPuts(100, FAMILY, QUALIFIER, tables[0]));

    // Remove all the limits
    admin.setQuota(QuotaSettingsFactory.unthrottleTable(TABLE_NAMES[0]));
    triggerTableCacheRefresh(TEST_UTIL, true, TABLE_NAMES[0]);
    assertEquals(80, doGets(80, tables[0], tables[1]));
  }

  @Test
  public void testTableGlobalReadAndWriteThrottle() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();

    // Add 6req/min limit for read request
    admin.setQuota(QuotaSettingsFactory.throttleTable(TABLE_NAMES[0], ThrottleType.READ_NUMBER, 6,
      TimeUnit.MINUTES));
    triggerTableCacheRefresh(TEST_UTIL, false, TABLE_NAMES[0]);

    // should execute at max 6 read requests and have no limit for write request
    assertEquals(6, doGets(100, tables[0]));
    assertEquals(100, doPuts(100, FAMILY, QUALIFIER, tables[0]));
    // should have no limits on tables[1]
    assertEquals(30, doPuts(30, FAMILY, QUALIFIER, tables[1]));
    assertEquals(30, doGets(30, tables[1]));

    // wait a minute and you should get other 6 requests executed
    waitMinuteQuota();

    // Add 6req/min limit for write request, too
    admin.setQuota(QuotaSettingsFactory.throttleTable(TABLE_NAMES[0], ThrottleType.WRITE_NUMBER, 6,
      TimeUnit.MINUTES));
    triggerTableCacheRefresh(TEST_UTIL, false, TABLE_NAMES[0]);

    // should execute at max 6 read requests and at max 6 write requests
    assertEquals(6, doGets(100, tables[0]));
    assertEquals(6, doPuts(100, FAMILY, QUALIFIER, tables[0]));
    // should have no limits on tables[1]
    assertEquals(30, doPuts(30, FAMILY, QUALIFIER, tables[1]));
    assertEquals(30, doGets(30, tables[1]));

    // Remove all the limits
    admin.setQuota(QuotaSettingsFactory.unthrottleTable(TABLE_NAMES[0]));
    triggerTableCacheRefresh(TEST_UTIL, true, TABLE_NAMES[0]);
    assertEquals(80, doGets(80, tables[0], tables[1]));
  }

  @Test
  public void testNamespaceGlobalThrottle() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();
    final String NAMESPACE = "default";

    // Add 6req/min limit
    admin.setQuota(QuotaSettingsFactory.throttleNamespace(NAMESPACE, ThrottleType.REQUEST_NUMBER, 6,
      TimeUnit.MINUTES));
    triggerNamespaceCacheRefresh(TEST_UTIL, false, TABLE_NAMES[0]);

    // should execute at max 6 requests
    assertEquals(6, doPuts(100, FAMILY, QUALIFIER, tables[0]));

    // wait a minute and you should get other 6 requests executed
    waitMinuteQuota();
    assertEquals(6, doPuts(100, FAMILY, QUALIFIER, tables[1]));

    admin.setQuota(QuotaSettingsFactory.unthrottleNamespace(NAMESPACE));
    triggerNamespaceCacheRefresh(TEST_UTIL, true, TABLE_NAMES[0]);
    assertEquals(40, doPuts(40, FAMILY, QUALIFIER, tables[0]));
  }

  @Test
  public void testNamespaceGlobalReadAndWriteThrottle() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();
    final String NAMESPACE = "default";

    // Add 6req/min limit for write request
    admin.setQuota(QuotaSettingsFactory.throttleNamespace(NAMESPACE, ThrottleType.WRITE_NUMBER, 6,
      TimeUnit.MINUTES));
    triggerNamespaceCacheRefresh(TEST_UTIL, false, TABLE_NAMES[0]);

    // should execute at max 6 write requests and no limit for read request
    assertEquals(6, doPuts(100, FAMILY, QUALIFIER, tables[0]));
    assertEquals(100, doGets(100, tables[0]));

    // wait a minute and you should get other 6 requests executed
    waitMinuteQuota();

    // Add 6req/min limit for read request, too
    admin.setQuota(QuotaSettingsFactory.throttleNamespace(NAMESPACE, ThrottleType.READ_NUMBER, 6,
      TimeUnit.MINUTES));
    triggerNamespaceCacheRefresh(TEST_UTIL, false, TABLE_NAMES[0]);

    // should execute at max 6 write requests and at max 6 read requests
    assertEquals(6, doPuts(100, FAMILY, QUALIFIER, tables[0]));
    assertEquals(6, doGets(100, tables[0]));

    admin.setQuota(QuotaSettingsFactory.unthrottleNamespace(NAMESPACE));
    triggerNamespaceCacheRefresh(TEST_UTIL, true, TABLE_NAMES[0]);
    assertEquals(40, doPuts(40, FAMILY, QUALIFIER, tables[0]));
  }

  @Test
  public void testUserAndTableThrottle() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();
    final String userName = User.getCurrent().getShortName();

    // Add 6req/min limit for the user on tables[0]
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[0],
      ThrottleType.REQUEST_NUMBER, 6, TimeUnit.MINUTES));
    triggerUserCacheRefresh(TEST_UTIL, false, TABLE_NAMES[0]);
    // Add 12req/min limit for the user
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, ThrottleType.REQUEST_NUMBER, 12,
      TimeUnit.MINUTES));
    triggerUserCacheRefresh(TEST_UTIL, false, TABLE_NAMES[1], TABLE_NAMES[2]);
    // Add 8req/min limit for the tables[1]
    admin.setQuota(QuotaSettingsFactory.throttleTable(TABLE_NAMES[1], ThrottleType.REQUEST_NUMBER,
      8, TimeUnit.MINUTES));
    triggerTableCacheRefresh(TEST_UTIL, false, TABLE_NAMES[1]);
    // Add a lower table level throttle on tables[0]
    admin.setQuota(QuotaSettingsFactory.throttleTable(TABLE_NAMES[0], ThrottleType.REQUEST_NUMBER,
      3, TimeUnit.MINUTES));
    triggerTableCacheRefresh(TEST_UTIL, false, TABLE_NAMES[0]);

    // should execute at max 12 requests
    assertEquals(12, doGets(100, tables[2]));

    // should execute at max 8 requests
    waitMinuteQuota();
    assertEquals(8, doGets(100, tables[1]));

    // should execute at max 3 requests
    waitMinuteQuota();
    assertEquals(3, doPuts(100, FAMILY, QUALIFIER, tables[0]));

    // Remove all the throttling rules
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName, TABLE_NAMES[0]));
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName));
    triggerUserCacheRefresh(TEST_UTIL, true, TABLE_NAMES[0], TABLE_NAMES[1]);

    admin.setQuota(QuotaSettingsFactory.unthrottleTable(TABLE_NAMES[1]));
    triggerTableCacheRefresh(TEST_UTIL, true, TABLE_NAMES[1]);
    waitMinuteQuota();
    assertEquals(40, doGets(40, tables[1]));

    admin.setQuota(QuotaSettingsFactory.unthrottleTable(TABLE_NAMES[0]));
    triggerTableCacheRefresh(TEST_UTIL, true, TABLE_NAMES[0]);
    waitMinuteQuota();
    assertEquals(40, doGets(40, tables[0]));
  }

  @Test
  public void testUserGlobalBypassThrottle() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();
    final String userName = User.getCurrent().getShortName();
    final String NAMESPACE = "default";

    // Add 6req/min limit for tables[0]
    admin.setQuota(QuotaSettingsFactory.throttleTable(TABLE_NAMES[0], ThrottleType.REQUEST_NUMBER,
      6, TimeUnit.MINUTES));
    triggerTableCacheRefresh(TEST_UTIL, false, TABLE_NAMES[0]);
    // Add 13req/min limit for the user
    admin.setQuota(QuotaSettingsFactory.throttleNamespace(NAMESPACE, ThrottleType.REQUEST_NUMBER,
      13, TimeUnit.MINUTES));
    triggerNamespaceCacheRefresh(TEST_UTIL, false, TABLE_NAMES[1]);

    // should execute at max 6 requests on table[0] and (13 - 6) on table[1]
    assertEquals(6, doPuts(100, FAMILY, QUALIFIER, tables[0]));
    assertEquals(7, doGets(100, tables[1]));
    waitMinuteQuota();

    // Set the global bypass for the user
    admin.setQuota(QuotaSettingsFactory.bypassGlobals(userName, true));
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[2],
      ThrottleType.REQUEST_NUMBER, 6, TimeUnit.MINUTES));
    triggerUserCacheRefresh(TEST_UTIL, false, TABLE_NAMES[2]);
    assertEquals(30, doGets(30, tables[0]));
    assertEquals(30, doGets(30, tables[1]));
    waitMinuteQuota();

    // Remove the global bypass
    // should execute at max 6 requests on table[0] and (13 - 6) on table[1]
    admin.setQuota(QuotaSettingsFactory.bypassGlobals(userName, false));
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName, TABLE_NAMES[2]));
    triggerUserCacheRefresh(TEST_UTIL, true, TABLE_NAMES[2]);
    assertEquals(6, doPuts(100, FAMILY, QUALIFIER, tables[0]));
    assertEquals(7, doGets(100, tables[1]));

    // unset throttle
    admin.setQuota(QuotaSettingsFactory.unthrottleTable(TABLE_NAMES[0]));
    admin.setQuota(QuotaSettingsFactory.unthrottleNamespace(NAMESPACE));
    waitMinuteQuota();
    triggerTableCacheRefresh(TEST_UTIL, true, TABLE_NAMES[0]);
    triggerNamespaceCacheRefresh(TEST_UTIL, true, TABLE_NAMES[1]);
    assertEquals(30, doGets(30, tables[0]));
    assertEquals(30, doGets(30, tables[1]));
  }

  @Test
  public void testTableWriteCapacityUnitThrottle() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();

    // Add 6CU/min limit
    admin.setQuota(QuotaSettingsFactory.throttleTable(TABLE_NAMES[0],
      ThrottleType.WRITE_CAPACITY_UNIT, 6, TimeUnit.MINUTES));
    triggerTableCacheRefresh(TEST_UTIL, false, TABLE_NAMES[0]);

    // should execute at max 6 capacity units because each put size is 1 capacity unit
    assertEquals(6, doPuts(20, 10, FAMILY, QUALIFIER, tables[0]));

    // wait a minute and you should execute at max 3 capacity units because each put size is 2
    // capacity unit
    waitMinuteQuota();
    assertEquals(3, doPuts(20, 1025, FAMILY, QUALIFIER, tables[0]));

    admin.setQuota(QuotaSettingsFactory.unthrottleTable(TABLE_NAMES[0]));
    triggerTableCacheRefresh(TEST_UTIL, true, TABLE_NAMES[0]);
  }

  @Test
  public void testTableReadCapacityUnitThrottle() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();

    // Add 6CU/min limit
    admin.setQuota(QuotaSettingsFactory.throttleTable(TABLE_NAMES[0],
      ThrottleType.READ_CAPACITY_UNIT, 6, TimeUnit.MINUTES));
    triggerTableCacheRefresh(TEST_UTIL, false, TABLE_NAMES[0]);

    assertEquals(20, doPuts(20, 10, FAMILY, QUALIFIER, tables[0]));
    // should execute at max 6 capacity units because each get size is 1 capacity unit
    assertEquals(6, doGets(20, tables[0]));

    assertEquals(20, doPuts(20, 2015, FAMILY, QUALIFIER, tables[0]));
    // wait a minute and you should execute at max 3 capacity units because each get size is 2
    // capacity unit on tables[0]
    waitMinuteQuota();
    assertEquals(3, doGets(20, tables[0]));

    admin.setQuota(QuotaSettingsFactory.unthrottleTable(TABLE_NAMES[0]));
    triggerTableCacheRefresh(TEST_UTIL, true, TABLE_NAMES[0]);
  }

  @Test
  public void testTableExistsGetThrottle() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();

    // Add throttle quota
    admin.setQuota(QuotaSettingsFactory.throttleTable(TABLE_NAMES[0], ThrottleType.REQUEST_NUMBER,
      100, TimeUnit.MINUTES));
    triggerTableCacheRefresh(TEST_UTIL, false, TABLE_NAMES[0]);

    Table table = TEST_UTIL.getConnection().getTable(TABLE_NAMES[0]);
    // An exists call when having throttle quota
    table.exists(new Get(Bytes.toBytes("abc")));

    admin.setQuota(QuotaSettingsFactory.unthrottleTable(TABLE_NAMES[0]));
    triggerTableCacheRefresh(TEST_UTIL, true, TABLE_NAMES[0]);
  }

  @Test
  public void testRegionServerThrottle() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();
    admin.setQuota(QuotaSettingsFactory.throttleTable(TABLE_NAMES[0], ThrottleType.WRITE_NUMBER, 5,
      TimeUnit.MINUTES));

    // requests are throttled by table quota
    admin.setQuota(QuotaSettingsFactory.throttleRegionServer(
      QuotaTableUtil.QUOTA_REGION_SERVER_ROW_KEY, ThrottleType.WRITE_NUMBER, 7, TimeUnit.MINUTES));
    triggerTableCacheRefresh(TEST_UTIL, false, TABLE_NAMES[0]);
    triggerRegionServerCacheRefresh(TEST_UTIL, false);
    assertEquals(5, doPuts(10, FAMILY, QUALIFIER, tables[0]));
    triggerRegionServerCacheRefresh(TEST_UTIL, false);
    assertEquals(5, doPuts(10, FAMILY, QUALIFIER, tables[0]));

    // requests are throttled by region server quota
    admin.setQuota(QuotaSettingsFactory.throttleRegionServer(
      QuotaTableUtil.QUOTA_REGION_SERVER_ROW_KEY, ThrottleType.WRITE_NUMBER, 4, TimeUnit.MINUTES));
    triggerRegionServerCacheRefresh(TEST_UTIL, false);
    assertEquals(4, doPuts(10, FAMILY, QUALIFIER, tables[0]));
    triggerRegionServerCacheRefresh(TEST_UTIL, false);
    assertEquals(4, doPuts(10, FAMILY, QUALIFIER, tables[0]));

    // unthrottle
    admin.setQuota(
      QuotaSettingsFactory.unthrottleRegionServer(QuotaTableUtil.QUOTA_REGION_SERVER_ROW_KEY));
    triggerRegionServerCacheRefresh(TEST_UTIL, true);
    admin.setQuota(QuotaSettingsFactory.unthrottleTable(TABLE_NAMES[0]));
    triggerTableCacheRefresh(TEST_UTIL, true, TABLE_NAMES[0]);
    triggerRegionServerCacheRefresh(TEST_UTIL, true);
  }

  @Test
  public void testExceedThrottleQuota() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();
    admin.setQuota(QuotaSettingsFactory.throttleTable(TABLE_NAMES[0], ThrottleType.WRITE_NUMBER, 5,
      TimeUnit.MINUTES));
    triggerTableCacheRefresh(TEST_UTIL, false, TABLE_NAMES[0]);
    admin.setQuota(QuotaSettingsFactory.throttleRegionServer(
      QuotaTableUtil.QUOTA_REGION_SERVER_ROW_KEY, ThrottleType.WRITE_NUMBER, 20, TimeUnit.SECONDS));
    admin.setQuota(QuotaSettingsFactory.throttleRegionServer(
      QuotaTableUtil.QUOTA_REGION_SERVER_ROW_KEY, ThrottleType.READ_NUMBER, 10, TimeUnit.SECONDS));
    triggerRegionServerCacheRefresh(TEST_UTIL, false);

    // enable exceed throttle quota
    admin.exceedThrottleQuotaSwitch(true);
    // exceed table limit and allowed by region server limit
    triggerExceedThrottleQuotaCacheRefresh(TEST_UTIL, true);
    waitMinuteQuota();
    assertEquals(10, doPuts(10, FAMILY, QUALIFIER, tables[0]));
    // exceed table limit and throttled by region server limit
    waitMinuteQuota();
    assertEquals(20, doPuts(25, FAMILY, QUALIFIER, tables[0]));

    // set region server limiter is lower than table limiter
    admin.setQuota(QuotaSettingsFactory.throttleRegionServer(
      QuotaTableUtil.QUOTA_REGION_SERVER_ROW_KEY, ThrottleType.WRITE_NUMBER, 2, TimeUnit.SECONDS));
    triggerRegionServerCacheRefresh(TEST_UTIL, false);
    // throttled by region server limiter
    waitMinuteQuota();
    assertEquals(2, doPuts(10, FAMILY, QUALIFIER, tables[0]));
    admin.setQuota(QuotaSettingsFactory.throttleRegionServer(
      QuotaTableUtil.QUOTA_REGION_SERVER_ROW_KEY, ThrottleType.WRITE_NUMBER, 20, TimeUnit.SECONDS));
    triggerRegionServerCacheRefresh(TEST_UTIL, false);

    // disable exceed throttle quota
    admin.exceedThrottleQuotaSwitch(false);
    triggerExceedThrottleQuotaCacheRefresh(TEST_UTIL, false);
    waitMinuteQuota();
    // throttled by table limit
    assertEquals(5, doPuts(10, FAMILY, QUALIFIER, tables[0]));

    // enable exceed throttle quota and unthrottle region server
    admin.exceedThrottleQuotaSwitch(true);
    triggerExceedThrottleQuotaCacheRefresh(TEST_UTIL, true);
    waitMinuteQuota();
    admin.setQuota(
      QuotaSettingsFactory.unthrottleRegionServer(QuotaTableUtil.QUOTA_REGION_SERVER_ROW_KEY));
    triggerRegionServerCacheRefresh(TEST_UTIL, true);
    waitMinuteQuota();
    // throttled by table limit
    assertEquals(5, doPuts(10, FAMILY, QUALIFIER, tables[0]));

    // disable exceed throttle quota
    admin.exceedThrottleQuotaSwitch(false);
    triggerExceedThrottleQuotaCacheRefresh(TEST_UTIL, false);
    // unthrottle table
    admin.setQuota(QuotaSettingsFactory.unthrottleTable(TABLE_NAMES[0]));
    triggerTableCacheRefresh(TEST_UTIL, true, TABLE_NAMES[0]);
  }
}
