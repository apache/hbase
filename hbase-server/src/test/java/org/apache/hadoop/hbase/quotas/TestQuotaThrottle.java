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

import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category({RegionServerTests.class, MediumTests.class})
public class TestQuotaThrottle {
  private final static Log LOG = LogFactory.getLog(TestQuotaThrottle.class);

  private final static int REFRESH_TIME = 30 * 60000;

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static byte[] FAMILY = Bytes.toBytes("cf");
  private final static byte[] QUALIFIER = Bytes.toBytes("q");

  private final static TableName[] TABLE_NAMES = new TableName[] {
    TableName.valueOf("TestQuotaAdmin0"),
    TableName.valueOf("TestQuotaAdmin1"),
    TableName.valueOf("TestQuotaAdmin2")
  };

  private static ManualEnvironmentEdge envEdge;
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

    envEdge = new ManualEnvironmentEdge();
    envEdge.setValue(EnvironmentEdgeManager.currentTime());
    EnvironmentEdgeManagerTestHelper.injectEdge(envEdge);
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
    for (RegionServerThread rst: TEST_UTIL.getMiniHBaseCluster().getRegionServerThreads()) {
      RegionServerQuotaManager quotaManager = rst.getRegionServer().getRegionServerQuotaManager();
      QuotaCache quotaCache = quotaManager.getQuotaCache();
      quotaCache.getNamespaceQuotaCache().clear();
      quotaCache.getTableQuotaCache().clear();
      quotaCache.getUserQuotaCache().clear();
    }
  }

  @Test(timeout=60000)
  public void testUserGlobalThrottle() throws Exception {
    final Admin admin = TEST_UTIL.getHBaseAdmin();
    final String userName = User.getCurrent().getShortName();

    // Add 6req/min limit
    admin.setQuota(QuotaSettingsFactory
      .throttleUser(userName, ThrottleType.REQUEST_NUMBER, 6, TimeUnit.MINUTES));
    triggerUserCacheRefresh(false, TABLE_NAMES);

    // should execute at max 6 requests
    assertEquals(6, doPuts(100, tables));

    // wait a minute and you should get other 6 requests executed
    waitMinuteQuota();
    assertEquals(6, doPuts(100, tables));

    // Remove all the limits
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName));
    triggerUserCacheRefresh(true, TABLE_NAMES);
    assertEquals(60, doPuts(60, tables));
    assertEquals(60, doGets(60, tables));
  }

  @Test(timeout=60000)
  public void testUserGlobalReadAndWriteThrottle() throws Exception {
    final Admin admin = TEST_UTIL.getHBaseAdmin();
    final String userName = User.getCurrent().getShortName();

    // Add 6req/min limit for read request
    admin.setQuota(QuotaSettingsFactory
      .throttleUser(userName, ThrottleType.READ_NUMBER, 6, TimeUnit.MINUTES));
    triggerUserCacheRefresh(false, TABLE_NAMES);

    // not limit for write request and should execute at max 6 read requests
    assertEquals(60, doPuts(60, tables));
    assertEquals(6, doGets(100, tables));

    waitMinuteQuota();

    // Add 6req/min limit for write request
    admin.setQuota(QuotaSettingsFactory
      .throttleUser(userName, ThrottleType.WRITE_NUMBER, 6, TimeUnit.MINUTES));
    triggerUserCacheRefresh(false, TABLE_NAMES);

    // should execute at max 6 read requests and at max 6 write write requests
    assertEquals(6, doGets(100, tables));
    assertEquals(6, doPuts(60, tables));

    // Remove all the limits
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName));
    triggerUserCacheRefresh(true, TABLE_NAMES);
    assertEquals(60, doPuts(60, tables));
    assertEquals(60, doGets(60, tables));
  }

  @Test(timeout=60000)
  public void testUserTableThrottle() throws Exception {
    final Admin admin = TEST_UTIL.getHBaseAdmin();
    final String userName = User.getCurrent().getShortName();

    // Add 6req/min limit
    admin.setQuota(QuotaSettingsFactory
      .throttleUser(userName, TABLE_NAMES[0], ThrottleType.REQUEST_NUMBER, 6, TimeUnit.MINUTES));
    triggerUserCacheRefresh(false, TABLE_NAMES[0]);

    // should execute at max 6 requests on tables[0] and have no limit on tables[1]
    assertEquals(6, doPuts(100, tables[0]));
    assertEquals(30, doPuts(30, tables[1]));

    // wait a minute and you should get other 6 requests executed
    waitMinuteQuota();
    assertEquals(6, doPuts(100, tables[0]));

    // Remove all the limits
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName, TABLE_NAMES[0]));
    triggerUserCacheRefresh(true, TABLE_NAMES);
    assertEquals(60, doPuts(60, tables));
    assertEquals(60, doGets(60, tables));
  }

  @Test(timeout=60000)
  public void testUserTableReadAndWriteThrottle() throws Exception {
    final Admin admin = TEST_UTIL.getHBaseAdmin();
    final String userName = User.getCurrent().getShortName();

    // Add 6req/min limit for write request on tables[0]
    admin.setQuota(QuotaSettingsFactory
      .throttleUser(userName, TABLE_NAMES[0], ThrottleType.WRITE_NUMBER, 6, TimeUnit.MINUTES));
    triggerUserCacheRefresh(false, TABLE_NAMES[0]);

    // should execute at max 6 write requests and have no limit for read request
    assertEquals(6, doPuts(100, tables[0]));
    assertEquals(60, doGets(60, tables[0]));

    // no limit on tables[1]
    assertEquals(60, doPuts(60, tables[1]));
    assertEquals(60, doGets(60, tables[1]));

    // wait a minute and you should get other 6  write requests executed
    waitMinuteQuota();

    // Add 6req/min limit for read request on tables[0]
    admin.setQuota(QuotaSettingsFactory
      .throttleUser(userName, TABLE_NAMES[0], ThrottleType.READ_NUMBER, 6, TimeUnit.MINUTES));
    triggerUserCacheRefresh(false, TABLE_NAMES[0]);

    // should execute at max 6 read requests and at max 6 write requests
    assertEquals(6, doPuts(100, tables[0]));
    assertEquals(6, doGets(60, tables[0]));

    // no limit on tables[1]
    assertEquals(30, doPuts(30, tables[1]));
    assertEquals(30, doGets(30, tables[1]));

    // Remove all the limits
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName, TABLE_NAMES[0]));
    triggerUserCacheRefresh(true, TABLE_NAMES);
    assertEquals(60, doPuts(60, tables));
    assertEquals(60, doGets(60, tables));
  }

  @Test(timeout=60000)
  public void testUserNamespaceThrottle() throws Exception {
    final Admin admin = TEST_UTIL.getHBaseAdmin();
    final String userName = User.getCurrent().getShortName();
    final String NAMESPACE = "default";

    // Add 6req/min limit
    admin.setQuota(QuotaSettingsFactory
      .throttleUser(userName, NAMESPACE, ThrottleType.REQUEST_NUMBER, 6, TimeUnit.MINUTES));
    triggerUserCacheRefresh(false, TABLE_NAMES[0]);

    // should execute at max 6 requests on tables[0] and have no limit on tables[1]
    assertEquals(6, doPuts(100, tables[0]));

    // wait a minute and you should get other 6 requests executed
    waitMinuteQuota();
    assertEquals(6, doPuts(100, tables[1]));

    // Remove all the limits
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName, NAMESPACE));
    triggerUserCacheRefresh(true, TABLE_NAMES);
    assertEquals(60, doPuts(60, tables));
    assertEquals(60, doGets(60, tables));
  }

  @Test(timeout=60000)
  public void testUserNamespaceReadAndWriteThrottle() throws Exception {
    final Admin admin = TEST_UTIL.getHBaseAdmin();
    final String userName = User.getCurrent().getShortName();
    final String NAMESPACE = "default";

    // Add 6req/min limit for read request
    admin.setQuota(QuotaSettingsFactory
      .throttleUser(userName, NAMESPACE, ThrottleType.READ_NUMBER, 6, TimeUnit.MINUTES));
    triggerUserCacheRefresh(false, TABLE_NAMES[0]);

    // should execute at max 6 read requests and have no limit for write request
    assertEquals(6, doGets(60, tables[0]));
    assertEquals(60, doPuts(60, tables[0]));

    waitMinuteQuota();

    // Add 6req/min limit for write request, too
    admin.setQuota(QuotaSettingsFactory
      .throttleUser(userName, NAMESPACE, ThrottleType.WRITE_NUMBER, 6, TimeUnit.MINUTES));
    triggerUserCacheRefresh(false, TABLE_NAMES[0]);

    // should execute at max 6 read requests and at max 6 write requests
    assertEquals(6, doGets(60, tables[0]));
    assertEquals(6, doPuts(60, tables[0]));

    // Remove all the limits
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName, NAMESPACE));
    triggerUserCacheRefresh(true, TABLE_NAMES);
    assertEquals(60, doPuts(60, tables));
    assertEquals(60, doGets(60, tables));
  }

  @Test(timeout=60000)
  public void testTableGlobalThrottle() throws Exception {
    final Admin admin = TEST_UTIL.getHBaseAdmin();

    // Add 6req/min limit
    admin.setQuota(QuotaSettingsFactory
      .throttleTable(TABLE_NAMES[0], ThrottleType.REQUEST_NUMBER, 6, TimeUnit.MINUTES));
    triggerTableCacheRefresh(false, TABLE_NAMES[0]);

    // should execute at max 6 requests
    assertEquals(6, doPuts(100, tables[0]));
    // should have no limits
    assertEquals(30, doPuts(30, tables[1]));

    // wait a minute and you should get other 6 requests executed
    waitMinuteQuota();
    assertEquals(6, doPuts(100, tables[0]));

    // Remove all the limits
    admin.setQuota(QuotaSettingsFactory.unthrottleTable(TABLE_NAMES[0]));
    triggerTableCacheRefresh(true, TABLE_NAMES[0]);
    assertEquals(80, doGets(80, tables[0], tables[1]));
  }

  @Test(timeout=60000)
  public void testTableGlobalReadAndWriteThrottle() throws Exception {
    final Admin admin = TEST_UTIL.getHBaseAdmin();

    // Add 6req/min limit for read request
    admin.setQuota(QuotaSettingsFactory
      .throttleTable(TABLE_NAMES[0], ThrottleType.READ_NUMBER, 6, TimeUnit.MINUTES));
    triggerTableCacheRefresh(false, TABLE_NAMES[0]);

    // should execute at max 6 read requests and have no limit for write request
    assertEquals(6, doGets(100, tables[0]));
    assertEquals(100, doPuts(100, tables[0]));
    // should have no limits on tables[1]
    assertEquals(30, doPuts(30, tables[1]));
    assertEquals(30, doGets(30, tables[1]));

    // wait a minute and you should get other 6 requests executed
    waitMinuteQuota();

    // Add 6req/min limit for write request, too
    admin.setQuota(QuotaSettingsFactory
      .throttleTable(TABLE_NAMES[0], ThrottleType.WRITE_NUMBER, 6, TimeUnit.MINUTES));
    triggerTableCacheRefresh(false, TABLE_NAMES[0]);

    // should execute at max 6 read requests and at max 6 write requests
    assertEquals(6, doGets(100, tables[0]));
    assertEquals(6, doPuts(100, tables[0]));
    // should have no limits on tables[1]
    assertEquals(30, doPuts(30, tables[1]));
    assertEquals(30, doGets(30, tables[1]));

    // Remove all the limits
    admin.setQuota(QuotaSettingsFactory.unthrottleTable(TABLE_NAMES[0]));
    triggerTableCacheRefresh(true, TABLE_NAMES[0]);
    assertEquals(80, doGets(80, tables[0], tables[1]));
  }

  @Test(timeout=60000)
  public void testNamespaceGlobalThrottle() throws Exception {
    final Admin admin = TEST_UTIL.getHBaseAdmin();
    final String NAMESPACE = "default";

    // Add 6req/min limit
    admin.setQuota(QuotaSettingsFactory
      .throttleNamespace(NAMESPACE, ThrottleType.REQUEST_NUMBER, 6, TimeUnit.MINUTES));
    triggerNamespaceCacheRefresh(false, TABLE_NAMES[0]);

    // should execute at max 6 requests
    assertEquals(6, doPuts(100, tables[0]));

    // wait a minute and you should get other 6 requests executed
    waitMinuteQuota();
    assertEquals(6, doPuts(100, tables[1]));

    admin.setQuota(QuotaSettingsFactory.unthrottleNamespace(NAMESPACE));
    triggerNamespaceCacheRefresh(true, TABLE_NAMES[0]);
    assertEquals(40, doPuts(40, tables[0]));
  }

  @Test(timeout=60000)
  public void testNamespaceGlobalReadAndWriteThrottle() throws Exception {
    final Admin admin = TEST_UTIL.getHBaseAdmin();
    final String NAMESPACE = "default";

    // Add 6req/min limit for write request
    admin.setQuota(QuotaSettingsFactory
      .throttleNamespace(NAMESPACE, ThrottleType.WRITE_NUMBER, 6, TimeUnit.MINUTES));
    triggerNamespaceCacheRefresh(false, TABLE_NAMES[0]);

    // should execute at max 6 write requests and no limit for read request
    assertEquals(6, doPuts(100, tables[0]));
    assertEquals(100, doGets(100, tables[0]));

    // wait a minute and you should get other 6 requests executed
    waitMinuteQuota();

    // Add 6req/min limit for read request, too
    admin.setQuota(QuotaSettingsFactory
      .throttleNamespace(NAMESPACE, ThrottleType.READ_NUMBER, 6, TimeUnit.MINUTES));
    triggerNamespaceCacheRefresh(false, TABLE_NAMES[0]);

    // should execute at max 6 write requests and at max 6 read requests
    assertEquals(6, doPuts(100, tables[0]));
    assertEquals(6, doGets(100, tables[0]));

    admin.setQuota(QuotaSettingsFactory.unthrottleNamespace(NAMESPACE));
    triggerNamespaceCacheRefresh(true, TABLE_NAMES[0]);
    assertEquals(40, doPuts(40, tables[0]));
  }

  @Test(timeout=60000)
  public void testUserAndTableThrottle() throws Exception {
    final Admin admin = TEST_UTIL.getHBaseAdmin();
    final String userName = User.getCurrent().getShortName();

    // Add 6req/min limit for the user on tables[0]
    admin.setQuota(QuotaSettingsFactory
      .throttleUser(userName, TABLE_NAMES[0], ThrottleType.REQUEST_NUMBER, 6, TimeUnit.MINUTES));
    triggerUserCacheRefresh(false, TABLE_NAMES[0]);
    // Add 12req/min limit for the user
    admin.setQuota(QuotaSettingsFactory
      .throttleUser(userName, ThrottleType.REQUEST_NUMBER, 12, TimeUnit.MINUTES));
    triggerUserCacheRefresh(false, TABLE_NAMES[1], TABLE_NAMES[2]);
    // Add 8req/min limit for the tables[1]
    admin.setQuota(QuotaSettingsFactory
      .throttleTable(TABLE_NAMES[1], ThrottleType.REQUEST_NUMBER, 8, TimeUnit.MINUTES));
    triggerTableCacheRefresh(false, TABLE_NAMES[1]);
    // Add a lower table level throttle on tables[0]
    admin.setQuota(QuotaSettingsFactory
      .throttleTable(TABLE_NAMES[0], ThrottleType.REQUEST_NUMBER, 3, TimeUnit.MINUTES));
    triggerTableCacheRefresh(false, TABLE_NAMES[0]);

    // should execute at max 12 requests
    assertEquals(12, doGets(100, tables[2]));

    // should execute at max 8 requests
    waitMinuteQuota();
    assertEquals(8, doGets(100, tables[1]));

    // should execute at max 3 requests
    waitMinuteQuota();
    assertEquals(3, doPuts(100, tables[0]));

    // Remove all the throttling rules
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName, TABLE_NAMES[0]));
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName));
    triggerUserCacheRefresh(true, TABLE_NAMES[0], TABLE_NAMES[1]);

    admin.setQuota(QuotaSettingsFactory.unthrottleTable(TABLE_NAMES[1]));
    triggerTableCacheRefresh(true, TABLE_NAMES[1]);
    waitMinuteQuota();
    assertEquals(40, doGets(40, tables[1]));

    admin.setQuota(QuotaSettingsFactory.unthrottleTable(TABLE_NAMES[0]));
    triggerTableCacheRefresh(true, TABLE_NAMES[0]);
    waitMinuteQuota();
    assertEquals(40, doGets(40, tables[0]));
  }

  @Test(timeout=60000)
  public void testUserGlobalBypassThrottle() throws Exception {
    final Admin admin = TEST_UTIL.getHBaseAdmin();
    final String userName = User.getCurrent().getShortName();
    final String NAMESPACE = "default";

    // Add 6req/min limit for tables[0]
    admin.setQuota(QuotaSettingsFactory
      .throttleTable(TABLE_NAMES[0], ThrottleType.REQUEST_NUMBER, 6, TimeUnit.MINUTES));
    triggerTableCacheRefresh(false, TABLE_NAMES[0]);
    // Add 13req/min limit for the user
    admin.setQuota(QuotaSettingsFactory
      .throttleNamespace(NAMESPACE, ThrottleType.REQUEST_NUMBER, 13, TimeUnit.MINUTES));
    triggerNamespaceCacheRefresh(false, TABLE_NAMES[1]);

    // should execute at max 6 requests on table[0] and (13 - 6) on table[1]
    assertEquals(6, doPuts(100, tables[0]));
    assertEquals(7, doGets(100, tables[1]));
    waitMinuteQuota();

    // Set the global bypass for the user
    admin.setQuota(QuotaSettingsFactory.bypassGlobals(userName, true));
    admin.setQuota(QuotaSettingsFactory
      .throttleUser(userName, TABLE_NAMES[2], ThrottleType.REQUEST_NUMBER, 6, TimeUnit.MINUTES));
    triggerUserCacheRefresh(false, TABLE_NAMES[2]);
    assertEquals(30, doGets(30, tables[0]));
    assertEquals(30, doGets(30, tables[1]));
    waitMinuteQuota();

    // Remove the global bypass
    // should execute at max 6 requests on table[0] and (13 - 6) on table[1]
    admin.setQuota(QuotaSettingsFactory.bypassGlobals(userName, false));
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName, TABLE_NAMES[2]));
    triggerUserCacheRefresh(true, TABLE_NAMES[2]);
    assertEquals(6, doPuts(100, tables[0]));
    assertEquals(7, doGets(100, tables[1]));

    // unset throttle
    admin.setQuota(QuotaSettingsFactory.unthrottleTable(TABLE_NAMES[0]));
    admin.setQuota(QuotaSettingsFactory.unthrottleNamespace(NAMESPACE));
    waitMinuteQuota();
    triggerTableCacheRefresh(true, TABLE_NAMES[0]);
    triggerNamespaceCacheRefresh(true, TABLE_NAMES[1]);
    assertEquals(30, doGets(30, tables[0]));
    assertEquals(30, doGets(30, tables[1]));
  }

  private int doPuts(int maxOps, final Table... tables) throws Exception {
    int count = 0;
    try {
      while (count < maxOps) {
        Put put = new Put(Bytes.toBytes("row-" + count));
        put.addColumn(FAMILY, QUALIFIER, Bytes.toBytes("data-" + count));
        for (final Table table: tables) {
          table.put(put);
        }
        count += tables.length;
      }
    } catch (RetriesExhaustedWithDetailsException e) {
      for (Throwable t: e.getCauses()) {
        if (!(t instanceof ThrottlingException)) {
          throw e;
        }
      }
      LOG.error("put failed after nRetries=" + count, e);
    }
    return count;
  }

  private long doGets(int maxOps, final Table... tables) throws Exception {
    int count = 0;
    try {
      while (count < maxOps) {
        Get get = new Get(Bytes.toBytes("row-" + count));
        for (final Table table: tables) {
          table.get(get);
        }
        count += tables.length;
      }
    } catch (ThrottlingException e) {
      LOG.error("get failed after nRetries=" + count, e);
    }
    return count;
  }

  private void triggerUserCacheRefresh(boolean bypass, TableName... tables) throws Exception {
    triggerCacheRefresh(bypass, true, false, false, tables);
  }

  private void triggerTableCacheRefresh(boolean bypass, TableName... tables) throws Exception {
    triggerCacheRefresh(bypass, false, true, false, tables);
  }

  private void triggerNamespaceCacheRefresh(boolean bypass, TableName... tables) throws Exception {
    triggerCacheRefresh(bypass, false, false, true, tables);
  }

  private void triggerCacheRefresh(boolean bypass, boolean userLimiter, boolean tableLimiter,
      boolean nsLimiter, final TableName... tables) throws Exception {
    envEdge.incValue(2 * REFRESH_TIME);
    for (RegionServerThread rst: TEST_UTIL.getMiniHBaseCluster().getRegionServerThreads()) {
      RegionServerQuotaManager quotaManager = rst.getRegionServer().getRegionServerQuotaManager();
      QuotaCache quotaCache = quotaManager.getQuotaCache();

      quotaCache.triggerCacheRefresh();
      // sleep for cache update
      Thread.sleep(250);

      for (TableName table: tables) {
        quotaCache.getTableLimiter(table);
      }

      boolean isUpdated = false;
      while (!isUpdated) {
        quotaCache.triggerCacheRefresh();
        isUpdated = true;
        for (TableName table: tables) {
          boolean isBypass = true;
          if (userLimiter) {
            isBypass &= quotaCache.getUserLimiter(User.getCurrent().getUGI(), table).isBypass();
          }
          if (tableLimiter) {
            isBypass &= quotaCache.getTableLimiter(table).isBypass();
          }
          if (nsLimiter) {
            isBypass &= quotaCache.getNamespaceLimiter(table.getNamespaceAsString()).isBypass();
          }
          if (isBypass != bypass) {
            envEdge.incValue(100);
            isUpdated = false;
            break;
          }
        }
      }

      LOG.debug("QuotaCache");
      LOG.debug(quotaCache.getNamespaceQuotaCache());
      LOG.debug(quotaCache.getTableQuotaCache());
      LOG.debug(quotaCache.getUserQuotaCache());
    }
  }

  private void waitMinuteQuota() {
    envEdge.incValue(70000);
  }
}
