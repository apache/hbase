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
import static org.apache.hadoop.hbase.quotas.ThrottleQuotaTestUtil.triggerNamespaceCacheRefresh;
import static org.apache.hadoop.hbase.quotas.ThrottleQuotaTestUtil.triggerTableCacheRefresh;
import static org.apache.hadoop.hbase.quotas.ThrottleQuotaTestUtil.triggerUserCacheRefresh;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, LargeTests.class })
public class TestClusterScopeQuotaThrottle {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestClusterScopeQuotaThrottle.class);

  private final static int REFRESH_TIME = 30 * 60000;
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private final static TableName[] TABLE_NAMES =
      new TableName[] { TableName.valueOf("TestQuotaAdmin0"), TableName.valueOf("TestQuotaAdmin1"),
          TableName.valueOf("TestQuotaAdmin2") };
  private final static byte[] FAMILY = Bytes.toBytes("cf");
  private final static byte[] QUALIFIER = Bytes.toBytes("q");
  private final static byte[][] SPLITS = new byte[][] { Bytes.toBytes("1") };
  private static Table[] tables;

  private final static String NAMESPACE = "TestNs";
  private final static TableName TABLE_NAME = TableName.valueOf(NAMESPACE, "TestTable");
  private static Table table;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    TEST_UTIL.getConfiguration().setInt(QuotaCache.REFRESH_CONF_KEY, REFRESH_TIME);
    TEST_UTIL.getConfiguration().setInt("hbase.hstore.compactionThreshold", 10);
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 250);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 6);
    TEST_UTIL.getConfiguration().setBoolean("hbase.master.enabletable.roundrobin", true);
    TEST_UTIL.startMiniCluster(2);
    TEST_UTIL.waitTableAvailable(QuotaTableUtil.QUOTA_TABLE_NAME);
    QuotaCache.TEST_FORCE_REFRESH = true;

    tables = new Table[TABLE_NAMES.length];
    for (int i = 0; i < TABLE_NAMES.length; ++i) {
      tables[i] = TEST_UTIL.createTable(TABLE_NAMES[i], FAMILY);
      TEST_UTIL.waitTableAvailable(TABLE_NAMES[i]);
    }
    TEST_UTIL.getAdmin().createNamespace(NamespaceDescriptor.create(NAMESPACE).build());
    table = TEST_UTIL.createTable(TABLE_NAME, FAMILY, SPLITS);
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
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
    TEST_UTIL.deleteTable(TABLE_NAME);
    TEST_UTIL.getAdmin().deleteNamespace(NAMESPACE);
    TEST_UTIL.shutdownMiniCluster();
  }

  @After
  public void tearDown() throws Exception {
    ThrottleQuotaTestUtil.clearQuotaCache(TEST_UTIL);
  }

  @Test
  public void testNamespaceClusterScopeQuota() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();
    final String NAMESPACE = "default";

    // Add 10req/min limit for write request in cluster scope
    admin.setQuota(QuotaSettingsFactory.throttleNamespace(NAMESPACE, ThrottleType.WRITE_NUMBER, 10,
      TimeUnit.MINUTES, QuotaScope.CLUSTER));
    // Add 6req/min limit for read request in machine scope
    admin.setQuota(QuotaSettingsFactory.throttleNamespace(NAMESPACE, ThrottleType.READ_NUMBER, 6,
      TimeUnit.MINUTES, QuotaScope.MACHINE));
    triggerNamespaceCacheRefresh(TEST_UTIL, false, TABLE_NAMES[0]);
    // should execute at max 5 write requests and at max 3 read requests
    assertEquals(5, doPuts(10, FAMILY, QUALIFIER, tables[0]));
    assertEquals(6, doGets(10, tables[0]));
    // Remove all the limits
    admin.setQuota(QuotaSettingsFactory.unthrottleNamespace(NAMESPACE));
    triggerNamespaceCacheRefresh(TEST_UTIL, true, TABLE_NAMES[0]);
  }

  @Test
  public void testTableClusterScopeQuota() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();
    admin.setQuota(QuotaSettingsFactory.throttleTable(TABLE_NAME, ThrottleType.READ_NUMBER, 20,
      TimeUnit.HOURS, QuotaScope.CLUSTER));
    triggerTableCacheRefresh(TEST_UTIL, false, TABLE_NAME);
    for (RegionServerThread rst : TEST_UTIL.getMiniHBaseCluster().getRegionServerThreads()) {
      for (TableName tableName : rst.getRegionServer().getOnlineTables()) {
        if (tableName.getNameAsString().equals(TABLE_NAME.getNameAsString())) {
          int rsRegionNum = rst.getRegionServer().getRegions(tableName).size();
          if (rsRegionNum == 0) {
            // If rs has 0 region, the machine limiter is 0 (20 * 0 / 2)
            break;
          } else if (rsRegionNum == 1) {
            // If rs has 1 region, the machine limiter is 10 (20 * 1 / 2)
            // Read rows from 1 region, so can read 10 first time and 0 second time
            long count = doGets(20, table);
            assertTrue(count == 0 || count == 10);
          } else if (rsRegionNum == 2) {
            // If rs has 2 regions, the machine limiter is 20 (20 * 2 / 2)
            assertEquals(20, doGets(20, table));
          }
          break;
        }
      }
    }
    admin.setQuota(QuotaSettingsFactory.unthrottleTable(TABLE_NAME));
    triggerTableCacheRefresh(TEST_UTIL, true, TABLE_NAME);
  }

  @Test
  public void testUserClusterScopeQuota() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();
    final String userName = User.getCurrent().getShortName();

    // Add 6req/min limit for read request in cluster scope
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, ThrottleType.READ_NUMBER, 6,
      TimeUnit.MINUTES, QuotaScope.CLUSTER));
    // Add 6req/min limit for write request in machine scope
    admin.setQuota(
      QuotaSettingsFactory.throttleUser(userName, ThrottleType.WRITE_NUMBER, 6, TimeUnit.MINUTES));
    triggerUserCacheRefresh(TEST_UTIL, false, TABLE_NAMES);
    // should execute at max 6 read requests and at max 3 write write requests
    assertEquals(6, doPuts(10, FAMILY, QUALIFIER, tables[0]));
    assertEquals(3, doGets(10, tables[0]));
    // Remove all the limits
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName));
    triggerUserCacheRefresh(TEST_UTIL, true, TABLE_NAMES);
  }

  @org.junit.Ignore @Test // Spews the log w/ triggering of scheduler? HBASE-24035
  public void testUserNamespaceClusterScopeQuota() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();
    final String userName = User.getCurrent().getShortName();
    final String namespace = TABLE_NAMES[0].getNamespaceAsString();

    // Add 10req/min limit for read request in cluster scope
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, namespace, ThrottleType.READ_NUMBER,
      10, TimeUnit.MINUTES, QuotaScope.CLUSTER));
    // Add 6req/min limit for write request in machine scope
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, namespace, ThrottleType.WRITE_NUMBER,
      6, TimeUnit.MINUTES));
    triggerUserCacheRefresh(TEST_UTIL, false, TABLE_NAMES[0]);
    // should execute at max 5 read requests and at max 6 write requests
    assertEquals(5, doGets(10, tables[0]));
    assertEquals(6, doPuts(10, FAMILY, QUALIFIER, tables[0]));

    // Remove all the limits
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName, namespace));
    triggerUserCacheRefresh(TEST_UTIL, true, TABLE_NAMES[0]);
  }

  @Test
  public void testUserTableClusterScopeQuota() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();
    final String userName = User.getCurrent().getShortName();
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAME, ThrottleType.READ_NUMBER,
      20, TimeUnit.HOURS, QuotaScope.CLUSTER));
    triggerUserCacheRefresh(TEST_UTIL, false, TABLE_NAME);
    for (RegionServerThread rst : TEST_UTIL.getMiniHBaseCluster().getRegionServerThreads()) {
      for (TableName tableName : rst.getRegionServer().getOnlineTables()) {
        if (tableName.getNameAsString().equals(TABLE_NAME.getNameAsString())) {
          int rsRegionNum = rst.getRegionServer().getRegions(tableName).size();
          if (rsRegionNum == 0) {
            // If rs has 0 region, the machine limiter is 0 (20 * 0 / 2)
            break;
          } else if (rsRegionNum == 1) {
            // If rs has 1 region, the machine limiter is 10 (20 * 1 / 2)
            // Read rows from 1 region, so can read 10 first time and 0 second time
            long count = doGets(20, table);
            assertTrue(count == 0 || count == 10);
          } else if (rsRegionNum == 2) {
            // If rs has 2 regions, the machine limiter is 20 (20 * 2 / 2)
            assertEquals(20, doGets(20, table));
          }
          break;
        }
      }
    }
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName));
    triggerUserCacheRefresh(TEST_UTIL, true, TABLE_NAME);
  }
}
