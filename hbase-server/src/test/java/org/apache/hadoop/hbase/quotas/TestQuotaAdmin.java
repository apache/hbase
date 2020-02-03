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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.Quotas;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.SpaceLimitRequest;

/**
 * minicluster tests that validate that quota  entries are properly set in the quota table
 */
@Category({ClientTests.class, LargeTests.class})
public class TestQuotaAdmin {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestQuotaAdmin.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestQuotaAdmin.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private final static TableName[] TABLE_NAMES =
      new TableName[] { TableName.valueOf("TestQuotaAdmin0"), TableName.valueOf("TestQuotaAdmin1"),
          TableName.valueOf("TestQuotaAdmin2") };

  private final static String[] NAMESPACES =
      new String[] { "NAMESPACE01", "NAMESPACE02", "NAMESPACE03" };

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    TEST_UTIL.getConfiguration().setInt(QuotaCache.REFRESH_CONF_KEY, 2000);
    TEST_UTIL.getConfiguration().setInt("hbase.hstore.compactionThreshold", 10);
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 250);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 6);
    TEST_UTIL.getConfiguration().setBoolean("hbase.master.enabletable.roundrobin", true);
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.waitTableAvailable(QuotaTableUtil.QUOTA_TABLE_NAME);
  }

  @After
  public void clearQuotaTable() throws Exception {
    if (TEST_UTIL.getAdmin().tableExists(QuotaUtil.QUOTA_TABLE_NAME)) {
      TEST_UTIL.getAdmin().disableTable(QuotaUtil.QUOTA_TABLE_NAME);
      TEST_UTIL.getAdmin().truncateTable(QuotaUtil.QUOTA_TABLE_NAME, false);
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testThrottleType() throws Exception {
    Admin admin = TEST_UTIL.getAdmin();
    String userName = User.getCurrent().getShortName();

    admin.setQuota(
        QuotaSettingsFactory.throttleUser(userName, ThrottleType.READ_NUMBER, 6, TimeUnit.MINUTES));
    admin.setQuota(QuotaSettingsFactory
        .throttleUser(userName, ThrottleType.WRITE_NUMBER, 12, TimeUnit.MINUTES));
    admin.setQuota(QuotaSettingsFactory.bypassGlobals(userName, true));

    try (QuotaRetriever scanner = QuotaRetriever.open(TEST_UTIL.getConfiguration())) {
      int countThrottle = 0;
      int countGlobalBypass = 0;
      for (QuotaSettings settings: scanner) {
        switch (settings.getQuotaType()) {
          case THROTTLE:
            ThrottleSettings throttle = (ThrottleSettings)settings;
            if (throttle.getSoftLimit() == 6) {
              assertEquals(ThrottleType.READ_NUMBER, throttle.getThrottleType());
            } else if (throttle.getSoftLimit() == 12) {
              assertEquals(ThrottleType.WRITE_NUMBER, throttle.getThrottleType());
            } else {
              fail("should not come here, because don't set quota with this limit");
            }
            assertEquals(userName, throttle.getUserName());
            assertEquals(null, throttle.getTableName());
            assertEquals(null, throttle.getNamespace());
            assertEquals(TimeUnit.MINUTES, throttle.getTimeUnit());
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
    }

    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName));
    assertNumResults(1, null);
    admin.setQuota(QuotaSettingsFactory.bypassGlobals(userName, false));
    assertNumResults(0, null);
  }

  @Test
  public void testSimpleScan() throws Exception {
    Admin admin = TEST_UTIL.getAdmin();
    String userName = User.getCurrent().getShortName();

    admin.setQuota(QuotaSettingsFactory
      .throttleUser(userName, ThrottleType.REQUEST_NUMBER, 6, TimeUnit.MINUTES));
    admin.setQuota(QuotaSettingsFactory.bypassGlobals(userName, true));

    try (QuotaRetriever scanner = QuotaRetriever.open(TEST_UTIL.getConfiguration())) {
      int countThrottle = 0;
      int countGlobalBypass = 0;
      for (QuotaSettings settings: scanner) {
        LOG.debug(Objects.toString(settings));
        switch (settings.getQuotaType()) {
          case THROTTLE:
            ThrottleSettings throttle = (ThrottleSettings)settings;
            assertEquals(userName, throttle.getUserName());
            assertEquals(null, throttle.getTableName());
            assertEquals(null, throttle.getNamespace());
            assertEquals(null, throttle.getRegionServer());
            assertEquals(6, throttle.getSoftLimit());
            assertEquals(TimeUnit.MINUTES, throttle.getTimeUnit());
            countThrottle++;
            break;
          case GLOBAL_BYPASS:
            countGlobalBypass++;
            break;
          default:
            fail("unexpected settings type: " + settings.getQuotaType());
        }
      }
      assertEquals(1, countThrottle);
      assertEquals(1, countGlobalBypass);
    }

    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName));
    assertNumResults(1, null);
    admin.setQuota(QuotaSettingsFactory.bypassGlobals(userName, false));
    assertNumResults(0, null);
  }

  @Test
  public void testMultiQuotaThrottling() throws Exception {
    byte[] FAMILY = Bytes.toBytes("testFamily");
    byte[] ROW = Bytes.toBytes("testRow");
    byte[] QUALIFIER = Bytes.toBytes("testQualifier");
    byte[] VALUE = Bytes.toBytes("testValue");

    Admin admin = TEST_UTIL.getAdmin();
    TableName tableName = TableName.valueOf("testMultiQuotaThrottling");
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build();
    admin.createTable(desc);

    // Set up the quota.
    admin.setQuota(QuotaSettingsFactory.throttleTable(tableName, ThrottleType.WRITE_NUMBER, 6,
        TimeUnit.SECONDS));

    Thread.sleep(1000);
    TEST_UTIL.getRSForFirstRegionInTable(tableName).getRegionServerRpcQuotaManager().
        getQuotaCache().triggerCacheRefresh();
    Thread.sleep(1000);

    Table t =  TEST_UTIL.getConnection().getTable(tableName);
    try {
      int size = 5;
      List actions = new ArrayList();
      Object[] results = new Object[size];

      for (int i = 0; i < size; i++) {
        Put put1 = new Put(ROW);
        put1.addColumn(FAMILY, QUALIFIER, VALUE);
        actions.add(put1);
      }
      t.batch(actions, results);
      t.batch(actions, results);
    } catch (IOException e) {
      fail("Not supposed to get ThrottlingExcepiton " + e);
    } finally {
      t.close();
    }
  }


  @Test
  public void testQuotaRetrieverFilter() throws Exception {
    Admin admin = TEST_UTIL.getAdmin();
    TableName[] tables = new TableName[] {
      TableName.valueOf("T0"), TableName.valueOf("T01"), TableName.valueOf("NS0:T2"),
    };
    String[] namespaces = new String[] { "NS0", "NS01", "NS2" };
    String[] users = new String[] { "User0", "User01", "User2" };

    for (String user: users) {
      admin.setQuota(QuotaSettingsFactory
        .throttleUser(user, ThrottleType.REQUEST_NUMBER, 1, TimeUnit.MINUTES));

      for (TableName table: tables) {
        admin.setQuota(QuotaSettingsFactory
          .throttleUser(user, table, ThrottleType.REQUEST_NUMBER, 2, TimeUnit.MINUTES));
      }

      for (String ns: namespaces) {
        admin.setQuota(QuotaSettingsFactory
          .throttleUser(user, ns, ThrottleType.REQUEST_NUMBER, 3, TimeUnit.MINUTES));
      }
    }
    assertNumResults(21, null);

    for (TableName table: tables) {
      admin.setQuota(QuotaSettingsFactory
        .throttleTable(table, ThrottleType.REQUEST_NUMBER, 4, TimeUnit.MINUTES));
    }
    assertNumResults(24, null);

    for (String ns: namespaces) {
      admin.setQuota(QuotaSettingsFactory
        .throttleNamespace(ns, ThrottleType.REQUEST_NUMBER, 5, TimeUnit.MINUTES));
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
    assertNumResults(6, new QuotaFilter().setUserFilter("User.*")
                                            .setTableFilter("T0").setNamespaceFilter("NS0"));
    assertNumResults(1, new QuotaFilter().setTableFilter("T0"));
    assertNumResults(0, new QuotaFilter().setTableFilter("T"));
    assertNumResults(2, new QuotaFilter().setTableFilter("T.*"));
    assertNumResults(3, new QuotaFilter().setTableFilter(".*T.*"));
    assertNumResults(1, new QuotaFilter().setNamespaceFilter("NS0"));
    assertNumResults(0, new QuotaFilter().setNamespaceFilter("NS"));
    assertNumResults(3, new QuotaFilter().setNamespaceFilter("NS.*"));

    for (String user: users) {
      admin.setQuota(QuotaSettingsFactory.unthrottleUser(user));
      for (TableName table: tables) {
        admin.setQuota(QuotaSettingsFactory.unthrottleUser(user, table));
      }
      for (String ns: namespaces) {
        admin.setQuota(QuotaSettingsFactory.unthrottleUser(user, ns));
      }
    }
    assertNumResults(6, null);

    for (TableName table: tables) {
      admin.setQuota(QuotaSettingsFactory.unthrottleTable(table));
    }
    assertNumResults(3, null);

    for (String ns: namespaces) {
      admin.setQuota(QuotaSettingsFactory.unthrottleNamespace(ns));
    }
    assertNumResults(0, null);
  }

  @Test
  public void testSetGetRemoveSpaceQuota() throws Exception {
    Admin admin = TEST_UTIL.getAdmin();
    final TableName tn = TableName.valueOf("sq_table1");
    final long sizeLimit = 1024L * 1024L * 1024L * 1024L * 5L; // 5TB
    final SpaceViolationPolicy violationPolicy = SpaceViolationPolicy.NO_WRITES;
    QuotaSettings settings = QuotaSettingsFactory.limitTableSpace(tn, sizeLimit, violationPolicy);
    admin.setQuota(settings);

    // Verify the Quotas in the table
    try (Table quotaTable = TEST_UTIL.getConnection().getTable(QuotaTableUtil.QUOTA_TABLE_NAME)) {
      ResultScanner scanner = quotaTable.getScanner(new Scan());
      try {
        Result r = Iterables.getOnlyElement(scanner);
        CellScanner cells = r.cellScanner();
        assertTrue("Expected to find a cell", cells.advance());
        assertSpaceQuota(sizeLimit, violationPolicy, cells.current());
      } finally {
        scanner.close();
      }
    }

    // Verify we can retrieve it via the QuotaRetriever API
    QuotaRetriever scanner = QuotaRetriever.open(admin.getConfiguration());
    try {
      assertSpaceQuota(sizeLimit, violationPolicy, Iterables.getOnlyElement(scanner));
    } finally {
      scanner.close();
    }

    // Now, remove the quota
    QuotaSettings removeQuota = QuotaSettingsFactory.removeTableSpaceLimit(tn);
    admin.setQuota(removeQuota);

    // Verify that the record doesn't exist in the table
    try (Table quotaTable = TEST_UTIL.getConnection().getTable(QuotaTableUtil.QUOTA_TABLE_NAME)) {
      ResultScanner rs = quotaTable.getScanner(new Scan());
      try {
        assertNull("Did not expect to find a quota entry", rs.next());
      } finally {
        rs.close();
      }
    }

    // Verify that we can also not fetch it via the API
    scanner = QuotaRetriever.open(admin.getConfiguration());
    try {
      assertNull("Did not expect to find a quota entry", scanner.next());
    } finally {
      scanner.close();
    }
  }

  @Test
  public void testSetModifyRemoveSpaceQuota() throws Exception {
    Admin admin = TEST_UTIL.getAdmin();
    final TableName tn = TableName.valueOf("sq_table2");
    final long originalSizeLimit = 1024L * 1024L * 1024L * 1024L * 5L; // 5TB
    final SpaceViolationPolicy violationPolicy = SpaceViolationPolicy.NO_WRITES;
    QuotaSettings settings = QuotaSettingsFactory.limitTableSpace(tn, originalSizeLimit,
        violationPolicy);
    admin.setQuota(settings);

    // Verify the Quotas in the table
    try (Table quotaTable = TEST_UTIL.getConnection().getTable(QuotaTableUtil.QUOTA_TABLE_NAME)) {
      ResultScanner scanner = quotaTable.getScanner(new Scan());
      try {
        Result r = Iterables.getOnlyElement(scanner);
        CellScanner cells = r.cellScanner();
        assertTrue("Expected to find a cell", cells.advance());
        assertSpaceQuota(originalSizeLimit, violationPolicy, cells.current());
      } finally {
        scanner.close();
      }
    }

    // Verify we can retrieve it via the QuotaRetriever API
    QuotaRetriever quotaScanner = QuotaRetriever.open(admin.getConfiguration());
    try {
      assertSpaceQuota(originalSizeLimit, violationPolicy, Iterables.getOnlyElement(quotaScanner));
    } finally {
      quotaScanner.close();
    }

    // Setting a new size and policy should be reflected
    final long newSizeLimit = 1024L * 1024L * 1024L * 1024L; // 1TB
    final SpaceViolationPolicy newViolationPolicy = SpaceViolationPolicy.NO_WRITES_COMPACTIONS;
    QuotaSettings newSettings = QuotaSettingsFactory.limitTableSpace(tn, newSizeLimit,
        newViolationPolicy);
    admin.setQuota(newSettings);

    // Verify the new Quotas in the table
    try (Table quotaTable = TEST_UTIL.getConnection().getTable(QuotaTableUtil.QUOTA_TABLE_NAME)) {
      ResultScanner scanner = quotaTable.getScanner(new Scan());
      try {
        Result r = Iterables.getOnlyElement(scanner);
        CellScanner cells = r.cellScanner();
        assertTrue("Expected to find a cell", cells.advance());
        assertSpaceQuota(newSizeLimit, newViolationPolicy, cells.current());
      } finally {
        scanner.close();
      }
    }

    // Verify we can retrieve the new quota via the QuotaRetriever API
    quotaScanner = QuotaRetriever.open(admin.getConfiguration());
    try {
      assertSpaceQuota(newSizeLimit, newViolationPolicy, Iterables.getOnlyElement(quotaScanner));
    } finally {
      quotaScanner.close();
    }

    // Now, remove the quota
    QuotaSettings removeQuota = QuotaSettingsFactory.removeTableSpaceLimit(tn);
    admin.setQuota(removeQuota);

    // Verify that the record doesn't exist in the table
    try (Table quotaTable = TEST_UTIL.getConnection().getTable(QuotaTableUtil.QUOTA_TABLE_NAME)) {
      ResultScanner scanner = quotaTable.getScanner(new Scan());
      try {
        assertNull("Did not expect to find a quota entry", scanner.next());
      } finally {
        scanner.close();
      }
    }

    // Verify that we can also not fetch it via the API
    quotaScanner = QuotaRetriever.open(admin.getConfiguration());
    try {
      assertNull("Did not expect to find a quota entry", quotaScanner.next());
    } finally {
      quotaScanner.close();
    }
  }

  private void assertNumResults(int expected, final QuotaFilter filter) throws Exception {
    assertEquals(expected, countResults(filter));
  }

  @Test
  public void testSetGetRemoveRPCQuota() throws Exception {
    testSetGetRemoveRPCQuota(ThrottleType.REQUEST_SIZE);
    testSetGetRemoveRPCQuota(ThrottleType.REQUEST_CAPACITY_UNIT);
  }

  private void testSetGetRemoveRPCQuota(ThrottleType throttleType) throws Exception {
    Admin admin = TEST_UTIL.getAdmin();
    final TableName tn = TableName.valueOf("sq_table1");
    QuotaSettings settings =
        QuotaSettingsFactory.throttleTable(tn, throttleType, 2L, TimeUnit.HOURS);
    admin.setQuota(settings);

    // Verify the Quota in the table
    verifyRecordPresentInQuotaTable(throttleType, 2L, TimeUnit.HOURS);

    // Verify we can retrieve it via the QuotaRetriever API
    verifyFetchableViaAPI(admin, throttleType, 2L, TimeUnit.HOURS);

    // Now, remove the quota
    QuotaSettings removeQuota = QuotaSettingsFactory.unthrottleTable(tn);
    admin.setQuota(removeQuota);

    // Verify that the record doesn't exist in the table
    verifyRecordNotPresentInQuotaTable();

    // Verify that we can also not fetch it via the API
    verifyNotFetchableViaAPI(admin);
  }

  @Test
  public void testSetModifyRemoveRPCQuota() throws Exception {
    Admin admin = TEST_UTIL.getAdmin();
    final TableName tn = TableName.valueOf("sq_table1");
    QuotaSettings settings =
        QuotaSettingsFactory.throttleTable(tn, ThrottleType.REQUEST_SIZE, 2L, TimeUnit.HOURS);
    admin.setQuota(settings);

    // Verify the Quota in the table
    verifyRecordPresentInQuotaTable(ThrottleType.REQUEST_SIZE, 2L, TimeUnit.HOURS);

    // Verify we can retrieve it via the QuotaRetriever API
    verifyFetchableViaAPI(admin, ThrottleType.REQUEST_SIZE, 2L, TimeUnit.HOURS);

    // Setting a limit and time unit should be reflected
    QuotaSettings newSettings =
        QuotaSettingsFactory.throttleTable(tn, ThrottleType.REQUEST_SIZE, 3L, TimeUnit.DAYS);
    admin.setQuota(newSettings);

    // Verify the new Quota in the table
    verifyRecordPresentInQuotaTable(ThrottleType.REQUEST_SIZE, 3L, TimeUnit.DAYS);

    // Verify we can retrieve the new quota via the QuotaRetriever API
    verifyFetchableViaAPI(admin, ThrottleType.REQUEST_SIZE, 3L, TimeUnit.DAYS);

    // Now, remove the quota
    QuotaSettings removeQuota = QuotaSettingsFactory.unthrottleTable(tn);
    admin.setQuota(removeQuota);

    // Verify that the record doesn't exist in the table
    verifyRecordNotPresentInQuotaTable();

    // Verify that we can also not fetch it via the API
    verifyNotFetchableViaAPI(admin);

  }

  @Test
  public void testSetAndRemoveRegionServerQuota() throws Exception {
    Admin admin = TEST_UTIL.getAdmin();
    String regionServer = QuotaTableUtil.QUOTA_REGION_SERVER_ROW_KEY;
    QuotaFilter rsFilter = new QuotaFilter().setRegionServerFilter(regionServer);

    admin.setQuota(QuotaSettingsFactory.throttleRegionServer(regionServer,
      ThrottleType.REQUEST_NUMBER, 10, TimeUnit.MINUTES));
    assertNumResults(1, rsFilter);
    // Verify the Quota in the table
    verifyRecordPresentInQuotaTable(ThrottleType.REQUEST_NUMBER, 10, TimeUnit.MINUTES);

    admin.setQuota(QuotaSettingsFactory.throttleRegionServer(regionServer,
      ThrottleType.REQUEST_NUMBER, 20, TimeUnit.MINUTES));
    assertNumResults(1, rsFilter);
    // Verify the Quota in the table
    verifyRecordPresentInQuotaTable(ThrottleType.REQUEST_NUMBER, 20, TimeUnit.MINUTES);

    admin.setQuota(QuotaSettingsFactory.throttleRegionServer(regionServer, ThrottleType.READ_NUMBER,
      30, TimeUnit.SECONDS));
    int count = 0;
    QuotaRetriever scanner = QuotaRetriever.open(TEST_UTIL.getConfiguration(), rsFilter);
    try {
      for (QuotaSettings settings : scanner) {
        assertTrue(settings.getQuotaType() == QuotaType.THROTTLE);
        ThrottleSettings throttleSettings = (ThrottleSettings) settings;
        assertEquals(regionServer, throttleSettings.getRegionServer());
        count++;
        if (throttleSettings.getThrottleType() == ThrottleType.REQUEST_NUMBER) {
          assertEquals(20, throttleSettings.getSoftLimit());
          assertEquals(TimeUnit.MINUTES, throttleSettings.getTimeUnit());
        } else if (throttleSettings.getThrottleType() == ThrottleType.READ_NUMBER) {
          assertEquals(30, throttleSettings.getSoftLimit());
          assertEquals(TimeUnit.SECONDS, throttleSettings.getTimeUnit());
        }
      }
    } finally {
      scanner.close();
    }
    assertEquals(2, count);

    admin.setQuota(QuotaSettingsFactory.unthrottleRegionServer(regionServer));
    assertNumResults(0, new QuotaFilter().setRegionServerFilter(regionServer));
  }

  @Test
  public void testRpcThrottleWhenStartup() throws IOException, InterruptedException {
    TEST_UTIL.getAdmin().switchRpcThrottle(false);
    assertFalse(TEST_UTIL.getAdmin().isRpcThrottleEnabled());
    TEST_UTIL.killMiniHBaseCluster();

    TEST_UTIL.startMiniHBaseCluster();
    assertFalse(TEST_UTIL.getAdmin().isRpcThrottleEnabled());
    for (JVMClusterUtil.RegionServerThread rs : TEST_UTIL.getHBaseCluster()
        .getRegionServerThreads()) {
      RegionServerRpcQuotaManager quotaManager =
          rs.getRegionServer().getRegionServerRpcQuotaManager();
      assertFalse(quotaManager.isRpcThrottleEnabled());
    }
    // enable rpc throttle
    TEST_UTIL.getAdmin().switchRpcThrottle(true);
    assertTrue(TEST_UTIL.getAdmin().isRpcThrottleEnabled());
  }

  @Test
  public void testSwitchRpcThrottle() throws IOException {
    Admin admin = TEST_UTIL.getAdmin();
    testSwitchRpcThrottle(admin, true, true);
    testSwitchRpcThrottle(admin, true, false);
    testSwitchRpcThrottle(admin, false, false);
    testSwitchRpcThrottle(admin, false, true);
  }

  @Test
  public void testSwitchExceedThrottleQuota() throws IOException {
    String regionServer = QuotaTableUtil.QUOTA_REGION_SERVER_ROW_KEY;
    Admin admin = TEST_UTIL.getAdmin();

    try {
      admin.exceedThrottleQuotaSwitch(true);
      fail("should not come here, because can't enable exceed throttle quota "
          + "if there is no region server quota");
    } catch (IOException e) {
      LOG.warn("Expected exception", e);
    }

    admin.setQuota(QuotaSettingsFactory.throttleRegionServer(regionServer,
      ThrottleType.WRITE_NUMBER, 100, TimeUnit.SECONDS));
    try {
      admin.exceedThrottleQuotaSwitch(true);
      fail("should not come here, because can't enable exceed throttle quota "
          + "if there is no read region server quota");
    } catch (IOException e) {
      LOG.warn("Expected exception", e);
    }

    admin.setQuota(QuotaSettingsFactory.throttleRegionServer(regionServer, ThrottleType.READ_NUMBER,
      20, TimeUnit.MINUTES));
    try {
      admin.exceedThrottleQuotaSwitch(true);
      fail("should not come here, because can't enable exceed throttle quota "
          + "because not all region server quota are in seconds time unit");
    } catch (IOException e) {
      LOG.warn("Expected exception", e);
    }
    admin.setQuota(QuotaSettingsFactory.throttleRegionServer(regionServer, ThrottleType.READ_NUMBER,
      20, TimeUnit.SECONDS));

    assertFalse(admin.exceedThrottleQuotaSwitch(true));
    assertTrue(admin.exceedThrottleQuotaSwitch(true));
    assertTrue(admin.exceedThrottleQuotaSwitch(false));
    assertFalse(admin.exceedThrottleQuotaSwitch(false));
    assertEquals(2, admin.getQuota(new QuotaFilter()).size());
    admin.setQuota(QuotaSettingsFactory.unthrottleRegionServer(regionServer));
  }

  @Test
  public void testQuotaScope() throws Exception {
    Admin admin = TEST_UTIL.getAdmin();
    String user = "user1";
    String namespace = "testQuotaScope_ns";
    TableName tableName = TableName.valueOf("testQuotaScope");
    QuotaFilter filter = new QuotaFilter();

    // set CLUSTER quota scope for namespace
    admin.setQuota(QuotaSettingsFactory.throttleNamespace(namespace, ThrottleType.REQUEST_NUMBER,
      10, TimeUnit.MINUTES, QuotaScope.CLUSTER));
    assertNumResults(1, filter);
    verifyRecordPresentInQuotaTable(ThrottleType.REQUEST_NUMBER, 10, TimeUnit.MINUTES,
      QuotaScope.CLUSTER);
    admin.setQuota(QuotaSettingsFactory.throttleNamespace(namespace, ThrottleType.REQUEST_NUMBER,
      10, TimeUnit.MINUTES, QuotaScope.MACHINE));
    assertNumResults(1, filter);
    verifyRecordPresentInQuotaTable(ThrottleType.REQUEST_NUMBER, 10, TimeUnit.MINUTES,
      QuotaScope.MACHINE);
    admin.setQuota(QuotaSettingsFactory.unthrottleNamespace(namespace));
    assertNumResults(0, filter);

    // set CLUSTER quota scope for table
    admin.setQuota(QuotaSettingsFactory.throttleTable(tableName, ThrottleType.REQUEST_NUMBER, 10,
      TimeUnit.MINUTES, QuotaScope.CLUSTER));
    verifyRecordPresentInQuotaTable(ThrottleType.REQUEST_NUMBER, 10, TimeUnit.MINUTES,
      QuotaScope.CLUSTER);
    admin.setQuota(QuotaSettingsFactory.unthrottleTable(tableName));

    // set CLUSTER quota scope for user
    admin.setQuota(QuotaSettingsFactory.throttleUser(user, ThrottleType.REQUEST_NUMBER, 10,
      TimeUnit.MINUTES, QuotaScope.CLUSTER));
    verifyRecordPresentInQuotaTable(ThrottleType.REQUEST_NUMBER, 10, TimeUnit.MINUTES,
      QuotaScope.CLUSTER);
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(user));

      // set CLUSTER quota scope for user and table
    admin.setQuota(QuotaSettingsFactory.throttleUser(user, tableName, ThrottleType.REQUEST_NUMBER,
      10, TimeUnit.MINUTES, QuotaScope.CLUSTER));
    verifyRecordPresentInQuotaTable(ThrottleType.REQUEST_NUMBER, 10, TimeUnit.MINUTES,
      QuotaScope.CLUSTER);
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(user));

      // set CLUSTER quota scope for user and namespace
    admin.setQuota(QuotaSettingsFactory.throttleUser(user, namespace, ThrottleType.REQUEST_NUMBER,
      10, TimeUnit.MINUTES, QuotaScope.CLUSTER));
    verifyRecordPresentInQuotaTable(ThrottleType.REQUEST_NUMBER, 10, TimeUnit.MINUTES,
      QuotaScope.CLUSTER);
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(user));
  }

  private void testSwitchRpcThrottle(Admin admin, boolean oldRpcThrottle, boolean newRpcThrottle)
      throws IOException {
    boolean state = admin.switchRpcThrottle(newRpcThrottle);
    Assert.assertEquals(oldRpcThrottle, state);
    Assert.assertEquals(newRpcThrottle, admin.isRpcThrottleEnabled());
    TEST_UTIL.getHBaseCluster().getRegionServerThreads().stream()
        .forEach(rs -> Assert.assertEquals(newRpcThrottle,
          rs.getRegionServer().getRegionServerRpcQuotaManager().isRpcThrottleEnabled()));
  }

  private void verifyRecordPresentInQuotaTable(ThrottleType type, long limit, TimeUnit tu)
      throws Exception {
    verifyRecordPresentInQuotaTable(type, limit, tu, QuotaScope.MACHINE);
  }

  private void verifyRecordPresentInQuotaTable(ThrottleType type, long limit, TimeUnit tu,
      QuotaScope scope) throws Exception {
    // Verify the RPC Quotas in the table
    try (Table quotaTable = TEST_UTIL.getConnection().getTable(QuotaTableUtil.QUOTA_TABLE_NAME);
        ResultScanner scanner = quotaTable.getScanner(new Scan())) {
      Result r = Iterables.getOnlyElement(scanner);
      CellScanner cells = r.cellScanner();
      assertTrue("Expected to find a cell", cells.advance());
      assertRPCQuota(type, limit, tu, scope, cells.current());
    }
  }

  private void verifyRecordNotPresentInQuotaTable() throws Exception {
    // Verify that the record doesn't exist in the QuotaTableUtil.QUOTA_TABLE_NAME
    try (Table quotaTable = TEST_UTIL.getConnection().getTable(QuotaTableUtil.QUOTA_TABLE_NAME);
        ResultScanner scanner = quotaTable.getScanner(new Scan())) {
      assertNull("Did not expect to find a quota entry", scanner.next());
    }
  }

  private void verifyFetchableViaAPI(Admin admin, ThrottleType type, long limit, TimeUnit tu)
      throws Exception {
    // Verify we can retrieve the new quota via the QuotaRetriever API
    try (QuotaRetriever quotaScanner = QuotaRetriever.open(admin.getConfiguration())) {
      assertRPCQuota(type, limit, tu, Iterables.getOnlyElement(quotaScanner));
    }
  }

  private void verifyNotFetchableViaAPI(Admin admin) throws Exception {
    // Verify that we can also not fetch it via the API
    try (QuotaRetriever quotaScanner = QuotaRetriever.open(admin.getConfiguration())) {
      assertNull("Did not expect to find a quota entry", quotaScanner.next());
    }
  }

  private void assertRPCQuota(ThrottleType type, long limit, TimeUnit tu, QuotaScope scope,
      Cell cell) throws Exception {
    Quotas q = QuotaTableUtil
        .quotasFromData(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
    assertTrue("Quota should have rpc quota defined", q.hasThrottle());

    QuotaProtos.Throttle rpcQuota = q.getThrottle();
    QuotaProtos.TimedQuota t = null;

    switch (type) {
      case REQUEST_SIZE:
        assertTrue(rpcQuota.hasReqSize());
        t = rpcQuota.getReqSize();
        break;
      case READ_NUMBER:
        assertTrue(rpcQuota.hasReadNum());
        t = rpcQuota.getReadNum();
        break;
      case READ_SIZE:
        assertTrue(rpcQuota.hasReadSize());
        t = rpcQuota.getReadSize();
        break;
      case REQUEST_NUMBER:
        assertTrue(rpcQuota.hasReqNum());
        t = rpcQuota.getReqNum();
        break;
      case WRITE_NUMBER:
        assertTrue(rpcQuota.hasWriteNum());
        t = rpcQuota.getWriteNum();
        break;
      case WRITE_SIZE:
        assertTrue(rpcQuota.hasWriteSize());
        t = rpcQuota.getWriteSize();
        break;
      case REQUEST_CAPACITY_UNIT:
        assertTrue(rpcQuota.hasReqCapacityUnit());
        t = rpcQuota.getReqCapacityUnit();
        break;
      case READ_CAPACITY_UNIT:
        assertTrue(rpcQuota.hasReadCapacityUnit());
        t = rpcQuota.getReadCapacityUnit();
        break;
      case WRITE_CAPACITY_UNIT:
        assertTrue(rpcQuota.hasWriteCapacityUnit());
        t = rpcQuota.getWriteCapacityUnit();
        break;
      default:
    }

    assertEquals(scope, ProtobufUtil.toQuotaScope(t.getScope()));
    assertEquals(t.getSoftLimit(), limit);
    assertEquals(t.getTimeUnit(), ProtobufUtil.toProtoTimeUnit(tu));
  }

  private void assertRPCQuota(ThrottleType type, long limit, TimeUnit tu,
      QuotaSettings actualSettings) throws Exception {
    assertTrue(
        "The actual QuotaSettings was not an instance of " + ThrottleSettings.class + " but of "
            + actualSettings.getClass(), actualSettings instanceof ThrottleSettings);
    QuotaProtos.ThrottleRequest throttleRequest = ((ThrottleSettings) actualSettings).getProto();
    assertEquals(limit, throttleRequest.getTimedQuota().getSoftLimit());
    assertEquals(ProtobufUtil.toProtoTimeUnit(tu), throttleRequest.getTimedQuota().getTimeUnit());
    assertEquals(ProtobufUtil.toProtoThrottleType(type), throttleRequest.getType());
  }

  private void assertSpaceQuota(
      long sizeLimit, SpaceViolationPolicy violationPolicy, Cell cell) throws Exception {
    Quotas q = QuotaTableUtil.quotasFromData(
        cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
    assertTrue("Quota should have space quota defined", q.hasSpace());
    QuotaProtos.SpaceQuota spaceQuota = q.getSpace();
    assertEquals(sizeLimit, spaceQuota.getSoftLimit());
    assertEquals(violationPolicy, ProtobufUtil.toViolationPolicy(spaceQuota.getViolationPolicy()));
  }

  private void assertSpaceQuota(
      long sizeLimit, SpaceViolationPolicy violationPolicy, QuotaSettings actualSettings) {
    assertTrue("The actual QuotaSettings was not an instance of " + SpaceLimitSettings.class
        + " but of " + actualSettings.getClass(), actualSettings instanceof SpaceLimitSettings);
    SpaceLimitRequest spaceLimitRequest = ((SpaceLimitSettings) actualSettings).getProto();
    assertEquals(sizeLimit, spaceLimitRequest.getQuota().getSoftLimit());
    assertEquals(violationPolicy,
        ProtobufUtil.toViolationPolicy(spaceLimitRequest.getQuota().getViolationPolicy()));
  }

  private int countResults(final QuotaFilter filter) throws Exception {
    QuotaRetriever scanner = QuotaRetriever.open(TEST_UTIL.getConfiguration(), filter);
    try {
      int count = 0;
      for (QuotaSettings settings: scanner) {
        LOG.debug(Objects.toString(settings));
        count++;
      }
      return count;
    } finally {
      scanner.close();
    }
  }

  @Test
  public void testUserUnThrottleByType() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();
    final String userName = User.getCurrent().getShortName();
    String userName01 = "user01";
    // Add 6req/min limit
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, ThrottleType.REQUEST_NUMBER, 6,
      TimeUnit.MINUTES));
    admin.setQuota(
      QuotaSettingsFactory.throttleUser(userName, ThrottleType.REQUEST_SIZE, 6, TimeUnit.MINUTES));
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName01, ThrottleType.REQUEST_NUMBER, 6,
      TimeUnit.MINUTES));
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName01, ThrottleType.REQUEST_SIZE, 6,
      TimeUnit.MINUTES));
    admin.setQuota(
      QuotaSettingsFactory.unthrottleUserByThrottleType(userName, ThrottleType.REQUEST_NUMBER));
    assertEquals(3, getQuotaSettingCount(admin));
    admin.setQuota(
      QuotaSettingsFactory.unthrottleUserByThrottleType(userName, ThrottleType.REQUEST_SIZE));
    assertEquals(2, getQuotaSettingCount(admin));
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName01));
    assertEquals(0, getQuotaSettingCount(admin));
  }

  @Test
  public void testUserTableUnThrottleByType() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();
    final String userName = User.getCurrent().getShortName();
    String userName01 = "user01";
    // Add 6req/min limit
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[0],
      ThrottleType.REQUEST_NUMBER, 6, TimeUnit.MINUTES));
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[0],
      ThrottleType.REQUEST_SIZE, 6, TimeUnit.MINUTES));
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName01, TABLE_NAMES[1],
      ThrottleType.REQUEST_NUMBER, 6, TimeUnit.MINUTES));
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName01, TABLE_NAMES[1],
      ThrottleType.REQUEST_SIZE, 6, TimeUnit.MINUTES));
    admin.setQuota(QuotaSettingsFactory.unthrottleUserByThrottleType(userName, TABLE_NAMES[0],
      ThrottleType.REQUEST_NUMBER));
    assertEquals(3, getQuotaSettingCount(admin));
    admin.setQuota(QuotaSettingsFactory.unthrottleUserByThrottleType(userName, TABLE_NAMES[0],
      ThrottleType.REQUEST_SIZE));
    assertEquals(2, getQuotaSettingCount(admin));
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName01));
    assertEquals(0, getQuotaSettingCount(admin));
  }

  @Test
  public void testUserNameSpaceUnThrottleByType() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();
    final String userName = User.getCurrent().getShortName();
    String userName01 = "user01";
    // Add 6req/min limit
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, NAMESPACES[0],
      ThrottleType.REQUEST_NUMBER, 6, TimeUnit.MINUTES));
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, NAMESPACES[0],
      ThrottleType.REQUEST_SIZE, 6, TimeUnit.MINUTES));
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName01, NAMESPACES[1],
      ThrottleType.REQUEST_NUMBER, 6, TimeUnit.MINUTES));
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName01, NAMESPACES[1],
      ThrottleType.REQUEST_SIZE, 6, TimeUnit.MINUTES));
    admin.setQuota(QuotaSettingsFactory.unthrottleUserByThrottleType(userName, NAMESPACES[0],
      ThrottleType.REQUEST_NUMBER));
    assertEquals(3, getQuotaSettingCount(admin));
    admin.setQuota(QuotaSettingsFactory.unthrottleUserByThrottleType(userName, NAMESPACES[0],
      ThrottleType.REQUEST_SIZE));
    assertEquals(2, getQuotaSettingCount(admin));
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName01));
    assertEquals(0, getQuotaSettingCount(admin));
  }

  @Test
  public void testTableUnThrottleByType() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();
    final String userName = User.getCurrent().getShortName();
    // Add 6req/min limit
    admin.setQuota(QuotaSettingsFactory.throttleTable(TABLE_NAMES[0], ThrottleType.REQUEST_NUMBER,
      6, TimeUnit.MINUTES));
    admin.setQuota(QuotaSettingsFactory.throttleTable(TABLE_NAMES[0], ThrottleType.REQUEST_SIZE, 6,
      TimeUnit.MINUTES));
    admin.setQuota(QuotaSettingsFactory.throttleTable(TABLE_NAMES[1], ThrottleType.REQUEST_NUMBER,
      6, TimeUnit.MINUTES));
    admin.setQuota(QuotaSettingsFactory.throttleTable(TABLE_NAMES[1], ThrottleType.REQUEST_SIZE, 6,
      TimeUnit.MINUTES));
    admin.setQuota(QuotaSettingsFactory.unthrottleTableByThrottleType(TABLE_NAMES[0],
      ThrottleType.REQUEST_NUMBER));
    assertEquals(3, getQuotaSettingCount(admin));
    admin.setQuota(QuotaSettingsFactory.unthrottleTableByThrottleType(TABLE_NAMES[0],
      ThrottleType.REQUEST_SIZE));
    assertEquals(2, getQuotaSettingCount(admin));
    admin.setQuota(QuotaSettingsFactory.unthrottleTable(TABLE_NAMES[1]));
    assertEquals(0, getQuotaSettingCount(admin));
  }

  @Test
  public void testNameSpaceUnThrottleByType() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();
    final String userName = User.getCurrent().getShortName();
    // Add 6req/min limit
    admin.setQuota(QuotaSettingsFactory.throttleNamespace(NAMESPACES[0],
      ThrottleType.REQUEST_NUMBER, 6, TimeUnit.MINUTES));
    admin.setQuota(QuotaSettingsFactory.throttleNamespace(NAMESPACES[0], ThrottleType.REQUEST_SIZE,
      6, TimeUnit.MINUTES));
    admin.setQuota(QuotaSettingsFactory.throttleNamespace(NAMESPACES[1],
      ThrottleType.REQUEST_NUMBER, 6, TimeUnit.MINUTES));
    admin.setQuota(QuotaSettingsFactory.throttleNamespace(NAMESPACES[1], ThrottleType.REQUEST_SIZE,
      6, TimeUnit.MINUTES));
    admin.setQuota(QuotaSettingsFactory.unthrottleNamespaceByThrottleType(NAMESPACES[0],
      ThrottleType.REQUEST_NUMBER));
    assertEquals(3, getQuotaSettingCount(admin));
    admin.setQuota(QuotaSettingsFactory.unthrottleNamespaceByThrottleType(NAMESPACES[0],
      ThrottleType.REQUEST_SIZE));
    assertEquals(2, getQuotaSettingCount(admin));
    admin.setQuota(QuotaSettingsFactory.unthrottleNamespace(NAMESPACES[1]));
    assertEquals(0, getQuotaSettingCount(admin));
  }

  @Test
  public void testRegionServerUnThrottleByType() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();
    final String[] REGIONSERVER = { "RS01", "RS02" };

    admin.setQuota(QuotaSettingsFactory.throttleRegionServer(REGIONSERVER[0],
      ThrottleType.READ_NUMBER, 4, TimeUnit.MINUTES));
    admin.setQuota(QuotaSettingsFactory.throttleRegionServer(REGIONSERVER[0],
      ThrottleType.WRITE_NUMBER, 4, TimeUnit.MINUTES));
    admin.setQuota(QuotaSettingsFactory.throttleRegionServer(REGIONSERVER[1],
      ThrottleType.READ_NUMBER, 4, TimeUnit.MINUTES));
    admin.setQuota(QuotaSettingsFactory.throttleRegionServer(REGIONSERVER[1],
      ThrottleType.WRITE_NUMBER, 4, TimeUnit.MINUTES));

    admin.setQuota(QuotaSettingsFactory.unthrottleRegionServerByThrottleType(REGIONSERVER[0],
      ThrottleType.READ_NUMBER));
    assertEquals(3, getQuotaSettingCount(admin));
    admin.setQuota(QuotaSettingsFactory.unthrottleRegionServerByThrottleType(REGIONSERVER[0],
      ThrottleType.WRITE_NUMBER));
    assertEquals(2, getQuotaSettingCount(admin));
    admin.setQuota(QuotaSettingsFactory.unthrottleRegionServer(REGIONSERVER[1]));
    assertEquals(0, getQuotaSettingCount(admin));
  }

  public int getQuotaSettingCount(Admin admin) throws IOException {
    List<QuotaSettings> list_quotas = admin.getQuota(new QuotaFilter());
    int quotaSettingCount = 0;
    for (QuotaSettings setting : list_quotas) {
      quotaSettingCount++;
      LOG.info("Quota Setting:" + setting);
    }
    return quotaSettingCount;
  }
}
