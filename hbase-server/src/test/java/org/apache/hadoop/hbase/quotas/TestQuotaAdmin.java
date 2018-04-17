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
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
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
@Category({ClientTests.class, MediumTests.class})
public class TestQuotaAdmin {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestQuotaAdmin.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestQuotaAdmin.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

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
  public void testSetModifyRemoveQuota() throws Exception {
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
}
