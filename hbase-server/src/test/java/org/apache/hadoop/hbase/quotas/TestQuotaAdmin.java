/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.hbase.quotas;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Quotas;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;

/**
 * minicluster tests that validate that quota entries are properly set in the quota table
 */
@Category({ MediumTests.class })
public class TestQuotaAdmin {
  private static final Log LOG = LogFactory.getLog(TestQuotaAdmin.class);

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

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testThrottleType() throws Exception {
    Admin admin = TEST_UTIL.getHBaseAdmin();
    String userName = User.getCurrent().getShortName();

    admin.setQuota(QuotaSettingsFactory
      .throttleUser(userName, ThrottleType.READ_NUMBER, 6, TimeUnit.MINUTES));
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
    Admin admin = TEST_UTIL.getHBaseAdmin();
    String userName = User.getCurrent().getShortName();

    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, ThrottleType.REQUEST_NUMBER, 6,
      TimeUnit.MINUTES));
    admin.setQuota(QuotaSettingsFactory.bypassGlobals(userName, true));

    try (QuotaRetriever scanner = QuotaRetriever.open(TEST_UTIL.getConfiguration())) {
      int countThrottle = 0;
      int countGlobalBypass = 0;
      for (QuotaSettings settings : scanner) {
        LOG.debug(settings);
        switch (settings.getQuotaType()) {
        case THROTTLE:
          ThrottleSettings throttle = (ThrottleSettings) settings;
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
  public void testQuotaRetrieverFilter() throws Exception {
    Admin admin = TEST_UTIL.getHBaseAdmin();
    TableName[] tables =
        new TableName[] { TableName.valueOf("T0"), TableName.valueOf("T01"),
            TableName.valueOf("NS0:T2"), };
    String[] namespaces = new String[] { "NS0", "NS01", "NS2" };
    String[] users = new String[] { "User0", "User01", "User2" };

    for (String user : users) {
      admin.setQuota(QuotaSettingsFactory.throttleUser(user, ThrottleType.REQUEST_NUMBER, 1,
        TimeUnit.MINUTES));

      for (TableName table : tables) {
        admin.setQuota(QuotaSettingsFactory.throttleUser(user, table, ThrottleType.REQUEST_NUMBER,
          2, TimeUnit.MINUTES));
      }

      for (String ns : namespaces) {
        admin.setQuota(QuotaSettingsFactory.throttleUser(user, ns, ThrottleType.REQUEST_NUMBER, 3,
          TimeUnit.MINUTES));
      }
    }
    assertNumResults(21, null);

    for (TableName table : tables) {
      admin.setQuota(QuotaSettingsFactory.throttleTable(table, ThrottleType.REQUEST_NUMBER, 4,
        TimeUnit.MINUTES));
    }
    assertNumResults(24, null);

    for (String ns : namespaces) {
      admin.setQuota(QuotaSettingsFactory.throttleNamespace(ns, ThrottleType.REQUEST_NUMBER, 5,
        TimeUnit.MINUTES));
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
    assertNumResults(6, new QuotaFilter().setUserFilter("User.*").setTableFilter("T0")
        .setNamespaceFilter("NS0"));
    assertNumResults(1, new QuotaFilter().setTableFilter("T0"));
    assertNumResults(0, new QuotaFilter().setTableFilter("T"));
    assertNumResults(2, new QuotaFilter().setTableFilter("T.*"));
    assertNumResults(3, new QuotaFilter().setTableFilter(".*T.*"));
    assertNumResults(1, new QuotaFilter().setNamespaceFilter("NS0"));
    assertNumResults(0, new QuotaFilter().setNamespaceFilter("NS"));
    assertNumResults(3, new QuotaFilter().setNamespaceFilter("NS.*"));

    for (String user : users) {
      admin.setQuota(QuotaSettingsFactory.unthrottleUser(user));
      for (TableName table : tables) {
        admin.setQuota(QuotaSettingsFactory.unthrottleUser(user, table));
      }
      for (String ns : namespaces) {
        admin.setQuota(QuotaSettingsFactory.unthrottleUser(user, ns));
      }
    }
    assertNumResults(6, null);

    for (TableName table : tables) {
      admin.setQuota(QuotaSettingsFactory.unthrottleTable(table));
    }
    assertNumResults(3, null);

    for (String ns : namespaces) {
      admin.setQuota(QuotaSettingsFactory.unthrottleNamespace(ns));
    }
    assertNumResults(0, null);
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
    Admin admin = TEST_UTIL.getHBaseAdmin();
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
    Admin admin = TEST_UTIL.getHBaseAdmin();
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

  private void verifyRecordPresentInQuotaTable(ThrottleType type, long limit, TimeUnit tu)
      throws Exception {
    // Verify the RPC Quotas in the table
    try (Table quotaTable = TEST_UTIL.getConnection().getTable(QuotaTableUtil.QUOTA_TABLE_NAME);
        ResultScanner scanner = quotaTable.getScanner(new Scan())) {
      Result r = Iterables.getOnlyElement(scanner);
      CellScanner cells = r.cellScanner();
      assertTrue("Expected to find a cell", cells.advance());
      assertRPCQuota(type, limit, tu, cells.current());
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

  private void assertRPCQuota(ThrottleType type, long limit, TimeUnit tu, Cell cell)
      throws Exception {
    Quotas q = QuotaTableUtil.quotasFromData(cell.getValue());
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

  private int countResults(final QuotaFilter filter) throws Exception {
    QuotaRetriever scanner = QuotaRetriever.open(TEST_UTIL.getConfiguration(), filter);
    try {
      int count = 0;
      for (QuotaSettings settings : scanner) {
        LOG.debug(settings);
        count++;
      }
      return count;
    } finally {
      scanner.close();
    }
  }
}
