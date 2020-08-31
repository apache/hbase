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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A test case to verify that region reports are expired when they are not sent.
 */
@Category(LargeTests.class)
public class TestQuotaObserverChoreRegionReports {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestQuotaObserverChoreRegionReports.class);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestQuotaObserverChoreRegionReports.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    // Increase the frequency of some of the chores for responsiveness of the test
    SpaceQuotaHelperForTests.updateConfigForQuotas(conf);
    conf.setInt(QuotaObserverChore.REGION_REPORT_RETENTION_DURATION_KEY, 1000);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testReportExpiration() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    // Send reports every 25 seconds
    conf.setInt(RegionSizeReportingChore.REGION_SIZE_REPORTING_CHORE_PERIOD_KEY, 25000);
    // Expire the reports after 5 seconds
    conf.setInt(QuotaObserverChore.REGION_REPORT_RETENTION_DURATION_KEY, 5000);
    TEST_UTIL.startMiniCluster(1);
    // Wait till quota table onlined.
    TEST_UTIL.waitFor(10000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return TEST_UTIL.getAdmin().tableExists(QuotaTableUtil.QUOTA_TABLE_NAME);
      }
    });

    final String FAM1 = "f1";
    final HMaster master = TEST_UTIL.getMiniHBaseCluster().getMaster();
    // Wait for the master to finish initialization.
    while (master.getMasterQuotaManager() == null) {
      LOG.debug("MasterQuotaManager is null, waiting...");
      Thread.sleep(500);
    }
    final MasterQuotaManager quotaManager = master.getMasterQuotaManager();

    // Create a table
    final TableName tn = TableName.valueOf("reportExpiration");
    TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(tn).addColumnFamily(
        ColumnFamilyDescriptorBuilder.of(FAM1)).build();
    TEST_UTIL.getAdmin().createTable(tableDesc);

    // No reports right after we created this table.
    assertEquals(0, getRegionReportsForTable(quotaManager.snapshotRegionSizes(), tn));

    // Set a quota
    final long sizeLimit = 100L * SpaceQuotaHelperForTests.ONE_MEGABYTE;
    final SpaceViolationPolicy violationPolicy = SpaceViolationPolicy.NO_INSERTS;
    QuotaSettings settings = QuotaSettingsFactory.limitTableSpace(tn, sizeLimit, violationPolicy);
    TEST_UTIL.getAdmin().setQuota(settings);

    // We should get one report for the one region we have.
    Waiter.waitFor(TEST_UTIL.getConfiguration(), 45000, 1000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        int numReports = getRegionReportsForTable(quotaManager.snapshotRegionSizes(), tn);
        LOG.debug("Saw " + numReports + " reports for " + tn + " while waiting for 1");
        return numReports == 1;
      }
    });

    // We should then see no reports for the single region
    Waiter.waitFor(TEST_UTIL.getConfiguration(), 15000, 1000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        int numReports = getRegionReportsForTable(quotaManager.snapshotRegionSizes(), tn);
        LOG.debug("Saw " + numReports + " reports for " + tn + " while waiting for none");
        return numReports == 0;
      }
    });
  }

  @Test
  public void testMissingReportsRemovesQuota() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    // Expire the reports after 5 seconds
    conf.setInt(QuotaObserverChore.REGION_REPORT_RETENTION_DURATION_KEY, 5000);
    TEST_UTIL.startMiniCluster(1);
    // Wait till quota table onlined.
    TEST_UTIL.waitFor(10000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return TEST_UTIL.getAdmin().tableExists(QuotaTableUtil.QUOTA_TABLE_NAME);
      }
    });

    final String FAM1 = "f1";

    // Create a table
    final TableName tn = TableName.valueOf("quotaAcceptanceWithoutReports");
    TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(tn).addColumnFamily(
        ColumnFamilyDescriptorBuilder.of(FAM1)).build();
    TEST_UTIL.getAdmin().createTable(tableDesc);

    // Set a quota
    final long sizeLimit = 1L * SpaceQuotaHelperForTests.ONE_KILOBYTE;
    final SpaceViolationPolicy violationPolicy = SpaceViolationPolicy.NO_INSERTS;
    QuotaSettings settings = QuotaSettingsFactory.limitTableSpace(tn, sizeLimit, violationPolicy);
    final Admin admin = TEST_UTIL.getAdmin();
    LOG.info("SET QUOTA");
    admin.setQuota(settings);
    final Connection conn = TEST_UTIL.getConnection();

    // Write enough data to invalidate the quota
    Put p = new Put(Bytes.toBytes("row1"));
    byte[] bytes = new byte[10];
    Arrays.fill(bytes, (byte) 2);
    for (int i = 0; i < 200; i++) {
      p.addColumn(Bytes.toBytes(FAM1), Bytes.toBytes("qual" + i), bytes);
    }
    conn.getTable(tn).put(p);
    admin.flush(tn);

    // Wait for the table to move into violation
    Waiter.waitFor(TEST_UTIL.getConfiguration(), 30000, 1000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        SpaceQuotaSnapshot snapshot = getSnapshotForTable(conn, tn);
        if (snapshot == null) {
          return false;
        }
        return snapshot.getQuotaStatus().isInViolation();
      }
    });

    // Close the region, prevent the server from sending new status reports.
    List<RegionInfo> regions = admin.getRegions(tn);
    assertEquals(1, regions.size());
    RegionInfo hri = regions.get(0);
    admin.unassign(hri.getRegionName(), true);

    // We should see this table move out of violation after the report expires.
    Waiter.waitFor(TEST_UTIL.getConfiguration(), 30000, 1000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        SpaceQuotaSnapshot snapshot = getSnapshotForTable(conn, tn);
        if (snapshot == null) {
          return false;
        }
        return !snapshot.getQuotaStatus().isInViolation();
      }
    });

    // The QuotaObserverChore's memory should also show it not in violation.
    final HMaster master = TEST_UTIL.getMiniHBaseCluster().getMaster();
    QuotaSnapshotStore<TableName> tableStore =
        master.getQuotaObserverChore().getTableSnapshotStore();
    SpaceQuotaSnapshot snapshot = tableStore.getCurrentState(tn);
    assertFalse("Quota should not be in violation", snapshot.getQuotaStatus().isInViolation());
  }

  private SpaceQuotaSnapshot getSnapshotForTable(
      Connection conn, TableName tn) throws IOException {
    try (Table quotaTable = conn.getTable(QuotaUtil.QUOTA_TABLE_NAME);
        ResultScanner scanner = quotaTable.getScanner(QuotaTableUtil.makeQuotaSnapshotScan())) {
      Map<TableName,SpaceQuotaSnapshot> activeViolations = new HashMap<>();
      for (Result result : scanner) {
        try {
          QuotaTableUtil.extractQuotaSnapshot(result, activeViolations);
        } catch (IllegalArgumentException e) {
          final String msg = "Failed to parse result for row " + Bytes.toString(result.getRow());
          LOG.error(msg, e);
          throw new IOException(msg, e);
        }
      }
      return activeViolations.get(tn);
    }
  }

  private int getRegionReportsForTable(Map<RegionInfo,Long> reports, TableName tn) {
    int numReports = 0;
    for (Entry<RegionInfo,Long> entry : reports.entrySet()) {
      if (tn.equals(entry.getKey().getTable())) {
        numReports++;
      }
    }
    return numReports;
  }
}
