/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot.SpaceQuotaStatus;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Test class for the quota status RPCs in the master and regionserver.
 */
@Category({MediumTests.class})
public class TestQuotaStatusRPCs {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final AtomicLong COUNTER = new AtomicLong(0);

  @Rule
  public TestName testName = new TestName();
  private SpaceQuotaHelperForTests helper;

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    // Increase the frequency of some of the chores for responsiveness of the test
    conf.setInt(FileSystemUtilizationChore.FS_UTILIZATION_CHORE_DELAY_KEY, 1000);
    conf.setInt(FileSystemUtilizationChore.FS_UTILIZATION_CHORE_PERIOD_KEY, 1000);
    conf.setInt(QuotaObserverChore.QUOTA_OBSERVER_CHORE_DELAY_KEY, 1000);
    conf.setInt(QuotaObserverChore.QUOTA_OBSERVER_CHORE_PERIOD_KEY, 1000);
    conf.setInt(SpaceQuotaRefresherChore.POLICY_REFRESHER_CHORE_DELAY_KEY, 1000);
    conf.setInt(SpaceQuotaRefresherChore.POLICY_REFRESHER_CHORE_PERIOD_KEY, 1000);
    conf.setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setupForTest() throws Exception {
    helper = new SpaceQuotaHelperForTests(TEST_UTIL, testName, COUNTER);
  }

  @Test
  public void testRegionSizesFromMaster() throws Exception {
    final long tableSize = 1024L * 10L; // 10KB
    final int numRegions = 10;
    final TableName tn = helper.createTableWithRegions(numRegions);
    // Will write at least `tableSize` data
    helper.writeData(tn, tableSize);

    final HMaster master = TEST_UTIL.getMiniHBaseCluster().getMaster();
    final MasterQuotaManager quotaManager = master.getMasterQuotaManager();
    // Make sure the master has all of the reports
    Waiter.waitFor(TEST_UTIL.getConfiguration(), 30 * 1000, new Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return numRegions == countRegionsForTable(tn, quotaManager.snapshotRegionSizes());
      }
    });

    Map<TableName,Long> sizes = QuotaTableUtil.getMasterReportedTableSizes(TEST_UTIL.getConnection());
    Long size = sizes.get(tn);
    assertNotNull("No reported size for " + tn, size);
    assertTrue("Reported table size was " + size, size.longValue() >= tableSize);
  }

  @Test
  public void testQuotaSnapshotsFromRS() throws Exception {
    final long sizeLimit = 1024L * 1024L; // 1MB
    final long tableSize = 1024L * 10L; // 10KB
    final int numRegions = 10;
    final TableName tn = helper.createTableWithRegions(numRegions);

    // Define the quota
    QuotaSettings settings = QuotaSettingsFactory.limitTableSpace(
        tn, sizeLimit, SpaceViolationPolicy.NO_INSERTS);
    TEST_UTIL.getAdmin().setQuota(settings);

    // Write at least `tableSize` data
    helper.writeData(tn, tableSize);

    final HRegionServer rs = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);
    final RegionServerSpaceQuotaManager manager = rs.getRegionServerSpaceQuotaManager();
    Waiter.waitFor(TEST_UTIL.getConfiguration(), 30 * 1000, new Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        SpaceQuotaSnapshot snapshot = manager.copyQuotaSnapshots().get(tn);
        if (null == snapshot) {
          return false;
        }
        return snapshot.getUsage() >= tableSize;
      }
    });

    Map<TableName, SpaceQuotaSnapshot> snapshots = QuotaTableUtil.getRegionServerQuotaSnapshots(
        TEST_UTIL.getConnection(), rs.getServerName());
    SpaceQuotaSnapshot snapshot = snapshots.get(tn);
    assertNotNull("Did not find snapshot for " + tn, snapshot);
    assertTrue(
        "Observed table usage was " + snapshot.getUsage(),
        snapshot.getUsage() >= tableSize);
    assertEquals(snapshot.getLimit(), sizeLimit);
    SpaceQuotaStatus pbStatus = snapshot.getQuotaStatus();
    assertFalse(pbStatus.isInViolation());
  }

  @Test
  public void testQuotaEnforcementsFromRS() throws Exception {
    final long sizeLimit = 1024L * 8L; // 8KB
    final long tableSize = 1024L * 10L; // 10KB
    final int numRegions = 10;
    final TableName tn = helper.createTableWithRegions(numRegions);

    // Define the quota
    QuotaSettings settings = QuotaSettingsFactory.limitTableSpace(
        tn, sizeLimit, SpaceViolationPolicy.NO_INSERTS);
    TEST_UTIL.getAdmin().setQuota(settings);

    // Write at least `tableSize` data
    try {
      helper.writeData(tn, tableSize);
    } catch (SpaceLimitingException e) {
      // Pass
    }

    final HRegionServer rs = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);
    final RegionServerSpaceQuotaManager manager = rs.getRegionServerSpaceQuotaManager();
    Waiter.waitFor(TEST_UTIL.getConfiguration(), 30 * 1000, new Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        ActivePolicyEnforcement enforcements = manager.getActiveEnforcements();
        SpaceViolationPolicyEnforcement enforcement = enforcements.getPolicyEnforcement(tn);
        return enforcement.getQuotaSnapshot().getQuotaStatus().isInViolation();
      }
    });

    Map<TableName,SpaceViolationPolicy> violations =
        QuotaTableUtil.getRegionServerQuotaViolations(
            TEST_UTIL.getConnection(), rs.getServerName());
    SpaceViolationPolicy policy = violations.get(tn);
    assertNotNull("Did not find policy for " + tn, policy);
    assertEquals(SpaceViolationPolicy.NO_INSERTS, policy);
  }

  private int countRegionsForTable(TableName tn, Map<HRegionInfo,Long> regionSizes) {
    int size = 0;
    for (HRegionInfo regionInfo : regionSizes.keySet()) {
      if (tn.equals(regionInfo.getTable())) {
        size++;
      }
    }
    return size;
  }
}
