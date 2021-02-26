/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.quotas;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(LargeTests.class)
public class TestSpaceQuotaBasicFunctioning {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSpaceQuotaBasicFunctioning.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestSpaceQuotaBasicFunctioning.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final int NUM_RETRIES = 10;

  @Rule
  public TestName testName = new TestName();
  private SpaceQuotaHelperForTests helper;

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    SpaceQuotaHelperForTests.updateConfigForQuotas(conf);
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void removeAllQuotas() throws Exception {
    helper = new SpaceQuotaHelperForTests(TEST_UTIL, testName, new AtomicLong(0));
    helper.removeAllQuotas();
  }

  @Test
  public void testNoInsertsWithPut() throws Exception {
    Put p = new Put(Bytes.toBytes("to_reject"));
    p.addColumn(Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("to"),
        Bytes.toBytes("reject"));
    helper.writeUntilViolationAndVerifyViolation(SpaceViolationPolicy.NO_INSERTS, p);
  }

  @Test
  public void testNoInsertsWithAppend() throws Exception {
    Append a = new Append(Bytes.toBytes("to_reject"));
    a.addColumn(Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("to"),
        Bytes.toBytes("reject"));
    helper.writeUntilViolationAndVerifyViolation(SpaceViolationPolicy.NO_INSERTS, a);
  }

  @Test
  public void testNoInsertsWithIncrement() throws Exception {
    Increment i = new Increment(Bytes.toBytes("to_reject"));
    i.addColumn(Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("count"), 0);
    helper.writeUntilViolationAndVerifyViolation(SpaceViolationPolicy.NO_INSERTS, i);
  }

  @Test
  public void testDeletesAfterNoInserts() throws Exception {
    final TableName tn = helper.writeUntilViolation(SpaceViolationPolicy.NO_INSERTS);
    // Try a couple of times to verify that the quota never gets enforced, same as we
    // do when we're trying to catch the failure.
    Delete d = new Delete(Bytes.toBytes("should_not_be_rejected"));
    for (int i = 0; i < NUM_RETRIES; i++) {
      try (Table t = TEST_UTIL.getConnection().getTable(tn)) {
        t.delete(d);
      }
    }
  }

  @Test
  public void testNoWritesWithPut() throws Exception {
    Put p = new Put(Bytes.toBytes("to_reject"));
    p.addColumn(Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("to"),
        Bytes.toBytes("reject"));
    helper.writeUntilViolationAndVerifyViolation(SpaceViolationPolicy.NO_WRITES, p);
  }

  @Test
  public void testNoWritesWithAppend() throws Exception {
    Append a = new Append(Bytes.toBytes("to_reject"));
    a.addColumn(Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("to"),
        Bytes.toBytes("reject"));
    helper.writeUntilViolationAndVerifyViolation(SpaceViolationPolicy.NO_WRITES, a);
  }

  @Test
  public void testNoWritesWithIncrement() throws Exception {
    Increment i = new Increment(Bytes.toBytes("to_reject"));
    i.addColumn(Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("count"), 0);
    helper.writeUntilViolationAndVerifyViolation(SpaceViolationPolicy.NO_WRITES, i);
  }

  @Test
  public void testNoWritesWithDelete() throws Exception {
    Delete d = new Delete(Bytes.toBytes("to_reject"));
    helper.writeUntilViolationAndVerifyViolation(SpaceViolationPolicy.NO_WRITES, d);
  }

  @Test
  public void testNoCompactions() throws Exception {
    Put p = new Put(Bytes.toBytes("to_reject"));
    p.addColumn(Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("to"),
        Bytes.toBytes("reject"));
    final TableName tn =
        helper.writeUntilViolationAndVerifyViolation(SpaceViolationPolicy.NO_WRITES_COMPACTIONS, p);
    // We know the policy is active at this point

    // Major compactions should be rejected
    try {
      TEST_UTIL.getAdmin().majorCompact(tn);
      fail("Expected that invoking the compaction should throw an Exception");
    } catch (DoNotRetryIOException e) {
      // Expected!
    }
    // Minor compactions should also be rejected.
    try {
      TEST_UTIL.getAdmin().compact(tn);
      fail("Expected that invoking the compaction should throw an Exception");
    } catch (DoNotRetryIOException e) {
      // Expected!
    }
  }

  @Test
  public void testNoEnableAfterDisablePolicy() throws Exception {
    Put p = new Put(Bytes.toBytes("to_reject"));
    p.addColumn(Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("to"),
        Bytes.toBytes("reject"));
    final TableName tn = helper.writeUntilViolation(SpaceViolationPolicy.DISABLE);
    final Admin admin = TEST_UTIL.getAdmin();
    // Disabling a table relies on some external action (over the other policies), so wait a bit
    // more than the other tests.
    for (int i = 0; i < NUM_RETRIES * 2; i++) {
      if (admin.isTableEnabled(tn)) {
        LOG.info(tn + " is still enabled, expecting it to be disabled. Will wait and re-check.");
        Thread.sleep(2000);
      }
    }
    assertFalse(tn + " is still enabled but it should be disabled", admin.isTableEnabled(tn));
    try {
      admin.enableTable(tn);
    } catch (AccessDeniedException e) {
      String exceptionContents = StringUtils.stringifyException(e);
      final String expectedText = "violated space quota";
      assertTrue(
          "Expected the exception to contain " + expectedText + ", but was: " + exceptionContents,
          exceptionContents.contains(expectedText));
    }
  }

  @Test
  public void testTableQuotaOverridesNamespaceQuota() throws Exception {
    final SpaceViolationPolicy policy = SpaceViolationPolicy.NO_INSERTS;
    final TableName tn = helper.createTableWithRegions(10);

    // 2MB limit on the table, 1GB limit on the namespace
    final long tableLimit = 2L * SpaceQuotaHelperForTests.ONE_MEGABYTE;
    final long namespaceLimit = 1024L * SpaceQuotaHelperForTests.ONE_MEGABYTE;
    TEST_UTIL.getAdmin().setQuota(QuotaSettingsFactory.limitTableSpace(tn, tableLimit, policy));
    TEST_UTIL.getAdmin().setQuota(QuotaSettingsFactory
        .limitNamespaceSpace(tn.getNamespaceAsString(), namespaceLimit, policy));

    // Write more data than should be allowed and flush it to disk
    helper.writeData(tn, 3L * SpaceQuotaHelperForTests.ONE_MEGABYTE);

    // This should be sufficient time for the chores to run and see the change.
    Thread.sleep(5000);

    // The write should be rejected because the table quota takes priority over the namespace
    Put p = new Put(Bytes.toBytes("to_reject"));
    p.addColumn(Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("to"),
        Bytes.toBytes("reject"));
    helper.verifyViolation(policy, tn, p);
  }

  @Test
  public void testDisablePolicyQuotaAndViolate() throws Exception {
    TableName tableName = helper.createTable();
    helper.setQuotaLimit(tableName, SpaceViolationPolicy.DISABLE, 1L);
    helper.writeData(tableName, SpaceQuotaHelperForTests.ONE_MEGABYTE * 2L);
    TEST_UTIL.getConfiguration()
        .setLong("hbase.master.quotas.region.report.retention.millis", 100);

    HMaster master = TEST_UTIL.getMiniHBaseCluster().getMaster();
    MasterQuotaManager quotaManager = master.getMasterQuotaManager();

    // Make sure the master has report for the table.
    Waiter.waitFor(TEST_UTIL.getConfiguration(), 30 * 1000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        Map<RegionInfo, Long> regionSizes = quotaManager.snapshotRegionSizes();
        List<RegionInfo> tableRegions =
            MetaTableAccessor.getTableRegions(TEST_UTIL.getConnection(), tableName);
        return regionSizes.containsKey(tableRegions.get(0));
      }
    });

    // Check if disabled table region report present in the map after retention period expired.
    // It should be present after retention period expired.
    final long regionSizes = quotaManager.snapshotRegionSizes().keySet().stream()
        .filter(k -> k.getTable().equals(tableName)).count();
    Assert.assertTrue(regionSizes > 0);
  }
}
