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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.ClientServiceCallable;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RpcRetryingCaller;
import org.apache.hadoop.hbase.client.RpcRetryingCallerFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.quotas.policies.DefaultViolationPolicyEnforcement;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * End-to-end test class for filesystem space quotas.
 */
@Category(LargeTests.class)
public class TestSpaceQuotas {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSpaceQuotas.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestSpaceQuotas.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  // Global for all tests in the class
  private static final AtomicLong COUNTER = new AtomicLong(0);
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
    final Connection conn = TEST_UTIL.getConnection();
    if (helper == null) {
      helper = new SpaceQuotaHelperForTests(TEST_UTIL, testName, COUNTER);
    }
    // Wait for the quota table to be created
    if (!conn.getAdmin().tableExists(QuotaUtil.QUOTA_TABLE_NAME)) {
      helper.waitForQuotaTable(conn);
    } else {
      // Or, clean up any quotas from previous test runs.
      helper.removeAllQuotas(conn);
      assertEquals(0, helper.listNumDefinedQuotas(conn));
    }
  }

  @Test
  public void testNoInsertsWithPut() throws Exception {
    Put p = new Put(Bytes.toBytes("to_reject"));
    p.addColumn(
        Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("to"), Bytes.toBytes("reject"));
    writeUntilViolationAndVerifyViolation(SpaceViolationPolicy.NO_INSERTS, p);
  }

  @Test
  public void testNoInsertsWithAppend() throws Exception {
    Append a = new Append(Bytes.toBytes("to_reject"));
    a.addColumn(
        Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("to"), Bytes.toBytes("reject"));
    writeUntilViolationAndVerifyViolation(SpaceViolationPolicy.NO_INSERTS, a);
  }

  @Test
  public void testNoInsertsWithIncrement() throws Exception {
    Increment i = new Increment(Bytes.toBytes("to_reject"));
    i.addColumn(Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("count"), 0);
    writeUntilViolationAndVerifyViolation(SpaceViolationPolicy.NO_INSERTS, i);
  }

  @Test
  public void testDeletesAfterNoInserts() throws Exception {
    final TableName tn = writeUntilViolation(SpaceViolationPolicy.NO_INSERTS);
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
    p.addColumn(
        Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("to"), Bytes.toBytes("reject"));
    writeUntilViolationAndVerifyViolation(SpaceViolationPolicy.NO_WRITES, p);
  }

  @Test
  public void testNoWritesWithAppend() throws Exception {
    Append a = new Append(Bytes.toBytes("to_reject"));
    a.addColumn(
        Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("to"), Bytes.toBytes("reject"));
    writeUntilViolationAndVerifyViolation(SpaceViolationPolicy.NO_WRITES, a);
  }

  @Test
  public void testNoWritesWithIncrement() throws Exception {
    Increment i = new Increment(Bytes.toBytes("to_reject"));
    i.addColumn(Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("count"), 0);
    writeUntilViolationAndVerifyViolation(SpaceViolationPolicy.NO_WRITES, i);
  }

  @Test
  public void testNoWritesWithDelete() throws Exception {
    Delete d = new Delete(Bytes.toBytes("to_reject"));
    writeUntilViolationAndVerifyViolation(SpaceViolationPolicy.NO_WRITES, d);
  }

  @Test
  public void testNoCompactions() throws Exception {
    Put p = new Put(Bytes.toBytes("to_reject"));
    p.addColumn(
        Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("to"), Bytes.toBytes("reject"));
    final TableName tn = writeUntilViolationAndVerifyViolation(
        SpaceViolationPolicy.NO_WRITES_COMPACTIONS, p);
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
    p.addColumn(
        Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("to"), Bytes.toBytes("reject"));
    final TableName tn = writeUntilViolation(SpaceViolationPolicy.DISABLE);
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
      assertTrue("Expected the exception to contain " + expectedText + ", but was: "
          + exceptionContents, exceptionContents.contains(expectedText));
    }
  }

  @Test
  public void testNoBulkLoadsWithNoWrites() throws Exception {
    Put p = new Put(Bytes.toBytes("to_reject"));
    p.addColumn(
        Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("to"), Bytes.toBytes("reject"));
    TableName tableName = writeUntilViolationAndVerifyViolation(SpaceViolationPolicy.NO_WRITES, p);

    // The table is now in violation. Try to do a bulk load
    ClientServiceCallable<Void> callable = helper.generateFileToLoad(tableName, 1, 50);
    RpcRetryingCallerFactory factory = new RpcRetryingCallerFactory(TEST_UTIL.getConfiguration());
    RpcRetryingCaller<Void> caller = factory.newCaller();
    try {
      caller.callWithRetries(callable, Integer.MAX_VALUE);
      fail("Expected the bulk load call to fail!");
    } catch (SpaceLimitingException e) {
      // Pass
      LOG.trace("Caught expected exception", e);
    }
  }

  @Test
  public void testAtomicBulkLoadUnderQuota() throws Exception {
    // Need to verify that if the batch of hfiles cannot be loaded, none are loaded.
    TableName tn = helper.createTableWithRegions(10);

    final long sizeLimit = 50L * SpaceQuotaHelperForTests.ONE_KILOBYTE;
    QuotaSettings settings = QuotaSettingsFactory.limitTableSpace(
        tn, sizeLimit, SpaceViolationPolicy.NO_INSERTS);
    TEST_UTIL.getAdmin().setQuota(settings);

    HRegionServer rs = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);
    RegionServerSpaceQuotaManager spaceQuotaManager = rs.getRegionServerSpaceQuotaManager();
    Map<TableName,SpaceQuotaSnapshot> snapshots = spaceQuotaManager.copyQuotaSnapshots();
    Map<RegionInfo,Long> regionSizes = getReportedSizesForTable(tn);
    while (true) {
      SpaceQuotaSnapshot snapshot = snapshots.get(tn);
      if (snapshot != null && snapshot.getLimit() > 0) {
        break;
      }
      LOG.debug(
          "Snapshot does not yet realize quota limit: " + snapshots + ", regionsizes: " +
          regionSizes);
      Thread.sleep(3000);
      snapshots = spaceQuotaManager.copyQuotaSnapshots();
      regionSizes = getReportedSizesForTable(tn);
    }
    // Our quota limit should be reflected in the latest snapshot
    SpaceQuotaSnapshot snapshot = snapshots.get(tn);
    assertEquals(0L, snapshot.getUsage());
    assertEquals(sizeLimit, snapshot.getLimit());

    // We would also not have a "real" policy in violation
    ActivePolicyEnforcement activePolicies = spaceQuotaManager.getActiveEnforcements();
    SpaceViolationPolicyEnforcement enforcement = activePolicies.getPolicyEnforcement(tn);
    assertTrue(
        "Expected to find Noop policy, but got " + enforcement.getClass().getSimpleName(),
        enforcement instanceof DefaultViolationPolicyEnforcement);

    // Should generate two files, each of which is over 25KB each
    ClientServiceCallable<Void> callable = helper.generateFileToLoad(tn, 2, 525);
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    FileStatus[] files = fs.listStatus(
        new Path(fs.getHomeDirectory(), testName.getMethodName() + "_files"));
    for (FileStatus file : files) {
      assertTrue(
          "Expected the file, " + file.getPath() + ",  length to be larger than 25KB, but was "
              + file.getLen(),
          file.getLen() > 25 * SpaceQuotaHelperForTests.ONE_KILOBYTE);
      LOG.debug(file.getPath() + " -> " + file.getLen() +"B");
    }

    RpcRetryingCallerFactory factory = new RpcRetryingCallerFactory(TEST_UTIL.getConfiguration());
    RpcRetryingCaller<Void> caller = factory.newCaller();
    try {
      caller.callWithRetries(callable, Integer.MAX_VALUE);
      fail("Expected the bulk load call to fail!");
    } catch (SpaceLimitingException e) {
      // Pass
      LOG.trace("Caught expected exception", e);
    }
    // Verify that we have no data in the table because neither file should have been
    // loaded even though one of the files could have.
    Table table = TEST_UTIL.getConnection().getTable(tn);
    ResultScanner scanner = table.getScanner(new Scan());
    try {
      assertNull("Expected no results", scanner.next());
    } finally{
      scanner.close();
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
    TEST_UTIL.getAdmin().setQuota(QuotaSettingsFactory.limitNamespaceSpace(
        tn.getNamespaceAsString(), namespaceLimit, policy));

    // Write more data than should be allowed and flush it to disk
    helper.writeData(tn, 3L * SpaceQuotaHelperForTests.ONE_MEGABYTE);

    // This should be sufficient time for the chores to run and see the change.
    Thread.sleep(5000);

    // The write should be rejected because the table quota takes priority over the namespace
    Put p = new Put(Bytes.toBytes("to_reject"));
    p.addColumn(
        Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("to"), Bytes.toBytes("reject"));
    verifyViolation(policy, tn, p);
  }

  private Map<RegionInfo,Long> getReportedSizesForTable(TableName tn) {
    HMaster master = TEST_UTIL.getMiniHBaseCluster().getMaster();
    MasterQuotaManager quotaManager = master.getMasterQuotaManager();
    Map<RegionInfo,Long> filteredRegionSizes = new HashMap<>();
    for (Entry<RegionInfo,Long> entry : quotaManager.snapshotRegionSizes().entrySet()) {
      if (entry.getKey().getTable().equals(tn)) {
        filteredRegionSizes.put(entry.getKey(), entry.getValue());
      }
    }
    return filteredRegionSizes;
  }

  private TableName writeUntilViolation(SpaceViolationPolicy policyToViolate) throws Exception {
    TableName tn = helper.createTableWithRegions(10);

    final long sizeLimit = 2L * SpaceQuotaHelperForTests.ONE_MEGABYTE;
    QuotaSettings settings = QuotaSettingsFactory.limitTableSpace(tn, sizeLimit, policyToViolate);
    TEST_UTIL.getAdmin().setQuota(settings);

    // Write more data than should be allowed and flush it to disk
    helper.writeData(tn, 3L * SpaceQuotaHelperForTests.ONE_MEGABYTE);

    // This should be sufficient time for the chores to run and see the change.
    Thread.sleep(5000);

    return tn;
  }

  private TableName writeUntilViolationAndVerifyViolation(
      SpaceViolationPolicy policyToViolate, Mutation m) throws Exception {
    final TableName tn = writeUntilViolation(policyToViolate);
    verifyViolation(policyToViolate, tn, m);
    return tn;
  }

  private void verifyViolation(
      SpaceViolationPolicy policyToViolate, TableName tn, Mutation m) throws Exception {
    // But let's try a few times to get the exception before failing
    boolean sawError = false;
    for (int i = 0; i < NUM_RETRIES && !sawError; i++) {
      try (Table table = TEST_UTIL.getConnection().getTable(tn)) {
        if (m instanceof Put) {
          table.put((Put) m);
        } else if (m instanceof Delete) {
          table.delete((Delete) m);
        } else if (m instanceof Append) {
          table.append((Append) m);
        } else if (m instanceof Increment) {
          table.increment((Increment) m);
        } else {
          fail(
              "Failed to apply " + m.getClass().getSimpleName() +
              " to the table. Programming error");
        }
        LOG.info("Did not reject the " + m.getClass().getSimpleName() + ", will sleep and retry");
        Thread.sleep(2000);
      } catch (Exception e) {
        String msg = StringUtils.stringifyException(e);
        assertTrue("Expected exception message to contain the word '" + policyToViolate.name() +
            "', but was " + msg, msg.contains(policyToViolate.name()));
        sawError = true;
      }
    }
    if (!sawError) {
      try (Table quotaTable = TEST_UTIL.getConnection().getTable(QuotaUtil.QUOTA_TABLE_NAME)) {
        ResultScanner scanner = quotaTable.getScanner(new Scan());
        Result result = null;
        LOG.info("Dumping contents of hbase:quota table");
        while ((result = scanner.next()) != null) {
          LOG.info(Bytes.toString(result.getRow()) + " => " + result.toString());
        }
        scanner.close();
      }
    }
    assertTrue(
        "Expected to see an exception writing data to a table exceeding its quota", sawError);
  }
}
