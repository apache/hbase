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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClientServiceCallable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RpcRetryingCaller;
import org.apache.hadoop.hbase.client.RpcRetryingCallerFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.quotas.policies.DefaultViolationPolicyEnforcement;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
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

@Category(MediumTests.class)
public class TestSpaceQuotaOnBulkLoad {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSpaceQuotaOnBulkLoad.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestSpaceQuotaOnBulkLoad.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

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
  public void testNoBulkLoadsWithNoWrites() throws Exception {
    Put p = new Put(Bytes.toBytes("to_reject"));
    p.addColumn(Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("to"),
        Bytes.toBytes("reject"));
    TableName tableName =
        helper.writeUntilViolationAndVerifyViolation(SpaceViolationPolicy.NO_WRITES, p);

    // The table is now in violation. Try to do a bulk load
    ClientServiceCallable<Void> callable = helper.generateFileToLoad(tableName, 1, 50);
    RpcRetryingCallerFactory factory = new RpcRetryingCallerFactory(TEST_UTIL.getConfiguration());
    RpcRetryingCaller<Void> caller = factory.<Void> newCaller();
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
    QuotaSettings settings =
        QuotaSettingsFactory.limitTableSpace(tn, sizeLimit, SpaceViolationPolicy.NO_INSERTS);
    TEST_UTIL.getAdmin().setQuota(settings);

    HRegionServer rs = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);
    RegionServerSpaceQuotaManager spaceQuotaManager = rs.getRegionServerSpaceQuotaManager();
    Map<TableName, SpaceQuotaSnapshot> snapshots = spaceQuotaManager.copyQuotaSnapshots();
    Map<RegionInfo, Long> regionSizes = getReportedSizesForTable(tn);
    while (true) {
      SpaceQuotaSnapshot snapshot = snapshots.get(tn);
      if (snapshot != null && snapshot.getLimit() > 0) {
        break;
      }
      LOG.debug("Snapshot does not yet realize quota limit: " + snapshots + ", regionsizes: "
          + regionSizes);
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
    assertTrue("Expected to find Noop policy, but got " + enforcement.getClass().getSimpleName(),
        enforcement instanceof DefaultViolationPolicyEnforcement);

    // Should generate two files, each of which is over 25KB each
    ClientServiceCallable<Void> callable = helper.generateFileToLoad(tn, 2, 500);
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    FileStatus[] files =
        fs.listStatus(new Path(fs.getHomeDirectory(), testName.getMethodName() + "_files"));
    for (FileStatus file : files) {
      assertTrue(
          "Expected the file, " + file.getPath() + ",  length to be larger than 25KB, but was "
              + file.getLen(), file.getLen() > 25 * SpaceQuotaHelperForTests.ONE_KILOBYTE);
      LOG.debug(file.getPath() + " -> " + file.getLen() + "B");
    }

    RpcRetryingCallerFactory factory = new RpcRetryingCallerFactory(TEST_UTIL.getConfiguration());
    RpcRetryingCaller<Void> caller = factory.<Void>newCaller();
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
    } finally {
      scanner.close();
    }
  }

  private Map<RegionInfo, Long> getReportedSizesForTable(TableName tn) {
    HMaster master = TEST_UTIL.getMiniHBaseCluster().getMaster();
    MasterQuotaManager quotaManager = master.getMasterQuotaManager();
    Map<RegionInfo, Long> filteredRegionSizes = new HashMap<>();
    for (Map.Entry<RegionInfo, Long> entry : quotaManager.snapshotRegionSizes().entrySet()) {
      if (entry.getKey().getTable().equals(tn)) {
        filteredRegionSizes.put(entry.getKey(), entry.getValue());
      }
    }
    return filteredRegionSizes;
  }
}
