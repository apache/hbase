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
package org.apache.hadoop.hbase.master.normalizer;

import static org.hamcrest.Matchers.comparesEqualTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MatcherPredicate;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Size;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.NormalizeTableFilterParams;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.TableNamespaceManager;
import org.apache.hadoop.hbase.master.normalizer.NormalizationPlan.PlanType;
import org.apache.hadoop.hbase.namespace.TestNamespaceAuditor;
import org.apache.hadoop.hbase.quotas.QuotaUtil;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.LoadTestKVGenerator;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
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
 * Testing {@link SimpleRegionNormalizer} on minicluster.
 */
@Category({MasterTests.class, MediumTests.class})
public class TestSimpleRegionNormalizerOnCluster {
  private static final Logger LOG =
    LoggerFactory.getLogger(TestSimpleRegionNormalizerOnCluster.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSimpleRegionNormalizerOnCluster.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] FAMILY_NAME = Bytes.toBytes("fam");

  private static Admin admin;
  private static HMaster master;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    // we will retry operations when PleaseHoldException is thrown
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 3);
    TEST_UTIL.getConfiguration().setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);

    // no way for the test to set the regionId on a created region, so disable this feature.
    TEST_UTIL.getConfiguration().setInt("hbase.normalizer.merge.min_region_age.days", 0);

    // disable the normalizer coming along and running via Chore
    TEST_UTIL.getConfiguration().setInt("hbase.normalizer.period", Integer.MAX_VALUE);

    TEST_UTIL.startMiniCluster(1);
    TestNamespaceAuditor.waitForQuotaInitialize(TEST_UTIL);
    admin = TEST_UTIL.getAdmin();
    master = TEST_UTIL.getHBaseCluster().getMaster();
    assertNotNull(master);
  }

  @AfterClass
  public static void afterAllTests() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void before() throws Exception {
    // disable the normalizer ahead of time, let the test enable it when its ready.
    admin.normalizerSwitch(false);
  }

  @Test
  public void testHonorsNormalizerSwitch() throws Exception {
    assertFalse(admin.isNormalizerEnabled());
    assertFalse(admin.normalize());
    assertFalse(admin.normalizerSwitch(true));
    assertTrue(admin.normalize());
  }

  /**
   * Test that disabling normalizer via table configuration is honored. There's
   * no side-effect to look for (other than a log message), so normalize two
   * tables, one with the disabled setting, and look for change in one and no
   * change in the other.
   */
  @Test
  public void testHonorsNormalizerTableSetting() throws Exception {
    final TableName tn1 = TableName.valueOf(name.getMethodName() + "1");
    final TableName tn2 = TableName.valueOf(name.getMethodName() + "2");
    final TableName tn3 = TableName.valueOf(name.getMethodName() + "3");

    try {
      final int tn1RegionCount = createTableBegsSplit(tn1, true, false);
      final int tn2RegionCount = createTableBegsSplit(tn2, false, false);
      final int tn3RegionCount = createTableBegsSplit(tn3, true, true);

      assertFalse(admin.normalizerSwitch(true));
      assertTrue(admin.normalize());
      waitForTableRegionCount(tn1, greaterThanOrEqualTo(tn1RegionCount + 1));

      // confirm that tn1 has (tn1RegionCount + 1) number of regions.
      // tn2 has tn2RegionCount number of regions because normalizer has not been enabled on it.
      // tn3 has tn3RegionCount number of regions because two plans are run:
      //    1. split one region to two
      //    2. merge two regions into one
      // and hence, total number of regions for tn3 remains same
      assertEquals(
        tn1 + " should have split.",
        tn1RegionCount + 1,
        getRegionCount(tn1));
      assertEquals(
        tn2 + " should not have split.",
        tn2RegionCount,
        getRegionCount(tn2));
      LOG.debug("waiting for t3 to settle...");
      waitForTableRegionCount(tn3, comparesEqualTo(tn3RegionCount));
    } finally {
      dropIfExists(tn1);
      dropIfExists(tn2);
      dropIfExists(tn3);
    }
  }

  @Test
  public void testRegionNormalizationSplitWithoutQuotaLimit() throws Exception {
    testRegionNormalizationSplit(false);
  }

  @Test
    public void testRegionNormalizationSplitWithQuotaLimit() throws Exception {
    testRegionNormalizationSplit(true);
  }

  void testRegionNormalizationSplit(boolean limitedByQuota) throws Exception {
    TableName tableName = null;
    try {
      tableName = limitedByQuota
        ? buildTableNameForQuotaTest(name.getMethodName())
        : TableName.valueOf(name.getMethodName());

      final int currentRegionCount = createTableBegsSplit(tableName, true, false);
      final long existingSkippedSplitCount = master.getRegionNormalizerManager()
        .getSkippedCount(PlanType.SPLIT);
      assertFalse(admin.normalizerSwitch(true));
      assertTrue(admin.normalize());
      if (limitedByQuota) {
        waitForSkippedSplits(master, existingSkippedSplitCount);
        assertEquals(
          tableName + " should not have split.",
          currentRegionCount,
          getRegionCount(tableName));
      } else {
        waitForTableRegionCount(tableName, greaterThanOrEqualTo(currentRegionCount + 1));
        assertEquals(
          tableName + " should have split.",
          currentRegionCount + 1,
          getRegionCount(tableName));
      }
    } finally {
      dropIfExists(tableName);
    }
  }

  @Test
  public void testRegionNormalizationMerge() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    try {
      final int currentRegionCount = createTableBegsMerge(tableName);
      assertFalse(admin.normalizerSwitch(true));
      assertTrue(admin.normalize());
      waitForTableRegionCount(tableName, lessThanOrEqualTo(currentRegionCount - 1));
      assertEquals(
        tableName + " should have merged.",
        currentRegionCount - 1,
        getRegionCount(tableName));
    } finally {
      dropIfExists(tableName);
    }
  }

  @Test
  public void testHonorsNamespaceFilter() throws Exception {
    final NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create("ns").build();
    final TableName tn1 = TableName.valueOf("ns", name.getMethodName());
    final TableName tn2 = TableName.valueOf(name.getMethodName());

    try {
      admin.createNamespace(namespaceDescriptor);
      final int tn1RegionCount = createTableBegsSplit(tn1, true, false);
      final int tn2RegionCount = createTableBegsSplit(tn2, true, false);
      final NormalizeTableFilterParams ntfp = new NormalizeTableFilterParams.Builder()
        .namespace("ns")
        .build();

      assertFalse(admin.normalizerSwitch(true));
      assertTrue(admin.normalize(ntfp));
      waitForTableRegionCount(tn1, greaterThanOrEqualTo(tn1RegionCount + 1));

      // confirm that tn1 has (tn1RegionCount + 1) number of regions.
      // tn2 has tn2RegionCount number of regions because it's not a member of the target namespace.
      assertEquals(
        tn1 + " should have split.",
        tn1RegionCount + 1,
        getRegionCount(tn1));
      waitForTableRegionCount(tn2, comparesEqualTo(tn2RegionCount));
    } finally {
      dropIfExists(tn1);
      dropIfExists(tn2);
    }
  }

  @Test
  public void testHonorsPatternFilter() throws Exception {
    final TableName tn1 = TableName.valueOf(name.getMethodName() + "1");
    final TableName tn2 = TableName.valueOf(name.getMethodName() + "2");

    try {
      final int tn1RegionCount = createTableBegsSplit(tn1, true, false);
      final int tn2RegionCount = createTableBegsSplit(tn2, true, false);
      final NormalizeTableFilterParams ntfp = new NormalizeTableFilterParams.Builder()
        .regex(".*[1]")
        .build();

      assertFalse(admin.normalizerSwitch(true));
      assertTrue(admin.normalize(ntfp));
      waitForTableRegionCount(tn1, greaterThanOrEqualTo(tn1RegionCount + 1));

      // confirm that tn1 has (tn1RegionCount + 1) number of regions.
      // tn2 has tn2RegionCount number of regions because it fails filter.
      assertEquals(
        tn1 + " should have split.",
        tn1RegionCount + 1,
        getRegionCount(tn1));
      waitForTableRegionCount(tn2, comparesEqualTo(tn2RegionCount));
    } finally {
      dropIfExists(tn1);
      dropIfExists(tn2);
    }
  }

  @Test
  public void testHonorsNameFilter() throws Exception {
    final TableName tn1 = TableName.valueOf(name.getMethodName() + "1");
    final TableName tn2 = TableName.valueOf(name.getMethodName() + "2");

    try {
      final int tn1RegionCount = createTableBegsSplit(tn1, true, false);
      final int tn2RegionCount = createTableBegsSplit(tn2, true, false);
      final NormalizeTableFilterParams ntfp = new NormalizeTableFilterParams.Builder()
        .tableNames(Collections.singletonList(tn1))
        .build();

      assertFalse(admin.normalizerSwitch(true));
      assertTrue(admin.normalize(ntfp));
      waitForTableRegionCount(tn1, greaterThanOrEqualTo(tn1RegionCount + 1));

      // confirm that tn1 has (tn1RegionCount + 1) number of regions.
      // tn2 has tn3RegionCount number of regions because it fails filter:
      assertEquals(
        tn1 + " should have split.",
        tn1RegionCount + 1,
        getRegionCount(tn1));
      waitForTableRegionCount(tn2, comparesEqualTo(tn2RegionCount));
    } finally {
      dropIfExists(tn1);
      dropIfExists(tn2);
    }
  }

  /**
   * A test for when a region is the target of both a split and a merge plan. Does not define
   * expected behavior, only that some change is applied to the table.
   */
  @Test
  public void testTargetOfSplitAndMerge() throws Exception {
    final TableName tn = TableName.valueOf(name.getMethodName());
    try {
      final int tnRegionCount = createTableTargetOfSplitAndMerge(tn);
      assertFalse(admin.normalizerSwitch(true));
      assertTrue(admin.normalize());
      TEST_UTIL.waitFor(TimeUnit.MINUTES.toMillis(5), new MatcherPredicate<>(
        "expected " + tn + " to split or merge (probably split)",
        () -> getRegionCountUnchecked(tn),
        not(comparesEqualTo(tnRegionCount))));
    } finally {
      dropIfExists(tn);
    }
  }

  private static TableName buildTableNameForQuotaTest(final String methodName) throws Exception {
    String nsp = "np2";
    NamespaceDescriptor nspDesc =
      NamespaceDescriptor.create(nsp)
        .addConfiguration(TableNamespaceManager.KEY_MAX_REGIONS, "5")
        .addConfiguration(TableNamespaceManager.KEY_MAX_TABLES, "2").build();
    admin.createNamespace(nspDesc);
    return TableName.valueOf(nsp, methodName);
  }

  private static void waitForSkippedSplits(final HMaster master,
    final long existingSkippedSplitCount) {
    TEST_UTIL.waitFor(TimeUnit.MINUTES.toMillis(5), new MatcherPredicate<>(
      "waiting to observe split attempt and skipped.",
      () -> master.getRegionNormalizerManager().getSkippedCount(PlanType.SPLIT),
      Matchers.greaterThan(existingSkippedSplitCount)));
  }

  private static void waitForTableRegionCount(final TableName tableName,
    Matcher<? super Integer> matcher) {
    TEST_UTIL.waitFor(TimeUnit.MINUTES.toMillis(5), new MatcherPredicate<>(
      "region count for table " + tableName + " does not match expected",
      () -> getRegionCountUnchecked(tableName),
      matcher));
  }

  private static List<HRegion> generateTestData(final TableName tableName,
    final int... regionSizesMb) throws IOException {
    final List<HRegion> generatedRegions;
    final int numRegions = regionSizesMb.length;
    LOG.debug("generating test data into {}, {} regions of sizes (mb) {}", tableName, numRegions,
      regionSizesMb);
    try (Table ignored = TEST_UTIL.createMultiRegionTable(tableName, FAMILY_NAME, numRegions)) {
      // Need to get sorted list of regions here
      generatedRegions = new ArrayList<>(TEST_UTIL.getHBaseCluster().getRegions(tableName));
      generatedRegions.sort(Comparator.comparing(HRegion::getRegionInfo, RegionInfo.COMPARATOR));
      assertEquals(numRegions, generatedRegions.size());
      for (int i = 0; i < numRegions; i++) {
        HRegion region = generatedRegions.get(i);
        generateTestData(region, regionSizesMb[i]);
        region.flush(true);
      }
    }
    return generatedRegions;
  }

  private static void generateTestData(Region region, int numRows) throws IOException {
    // generating 1Mb values
    LOG.debug("writing {}mb to {}", numRows, region);
    LoadTestKVGenerator dataGenerator = new LoadTestKVGenerator(1024 * 1024, 1024 * 1024);
    for (int i = 0; i < numRows; ++i) {
      byte[] key = Bytes.add(region.getRegionInfo().getStartKey(), Bytes.toBytes(i));
      for (int j = 0; j < 1; ++j) {
        Put put = new Put(key);
        byte[] col = Bytes.toBytes(String.valueOf(j));
        byte[] value = dataGenerator.generateRandomSizeValue(key, col);
        put.addColumn(FAMILY_NAME, col, value);
        region.put(put);
      }
    }
  }

  private static double getRegionSizeMB(final MasterServices masterServices,
    final RegionInfo regionInfo) {
    final ServerName sn = masterServices.getAssignmentManager()
      .getRegionStates()
      .getRegionServerOfRegion(regionInfo);
    final RegionMetrics regionLoad = masterServices.getServerManager()
      .getLoad(sn)
      .getRegionMetrics()
      .get(regionInfo.getRegionName());
    if (regionLoad == null) {
      LOG.debug("{} was not found in RegionsLoad", regionInfo.getRegionNameAsString());
      return -1;
    }
    return regionLoad.getStoreFileSize().get(Size.Unit.MEGABYTE);
  }

  /**
   * create a table with 5 regions, having region sizes so as to provoke a split
   * of the largest region.
   * <ul>
   *   <li>total table size: 12</li>
   *   <li>average region size: 2.4</li>
   *   <li>split threshold: 2.4 * 2 = 4.8</li>
   * </ul>
   */
  private static int createTableBegsSplit(final TableName tableName,
      final boolean normalizerEnabled, final boolean isMergeEnabled)
    throws Exception {
    final List<HRegion> generatedRegions = generateTestData(tableName, 1, 1, 2, 3, 5);
    assertEquals(5, getRegionCount(tableName));
    admin.flush(tableName);

    final TableDescriptor td = TableDescriptorBuilder
      .newBuilder(admin.getDescriptor(tableName))
      .setNormalizationEnabled(normalizerEnabled)
      .setMergeEnabled(isMergeEnabled)
      .build();
    admin.modifyTable(td);

    // make sure relatively accurate region statistics are available for the test table. use
    // the last/largest region as clue.
    TEST_UTIL.waitFor(TimeUnit.MINUTES.toMillis(1), new ExplainingPredicate<IOException>() {
      @Override public String explainFailure() {
        return "expected largest region to be >= 4mb.";
      }
      @Override public boolean evaluate() {
        return generatedRegions.stream()
          .mapToDouble(val -> getRegionSizeMB(master, val.getRegionInfo()))
          .allMatch(val -> val > 0)
          && getRegionSizeMB(master, generatedRegions.get(4).getRegionInfo()) >= 4.0;
      }
    });
    return 5;
  }

  /**
   * create a table with 5 regions, having region sizes so as to provoke a merge
   * of the smallest regions.
   * <ul>
   *   <li>total table size: 13</li>
   *   <li>average region size: 2.6</li>
   *   <li>sum of sizes of first two regions < average</li>
   * </ul>
   */
  private static int createTableBegsMerge(final TableName tableName) throws Exception {
    // create 5 regions with sizes to trigger merge of small regions
    final List<HRegion> generatedRegions = generateTestData(tableName, 1, 1, 3, 3, 5);
    assertEquals(5, getRegionCount(tableName));
    admin.flush(tableName);

    final TableDescriptor td = TableDescriptorBuilder
      .newBuilder(admin.getDescriptor(tableName))
      .setNormalizationEnabled(true)
      .build();
    admin.modifyTable(td);

    // make sure relatively accurate region statistics are available for the test table. use
    // the last/largest region as clue.
    LOG.debug("waiting for region statistics to settle.");
    TEST_UTIL.waitFor(TimeUnit.MINUTES.toMillis(1), new ExplainingPredicate<IOException>() {
      @Override public String explainFailure() {
        return "expected largest region to be >= 4mb.";
      }
      @Override public boolean evaluate() {
        return generatedRegions.stream()
          .mapToDouble(val -> getRegionSizeMB(master, val.getRegionInfo()))
          .allMatch(val -> val > 0)
          && getRegionSizeMB(master, generatedRegions.get(4).getRegionInfo()) >= 4.0;
      }
    });
    return 5;
  }

  /**
   * Create a table with 4 regions, having region sizes so as to provoke a split of the largest
   * region and a merge of an empty region into the largest.
   * <ul>
   *   <li>total table size: 14</li>
   *   <li>average region size: 3.5</li>
   * </ul>
   */
  private static int createTableTargetOfSplitAndMerge(final TableName tableName) throws Exception {
    final int[] regionSizesMb = { 10, 0, 2, 2 };
    final List<HRegion> generatedRegions = generateTestData(tableName, regionSizesMb);
    assertEquals(4, getRegionCount(tableName));
    admin.flush(tableName);

    final TableDescriptor td = TableDescriptorBuilder
      .newBuilder(admin.getDescriptor(tableName))
      .setNormalizationEnabled(true)
      .build();
    admin.modifyTable(td);

    // make sure relatively accurate region statistics are available for the test table. use
    // the last/largest region as clue.
    LOG.debug("waiting for region statistics to settle.");
    TEST_UTIL.waitFor(TimeUnit.MINUTES.toMillis(5), new ExplainingPredicate<IOException>() {
      @Override public String explainFailure() {
        return "expected largest region to be >= 10mb.";
      }
      @Override public boolean evaluate() {
        for (int i = 0; i < generatedRegions.size(); i++) {
          final RegionInfo regionInfo = generatedRegions.get(i).getRegionInfo();
          if (!(getRegionSizeMB(master, regionInfo) >= regionSizesMb[i])) {
            return false;
          }
        }
        return true;
      }
    });
    return 4;
  }

  private static void dropIfExists(final TableName tableName) throws Exception {
    if (tableName != null && admin.tableExists(tableName)) {
      if (admin.isTableEnabled(tableName)) {
        admin.disableTable(tableName);
      }
      admin.deleteTable(tableName);
    }
  }

  private static int getRegionCount(TableName tableName) throws IOException {
    try (RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
      return locator.getAllRegionLocations().size();
    }
  }

  private static int getRegionCountUnchecked(final TableName tableName) {
    try {
      return getRegionCount(tableName);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
