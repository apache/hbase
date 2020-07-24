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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Size;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
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
  public void before() throws IOException {
    // disable the normalizer ahead of time, let the test enable it when its ready.
    admin.normalizerSwitch(false);
  }

  @Test
  public void testHonorsNormalizerSwitch() throws IOException {
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
      waitForTableSplit(tn1, tn1RegionCount + 1);

      // confirm that tn1 has (tn1RegionCount + 1) number of regions.
      // tn2 has tn2RegionCount number of regions because normalizer has not been enabled on it.
      // tn3 has tn3RegionCount number of regions because two plans are run:
      //    1. split one region to two
      //    2. merge two regions into one
      // and hence, total number of regions for tn3 remains same
      assertEquals(
        tn1 + " should have split.",
        tn1RegionCount + 1,
        MetaTableAccessor.getRegionCount(TEST_UTIL.getConnection(), tn1));
      assertEquals(
        tn2 + " should not have split.",
        tn2RegionCount,
        MetaTableAccessor.getRegionCount(TEST_UTIL.getConnection(), tn2));
      waitForTableRegionCount(tn3, tn3RegionCount);
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
      final long existingSkippedSplitCount = master.getRegionNormalizer()
        .getSkippedCount(PlanType.SPLIT);
      assertFalse(admin.normalizerSwitch(true));
      assertTrue(admin.normalize());
      if (limitedByQuota) {
        waitForSkippedSplits(master, existingSkippedSplitCount);
        assertEquals(
          tableName + " should not have split.",
          currentRegionCount,
          MetaTableAccessor.getRegionCount(TEST_UTIL.getConnection(), tableName));
      } else {
        waitForTableSplit(tableName, currentRegionCount + 1);
        assertEquals(
          tableName + " should have split.",
          currentRegionCount + 1,
          MetaTableAccessor.getRegionCount(TEST_UTIL.getConnection(), tableName));
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
      waitForTableMerge(tableName, currentRegionCount - 1);
      assertEquals(
        tableName + " should have merged.",
        currentRegionCount - 1,
        MetaTableAccessor.getRegionCount(TEST_UTIL.getConnection(), tableName));
    } finally {
      dropIfExists(tableName);
    }
  }

  private static TableName buildTableNameForQuotaTest(final String methodName) throws IOException {
    String nsp = "np2";
    NamespaceDescriptor nspDesc =
      NamespaceDescriptor.create(nsp)
        .addConfiguration(TableNamespaceManager.KEY_MAX_REGIONS, "5")
        .addConfiguration(TableNamespaceManager.KEY_MAX_TABLES, "2").build();
    admin.createNamespace(nspDesc);
    return TableName.valueOf(nsp + TableName.NAMESPACE_DELIM + methodName);
  }

  private static void waitForSkippedSplits(final HMaster master,
    final long existingSkippedSplitCount) throws Exception {
    TEST_UTIL.waitFor(TimeUnit.MINUTES.toMillis(5), new ExplainingPredicate<Exception>() {
      @Override public String explainFailure() {
        return "waiting to observe split attempt and skipped.";
      }
      @Override public boolean evaluate() {
        final long skippedSplitCount = master.getRegionNormalizer().getSkippedCount(PlanType.SPLIT);
        return skippedSplitCount > existingSkippedSplitCount;
      }
    });
  }

  private static void waitForTableRegionCount(final TableName tableName,
      final int targetRegionCount) throws IOException {
    TEST_UTIL.waitFor(TimeUnit.MINUTES.toMillis(5), new ExplainingPredicate<IOException>() {
      @Override
      public String explainFailure() {
        return "expected " + targetRegionCount + " number of regions for table " + tableName;
      }
      @Override
      public boolean evaluate() throws IOException {
        final int currentRegionCount =
          MetaTableAccessor.getRegionCount(TEST_UTIL.getConnection(), tableName);
        return currentRegionCount == targetRegionCount;
      }
    });
  }

  private static void waitForTableSplit(final TableName tableName, final int targetRegionCount)
      throws IOException {
    TEST_UTIL.waitFor(TimeUnit.MINUTES.toMillis(5), new ExplainingPredicate<IOException>() {
      @Override public String explainFailure() {
        return "expected normalizer to split region.";
      }
      @Override public boolean evaluate() throws IOException {
        final int currentRegionCount =
          MetaTableAccessor.getRegionCount(TEST_UTIL.getConnection(), tableName);
        return currentRegionCount >= targetRegionCount;
      }
    });
  }

  private static void waitForTableMerge(final TableName tableName, final int targetRegionCount)
      throws IOException {
    TEST_UTIL.waitFor(TimeUnit.MINUTES.toMillis(5), new ExplainingPredicate<IOException>() {
      @Override public String explainFailure() {
        return "expected normalizer to merge regions.";
      }
      @Override public boolean evaluate() throws IOException {
        final int currentRegionCount =
          MetaTableAccessor.getRegionCount(TEST_UTIL.getConnection(), tableName);
        return currentRegionCount <= targetRegionCount;
      }
    });
  }

  private static List<HRegion> generateTestData(final TableName tableName,
    final int... regionSizesMb) throws IOException {
    final List<HRegion> generatedRegions;
    final int numRegions = regionSizesMb.length;
    try (Table ignored = TEST_UTIL.createMultiRegionTable(tableName, FAMILY_NAME, numRegions)) {
      // Need to get sorted list of regions here
      generatedRegions = TEST_UTIL.getHBaseCluster().getRegions(tableName);
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
    throws IOException {
    final List<HRegion> generatedRegions = generateTestData(tableName, 1, 1, 2, 3, 5);
    assertEquals(5, MetaTableAccessor.getRegionCount(TEST_UTIL.getConnection(), tableName));
    admin.flush(tableName);

    final TableDescriptor td = TableDescriptorBuilder.newBuilder(admin.getDescriptor(tableName))
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
  private static int createTableBegsMerge(final TableName tableName) throws IOException {
    // create 5 regions with sizes to trigger merge of small regions
    final List<HRegion> generatedRegions = generateTestData(tableName, 1, 1, 3, 3, 5);
    assertEquals(5, MetaTableAccessor.getRegionCount(TEST_UTIL.getConnection(), tableName));
    admin.flush(tableName);

    final TableDescriptor td = TableDescriptorBuilder.newBuilder(admin.getDescriptor(tableName))
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

  private static void dropIfExists(final TableName tableName) throws IOException {
    if (tableName != null && admin.tableExists(tableName)) {
      if (admin.isTableEnabled(tableName)) {
        admin.disableTable(tableName);
      }
      admin.deleteTable(tableName);
    }
  }
}
