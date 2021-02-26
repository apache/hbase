/**
 *
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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.quotas.QuotaUtil;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.LoadTestKVGenerator;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Testing {@link SimpleRegionNormalizer} on minicluster.
 */
@Category(MediumTests.class)
public class TestSimpleRegionNormalizerOnCluster {
  private static final Log LOG = LogFactory.getLog(TestSimpleRegionNormalizerOnCluster.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] FAMILYNAME = Bytes.toBytes("fam");
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

    // Start a cluster of two regionservers.
    TEST_UTIL.startMiniCluster(1);
    //TestNamespaceAuditor.waitForQuotaEnabled();
    admin = TEST_UTIL.getHBaseAdmin();
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
    admin.setNormalizerRunning(false);
  }

  @Test
  public void testHonorsNormalizerSwitch() throws IOException {
    assertFalse(admin.isNormalizerEnabled());
    assertFalse(admin.normalize());
    assertFalse(admin.setNormalizerRunning(true));
    assertTrue(admin.normalize());
  }


  @Test(timeout = 60000)
  @SuppressWarnings("deprecation")
  public void testRegionNormalizationSplitOnCluster() throws Exception {
    final TableName TABLENAME = TableName.valueOf(name.getMethodName());

    try (HTable ht = TEST_UTIL.createMultiRegionTable(TABLENAME, FAMILYNAME, 5)) {
      // Need to get sorted list of regions here
      List<HRegion> generatedRegions = TEST_UTIL.getHBaseCluster().getRegions(TABLENAME);
      Collections.sort(generatedRegions, new Comparator<HRegion>() {
        @Override
        public int compare(HRegion o1, HRegion o2) {
          return o1.getRegionInfo().compareTo(o2.getRegionInfo());
        }
      });

      HRegion region = generatedRegions.get(0);
      generateTestData(region, 1);
      region.flush(true);

      region = generatedRegions.get(1);
      generateTestData(region, 1);
      region.flush(true);

      region = generatedRegions.get(2);
      generateTestData(region, 2);
      region.flush(true);

      region = generatedRegions.get(3);
      generateTestData(region, 2);
      region.flush(true);

      region = generatedRegions.get(4);
      generateTestData(region, 5);
      region.flush(true);

    }

    HTableDescriptor htd = admin.getTableDescriptor(TABLENAME);
    htd.setNormalizationEnabled(true);
    admin.modifyTable(TABLENAME, htd);

    admin.flush(TABLENAME);
    admin.setNormalizerRunning(true);

    System.out.println(admin.getTableDescriptor(TABLENAME));

    assertEquals(5, MetaTableAccessor.getRegionCount(TEST_UTIL.getConnection(), TABLENAME));

    // Now trigger a split and stop when the split is in progress
    Thread.sleep(5000); // to let region load to update
    boolean b = master.normalizeRegions();
    assertTrue(b);

    while (true) {
      List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(TABLENAME);
      int cnt = 0;
      for (HRegion region : regions) {
        String regionName = region.getRegionInfo().getRegionNameAsString();
        if (regionName.startsWith("testRegionNormalizationSplitOnCluster,zzzzz")) {
          cnt++;
        }
      }
      if (cnt >= 2) {
        break;
      }
    }
    admin.disableTable(TABLENAME);
    admin.deleteTable(TABLENAME);
  }

  @Test(timeout = 60000)
  @SuppressWarnings("deprecation")
  public void testRegionNormalizationMergeOnCluster() throws Exception {
    final TableName TABLENAME = TableName.valueOf(name.getMethodName());

    // create 5 regions with sizes to trigger merge of small regions
    try (HTable ht = TEST_UTIL.createMultiRegionTable(TABLENAME, FAMILYNAME, 5)) {
      // Need to get sorted list of regions here
      List<HRegion> generatedRegions = TEST_UTIL.getHBaseCluster().getRegions(TABLENAME);
      Collections.sort(generatedRegions, new Comparator<HRegion>() {
        @Override
        public int compare(HRegion o1, HRegion o2) {
          return o1.getRegionInfo().compareTo(o2.getRegionInfo());
        }
      });

      HRegion region = generatedRegions.get(0);
      generateTestData(region, 1);
      region.flush(true);

      region = generatedRegions.get(1);
      generateTestData(region, 1);
      region.flush(true);

      region = generatedRegions.get(2);
      generateTestData(region, 3);
      region.flush(true);

      region = generatedRegions.get(3);
      generateTestData(region, 3);
      region.flush(true);

      region = generatedRegions.get(4);
      generateTestData(region, 5);
      region.flush(true);
    }

    HTableDescriptor htd = admin.getTableDescriptor(TABLENAME);
    htd.setNormalizationEnabled(true);
    admin.modifyTable(TABLENAME, htd);

    admin.flush(TABLENAME);

    assertEquals(5, MetaTableAccessor.getRegionCount(TEST_UTIL.getConnection(), TABLENAME));

    // Now trigger a merge and stop when the merge is in progress
    admin.setNormalizerRunning(true);
    Thread.sleep(5000); // to let region load to update
    master.normalizeRegions();

    while (MetaTableAccessor.getRegionCount(TEST_UTIL.getConnection(), TABLENAME) > 4) {
      LOG.info("Waiting for normalization merge to complete");
      Thread.sleep(100);
    }

    assertEquals(4, MetaTableAccessor.getRegionCount(TEST_UTIL.getConnection(), TABLENAME));
    dropIfExists(TABLENAME);
  }

  private static void waitForTableSplit(final TableName tableName, final int targetRegionCount)
    throws IOException {
    TEST_UTIL.waitFor(10*1000, new Waiter.ExplainingPredicate<IOException>() {
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

  private static List<HRegion> generateTestData(final TableName tableName,
                                                final int... regionSizesMb) throws IOException {
    final List<HRegion> generatedRegions;
    final int numRegions = regionSizesMb.length;
    try (HTable ignored = TEST_UTIL.createMultiRegionTable(tableName, FAMILYNAME, numRegions)) {
      // Need to get sorted list of regions here
      generatedRegions = TEST_UTIL.getHBaseCluster().getRegions(tableName);
      //generatedRegions.sort(Comparator.comparing(HRegion::getRegionInfo, RegionInfo.COMPARATOR));
      Collections.sort(generatedRegions, new Comparator<HRegion>() {
        @Override
        public int compare(HRegion o1, HRegion o2) {
          return o1.getRegionInfo().compareTo(o2.getRegionInfo());
        }
      });
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
        put.add(FAMILYNAME, col, value);
        region.put(put);
      }
    }
  }

  private static double getRegionSizeMB(final MasterServices masterServices,
                                        final HRegionInfo regionInfo) {
    ServerName sn =
      masterServices.getAssignmentManager().getRegionStates().getRegionServerOfRegion(regionInfo);
    if (sn == null) {
      LOG.debug(regionInfo.getRegionNameAsString() + " region was not found on any Server");
      return -1;
    }
    ServerLoad load = masterServices.getServerManager().getLoad(sn);
    if (load == null) {
      LOG.debug(sn.getServerName() + " was not found in online servers");
      return -1;
    }
    RegionLoad regionLoad = load.getRegionsLoad().get(regionInfo.getRegionName());
    if (regionLoad == null) {
      LOG.debug(regionInfo.getRegionNameAsString() + " was not found in RegionsLoad");
      return -1;
    }
    return regionLoad.getStorefileSizeMB();
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
