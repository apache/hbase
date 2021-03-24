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

import static org.apache.hadoop.hbase.master.normalizer.SimpleRegionNormalizer.DEFAULT_MERGE_MIN_REGION_AGE_DAYS;
import static org.apache.hadoop.hbase.master.normalizer.SimpleRegionNormalizer.MERGE_ENABLED_KEY;
import static org.apache.hadoop.hbase.master.normalizer.SimpleRegionNormalizer.MERGE_MIN_REGION_AGE_DAYS_KEY;
import static org.apache.hadoop.hbase.master.normalizer.SimpleRegionNormalizer.MERGE_MIN_REGION_SIZE_MB_KEY;
import static org.apache.hadoop.hbase.master.normalizer.SimpleRegionNormalizer.MIN_REGION_COUNT_KEY;
import static org.apache.hadoop.hbase.master.normalizer.SimpleRegionNormalizer.SPLIT_ENABLED_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.when;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.MasterRpcServices;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsSplitOrMergeEnabledRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsSplitOrMergeEnabledResponse;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;

/**
 * Tests logic of {@link SimpleRegionNormalizer}.
 */
@Category(SmallTests.class)
public class TestSimpleRegionNormalizer {
  private static final Log LOG = LogFactory.getLog(TestSimpleRegionNormalizer.class);

  private static SimpleRegionNormalizer normalizer;
  private static Configuration conf;

  // mocks
  private static MasterServices masterServices;
  private static MasterRpcServices masterRpcServices;

  @Rule
  public TestName name = new TestName();

  @Before
  public void before() {
    conf = HBaseConfiguration.create();
  }

  @Test
  public void testNoNormalizationForMetaTable() throws HBaseIOException {
    TableName testTable = TableName.META_TABLE_NAME;
    List<HRegionInfo> hris = new ArrayList<>();
    Map<byte[], Integer> regionSizes = new HashMap<>();

    setupMocksForNormalizer(regionSizes, hris);
    List<NormalizationPlan> plans = normalizer.computePlansForTable(testTable);
    assertTrue(plans.isEmpty());
  }

  @Test
  public void testNoNormalizationIfTooFewRegions() throws HBaseIOException {
    TableName testTable = TableName.valueOf(name.getMethodName());
    final List<HRegionInfo> regionInfos = createRegionInfos(testTable, 2);
    Map<byte[], Integer> regionSizes = createRegionSizesMap(regionInfos, 10, 15);
    setupMocksForNormalizer(regionSizes, regionInfos);
    List<NormalizationPlan> plans = normalizer.computePlansForTable(testTable);

    assertTrue(plans.isEmpty());
  }

  @Test
  public void testNoNormalizationOnNormalizedCluster() throws HBaseIOException {
    TableName testTable = TableName.valueOf(name.getMethodName());
    List<HRegionInfo> regionInfos = createRegionInfos(testTable, 4);
    Map<byte[], Integer> regionSizes = createRegionSizesMap(regionInfos, 10, 15, 8, 10);

    setupMocksForNormalizer(regionSizes, regionInfos);
    List<NormalizationPlan> plans = normalizer.computePlansForTable(testTable);
    assertTrue(plans.isEmpty());
  }

  @Test
  public void testMergeOfSmallRegions() throws HBaseIOException {
    TableName testTable = TableName.valueOf(name.getMethodName());
    List<HRegionInfo> regionInfos = createRegionInfos(testTable, 5);
    Map<byte[], Integer> regionSizes = createRegionSizesMap(regionInfos, 15, 5, 5, 15, 16);

    setupMocksForNormalizer(regionSizes, regionInfos);
    List<NormalizationPlan> plans = normalizer.computePlansForTable(testTable);

    NormalizationPlan plan = plans.get(0);
    assertTrue(plan instanceof MergeNormalizationPlan);
    assertEquals(regionInfos.get(1), ((MergeNormalizationPlan) plan).getFirstRegion());
    assertEquals(regionInfos.get(2), ((MergeNormalizationPlan) plan).getSecondRegion());
  }

  // Test for situation illustrated in HBASE-14867
  @Test
  public void testMergeOfSecondSmallestRegions() throws HBaseIOException {
    TableName testTable = TableName.valueOf(name.getMethodName());
    List<HRegionInfo> regionInfos = createRegionInfos(testTable, 6);
    Map<byte[], Integer> regionSizes =
        createRegionSizesMap(regionInfos, 1, 10000, 10000, 10000, 2700, 2700);

    setupMocksForNormalizer(regionSizes, regionInfos);
    List<NormalizationPlan> plans = normalizer.computePlansForTable(testTable);
    NormalizationPlan plan = plans.get(0);

    assertTrue(plan instanceof MergeNormalizationPlan);
    assertEquals(regionInfos.get(4), ((MergeNormalizationPlan) plan).getFirstRegion());
    assertEquals(regionInfos.get(5), ((MergeNormalizationPlan) plan).getSecondRegion());
  }

  @Test
  public void testMergeOfSmallNonAdjacentRegions() throws HBaseIOException {
    TableName testTable = TableName.valueOf(name.getMethodName());
    List<HRegionInfo> regionInfos = createRegionInfos(testTable, 5);
    Map<byte[], Integer> regionSizes = createRegionSizesMap(regionInfos, 15, 5, 16, 15, 5);

    setupMocksForNormalizer(regionSizes, regionInfos);
    List<NormalizationPlan> plans = normalizer.computePlansForTable(testTable);

    assertTrue(plans.isEmpty());
  }

  @Test
  public void testSplitOfLargeRegion() throws HBaseIOException {
    TableName testTable = TableName.valueOf(name.getMethodName());
    List<HRegionInfo> regionInfos = createRegionInfos(testTable, 4);
    Map<byte[], Integer> regionSizes = createRegionSizesMap(regionInfos, 8, 6, 10, 30);

    setupMocksForNormalizer(regionSizes, regionInfos);
    List<NormalizationPlan> plans = normalizer.computePlansForTable(testTable);
    NormalizationPlan plan = plans.get(0);

    assertTrue(plan instanceof SplitNormalizationPlan);
    assertEquals(regionInfos.get(3), ((SplitNormalizationPlan) plan).getRegionInfo());
  }

  @Test
  public void testSplitWithTargetRegionCount() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    List<HRegionInfo> regionInfo = createRegionInfos(tableName, 6);
    Map<byte[], Integer> regionSizes = createRegionSizesMap(regionInfo, 20, 40, 60, 80, 100, 120);
    setupMocksForNormalizer(regionSizes, regionInfo);

    // test when target region size is 20
    when(
      masterServices.getTableDescriptors().get((TableName) any()).getNormalizerTargetRegionSize())
          .thenReturn(20L);
    List<NormalizationPlan> plans = normalizer.computePlansForTable(tableName);
    assertEquals(4, plans.size());

    for (NormalizationPlan plan : plans) {
      assertTrue(plan instanceof SplitNormalizationPlan);
    }

    // test when target region size is 200
    when(
      masterServices.getTableDescriptors().get((TableName) any()).getNormalizerTargetRegionSize())
          .thenReturn(200L);
    plans = normalizer.computePlansForTable(tableName);
    assertEquals(2, plans.size());
    NormalizationPlan plan = plans.get(0);
    assertTrue(plan instanceof MergeNormalizationPlan);
    assertEquals(regionInfo.get(0), ((MergeNormalizationPlan) plan).getFirstRegion());
    assertEquals(regionInfo.get(1), ((MergeNormalizationPlan) plan).getSecondRegion());
  }

  @Test
  public void testSplitWithTargetRegionSize() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final List<HRegionInfo> regionInfos = createRegionInfos(tableName, 4);
    final Map<byte[], Integer> regionSizes = createRegionSizesMap(regionInfos, 20, 40, 60, 80);
    setupMocksForNormalizer(regionSizes, regionInfos);

    // test when target region count is 8
    when(
      masterServices.getTableDescriptors().get((TableName) any()).getNormalizerTargetRegionCount())
          .thenReturn(8);
    List<NormalizationPlan> plans = normalizer.computePlansForTable(tableName);
    assertEquals(2, plans.size());

    for (NormalizationPlan plan : plans) {
      assertTrue(plan instanceof SplitNormalizationPlan);
    }

    // test when target region count is 3
    when(
      masterServices.getTableDescriptors().get((TableName) any()).getNormalizerTargetRegionCount())
          .thenReturn(3);
    plans = normalizer.computePlansForTable(tableName);
    assertEquals(1, plans.size());
    NormalizationPlan plan = plans.get(0);
    assertTrue(plan instanceof MergeNormalizationPlan);
    assertEquals(regionInfos.get(0), ((MergeNormalizationPlan) plan).getFirstRegion());
    assertEquals(regionInfos.get(1), ((MergeNormalizationPlan) plan).getSecondRegion());
  }

  @Test
  public void testHonorsSplitEnabled() throws HBaseIOException {
    conf.setBoolean(SPLIT_ENABLED_KEY, true);
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final List<HRegionInfo> regionInfos = createRegionInfos(tableName, 5);
    final Map<byte[], Integer> regionSizes = createRegionSizesMap(regionInfos, 5, 5, 20, 5, 5);
    setupMocksForNormalizer(regionSizes, regionInfos);
    List<NormalizationPlan> plans = normalizer.computePlansForTable(tableName);
    boolean present = false;
    for (NormalizationPlan plan : plans) {
      if (plan instanceof SplitNormalizationPlan) {
        present = true;
        break;
      }
    }
    assertTrue(present);
    conf.setBoolean(SPLIT_ENABLED_KEY, false);
    setupMocksForNormalizer(regionSizes, regionInfos);
    plans = normalizer.computePlansForTable(tableName);
    assertTrue(plans.isEmpty());
  }

  @Test
  public void testHonorsMergeEnabled() throws HBaseIOException {
    conf.setBoolean(MERGE_ENABLED_KEY, true);
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final List<HRegionInfo> regionInfos = createRegionInfos(tableName, 5);
    final Map<byte[], Integer> regionSizes = createRegionSizesMap(regionInfos, 20, 5, 5, 20, 20);
    setupMocksForNormalizer(regionSizes, regionInfos);
    List<NormalizationPlan> plans = normalizer.computePlansForTable(tableName);
    boolean present = false;
    for (NormalizationPlan plan : plans) {
      if (plan instanceof MergeNormalizationPlan) {
        present = true;
        break;
      }
    }
    assertTrue(present);
    conf.setBoolean(MERGE_ENABLED_KEY, false);
    setupMocksForNormalizer(regionSizes, regionInfos);
    plans = normalizer.computePlansForTable(tableName);
    assertTrue(plans.isEmpty());
  }

  @Test
  public void testHonorsMinimumRegionCount() throws HBaseIOException {
    conf.setInt(MIN_REGION_COUNT_KEY, 1);
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final List<HRegionInfo> regionInfos = createRegionInfos(tableName, 3);
    // create a table topology that results in both a merge plan and a split plan. Assert that the
    // merge is only created when the when the number of table regions is above the region count
    // threshold, and that the split plan is create in both cases.
    final Map<byte[], Integer> regionSizes = createRegionSizesMap(regionInfos, 1, 1, 10);
    setupMocksForNormalizer(regionSizes, regionInfos);

    List<NormalizationPlan> plans = normalizer.computePlansForTable(tableName);
    boolean splitPlanPresent = false;
    boolean mergePlanPresent = false;
    for (NormalizationPlan plan : plans) {
      if (plan instanceof MergeNormalizationPlan) {
        mergePlanPresent = true;
        break;
      } else if (plan instanceof SplitNormalizationPlan) {
        splitPlanPresent = true;
      }
    }
    assertTrue(splitPlanPresent && mergePlanPresent);
    SplitNormalizationPlan splitPlan = (SplitNormalizationPlan) plans.get(0);
    assertEquals(regionInfos.get(2), splitPlan.getRegionInfo());
    MergeNormalizationPlan mergePlan = (MergeNormalizationPlan) plans.get(1);
    assertEquals(regionInfos.get(0), mergePlan.getFirstRegion());
    assertEquals(regionInfos.get(1), mergePlan.getSecondRegion());

    // have to call setupMocks again because we don't have dynamic config update on normalizer.
    conf.setInt(MIN_REGION_COUNT_KEY, 4);
    setupMocksForNormalizer(regionSizes, regionInfos);
    plans = normalizer.computePlansForTable(tableName);
    splitPlanPresent = false;
    for (NormalizationPlan plan : plans) {
      if (plan instanceof SplitNormalizationPlan) {
        splitPlanPresent = true;
        break;
      }
    }
    assertTrue(splitPlanPresent);
    splitPlan = (SplitNormalizationPlan) plans.get(0);
    assertEquals(regionInfos.get(2), splitPlan.getRegionInfo());
  }

  @Test
  public void testHonorsMergeMinRegionAge() throws HBaseIOException {
    conf.setInt(MERGE_MIN_REGION_AGE_DAYS_KEY, 7);
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final List<HRegionInfo> regionInfos = createRegionInfos(tableName, 4);
    final Map<byte[], Integer> regionSizes = createRegionSizesMap(regionInfos, 1, 1, 10, 10);
    setupMocksForNormalizer(regionSizes, regionInfos);
    assertEquals(7, normalizer.getMergeMinRegionAge());
    final List<NormalizationPlan> plans = normalizer.computePlansForTable(tableName);
    for (NormalizationPlan plan : plans) {
      assertFalse(plan instanceof MergeNormalizationPlan);
    }
    // have to call setupMocks again because we don't have dynamic config update on normalizer.
    conf.unset(MERGE_MIN_REGION_AGE_DAYS_KEY);
    setupMocksForNormalizer(regionSizes, regionInfos);
    assertEquals(DEFAULT_MERGE_MIN_REGION_AGE_DAYS, normalizer.getMergeMinRegionAge());
    final List<NormalizationPlan> plans1 = normalizer.computePlansForTable(tableName);
    assertTrue(!plans1.isEmpty());
    for (NormalizationPlan plan : plans) {
      assertTrue(plan instanceof MergeNormalizationPlan);
    }
  }

  @Test
  public void testHonorsMergeMinRegionSize() throws HBaseIOException {
    conf.setBoolean(SPLIT_ENABLED_KEY, false);
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final List<HRegionInfo> regionInfos = createRegionInfos(tableName, 5);
    final Map<byte[], Integer> regionSizes = createRegionSizesMap(regionInfos, 1, 2, 0, 10, 10);
    setupMocksForNormalizer(regionSizes, regionInfos);

    assertFalse(normalizer.isSplitEnabled());
    assertEquals(1, normalizer.getMergeMinRegionSizeMb());
    final List<NormalizationPlan> plans = normalizer.computePlansForTable(tableName);
    for (NormalizationPlan plan : plans) {
      assertTrue(plan instanceof MergeNormalizationPlan);
    }
    assertEquals(plans.size(), 1);
    final MergeNormalizationPlan plan = (MergeNormalizationPlan) plans.get(0);
    assertEquals(regionInfos.get(0), plan.getFirstRegion());
    assertEquals(regionInfos.get(1), plan.getSecondRegion());

    conf.setInt(MERGE_MIN_REGION_SIZE_MB_KEY, 3);
    setupMocksForNormalizer(regionSizes, regionInfos);
    assertEquals(3, normalizer.getMergeMinRegionSizeMb());
    assertTrue(normalizer.computePlansForTable(tableName).isEmpty());
  }

  // This test is to make sure that normalizer is only going to merge adjacent regions.
  @Test
  public void testNormalizerCannotMergeNonAdjacentRegions() throws HBaseIOException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    // create 5 regions with sizes to trigger merge of small regions. region ranges are:
    // [, "aa"), ["aa", "aa1"), ["aa1", "aa1!"), ["aa1!", "aa2"), ["aa2", )
    // Region ["aa", "aa1") and ["aa1!", "aa2") are not adjacent, they are not supposed to
    // merged.
    final byte[][] keys = { null, Bytes.toBytes("aa"), Bytes.toBytes("aa1!"), Bytes.toBytes("aa1"),
        Bytes.toBytes("aa2"), null, };
    final List<HRegionInfo> regionInfos = createRegionInfos(tableName, keys);
    final Map<byte[], Integer> regionSizes = createRegionSizesMap(regionInfos, 3, 1, 1, 3, 5);
    setupMocksForNormalizer(regionSizes, regionInfos);

    // Compute the plan, no merge plan returned as they are not adjacent.
    List<NormalizationPlan> plans = normalizer.computePlansForTable(tableName);
    assertTrue(plans.isEmpty());
  }

  @SuppressWarnings("MockitoCast")
  protected void setupMocksForNormalizer(Map<byte[], Integer> regionSizes, List<HRegionInfo> hris) {
    masterServices = Mockito.mock(MasterServices.class, RETURNS_DEEP_STUBS);
    masterRpcServices = Mockito.mock(MasterRpcServices.class, RETURNS_DEEP_STUBS);

    // for simplicity all regions are assumed to be on one server; doesn't matter to us
    ServerName sn = ServerName.valueOf("localhost", 0, 1L);
    when(masterServices.getAssignmentManager().getRegionStates()
        .getRegionsOfTable(any(TableName.class))).thenReturn(hris);
    when(masterServices.getAssignmentManager().getRegionStates()
        .getRegionServerOfRegion(any(HRegionInfo.class))).thenReturn(sn);
    when(masterServices.getAssignmentManager().getRegionStates()
        .isRegionInState(any(HRegionInfo.class), any(RegionState.State.class))).thenReturn(true);

    for (Map.Entry<byte[], Integer> region : regionSizes.entrySet()) {
      RegionLoad regionLoad = Mockito.mock(RegionLoad.class);
      when(regionLoad.getName()).thenReturn(region.getKey());
      when(regionLoad.getStorefileSizeMB()).thenReturn(region.getValue());

      // this is possibly broken with jdk9, unclear if false positive or not
      // suppress it for now, fix it when we get to running tests on 9
      // see: http://errorprone.info/bugpattern/MockitoCast
      when((Object) masterServices.getServerManager().getLoad(sn).getRegionsLoad()
          .get(region.getKey())).thenReturn(regionLoad);
    }
    try {
      when(masterRpcServices.isSplitOrMergeEnabled(any(RpcController.class),
        any(IsSplitOrMergeEnabledRequest.class)))
            .thenReturn(IsSplitOrMergeEnabledResponse.newBuilder().setEnabled(true).build());
    } catch (ServiceException se) {
      LOG.debug("error setting isSplitOrMergeEnabled switch", se);
    }

    normalizer = new SimpleRegionNormalizer();
    normalizer.setMasterServices(masterServices);
    normalizer.setMasterRpcServices(masterRpcServices);
    normalizer.setConf(conf);
  }

  /**
   * Create a list of {@link HRegionInfo}s that represent a region chain of the specified length.
   */
  private static List<HRegionInfo> createRegionInfos(final TableName tableName, final int length) {
    if (length < 1) {
      throw new IllegalStateException("length must be greater than or equal to 1.");
    }

    final byte[] startKey = Bytes.toBytes("aaaaa");
    final byte[] endKey = Bytes.toBytes("zzzzz");
    if (length == 1) {
      return Collections.singletonList(createRegionInfo(tableName, startKey, endKey));
    }

    final byte[][] splitKeys = Bytes.split(startKey, endKey, length - 1);
    final List<HRegionInfo> ret = new ArrayList<>(length);
    for (int i = 0; i < splitKeys.length - 1; i++) {
      ret.add(createRegionInfo(tableName, splitKeys[i], splitKeys[i + 1]));
    }
    return ret;
  }

  private static HRegionInfo createRegionInfo(final TableName tableName, final byte[] startKey,
      final byte[] endKey) {
    return new HRegionInfo(tableName, startKey, endKey, false, generateRegionId());
  }

  private static long generateRegionId() {
    final Timestamp currentTime = new Timestamp(EnvironmentEdgeManager.currentTime());
    return new Timestamp(
        currentTime.getTime() - TimeUnit.DAYS.toMillis(DEFAULT_MERGE_MIN_REGION_AGE_DAYS + 1))
            .getTime();
  }

  private static List<HRegionInfo> createRegionInfos(final TableName tableName,
      final byte[][] splitKeys) {
    final List<HRegionInfo> ret = new ArrayList<>(splitKeys.length);
    for (int i = 0; i < splitKeys.length - 1; i++) {
      ret.add(createRegionInfo(tableName, splitKeys[i], splitKeys[i + 1]));
    }
    return ret;
  }

  private static Map<byte[], Integer> createRegionSizesMap(final List<HRegionInfo> regionInfos,
      int... sizes) {
    if (regionInfos.size() != sizes.length) {
      throw new IllegalStateException("Parameter lengths must match.");
    }

    final Map<byte[], Integer> ret = new HashMap<>(regionInfos.size());
    for (int i = 0; i < regionInfos.size(); i++) {
      ret.put(regionInfos.get(i).getRegionName(), sizes[i]);
    }
    return ret;
  }
}
