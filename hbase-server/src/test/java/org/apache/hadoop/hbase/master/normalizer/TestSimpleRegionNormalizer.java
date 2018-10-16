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
package org.apache.hadoop.hbase.master.normalizer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Size;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.master.MasterRpcServices;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSplitOrMergeEnabledResponse;

/**
 * Tests logic of {@link SimpleRegionNormalizer}.
 */
@Category({MasterTests.class, SmallTests.class})
public class TestSimpleRegionNormalizer {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSimpleRegionNormalizer.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestSimpleRegionNormalizer.class);

  private static RegionNormalizer normalizer;

  // mocks
  private static MasterServices masterServices;
  private static MasterRpcServices masterRpcServices;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    normalizer = new SimpleRegionNormalizer();
  }

  @Test
  public void testPlanComparator() {
    Comparator<NormalizationPlan> comparator = new SimpleRegionNormalizer.PlanComparator();
    NormalizationPlan splitPlan1 = new SplitNormalizationPlan(null, null);
    NormalizationPlan splitPlan2 = new SplitNormalizationPlan(null, null);
    NormalizationPlan mergePlan1 = new MergeNormalizationPlan(null, null);
    NormalizationPlan mergePlan2 = new MergeNormalizationPlan(null, null);

    assertTrue(comparator.compare(splitPlan1, splitPlan2) == 0);
    assertTrue(comparator.compare(splitPlan2, splitPlan1) == 0);
    assertTrue(comparator.compare(mergePlan1, mergePlan2) == 0);
    assertTrue(comparator.compare(mergePlan2, mergePlan1) == 0);
    assertTrue(comparator.compare(splitPlan1, mergePlan1) < 0);
    assertTrue(comparator.compare(mergePlan1, splitPlan1) > 0);
  }

  @Test
  public void testNoNormalizationForMetaTable() throws HBaseIOException {
    TableName testTable = TableName.META_TABLE_NAME;
    List<RegionInfo> RegionInfo = new ArrayList<>();
    Map<byte[], Integer> regionSizes = new HashMap<>();

    setupMocksForNormalizer(regionSizes, RegionInfo);
    List<NormalizationPlan> plans = normalizer.computePlanForTable(testTable);
    assertTrue(plans == null);
  }

  @Test
  public void testNoNormalizationIfTooFewRegions() throws HBaseIOException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    List<RegionInfo> RegionInfo = new ArrayList<>();
    Map<byte[], Integer> regionSizes = new HashMap<>();
    RegionInfo hri1 = RegionInfoBuilder.newBuilder(tableName)
        .setStartKey(Bytes.toBytes("aaa"))
        .setEndKey(Bytes.toBytes("bbb"))
        .build();
    RegionInfo.add(hri1);
    regionSizes.put(hri1.getRegionName(), 10);

    RegionInfo hri2 = RegionInfoBuilder.newBuilder(tableName)
        .setStartKey(Bytes.toBytes("bbb"))
        .setEndKey(Bytes.toBytes("ccc"))
        .build();
    RegionInfo.add(hri2);
    regionSizes.put(hri2.getRegionName(), 15);

    setupMocksForNormalizer(regionSizes, RegionInfo);
    List<NormalizationPlan> plans = normalizer.computePlanForTable(tableName);
    assertTrue(plans == null);
  }

  @Test
  public void testNoNormalizationOnNormalizedCluster() throws HBaseIOException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    List<RegionInfo> RegionInfo = new ArrayList<>();
    Map<byte[], Integer> regionSizes = new HashMap<>();

    RegionInfo hri1 = RegionInfoBuilder.newBuilder(tableName)
        .setStartKey(Bytes.toBytes("aaa"))
        .setEndKey(Bytes.toBytes("bbb"))
        .build();
    RegionInfo.add(hri1);
    regionSizes.put(hri1.getRegionName(), 10);

    RegionInfo hri2 = RegionInfoBuilder.newBuilder(tableName)
        .setStartKey(Bytes.toBytes("bbb"))
        .setEndKey(Bytes.toBytes("ccc"))
        .build();
    RegionInfo.add(hri2);
    regionSizes.put(hri2.getRegionName(), 15);

    RegionInfo hri3 = RegionInfoBuilder.newBuilder(tableName)
        .setStartKey(Bytes.toBytes("ccc"))
        .setEndKey(Bytes.toBytes("ddd"))
        .build();
    RegionInfo.add(hri3);
    regionSizes.put(hri3.getRegionName(), 8);

    RegionInfo hri4 = RegionInfoBuilder.newBuilder(tableName)
        .setStartKey(Bytes.toBytes("ddd"))
        .setEndKey(Bytes.toBytes("eee"))
        .build();
    regionSizes.put(hri4.getRegionName(), 10);

    setupMocksForNormalizer(regionSizes, RegionInfo);
    List<NormalizationPlan> plans = normalizer.computePlanForTable(tableName);
    assertTrue(plans == null);
  }

  @Test
  public void testMergeOfSmallRegions() throws HBaseIOException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    List<RegionInfo> RegionInfo = new ArrayList<>();
    Map<byte[], Integer> regionSizes = new HashMap<>();

    RegionInfo hri1 = RegionInfoBuilder.newBuilder(tableName)
        .setStartKey(Bytes.toBytes("aaa"))
        .setEndKey(Bytes.toBytes("bbb"))
        .build();
    RegionInfo.add(hri1);
    regionSizes.put(hri1.getRegionName(), 15);

    RegionInfo hri2 = RegionInfoBuilder.newBuilder(tableName)
        .setStartKey(Bytes.toBytes("bbb"))
        .setEndKey(Bytes.toBytes("ccc"))
        .build();
    RegionInfo.add(hri2);
    regionSizes.put(hri2.getRegionName(), 5);

    RegionInfo hri3 = RegionInfoBuilder.newBuilder(tableName)
        .setStartKey(Bytes.toBytes("ccc"))
        .setEndKey(Bytes.toBytes("ddd"))
        .build();
    RegionInfo.add(hri3);
    regionSizes.put(hri3.getRegionName(), 5);

    RegionInfo hri4 = RegionInfoBuilder.newBuilder(tableName)
        .setStartKey(Bytes.toBytes("ddd"))
        .setEndKey(Bytes.toBytes("eee"))
        .build();
    RegionInfo.add(hri4);
    regionSizes.put(hri4.getRegionName(), 15);

    RegionInfo hri5 = RegionInfoBuilder.newBuilder(tableName)
        .setStartKey(Bytes.toBytes("eee"))
        .setEndKey(Bytes.toBytes("fff"))
        .build();
    RegionInfo.add(hri5);
    regionSizes.put(hri5.getRegionName(), 16);

    setupMocksForNormalizer(regionSizes, RegionInfo);
    List<NormalizationPlan> plans = normalizer.computePlanForTable(tableName);

    NormalizationPlan plan = plans.get(0);
    assertTrue(plan instanceof MergeNormalizationPlan);
    assertEquals(hri2, ((MergeNormalizationPlan) plan).getFirstRegion());
    assertEquals(hri3, ((MergeNormalizationPlan) plan).getSecondRegion());
  }

  // Test for situation illustrated in HBASE-14867
  @Test
  public void testMergeOfSecondSmallestRegions() throws HBaseIOException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    List<RegionInfo> RegionInfo = new ArrayList<>();
    Map<byte[], Integer> regionSizes = new HashMap<>();

    RegionInfo hri1 = RegionInfoBuilder.newBuilder(tableName)
        .setStartKey(Bytes.toBytes("aaa"))
        .setEndKey(Bytes.toBytes("bbb"))
        .build();
    RegionInfo.add(hri1);
    regionSizes.put(hri1.getRegionName(), 1);

    RegionInfo hri2 = RegionInfoBuilder.newBuilder(tableName)
        .setStartKey(Bytes.toBytes("bbb"))
        .setEndKey(Bytes.toBytes("ccc"))
        .build();
    RegionInfo.add(hri2);
    regionSizes.put(hri2.getRegionName(), 10000);

    RegionInfo hri3 = RegionInfoBuilder.newBuilder(tableName)
        .setStartKey(Bytes.toBytes("ccc"))
        .setEndKey(Bytes.toBytes("ddd"))
        .build();
    RegionInfo.add(hri3);
    regionSizes.put(hri3.getRegionName(), 10000);

    RegionInfo hri4 = RegionInfoBuilder.newBuilder(tableName)
        .setStartKey(Bytes.toBytes("ddd"))
        .setEndKey(Bytes.toBytes("eee"))
        .build();
    RegionInfo.add(hri4);
    regionSizes.put(hri4.getRegionName(), 10000);

    RegionInfo hri5 = RegionInfoBuilder.newBuilder(tableName)
        .setStartKey(Bytes.toBytes("eee"))
        .setEndKey(Bytes.toBytes("fff"))
        .build();
    RegionInfo.add(hri5);
    regionSizes.put(hri5.getRegionName(), 2700);

    RegionInfo hri6 = RegionInfoBuilder.newBuilder(tableName)
        .setStartKey(Bytes.toBytes("fff"))
        .setEndKey(Bytes.toBytes("ggg"))
        .build();
    RegionInfo.add(hri6);
    regionSizes.put(hri6.getRegionName(), 2700);

    setupMocksForNormalizer(regionSizes, RegionInfo);
    List<NormalizationPlan> plans = normalizer.computePlanForTable(tableName);
    NormalizationPlan plan = plans.get(0);

    assertTrue(plan instanceof MergeNormalizationPlan);
    assertEquals(hri5, ((MergeNormalizationPlan) plan).getFirstRegion());
    assertEquals(hri6, ((MergeNormalizationPlan) plan).getSecondRegion());
  }

  @Test
  public void testMergeOfSmallNonAdjacentRegions() throws HBaseIOException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    List<RegionInfo> RegionInfo = new ArrayList<>();
    Map<byte[], Integer> regionSizes = new HashMap<>();

    RegionInfo hri1 = RegionInfoBuilder.newBuilder(tableName)
        .setStartKey(Bytes.toBytes("aaa"))
        .setEndKey(Bytes.toBytes("bbb"))
        .build();
    RegionInfo.add(hri1);
    regionSizes.put(hri1.getRegionName(), 15);

    RegionInfo hri2 = RegionInfoBuilder.newBuilder(tableName)
        .setStartKey(Bytes.toBytes("bbb"))
        .setEndKey(Bytes.toBytes("ccc"))
        .build();
    RegionInfo.add(hri2);
    regionSizes.put(hri2.getRegionName(), 5);

    RegionInfo hri3 = RegionInfoBuilder.newBuilder(tableName)
        .setStartKey(Bytes.toBytes("ccc"))
        .setEndKey(Bytes.toBytes("ddd"))
        .build();
    RegionInfo.add(hri3);
    regionSizes.put(hri3.getRegionName(), 16);

    RegionInfo hri4 = RegionInfoBuilder.newBuilder(tableName)
        .setStartKey(Bytes.toBytes("ddd"))
        .setEndKey(Bytes.toBytes("eee"))
        .build();
    RegionInfo.add(hri4);
    regionSizes.put(hri4.getRegionName(), 15);

    RegionInfo hri5 = RegionInfoBuilder.newBuilder(tableName)
        .setStartKey(Bytes.toBytes("ddd"))
        .setEndKey(Bytes.toBytes("eee"))
        .build();
    RegionInfo.add(hri4);
    regionSizes.put(hri5.getRegionName(), 5);

    setupMocksForNormalizer(regionSizes, RegionInfo);
    List<NormalizationPlan> plans = normalizer.computePlanForTable(tableName);

    assertTrue(plans == null);
  }

  @Test
  public void testSplitOfLargeRegion() throws HBaseIOException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    List<RegionInfo> RegionInfo = new ArrayList<>();
    Map<byte[], Integer> regionSizes = new HashMap<>();

    RegionInfo hri1 = RegionInfoBuilder.newBuilder(tableName)
        .setStartKey(Bytes.toBytes("aaa"))
        .setEndKey(Bytes.toBytes("bbb"))
        .build();
    RegionInfo.add(hri1);
    regionSizes.put(hri1.getRegionName(), 8);

    RegionInfo hri2 = RegionInfoBuilder.newBuilder(tableName)
        .setStartKey(Bytes.toBytes("bbb"))
        .setEndKey(Bytes.toBytes("ccc"))
        .build();
    RegionInfo.add(hri2);
    regionSizes.put(hri2.getRegionName(), 6);

    RegionInfo hri3 = RegionInfoBuilder.newBuilder(tableName)
        .setStartKey(Bytes.toBytes("ccc"))
        .setEndKey(Bytes.toBytes("ddd"))
        .build();
    RegionInfo.add(hri3);
    regionSizes.put(hri3.getRegionName(), 10);

    RegionInfo hri4 = RegionInfoBuilder.newBuilder(tableName)
        .setStartKey(Bytes.toBytes("ddd"))
        .setEndKey(Bytes.toBytes("eee"))
        .build();
    RegionInfo.add(hri4);
    regionSizes.put(hri4.getRegionName(), 30);

    setupMocksForNormalizer(regionSizes, RegionInfo);
    List<NormalizationPlan> plans = normalizer.computePlanForTable(tableName);
    NormalizationPlan plan = plans.get(0);

    assertTrue(plan instanceof SplitNormalizationPlan);
    assertEquals(hri4, ((SplitNormalizationPlan) plan).getRegionInfo());
  }

  @Test
  public void testSplitWithTargetRegionCount() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    List<RegionInfo> RegionInfo = new ArrayList<>();
    Map<byte[], Integer> regionSizes = new HashMap<>();

    RegionInfo hri1 = RegionInfoBuilder.newBuilder(tableName).setStartKey(Bytes.toBytes("aaa"))
        .setEndKey(Bytes.toBytes("bbb")).build();
    RegionInfo.add(hri1);
    regionSizes.put(hri1.getRegionName(), 20);

    RegionInfo hri2 = RegionInfoBuilder.newBuilder(tableName).setStartKey(Bytes.toBytes("bbb"))
        .setEndKey(Bytes.toBytes("ccc")).build();
    RegionInfo.add(hri2);
    regionSizes.put(hri2.getRegionName(), 40);

    RegionInfo hri3 = RegionInfoBuilder.newBuilder(tableName).setStartKey(Bytes.toBytes("ccc"))
        .setEndKey(Bytes.toBytes("ddd")).build();
    RegionInfo.add(hri3);
    regionSizes.put(hri3.getRegionName(), 60);

    RegionInfo hri4 = RegionInfoBuilder.newBuilder(tableName).setStartKey(Bytes.toBytes("ddd"))
        .setEndKey(Bytes.toBytes("eee")).build();
    RegionInfo.add(hri4);
    regionSizes.put(hri4.getRegionName(), 80);

    RegionInfo hri5 = RegionInfoBuilder.newBuilder(tableName).setStartKey(Bytes.toBytes("eee"))
        .setEndKey(Bytes.toBytes("fff")).build();
    RegionInfo.add(hri5);
    regionSizes.put(hri5.getRegionName(), 100);

    RegionInfo hri6 = RegionInfoBuilder.newBuilder(tableName).setStartKey(Bytes.toBytes("fff"))
        .setEndKey(Bytes.toBytes("ggg")).build();
    RegionInfo.add(hri6);
    regionSizes.put(hri6.getRegionName(), 120);

    setupMocksForNormalizer(regionSizes, RegionInfo);

    // test when target region size is 20
    when(masterServices.getTableDescriptors().get(any()).getNormalizerTargetRegionSize())
        .thenReturn(20L);
    List<NormalizationPlan> plans = normalizer.computePlanForTable(tableName);
    Assert.assertEquals(4, plans.size());

    for (NormalizationPlan plan : plans) {
      assertTrue(plan instanceof SplitNormalizationPlan);
    }

    // test when target region size is 200
    when(masterServices.getTableDescriptors().get(any()).getNormalizerTargetRegionSize())
        .thenReturn(200L);
    plans = normalizer.computePlanForTable(tableName);
    Assert.assertEquals(2, plans.size());
    NormalizationPlan plan = plans.get(0);
    assertTrue(plan instanceof MergeNormalizationPlan);
    assertEquals(hri1, ((MergeNormalizationPlan) plan).getFirstRegion());
    assertEquals(hri2, ((MergeNormalizationPlan) plan).getSecondRegion());
  }

  @Test
  public void testSplitWithTargetRegionSize() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    List<RegionInfo> RegionInfo = new ArrayList<>();
    Map<byte[], Integer> regionSizes = new HashMap<>();

    RegionInfo hri1 = RegionInfoBuilder.newBuilder(tableName).setStartKey(Bytes.toBytes("aaa"))
        .setEndKey(Bytes.toBytes("bbb")).build();
    RegionInfo.add(hri1);
    regionSizes.put(hri1.getRegionName(), 20);

    RegionInfo hri2 = RegionInfoBuilder.newBuilder(tableName).setStartKey(Bytes.toBytes("bbb"))
        .setEndKey(Bytes.toBytes("ccc")).build();
    RegionInfo.add(hri2);
    regionSizes.put(hri2.getRegionName(), 40);

    RegionInfo hri3 = RegionInfoBuilder.newBuilder(tableName).setStartKey(Bytes.toBytes("ccc"))
        .setEndKey(Bytes.toBytes("ddd")).build();
    RegionInfo.add(hri3);
    regionSizes.put(hri3.getRegionName(), 60);

    RegionInfo hri4 = RegionInfoBuilder.newBuilder(tableName).setStartKey(Bytes.toBytes("ddd"))
        .setEndKey(Bytes.toBytes("eee")).build();
    RegionInfo.add(hri4);
    regionSizes.put(hri4.getRegionName(), 80);

    setupMocksForNormalizer(regionSizes, RegionInfo);

    // test when target region count is 8
    when(masterServices.getTableDescriptors().get(any()).getNormalizerTargetRegionCount())
        .thenReturn(8);
    List<NormalizationPlan> plans = normalizer.computePlanForTable(tableName);
    Assert.assertEquals(2, plans.size());

    for (NormalizationPlan plan : plans) {
      assertTrue(plan instanceof SplitNormalizationPlan);
    }

    // test when target region count is 3
    when(masterServices.getTableDescriptors().get(any()).getNormalizerTargetRegionCount())
        .thenReturn(3);
    plans = normalizer.computePlanForTable(tableName);
    Assert.assertEquals(1, plans.size());
    NormalizationPlan plan = plans.get(0);
    assertTrue(plan instanceof MergeNormalizationPlan);
    assertEquals(hri1, ((MergeNormalizationPlan) plan).getFirstRegion());
    assertEquals(hri2, ((MergeNormalizationPlan) plan).getSecondRegion());
  }

  @SuppressWarnings("MockitoCast")
  protected void setupMocksForNormalizer(Map<byte[], Integer> regionSizes,
                                         List<RegionInfo> RegionInfo) {
    masterServices = Mockito.mock(MasterServices.class, RETURNS_DEEP_STUBS);
    masterRpcServices = Mockito.mock(MasterRpcServices.class, RETURNS_DEEP_STUBS);

    // for simplicity all regions are assumed to be on one server; doesn't matter to us
    ServerName sn = ServerName.valueOf("localhost", 0, 1L);
    when(masterServices.getAssignmentManager().getRegionStates().
      getRegionsOfTable(any())).thenReturn(RegionInfo);
    when(masterServices.getAssignmentManager().getRegionStates().
      getRegionServerOfRegion(any())).thenReturn(sn);

    for (Map.Entry<byte[], Integer> region : regionSizes.entrySet()) {
      RegionMetrics regionLoad = Mockito.mock(RegionMetrics.class);
      when(regionLoad.getRegionName()).thenReturn(region.getKey());
      when(regionLoad.getStoreFileSize())
        .thenReturn(new Size(region.getValue(), Size.Unit.MEGABYTE));

      // this is possibly broken with jdk9, unclear if false positive or not
      // suppress it for now, fix it when we get to running tests on 9
      // see: http://errorprone.info/bugpattern/MockitoCast
      when((Object) masterServices.getServerManager().getLoad(sn).
        getRegionMetrics().get(region.getKey())).thenReturn(regionLoad);
    }
    try {
      when(masterRpcServices.isSplitOrMergeEnabled(any(),
        any())).thenReturn(
          IsSplitOrMergeEnabledResponse.newBuilder().setEnabled(true).build());
    } catch (ServiceException se) {
      LOG.debug("error setting isSplitOrMergeEnabled switch", se);
    }

    normalizer.setMasterServices(masterServices);
    normalizer.setMasterRpcServices(masterRpcServices);
  }
}
