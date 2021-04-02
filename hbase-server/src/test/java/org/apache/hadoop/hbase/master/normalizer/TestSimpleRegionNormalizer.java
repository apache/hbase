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

import static java.lang.String.format;
import static org.apache.hadoop.hbase.master.normalizer.SimpleRegionNormalizer.DEFAULT_MERGE_MIN_REGION_AGE_DAYS;
import static org.apache.hadoop.hbase.master.normalizer.SimpleRegionNormalizer.MERGE_ENABLED_KEY;
import static org.apache.hadoop.hbase.master.normalizer.SimpleRegionNormalizer.MERGE_MIN_REGION_AGE_DAYS_KEY;
import static org.apache.hadoop.hbase.master.normalizer.SimpleRegionNormalizer.MERGE_MIN_REGION_SIZE_MB_KEY;
import static org.apache.hadoop.hbase.master.normalizer.SimpleRegionNormalizer.MIN_REGION_COUNT_KEY;
import static org.apache.hadoop.hbase.master.normalizer.SimpleRegionNormalizer.SPLIT_ENABLED_KEY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.when;
import java.time.Instant;
import java.time.Period;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Size;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNameTestRule;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * Tests logic of {@link SimpleRegionNormalizer}.
 */
@Category({MasterTests.class, SmallTests.class})
public class TestSimpleRegionNormalizer {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSimpleRegionNormalizer.class);

  private Configuration conf;
  private SimpleRegionNormalizer normalizer;
  private MasterServices masterServices;
  private TableDescriptor tableDescriptor;

  @Rule
  public TableNameTestRule name = new TableNameTestRule();

  @Before
  public void before() {
    conf = HBaseConfiguration.create();
    tableDescriptor = TableDescriptorBuilder.newBuilder(name.getTableName()).build();
  }

  @Test
  public void testNoNormalizationForMetaTable() {
    TableName testTable = TableName.META_TABLE_NAME;
    TableDescriptor testMetaTd = TableDescriptorBuilder.newBuilder(testTable).build();
    List<RegionInfo> RegionInfo = new ArrayList<>();
    Map<byte[], Integer> regionSizes = new HashMap<>();

    setupMocksForNormalizer(regionSizes, RegionInfo);
    List<NormalizationPlan> plans = normalizer.computePlansForTable(testMetaTd);
    assertThat(plans, empty());
  }

  @Test
  public void testNoNormalizationIfTooFewRegions() {
    final TableName tableName = name.getTableName();
    final List<RegionInfo> regionInfos = createRegionInfos(tableName, 2);
    final Map<byte[], Integer> regionSizes = createRegionSizesMap(regionInfos, 10, 15);
    setupMocksForNormalizer(regionSizes, regionInfos);

    List<NormalizationPlan> plans = normalizer.computePlansForTable(tableDescriptor);
    assertThat(plans, empty());
  }

  @Test
  public void testNoNormalizationOnNormalizedCluster() {
    final TableName tableName = name.getTableName();
    final List<RegionInfo> regionInfos = createRegionInfos(tableName, 4);
    final Map<byte[], Integer> regionSizes =
      createRegionSizesMap(regionInfos, 10, 15, 8, 10);
    setupMocksForNormalizer(regionSizes, regionInfos);

    List<NormalizationPlan> plans = normalizer.computePlansForTable(tableDescriptor);
    assertThat(plans, empty());
  }

  private void noNormalizationOnTransitioningRegions(final RegionState.State state) {
    final TableName tableName = name.getTableName();
    final List<RegionInfo> regionInfos = createRegionInfos(tableName, 3);
    final Map<byte[], Integer> regionSizes = createRegionSizesMap(regionInfos, 10, 1, 100);

    setupMocksForNormalizer(regionSizes, regionInfos);
    when(masterServices.getAssignmentManager().getRegionStates()
      .getRegionState(any(RegionInfo.class)))
      .thenReturn(RegionState.createForTesting(null, state));
    assertThat(normalizer.getMinRegionCount(), greaterThanOrEqualTo(regionInfos.size()));

    List<NormalizationPlan> plans = normalizer.computePlansForTable(tableDescriptor);
    assertThat(format("Unexpected plans for RegionState %s", state), plans, empty());
  }

  @Test
  public void testNoNormalizationOnMergingNewRegions() {
    noNormalizationOnTransitioningRegions(RegionState.State.MERGING_NEW);
  }

  @Test
  public void testNoNormalizationOnMergingRegions() {
    noNormalizationOnTransitioningRegions(RegionState.State.MERGING);
  }

  @Test
  public void testNoNormalizationOnMergedRegions() {
    noNormalizationOnTransitioningRegions(RegionState.State.MERGED);
  }

  @Test
  public void testNoNormalizationOnSplittingNewRegions() {
    noNormalizationOnTransitioningRegions(RegionState.State.SPLITTING_NEW);
  }

  @Test
  public void testNoNormalizationOnSplittingRegions() {
    noNormalizationOnTransitioningRegions(RegionState.State.SPLITTING);
  }

  @Test
  public void testNoNormalizationOnSplitRegions() {
    noNormalizationOnTransitioningRegions(RegionState.State.SPLIT);
  }

  @Test
  public void testMergeOfSmallRegions() {
    final TableName tableName = name.getTableName();
    final List<RegionInfo> regionInfos = createRegionInfos(tableName, 5);
    final Map<byte[], Integer> regionSizes =
      createRegionSizesMap(regionInfos, 15, 5, 5, 15, 16);
    setupMocksForNormalizer(regionSizes, regionInfos);

    assertThat(
      normalizer.computePlansForTable(tableDescriptor),
      contains(new MergeNormalizationPlan.Builder()
        .addTarget(regionInfos.get(1), 5)
        .addTarget(regionInfos.get(2), 5)
        .build()));
  }

  // Test for situation illustrated in HBASE-14867
  @Test
  public void testMergeOfSecondSmallestRegions() {
    final TableName tableName = name.getTableName();
    final List<RegionInfo> regionInfos = createRegionInfos(tableName, 6);
    final Map<byte[], Integer> regionSizes =
      createRegionSizesMap(regionInfos, 1, 10000, 10000, 10000, 2700, 2700);
    setupMocksForNormalizer(regionSizes, regionInfos);

    assertThat(
      normalizer.computePlansForTable(tableDescriptor),
      contains(new MergeNormalizationPlan.Builder()
        .addTarget(regionInfos.get(4), 2700)
        .addTarget(regionInfos.get(5), 2700)
        .build()));
  }

  @Test
  public void testMergeOfSmallNonAdjacentRegions() {
    final TableName tableName = name.getTableName();
    final List<RegionInfo> regionInfos = createRegionInfos(tableName, 5);
    final Map<byte[], Integer> regionSizes =
      createRegionSizesMap(regionInfos, 15, 5, 16, 15, 5);
    setupMocksForNormalizer(regionSizes, regionInfos);

    List<NormalizationPlan> plans = normalizer.computePlansForTable(tableDescriptor);
    assertThat(plans, empty());
  }

  @Test
  public void testSplitOfLargeRegion() {
    final TableName tableName = name.getTableName();
    final List<RegionInfo> regionInfos = createRegionInfos(tableName, 4);
    final Map<byte[], Integer> regionSizes =
      createRegionSizesMap(regionInfos, 8, 6, 10, 30);
    setupMocksForNormalizer(regionSizes, regionInfos);

    assertThat(normalizer.computePlansForTable(tableDescriptor), contains(
      new SplitNormalizationPlan(regionInfos.get(3), 30)));
  }

  @Test
  public void testWithTargetRegionSize() throws Exception {
    final TableName tableName = name.getTableName();
    final List<RegionInfo> regionInfos = createRegionInfos(tableName, 6);
    final Map<byte[], Integer> regionSizes =
      createRegionSizesMap(regionInfos, 20, 40, 60, 80, 100, 120);
    setupMocksForNormalizer(regionSizes, regionInfos);

    // test when target region size is 20
    when(tableDescriptor.getNormalizerTargetRegionSize()).thenReturn(20L);
    assertThat(normalizer.computePlansForTable(tableDescriptor), contains(
      new SplitNormalizationPlan(regionInfos.get(2), 60),
      new SplitNormalizationPlan(regionInfos.get(3), 80),
      new SplitNormalizationPlan(regionInfos.get(4), 100),
      new SplitNormalizationPlan(regionInfos.get(5), 120)
    ));

    // test when target region size is 200
    when(tableDescriptor.getNormalizerTargetRegionSize()).thenReturn(200L);
    assertThat(
      normalizer.computePlansForTable(tableDescriptor),
      contains(
        new MergeNormalizationPlan.Builder()
          .addTarget(regionInfos.get(0), 20)
          .addTarget(regionInfos.get(1), 40)
          .addTarget(regionInfos.get(2), 60)
          .addTarget(regionInfos.get(3), 80)
          .build()));
  }

  @Test
  public void testSplitWithTargetRegionCount() throws Exception {
    final TableName tableName = name.getTableName();
    final List<RegionInfo> regionInfos = createRegionInfos(tableName, 4);
    final Map<byte[], Integer> regionSizes =
      createRegionSizesMap(regionInfos, 20, 40, 60, 80);
    setupMocksForNormalizer(regionSizes, regionInfos);

    // test when target region count is 8
    when(tableDescriptor.getNormalizerTargetRegionCount()).thenReturn(8);
    assertThat(normalizer.computePlansForTable(tableDescriptor), contains(
      new SplitNormalizationPlan(regionInfos.get(2), 60),
      new SplitNormalizationPlan(regionInfos.get(3), 80)));

    // test when target region count is 3
    when(tableDescriptor.getNormalizerTargetRegionCount()).thenReturn(3);
    assertThat(
      normalizer.computePlansForTable(tableDescriptor),
      contains(new MergeNormalizationPlan.Builder()
        .addTarget(regionInfos.get(0), 20)
        .addTarget(regionInfos.get(1), 40)
        .build()));
  }

  @Test
  public void testHonorsSplitEnabled() {
    conf.setBoolean(SPLIT_ENABLED_KEY, true);
    final TableName tableName = name.getTableName();
    final List<RegionInfo> regionInfos = createRegionInfos(tableName, 5);
    final Map<byte[], Integer> regionSizes =
      createRegionSizesMap(regionInfos, 5, 5, 20, 5, 5);
    setupMocksForNormalizer(regionSizes, regionInfos);
    assertThat(
      normalizer.computePlansForTable(tableDescriptor),
      contains(instanceOf(SplitNormalizationPlan.class)));

    conf.setBoolean(SPLIT_ENABLED_KEY, false);
    setupMocksForNormalizer(regionSizes, regionInfos);
    assertThat(normalizer.computePlansForTable(tableDescriptor), empty());
  }

  @Test
  public void testHonorsSplitEnabledInTD() {
    conf.setBoolean(SPLIT_ENABLED_KEY, true);
    final TableName tableName = name.getTableName();
    final List<RegionInfo> regionInfos = createRegionInfos(tableName, 5);
    final Map<byte[], Integer> regionSizes =
      createRegionSizesMap(regionInfos, 5, 5, 20, 5, 5);
    setupMocksForNormalizer(regionSizes, regionInfos);
    assertThat(
      normalizer.computePlansForTable(tableDescriptor),
      contains(instanceOf(SplitNormalizationPlan.class)));

    // When hbase.normalizer.split.enabled is true in configuration, but false in table descriptor
    when(tableDescriptor.getValue(SPLIT_ENABLED_KEY)).thenReturn("false");
    assertThat(normalizer.computePlansForTable(tableDescriptor), empty());

    // When hbase.normalizer.split.enabled is false in configuration, but true in table descriptor
    conf.setBoolean(SPLIT_ENABLED_KEY, false);
    setupMocksForNormalizer(regionSizes, regionInfos);
    when(tableDescriptor.getValue(SPLIT_ENABLED_KEY)).thenReturn("true");
    assertThat(
      normalizer.computePlansForTable(tableDescriptor),
      contains(instanceOf(SplitNormalizationPlan.class)));
  }

  @Test
  public void testHonorsMergeEnabled() {
    conf.setBoolean(MERGE_ENABLED_KEY, true);
    final TableName tableName = name.getTableName();
    final List<RegionInfo> regionInfos = createRegionInfos(tableName, 5);
    final Map<byte[], Integer> regionSizes =
      createRegionSizesMap(regionInfos, 20, 5, 5, 20, 20);
    setupMocksForNormalizer(regionSizes, regionInfos);
    assertThat(
      normalizer.computePlansForTable(tableDescriptor),
      contains(instanceOf(MergeNormalizationPlan.class)));

    conf.setBoolean(MERGE_ENABLED_KEY, false);
    setupMocksForNormalizer(regionSizes, regionInfos);
    assertThat(normalizer.computePlansForTable(tableDescriptor), empty());
  }

  @Test
  public void testHonorsMergeEnabledInTD() {
    conf.setBoolean(MERGE_ENABLED_KEY, true);
    final TableName tableName = name.getTableName();
    final List<RegionInfo> regionInfos = createRegionInfos(tableName, 5);
    final Map<byte[], Integer> regionSizes =
      createRegionSizesMap(regionInfos, 20, 5, 5, 20, 20);
    setupMocksForNormalizer(regionSizes, regionInfos);
    assertThat(
      normalizer.computePlansForTable(tableDescriptor),
      contains(instanceOf(MergeNormalizationPlan.class)));

    // When hbase.normalizer.merge.enabled is true in configuration, but false in table descriptor
    when(tableDescriptor.getValue(MERGE_ENABLED_KEY)).thenReturn("false");
    assertThat(normalizer.computePlansForTable(tableDescriptor), empty());

    // When hbase.normalizer.merge.enabled is false in configuration, but true in table descriptor
    conf.setBoolean(MERGE_ENABLED_KEY, false);
    setupMocksForNormalizer(regionSizes, regionInfos);
    when(tableDescriptor.getValue(MERGE_ENABLED_KEY)).thenReturn("true");
    assertThat(
      normalizer.computePlansForTable(tableDescriptor),
      contains(instanceOf(MergeNormalizationPlan.class)));
  }

  @Test
  public void testHonorsMinimumRegionCount() {
    conf.setInt(MIN_REGION_COUNT_KEY, 1);
    final TableName tableName = name.getTableName();
    final List<RegionInfo> regionInfos = createRegionInfos(tableName, 3);
    // create a table topology that results in both a merge plan and a split plan. Assert that the
    // merge is only created when the when the number of table regions is above the region count
    // threshold, and that the split plan is create in both cases.
    final Map<byte[], Integer> regionSizes = createRegionSizesMap(regionInfos, 1, 1, 10);
    setupMocksForNormalizer(regionSizes, regionInfos);

    List<NormalizationPlan> plans = normalizer.computePlansForTable(tableDescriptor);
    assertThat(plans, contains(
      new SplitNormalizationPlan(regionInfos.get(2), 10),
      new MergeNormalizationPlan.Builder()
        .addTarget(regionInfos.get(0), 1)
        .addTarget(regionInfos.get(1), 1)
        .build()));

    // have to call setupMocks again because we don't have dynamic config update on normalizer.
    conf.setInt(MIN_REGION_COUNT_KEY, 4);
    setupMocksForNormalizer(regionSizes, regionInfos);
    assertThat(normalizer.computePlansForTable(tableDescriptor), contains(
      new SplitNormalizationPlan(regionInfos.get(2), 10)));
  }

  @Test
  public void testHonorsMinimumRegionCountInTD() {
    conf.setInt(MIN_REGION_COUNT_KEY, 1);
    final TableName tableName = name.getTableName();
    final List<RegionInfo> regionInfos = createRegionInfos(tableName, 3);
    // create a table topology that results in both a merge plan and a split plan. Assert that the
    // merge is only created when the when the number of table regions is above the region count
    // threshold, and that the split plan is create in both cases.
    final Map<byte[], Integer> regionSizes = createRegionSizesMap(regionInfos, 1, 1, 10);
    setupMocksForNormalizer(regionSizes, regionInfos);

    List<NormalizationPlan> plans = normalizer.computePlansForTable(tableDescriptor);
    assertThat(plans, contains(
      new SplitNormalizationPlan(regionInfos.get(2), 10),
      new MergeNormalizationPlan.Builder()
        .addTarget(regionInfos.get(0), 1)
        .addTarget(regionInfos.get(1), 1)
        .build()));

    when(tableDescriptor.getValue(MIN_REGION_COUNT_KEY)).thenReturn("4");
    assertThat(normalizer.computePlansForTable(tableDescriptor), contains(
      new SplitNormalizationPlan(regionInfos.get(2), 10)));
  }

  @Test
  public void testHonorsMergeMinRegionAge() {
    conf.setInt(MERGE_MIN_REGION_AGE_DAYS_KEY, 7);
    final TableName tableName = name.getTableName();
    final List<RegionInfo> regionInfos = createRegionInfos(tableName, 4);
    final Map<byte[], Integer> regionSizes =
      createRegionSizesMap(regionInfos, 1, 1, 10, 10);
    setupMocksForNormalizer(regionSizes, regionInfos);
    assertEquals(Period.ofDays(7), normalizer.getMergeMinRegionAge());
    assertThat(
      normalizer.computePlansForTable(tableDescriptor),
      everyItem(not(instanceOf(MergeNormalizationPlan.class))));

    // have to call setupMocks again because we don't have dynamic config update on normalizer.
    conf.unset(MERGE_MIN_REGION_AGE_DAYS_KEY);
    setupMocksForNormalizer(regionSizes, regionInfos);
    assertEquals(
      Period.ofDays(DEFAULT_MERGE_MIN_REGION_AGE_DAYS), normalizer.getMergeMinRegionAge());
    final List<NormalizationPlan> plans = normalizer.computePlansForTable(tableDescriptor);
    assertThat(plans, not(empty()));
    assertThat(plans, everyItem(instanceOf(MergeNormalizationPlan.class)));
  }

  @Test
  public void testHonorsMergeMinRegionAgeInTD() {
    conf.setInt(MERGE_MIN_REGION_AGE_DAYS_KEY, 7);
    final TableName tableName = name.getTableName();
    final List<RegionInfo> regionInfos = createRegionInfos(tableName, 4);
    final Map<byte[], Integer> regionSizes =
      createRegionSizesMap(regionInfos, 1, 1, 10, 10);
    setupMocksForNormalizer(regionSizes, regionInfos);
    assertEquals(Period.ofDays(7), normalizer.getMergeMinRegionAge());
    assertThat(
      normalizer.computePlansForTable(tableDescriptor),
      everyItem(not(instanceOf(MergeNormalizationPlan.class))));

    conf.unset(MERGE_MIN_REGION_AGE_DAYS_KEY);
    setupMocksForNormalizer(regionSizes, regionInfos);
    when(tableDescriptor.getValue(MERGE_MIN_REGION_AGE_DAYS_KEY)).thenReturn("-1");
    List<NormalizationPlan> plans = normalizer.computePlansForTable(tableDescriptor);
    assertThat(plans, not(empty()));
    assertThat(plans, everyItem(instanceOf(MergeNormalizationPlan.class)));

    when(tableDescriptor.getValue(MERGE_MIN_REGION_AGE_DAYS_KEY)).thenReturn("5");
    plans = normalizer.computePlansForTable(tableDescriptor);
    assertThat(plans, empty());
    assertThat(plans, everyItem(not(instanceOf(MergeNormalizationPlan.class))));
  }

  @Test
  public void testHonorsMergeMinRegionSize() {
    conf.setBoolean(SPLIT_ENABLED_KEY, false);
    final TableName tableName = name.getTableName();
    final List<RegionInfo> regionInfos = createRegionInfos(tableName, 5);
    final Map<byte[], Integer> regionSizes =
      createRegionSizesMap(regionInfos, 1, 2, 0, 10, 10);
    setupMocksForNormalizer(regionSizes, regionInfos);

    assertFalse(normalizer.isSplitEnabled());
    assertEquals(1, normalizer.getMergeMinRegionSizeMb());
    assertThat(
      normalizer.computePlansForTable(tableDescriptor),
      contains(new MergeNormalizationPlan.Builder()
        .addTarget(regionInfos.get(0), 1)
        .addTarget(regionInfos.get(1), 2)
        .build()));

    conf.setInt(MERGE_MIN_REGION_SIZE_MB_KEY, 3);
    setupMocksForNormalizer(regionSizes, regionInfos);
    assertEquals(3, normalizer.getMergeMinRegionSizeMb());
    assertThat(normalizer.computePlansForTable(tableDescriptor), empty());
  }

  @Test
  public void testHonorsMergeMinRegionSizeInTD() {
    conf.setBoolean(SPLIT_ENABLED_KEY, false);
    final TableName tableName = name.getTableName();
    final List<RegionInfo> regionInfos = createRegionInfos(tableName, 5);
    final Map<byte[], Integer> regionSizes =
      createRegionSizesMap(regionInfos, 1, 2, 0, 10, 10);
    setupMocksForNormalizer(regionSizes, regionInfos);

    assertFalse(normalizer.isSplitEnabled());
    assertEquals(1, normalizer.getMergeMinRegionSizeMb());
    assertThat(
      normalizer.computePlansForTable(tableDescriptor),
      contains(new MergeNormalizationPlan.Builder()
        .addTarget(regionInfos.get(0), 1)
        .addTarget(regionInfos.get(1), 2)
        .build()));

    when(tableDescriptor.getValue(MERGE_MIN_REGION_SIZE_MB_KEY)).thenReturn("3");
    assertThat(normalizer.computePlansForTable(tableDescriptor), empty());
  }

  @Test
  public void testMergeEmptyRegions0() {
    conf.setBoolean(SPLIT_ENABLED_KEY, false);
    conf.setInt(MERGE_MIN_REGION_SIZE_MB_KEY, 0);
    final TableName tableName = name.getTableName();
    final List<RegionInfo> regionInfos = createRegionInfos(tableName, 7);
    final Map<byte[], Integer> regionSizes =
      createRegionSizesMap(regionInfos, 0, 1, 10, 0, 9, 10, 0);
    setupMocksForNormalizer(regionSizes, regionInfos);

    assertFalse(normalizer.isSplitEnabled());
    assertEquals(0, normalizer.getMergeMinRegionSizeMb());
    assertThat(normalizer.computePlansForTable(tableDescriptor), contains(
      new MergeNormalizationPlan.Builder()
        .addTarget(regionInfos.get(0), 0)
        .addTarget(regionInfos.get(1), 1)
        .build(),
      new MergeNormalizationPlan.Builder()
        .addTarget(regionInfos.get(2), 10)
        .addTarget(regionInfos.get(3), 0)
        .build(),
      new MergeNormalizationPlan.Builder()
        .addTarget(regionInfos.get(5), 10)
        .addTarget(regionInfos.get(6), 0)
        .build()));
  }

  @Test
  public void testMergeEmptyRegions1() {
    conf.setBoolean(SPLIT_ENABLED_KEY, false);
    conf.setInt(MERGE_MIN_REGION_SIZE_MB_KEY, 0);
    final TableName tableName = name.getTableName();
    final List<RegionInfo> regionInfos = createRegionInfos(tableName, 8);
    final Map<byte[], Integer> regionSizes =
      createRegionSizesMap(regionInfos, 0, 1, 10, 0, 9, 0, 10, 0);
    setupMocksForNormalizer(regionSizes, regionInfos);

    assertFalse(normalizer.isSplitEnabled());
    assertEquals(0, normalizer.getMergeMinRegionSizeMb());
    assertThat(normalizer.computePlansForTable(tableDescriptor), contains(
      new MergeNormalizationPlan.Builder()
        .addTarget(regionInfos.get(0), 0)
        .addTarget(regionInfos.get(1), 1)
        .build(),
      new MergeNormalizationPlan.Builder()
        .addTarget(regionInfos.get(2), 10)
        .addTarget(regionInfos.get(3), 0)
        .build(),
      new MergeNormalizationPlan.Builder()
        .addTarget(regionInfos.get(4), 9)
        .addTarget(regionInfos.get(5), 0)
        .build(),
      new MergeNormalizationPlan.Builder()
        .addTarget(regionInfos.get(6), 10)
        .addTarget(regionInfos.get(7), 0)
        .build()));
  }

  @Test
  public void testMergeEmptyRegions2() {
    conf.setBoolean(SPLIT_ENABLED_KEY, false);
    conf.setInt(MERGE_MIN_REGION_SIZE_MB_KEY, 0);
    final TableName tableName = name.getTableName();
    final List<RegionInfo> regionInfos = createRegionInfos(tableName, 8);
    final Map<byte[], Integer> regionSizes =
      createRegionSizesMap(regionInfos, 0, 10, 1, 0, 9, 0, 10, 0);
    setupMocksForNormalizer(regionSizes, regionInfos);

    assertFalse(normalizer.isSplitEnabled());
    assertEquals(0, normalizer.getMergeMinRegionSizeMb());
    List<NormalizationPlan> plans = normalizer.computePlansForTable(tableDescriptor);
    assertThat(plans, contains(
      new MergeNormalizationPlan.Builder()
        .addTarget(regionInfos.get(0), 0)
        .addTarget(regionInfos.get(1), 10)
        .build(),
      new MergeNormalizationPlan.Builder()
        .addTarget(regionInfos.get(2), 1)
        .addTarget(regionInfos.get(3), 0)
        .build(),
      new MergeNormalizationPlan.Builder()
        .addTarget(regionInfos.get(4), 9)
        .addTarget(regionInfos.get(5), 0)
        .build(),
      new MergeNormalizationPlan.Builder()
        .addTarget(regionInfos.get(6), 10)
        .addTarget(regionInfos.get(7), 0)
        .build()));
  }

  @Test
  public void testSplitAndMultiMerge() {
    conf.setInt(MERGE_MIN_REGION_SIZE_MB_KEY, 0);
    final TableName tableName = name.getTableName();
    final List<RegionInfo> regionInfos = createRegionInfos(tableName, 8);
    final Map<byte[], Integer> regionSizes =
      createRegionSizesMap(regionInfos, 3, 1, 1, 30, 9, 3, 1, 0);
    setupMocksForNormalizer(regionSizes, regionInfos);

    assertTrue(normalizer.isMergeEnabled());
    assertTrue(normalizer.isSplitEnabled());
    assertEquals(0, normalizer.getMergeMinRegionSizeMb());
    assertThat(normalizer.computePlansForTable(tableDescriptor), contains(
      new SplitNormalizationPlan(regionInfos.get(3), 30),
      new MergeNormalizationPlan.Builder()
        .addTarget(regionInfos.get(0), 3)
        .addTarget(regionInfos.get(1), 1)
        .addTarget(regionInfos.get(2), 1)
        .build(),
      new MergeNormalizationPlan.Builder()
        .addTarget(regionInfos.get(5), 3)
        .addTarget(regionInfos.get(6), 1)
        .addTarget(regionInfos.get(7), 0)
        .build()));
  }

  // This test is to make sure that normalizer is only going to merge adjacent regions.
  @Test
  public void testNormalizerCannotMergeNonAdjacentRegions() {
    final TableName tableName = name.getTableName();
    // create 5 regions with sizes to trigger merge of small regions. region ranges are:
    // [, "aa"), ["aa", "aa1"), ["aa1", "aa1!"), ["aa1!", "aa2"), ["aa2", )
    // Region ["aa", "aa1") and ["aa1!", "aa2") are not adjacent, they are not supposed to
    // merged.
    final byte[][] keys = {
      null,
      Bytes.toBytes("aa"),
      Bytes.toBytes("aa1!"),
      Bytes.toBytes("aa1"),
      Bytes.toBytes("aa2"),
      null,
    };
    final List<RegionInfo> regionInfos = createRegionInfos(tableName, keys);
    final Map<byte[], Integer> regionSizes =
      createRegionSizesMap(regionInfos, 3, 1, 1, 3, 5);
    setupMocksForNormalizer(regionSizes, regionInfos);

    // Compute the plan, no merge plan returned as they are not adjacent.
    List<NormalizationPlan> plans = normalizer.computePlansForTable(tableDescriptor);
    assertThat(plans, empty());
  }

  @SuppressWarnings("MockitoCast")
  private void setupMocksForNormalizer(Map<byte[], Integer> regionSizes,
    List<RegionInfo> regionInfoList) {
    masterServices = Mockito.mock(MasterServices.class, RETURNS_DEEP_STUBS);
    tableDescriptor = Mockito.mock(TableDescriptor.class, RETURNS_DEEP_STUBS);

    // for simplicity all regions are assumed to be on one server; doesn't matter to us
    ServerName sn = ServerName.valueOf("localhost", 0, 0L);
    when(masterServices.getAssignmentManager().getRegionStates()
      .getRegionsOfTable(any())).thenReturn(regionInfoList);
    when(masterServices.getAssignmentManager().getRegionStates()
      .getRegionServerOfRegion(any())).thenReturn(sn);
    when(masterServices.getAssignmentManager().getRegionStates()
      .getRegionState(any(RegionInfo.class))).thenReturn(
        RegionState.createForTesting(null, RegionState.State.OPEN));

    for (Map.Entry<byte[], Integer> region : regionSizes.entrySet()) {
      RegionMetrics regionLoad = Mockito.mock(RegionMetrics.class);
      when(regionLoad.getRegionName()).thenReturn(region.getKey());
      when(regionLoad.getStoreFileSize())
        .thenReturn(new Size(region.getValue(), Size.Unit.MEGABYTE));

      // this is possibly broken with jdk9, unclear if false positive or not
      // suppress it for now, fix it when we get to running tests on 9
      // see: http://errorprone.info/bugpattern/MockitoCast
      when((Object) masterServices.getServerManager().getLoad(sn)
        .getRegionMetrics().get(region.getKey())).thenReturn(regionLoad);
    }

    when(masterServices.isSplitOrMergeEnabled(any())).thenReturn(true);
    when(tableDescriptor.getTableName()).thenReturn(name.getTableName());

    normalizer = new SimpleRegionNormalizer();
    normalizer.setConf(conf);
    normalizer.setMasterServices(masterServices);
  }

  /**
   * Create a list of {@link RegionInfo}s that represent a region chain of the specified length.
   */
  private static List<RegionInfo> createRegionInfos(final TableName tableName, final int length) {
    if (length < 1) {
      throw new IllegalStateException("length must be greater than or equal to 1.");
    }

    final byte[] startKey = Bytes.toBytes("aaaaa");
    final byte[] endKey = Bytes.toBytes("zzzzz");
    if (length == 1) {
      return Collections.singletonList(createRegionInfo(tableName, startKey, endKey));
    }

    final byte[][] splitKeys = Bytes.split(startKey, endKey, length - 1);
    final List<RegionInfo> ret = new ArrayList<>(length);
    for (int i = 0; i < splitKeys.length - 1; i++) {
      ret.add(createRegionInfo(tableName, splitKeys[i], splitKeys[i+1]));
    }
    return ret;
  }

  private static RegionInfo createRegionInfo(final TableName tableName, final byte[] startKey,
    final byte[] endKey) {
    return RegionInfoBuilder.newBuilder(tableName)
      .setStartKey(startKey)
      .setEndKey(endKey)
      .setRegionId(generateRegionId())
      .build();
  }

  private static long generateRegionId() {
    return Instant.ofEpochMilli(EnvironmentEdgeManager.currentTime())
      .minus(Period.ofDays(DEFAULT_MERGE_MIN_REGION_AGE_DAYS + 1))
      .toEpochMilli();
  }

  private static List<RegionInfo> createRegionInfos(final TableName tableName,
    final byte[][] splitKeys) {
    final List<RegionInfo> ret = new ArrayList<>(splitKeys.length);
    for (int i = 0; i < splitKeys.length - 1; i++) {
      ret.add(createRegionInfo(tableName, splitKeys[i], splitKeys[i+1]));
    }
    return ret;
  }

  private static Map<byte[], Integer> createRegionSizesMap(final List<RegionInfo> regionInfos,
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
