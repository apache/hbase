/*
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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.when;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos;

@Category({ MasterTests.class, SmallTests.class })
public class TestMergeNormalizer {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMergeNormalizer.class);

  private static final Logger LOG = LoggerFactory.getLogger(MergeNormalizer.class);

  private RegionNormalizer normalizer;

  @Test
  public void testNoNormalizationForMetaTable() throws HBaseIOException {
    TableName testTable = TableName.META_TABLE_NAME;
    List<RegionInfo> hris = new ArrayList<>();
    Map<byte[], Integer> regionSizes = new HashMap<>();

    setupMocksForNormalizer(regionSizes, hris);
    List<NormalizationPlan> plans = normalizer.computePlanForTable(testTable);
    assertNull(plans);
  }

  @Test
  public void testNoNormalizationIfTooFewRegions() throws HBaseIOException {
    TableName testTable = TableName.valueOf("testSplitOfSmallRegion");
    List<RegionInfo> hris = new ArrayList<>();
    Map<byte[], Integer> regionSizes = new HashMap<>();

    RegionInfo hri1 = RegionInfoBuilder.newBuilder(testTable).setStartKey(Bytes.toBytes("aaa"))
        .setEndKey(Bytes.toBytes("bbb")).build();
    regionSizes.put(hri1.getRegionName(), 10);

    RegionInfo hri2 = RegionInfoBuilder.newBuilder(testTable).setStartKey(Bytes.toBytes("bbb"))
        .setEndKey(Bytes.toBytes("ccc")).build();
    hris.add(hri2);
    regionSizes.put(hri2.getRegionName(), 15);

    setupMocksForNormalizer(regionSizes, hris);
    List<NormalizationPlan> plans = normalizer.computePlanForTable(testTable);
    assertNull(plans);
  }

  @Test
  public void testNoNormalizationOnNormalizedCluster() throws HBaseIOException {
    TableName testTable = TableName.valueOf("testSplitOfSmallRegion");
    List<RegionInfo> hris = new ArrayList<>();
    Map<byte[], Integer> regionSizes = new HashMap<>();

    RegionInfo hri1 = RegionInfoBuilder.newBuilder(testTable).setStartKey(Bytes.toBytes("aaa"))
        .setEndKey(Bytes.toBytes("bbb")).build();
    hris.add(hri1);
    regionSizes.put(hri1.getRegionName(), 10);

    RegionInfo hri2 = RegionInfoBuilder.newBuilder(testTable).setStartKey(Bytes.toBytes("bbb"))
        .setEndKey(Bytes.toBytes("ccc")).build();
    hris.add(hri2);
    regionSizes.put(hri2.getRegionName(), 15);

    RegionInfo hri3 = RegionInfoBuilder.newBuilder(testTable).setStartKey(Bytes.toBytes("ccc"))
        .setEndKey(Bytes.toBytes("ddd")).build();
    hris.add(hri3);
    regionSizes.put(hri3.getRegionName(), 8);

    RegionInfo hri4 = RegionInfoBuilder.newBuilder(testTable).setStartKey(Bytes.toBytes("ddd"))
        .setEndKey(Bytes.toBytes("eee")).build();
    hris.add(hri4);
    regionSizes.put(hri4.getRegionName(), 10);

    setupMocksForNormalizer(regionSizes, hris);
    List<NormalizationPlan> plans = normalizer.computePlanForTable(testTable);
    assertNull(plans);
  }

  @Test
  public void testMergeOfSmallRegions() throws HBaseIOException {
    TableName testTable = TableName.valueOf("testMergeOfSmallRegions");
    List<RegionInfo> hris = new ArrayList<>();
    Map<byte[], Integer> regionSizes = new HashMap<>();

    Timestamp currentTime = new Timestamp(System.currentTimeMillis());
    Timestamp threeDaysBefore = new Timestamp(currentTime.getTime() - TimeUnit.DAYS.toMillis(3));

    RegionInfo hri1 = RegionInfoBuilder.newBuilder(testTable).setStartKey(Bytes.toBytes("aaa"))
        .setEndKey(Bytes.toBytes("bbb")).setRegionId(threeDaysBefore.getTime()).build();
    hris.add(hri1);
    regionSizes.put(hri1.getRegionName(), 15);

    RegionInfo hri2 = RegionInfoBuilder.newBuilder(testTable).setStartKey(Bytes.toBytes("bbb"))
        .setEndKey(Bytes.toBytes("ccc")).setRegionId(threeDaysBefore.getTime()).build();
    hris.add(hri2);
    regionSizes.put(hri2.getRegionName(), 5);

    RegionInfo hri3 = RegionInfoBuilder.newBuilder(testTable).setStartKey(Bytes.toBytes("ccc"))
        .setEndKey(Bytes.toBytes("ddd")).setRegionId(threeDaysBefore.getTime()).build();
    hris.add(hri3);
    regionSizes.put(hri3.getRegionName(), 5);

    RegionInfo hri4 = RegionInfoBuilder.newBuilder(testTable).setStartKey(Bytes.toBytes("ddd"))
        .setEndKey(Bytes.toBytes("eee")).setRegionId(threeDaysBefore.getTime()).build();
    hris.add(hri4);
    regionSizes.put(hri4.getRegionName(), 15);

    RegionInfo hri5 = RegionInfoBuilder.newBuilder(testTable).setStartKey(Bytes.toBytes("eee"))
        .setEndKey(Bytes.toBytes("fff")).build();
    hris.add(hri5);
    regionSizes.put(hri5.getRegionName(), 16);

    RegionInfo hri6 = RegionInfoBuilder.newBuilder(testTable).setStartKey(Bytes.toBytes("fff"))
        .setEndKey(Bytes.toBytes("ggg")).setRegionId(threeDaysBefore.getTime()).build();
    hris.add(hri6);
    regionSizes.put(hri6.getRegionName(), 0);

    RegionInfo hri7 = RegionInfoBuilder.newBuilder(testTable).setStartKey(Bytes.toBytes("ggg"))
        .setEndKey(Bytes.toBytes("hhh")).build();
    hris.add(hri7);
    regionSizes.put(hri7.getRegionName(), 0);

    setupMocksForNormalizer(regionSizes, hris);
    List<NormalizationPlan> plans = normalizer.computePlanForTable(testTable);

    NormalizationPlan plan = plans.get(0);
    assertTrue(plan instanceof MergeNormalizationPlan);
    assertEquals(hri2, ((MergeNormalizationPlan) plan).getFirstRegion());
    assertEquals(hri3, ((MergeNormalizationPlan) plan).getSecondRegion());

    // to check last 0 sized regions are merged
    plan = plans.get(1);
    assertEquals(hri6, ((MergeNormalizationPlan) plan).getFirstRegion());
    assertEquals(hri7, ((MergeNormalizationPlan) plan).getSecondRegion());
  }

  @Test
  public void testMergeOfNewSmallRegions() throws HBaseIOException {
    TableName testTable = TableName.valueOf("testMergeOfNewSmallRegions");
    List<RegionInfo> hris = new ArrayList<>();
    Map<byte[], Integer> regionSizes = new HashMap<>();

    RegionInfo hri1 = RegionInfoBuilder.newBuilder(testTable).setStartKey(Bytes.toBytes("aaa"))
        .setEndKey(Bytes.toBytes("bbb")).build();
    hris.add(hri1);
    regionSizes.put(hri1.getRegionName(), 15);

    RegionInfo hri2 = RegionInfoBuilder.newBuilder(testTable).setStartKey(Bytes.toBytes("bbb"))
        .setEndKey(Bytes.toBytes("ccc")).build();
    hris.add(hri2);
    regionSizes.put(hri2.getRegionName(), 5);

    RegionInfo hri3 = RegionInfoBuilder.newBuilder(testTable).setStartKey(Bytes.toBytes("ccc"))
        .setEndKey(Bytes.toBytes("ddd")).build();
    hris.add(hri3);
    regionSizes.put(hri3.getRegionName(), 16);

    RegionInfo hri4 = RegionInfoBuilder.newBuilder(testTable).setStartKey(Bytes.toBytes("ddd"))
        .setEndKey(Bytes.toBytes("eee")).build();
    hris.add(hri4);
    regionSizes.put(hri4.getRegionName(), 15);

    RegionInfo hri5 = RegionInfoBuilder.newBuilder(testTable).setStartKey(Bytes.toBytes("eee"))
        .setEndKey(Bytes.toBytes("fff")).build();
    hris.add(hri4);
    regionSizes.put(hri5.getRegionName(), 5);

    setupMocksForNormalizer(regionSizes, hris);
    List<NormalizationPlan> plans = normalizer.computePlanForTable(testTable);

    assertNull(plans);
  }

  @SuppressWarnings("MockitoCast")
  protected void setupMocksForNormalizer(Map<byte[], Integer> regionSizes,
    List<RegionInfo> RegionInfo) {
    MasterServices masterServices = Mockito.mock(MasterServices.class, RETURNS_DEEP_STUBS);
    MasterRpcServices masterRpcServices = Mockito.mock(MasterRpcServices.class, RETURNS_DEEP_STUBS);

    // for simplicity all regions are assumed to be on one server; doesn't matter to us
    ServerName sn = ServerName.valueOf("localhost", 0, 1L);
    when(masterServices.getAssignmentManager().getRegionStates()
      .getRegionsOfTable(any())).thenReturn(RegionInfo);
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
      when((Object) masterServices.getServerManager().getLoad(sn).getRegionMetrics()
          .get(region.getKey())).thenReturn(regionLoad);
    }
    try {
      when(masterRpcServices.isSplitOrMergeEnabled(any(), any())).thenReturn(
        MasterProtos.IsSplitOrMergeEnabledResponse.newBuilder().setEnabled(true).build());
    } catch (ServiceException se) {
      LOG.debug("error setting isSplitOrMergeEnabled switch", se);
    }

    normalizer = new SimpleRegionNormalizer();
    normalizer.setMasterServices(masterServices);
    normalizer.setMasterRpcServices(masterRpcServices);
  }
}
