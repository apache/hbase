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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.when;

/**
 * Tests logic of {@link SimpleRegionNormalizer}.
 */
@Category(SmallTests.class)
public class TestSimpleRegionNormalizer {
  private static final Log LOG = LogFactory.getLog(TestSimpleRegionNormalizer.class);

  private static RegionNormalizer normalizer;

  // mocks
  private static MasterServices masterServices;

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    normalizer = new SimpleRegionNormalizer();
  }

  @Test
  public void testNoNormalizationForMetaTable() throws HBaseIOException {
    TableName testTable = TableName.META_TABLE_NAME;
    List<HRegionInfo> hris = new ArrayList<>();
    Map<byte[], Integer> regionSizes = new HashMap<>();

    setupMocksForNormalizer(regionSizes, hris);
    List<NormalizationPlan> plans = normalizer.computePlanForTable(testTable);
    assertTrue(plans == null);
  }

  @Test
  public void testNoNormalizationIfTooFewRegions() throws HBaseIOException {
    TableName testTable = TableName.valueOf("testSplitOfSmallRegion");
    List<HRegionInfo> hris = new ArrayList<>();
    Map<byte[], Integer> regionSizes = new HashMap<>();

    HRegionInfo hri1 = new HRegionInfo(testTable, Bytes.toBytes("aaa"), Bytes.toBytes("bbb"));
    hris.add(hri1);
    regionSizes.put(hri1.getRegionName(), 10);

    HRegionInfo hri2 = new HRegionInfo(testTable, Bytes.toBytes("bbb"), Bytes.toBytes("ccc"));
    hris.add(hri2);
    regionSizes.put(hri2.getRegionName(), 15);

    setupMocksForNormalizer(regionSizes, hris);
    List<NormalizationPlan> plans = normalizer.computePlanForTable(testTable);
    assertTrue(plans == null);
  }

  @Test
  public void testNoNormalizationOnNormalizedCluster() throws HBaseIOException {
    TableName testTable = TableName.valueOf("testSplitOfSmallRegion");
    List<HRegionInfo> hris = new ArrayList<>();
    Map<byte[], Integer> regionSizes = new HashMap<>();

    HRegionInfo hri1 = new HRegionInfo(testTable, Bytes.toBytes("aaa"), Bytes.toBytes("bbb"));
    hris.add(hri1);
    regionSizes.put(hri1.getRegionName(), 10);

    HRegionInfo hri2 = new HRegionInfo(testTable, Bytes.toBytes("bbb"), Bytes.toBytes("ccc"));
    hris.add(hri2);
    regionSizes.put(hri2.getRegionName(), 15);

    HRegionInfo hri3 = new HRegionInfo(testTable, Bytes.toBytes("ccc"), Bytes.toBytes("ddd"));
    hris.add(hri3);
    regionSizes.put(hri3.getRegionName(), 8);

    HRegionInfo hri4 = new HRegionInfo(testTable, Bytes.toBytes("ddd"), Bytes.toBytes("eee"));
    hris.add(hri4);
    regionSizes.put(hri4.getRegionName(), 10);

    setupMocksForNormalizer(regionSizes, hris);
    List<NormalizationPlan> plans = normalizer.computePlanForTable(testTable);
    assertTrue(plans == null);
  }

  @Test
  public void testMergeOfSmallRegions() throws HBaseIOException {
    TableName testTable = TableName.valueOf("testMergeOfSmallRegions");
    List<HRegionInfo> hris = new ArrayList<>();
    Map<byte[], Integer> regionSizes = new HashMap<>();

    HRegionInfo hri1 = new HRegionInfo(testTable, Bytes.toBytes("aaa"), Bytes.toBytes("bbb"));
    hris.add(hri1);
    regionSizes.put(hri1.getRegionName(), 15);

    HRegionInfo hri2 = new HRegionInfo(testTable, Bytes.toBytes("bbb"), Bytes.toBytes("ccc"));
    hris.add(hri2);
    regionSizes.put(hri2.getRegionName(), 5);

    HRegionInfo hri3 = new HRegionInfo(testTable, Bytes.toBytes("ccc"), Bytes.toBytes("ddd"));
    hris.add(hri3);
    regionSizes.put(hri3.getRegionName(), 5);

    HRegionInfo hri4 = new HRegionInfo(testTable, Bytes.toBytes("ddd"), Bytes.toBytes("eee"));
    hris.add(hri4);
    regionSizes.put(hri4.getRegionName(), 15);

    HRegionInfo hri5 = new HRegionInfo(testTable, Bytes.toBytes("eee"), Bytes.toBytes("fff"));
    hris.add(hri5);
    regionSizes.put(hri5.getRegionName(), 16);

    setupMocksForNormalizer(regionSizes, hris);
    List<NormalizationPlan> plans = normalizer.computePlanForTable(testTable);

    NormalizationPlan plan = plans.get(0);
    assertTrue(plan instanceof MergeNormalizationPlan);
    assertEquals(hri2, ((MergeNormalizationPlan) plan).getFirstRegion());
    assertEquals(hri3, ((MergeNormalizationPlan) plan).getSecondRegion());
  }

  // Test for situation illustrated in HBASE-14867
  @Test
  public void testMergeOfSecondSmallestRegions() throws HBaseIOException {
    TableName testTable = TableName.valueOf("testMergeOfSmallRegions");
    List<HRegionInfo> hris = new ArrayList<>();
    Map<byte[], Integer> regionSizes = new HashMap<>();

    HRegionInfo hri1 = new HRegionInfo(testTable, Bytes.toBytes("aaa"), Bytes.toBytes("bbb"));
    hris.add(hri1);
    regionSizes.put(hri1.getRegionName(), 1);

    HRegionInfo hri2 = new HRegionInfo(testTable, Bytes.toBytes("bbb"), Bytes.toBytes("ccc"));
    hris.add(hri2);
    regionSizes.put(hri2.getRegionName(), 10000);

    HRegionInfo hri3 = new HRegionInfo(testTable, Bytes.toBytes("ccc"), Bytes.toBytes("ddd"));
    hris.add(hri3);
    regionSizes.put(hri3.getRegionName(), 10000);

    HRegionInfo hri4 = new HRegionInfo(testTable, Bytes.toBytes("ddd"), Bytes.toBytes("eee"));
    hris.add(hri4);
    regionSizes.put(hri4.getRegionName(), 10000);

    HRegionInfo hri5 = new HRegionInfo(testTable, Bytes.toBytes("eee"), Bytes.toBytes("fff"));
    hris.add(hri5);
    regionSizes.put(hri5.getRegionName(), 2700);

    HRegionInfo hri6 = new HRegionInfo(testTable, Bytes.toBytes("fff"), Bytes.toBytes("ggg"));
    hris.add(hri6);
    regionSizes.put(hri6.getRegionName(), 2700);

    setupMocksForNormalizer(regionSizes, hris);
    List<NormalizationPlan> plans = normalizer.computePlanForTable(testTable);
    NormalizationPlan plan = plans.get(0);

    assertTrue(plan instanceof MergeNormalizationPlan);
    assertEquals(hri5, ((MergeNormalizationPlan) plan).getFirstRegion());
    assertEquals(hri6, ((MergeNormalizationPlan) plan).getSecondRegion());
  }

  @Test
  public void testMergeOfSmallNonAdjacentRegions() throws HBaseIOException {
    TableName testTable = TableName.valueOf("testMergeOfSmallRegions");
    List<HRegionInfo> hris = new ArrayList<>();
    Map<byte[], Integer> regionSizes = new HashMap<>();

    HRegionInfo hri1 = new HRegionInfo(testTable, Bytes.toBytes("aaa"), Bytes.toBytes("bbb"));
    hris.add(hri1);
    regionSizes.put(hri1.getRegionName(), 15);

    HRegionInfo hri2 = new HRegionInfo(testTable, Bytes.toBytes("bbb"), Bytes.toBytes("ccc"));
    hris.add(hri2);
    regionSizes.put(hri2.getRegionName(), 5);

    HRegionInfo hri3 = new HRegionInfo(testTable, Bytes.toBytes("ccc"), Bytes.toBytes("ddd"));
    hris.add(hri3);
    regionSizes.put(hri3.getRegionName(), 16);

    HRegionInfo hri4 = new HRegionInfo(testTable, Bytes.toBytes("ddd"), Bytes.toBytes("eee"));
    hris.add(hri4);
    regionSizes.put(hri4.getRegionName(), 15);

    HRegionInfo hri5 = new HRegionInfo(testTable, Bytes.toBytes("ddd"), Bytes.toBytes("eee"));
    hris.add(hri4);
    regionSizes.put(hri5.getRegionName(), 5);

    setupMocksForNormalizer(regionSizes, hris);
    List<NormalizationPlan> plans = normalizer.computePlanForTable(testTable);

    assertTrue(plans == null);
  }

  @Test
  public void testSplitOfLargeRegion() throws HBaseIOException {
    TableName testTable = TableName.valueOf("testSplitOfLargeRegion");
    List<HRegionInfo> hris = new ArrayList<>();
    Map<byte[], Integer> regionSizes = new HashMap<>();

    HRegionInfo hri1 = new HRegionInfo(testTable, Bytes.toBytes("aaa"), Bytes.toBytes("bbb"));
    hris.add(hri1);
    regionSizes.put(hri1.getRegionName(), 8);

    HRegionInfo hri2 = new HRegionInfo(testTable, Bytes.toBytes("bbb"), Bytes.toBytes("ccc"));
    hris.add(hri2);
    regionSizes.put(hri2.getRegionName(), 6);

    HRegionInfo hri3 = new HRegionInfo(testTable, Bytes.toBytes("ccc"), Bytes.toBytes("ddd"));
    hris.add(hri3);
    regionSizes.put(hri3.getRegionName(), 10);

    HRegionInfo hri4 = new HRegionInfo(testTable, Bytes.toBytes("ddd"), Bytes.toBytes("eee"));
    hris.add(hri4);
    regionSizes.put(hri4.getRegionName(), 30);

    setupMocksForNormalizer(regionSizes, hris);
    List<NormalizationPlan> plans = normalizer.computePlanForTable(testTable);
    NormalizationPlan plan = plans.get(0);

    assertTrue(plan instanceof SplitNormalizationPlan);
    assertEquals(hri4, ((SplitNormalizationPlan) plan).getRegionInfo());
  }

  protected void setupMocksForNormalizer(Map<byte[], Integer> regionSizes,
                                         List<HRegionInfo> hris) {
    masterServices = Mockito.mock(MasterServices.class, RETURNS_DEEP_STUBS);

    // for simplicity all regions are assumed to be on one server; doesn't matter to us
    ServerName sn = ServerName.valueOf("localhost", -1, 1L);
    when(masterServices.getAssignmentManager().getRegionStates().
      getRegionsOfTable(any(TableName.class))).thenReturn(hris);
    when(masterServices.getAssignmentManager().getRegionStates().
      getRegionServerOfRegion(any(HRegionInfo.class))).thenReturn(sn);

    for (Map.Entry<byte[], Integer> region : regionSizes.entrySet()) {
      RegionLoad regionLoad = Mockito.mock(RegionLoad.class);
      when(regionLoad.getName()).thenReturn(region.getKey());
      when(regionLoad.getStorefileSizeMB()).thenReturn(region.getValue());

      when(masterServices.getServerManager().getLoad(sn).
        getRegionsLoad().get(region.getKey())).thenReturn(regionLoad);
    }

    normalizer.setMasterServices(masterServices);
  }
}
