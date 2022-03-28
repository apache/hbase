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
package org.apache.hadoop.hbase.master.janitor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test small utility methods inside {@link MetaFixer}. For cluster tests see {@link TestMetaFixer}
 */
@Category({ MasterTests.class, SmallTests.class })
public class TestMetaFixerNoCluster {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMetaFixerNoCluster.class);
  private static byte[] A = Bytes.toBytes("a");
  private static byte[] B = Bytes.toBytes("b");
  private static byte[] C = Bytes.toBytes("c");
  private static byte[] D = Bytes.toBytes("d");
  private static RegionInfo ALL = RegionInfoBuilder.FIRST_META_REGIONINFO;
  private static RegionInfo _ARI =
    RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).setEndKey(A).build();
  private static RegionInfo _BRI =
    RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).setEndKey(B).build();
  private static RegionInfo ABRI =
    RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).setStartKey(A).setEndKey(B).build();
  private static RegionInfo ACRI = org.apache.hadoop.hbase.client.RegionInfoBuilder
    .newBuilder(TableName.META_TABLE_NAME).setStartKey(A).setEndKey(C).build();
  private static RegionInfo CDRI = org.apache.hadoop.hbase.client.RegionInfoBuilder
    .newBuilder(TableName.META_TABLE_NAME).setStartKey(C).setEndKey(D).build();
  private static RegionInfo ADRI = org.apache.hadoop.hbase.client.RegionInfoBuilder
    .newBuilder(TableName.META_TABLE_NAME).setStartKey(A).setEndKey(D).build();
  private static RegionInfo D_RI = org.apache.hadoop.hbase.client.RegionInfoBuilder
    .newBuilder(TableName.META_TABLE_NAME).setStartKey(D).build();
  private static RegionInfo C_RI = org.apache.hadoop.hbase.client.RegionInfoBuilder
    .newBuilder(TableName.META_TABLE_NAME).setStartKey(C).build();

  @Test
  public void testGetRegionInfoWithLargestEndKey() {
    assertTrue(MetaFixer.getRegionInfoWithLargestEndKey(_ARI, _BRI).equals(_BRI));
    assertTrue(MetaFixer.getRegionInfoWithLargestEndKey(C_RI, D_RI).equals(C_RI));
    assertTrue(MetaFixer.getRegionInfoWithLargestEndKey(ABRI, CDRI).equals(CDRI));
    assertTrue(MetaFixer.getRegionInfoWithLargestEndKey(null, CDRI).equals(CDRI));
    assertTrue(MetaFixer.getRegionInfoWithLargestEndKey(null, null) == null);
  }

  @Test
  public void testIsOverlap() {
    assertTrue(MetaFixer.isOverlap(_BRI, new Pair<RegionInfo, RegionInfo>(ABRI, ACRI)));
    assertFalse(MetaFixer.isOverlap(_ARI, new Pair<RegionInfo, RegionInfo>(C_RI, D_RI)));
    assertTrue(MetaFixer.isOverlap(ADRI, new Pair<RegionInfo, RegionInfo>(CDRI, C_RI)));
    assertFalse(MetaFixer.isOverlap(_BRI, new Pair<RegionInfo, RegionInfo>(CDRI, C_RI)));
  }

  @Test
  public void testCalculateMergesNoAggregation() {
    List<Pair<RegionInfo, RegionInfo>> overlaps = new ArrayList<>();
    overlaps.add(new Pair<RegionInfo, RegionInfo>(_ARI, _BRI));
    overlaps.add(new Pair<RegionInfo, RegionInfo>(C_RI, D_RI));
    List<SortedSet<RegionInfo>> merges = MetaFixer.calculateMerges(10, overlaps);
    assertEquals(2, merges.size());
    assertEquals(2, merges.get(0).size());
    assertEquals(2, merges.get(1).size());
  }

  @Test
  public void testCalculateMergesAggregation() {
    List<Pair<RegionInfo, RegionInfo>> overlaps = new ArrayList<>();
    overlaps.add(new Pair<RegionInfo, RegionInfo>(ALL, D_RI));
    overlaps.add(new Pair<RegionInfo, RegionInfo>(_ARI, _BRI));
    overlaps.add(new Pair<RegionInfo, RegionInfo>(C_RI, D_RI));
    List<SortedSet<RegionInfo>> merges = MetaFixer.calculateMerges(10, overlaps);
    assertEquals(1, merges.size());
    assertEquals(5, merges.get(0).size());
  }

  @Test
  public void testCalculateMergesNoRepeatOfRegionNames() {
    List<Pair<RegionInfo, RegionInfo>> overlaps = new ArrayList<>();
    overlaps.add(new Pair<RegionInfo, RegionInfo>(_BRI, ABRI));
    overlaps.add(new Pair<RegionInfo, RegionInfo>(ABRI, ADRI));
    List<SortedSet<RegionInfo>> merges = MetaFixer.calculateMerges(10, overlaps);
    assertEquals(1, merges.size());
    // There should be three regions to merge, not four.
    assertEquals(3, merges.get(0).size());
  }

  @Test
  public void testCalculateMergesRespectsMax() {
    List<Pair<RegionInfo, RegionInfo>> overlaps = new ArrayList<>();
    overlaps.add(new Pair<RegionInfo, RegionInfo>(_BRI, ABRI));
    overlaps.add(new Pair<RegionInfo, RegionInfo>(ABRI, ADRI));
    overlaps.add(new Pair<RegionInfo, RegionInfo>(C_RI, D_RI));
    List<SortedSet<RegionInfo>> merges = MetaFixer.calculateMerges(3, overlaps);
    assertEquals(2, merges.size());
    // There should be three regions to merge, not four.
    assertEquals(3, merges.get(0).size());
    assertEquals(2, merges.get(1).size());
  }
}
