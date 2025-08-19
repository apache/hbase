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
package org.apache.hadoop.hbase.io.hfile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({ IOTests.class, SmallTests.class })
public class TestRowCacheKey {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRowCacheKey.class);

  private static HRegion region1;
  private static HRegion region2;
  private static RegionInfo regionInfo1;

  @BeforeClass
  public static void beforeClass() {
    TableName tableName = TableName.valueOf("table1");

    regionInfo1 = Mockito.mock(RegionInfo.class);
    Mockito.when(regionInfo1.getEncodedName()).thenReturn("region1");
    Mockito.when(regionInfo1.getTable()).thenReturn(tableName);

    region1 = Mockito.mock(HRegion.class);
    Mockito.when(region1.getRegionInfo()).thenReturn(regionInfo1);

    RegionInfo regionInfo2 = Mockito.mock(RegionInfo.class);
    Mockito.when(regionInfo2.getEncodedName()).thenReturn("region2");
    Mockito.when(regionInfo2.getTable()).thenReturn(tableName);

    region2 = Mockito.mock(HRegion.class);
    Mockito.when(region2.getRegionInfo()).thenReturn(regionInfo2);
  }

  @Test
  public void testEquality() {
    RowCacheKey key11 = new RowCacheKey(region1, "row1".getBytes());
    RowCacheKey key12 = new RowCacheKey(region1, "row2".getBytes());
    RowCacheKey key21 = new RowCacheKey(region2, "row1".getBytes());
    RowCacheKey key22 = new RowCacheKey(region2, "row2".getBytes());
    RowCacheKey key11Another = new RowCacheKey(region1, "row1".getBytes());
    assertNotSame(key11, key11Another);

    // Ensure hashCode works well
    assertNotEquals(key11.hashCode(), key12.hashCode());
    assertNotEquals(key11.hashCode(), key21.hashCode());
    assertNotEquals(key11.hashCode(), key22.hashCode());
    assertEquals(key11.hashCode(), key11Another.hashCode());

    // Ensure equals works well
    assertNotEquals(key11, key12);
    assertNotEquals(key11, key21);
    assertNotEquals(key11, key22);
    assertEquals(key11, key11Another);
  }

  @Test
  public void testDifferentRowCacheSeqNum() {
    RowCacheKey key1 = new RowCacheKey(region1, "row1".getBytes());

    HRegion region1Another = Mockito.mock(HRegion.class);
    Mockito.when(region1Another.getRegionInfo()).thenReturn(regionInfo1);
    Mockito.when(region1Another.getRowCacheSeqNum()).thenReturn(1L);
    RowCacheKey key1Another = new RowCacheKey(region1Another, "row1".getBytes());

    assertNotEquals(key1.hashCode(), key1Another.hashCode());
    assertNotEquals(key1, key1Another);
  }

  @Test
  public void testHeapSize() {
    RowCacheKey key;
    long base = RowCacheKey.FIXED_OVERHEAD;

    key = new RowCacheKey(region1, "1".getBytes());
    assertEquals(base + 8, key.heapSize());

    key = new RowCacheKey(region1, "12345678".getBytes());
    assertEquals(base + 8, key.heapSize());

    key = new RowCacheKey(region1, "123456789".getBytes());
    assertEquals(base + 8 * 2, key.heapSize());
  }
}
