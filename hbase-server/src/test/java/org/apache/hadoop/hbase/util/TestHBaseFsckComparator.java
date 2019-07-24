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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.HbckRegionInfo.MetaEntry;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test the comparator used by Hbck.
 */
@Category({MiscTests.class, SmallTests.class})
public class TestHBaseFsckComparator {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHBaseFsckComparator.class);

  TableName table =
      TableName.valueOf("table1");
  TableName table2 =
      TableName.valueOf("table2");
  byte[] keyStart = Bytes.toBytes("");
  byte[] keyA = Bytes.toBytes("A");
  byte[] keyB = Bytes.toBytes("B");
  byte[] keyC = Bytes.toBytes("C");
  byte[] keyEnd = Bytes.toBytes("");

  static HbckRegionInfo genHbckInfo(TableName table, byte[] start, byte[] end, int time) {
    return new HbckRegionInfo(new MetaEntry(new HRegionInfo(table, start, end), null,
        time));
  }

  @Test
  public void testEquals() {
    HbckRegionInfo hi1 = genHbckInfo(table, keyA, keyB, 0);
    HbckRegionInfo hi2 = genHbckInfo(table, keyA, keyB, 0);
    assertEquals(0, HbckRegionInfo.COMPARATOR.compare(hi1, hi2));
    assertEquals(0, HbckRegionInfo.COMPARATOR.compare(hi2, hi1));
  }

  @Test
  public void testEqualsInstance() {
    HbckRegionInfo hi1 = genHbckInfo(table, keyA, keyB, 0);
    HbckRegionInfo hi2 = hi1;
    assertEquals(0, HbckRegionInfo.COMPARATOR.compare(hi1, hi2));
    assertEquals(0, HbckRegionInfo.COMPARATOR.compare(hi2, hi1));
  }

  @Test
  public void testDiffTable() {
    HbckRegionInfo hi1 = genHbckInfo(table, keyA, keyC, 0);
    HbckRegionInfo hi2 = genHbckInfo(table2, keyA, keyC, 0);
    assertTrue(HbckRegionInfo.COMPARATOR.compare(hi1, hi2) < 0);
    assertTrue(HbckRegionInfo.COMPARATOR.compare(hi2, hi1) > 0);
  }

  @Test
  public void testDiffStartKey() {
    HbckRegionInfo hi1 = genHbckInfo(table, keyStart, keyC, 0);
    HbckRegionInfo hi2 = genHbckInfo(table, keyA, keyC, 0);
    assertTrue(HbckRegionInfo.COMPARATOR.compare(hi1, hi2) < 0);
    assertTrue(HbckRegionInfo.COMPARATOR.compare(hi2, hi1) > 0);
  }

  @Test
  public void testDiffEndKey() {
    HbckRegionInfo hi1 = genHbckInfo(table, keyA, keyB, 0);
    HbckRegionInfo hi2 = genHbckInfo(table, keyA, keyC, 0);
    assertTrue(HbckRegionInfo.COMPARATOR.compare(hi1, hi2) < 0);
    assertTrue(HbckRegionInfo.COMPARATOR.compare(hi2, hi1) > 0);
  }

  @Test
  public void testAbsEndKey() {
    HbckRegionInfo hi1 = genHbckInfo(table, keyA, keyC, 0);
    HbckRegionInfo hi2 = genHbckInfo(table, keyA, keyEnd, 0);
    assertTrue(HbckRegionInfo.COMPARATOR.compare(hi1, hi2) < 0);
    assertTrue(HbckRegionInfo.COMPARATOR.compare(hi2, hi1) > 0);
  }
}

