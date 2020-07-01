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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MiscTests.class, SmallTests.class })
public class TestHRegionLocation {

  private static final Logger LOG = LoggerFactory.getLogger(TestHRegionLocation.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHRegionLocation.class);

  /**
   * HRegionLocations are equal if they have the same 'location' -- i.e. host and port -- even if
   * they are carrying different regions. Verify that is indeed the case.
   */
  @Test
  public void testHashAndEqualsCode() {
    ServerName hsa1 = ServerName.valueOf("localhost", 1234, -1L);
    RegionInfo ri = RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).build();
    HRegionLocation hrl1 = new HRegionLocation(ri, hsa1);
    HRegionLocation hrl2 = new HRegionLocation(ri, hsa1);
    assertEquals(hrl1.hashCode(), hrl2.hashCode());
    assertTrue(hrl1.equals(hrl2));
    HRegionLocation hrl3 = new HRegionLocation(ri, hsa1);
    assertNotSame(hrl1, hrl3);
    // They are equal because they have same location even though they are
    // carrying different regions or timestamp.
    assertTrue(hrl1.equals(hrl3));
    ServerName hsa2 = ServerName.valueOf("localhost", 12345, -1L);
    HRegionLocation hrl4 = new HRegionLocation(ri, hsa2);
    // These have same HRI but different locations so should be different.
    assertFalse(hrl3.equals(hrl4));
    HRegionLocation hrl5 =
      new HRegionLocation(hrl4.getRegion(), hrl4.getServerName(), hrl4.getSeqNum() + 1);
    assertTrue(hrl4.equals(hrl5));
  }

  @Test
  public void testToString() {
    ServerName hsa1 = ServerName.valueOf("localhost", 1234, -1L);
    RegionInfo ri = RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).build();
    HRegionLocation hrl1 = new HRegionLocation(ri, hsa1);
    LOG.info(hrl1.toString());
  }

  @SuppressWarnings("SelfComparison")
  @Test
  public void testCompareTo() {
    RegionInfo ri = RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).build();
    ServerName hsa1 = ServerName.valueOf("localhost", 1234, -1L);
    HRegionLocation hsl1 = new HRegionLocation(ri, hsa1);
    ServerName hsa2 = ServerName.valueOf("localhost", 1235, -1L);
    HRegionLocation hsl2 = new HRegionLocation(ri, hsa2);
    assertEquals(0, hsl1.compareTo(hsl1));
    assertEquals(0, hsl2.compareTo(hsl2));
    int compare1 = hsl1.compareTo(hsl2);
    int compare2 = hsl2.compareTo(hsl1);
    assertTrue((compare1 > 0) ? compare2 < 0 : compare2 > 0);
  }
}
