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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, MediumTests.class })
public class TestSplitMerge {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSplitMerge.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_META_OPERATION_TIMEOUT, 1000);
    UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws Exception {
    TableName tableName = TableName.valueOf("SplitMerge");
    byte[] family = Bytes.toBytes("CF");
    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(family)).build();
    UTIL.getAdmin().createTable(td, new byte[][] { Bytes.toBytes(1) });
    UTIL.waitTableAvailable(tableName);
    UTIL.getAdmin().split(tableName, Bytes.toBytes(2));
    UTIL.waitFor(30000, new ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        return UTIL.getMiniHBaseCluster().getRegions(tableName).size() == 3;
      }

      @Override
      public String explainFailure() throws Exception {
        return "Split has not finished yet";
      }
    });
    UTIL.waitUntilNoRegionsInTransition();
    RegionInfo regionA = null;
    RegionInfo regionB = null;
    for (RegionInfo region : UTIL.getAdmin().getRegions(tableName)) {
      if (region.getStartKey().length == 0) {
        regionA = region;
      } else if (Bytes.equals(region.getStartKey(), Bytes.toBytes(1))) {
        regionB = region;
      }
    }
    assertNotNull(regionA);
    assertNotNull(regionB);
    UTIL.getAdmin().mergeRegionsAsync(regionA.getRegionName(), regionB.getRegionName(), false)
      .get(30, TimeUnit.SECONDS);
    assertEquals(2, UTIL.getAdmin().getRegions(tableName).size());

    ServerName expected = UTIL.getMiniHBaseCluster().getRegionServer(0).getServerName();
    assertEquals(expected, UTIL.getConnection().getRegionLocator(tableName)
      .getRegionLocation(Bytes.toBytes(1), true).getServerName());
    try (AsyncConnection asyncConn =
      ConnectionFactory.createAsyncConnection(UTIL.getConfiguration()).get()) {
      assertEquals(expected, asyncConn.getRegionLocator(tableName)
        .getRegionLocation(Bytes.toBytes(1), true).get().getServerName());
    }
  }

  @Test
  public void testMergeRegionOrder() throws Exception {
    int regionCount= 20;

    TableName tableName = TableName.valueOf("MergeRegionOrder");
    byte[] family = Bytes.toBytes("CF");
    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(family)).build();

    byte[][] splitKeys = new byte[regionCount-1][];

    for (int c = 0; c < regionCount-1; c++) {
      splitKeys[c] = Bytes.toBytes(c+1 * 1000);
    }

    UTIL.getAdmin().createTable(td, splitKeys);
    UTIL.waitTableAvailable(tableName);

    List<RegionInfo> regions = UTIL.getAdmin().getRegions(tableName);

    byte[][] regionNames = new byte[regionCount][];
    for (int c = 0; c < regionCount; c++) {
      regionNames[c] = regions.get(c).getRegionName();
    }

    UTIL.getAdmin().mergeRegionsAsync(regionNames, false).get(60, TimeUnit.SECONDS);

    List<RegionInfo> mergedRegions =
        MetaTableAccessor.getTableRegions(UTIL.getConnection(), tableName);

    assertEquals(1, mergedRegions.size());

    RegionInfo mergedRegion = mergedRegions.get(0);

    List<RegionInfo> mergeParentRegions = MetaTableAccessor.getMergeRegions(UTIL.getConnection(),
      mergedRegion.getRegionName());

    assertEquals(mergeParentRegions.size(), regionCount);

    for (int c = 0; c < regionCount - 1; c++) {
      assertTrue(Bytes.compareTo(mergeParentRegions.get(c).getStartKey(),
        mergeParentRegions.get(c + 1).getStartKey()) < 0);
    }
  }
}
