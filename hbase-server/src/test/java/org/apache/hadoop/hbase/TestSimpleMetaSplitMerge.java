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

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, MediumTests.class })
public class TestSimpleMetaSplitMerge {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSimpleMetaSplitMerge.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static byte[] CF = Bytes.toBytes("cf");

  private static byte[] CQ = Bytes.toBytes("cq");

  private static TableDescriptor TD1 = TableDescriptorBuilder.newBuilder(TableName.valueOf("a"))
    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF)).build();

  private static TableDescriptor TD2 = TableDescriptorBuilder.newBuilder(TableName.valueOf("b"))
    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF)).build();

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(3);
    UTIL.getAdmin().createTable(TD1);
    UTIL.getAdmin().createTable(TD2);
    UTIL.waitTableAvailable(TD1.getTableName());
    UTIL.waitTableAvailable(TD2.getTableName());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  private void assertMetaRegionCount(int count) {
    // do not count it from client as it will reset the location cache for meta table
    assertEquals(count, UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
      .getRegionStates().getRegionsOfTable(TableName.META_TABLE_NAME).size());
  }

  private void clearCache(TableName tableName) throws IOException {
    try (RegionLocator locator = UTIL.getConnection().getRegionLocator(tableName)) {
      locator.clearRegionLocationCache();
    }
  }

  private void assertValue(TableName tableName, String row) throws IOException {
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      Result result = table.get(new Get(Bytes.toBytes(row)));
      assertEquals(row, Bytes.toString(result.getValue(CF, CQ)));
    }
  }

  @Test
  public void test() throws Exception {
    try (Table table = UTIL.getConnection().getTable(TD1.getTableName())) {
      table.put(new Put(Bytes.toBytes("row1")).addColumn(CF, CQ, Bytes.toBytes("row1")));
    }
    try (Table table = UTIL.getConnection().getTable(TD2.getTableName())) {
      table.put(new Put(Bytes.toBytes("row2")).addColumn(CF, CQ, Bytes.toBytes("row2")));
    }
    Admin admin = UTIL.getAdmin();
    // split meta
    admin.split(TableName.META_TABLE_NAME, Bytes.toBytes("b"));
    assertMetaRegionCount(2);
    // clear the cache for table 'b'
    clearCache(TD2.getTableName());
    // make sure that we could get the location of the TD2 from the second meta region
    assertValue(TD2.getTableName(), "row2");
    // assert from client side
    List<RegionInfo> regions = admin.getRegions(TableName.META_TABLE_NAME);
    assertEquals(2, regions.size());
    // compact to make sure we can merge
    for (JVMClusterUtil.RegionServerThread t : UTIL.getMiniHBaseCluster()
      .getRegionServerThreads()) {
      for (HRegion r : t.getRegionServer().getOnlineRegionsLocalContext()) {
        if (TableName.isMetaTableName(r.getRegionInfo().getTable())) {
          r.compact(true);
          for (HStore store : r.getStores()) {
            store.closeAndArchiveCompactedFiles();
          }
        }
      }
    }
    // merge the 2 regions back to 1
    admin.mergeRegionsAsync(regions.stream().map(RegionInfo::getRegionName).toArray(byte[][]::new),
      false).get();
    assertMetaRegionCount(1);
    // clear the cache for table 'a' and 'b'
    clearCache(TD1.getTableName());
    clearCache(TD2.getTableName());

    // make sure that we could still get the locations from the new meta region
    assertValue(TD2.getTableName(), "row2");
    assertValue(TD1.getTableName(), "row1");
  }
}
