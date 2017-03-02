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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Class to test asynchronous region admin operations.
 */
@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncRegionAdminApi extends TestAsyncAdminBase {

  private void createTableWithDefaultConf(TableName TABLENAME) throws Exception {
    HTableDescriptor htd = new HTableDescriptor(TABLENAME);
    HColumnDescriptor hcd = new HColumnDescriptor("value");
    htd.addFamily(hcd);

    admin.createTable(htd, null).get();
  }

  @Test
  public void testCloseRegion() throws Exception {
    TableName TABLENAME = TableName.valueOf("TestHBACloseRegion");
    createTableWithDefaultConf(TABLENAME);

    HRegionInfo info = null;
    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(TABLENAME);
    List<HRegionInfo> onlineRegions = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices());
    for (HRegionInfo regionInfo : onlineRegions) {
      if (!regionInfo.getTable().isSystemTable()) {
        info = regionInfo;
        boolean closed = admin.closeRegionWithEncodedRegionName(regionInfo.getEncodedName(),
          rs.getServerName().getServerName()).get();
        assertTrue(closed);
      }
    }
    boolean isInList = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices()).contains(info);
    long timeout = System.currentTimeMillis() + 10000;
    while ((System.currentTimeMillis() < timeout) && (isInList)) {
      Thread.sleep(100);
      isInList = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices()).contains(info);
    }

    assertFalse("The region should not be present in online regions list.", isInList);
  }

  @Test
  public void testCloseRegionIfInvalidRegionNameIsPassed() throws Exception {
    final String name = "TestHBACloseRegion1";
    byte[] TABLENAME = Bytes.toBytes(name);
    createTableWithDefaultConf(TableName.valueOf(TABLENAME));

    HRegionInfo info = null;
    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(TableName.valueOf(TABLENAME));
    List<HRegionInfo> onlineRegions = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices());
    for (HRegionInfo regionInfo : onlineRegions) {
      if (!regionInfo.isMetaTable()) {
        if (regionInfo.getRegionNameAsString().contains(name)) {
          info = regionInfo;
          boolean catchNotServingException = false;
          try {
            admin.closeRegionWithEncodedRegionName("sample", rs.getServerName().getServerName())
                .get();
          } catch (Exception e) {
            catchNotServingException = true;
            // expected, ignore it
          }
          assertTrue(catchNotServingException);
        }
      }
    }
    onlineRegions = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices());
    assertTrue("The region should be present in online regions list.",
      onlineRegions.contains(info));
  }

  @Test
  public void testCloseRegionWhenServerNameIsNull() throws Exception {
    byte[] TABLENAME = Bytes.toBytes("TestHBACloseRegion3");
    createTableWithDefaultConf(TableName.valueOf(TABLENAME));

    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(TableName.valueOf(TABLENAME));

    try {
      List<HRegionInfo> onlineRegions = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices());
      for (HRegionInfo regionInfo : onlineRegions) {
        if (!regionInfo.isMetaTable()) {
          if (regionInfo.getRegionNameAsString().contains("TestHBACloseRegion3")) {
            admin.closeRegionWithEncodedRegionName(regionInfo.getEncodedName(), null).get();
          }
        }
      }
      fail("The test should throw exception if the servername passed is null.");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testCloseRegionWhenServerNameIsEmpty() throws Exception {
    byte[] TABLENAME = Bytes.toBytes("TestHBACloseRegionWhenServerNameIsEmpty");
    createTableWithDefaultConf(TableName.valueOf(TABLENAME));

    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(TableName.valueOf(TABLENAME));

    try {
      List<HRegionInfo> onlineRegions = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices());
      for (HRegionInfo regionInfo : onlineRegions) {
        if (!regionInfo.isMetaTable()) {
          if (regionInfo.getRegionNameAsString()
              .contains("TestHBACloseRegionWhenServerNameIsEmpty")) {
            admin.closeRegionWithEncodedRegionName(regionInfo.getEncodedName(), " ").get();
          }
        }
      }
      fail("The test should throw exception if the servername passed is empty.");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testCloseRegionWhenEncodedRegionNameIsNotGiven() throws Exception {
    byte[] TABLENAME = Bytes.toBytes("TestHBACloseRegion4");
    createTableWithDefaultConf(TableName.valueOf(TABLENAME));

    HRegionInfo info = null;
    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(TableName.valueOf(TABLENAME));

    List<HRegionInfo> onlineRegions = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices());
    for (HRegionInfo regionInfo : onlineRegions) {
      if (!regionInfo.isMetaTable()) {
        if (regionInfo.getRegionNameAsString().contains("TestHBACloseRegion4")) {
          info = regionInfo;
          boolean catchNotServingException = false;
          try {
            admin.closeRegionWithEncodedRegionName(regionInfo.getRegionNameAsString(),
              rs.getServerName().getServerName()).get();
          } catch (Exception e) {
            // expected, ignore it.
            catchNotServingException = true;
          }
          assertTrue(catchNotServingException);
        }
      }
    }
    onlineRegions = ProtobufUtil.getOnlineRegions(rs.getRSRpcServices());
    assertTrue("The region should be present in online regions list.",
      onlineRegions.contains(info));
  }

  @Test
  public void testGetRegion() throws Exception {
    AsyncHBaseAdmin rawAdmin = (AsyncHBaseAdmin) admin;

    final TableName tableName = TableName.valueOf("testGetRegion");
    LOG.info("Started " + tableName);
    TEST_UTIL.createMultiRegionTable(tableName, HConstants.CATALOG_FAMILY);

    try (RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
      HRegionLocation regionLocation = locator.getRegionLocation(Bytes.toBytes("mmm"));
      HRegionInfo region = regionLocation.getRegionInfo();
      byte[] regionName = region.getRegionName();
      Pair<HRegionInfo, ServerName> pair = rawAdmin.getRegion(regionName).get();
      assertTrue(Bytes.equals(regionName, pair.getFirst().getRegionName()));
      pair = rawAdmin.getRegion(region.getEncodedNameAsBytes()).get();
      assertTrue(Bytes.equals(regionName, pair.getFirst().getRegionName()));
    }
  }

  @Test
  public void testMergeRegions() throws Exception {
    final TableName tableName = TableName.valueOf("testMergeRegions");
    HColumnDescriptor cd = new HColumnDescriptor("d");
    HTableDescriptor td = new HTableDescriptor(tableName);
    td.addFamily(cd);
    byte[][] splitRows = new byte[][] { "3".getBytes(), "6".getBytes() };
    Admin syncAdmin = TEST_UTIL.getAdmin();
    try {
      TEST_UTIL.createTable(td, splitRows);
      TEST_UTIL.waitTableAvailable(tableName);

      List<HRegionInfo> tableRegions;
      HRegionInfo regionA;
      HRegionInfo regionB;

      // merge with full name
      tableRegions = syncAdmin.getTableRegions(tableName);
      assertEquals(3, syncAdmin.getTableRegions(tableName).size());
      regionA = tableRegions.get(0);
      regionB = tableRegions.get(1);
      admin.mergeRegions(regionA.getRegionName(), regionB.getRegionName(), false).get();

      assertEquals(2, syncAdmin.getTableRegions(tableName).size());

      // merge with encoded name
      tableRegions = syncAdmin.getTableRegions(tableName);
      regionA = tableRegions.get(0);
      regionB = tableRegions.get(1);
      admin.mergeRegions(regionA.getRegionName(), regionB.getRegionName(), false).get();

      assertEquals(1, syncAdmin.getTableRegions(tableName).size());
    } finally {
      syncAdmin.disableTable(tableName);
      syncAdmin.deleteTable(tableName);
    }
  }

  @Test
  public void testSplitTable() throws Exception {
    splitTests(TableName.valueOf("testSplitTable"), 3000, false, null);
    splitTests(TableName.valueOf("testSplitTableWithSplitPoint"), 3000, false, Bytes.toBytes("3"));
    splitTests(TableName.valueOf("testSplitRegion"), 3000, true, null);
    splitTests(TableName.valueOf("testSplitRegionWithSplitPoint"), 3000, true, Bytes.toBytes("3"));
  }

  private void splitTests(TableName tableName, int rowCount, boolean isSplitRegion,
      byte[] splitPoint) throws Exception {
    int count = 0;
    // create table
    HColumnDescriptor cd = new HColumnDescriptor("d");
    HTableDescriptor td = new HTableDescriptor(tableName);
    td.addFamily(cd);
    Table table = TEST_UTIL.createTable(td, null);
    TEST_UTIL.waitTableAvailable(tableName);

    List<HRegionInfo> regions = TEST_UTIL.getAdmin().getTableRegions(tableName);
    assertEquals(regions.size(), 1);

    List<Put> puts = new ArrayList<>();
    for (int i = 0; i < rowCount; i++) {
      Put put = new Put(Bytes.toBytes(i));
      put.addColumn(Bytes.toBytes("d"), null, Bytes.toBytes("value" + i));
      puts.add(put);
    }
    table.put(puts);

    if (isSplitRegion) {
      admin.splitRegion(regions.get(0).getRegionName(), splitPoint).get();
    } else {
      if (splitPoint == null) {
        admin.split(tableName).get();
      } else {
        admin.split(tableName, splitPoint).get();
      }
    }

    for (int i = 0; i < 45; i++) {
      try {
        List<HRegionInfo> hRegionInfos = TEST_UTIL.getAdmin().getTableRegions(tableName);
        count = hRegionInfos.size();
        if (count >= 2) {
          break;
        }
        Thread.sleep(1000L);
      } catch (Exception e) {
        LOG.error(e);
      }
    }

    assertEquals(count, 2);
  }

}
