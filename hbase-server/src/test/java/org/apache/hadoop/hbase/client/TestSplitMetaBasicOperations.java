/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CatalogFamilyFormat;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.CatalogAccessor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hbase.thirdparty.com.google.common.collect.Iterators;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Category(MediumTests.class)
public class TestSplitMetaBasicOperations {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSplitMetaBasicOperations.class);

  static Configuration conf = HBaseConfiguration.create();
  private static final Logger LOG = LoggerFactory.getLogger(TestSplitMetaBasicOperations.class);

  /**
   * Tests basic split and merge operations on meta and user tables
   * as well as tests that the tables are still accessible using basic operations.
   * Then performs some tests which verify that the proper comparator is used
   * when meta row keys are involved as the meta row key is broken up into
   * 3 parts and lexicographical ordering is performed on each one individually.
   * Given that the ',' is the delimiter for these 3 parts we have chosen split keys
   * which have table splits keys that are lexicographically smaller than ','
   * in certain scenarios to verify that the proper comparator is used.
   */
  @Test(timeout = 120000)
  public void testBasicSplitMerge() throws Exception {
    final TableName tableName = TableName.valueOf("testSplitAtSplitPoint");
    conf = HBaseConfiguration.create();
    conf.set("hbase.client.retries.by.server", "10");
    conf.set("hbase.client.retries.number", "10");
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(conf);
    try {
      TEST_UTIL.startMiniCluster(1, 3);
      Connection conn = TEST_UTIL.getConnection();
      MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
      cluster.waitForActiveAndReadyMaster();

      TableDescriptor desc =
        TableDescriptorBuilder.newBuilder(tableName)
          .setColumnFamily(ColumnFamilyDescriptorBuilder
            .newBuilder(Bytes.toBytes("cf"))
            .setBlocksize(30)
            .build()).build();

      byte[][] splits = {{0x02},{0x03},{0x04},{0x05}};
      final Admin hbaseAdmin = conn.getAdmin();
      hbaseAdmin.createTable(desc, splits);

      List<Result> tableRegionList = CatalogAccessor.fullScanRegions(TEST_UTIL.getConnection());
      LOG.info("Splitting meta");
      List<HRegionLocation> tableRegionLocations =
        conn.getTable(tableName).getRegionLocator().getAllRegionLocations();

//      //Test malformed split key
//      try {
//        splitTable(TEST_UTIL, TableName.META_TABLE_NAME, Bytes.toBytesBinary("5555"));
//        fail("Expected malformed split key related exception");
//      } catch (Exception ex) {
//      }
//
//      //Test malformed split key, empty table name
//      try {
//;        splitTable(TEST_UTIL, TableName.META_TABLE_NAME, Bytes.toBytesBinary(",,123"));
//        fail("Expected malformed split key related exception");
//      } catch (Exception ex) {
//      }
//
//      //Test malformed split key, empty id component
//      try {
//        splitTable(TEST_UTIL, TableName.META_TABLE_NAME, Bytes.toBytesBinary("123,,"));
//        fail("Expected malformed split key related exception");
//      } catch (Exception ex) {
//      }

      splitTable(TEST_UTIL, TableName.META_TABLE_NAME,
        Iterators.get(tableRegionLocations.iterator(), 0).getRegion().getRegionName());

      TEST_UTIL.flush(TableName.ROOT_TABLE_NAME);
      for(Result r : conn.getTable(TableName.ROOT_TABLE_NAME).getScanner(new Scan())) {
        LOG.debug("hbase:root-->"+r);
      }

      // Root should have two entries as meta got split in 2
      List<RegionInfo> regions =
        CatalogAccessor.getTableRegions(
          conn,
          TableName.META_TABLE_NAME,
          true);
      assertEquals(regions.toString(), 2, regions.size());

      TEST_UTIL.flush(TableName.ROOT_TABLE_NAME);
      for(Result r : conn.getTable(TableName.ROOT_TABLE_NAME).getScanner(new Scan())) {
        LOG.debug("hbase:root-->"+r);
      }
      final Table table = conn.getTable(TableName.META_TABLE_NAME);
        table.getRegionLocator().getRegionLocation(
          Iterators.get(tableRegionLocations.iterator(), 1).getRegion().getRegionName(),
          true).getRegion();

      splitTable(TEST_UTIL, TableName.META_TABLE_NAME,
        Iterators.get(tableRegionLocations.iterator(), 1).getRegion().getRegionName());

      splitTable(TEST_UTIL, TableName.META_TABLE_NAME,
        Iterators.get(tableRegionLocations.iterator(), 2).getRegion().getRegionName());

      assertEquals(4,
        CatalogAccessor.getTableRegions(
          TEST_UTIL.getConnection(),
          TableName.META_TABLE_NAME,
          true).size());

      checkBasicOps(conn, tableName, tableRegionList);

      ResultScanner resultScanner =
        TEST_UTIL.getConnection().getTable(TableName.ROOT_TABLE_NAME).getScanner(new Scan());

      splitTable(TEST_UTIL, tableName, new byte[]{0x02, 0x00});

      //Always compact the tables before running a catalog scan
      //to make sure everything can be cleaned up
      TEST_UTIL.compact(tableName, true);
      TEST_UTIL.compact(TableName.META_TABLE_NAME, true);
      archiveStores(TEST_UTIL, TableName.META_TABLE_NAME);
      hbaseAdmin.runCatalogJanitor();
      //TODO need to see why we have to wait for catalog janitor to run
      Thread.sleep(1000);

      tableRegionList = CatalogAccessor.fullScanRegions(TEST_UTIL.getConnection());
      List<RegionInfo> regionsList = new ArrayList<>();
      for (Result res : tableRegionList) {
        regionsList.add(CatalogFamilyFormat.getRegionInfo(res));
      }
      assertEquals("Got list :"+regionsList,splits.length + 2, tableRegionList.size());
      checkBasicOps(conn, tableName, tableRegionList);

      mergeRegions(TEST_UTIL, tableName, new byte[]{0x02}, new byte[]{0x02, 0x00});
      TEST_UTIL.compact(tableName, true);
      TEST_UTIL.compact(TableName.META_TABLE_NAME, true);
      archiveStores(TEST_UTIL, TableName.META_TABLE_NAME);
      hbaseAdmin.runCatalogJanitor();

      tableRegionList = CatalogAccessor.fullScanRegions(TEST_UTIL.getConnection());
      assertEquals(splits.length + 1, tableRegionList.size());

      mergeRegions(TEST_UTIL,
        TableName.META_TABLE_NAME,
        Iterators.get(tableRegionLocations.iterator(), 0).getRegion().getRegionName(),
        Iterators.get(tableRegionLocations.iterator(), 1).getRegion().getRegionName());

      checkBasicOps(conn, tableName, tableRegionList);

    } finally {
      TEST_UTIL.deleteTable(tableName);
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  @Test(timeout = 120000)
  public void testSplitNoSplitPoint() throws Exception {
    final TableName tableName = TableName.valueOf("testSplitNoSplitPoint");
    conf = HBaseConfiguration.create();
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(conf);
    Admin hbaseAdmin = null;
    try {
      TEST_UTIL.startMiniCluster(1, 3);
      MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
      cluster.waitForActiveAndReadyMaster();
      hbaseAdmin = TEST_UTIL.getAdmin();

      TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf"))
          .setBlocksize(30).build()).build();
      RegionSplitter.SplitAlgorithm algo = new RegionSplitter.HexStringSplit();
      // Have more splits so entries in meta span more than one block
      byte[][] splits = algo.split(20);
      hbaseAdmin.createTable(desc, splits);
      hbaseAdmin.flush(TableName.META_TABLE_NAME);

      List<Result> list =
        CatalogAccessor.fullScanRegions(TEST_UTIL.getConnection());

      LOG.info("Splitting meta");
      int metaServerIndex =
        TEST_UTIL.getHBaseCluster().getServerWith(
          RegionInfoBuilder.FIRST_META_REGIONINFO.getRegionName());
      HRegionServer server = TEST_UTIL.getHBaseCluster().getRegionServer(metaServerIndex);
      int regionCount = hbaseAdmin.getRegions(server.getServerName()).size();
      hbaseAdmin.split(TableName.META_TABLE_NAME);

      int metaSize =0;
      for (int i = 0; i < 300; i++) {
        LOG.debug("Waiting on region to split");
        metaSize =
          CatalogAccessor.getTableRegions(
            TEST_UTIL.getConnection(),
            TableName.META_TABLE_NAME,
            true).size();
        if (metaSize > 1) {
          break;
        }
        Thread.sleep(1000);
      }

      assertEquals(2, metaSize);
      LOG.info("Splitting done");
      checkBasicOps(TEST_UTIL.getConnection(), tableName, list);
    } finally {
      if (hbaseAdmin != null) {
        hbaseAdmin.disableTable(tableName);
        hbaseAdmin.deleteTable(tableName);
      }
      TEST_UTIL.shutdownMiniCluster();
    }
  }


  private void checkBasicOps(Connection conn, TableName tableName,
    List<Result> expectedList) throws Exception {
    LOG.info("Splitting done");

    // Scan meta after split
    Table table = conn.getTable(TableName.META_TABLE_NAME);
    Scan s = new Scan().withStartRow(HConstants.EMPTY_START_ROW)
      .addFamily(HConstants.CATALOG_FAMILY);
    ResultScanner scanner = table.getScanner(s);
    Result r = scanner.next();
    int i = 0;
    while (r != null) {
      assertEquals(CatalogFamilyFormat.getRegionInfo(r),
        CatalogFamilyFormat.getRegionInfo(expectedList.get(i)));
      r = scanner.next();
      i++;
    }

    table = conn.getTable(tableName);
    // try adding/retrieving a row to a user region referenced by the first meta
    byte[] rowKey = Bytes.toBytes("f0000000");
    byte[] family = Bytes.toBytes("cf");
    Put put = new Put(rowKey);
    put.addColumn(family, Bytes.toBytes("A"), Bytes.toBytes("1"));
    table.put(put);
    Get get = new Get(rowKey);
    Result result = table.get(get);
    assertTrue("Column A value should be 1",
      Bytes.toString(result.getValue(family, Bytes.toBytes("A"))).equals("1"));

    // try adding/retrieving a row to a user region referenced by the second meta
    rowKey = Bytes.toBytes("10000000");
    family = Bytes.toBytes("cf");
    put = new Put(rowKey);
    put.addColumn(family, Bytes.toBytes("A"), Bytes.toBytes("2"));
    table.put(put);
    get = new Get(rowKey);
    result = conn.getTable(tableName).get(get);
    assertTrue("Column A value should be 2",
      Bytes.toString(result.getValue(family, Bytes.toBytes("A"))).equals("2"));

    rowKey = Bytes.toBytes("f0000000");
    Delete d = new Delete(rowKey);
    table.delete(d);
    assertTrue(table.get(new Get(rowKey)).isEmpty());

    rowKey = Bytes.toBytes("10000000");
    d = new Delete(rowKey);
    table.delete(d);
    assertTrue(table.get(new Get(rowKey)).isEmpty());
  }

  public void splitTable(HBaseTestingUtility util, TableName tableName, final byte[] splitKey)
    throws Exception {
    final Table table = util.getConnection().getTable(tableName);
    final RegionInfo targetRegion =
      table.getRegionLocator().getRegionLocation(splitKey, true).getRegion();

    util.getAdmin().flush(tableName);
    util.compact(tableName, true);
    archiveStores(util, tableName);
    util.getAdmin().split(targetRegion.getTable(), splitKey);

    util.waitFor(60000, new Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        HRegionLocation loc1 =
          table.getRegionLocator().getRegionLocation(targetRegion.getStartKey(), true);
        HRegionLocation loc2 =
          table.getRegionLocator().getRegionLocation(splitKey, true);
        return !loc1.getRegion().getRegionNameAsString().equals(
          loc2.getRegion().getRegionNameAsString());
      }
    });

    //make sure regions are online
    byte[][] keys = {targetRegion.getStartKey(), splitKey};
    for(byte[] el : keys) {
      byte[] key = el;
      if (key.length == 0) {
        key = new byte[]{0x00};
      }
      table.get(new Get(key));
    }
  }


  public void mergeRegions(HBaseTestingUtility util, TableName tableName,
    final byte[] startKey1,
    final byte[] startKey2)
    throws Exception {
    Connection conn = util.getConnection();
    final Table table = conn.getTable(tableName);
    final RegionInfo targetRegion1 =
      table.getRegionLocator().getRegionLocation(startKey1, true).getRegion();
    final RegionInfo targetRegion2 =
      table.getRegionLocator().getRegionLocation(startKey2, true).getRegion();

    Table metaTable = conn.getTable(TableName.ROOT_TABLE_NAME);
    if (!tableName.equals(TableName.META_TABLE_NAME)) {
      metaTable = conn.getTable(TableName.META_TABLE_NAME);
    }

    Scan s = new Scan().withStartRow(HConstants.EMPTY_START_ROW)
      .addFamily(HConstants.CATALOG_FAMILY);
    ResultScanner scanner = metaTable.getScanner(s);
    Result r = scanner.next();
    List<RegionInfo> splitList = new ArrayList<RegionInfo>();
    while (r != null) {
      splitList.add(CatalogFamilyFormat.getRegionInfo(r));
      r = scanner.next();
    }
    scanner.close();

    util.getAdmin().flush(tableName);
    util.compact(tableName, true);
    archiveStores(util, tableName);

    util.getAdmin().mergeRegionsAsync(new byte[][]{targetRegion1.getRegionName(),
      targetRegion2.getRegionName()},false).get();
    util.waitFor(60000, new Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        HRegionLocation loc1 = table.getRegionLocator().getRegionLocation(startKey1, true);
        HRegionLocation loc2 = table.getRegionLocator().getRegionLocation(startKey2, true);
        LOG.debug("loc-->"+Bytes.toStringBinary(startKey1)+"="+loc1+" --- "+Bytes.toStringBinary(startKey2)+"="+loc2);
        return loc1.getRegion().getRegionNameAsString().equals(
          loc2.getRegion().getRegionNameAsString());
      }
    });


    s = new Scan().withStartRow(HConstants.EMPTY_START_ROW).addFamily(HConstants.CATALOG_FAMILY);
    scanner = metaTable.getScanner(s);
    r = scanner.next();
    splitList = new ArrayList<RegionInfo>();
    while (r != null) {
      splitList.add(CatalogFamilyFormat.getRegionInfo(r));
      r = scanner.next();
    }

    //make sure region is online
    byte[] key = startKey1;
    if (key.length == 0) {
      key = new byte[]{0x00};
    }
    table.get(new Get(key));
  }

  private void archiveStores(HBaseTestingUtility util, TableName tableName) throws IOException {
    for (HRegion region : util.getMiniHBaseCluster().getRegions(tableName)) {
      for (HStore store : region.getStores()) {
        store.closeAndArchiveCompactedFiles();
      }
    }
  }
}
