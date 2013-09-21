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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 *  Class that test tags
 */
@Category(MediumTests.class)
public class TestTags {
  static boolean useFilter = false;

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt("hfile.format.version", 3);
    conf.setStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
        TestCoprocessorForTags.class.getName());
    TEST_UTIL.startMiniCluster(1, 2);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @After
  public void tearDown() {
    useFilter = false;
  }

  @Test
  public void testTags() throws Exception {
    HTable table = null;
    try {
      TableName tableName = TableName.valueOf("testTags");
      byte[] fam = Bytes.toBytes("info");
      byte[] row = Bytes.toBytes("rowa");
      // column names
      byte[] qual = Bytes.toBytes("qual");

      byte[] row1 = Bytes.toBytes("rowb");

      byte[] row2 = Bytes.toBytes("rowc");

      HTableDescriptor desc = new HTableDescriptor(tableName);
      HColumnDescriptor colDesc = new HColumnDescriptor(fam);
      colDesc.setBlockCacheEnabled(true);
      // colDesc.setDataBlockEncoding(DataBlockEncoding.NONE);
      colDesc.setDataBlockEncoding(DataBlockEncoding.PREFIX_TREE);
      desc.addFamily(colDesc);
      HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
      admin.createTable(desc);
      byte[] value = Bytes.toBytes("value");
      table = new HTable(TEST_UTIL.getConfiguration(), tableName);
      Put put = new Put(row);
      put.add(fam, qual, HConstants.LATEST_TIMESTAMP, value);
      put.setAttribute("visibility", Bytes.toBytes("myTag"));
      table.put(put);
      admin.flush(tableName.getName());
      List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(tableName.getName());
      for(HRegion region : regions) {
        Store store = region.getStore(fam);
        while(!(store.getStorefilesCount() > 0)) {
          Thread.sleep(10);
        }
      }

      Put put1 = new Put(row1);
      byte[] value1 = Bytes.toBytes("1000dfsdf");
      put1.add(fam, qual, HConstants.LATEST_TIMESTAMP, value1);
      // put1.setAttribute("visibility", Bytes.toBytes("myTag3"));
      table.put(put1);
      admin.flush(tableName.getName());
      regions = TEST_UTIL.getHBaseCluster().getRegions(tableName.getName());
      for(HRegion region : regions) {
        Store store = region.getStore(fam);
        while(!(store.getStorefilesCount() > 1)) {
          Thread.sleep(10);
        }
      }

      Put put2 = new Put(row2);
      byte[] value2 = Bytes.toBytes("1000dfsdf");
      put2.add(fam, qual, HConstants.LATEST_TIMESTAMP, value2);
      put2.setAttribute("visibility", Bytes.toBytes("myTag3"));
      table.put(put2);

      admin.flush(tableName.getName());
      regions = TEST_UTIL.getHBaseCluster().getRegions(tableName.getName());
      for(HRegion region : regions) {
        Store store = region.getStore(fam);
        while(!(store.getStorefilesCount() > 2)) {
          Thread.sleep(10);
        }
      }
      result(fam, row, qual, row2, table, value, value2, row1, value1);
      admin.compact(tableName.getName());
      while(admin.getCompactionState(tableName.getName()) != CompactionState.NONE) {
        Thread.sleep(10);
      }
      result(fam, row, qual, row2, table, value, value2, row1, value1);
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  @Test
  public void testFlushAndCompactionWithoutTags() throws Exception {
    HTable table = null;
    try {
      TableName tableName = TableName.valueOf("testFlushAndCompactionWithoutTags");
      byte[] fam = Bytes.toBytes("info");
      byte[] row = Bytes.toBytes("rowa");
      // column names
      byte[] qual = Bytes.toBytes("qual");

      byte[] row1 = Bytes.toBytes("rowb");

      byte[] row2 = Bytes.toBytes("rowc");

      HTableDescriptor desc = new HTableDescriptor(tableName);
      HColumnDescriptor colDesc = new HColumnDescriptor(fam);
      colDesc.setBlockCacheEnabled(true);
      // colDesc.setDataBlockEncoding(DataBlockEncoding.NONE);
      colDesc.setDataBlockEncoding(DataBlockEncoding.PREFIX_TREE);
      desc.addFamily(colDesc);
      HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
      admin.createTable(desc);

      table = new HTable(TEST_UTIL.getConfiguration(), tableName);
      Put put = new Put(row);
      byte[] value = Bytes.toBytes("value");
      put.add(fam, qual, HConstants.LATEST_TIMESTAMP, value);
      table.put(put);
      admin.flush(tableName.getName());
      List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(tableName.getName());
      for(HRegion region : regions) {
        Store store = region.getStore(fam);
        while(!(store.getStorefilesCount() > 0)) {
          Thread.sleep(10);
        }
      }

      Put put1 = new Put(row1);
      byte[] value1 = Bytes.toBytes("1000dfsdf");
      put1.add(fam, qual, HConstants.LATEST_TIMESTAMP, value1);
      table.put(put1);
      admin.flush(tableName.getName());
      regions = TEST_UTIL.getHBaseCluster().getRegions(tableName.getName());
      for(HRegion region : regions) {
        Store store = region.getStore(fam);
        while(!(store.getStorefilesCount() > 1)) {
          Thread.sleep(10);
        }
      }

      Put put2 = new Put(row2);
      byte[] value2 = Bytes.toBytes("1000dfsdf");
      put2.add(fam, qual, HConstants.LATEST_TIMESTAMP, value2);
      table.put(put2);

      admin.flush(tableName.getName());
      regions = TEST_UTIL.getHBaseCluster().getRegions(tableName.getName());
      for(HRegion region : regions) {
        Store store = region.getStore(fam);
        while(!(store.getStorefilesCount() > 2)) {
          Thread.sleep(10);
        }
      }
      Scan s = new Scan(row);
      ResultScanner scanner = table.getScanner(s);
      try {
        Result[] next = scanner.next(3);
        for (Result result : next) {
          CellScanner cellScanner = result.cellScanner();
          boolean advance = cellScanner.advance();
          KeyValue current = (KeyValue) cellScanner.current();
          assertTrue(current.getValueOffset() + current.getValueLength() == current.getLength());
        }
      } finally {
        if (scanner != null)
          scanner.close();
      }
      admin.compact(tableName.getName());
      while(admin.getCompactionState(tableName.getName()) != CompactionState.NONE) {
        Thread.sleep(10);
      }
      s = new Scan(row);
      scanner = table.getScanner(s);
      try {
        Result[] next = scanner.next(3);
        for (Result result : next) {
          CellScanner cellScanner = result.cellScanner();
          boolean advance = cellScanner.advance();
          KeyValue current = (KeyValue) cellScanner.current();
          assertTrue(current.getValueOffset() + current.getValueLength() == current.getLength());
        }
      } finally {
        if (scanner != null) {
          scanner.close();
        }
      }
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }
  @Test
  public void testFlushAndCompactionwithCombinations() throws Exception {
    HTable table = null;
    try {
      TableName tableName = TableName.valueOf("testFlushAndCompactionwithCombinations");
      byte[] fam = Bytes.toBytes("info");
      byte[] row = Bytes.toBytes("rowa");
      // column names
      byte[] qual = Bytes.toBytes("qual");

      byte[] row1 = Bytes.toBytes("rowb");

      byte[] row2 = Bytes.toBytes("rowc");
      byte[] rowd = Bytes.toBytes("rowd");
      byte[] rowe = Bytes.toBytes("rowe");

      HTableDescriptor desc = new HTableDescriptor(tableName);
      HColumnDescriptor colDesc = new HColumnDescriptor(fam);
      colDesc.setBlockCacheEnabled(true);
      // colDesc.setDataBlockEncoding(DataBlockEncoding.NONE);
      colDesc.setDataBlockEncoding(DataBlockEncoding.PREFIX_TREE);
      desc.addFamily(colDesc);
      HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
      admin.createTable(desc);

      table = new HTable(TEST_UTIL.getConfiguration(), tableName);
      Put put = new Put(row);
      byte[] value = Bytes.toBytes("value");
      Tag[] tags = new Tag[1];
      tags[0] = new Tag((byte) 1, "ram");
      put.add(fam, qual, HConstants.LATEST_TIMESTAMP, value, tags);
      // put.setAttribute("visibility", Bytes.toBytes("myTag"));
      table.put(put);
      Put put1 = new Put(row1);
      byte[] value1 = Bytes.toBytes("1000dfsdf");
      put1.add(fam, qual, HConstants.LATEST_TIMESTAMP, value1);
      table.put(put1);
      admin.flush(tableName.getName());
      List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(tableName.getName());
      for(HRegion region : regions) {
        Store store = region.getStore(fam);
        while(!(store.getStorefilesCount() > 0)) {
          Thread.sleep(10);
        }
      }

      put1 = new Put(row2);
      value1 = Bytes.toBytes("1000dfsdf");
      put1.add(fam, qual, HConstants.LATEST_TIMESTAMP, value1);
      table.put(put1);
      admin.flush(tableName.getName());
      regions = TEST_UTIL.getHBaseCluster().getRegions(tableName.getName());
      for(HRegion region : regions) {
        Store store = region.getStore(fam);
        while(!(store.getStorefilesCount() > 1)) {
          Thread.sleep(10);
        }
      }
      Put put2 = new Put(rowd);
      byte[] value2 = Bytes.toBytes("1000dfsdf");
      put2.add(fam, qual, HConstants.LATEST_TIMESTAMP, value2);
      table.put(put2);
      put2 = new Put(rowe);
      value2 = Bytes.toBytes("1000dfsddfdf");
      put2.add(fam, qual, HConstants.LATEST_TIMESTAMP, value2, tags);
      // put2.setAttribute("visibility", Bytes.toBytes("myTag3"));
      table.put(put2);
      admin.flush(tableName.getName());
      regions = TEST_UTIL.getHBaseCluster().getRegions(tableName.getName());
      for(HRegion region : regions) {
        Store store = region.getStore(fam);
        while(!(store.getStorefilesCount() > 2)) {
          Thread.sleep(10);
        }
      }
      Scan s = new Scan(row);
      ResultScanner scanner = table.getScanner(s);
      try {
        Result[] next = scanner.next(5);
        for (Result result : next) {
          CellScanner cellScanner = result.cellScanner();
          boolean advance = cellScanner.advance();
          KeyValue current = (KeyValue) cellScanner.current();
          // System.out.println(current);
          int tagsLength = current.getTagsLength();
          if (tagsLength == 0) {
            assertTrue(current.getValueOffset() + current.getValueLength() == current.getLength());
          } else {
            // even if taglength is going to be > 0 the byte array would be same
            assertTrue(current.getValueOffset() + current.getValueLength() != current.getLength());
          }
        }
      } finally {
        if (scanner != null) {
          scanner.close();
        }
      }
      while(admin.getCompactionState(tableName.getName()) != CompactionState.NONE) {
        Thread.sleep(10);
      }
      s = new Scan(row);
      scanner = table.getScanner(s);
      try {
        Result[] next = scanner.next(5);
        for (Result result : next) {
          CellScanner cellScanner = result.cellScanner();
          boolean advance = cellScanner.advance();
          KeyValue current = (KeyValue) cellScanner.current();
          // System.out.println(current);
          if (current.getTagsLength() == 0) {
            assertTrue(current.getValueOffset() + current.getValueLength() == current.getLength());
          } else {
            // even if taglength is going to be > 0 the byte array would be same
            assertTrue(current.getValueOffset() + current.getValueLength() != current.getLength());
          }
        }
      } finally {
        if (scanner != null) {
          scanner.close();
        }
      }
    } finally {
      if (table != null) {
        table.close();
      }
    }
  }

  private void result(byte[] fam, byte[] row, byte[] qual, byte[] row2, HTable table, byte[] value,
      byte[] value2, byte[] row1, byte[] value1) throws IOException {
    Scan s = new Scan(row);
    // If filters are used this attribute can be specifically check for in
    // filterKV method and
    // kvs can be filtered out if the tags of interest is not found in that kv
    s.setAttribute("visibility", Bytes.toBytes("myTag"));
    ResultScanner scanner = null;
    try {
      scanner = table.getScanner(s);
      Result next = scanner.next();
      CellScanner cellScanner = next.cellScanner();
      boolean advance = cellScanner.advance();
      KeyValue current = (KeyValue) cellScanner.current();

      assertTrue(Bytes.equals(next.getRow(), row));
      assertTrue(Bytes.equals(next.getValue(fam, qual), value));

      Result next2 = scanner.next();
      assertTrue(next2 != null);
      assertTrue(Bytes.equals(next2.getRow(), row1));
      assertTrue(Bytes.equals(next2.getValue(fam, qual), value1));

      next2 = scanner.next();
      assertTrue(next2 != null);
      assertTrue(Bytes.equals(next2.getRow(), row2));
      assertTrue(Bytes.equals(next2.getValue(fam, qual), value2));

    } finally {
      if (scanner != null)
        scanner.close();
    }
  }

  public static class TestCoprocessorForTags extends BaseRegionObserver {
    @Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e, final Put put,
        final WALEdit edit, final Durability durability) throws IOException {
      byte[] attribute = put.getAttribute("visibility");
      byte[] cf = null;
      List<Cell> updatedCells = new ArrayList<Cell>();
      if (attribute != null) {
        for (List<? extends Cell> edits : put.getFamilyCellMap().values()) {
          for (Cell cell : edits) {
            KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
            if (cf == null) {
              cf = kv.getFamily();
            }
            Tag tag = new Tag((byte) 1, attribute);
            List<Tag> tagList = new ArrayList<Tag>();
            tagList.add(tag);

            KeyValue newKV = new KeyValue(kv.getRow(), 0, kv.getRowLength(), kv.getFamily(), 0,
                kv.getFamilyLength(), kv.getQualifier(), 0, kv.getQualifierLength(),
                kv.getTimestamp(), KeyValue.Type.codeToType(kv.getType()), kv.getValue(), 0,
                kv.getValueLength(), tagList);
            ((List<Cell>) updatedCells).add(newKV);
          }
        }
        // add new set of familymap to the put. Can we update the existing kvs
        // itself
        NavigableMap<byte[], List<? extends Cell>> familyMap = new TreeMap<byte[], List<? extends Cell>>(
            Bytes.BYTES_COMPARATOR);
        put.getFamilyCellMap().remove(cf);
        // Update the family map
        put.getFamilyCellMap().put(cf, updatedCells);
      }
    }
  }
}
