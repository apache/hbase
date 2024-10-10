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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.ExtendedCellScanner;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that test tags
 */
@Category({ RegionServerTests.class, MediumTests.class })
public class TestTags {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestTags.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestTags.class);

  static boolean useFilter = false;

  private final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  @Rule
  public final TestName TEST_NAME = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt("hfile.format.version", 3);
    conf.setStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
      TestCoprocessorForTags.class.getName());
    TEST_UTIL.startMiniCluster(2);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @After
  public void tearDown() {
    useFilter = false;
  }

  /**
   * Test that we can do reverse scans when writing tags and using DataBlockEncoding. Fails with an
   * exception for PREFIX, DIFF, and FAST_DIFF prior to HBASE-27580
   */
  @Test
  public void testReverseScanWithDBE() throws IOException {
    byte[] family = Bytes.toBytes("0");

    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);

    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      for (DataBlockEncoding encoding : DataBlockEncoding.values()) {
        testReverseScanWithDBE(connection, encoding, family, HConstants.DEFAULT_BLOCKSIZE, 10);
      }
    }
  }

  /**
   * Test that we can do reverse scans when writing tags and using DataBlockEncoding. Fails with an
   * exception for PREFIX, DIFF, and FAST_DIFF
   */
  @Test
  public void testReverseScanWithDBEWhenCurrentBlockUpdates() throws IOException {
    byte[] family = Bytes.toBytes("0");

    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);

    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      for (DataBlockEncoding encoding : DataBlockEncoding.values()) {
        testReverseScanWithDBE(connection, encoding, family, 1024, 30000);
      }
    }
  }

  private void testReverseScanWithDBE(Connection conn, DataBlockEncoding encoding, byte[] family,
    int blockSize, int maxRows) throws IOException {
    LOG.info("Running test with DBE={}", encoding);
    TableName tableName = TableName.valueOf(TEST_NAME.getMethodName() + "-" + encoding);
    TEST_UTIL.createTable(
      TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(ColumnFamilyDescriptorBuilder
        .newBuilder(family).setDataBlockEncoding(encoding).setBlocksize(blockSize).build()).build(),
      null);

    Table table = conn.getTable(tableName);

    byte[] val1 = new byte[10];
    byte[] val2 = new byte[10];
    Bytes.random(val1);
    Bytes.random(val2);

    for (int i = 0; i < maxRows; i++) {
      table.put(new Put(Bytes.toBytes(i)).addColumn(family, Bytes.toBytes(1), val1)
        .addColumn(family, Bytes.toBytes(2), val2).setTTL(600_000));
    }

    TEST_UTIL.flush(table.getName());

    Scan scan = new Scan();
    scan.setReversed(true);

    try (ResultScanner scanner = table.getScanner(scan)) {
      for (int i = maxRows - 1; i >= 0; i--) {
        Result row = scanner.next();
        assertEquals(2, row.size());

        Cell cell1 = row.getColumnLatestCell(family, Bytes.toBytes(1));
        assertTrue(CellUtil.matchingRows(cell1, Bytes.toBytes(i)));
        assertTrue(CellUtil.matchingValue(cell1, val1));

        Cell cell2 = row.getColumnLatestCell(family, Bytes.toBytes(2));
        assertTrue(CellUtil.matchingRows(cell2, Bytes.toBytes(i)));
        assertTrue(CellUtil.matchingValue(cell2, val2));
      }
    }

  }

  @Test
  public void testTags() throws Exception {
    Table table = null;
    try {
      TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
      byte[] fam = Bytes.toBytes("info");
      byte[] row = Bytes.toBytes("rowa");
      // column names
      byte[] qual = Bytes.toBytes("qual");

      byte[] row1 = Bytes.toBytes("rowb");

      byte[] row2 = Bytes.toBytes("rowc");

      TableDescriptor tableDescriptor =
        TableDescriptorBuilder
          .newBuilder(tableName).setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(fam)
            .setBlockCacheEnabled(true).setDataBlockEncoding(DataBlockEncoding.NONE).build())
          .build();
      Admin admin = TEST_UTIL.getAdmin();
      admin.createTable(tableDescriptor);
      byte[] value = Bytes.toBytes("value");
      table = TEST_UTIL.getConnection().getTable(tableName);
      Put put = new Put(row);
      put.addColumn(fam, qual, HConstants.LATEST_TIMESTAMP, value);
      put.setAttribute("visibility", Bytes.toBytes("myTag"));
      table.put(put);
      admin.flush(tableName);
      // We are lacking an API for confirming flush request compaction.
      // Just sleep for a short time. We won't be able to confirm flush
      // completion but the test won't hang now or in the future if
      // default compaction policy causes compaction between flush and
      // when we go to confirm it.
      Thread.sleep(1000);

      Put put1 = new Put(row1);
      byte[] value1 = Bytes.toBytes("1000dfsdf");
      put1.addColumn(fam, qual, HConstants.LATEST_TIMESTAMP, value1);
      // put1.setAttribute("visibility", Bytes.toBytes("myTag3"));
      table.put(put1);
      admin.flush(tableName);
      Thread.sleep(1000);

      Put put2 = new Put(row2);
      byte[] value2 = Bytes.toBytes("1000dfsdf");
      put2.addColumn(fam, qual, HConstants.LATEST_TIMESTAMP, value2);
      put2.setAttribute("visibility", Bytes.toBytes("myTag3"));
      table.put(put2);
      admin.flush(tableName);
      Thread.sleep(1000);

      result(fam, row, qual, row2, table, value, value2, row1, value1);

      admin.compact(tableName);
      while (admin.getCompactionState(tableName) != CompactionState.NONE) {
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
    Table table = null;
    try {
      TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
      byte[] fam = Bytes.toBytes("info");
      byte[] row = Bytes.toBytes("rowa");
      // column names
      byte[] qual = Bytes.toBytes("qual");

      byte[] row1 = Bytes.toBytes("rowb");

      byte[] row2 = Bytes.toBytes("rowc");

      TableDescriptor tableDescriptor =
        TableDescriptorBuilder.newBuilder(tableName)
          .setColumnFamily(
            ColumnFamilyDescriptorBuilder.newBuilder(fam).setBlockCacheEnabled(true).build())
          .build();
      Admin admin = TEST_UTIL.getAdmin();
      admin.createTable(tableDescriptor);

      table = TEST_UTIL.getConnection().getTable(tableName);
      Put put = new Put(row);
      byte[] value = Bytes.toBytes("value");
      put.addColumn(fam, qual, HConstants.LATEST_TIMESTAMP, value);
      table.put(put);
      admin.flush(tableName);
      // We are lacking an API for confirming flush request compaction.
      // Just sleep for a short time. We won't be able to confirm flush
      // completion but the test won't hang now or in the future if
      // default compaction policy causes compaction between flush and
      // when we go to confirm it.
      Thread.sleep(1000);

      Put put1 = new Put(row1);
      byte[] value1 = Bytes.toBytes("1000dfsdf");
      put1.addColumn(fam, qual, HConstants.LATEST_TIMESTAMP, value1);
      table.put(put1);
      admin.flush(tableName);
      Thread.sleep(1000);

      Put put2 = new Put(row2);
      byte[] value2 = Bytes.toBytes("1000dfsdf");
      put2.addColumn(fam, qual, HConstants.LATEST_TIMESTAMP, value2);
      table.put(put2);
      admin.flush(tableName);
      Thread.sleep(1000);

      Scan s = new Scan().withStartRow(row);
      ResultScanner scanner = table.getScanner(s);
      try {
        Result[] next = scanner.next(3);
        for (Result result : next) {
          ExtendedCellScanner cellScanner = result.cellScanner();
          cellScanner.advance();
          ExtendedCell current = cellScanner.current();
          assertEquals(0, current.getTagsLength());
        }
      } finally {
        if (scanner != null) scanner.close();
      }
      admin.compact(tableName);
      while (admin.getCompactionState(tableName) != CompactionState.NONE) {
        Thread.sleep(10);
      }
      s = new Scan().withStartRow(row);
      scanner = table.getScanner(s);
      try {
        Result[] next = scanner.next(3);
        for (Result result : next) {
          ExtendedCellScanner cellScanner = result.cellScanner();
          cellScanner.advance();
          ExtendedCell current = cellScanner.current();
          assertEquals(0, current.getTagsLength());
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
    TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    byte[] fam = Bytes.toBytes("info");
    byte[] row = Bytes.toBytes("rowa");
    // column names
    byte[] qual = Bytes.toBytes("qual");

    byte[] row1 = Bytes.toBytes("rowb");

    byte[] row2 = Bytes.toBytes("rowc");
    byte[] rowd = Bytes.toBytes("rowd");
    byte[] rowe = Bytes.toBytes("rowe");
    Table table = null;
    for (DataBlockEncoding encoding : DataBlockEncoding.values()) {
      TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(fam).setBlockCacheEnabled(true)
          .setDataBlockEncoding(encoding).build())
        .build();
      Admin admin = TEST_UTIL.getAdmin();
      admin.createTable(tableDescriptor);
      try {
        table = TEST_UTIL.getConnection().getTable(tableName);
        Put put = new Put(row);
        byte[] value = Bytes.toBytes("value");
        put.addColumn(fam, qual, HConstants.LATEST_TIMESTAMP, value);
        int bigTagLen = Short.MAX_VALUE - 5;
        put.setAttribute("visibility", new byte[bigTagLen]);
        table.put(put);
        Put put1 = new Put(row1);
        byte[] value1 = Bytes.toBytes("1000dfsdf");
        put1.addColumn(fam, qual, HConstants.LATEST_TIMESTAMP, value1);
        table.put(put1);
        admin.flush(tableName);
        // We are lacking an API for confirming flush request compaction.
        // Just sleep for a short time. We won't be able to confirm flush
        // completion but the test won't hang now or in the future if
        // default compaction policy causes compaction between flush and
        // when we go to confirm it.
        Thread.sleep(1000);

        put1 = new Put(row2);
        value1 = Bytes.toBytes("1000dfsdf");
        put1.addColumn(fam, qual, HConstants.LATEST_TIMESTAMP, value1);
        table.put(put1);
        admin.flush(tableName);
        Thread.sleep(1000);

        Put put2 = new Put(rowd);
        byte[] value2 = Bytes.toBytes("1000dfsdf");
        put2.addColumn(fam, qual, HConstants.LATEST_TIMESTAMP, value2);
        table.put(put2);
        put2 = new Put(rowe);
        value2 = Bytes.toBytes("1000dfsddfdf");
        put2.addColumn(fam, qual, HConstants.LATEST_TIMESTAMP, value2);
        put.setAttribute("visibility", Bytes.toBytes("ram"));
        table.put(put2);
        admin.flush(tableName);
        Thread.sleep(1000);

        TestCoprocessorForTags.checkTagPresence = true;
        Scan s = new Scan().withStartRow(row);
        s.setCaching(1);
        ResultScanner scanner = table.getScanner(s);
        try {
          Result next = null;
          while ((next = scanner.next()) != null) {
            CellScanner cellScanner = next.cellScanner();
            cellScanner.advance();
            Cell current = cellScanner.current();
            if (CellUtil.matchingRows(current, row)) {
              assertEquals(1, TestCoprocessorForTags.tags.size());
              Tag tag = TestCoprocessorForTags.tags.get(0);
              assertEquals(bigTagLen, tag.getValueLength());
            } else {
              assertEquals(0, TestCoprocessorForTags.tags.size());
            }
          }
        } finally {
          if (scanner != null) {
            scanner.close();
          }
          TestCoprocessorForTags.checkTagPresence = false;
        }
        while (admin.getCompactionState(tableName) != CompactionState.NONE) {
          Thread.sleep(10);
        }
        TestCoprocessorForTags.checkTagPresence = true;
        scanner = table.getScanner(s);
        try {
          Result next = null;
          while ((next = scanner.next()) != null) {
            CellScanner cellScanner = next.cellScanner();
            cellScanner.advance();
            Cell current = cellScanner.current();
            if (CellUtil.matchingRows(current, row)) {
              assertEquals(1, TestCoprocessorForTags.tags.size());
              Tag tag = TestCoprocessorForTags.tags.get(0);
              assertEquals(bigTagLen, tag.getValueLength());
            } else {
              assertEquals(0, TestCoprocessorForTags.tags.size());
            }
          }
        } finally {
          if (scanner != null) {
            scanner.close();
          }
          TestCoprocessorForTags.checkTagPresence = false;
        }
      } finally {
        if (table != null) {
          table.close();
        }
        // delete the table
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
      }
    }
  }

  @Test
  public void testTagsWithAppendAndIncrement() throws Exception {
    TableName tableName = TableName.valueOf(TEST_NAME.getMethodName());
    byte[] f = Bytes.toBytes("f");
    byte[] q = Bytes.toBytes("q");
    byte[] row1 = Bytes.toBytes("r1");
    byte[] row2 = Bytes.toBytes("r2");

    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(f)).build();
    TEST_UTIL.getAdmin().createTable(tableDescriptor);

    Table table = null;
    try {
      table = TEST_UTIL.getConnection().getTable(tableName);
      Put put = new Put(row1);
      byte[] v = Bytes.toBytes(2L);
      put.addColumn(f, q, v);
      put.setAttribute("visibility", Bytes.toBytes("tag1"));
      table.put(put);
      Increment increment = new Increment(row1);
      increment.addColumn(f, q, 1L);
      table.increment(increment);
      TestCoprocessorForTags.checkTagPresence = true;
      ResultScanner scanner = table.getScanner(new Scan());
      Result result = scanner.next();
      KeyValue kv = KeyValueUtil.ensureKeyValue((ExtendedCell) result.getColumnLatestCell(f, q));
      List<Tag> tags = TestCoprocessorForTags.tags;
      assertEquals(3L, Bytes.toLong(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength()));
      assertEquals(1, tags.size());
      assertEquals("tag1", Bytes.toString(Tag.cloneValue(tags.get(0))));
      TestCoprocessorForTags.checkTagPresence = false;
      TestCoprocessorForTags.tags = null;

      increment = new Increment(row1);
      increment.add(new KeyValue(row1, f, q, 1234L, v));
      increment.setAttribute("visibility", Bytes.toBytes("tag2"));
      table.increment(increment);
      TestCoprocessorForTags.checkTagPresence = true;
      scanner = table.getScanner(new Scan());
      result = scanner.next();
      kv = KeyValueUtil.ensureKeyValue((ExtendedCell) result.getColumnLatestCell(f, q));
      tags = TestCoprocessorForTags.tags;
      assertEquals(5L, Bytes.toLong(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength()));
      assertEquals(2, tags.size());
      // We cannot assume the ordering of tags
      List<String> tagValues = new ArrayList<>();
      for (Tag tag : tags) {
        tagValues.add(Bytes.toString(Tag.cloneValue(tag)));
      }
      assertTrue(tagValues.contains("tag1"));
      assertTrue(tagValues.contains("tag2"));
      TestCoprocessorForTags.checkTagPresence = false;
      TestCoprocessorForTags.tags = null;

      put = new Put(row2);
      v = Bytes.toBytes(2L);
      put.addColumn(f, q, v);
      table.put(put);
      increment = new Increment(row2);
      increment.add(new KeyValue(row2, f, q, 1234L, v));
      increment.setAttribute("visibility", Bytes.toBytes("tag2"));
      table.increment(increment);
      TestCoprocessorForTags.checkTagPresence = true;
      scanner = table.getScanner(new Scan().withStartRow(row2));
      result = scanner.next();
      kv = KeyValueUtil.ensureKeyValue((ExtendedCell) result.getColumnLatestCell(f, q));
      tags = TestCoprocessorForTags.tags;
      assertEquals(4L, Bytes.toLong(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength()));
      assertEquals(1, tags.size());
      assertEquals("tag2", Bytes.toString(Tag.cloneValue(tags.get(0))));
      TestCoprocessorForTags.checkTagPresence = false;
      TestCoprocessorForTags.tags = null;

      // Test Append
      byte[] row3 = Bytes.toBytes("r3");
      put = new Put(row3);
      put.addColumn(f, q, Bytes.toBytes("a"));
      put.setAttribute("visibility", Bytes.toBytes("tag1"));
      table.put(put);
      Append append = new Append(row3);
      append.addColumn(f, q, Bytes.toBytes("b"));
      table.append(append);
      TestCoprocessorForTags.checkTagPresence = true;
      scanner = table.getScanner(new Scan().withStartRow(row3));
      result = scanner.next();
      kv = KeyValueUtil.ensureKeyValue((ExtendedCell) result.getColumnLatestCell(f, q));
      tags = TestCoprocessorForTags.tags;
      assertEquals(1, tags.size());
      assertEquals("tag1", Bytes.toString(Tag.cloneValue(tags.get(0))));
      TestCoprocessorForTags.checkTagPresence = false;
      TestCoprocessorForTags.tags = null;

      append = new Append(row3);
      append.add(new KeyValue(row3, f, q, 1234L, v));
      append.setAttribute("visibility", Bytes.toBytes("tag2"));
      table.append(append);
      TestCoprocessorForTags.checkTagPresence = true;
      scanner = table.getScanner(new Scan().withStartRow(row3));
      result = scanner.next();
      kv = KeyValueUtil.ensureKeyValue((ExtendedCell) result.getColumnLatestCell(f, q));
      tags = TestCoprocessorForTags.tags;
      assertEquals(2, tags.size());
      // We cannot assume the ordering of tags
      tagValues.clear();
      for (Tag tag : tags) {
        tagValues.add(Bytes.toString(Tag.cloneValue(tag)));
      }
      assertTrue(tagValues.contains("tag1"));
      assertTrue(tagValues.contains("tag2"));
      TestCoprocessorForTags.checkTagPresence = false;
      TestCoprocessorForTags.tags = null;

      byte[] row4 = Bytes.toBytes("r4");
      put = new Put(row4);
      put.addColumn(f, q, Bytes.toBytes("a"));
      table.put(put);
      append = new Append(row4);
      append.add(new KeyValue(row4, f, q, 1234L, v));
      append.setAttribute("visibility", Bytes.toBytes("tag2"));
      table.append(append);
      TestCoprocessorForTags.checkTagPresence = true;
      scanner = table.getScanner(new Scan().withStartRow(row4));
      result = scanner.next();
      kv = KeyValueUtil.ensureKeyValue((ExtendedCell) result.getColumnLatestCell(f, q));
      tags = TestCoprocessorForTags.tags;
      assertEquals(1, tags.size());
      assertEquals("tag2", Bytes.toString(Tag.cloneValue(tags.get(0))));
    } finally {
      TestCoprocessorForTags.checkTagPresence = false;
      TestCoprocessorForTags.tags = null;
      if (table != null) {
        table.close();
      }
    }
  }

  private void result(byte[] fam, byte[] row, byte[] qual, byte[] row2, Table table, byte[] value,
    byte[] value2, byte[] row1, byte[] value1) throws IOException {
    Scan s = new Scan().withStartRow(row);
    // If filters are used this attribute can be specifically check for in
    // filterKV method and
    // kvs can be filtered out if the tags of interest is not found in that kv
    s.setAttribute("visibility", Bytes.toBytes("myTag"));
    ResultScanner scanner = null;
    try {
      scanner = table.getScanner(s);
      Result next = scanner.next();

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
      if (scanner != null) scanner.close();
    }
  }

  public static class TestCoprocessorForTags implements RegionCoprocessor, RegionObserver {

    public static volatile boolean checkTagPresence = false;
    public static List<Tag> tags = null;

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void prePut(final ObserverContext<? extends RegionCoprocessorEnvironment> e,
      final Put put, final WALEdit edit, final Durability durability) throws IOException {
      updateMutationAddingTags(put);
    }

    private void updateMutationAddingTags(final Mutation m) {
      byte[] attribute = m.getAttribute("visibility");
      byte[] cf = null;
      List<Cell> updatedCells = new ArrayList<>();
      if (attribute != null) {
        for (List<? extends Cell> edits : m.getFamilyCellMap().values()) {
          for (Cell cell : edits) {
            KeyValue kv = KeyValueUtil.ensureKeyValue((ExtendedCell) cell);
            if (cf == null) {
              cf = CellUtil.cloneFamily(kv);
            }
            Tag tag = new ArrayBackedTag((byte) 1, attribute);
            List<Tag> tagList = new ArrayList<>();
            tagList.add(tag);

            KeyValue newKV =
              new KeyValue(CellUtil.cloneRow(kv), 0, kv.getRowLength(), CellUtil.cloneFamily(kv), 0,
                kv.getFamilyLength(), CellUtil.cloneQualifier(kv), 0, kv.getQualifierLength(),
                kv.getTimestamp(), KeyValue.Type.codeToType(kv.getTypeByte()),
                CellUtil.cloneValue(kv), 0, kv.getValueLength(), tagList);
            ((List<Cell>) updatedCells).add(newKV);
          }
        }
        m.getFamilyCellMap().remove(cf);
        // Update the family map
        m.getFamilyCellMap().put(cf, updatedCells);
      }
    }

    @Override
    public Result preIncrement(ObserverContext<? extends RegionCoprocessorEnvironment> e,
      Increment increment) throws IOException {
      updateMutationAddingTags(increment);
      return null;
    }

    @Override
    public Result preAppend(ObserverContext<? extends RegionCoprocessorEnvironment> e,
      Append append) throws IOException {
      updateMutationAddingTags(append);
      return null;
    }

    @Override
    public boolean postScannerNext(ObserverContext<? extends RegionCoprocessorEnvironment> e,
      InternalScanner s, List<Result> results, int limit, boolean hasMore) throws IOException {
      if (checkTagPresence) {
        if (results.size() > 0) {
          // Check tag presence in the 1st cell in 1st Result
          Result result = results.get(0);
          ExtendedCellScanner cellScanner = result.cellScanner();
          if (cellScanner.advance()) {
            ExtendedCell cell = cellScanner.current();
            tags = PrivateCellUtil.getTags(cell);
          }
        }
      }
      return hasMore;
    }
  }
}
