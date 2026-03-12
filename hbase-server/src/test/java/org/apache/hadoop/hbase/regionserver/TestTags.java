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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Class that test tags
 */
@org.junit.jupiter.api.Tag(RegionServerTests.TAG)
@org.junit.jupiter.api.Tag(MediumTests.TAG)
public class TestTags {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt("hfile.format.version", 3);
    conf.setStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
      TestCoprocessorForTags.class.getName());
    TEST_UTIL.startMiniCluster(2);
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @AfterEach
  public void tearDown() throws InterruptedException {
    resetCoprocessorFlags();
  }

  private void resetCoprocessorFlags() throws InterruptedException {
    TestCoprocessorForTags.checkTagPresence = false;
    // to avoid data race, here we sleep for a little while before reseting tags
    Thread.sleep(100);
    TestCoprocessorForTags.tags.clear();
  }

  @Test
  public void testTags(TestInfo testInfo) throws Exception {
    TableName tableName = TableName.valueOf(testInfo.getTestMethod().get().getName());
    byte[] fam = Bytes.toBytes("info");
    byte[] row = Bytes.toBytes("rowa");
    // column names
    byte[] qual = Bytes.toBytes("qual");
    byte[] row1 = Bytes.toBytes("rowb");
    byte[] row2 = Bytes.toBytes("rowc");

    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(fam).setBlockCacheEnabled(true)
        .setDataBlockEncoding(DataBlockEncoding.NONE).build())
      .build();
    try (Table table = TEST_UTIL.createTable(tableDescriptor, null)) {
      byte[] value = Bytes.toBytes("value");
      Put put = new Put(row);
      put.addColumn(fam, qual, HConstants.LATEST_TIMESTAMP, value);
      put.setAttribute("visibility", Bytes.toBytes("myTag"));
      table.put(put);
      TEST_UTIL.flush(tableName);

      Put put1 = new Put(row1);
      byte[] value1 = Bytes.toBytes("1000dfsdf");
      put1.addColumn(fam, qual, HConstants.LATEST_TIMESTAMP, value1);
      // put1.setAttribute("visibility", Bytes.toBytes("myTag3"));
      table.put(put1);
      TEST_UTIL.flush(tableName);

      Put put2 = new Put(row2);
      byte[] value2 = Bytes.toBytes("1000dfsdf");
      put2.addColumn(fam, qual, HConstants.LATEST_TIMESTAMP, value2);
      put2.setAttribute("visibility", Bytes.toBytes("myTag3"));
      table.put(put2);
      TEST_UTIL.flush(tableName);

      assertResult(fam, row, qual, row2, table, value, value2, row1, value1);

      TEST_UTIL.compact(tableName, false);
      assertResult(fam, row, qual, row2, table, value, value2, row1, value1);
    }
  }

  @Test
  public void testFlushAndCompactionWithoutTags(TestInfo testInfo) throws Exception {
    TableName tableName = TableName.valueOf(testInfo.getTestMethod().get().getName());
    byte[] fam = Bytes.toBytes("info");
    byte[] row = Bytes.toBytes("rowa");
    // column names
    byte[] qual = Bytes.toBytes("qual");
    byte[] row1 = Bytes.toBytes("rowb");
    byte[] row2 = Bytes.toBytes("rowc");

    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(
      ColumnFamilyDescriptorBuilder.newBuilder(fam).setBlockCacheEnabled(true).build()).build();
    try (Table table = TEST_UTIL.createTable(tableDescriptor, null)) {
      Put put = new Put(row);
      byte[] value = Bytes.toBytes("value");
      put.addColumn(fam, qual, HConstants.LATEST_TIMESTAMP, value);
      table.put(put);
      TEST_UTIL.flush(tableName);

      Put put1 = new Put(row1);
      byte[] value1 = Bytes.toBytes("1000dfsdf");
      put1.addColumn(fam, qual, HConstants.LATEST_TIMESTAMP, value1);
      table.put(put1);
      TEST_UTIL.flush(tableName);

      Put put2 = new Put(row2);
      byte[] value2 = Bytes.toBytes("1000dfsdf");
      put2.addColumn(fam, qual, HConstants.LATEST_TIMESTAMP, value2);
      table.put(put2);
      TEST_UTIL.flush(tableName);

      Scan s = new Scan().withStartRow(row);
      try (ResultScanner scanner = table.getScanner(s)) {
        Result[] next = scanner.next(3);
        for (Result result : next) {
          CellScanner cellScanner = result.cellScanner();
          cellScanner.advance();
          Cell current = cellScanner.current();
          assertEquals(0, current.getTagsLength());
        }
      }

      TEST_UTIL.compact(tableName, false);
      s = new Scan().withStartRow(row);
      try (ResultScanner scanner = table.getScanner(s)) {
        Result[] next = scanner.next(3);
        for (Result result : next) {
          CellScanner cellScanner = result.cellScanner();
          cellScanner.advance();
          Cell current = cellScanner.current();
          assertEquals(0, current.getTagsLength());
        }
      }
    }
  }

  @ParameterizedTest(name = "{index}: DataBlockEncoding = {0}")
  @EnumSource(DataBlockEncoding.class)
  public void testFlushAndCompactionwithCombinations(DataBlockEncoding encoding, TestInfo testInfo)
    throws Exception {
    TableName tableName =
      TableName.valueOf(testInfo.getTestMethod().get().getName() + "-" + encoding.name());
    byte[] fam = Bytes.toBytes("info");
    byte[] row = Bytes.toBytes("rowa");
    // column names
    byte[] qual = Bytes.toBytes("qual");
    byte[] row1 = Bytes.toBytes("rowb");
    byte[] row2 = Bytes.toBytes("rowc");
    byte[] rowd = Bytes.toBytes("rowd");
    byte[] rowe = Bytes.toBytes("rowe");
    TableDescriptor tableDescriptor =
      TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(ColumnFamilyDescriptorBuilder
        .newBuilder(fam).setBlockCacheEnabled(true).setDataBlockEncoding(encoding).build()).build();
    try (Table table = TEST_UTIL.createTable(tableDescriptor, null)) {
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
      TEST_UTIL.flush(tableName);

      put1 = new Put(row2);
      value1 = Bytes.toBytes("1000dfsdf");
      put1.addColumn(fam, qual, HConstants.LATEST_TIMESTAMP, value1);
      table.put(put1);
      TEST_UTIL.flush(tableName);

      Put put2 = new Put(rowd);
      byte[] value2 = Bytes.toBytes("1000dfsdf");
      put2.addColumn(fam, qual, HConstants.LATEST_TIMESTAMP, value2);
      table.put(put2);
      put2 = new Put(rowe);
      value2 = Bytes.toBytes("1000dfsddfdf");
      put2.addColumn(fam, qual, HConstants.LATEST_TIMESTAMP, value2);
      put.setAttribute("visibility", Bytes.toBytes("ram"));
      table.put(put2);
      TEST_UTIL.flush(tableName);

      TestCoprocessorForTags.checkTagPresence = true;
      Scan s = new Scan().withStartRow(row);
      s.setCaching(1);
      try (ResultScanner scanner = table.getScanner(s)) {
        Result next = null;
        while ((next = scanner.next()) != null) {
          CellScanner cellScanner = next.cellScanner();
          cellScanner.advance();
          Cell current = cellScanner.current();
          List<Tag> tags = TestCoprocessorForTags.tags.poll();
          if (CellUtil.matchingRows(current, row)) {
            assertEquals(1, tags.size());
            Tag tag = tags.get(0);
            assertEquals(bigTagLen, tag.getValueLength());
          } else {
            assertEquals(0, tags.size());
          }
        }
      }
      resetCoprocessorFlags();

      TEST_UTIL.compact(tableName, false);

      TestCoprocessorForTags.checkTagPresence = true;

      try (ResultScanner scanner = table.getScanner(s)) {
        Result next = null;
        while ((next = scanner.next()) != null) {
          CellScanner cellScanner = next.cellScanner();
          cellScanner.advance();
          Cell current = cellScanner.current();
          List<Tag> tags = TestCoprocessorForTags.tags.poll();
          if (CellUtil.matchingRows(current, row)) {
            assertEquals(1, tags.size());
            Tag tag = tags.get(0);
            assertEquals(bigTagLen, tag.getValueLength());
          } else {
            assertEquals(0, tags.size());
          }
        }
      }
    }
  }

  @Test
  public void testTagsWithAppendAndIncrement(TestInfo testInfo) throws Exception {
    TableName tableName = TableName.valueOf(testInfo.getTestMethod().get().getName());
    byte[] f = Bytes.toBytes("f");
    byte[] q = Bytes.toBytes("q");
    byte[] row1 = Bytes.toBytes("r1");
    byte[] row2 = Bytes.toBytes("r2");

    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(f)).build();

    try (Table table = TEST_UTIL.createTable(tableDescriptor, null)) {
      Put put = new Put(row1);
      byte[] v = Bytes.toBytes(2L);
      put.addColumn(f, q, v);
      put.setAttribute("visibility", Bytes.toBytes("tag1"));
      table.put(put);
      Increment increment = new Increment(row1);
      increment.addColumn(f, q, 1L);
      table.increment(increment);
      TestCoprocessorForTags.checkTagPresence = true;
      try (ResultScanner scanner = table.getScanner(new Scan())) {
        Result result = scanner.next();
        Cell kv = result.getColumnLatestCell(f, q);
        List<Tag> tags = TestCoprocessorForTags.tags.poll();
        assertEquals(3L,
          Bytes.toLong(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength()));
        assertEquals(1, tags.size());
        assertEquals("tag1", Bytes.toString(Tag.cloneValue(tags.get(0))));
      }
      resetCoprocessorFlags();

      increment = new Increment(row1);
      increment.add(new KeyValue(row1, f, q, 1234L, v));
      increment.setAttribute("visibility", Bytes.toBytes("tag2"));
      table.increment(increment);
      TestCoprocessorForTags.checkTagPresence = true;
      try (ResultScanner scanner = table.getScanner(new Scan())) {
        Result result = scanner.next();
        Cell kv = result.getColumnLatestCell(f, q);
        List<Tag> tags = TestCoprocessorForTags.tags.poll();
        assertEquals(5L,
          Bytes.toLong(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength()));
        assertEquals(2, tags.size());
        // We cannot assume the ordering of tags
        List<String> tagValues = new ArrayList<>();
        for (Tag tag : tags) {
          tagValues.add(Bytes.toString(Tag.cloneValue(tag)));
        }
        assertTrue(tagValues.contains("tag1"));
        assertTrue(tagValues.contains("tag2"));
      }
      resetCoprocessorFlags();

      put = new Put(row2);
      v = Bytes.toBytes(2L);
      put.addColumn(f, q, v);
      table.put(put);
      increment = new Increment(row2);
      increment.add(new KeyValue(row2, f, q, 1234L, v));
      increment.setAttribute("visibility", Bytes.toBytes("tag2"));
      table.increment(increment);
      TestCoprocessorForTags.checkTagPresence = true;
      try (ResultScanner scanner = table.getScanner(new Scan().withStartRow(row2))) {
        Result result = scanner.next();
        Cell kv = result.getColumnLatestCell(f, q);
        List<Tag> tags = TestCoprocessorForTags.tags.poll();
        assertEquals(4L,
          Bytes.toLong(kv.getValueArray(), kv.getValueOffset(), kv.getValueLength()));
        assertEquals(1, tags.size());
        assertEquals("tag2", Bytes.toString(Tag.cloneValue(tags.get(0))));
      }
      resetCoprocessorFlags();

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
      try (ResultScanner scanner = table.getScanner(new Scan().withStartRow(row3))) {
        assertNotNull(scanner.next());
        List<Tag> tags = TestCoprocessorForTags.tags.poll();
        assertEquals(1, tags.size());
        assertEquals("tag1", Bytes.toString(Tag.cloneValue(tags.get(0))));
      }
      resetCoprocessorFlags();

      append = new Append(row3);
      append.add(new KeyValue(row3, f, q, 1234L, v));
      append.setAttribute("visibility", Bytes.toBytes("tag2"));
      table.append(append);
      TestCoprocessorForTags.checkTagPresence = true;
      try (ResultScanner scanner = table.getScanner(new Scan().withStartRow(row3))) {
        assertNotNull(scanner.next());
        List<Tag> tags = TestCoprocessorForTags.tags.poll();
        assertEquals(2, tags.size());
        // We cannot assume the ordering of tags
        List<String> tagValues = new ArrayList<>();
        for (Tag tag : tags) {
          tagValues.add(Bytes.toString(Tag.cloneValue(tag)));
        }
        assertTrue(tagValues.contains("tag1"));
        assertTrue(tagValues.contains("tag2"));
      }
      resetCoprocessorFlags();

      byte[] row4 = Bytes.toBytes("r4");
      put = new Put(row4);
      put.addColumn(f, q, Bytes.toBytes("a"));
      table.put(put);
      append = new Append(row4);
      append.add(new KeyValue(row4, f, q, 1234L, v));
      append.setAttribute("visibility", Bytes.toBytes("tag2"));
      table.append(append);
      TestCoprocessorForTags.checkTagPresence = true;
      try (ResultScanner scanner = table.getScanner(new Scan().withStartRow(row4))) {
        assertNotNull(scanner.next());
        List<Tag> tags = TestCoprocessorForTags.tags.poll();
        assertEquals(1, tags.size());
        assertEquals("tag2", Bytes.toString(Tag.cloneValue(tags.get(0))));
      }
    }
  }

  private void assertResult(byte[] fam, byte[] row, byte[] qual, byte[] row2, Table table,
    byte[] value, byte[] value2, byte[] row1, byte[] value1) throws IOException {
    Scan s = new Scan().withStartRow(row);
    // If filters are used this attribute can be specifically check for in
    // filterKV method and
    // kvs can be filtered out if the tags of interest is not found in that kv
    s.setAttribute("visibility", Bytes.toBytes("myTag"));
    try (ResultScanner scanner = table.getScanner(s)) {
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
    }
  }

  public static class TestCoprocessorForTags implements RegionCoprocessor, RegionObserver {

    public static volatile boolean checkTagPresence = false;
    public static final ConcurrentLinkedQueue<List<Tag>> tags = new ConcurrentLinkedQueue<>();

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e, final Put put,
      final WALEdit edit, final Durability durability) throws IOException {
      updateMutationAddingTags(put);
    }

    private void updateMutationAddingTags(final Mutation m) {
      byte[] attribute = m.getAttribute("visibility");
      byte[] cf = null;
      List<Cell> updatedCells = new ArrayList<>();
      if (attribute != null) {
        for (List<? extends Cell> edits : m.getFamilyCellMap().values()) {
          for (Cell cell : edits) {
            KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
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
    public Result preIncrement(ObserverContext<RegionCoprocessorEnvironment> e, Increment increment)
      throws IOException {
      updateMutationAddingTags(increment);
      return null;
    }

    @Override
    public Result preAppend(ObserverContext<RegionCoprocessorEnvironment> e, Append append)
      throws IOException {
      updateMutationAddingTags(append);
      return null;
    }

    @Override
    public boolean postScannerNext(ObserverContext<RegionCoprocessorEnvironment> e,
      InternalScanner s, List<Result> results, int limit, boolean hasMore) throws IOException {
      if (checkTagPresence) {
        if (results.size() > 0) {
          // Check tag presence in the 1st cell in 1st Result
          Result result = results.get(0);
          CellScanner cellScanner = result.cellScanner();
          if (cellScanner.advance()) {
            Cell cell = cellScanner.current();
            tags.add(PrivateCellUtil.getTags(cell));
          }
        }
      }
      return hasMore;
    }
  }
}
