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
package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ CoprocessorTests.class, MediumTests.class })
public class TestPassCustomCellViaRegionObserver {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestPassCustomCellViaRegionObserver.class);

  @Rule
  public TestName testName = new TestName();

  private TableName tableName;
  private Table table = null;

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final byte[] ROW = Bytes.toBytes("ROW");
  private static final byte[] FAMILY = Bytes.toBytes("FAMILY");
  private static final byte[] QUALIFIER = Bytes.toBytes("QUALIFIER");
  private static final byte[] VALUE = Bytes.toBytes(10L);
  private static final byte[] APPEND_VALUE = Bytes.toBytes("MB");

  private static final byte[] QUALIFIER_FROM_CP = Bytes.toBytes("QUALIFIER_FROM_CP");

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // small retry number can speed up the failed tests.
    UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void clearTable() throws IOException {
    RegionObserverImpl.COUNT.set(0);
    tableName = TableName.valueOf(testName.getMethodName());
    if (table != null) {
      table.close();
    }
    try (Admin admin = UTIL.getAdmin()) {
      for (TableName name : admin.listTableNames()) {
        try {
          admin.disableTable(name);
        } catch (IOException e) {
        }
        admin.deleteTable(name);
      }
      table = UTIL.createTable(TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY))
        .setCoprocessor(RegionObserverImpl.class.getName())
        .build(), null);
    }
  }

  @Test
  public void testMutation() throws Exception {

    Put put = new Put(ROW);
    put.addColumn(FAMILY, QUALIFIER, VALUE);
    table.put(put);
    byte[] value = VALUE;
    assertResult(table.get(new Get(ROW)), value, value);
    assertObserverHasExecuted();

    Increment inc = new Increment(ROW);
    inc.addColumn(FAMILY, QUALIFIER, 10L);
    table.increment(inc);
    // QUALIFIER -> 10 (put) + 10 (increment)
    // QUALIFIER_FROM_CP -> 10 (from cp's put) + 10 (from cp's increment)
    value = Bytes.toBytes(20L);
    assertResult(table.get(new Get(ROW)), value, value);
    assertObserverHasExecuted();

    Append append = new Append(ROW);
    append.addColumn(FAMILY, QUALIFIER, APPEND_VALUE);
    table.append(append);
    // 10L + "MB"
    value = ByteBuffer.wrap(new byte[value.length + APPEND_VALUE.length])
      .put(value)
      .put(APPEND_VALUE)
      .array();
    assertResult(table.get(new Get(ROW)), value, value);
    assertObserverHasExecuted();

    Delete delete = new Delete(ROW);
    delete.addColumns(FAMILY, QUALIFIER);
    table.delete(delete);
    assertTrue(Arrays.asList(table.get(new Get(ROW)).rawCells()).toString(),
      table.get(new Get(ROW)).isEmpty());
    assertObserverHasExecuted();

    assertTrue(table.checkAndPut(ROW, FAMILY, QUALIFIER, null, put));
    assertObserverHasExecuted();

    assertTrue(table.checkAndDelete(ROW, FAMILY, QUALIFIER, VALUE, delete));
    assertObserverHasExecuted();

    assertTrue(table.get(new Get(ROW)).isEmpty());
  }

  @Test
  public void testMultiPut() throws Exception {
    List<Put> puts = IntStream.range(0, 10)
      .mapToObj(i -> new Put(ROW).addColumn(FAMILY, Bytes.toBytes(i), VALUE))
      .collect(Collectors.toList());
    table.put(puts);
    assertResult(table.get(new Get(ROW)), VALUE);
    assertObserverHasExecuted();

    List<Delete> deletes = IntStream.range(0, 10)
      .mapToObj(i -> new Delete(ROW).addColumn(FAMILY, Bytes.toBytes(i)))
      .collect(Collectors.toList());
    table.delete(deletes);
    assertTrue(table.get(new Get(ROW)).isEmpty());
    assertObserverHasExecuted();
  }

  private static void assertObserverHasExecuted() {
    assertTrue(RegionObserverImpl.COUNT.getAndSet(0) > 0);
  }

  private static void assertResult(Result result, byte[] expectedValue) {
    assertFalse(result.isEmpty());
    for (Cell c : result.rawCells()) {
      assertTrue(c.toString(), Bytes.equals(ROW, CellUtil.cloneRow(c)));
      assertTrue(c.toString(), Bytes.equals(FAMILY, CellUtil.cloneFamily(c)));
      assertTrue(c.toString(), Bytes.equals(expectedValue, CellUtil.cloneValue(c)));
    }
  }

  private static void assertResult(Result result, byte[] expectedValue, byte[] expectedFromCp) {
    assertFalse(result.isEmpty());
    for (Cell c : result.rawCells()) {
      assertTrue(c.toString(), Bytes.equals(ROW, CellUtil.cloneRow(c)));
      assertTrue(c.toString(), Bytes.equals(FAMILY, CellUtil.cloneFamily(c)));
      if (Bytes.equals(QUALIFIER, CellUtil.cloneQualifier(c))) {
        assertTrue(c.toString(), Bytes.equals(expectedValue, CellUtil.cloneValue(c)));
      } else if (Bytes.equals(QUALIFIER_FROM_CP, CellUtil.cloneQualifier(c))) {
        assertTrue(c.toString(), Bytes.equals(expectedFromCp, CellUtil.cloneValue(c)));
      } else {
        fail("No valid qualifier");
      }
    }
  }

  private static Cell createCustomCell(byte[] row, byte[] family, byte[] qualifier,
    Cell.Type type, byte[] value) {
    return new Cell() {

      @Override
      public long heapSize() {
        return 0;
      }

      private byte[] getArray(byte[] array) {
        return array == null ? HConstants.EMPTY_BYTE_ARRAY : array;
      }

      private int length(byte[] array) {
        return array == null ? 0 : array.length;
      }

      @Override
      public byte[] getRowArray() {
        return getArray(row);
      }

      @Override
      public int getRowOffset() {
        return 0;
      }

      @Override
      public short getRowLength() {
        return (short) length(row);
      }

      @Override
      public byte[] getFamilyArray() {
        return getArray(family);
      }

      @Override
      public int getFamilyOffset() {
        return 0;
      }

      @Override
      public byte getFamilyLength() {
        return (byte) length(family);
      }

      @Override
      public byte[] getQualifierArray() {
        return getArray(qualifier);
      }

      @Override
      public int getQualifierOffset() {
        return 0;
      }

      @Override
      public int getQualifierLength() {
        return length(qualifier);
      }

      @Override
      public long getTimestamp() {
        return HConstants.LATEST_TIMESTAMP;
      }

      @Override
      public byte getTypeByte() {
        return type.getCode();
      }

      @Override
      public long getSequenceId() {
        return 0;
      }

      @Override
      public byte[] getValueArray() {
        return getArray(value);
      }

      @Override
      public int getValueOffset() {
        return 0;
      }

      @Override
      public int getValueLength() {
        return length(value);
      }

      @Override
      public int getSerializedSize() {
        return KeyValueUtil.getSerializedSize(this, true);
      }

      @Override
      public byte[] getTagsArray() {
        return getArray(null);
      }

      @Override
      public int getTagsOffset() {
        return 0;
      }

      @Override
      public int getTagsLength() {
        return length(null);
      }

      @Override
      public Type getType() {
        return type;
      }
    };
  }

  private static Cell createCustomCell(Put put) {
    return createCustomCell(put.getRow(), FAMILY, QUALIFIER_FROM_CP, Cell.Type.Put, VALUE);
  }

  private static Cell createCustomCell(Append append) {
    return createCustomCell(append.getRow(), FAMILY, QUALIFIER_FROM_CP, Cell.Type.Put,
      APPEND_VALUE);
  }

  private static Cell createCustomCell(Increment inc) {
    return createCustomCell(inc.getRow(), FAMILY, QUALIFIER_FROM_CP, Cell.Type.Put, VALUE);
  }

  private static Cell createCustomCell(Delete delete) {
    return createCustomCell(delete.getRow(), FAMILY, QUALIFIER_FROM_CP,
      Cell.Type.DeleteColumn, null);
  }

  public static class RegionObserverImpl implements RegionCoprocessor, RegionObserver {
    static final AtomicInteger COUNT = new AtomicInteger(0);

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit,
      Durability durability) throws IOException {
      put.add(createCustomCell(put));
      COUNT.incrementAndGet();
    }

    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> c, Delete delete,
      WALEdit edit, Durability durability) throws IOException {
      delete.add(createCustomCell(delete));
      COUNT.incrementAndGet();
    }

    @Override
    public boolean preCheckAndPut(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row,
      byte[] family, byte[] qualifier, CompareOperator op, ByteArrayComparable comparator, Put put,
      boolean result) throws IOException {
      put.add(createCustomCell(put));
      COUNT.incrementAndGet();
      return result;
    }

    @Override
    public boolean preCheckAndDelete(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row,
      byte[] family, byte[] qualifier, CompareOperator op, ByteArrayComparable comparator,
      Delete delete, boolean result) throws IOException {
      delete.add(createCustomCell(delete));
      COUNT.incrementAndGet();
      return result;
    }

    @Override
    public Result preAppend(ObserverContext<RegionCoprocessorEnvironment> c, Append append)
      throws IOException {
      append.add(createCustomCell(append));
      COUNT.incrementAndGet();
      return null;
    }


    @Override
    public Result preIncrement(ObserverContext<RegionCoprocessorEnvironment> c, Increment increment)
      throws IOException {
      increment.add(createCustomCell(increment));
      COUNT.incrementAndGet();
      return null;
    }

  }

}
