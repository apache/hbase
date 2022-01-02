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
package org.apache.hadoop.hbase.thrift2;

import static java.nio.ByteBuffer.wrap;
import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD;
import static org.apache.hadoop.hbase.thrift.HBaseServiceHandler.CLEANUP_INTERVAL;
import static org.apache.hadoop.hbase.thrift.HBaseServiceHandler.MAX_IDLETIME;
import static org.apache.hadoop.hbase.thrift2.ThriftUtilities.deleteFromThrift;
import static org.apache.hadoop.hbase.thrift2.ThriftUtilities.getFromThrift;
import static org.apache.hadoop.hbase.thrift2.ThriftUtilities.incrementFromThrift;
import static org.apache.hadoop.hbase.thrift2.ThriftUtilities.putFromThrift;
import static org.apache.hadoop.hbase.thrift2.ThriftUtilities.scanFromThrift;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.LogQueryFilter;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.thrift.ErrorThrowingGetObserver;
import org.apache.hadoop.hbase.thrift.HBaseThriftTestingUtility;
import org.apache.hadoop.hbase.thrift.HbaseHandlerMetricsProxy;
import org.apache.hadoop.hbase.thrift.ThriftMetrics;
import org.apache.hadoop.hbase.thrift.ThriftMetrics.ThriftServerType;
import org.apache.hadoop.hbase.thrift2.generated.TAppend;
import org.apache.hadoop.hbase.thrift2.generated.TColumn;
import org.apache.hadoop.hbase.thrift2.generated.TColumnFamilyDescriptor;
import org.apache.hadoop.hbase.thrift2.generated.TColumnIncrement;
import org.apache.hadoop.hbase.thrift2.generated.TColumnValue;
import org.apache.hadoop.hbase.thrift2.generated.TCompareOp;
import org.apache.hadoop.hbase.thrift2.generated.TConsistency;
import org.apache.hadoop.hbase.thrift2.generated.TDataBlockEncoding;
import org.apache.hadoop.hbase.thrift2.generated.TDelete;
import org.apache.hadoop.hbase.thrift2.generated.TDeleteType;
import org.apache.hadoop.hbase.thrift2.generated.TDurability;
import org.apache.hadoop.hbase.thrift2.generated.TFilterByOperator;
import org.apache.hadoop.hbase.thrift2.generated.TGet;
import org.apache.hadoop.hbase.thrift2.generated.THBaseService;
import org.apache.hadoop.hbase.thrift2.generated.TIOError;
import org.apache.hadoop.hbase.thrift2.generated.TIllegalArgument;
import org.apache.hadoop.hbase.thrift2.generated.TIncrement;
import org.apache.hadoop.hbase.thrift2.generated.TLogQueryFilter;
import org.apache.hadoop.hbase.thrift2.generated.TMutation;
import org.apache.hadoop.hbase.thrift2.generated.TNamespaceDescriptor;
import org.apache.hadoop.hbase.thrift2.generated.TOnlineLogRecord;
import org.apache.hadoop.hbase.thrift2.generated.TPut;
import org.apache.hadoop.hbase.thrift2.generated.TReadType;
import org.apache.hadoop.hbase.thrift2.generated.TResult;
import org.apache.hadoop.hbase.thrift2.generated.TRowMutations;
import org.apache.hadoop.hbase.thrift2.generated.TScan;
import org.apache.hadoop.hbase.thrift2.generated.TServerName;
import org.apache.hadoop.hbase.thrift2.generated.TTableDescriptor;
import org.apache.hadoop.hbase.thrift2.generated.TTableName;
import org.apache.hadoop.hbase.thrift2.generated.TThriftServerType;
import org.apache.hadoop.hbase.thrift2.generated.TTimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

/**
 * Unit testing for ThriftServer.HBaseServiceHandler, a part of the org.apache.hadoop.hbase.thrift2
 * package.
 */
@Category({ClientTests.class, MediumTests.class})
public class TestThriftHBaseServiceHandler {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestThriftHBaseServiceHandler.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestThriftHBaseServiceHandler.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  // Static names for tables, columns, rows, and values
  private static byte[] tableAname = Bytes.toBytes("tableA");
  private static byte[] familyAname = Bytes.toBytes("familyA");
  private static byte[] familyBname = Bytes.toBytes("familyB");
  private static byte[] qualifierAname = Bytes.toBytes("qualifierA");
  private static byte[] qualifierBname = Bytes.toBytes("qualifierB");
  private static byte[] valueAname = Bytes.toBytes("valueA");
  private static byte[] valueBname = Bytes.toBytes("valueB");
  private static HColumnDescriptor[] families = new HColumnDescriptor[] {
      new HColumnDescriptor(familyAname).setMaxVersions(3),
      new HColumnDescriptor(familyBname).setMaxVersions(2)
  };


  private static final MetricsAssertHelper metricsHelper =
      CompatibilityFactory.getInstance(MetricsAssertHelper.class);

  @Rule
  public TestName name = new TestName();


  public void assertTColumnValuesEqual(List<TColumnValue> columnValuesA,
      List<TColumnValue> columnValuesB) {
    assertEquals(columnValuesA.size(), columnValuesB.size());
    Comparator<TColumnValue> comparator = new Comparator<TColumnValue>() {
      @Override
      public int compare(TColumnValue o1, TColumnValue o2) {
        return Bytes.compareTo(Bytes.add(o1.getFamily(), o1.getQualifier()),
            Bytes.add(o2.getFamily(), o2.getQualifier()));
      }
    };
    Collections.sort(columnValuesA, comparator);
    Collections.sort(columnValuesB, comparator);

    for (int i = 0; i < columnValuesA.size(); i++) {
      TColumnValue a = columnValuesA.get(i);
      TColumnValue b = columnValuesB.get(i);
      assertTColumnValueEqual(a, b);
    }
  }

  public void assertTColumnValueEqual(TColumnValue a, TColumnValue b) {
    assertArrayEquals(a.getFamily(), b.getFamily());
    assertArrayEquals(a.getQualifier(), b.getQualifier());
    assertArrayEquals(a.getValue(), b.getValue());
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    UTIL.getConfiguration().set("hbase.client.retries.number", "3");
    UTIL.getConfiguration().setBoolean("hbase.regionserver.slowlog.buffer.enabled", true);
    UTIL.startMiniCluster();
    HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableAname));
    for (HColumnDescriptor family : families) {
      tableDescriptor.addFamily(family);
    }
    try (Admin admin = UTIL.getAdmin()) {
      admin.createTable(tableDescriptor);
    }
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void setup() throws Exception {

  }

  private ThriftHBaseServiceHandler createHandler() throws TException {
    try {
      Configuration conf = UTIL.getConfiguration();
      return new ThriftHBaseServiceHandler(conf, UserProvider.instantiate(conf));
    } catch (IOException ie) {
      throw new TException(ie);
    }
  }

  @Test
  public void testExists() throws TIOError, TException {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = Bytes.toBytes("testExists");
    ByteBuffer table = wrap(tableAname);

    TGet get = new TGet(wrap(rowName));
    assertFalse(handler.exists(table, get));

    List<TColumnValue> columnValues = new ArrayList<>(2);
    columnValues.add(new TColumnValue(wrap(familyAname), wrap(qualifierAname), wrap(valueAname)));
    columnValues.add(new TColumnValue(wrap(familyBname), wrap(qualifierBname), wrap(valueBname)));
    TPut put = new TPut(wrap(rowName), columnValues);
    put.setColumnValues(columnValues);

    handler.put(table, put);

    assertTrue(handler.exists(table, get));
  }

  @Test
  public void testExistsAll() throws TIOError, TException {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName1 = Bytes.toBytes("testExistsAll1");
    byte[] rowName2 = Bytes.toBytes("testExistsAll2");
    ByteBuffer table = wrap(tableAname);

    List<TGet> gets = new ArrayList<>();
    gets.add(new TGet(wrap(rowName2)));
    gets.add(new TGet(wrap(rowName2)));
    List<Boolean> existsResult1 = handler.existsAll(table, gets);
    assertFalse(existsResult1.get(0));
    assertFalse(existsResult1.get(1));

    List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
    columnValues.add(new TColumnValue(wrap(familyAname), wrap(qualifierAname), wrap(valueAname)));
    columnValues.add(new TColumnValue(wrap(familyBname), wrap(qualifierBname), wrap(valueBname)));
    List<TPut> puts = new ArrayList<TPut>();
    puts.add(new TPut(wrap(rowName1), columnValues));
    puts.add(new TPut(wrap(rowName2), columnValues));

    handler.putMultiple(table, puts);
    List<Boolean> existsResult2 = handler.existsAll(table,gets);

    assertTrue(existsResult2.get(0));
    assertTrue(existsResult2.get(1));
  }

  @Test
  public void testPutGet() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = Bytes.toBytes("testPutGet");
    ByteBuffer table = wrap(tableAname);

    List<TColumnValue> columnValues = new ArrayList<>(2);
    columnValues.add(new TColumnValue(wrap(familyAname), wrap(qualifierAname), wrap(valueAname)));
    columnValues.add(new TColumnValue(wrap(familyBname), wrap(qualifierBname), wrap(valueBname)));
    TPut put = new TPut(wrap(rowName), columnValues);

    put.setColumnValues(columnValues);

    handler.put(table, put);

    TGet get = new TGet(wrap(rowName));

    TResult result = handler.get(table, get);
    assertArrayEquals(rowName, result.getRow());
    List<TColumnValue> returnedColumnValues = result.getColumnValues();
    assertTColumnValuesEqual(columnValues, returnedColumnValues);
  }

  @Test
  public void testPutGetMultiple() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    ByteBuffer table = wrap(tableAname);
    byte[] rowName1 = Bytes.toBytes("testPutGetMultiple1");
    byte[] rowName2 = Bytes.toBytes("testPutGetMultiple2");

    List<TColumnValue> columnValues = new ArrayList<>(2);
    columnValues.add(new TColumnValue(wrap(familyAname), wrap(qualifierAname), wrap(valueAname)));
    columnValues.add(new TColumnValue(wrap(familyBname), wrap(qualifierBname), wrap(valueBname)));
    List<TPut> puts = new ArrayList<>(2);
    puts.add(new TPut(wrap(rowName1), columnValues));
    puts.add(new TPut(wrap(rowName2), columnValues));

    handler.putMultiple(table, puts);

    List<TGet> gets = new ArrayList<>(2);
    gets.add(new TGet(wrap(rowName1)));
    gets.add(new TGet(wrap(rowName2)));

    List<TResult> results = handler.getMultiple(table, gets);
    assertEquals(2, results.size());

    assertArrayEquals(rowName1, results.get(0).getRow());
    assertTColumnValuesEqual(columnValues, results.get(0).getColumnValues());

    assertArrayEquals(rowName2, results.get(1).getRow());
    assertTColumnValuesEqual(columnValues, results.get(1).getColumnValues());
  }

  @Test
  public void testDeleteMultiple() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    ByteBuffer table = wrap(tableAname);
    byte[] rowName1 = Bytes.toBytes("testDeleteMultiple1");
    byte[] rowName2 = Bytes.toBytes("testDeleteMultiple2");

    List<TColumnValue> columnValues = new ArrayList<>(2);
    columnValues.add(new TColumnValue(wrap(familyAname), wrap(qualifierAname), wrap(valueAname)));
    columnValues.add(new TColumnValue(wrap(familyBname), wrap(qualifierBname), wrap(valueBname)));
    List<TPut> puts = new ArrayList<>(2);
    puts.add(new TPut(wrap(rowName1), columnValues));
    puts.add(new TPut(wrap(rowName2), columnValues));

    handler.putMultiple(table, puts);

    List<TDelete> deletes = new ArrayList<>(2);
    deletes.add(new TDelete(wrap(rowName1)));
    deletes.add(new TDelete(wrap(rowName2)));

    List<TDelete> deleteResults = handler.deleteMultiple(table, deletes);
    // 0 means they were all successfully applies
    assertEquals(0, deleteResults.size());

    assertFalse(handler.exists(table, new TGet(wrap(rowName1))));
    assertFalse(handler.exists(table, new TGet(wrap(rowName2))));
  }

  @Test
  public void testDelete() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = Bytes.toBytes("testDelete");
    ByteBuffer table = wrap(tableAname);

    List<TColumnValue> columnValues = new ArrayList<>(2);
    TColumnValue columnValueA = new TColumnValue(wrap(familyAname), wrap(qualifierAname),
      wrap(valueAname));
    TColumnValue columnValueB = new TColumnValue(wrap(familyBname), wrap(qualifierBname),
      wrap(valueBname));
    columnValues.add(columnValueA);
    columnValues.add(columnValueB);
    TPut put = new TPut(wrap(rowName), columnValues);

    put.setColumnValues(columnValues);

    handler.put(table, put);

    TDelete delete = new TDelete(wrap(rowName));
    List<TColumn> deleteColumns = new ArrayList<>(1);
    TColumn deleteColumn = new TColumn(wrap(familyAname));
    deleteColumn.setQualifier(qualifierAname);
    deleteColumns.add(deleteColumn);
    delete.setColumns(deleteColumns);

    handler.deleteSingle(table, delete);

    TGet get = new TGet(wrap(rowName));
    TResult result = handler.get(table, get);
    assertArrayEquals(rowName, result.getRow());
    List<TColumnValue> returnedColumnValues = result.getColumnValues();
    List<TColumnValue> expectedColumnValues = new ArrayList<>(1);
    expectedColumnValues.add(columnValueB);
    assertTColumnValuesEqual(expectedColumnValues, returnedColumnValues);
  }

  @Test
  public void testDeleteAllTimestamps() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = Bytes.toBytes("testDeleteAllTimestamps");
    ByteBuffer table = wrap(tableAname);

    List<TColumnValue> columnValues = new ArrayList<>(1);
    TColumnValue columnValueA = new TColumnValue(wrap(familyAname), wrap(qualifierAname),
      wrap(valueAname));
    columnValueA.setTimestamp(System.currentTimeMillis() - 10);
    columnValues.add(columnValueA);
    TPut put = new TPut(wrap(rowName), columnValues);

    put.setColumnValues(columnValues);

    handler.put(table, put);
    columnValueA.setTimestamp(System.currentTimeMillis());
    handler.put(table, put);

    TGet get = new TGet(wrap(rowName));
    get.setMaxVersions(2);
    TResult result = handler.get(table, get);
    assertEquals(2, result.getColumnValuesSize());

    TDelete delete = new TDelete(wrap(rowName));
    List<TColumn> deleteColumns = new ArrayList<>(1);
    TColumn deleteColumn = new TColumn(wrap(familyAname));
    deleteColumn.setQualifier(qualifierAname);
    deleteColumns.add(deleteColumn);
    delete.setColumns(deleteColumns);
    delete.setDeleteType(TDeleteType.DELETE_COLUMNS); // This is the default anyway.

    handler.deleteSingle(table, delete);

    get = new TGet(wrap(rowName));
    result = handler.get(table, get);
    assertNull(result.getRow());
    assertEquals(0, result.getColumnValuesSize());
  }

  @Test
  public void testDeleteSingleTimestamp() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = Bytes.toBytes("testDeleteSingleTimestamp");
    ByteBuffer table = wrap(tableAname);

    long timestamp1 = System.currentTimeMillis() - 10;
    long timestamp2 = System.currentTimeMillis();

    List<TColumnValue> columnValues = new ArrayList<>(1);
    TColumnValue columnValueA = new TColumnValue(wrap(familyAname), wrap(qualifierAname),
      wrap(valueAname));
    columnValueA.setTimestamp(timestamp1);
    columnValues.add(columnValueA);
    TPut put = new TPut(wrap(rowName), columnValues);

    put.setColumnValues(columnValues);

    handler.put(table, put);
    columnValueA.setTimestamp(timestamp2);
    handler.put(table, put);

    TGet get = new TGet(wrap(rowName));
    get.setMaxVersions(2);
    TResult result = handler.get(table, get);
    assertEquals(2, result.getColumnValuesSize());

    TDelete delete = new TDelete(wrap(rowName));
    List<TColumn> deleteColumns = new ArrayList<>(1);
    TColumn deleteColumn = new TColumn(wrap(familyAname));
    deleteColumn.setQualifier(qualifierAname);
    deleteColumns.add(deleteColumn);
    delete.setColumns(deleteColumns);
    delete.setDeleteType(TDeleteType.DELETE_COLUMN);

    handler.deleteSingle(table, delete);

    get = new TGet(wrap(rowName));
    result = handler.get(table, get);
    assertArrayEquals(rowName, result.getRow());
    assertEquals(1, result.getColumnValuesSize());
    // the older timestamp should remain.
    assertEquals(timestamp1, result.getColumnValues().get(0).getTimestamp());
  }

  @Test
  public void testDeleteFamily() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = Bytes.toBytes("testDeleteFamily");
    ByteBuffer table = wrap(tableAname);

    long timestamp1 = System.currentTimeMillis() - 10;
    long timestamp2 = System.currentTimeMillis();

    List<TColumnValue> columnValues = new ArrayList<>();
    TColumnValue columnValueA =
        new TColumnValue(wrap(familyAname), wrap(qualifierAname), wrap(valueAname));
    columnValueA.setTimestamp(timestamp1);
    columnValues.add(columnValueA);
    TPut put = new TPut(wrap(rowName), columnValues);

    put.setColumnValues(columnValues);

    handler.put(table, put);
    columnValueA.setTimestamp(timestamp2);
    handler.put(table, put);

    TGet get = new TGet(wrap(rowName));
    get.setMaxVersions(2);
    TResult result = handler.get(table, get);
    assertEquals(2, result.getColumnValuesSize());

    TDelete delete = new TDelete(wrap(rowName));
    List<TColumn> deleteColumns = new ArrayList<>();
    TColumn deleteColumn = new TColumn(wrap(familyAname));
    deleteColumns.add(deleteColumn);
    delete.setColumns(deleteColumns);
    delete.setDeleteType(TDeleteType.DELETE_FAMILY);

    handler.deleteSingle(table, delete);

    get = new TGet(wrap(rowName));
    result = handler.get(table, get);
    assertArrayEquals(null, result.getRow());
    assertEquals(0, result.getColumnValuesSize());
  }

  @Test
  public void testDeleteFamilyVersion() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = Bytes.toBytes("testDeleteFamilyVersion");
    ByteBuffer table = wrap(tableAname);

    long timestamp1 = System.currentTimeMillis() - 10;
    long timestamp2 = System.currentTimeMillis();

    List<TColumnValue> columnValues = new ArrayList<>();
    TColumnValue columnValueA =
        new TColumnValue(wrap(familyAname), wrap(qualifierAname), wrap(valueAname));
    columnValueA.setTimestamp(timestamp1);
    columnValues.add(columnValueA);
    TPut put = new TPut(wrap(rowName), columnValues);

    put.setColumnValues(columnValues);

    handler.put(table, put);
    columnValueA.setTimestamp(timestamp2);
    handler.put(table, put);

    TGet get = new TGet(wrap(rowName));
    get.setMaxVersions(2);
    TResult result = handler.get(table, get);
    assertEquals(2, result.getColumnValuesSize());

    TDelete delete = new TDelete(wrap(rowName));
    List<TColumn> deleteColumns = new ArrayList<>();
    TColumn deleteColumn = new TColumn(wrap(familyAname));
    deleteColumn.setTimestamp(timestamp1);
    deleteColumns.add(deleteColumn);
    delete.setColumns(deleteColumns);
    delete.setDeleteType(TDeleteType.DELETE_FAMILY_VERSION);

    handler.deleteSingle(table, delete);

    get = new TGet(wrap(rowName));
    result = handler.get(table, get);
    assertArrayEquals(rowName, result.getRow());
    assertEquals(1, result.getColumnValuesSize());
    assertEquals(timestamp2, result.getColumnValues().get(0).getTimestamp());
  }

  @Test
  public void testIncrement() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = Bytes.toBytes("testIncrement");
    ByteBuffer table = wrap(tableAname);

    List<TColumnValue> columnValues = new ArrayList<>(1);
    columnValues.add(new TColumnValue(wrap(familyAname), wrap(qualifierAname),
      wrap(Bytes.toBytes(1L))));
    TPut put = new TPut(wrap(rowName), columnValues);
    put.setColumnValues(columnValues);
    handler.put(table, put);

    List<TColumnIncrement> incrementColumns = new ArrayList<>(1);
    incrementColumns.add(new TColumnIncrement(wrap(familyAname), wrap(qualifierAname)));
    TIncrement increment = new TIncrement(wrap(rowName), incrementColumns);
    handler.increment(table, increment);

    TGet get = new TGet(wrap(rowName));
    TResult result = handler.get(table, get);

    assertArrayEquals(rowName, result.getRow());
    assertEquals(1, result.getColumnValuesSize());
    TColumnValue columnValue = result.getColumnValues().get(0);
    assertArrayEquals(Bytes.toBytes(2L), columnValue.getValue());
  }

  @Test
  public void testAppend() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = Bytes.toBytes("testAppend");
    ByteBuffer table = wrap(tableAname);
    byte[] v1 = Bytes.toBytes("42");
    byte[] v2 = Bytes.toBytes("23");
    List<TColumnValue> columnValues = new ArrayList<>(1);
    columnValues.add(new TColumnValue(wrap(familyAname), wrap(qualifierAname), wrap(v1)));
    TPut put = new TPut(wrap(rowName), columnValues);
    put.setColumnValues(columnValues);
    handler.put(table, put);

    List<TColumnValue> appendColumns = new ArrayList<>(1);
    appendColumns.add(new TColumnValue(wrap(familyAname), wrap(qualifierAname), wrap(v2)));
    TAppend append = new TAppend(wrap(rowName), appendColumns);
    handler.append(table, append);

    TGet get = new TGet(wrap(rowName));
    TResult result = handler.get(table, get);

    assertArrayEquals(rowName, result.getRow());
    assertEquals(1, result.getColumnValuesSize());
    TColumnValue columnValue = result.getColumnValues().get(0);
    assertArrayEquals(Bytes.add(v1, v2), columnValue.getValue());
  }

  /**
   * check that checkAndPut fails if the cell does not exist, then put in the cell, then check
   * that the checkAndPut succeeds.
   */
  @Test
  public void testCheckAndPut() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = Bytes.toBytes("testCheckAndPut");
    ByteBuffer table = wrap(tableAname);

    List<TColumnValue> columnValuesA = new ArrayList<>(1);
    TColumnValue columnValueA = new TColumnValue(wrap(familyAname), wrap(qualifierAname),
      wrap(valueAname));
    columnValuesA.add(columnValueA);
    TPut putA = new TPut(wrap(rowName), columnValuesA);
    putA.setColumnValues(columnValuesA);

    List<TColumnValue> columnValuesB = new ArrayList<>(1);
    TColumnValue columnValueB = new TColumnValue(wrap(familyBname), wrap(qualifierBname),
      wrap(valueBname));
    columnValuesB.add(columnValueB);
    TPut putB = new TPut(wrap(rowName), columnValuesB);
    putB.setColumnValues(columnValuesB);

    assertFalse(handler.checkAndPut(table, wrap(rowName), wrap(familyAname),
      wrap(qualifierAname), wrap(valueAname), putB));

    TGet get = new TGet(wrap(rowName));
    TResult result = handler.get(table, get);
    assertEquals(0, result.getColumnValuesSize());

    handler.put(table, putA);

    assertTrue(handler.checkAndPut(table, wrap(rowName), wrap(familyAname),
      wrap(qualifierAname), wrap(valueAname), putB));

    result = handler.get(table, get);
    assertArrayEquals(rowName, result.getRow());
    List<TColumnValue> returnedColumnValues = result.getColumnValues();
    List<TColumnValue> expectedColumnValues = new ArrayList<>(2);
    expectedColumnValues.add(columnValueA);
    expectedColumnValues.add(columnValueB);
    assertTColumnValuesEqual(expectedColumnValues, returnedColumnValues);
  }

  /**
   * check that checkAndDelete fails if the cell does not exist, then put in the cell, then
   * check that the checkAndDelete succeeds.
   */
  @Test
  public void testCheckAndDelete() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = Bytes.toBytes("testCheckAndDelete");
    ByteBuffer table = wrap(tableAname);

    List<TColumnValue> columnValuesA = new ArrayList<>(1);
    TColumnValue columnValueA = new TColumnValue(wrap(familyAname), wrap(qualifierAname),
      wrap(valueAname));
    columnValuesA.add(columnValueA);
    TPut putA = new TPut(wrap(rowName), columnValuesA);
    putA.setColumnValues(columnValuesA);

    List<TColumnValue> columnValuesB = new ArrayList<>(1);
    TColumnValue columnValueB = new TColumnValue(wrap(familyBname), wrap(qualifierBname),
      wrap(valueBname));
    columnValuesB.add(columnValueB);
    TPut putB = new TPut(wrap(rowName), columnValuesB);
    putB.setColumnValues(columnValuesB);

    // put putB so that we know whether the row has been deleted or not
    handler.put(table, putB);

    TDelete delete = new TDelete(wrap(rowName));

    assertFalse(handler.checkAndDelete(table, wrap(rowName), wrap(familyAname),
        wrap(qualifierAname), wrap(valueAname), delete));

    TGet get = new TGet(wrap(rowName));
    TResult result = handler.get(table, get);
    assertArrayEquals(rowName, result.getRow());
    assertTColumnValuesEqual(columnValuesB, result.getColumnValues());

    handler.put(table, putA);

    assertTrue(handler.checkAndDelete(table, wrap(rowName), wrap(familyAname),
      wrap(qualifierAname), wrap(valueAname), delete));

    result = handler.get(table, get);
    assertFalse(result.isSetRow());
    assertEquals(0, result.getColumnValuesSize());
  }

  @Test
  public void testScan() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    ByteBuffer table = wrap(tableAname);

    // insert data
    TColumnValue columnValue = new TColumnValue(wrap(familyAname), wrap(qualifierAname),
      wrap(valueAname));
    List<TColumnValue> columnValues = new ArrayList<>(1);
    columnValues.add(columnValue);
    for (int i = 0; i < 10; i++) {
      TPut put = new TPut(wrap(Bytes.toBytes("testScan" + i)), columnValues);
      handler.put(table, put);
    }

    // create scan instance
    TScan scan = new TScan();
    List<TColumn> columns = new ArrayList<>(1);
    TColumn column = new TColumn();
    column.setFamily(familyAname);
    column.setQualifier(qualifierAname);
    columns.add(column);
    scan.setColumns(columns);
    scan.setStartRow(Bytes.toBytes("testScan"));
    scan.setStopRow(Bytes.toBytes("testScan\uffff"));

    // get scanner and rows
    int scanId = handler.openScanner(table, scan);
    List<TResult> results = handler.getScannerRows(scanId, 10);
    assertEquals(10, results.size());
    for (int i = 0; i < 10; i++) {
      // check if the rows are returned and in order
      assertArrayEquals(Bytes.toBytes("testScan" + i), results.get(i).getRow());
    }

    // check that we are at the end of the scan
    results = handler.getScannerRows(scanId, 10);
    assertEquals(0, results.size());

    // close scanner and check that it was indeed closed
    handler.closeScanner(scanId);
    try {
      handler.getScannerRows(scanId, 10);
      fail("Scanner id should be invalid");
    } catch (TIllegalArgument e) {
    }
  }

  /**
   * Tests keeping a HBase scanner alive for long periods of time. Each call to getScannerRow()
   * should reset the ConnectionCache timeout for the scanner's connection
   */
  @org.junit.Ignore @Test // Flakey. Diasabled by HBASE-24079. Renable with Fails with HBASE-24083.
  //  Caused by: java.util.concurrent.RejectedExecutionException:
  //  Task org.apache.hadoop.hbase.client.ResultBoundedCompletionService$QueueingFuture@e385431
  //  rejected from java.util.concurrent.ThreadPoolExecutor@   52b027d[Terminated, pool size = 0,
  //  active threads = 0, queued tasks = 0, completed tasks = 1]
  //  at org.apache.hadoop.hbase.thrift2.TestThriftHBaseServiceHandler.
  //  testLongLivedScan(TestThriftHBaseServiceHandler.java:804)
  public void testLongLivedScan() throws Exception {
    int numTrials = 6;
    int trialPause = 1000;
    int cleanUpInterval = 100;
    Configuration conf = new Configuration(UTIL.getConfiguration());
    // Set the ConnectionCache timeout to trigger halfway through the trials
    conf.setInt(MAX_IDLETIME, (numTrials / 2) * trialPause);
    conf.setInt(CLEANUP_INTERVAL, cleanUpInterval);
    ThriftHBaseServiceHandler handler = new ThriftHBaseServiceHandler(conf,
        UserProvider.instantiate(conf));

    ByteBuffer table = wrap(tableAname);
    // insert data
    TColumnValue columnValue = new TColumnValue(wrap(familyAname), wrap(qualifierAname),
        wrap(valueAname));
    List<TColumnValue> columnValues = new ArrayList<>(1);
    columnValues.add(columnValue);
    for (int i = 0; i < numTrials; i++) {
      TPut put = new TPut(wrap(Bytes.toBytes("testScan" + i)), columnValues);
      handler.put(table, put);
    }

    // create scan instance
    TScan scan = new TScan();
    List<TColumn> columns = new ArrayList<>(1);
    TColumn column = new TColumn();
    column.setFamily(familyAname);
    column.setQualifier(qualifierAname);
    columns.add(column);
    scan.setColumns(columns);
    scan.setStartRow(Bytes.toBytes("testScan"));
    scan.setStopRow(Bytes.toBytes("testScan\uffff"));
    // Prevent the scanner from caching results
    scan.setCaching(1);

    // get scanner and rows
    int scanId = handler.openScanner(table, scan);
    for (int i = 0; i < numTrials; i++) {
      // Make sure that the Scanner doesn't throw an exception after the ConnectionCache timeout
      List<TResult> results = handler.getScannerRows(scanId, 1);
      assertArrayEquals(Bytes.toBytes("testScan" + i), results.get(0).getRow());
      Thread.sleep(trialPause);
    }
  }

  @Test
  public void testReverseScan() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    ByteBuffer table = wrap(tableAname);

    // insert data
    TColumnValue columnValue = new TColumnValue(wrap(familyAname), wrap(qualifierAname),
      wrap(valueAname));
    List<TColumnValue> columnValues = new ArrayList<>(1);
    columnValues.add(columnValue);
    for (int i = 0; i < 10; i++) {
      TPut put = new TPut(wrap(Bytes.toBytes("testReverseScan" + i)), columnValues);
      handler.put(table, put);
    }

    // create reverse scan instance
    TScan scan = new TScan();
    scan.setReversed(true);
    List<TColumn> columns = new ArrayList<>(1);
    TColumn column = new TColumn();
    column.setFamily(familyAname);
    column.setQualifier(qualifierAname);
    columns.add(column);
    scan.setColumns(columns);
    scan.setStartRow(Bytes.toBytes("testReverseScan\uffff"));
    scan.setStopRow(Bytes.toBytes("testReverseScan"));

    // get scanner and rows
    int scanId = handler.openScanner(table, scan);
    List<TResult> results = handler.getScannerRows(scanId, 10);
    assertEquals(10, results.size());
    for (int i = 0; i < 10; i++) {
      // check if the rows are returned and in order
      assertArrayEquals(Bytes.toBytes("testReverseScan" + (9 - i)), results.get(i).getRow());
    }

    // check that we are at the end of the scan
    results = handler.getScannerRows(scanId, 10);
    assertEquals(0, results.size());

    // close scanner and check that it was indeed closed
    handler.closeScanner(scanId);
    try {
      handler.getScannerRows(scanId, 10);
      fail("Scanner id should be invalid");
    } catch (TIllegalArgument e) {
    }
  }

  @Test
  public void testScanWithFilter() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    ByteBuffer table = wrap(tableAname);

    // insert data
    TColumnValue columnValue = new TColumnValue(wrap(familyAname), wrap(qualifierAname),
      wrap(valueAname));
    List<TColumnValue> columnValues = new ArrayList<>(1);
    columnValues.add(columnValue);
    for (int i = 0; i < 10; i++) {
      TPut put = new TPut(wrap(Bytes.toBytes("testScanWithFilter" + i)), columnValues);
      handler.put(table, put);
    }

    // create scan instance with filter
    TScan scan = new TScan();
    List<TColumn> columns = new ArrayList<>(1);
    TColumn column = new TColumn();
    column.setFamily(familyAname);
    column.setQualifier(qualifierAname);
    columns.add(column);
    scan.setColumns(columns);
    scan.setStartRow(Bytes.toBytes("testScanWithFilter"));
    scan.setStopRow(Bytes.toBytes("testScanWithFilter\uffff"));
    // only get the key part
    scan.setFilterString(wrap(Bytes.toBytes("KeyOnlyFilter()")));

    // get scanner and rows
    int scanId = handler.openScanner(table, scan);
    List<TResult> results = handler.getScannerRows(scanId, 10);
    assertEquals(10, results.size());
    for (int i = 0; i < 10; i++) {
      // check if the rows are returned and in order
      assertArrayEquals(Bytes.toBytes("testScanWithFilter" + i), results.get(i).getRow());
      // check that the value is indeed stripped by the filter
      assertEquals(0, results.get(i).getColumnValues().get(0).getValue().length);
    }

    // check that we are at the end of the scan
    results = handler.getScannerRows(scanId, 10);
    assertEquals(0, results.size());

    // close scanner and check that it was indeed closed
    handler.closeScanner(scanId);
    try {
      handler.getScannerRows(scanId, 10);
      fail("Scanner id should be invalid");
    } catch (TIllegalArgument e) {
    }
  }

  @Test
  public void testScanWithColumnFamilyTimeRange() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    ByteBuffer table = wrap(tableAname);

    // insert data
    TColumnValue familyAColumnValue = new TColumnValue(wrap(familyAname), wrap(qualifierAname),
        wrap(valueAname));
    TColumnValue familyBColumnValue = new TColumnValue(wrap(familyBname), wrap(qualifierBname),
        wrap(valueBname));
    long minTimestamp = System.currentTimeMillis();
    for (int i = 0; i < 10; i++) {
      familyAColumnValue.setTimestamp(minTimestamp + i);
      familyBColumnValue.setTimestamp(minTimestamp + i);
      List<TColumnValue> columnValues = new ArrayList<>(2);
      columnValues.add(familyAColumnValue);
      columnValues.add(familyBColumnValue);
      TPut put = new TPut(wrap(Bytes.toBytes("testScanWithColumnFamilyTimeRange" + i)),
          columnValues);
      handler.put(table, put);
    }

    // create scan instance with column family time range
    TScan scan = new TScan();
    Map<ByteBuffer,TTimeRange> colFamTimeRangeMap = new HashMap<>(2);
    colFamTimeRangeMap.put(wrap(familyAname), new TTimeRange(minTimestamp + 3, minTimestamp + 5));
    colFamTimeRangeMap.put(wrap(familyBname), new TTimeRange(minTimestamp + 6, minTimestamp + 9));
    scan.setColFamTimeRangeMap(colFamTimeRangeMap);

    // get scanner and rows
    int scanId = handler.openScanner(table, scan);
    List<TResult> results = handler.getScannerRows(scanId, 5);
    assertEquals(5, results.size());
    int familyACount = 0;
    int familyBCount = 0;
    for (TResult result : results) {
      List<TColumnValue> columnValues = result.getColumnValues();
      if (CollectionUtils.isNotEmpty(columnValues)) {
        if (Bytes.equals(familyAname, columnValues.get(0).getFamily())) {
          familyACount++;
        } else if (Bytes.equals(familyBname, columnValues.get(0).getFamily())) {
          familyBCount++;
        }
      }
    }
    assertEquals(2, familyACount);
    assertEquals(3, familyBCount);

    // check that we are at the end of the scan
    results = handler.getScannerRows(scanId, 1);
    assertEquals(0, results.size());

    // close scanner and check that it was indeed closed
    handler.closeScanner(scanId);
    try {
      handler.getScannerRows(scanId, 1);
      fail("Scanner id should be invalid");
    } catch (TIllegalArgument e) {
    }
  }

  @Test
  public void testSmallScan() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    ByteBuffer table = wrap(tableAname);

    // insert data
    TColumnValue columnValue = new TColumnValue(wrap(familyAname), wrap(qualifierAname),
            wrap(valueAname));
    List<TColumnValue> columnValues = new ArrayList<>();
    columnValues.add(columnValue);
    for (int i = 0; i < 10; i++) {
      TPut put = new TPut(wrap(Bytes.toBytes("testSmallScan" + i)), columnValues);
      handler.put(table, put);
    }

    // small scan instance
    TScan scan = new TScan();
    scan.setStartRow(Bytes.toBytes("testSmallScan"));
    scan.setStopRow(Bytes.toBytes("testSmallScan\uffff"));
    scan.setReadType(TReadType.PREAD);
    scan.setCaching(2);

    // get scanner and rows
    int scanId = handler.openScanner(table, scan);
    List<TResult> results = handler.getScannerRows(scanId, 10);
    assertEquals(10, results.size());
    for (int i = 0; i < 10; i++) {
      // check if the rows are returned and in order
      assertArrayEquals(Bytes.toBytes("testSmallScan" + i), results.get(i).getRow());
    }

    // check that we are at the end of the scan
    results = handler.getScannerRows(scanId, 10);
    assertEquals(0, results.size());

    // close scanner and check that it was indeed closed
    handler.closeScanner(scanId);
    try {
      handler.getScannerRows(scanId, 10);
      fail("Scanner id should be invalid");
    } catch (TIllegalArgument e) {
    }
  }

  @Test
  public void testExpiredScanner() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setLong(HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 1000);
    ThriftHBaseServiceHandler handler =
      new ThriftHBaseServiceHandler(conf, UserProvider.instantiate(conf));

    TScan scan = new TScan();
    ByteBuffer table = wrap(tableAname);

    int scannerId = handler.openScanner(table, scan);
    handler.getScannerRows(scannerId, 1);
    Thread.sleep(1000);

    try {
      handler.getScannerRows(scannerId, 1);
      fail("The scanner should be expired and have an TIllegalArgument exception here.");
    } catch (TIllegalArgument e) {
      assertEquals("Invalid scanner Id", e.getMessage());
    } finally {
      conf.setLong(HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,
        DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD);
    }
  }

  @Test
  public void testPutTTL() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = Bytes.toBytes("testPutTTL");
    ByteBuffer table = wrap(tableAname);
    List<TColumnValue> columnValues = new ArrayList<>(1);

    // Add some dummy data
    columnValues.add(
        new TColumnValue(
            wrap(familyAname),
            wrap(qualifierAname),
            wrap(Bytes.toBytes(1L))));


    TPut put = new TPut(wrap(rowName), columnValues);
    put.setColumnValues(columnValues);

    Map<ByteBuffer, ByteBuffer> attributes = new HashMap<>();

    // Time in ms for the kv's to live.
    long ttlTimeMs = 2000L;

    // the _ttl attribute is a number of ms ttl for key values in this put.
    attributes.put(wrap(Bytes.toBytes("_ttl")), wrap(Bytes.toBytes(ttlTimeMs)));
    // Attach the attributes
    put.setAttributes(attributes);
    // Send it.
    handler.put(table, put);

    // Now get the data back
    TGet getOne = new TGet(wrap(rowName));
    TResult resultOne = handler.get(table, getOne);

    // It's there.
    assertArrayEquals(rowName, resultOne.getRow());
    assertEquals(1, resultOne.getColumnValuesSize());

    // Sleep 30 seconds just to make 100% sure that the key value should be expired.
    Thread.sleep(ttlTimeMs * 15);

    TGet getTwo = new TGet(wrap(rowName));
    TResult resultTwo = handler.get(table, getTwo);


    // Nothing should be there since it's ttl'd out.
    assertNull(resultTwo.getRow());
    assertEquals(0, resultTwo.getColumnValuesSize());
  }

  /**
   * Padding numbers to make comparison of sort order easier in a for loop
   *
   * @param n  The number to pad.
   * @param pad  The length to pad up to.
   * @return The padded number as a string.
   */
  private String pad(int n, byte pad) {
    String res = Integer.toString(n);
    while (res.length() < pad) {
      res = "0" + res;
    }
    return res;
  }

  @Test
  public void testScanWithBatchSize() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    ByteBuffer table = wrap(tableAname);

    // insert data
    List<TColumnValue> columnValues = new ArrayList<>(100);
    for (int i = 0; i < 100; i++) {
      String colNum = pad(i, (byte) 3);
      TColumnValue columnValue = new TColumnValue(wrap(familyAname),
        wrap(Bytes.toBytes("col" + colNum)), wrap(Bytes.toBytes("val" + colNum)));
      columnValues.add(columnValue);
    }
    TPut put = new TPut(wrap(Bytes.toBytes("testScanWithBatchSize")), columnValues);
    handler.put(table, put);

    // create scan instance
    TScan scan = new TScan();
    List<TColumn> columns = new ArrayList<>(1);
    TColumn column = new TColumn();
    column.setFamily(familyAname);
    columns.add(column);
    scan.setColumns(columns);
    scan.setStartRow(Bytes.toBytes("testScanWithBatchSize"));
    scan.setStopRow(Bytes.toBytes("testScanWithBatchSize\uffff"));
    // set batch size to 10 columns per call
    scan.setBatchSize(10);

    // get scanner
    int scanId = handler.openScanner(table, scan);
    List<TResult> results = null;
    for (int i = 0; i < 10; i++) {
      // get batch for single row (10x10 is what we expect)
      results = handler.getScannerRows(scanId, 1);
      assertEquals(1, results.size());
      // check length of batch
      List<TColumnValue> cols = results.get(0).getColumnValues();
      assertEquals(10, cols.size());
      // check if the columns are returned and in order
      for (int y = 0; y < 10; y++) {
        int colNum = y + (10 * i);
        String colNumPad = pad(colNum, (byte) 3);
        assertArrayEquals(Bytes.toBytes("col" + colNumPad), cols.get(y).getQualifier());
      }
    }

    // check that we are at the end of the scan
    results = handler.getScannerRows(scanId, 1);
    assertEquals(0, results.size());

    // close scanner and check that it was indeed closed
    handler.closeScanner(scanId);
    try {
      handler.getScannerRows(scanId, 1);
      fail("Scanner id should be invalid");
    } catch (TIllegalArgument e) {
    }
  }

  @Test
  public void testGetScannerResults() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    ByteBuffer table = wrap(tableAname);

    // insert data
    TColumnValue columnValue =
        new TColumnValue(wrap(familyAname), wrap(qualifierAname), wrap(valueAname));
    List<TColumnValue> columnValues = new ArrayList<>(1);
    columnValues.add(columnValue);
    for (int i = 0; i < 20; i++) {
      TPut put =
          new TPut(wrap(Bytes.toBytes("testGetScannerResults" + pad(i, (byte) 2))), columnValues);
      handler.put(table, put);
    }

    // create scan instance
    TScan scan = new TScan();
    List<TColumn> columns = new ArrayList<>(1);
    TColumn column = new TColumn();
    column.setFamily(familyAname);
    column.setQualifier(qualifierAname);
    columns.add(column);
    scan.setColumns(columns);
    scan.setStartRow(Bytes.toBytes("testGetScannerResults"));

    // get 5 rows and check the returned results
    scan.setStopRow(Bytes.toBytes("testGetScannerResults05"));
    List<TResult> results = handler.getScannerResults(table, scan, 5);
    assertEquals(5, results.size());
    for (int i = 0; i < 5; i++) {
      // check if the rows are returned and in order
      assertArrayEquals(Bytes.toBytes("testGetScannerResults" + pad(i, (byte) 2)), results.get(i)
          .getRow());
    }

    // get 10 rows and check the returned results
    scan.setStopRow(Bytes.toBytes("testGetScannerResults10"));
    results = handler.getScannerResults(table, scan, 10);
    assertEquals(10, results.size());
    for (int i = 0; i < 10; i++) {
      // check if the rows are returned and in order
      assertArrayEquals(Bytes.toBytes("testGetScannerResults" + pad(i, (byte) 2)), results.get(i)
          .getRow());
    }

    // get 20 rows and check the returned results
    scan.setStopRow(Bytes.toBytes("testGetScannerResults20"));
    results = handler.getScannerResults(table, scan, 20);
    assertEquals(20, results.size());
    for (int i = 0; i < 20; i++) {
      // check if the rows are returned and in order
      assertArrayEquals(Bytes.toBytes("testGetScannerResults" + pad(i, (byte) 2)), results.get(i)
          .getRow());
    }

    // reverse scan
    scan = new TScan();
    scan.setColumns(columns);
    scan.setReversed(true);
    scan.setStartRow(Bytes.toBytes("testGetScannerResults20"));
    scan.setStopRow(Bytes.toBytes("testGetScannerResults"));
    results = handler.getScannerResults(table, scan, 20);
    assertEquals(20, results.size());
    for (int i = 0; i < 20; i++) {
      // check if the rows are returned and in order
      assertArrayEquals(Bytes.toBytes("testGetScannerResults" + pad(19 - i, (byte) 2)),
          results.get(i).getRow());
    }
  }

  @Test
  public void testFilterRegistration() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.set("hbase.thrift.filters", "MyFilter:filterclass");
    ThriftServer.registerFilters(conf);
    Map<String, String> registeredFilters = ParseFilter.getAllFilters();
    assertEquals("filterclass", registeredFilters.get("MyFilter"));
  }

  @Test
  public void testMetrics() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    ThriftMetrics metrics = getMetrics(conf);
    ThriftHBaseServiceHandler hbaseHandler = createHandler();
    THBaseService.Iface handler =
        HbaseHandlerMetricsProxy.newInstance(hbaseHandler, metrics,  conf);
    byte[] rowName = Bytes.toBytes("testMetrics");
    ByteBuffer table = wrap(tableAname);

    TGet get = new TGet(wrap(rowName));
    assertFalse(handler.exists(table, get));

    List<TColumnValue> columnValues = new ArrayList<>(2);
    columnValues.add(new TColumnValue(wrap(familyAname), wrap(qualifierAname), wrap(valueAname)));
    columnValues.add(new TColumnValue(wrap(familyBname), wrap(qualifierBname),  wrap(valueBname)));
    TPut put = new TPut(wrap(rowName), columnValues);
    put.setColumnValues(columnValues);

    handler.put(table, put);

    assertTrue(handler.exists(table, get));
    metricsHelper.assertCounter("put_num_ops", 1, metrics.getSource());
    metricsHelper.assertCounter("exists_num_ops", 2, metrics.getSource());
  }

  private static ThriftMetrics getMetrics(Configuration conf) throws Exception {
    ThriftMetrics m = new ThriftMetrics(conf, ThriftMetrics.ThriftServerType.TWO);
    m.getSource().init(); //Clear all the metrics
    return m;
  }

  @Test
  public void testMetricsWithException() throws Exception {
    byte[] rowkey = Bytes.toBytes("row1");
    byte[] family = Bytes.toBytes("f");
    byte[] col = Bytes.toBytes("c");
    // create a table which will throw exceptions for requests
    TableName tableName = TableName.valueOf(name.getMethodName());
    HTableDescriptor tableDesc = new HTableDescriptor(tableName);
    tableDesc.addCoprocessor(ErrorThrowingGetObserver.class.getName());
    tableDesc.addFamily(new HColumnDescriptor(family));

    Table table = UTIL.createTable(tableDesc, null);
    table.put(new Put(rowkey).addColumn(family, col, Bytes.toBytes("val1")));

    ThriftHBaseServiceHandler hbaseHandler = createHandler();
    ThriftMetrics metrics = getMetrics(UTIL.getConfiguration());
    THBaseService.Iface handler =
        HbaseHandlerMetricsProxy.newInstance(hbaseHandler, metrics, null);
    ByteBuffer tTableName = wrap(tableName.getName());

    // check metrics increment with a successful get
    long preGetCounter = metricsHelper.checkCounterExists("get_num_ops", metrics.getSource()) ?
        metricsHelper.getCounter("get_num_ops", metrics.getSource()) :
        0;
    TGet tGet = new TGet(wrap(rowkey));
    TResult tResult = handler.get(tTableName, tGet);

    List<TColumnValue> expectedColumnValues = Lists.newArrayList(
        new TColumnValue(wrap(family), wrap(col), wrap(Bytes.toBytes("val1")))
    );
    assertArrayEquals(rowkey, tResult.getRow());
    List<TColumnValue> returnedColumnValues = tResult.getColumnValues();
    assertTColumnValuesEqual(expectedColumnValues, returnedColumnValues);

    metricsHelper.assertCounter("get_num_ops", preGetCounter + 1, metrics.getSource());

    // check metrics increment when the get throws each exception type
    for (ErrorThrowingGetObserver.ErrorType type : ErrorThrowingGetObserver.ErrorType.values()) {
      testExceptionType(handler, metrics, tTableName, rowkey, type);
    }
  }

  private void testExceptionType(THBaseService.Iface handler, ThriftMetrics metrics,
      ByteBuffer tTableName, byte[] rowkey, ErrorThrowingGetObserver.ErrorType errorType) {
    long preGetCounter = metricsHelper.getCounter("get_num_ops", metrics.getSource());
    String exceptionKey = errorType.getMetricName();
    long preExceptionCounter = metricsHelper.checkCounterExists(exceptionKey, metrics.getSource()) ?
        metricsHelper.getCounter(exceptionKey, metrics.getSource()) :
        0;
    TGet tGet = new TGet(wrap(rowkey));
    Map<ByteBuffer, ByteBuffer> attributes = new HashMap<>();
    attributes.put(wrap(Bytes.toBytes(ErrorThrowingGetObserver.SHOULD_ERROR_ATTRIBUTE)),
        wrap(Bytes.toBytes(errorType.name())));
    tGet.setAttributes(attributes);
    try {
      TResult tResult = handler.get(tTableName, tGet);
      fail("Get with error attribute should have thrown an exception");
    } catch (TException e) {
      LOG.info("Received exception: ", e);
      metricsHelper.assertCounter("get_num_ops", preGetCounter + 1, metrics.getSource());
      metricsHelper.assertCounter(exceptionKey, preExceptionCounter + 1, metrics.getSource());
    }

  }

  /**
   * See HBASE-17611
   *
   * Latency metrics were capped at ~ 2 seconds due to the use of an int variable to capture the
   * duration.
   */
  @Test
  public void testMetricsPrecision() throws Exception {
    byte[] rowkey = Bytes.toBytes("row1");
    byte[] family = Bytes.toBytes("f");
    byte[] col = Bytes.toBytes("c");
    // create a table which will throw exceptions for requests
    TableName tableName = TableName.valueOf("testMetricsPrecision");
    HTableDescriptor tableDesc = new HTableDescriptor(tableName);
    tableDesc.addCoprocessor(DelayingRegionObserver.class.getName());
    tableDesc.addFamily(new HColumnDescriptor(family));

    Table table = null;
    try {
      table = UTIL.createTable(tableDesc, null);

      table.put(new Put(rowkey).addColumn(family, col, Bytes.toBytes("val1")));

      ThriftHBaseServiceHandler hbaseHandler = createHandler();
      ThriftMetrics metrics = getMetrics(UTIL.getConfiguration());
      THBaseService.Iface handler =
          HbaseHandlerMetricsProxy.newInstance(hbaseHandler, metrics, null);
      ByteBuffer tTableName = wrap(tableName.getName());

      // check metrics latency with a successful get
      TGet tGet = new TGet(wrap(rowkey));
      TResult tResult = handler.get(tTableName, tGet);

      List<TColumnValue> expectedColumnValues = Lists.newArrayList(
          new TColumnValue(wrap(family), wrap(col), wrap(Bytes.toBytes("val1")))
      );
      assertArrayEquals(rowkey, tResult.getRow());
      List<TColumnValue> returnedColumnValues = tResult.getColumnValues();
      assertTColumnValuesEqual(expectedColumnValues, returnedColumnValues);

      metricsHelper.assertGaugeGt("get_max", 3000L, metrics.getSource());
    } finally {
      if (table != null) {
        try {
          table.close();
        } catch (IOException ignored) {
        }
        UTIL.deleteTable(tableName);
      }
    }
  }


  @Test
  public void testAttribute() throws Exception {
    byte[] rowName = Bytes.toBytes("testAttribute");
    byte[] attributeKey = Bytes.toBytes("attribute1");
    byte[] attributeValue = Bytes.toBytes("value1");
    Map<ByteBuffer, ByteBuffer> attributes = new HashMap<>();
    attributes.put(wrap(attributeKey), wrap(attributeValue));

    TGet tGet = new TGet(wrap(rowName));
    tGet.setAttributes(attributes);
    Get get = getFromThrift(tGet);
    assertArrayEquals(get.getAttribute("attribute1"), attributeValue);

    List<TColumnValue> columnValues = new ArrayList<>(1);
    columnValues.add(new TColumnValue(wrap(familyAname), wrap(qualifierAname), wrap(valueAname)));
    TPut tPut = new TPut(wrap(rowName) , columnValues);
    tPut.setAttributes(attributes);
    Put put = putFromThrift(tPut);
    assertArrayEquals(put.getAttribute("attribute1"), attributeValue);

    TScan tScan = new TScan();
    tScan.setAttributes(attributes);
    Scan scan = scanFromThrift(tScan);
    assertArrayEquals(scan.getAttribute("attribute1"), attributeValue);

    List<TColumnIncrement> incrementColumns = new ArrayList<>(1);
    incrementColumns.add(new TColumnIncrement(wrap(familyAname), wrap(qualifierAname)));
    TIncrement tIncrement = new TIncrement(wrap(rowName), incrementColumns);
    tIncrement.setAttributes(attributes);
    Increment increment = incrementFromThrift(tIncrement);
    assertArrayEquals(increment.getAttribute("attribute1"), attributeValue);

    TDelete tDelete = new TDelete(wrap(rowName));
    tDelete.setAttributes(attributes);
    Delete delete = deleteFromThrift(tDelete);
    assertArrayEquals(delete.getAttribute("attribute1"), attributeValue);
  }

  /**
   * Put valueA to a row, make sure put has happened, then create a mutation object to put valueB
   * and delete ValueA, then check that the row value is only valueB.
   */
  @Test
  public void testMutateRow() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = Bytes.toBytes("testMutateRow");
    ByteBuffer table = wrap(tableAname);

    List<TColumnValue> columnValuesA = new ArrayList<>(1);
    TColumnValue columnValueA = new TColumnValue(wrap(familyAname), wrap(qualifierAname),
        wrap(valueAname));
    columnValuesA.add(columnValueA);
    TPut putA = new TPut(wrap(rowName), columnValuesA);
    putA.setColumnValues(columnValuesA);

    handler.put(table,putA);

    TGet get = new TGet(wrap(rowName));
    TResult result = handler.get(table, get);
    assertArrayEquals(rowName, result.getRow());
    List<TColumnValue> returnedColumnValues = result.getColumnValues();

    List<TColumnValue> expectedColumnValues = new ArrayList<>(1);
    expectedColumnValues.add(columnValueA);
    assertTColumnValuesEqual(expectedColumnValues, returnedColumnValues);

    List<TColumnValue> columnValuesB = new ArrayList<>(1);
    TColumnValue columnValueB = new TColumnValue(wrap(familyAname), wrap(qualifierBname),
        wrap(valueBname));
    columnValuesB.add(columnValueB);
    TPut putB = new TPut(wrap(rowName), columnValuesB);
    putB.setColumnValues(columnValuesB);

    TDelete delete = new TDelete(wrap(rowName));
    List<TColumn> deleteColumns = new ArrayList<>(1);
    TColumn deleteColumn = new TColumn(wrap(familyAname));
    deleteColumn.setQualifier(qualifierAname);
    deleteColumns.add(deleteColumn);
    delete.setColumns(deleteColumns);

    List<TMutation> mutations = new ArrayList<>(2);
    TMutation mutationA = TMutation.put(putB);
    mutations.add(mutationA);

    TMutation mutationB = TMutation.deleteSingle(delete);
    mutations.add(mutationB);

    TRowMutations tRowMutations = new TRowMutations(wrap(rowName),mutations);
    handler.mutateRow(table,tRowMutations);

    result = handler.get(table, get);
    assertArrayEquals(rowName, result.getRow());
    returnedColumnValues = result.getColumnValues();

    expectedColumnValues = new ArrayList<>(1);
    expectedColumnValues.add(columnValueB);
    assertTColumnValuesEqual(expectedColumnValues, returnedColumnValues);
  }

  /**
   * Create TPut, TDelete , TIncrement objects, set durability then call ThriftUtility
   * functions to get Put , Delete and Increment respectively. Use getDurability to make sure
   * the returned objects have the appropriate durability setting.
   */
  @Test
  public void testDurability() throws Exception {
    byte[] rowName = Bytes.toBytes("testDurability");
    List<TColumnValue> columnValues = new ArrayList<>(1);
    columnValues.add(new TColumnValue(wrap(familyAname), wrap(qualifierAname), wrap(valueAname)));

    List<TColumnIncrement> incrementColumns = new ArrayList<>(1);
    incrementColumns.add(new TColumnIncrement(wrap(familyAname), wrap(qualifierAname)));

    TDelete tDelete = new TDelete(wrap(rowName));
    tDelete.setDurability(TDurability.SKIP_WAL);
    Delete delete = deleteFromThrift(tDelete);
    assertEquals(Durability.SKIP_WAL, delete.getDurability());

    tDelete.setDurability(TDurability.ASYNC_WAL);
    delete = deleteFromThrift(tDelete);
    assertEquals(Durability.ASYNC_WAL, delete.getDurability());

    tDelete.setDurability(TDurability.SYNC_WAL);
    delete = deleteFromThrift(tDelete);
    assertEquals(Durability.SYNC_WAL, delete.getDurability());

    tDelete.setDurability(TDurability.FSYNC_WAL);
    delete = deleteFromThrift(tDelete);
    assertEquals(Durability.FSYNC_WAL, delete.getDurability());

    TPut tPut = new TPut(wrap(rowName), columnValues);
    tPut.setDurability(TDurability.SKIP_WAL);
    Put put = putFromThrift(tPut);
    assertEquals(Durability.SKIP_WAL, put.getDurability());

    tPut.setDurability(TDurability.ASYNC_WAL);
    put = putFromThrift(tPut);
    assertEquals(Durability.ASYNC_WAL, put.getDurability());

    tPut.setDurability(TDurability.SYNC_WAL);
    put = putFromThrift(tPut);
    assertEquals(Durability.SYNC_WAL, put.getDurability());

    tPut.setDurability(TDurability.FSYNC_WAL);
    put = putFromThrift(tPut);
    assertEquals(Durability.FSYNC_WAL, put.getDurability());

    TIncrement tIncrement = new TIncrement(wrap(rowName), incrementColumns);

    tIncrement.setDurability(TDurability.SKIP_WAL);
    Increment increment = incrementFromThrift(tIncrement);
    assertEquals(Durability.SKIP_WAL, increment.getDurability());

    tIncrement.setDurability(TDurability.ASYNC_WAL);
    increment = incrementFromThrift(tIncrement);
    assertEquals(Durability.ASYNC_WAL, increment.getDurability());

    tIncrement.setDurability(TDurability.SYNC_WAL);
    increment = incrementFromThrift(tIncrement);
    assertEquals(Durability.SYNC_WAL, increment.getDurability());

    tIncrement.setDurability(TDurability.FSYNC_WAL);
    increment = incrementFromThrift(tIncrement);
    assertEquals(Durability.FSYNC_WAL, increment.getDurability());
  }

  @Test
  public void testCheckAndMutate() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    ByteBuffer table = wrap(tableAname);
    ByteBuffer row = wrap(Bytes.toBytes("row"));
    ByteBuffer family = wrap(familyAname);
    ByteBuffer qualifier = wrap(qualifierAname);
    ByteBuffer value = wrap(valueAname);

    // Create a mutation to write to 'B', our "mutate" of "checkAndMutate"
    List<TColumnValue> columnValuesB = new ArrayList<>(1);
    TColumnValue columnValueB = new TColumnValue(family, wrap(qualifierBname), wrap(valueBname));
    columnValuesB.add(columnValueB);
    TPut putB = new TPut(row, columnValuesB);
    putB.setColumnValues(columnValuesB);

    TRowMutations tRowMutations = new TRowMutations(row,
        Arrays.<TMutation> asList(TMutation.put(putB)));

    // Empty table when we begin
    TResult result = handler.get(table, new TGet(row));
    assertEquals(0, result.getColumnValuesSize());

    // checkAndMutate -- condition should fail because the value doesn't exist.
    assertFalse("Expected condition to not pass",
        handler.checkAndMutate(table, row, family, qualifier, TCompareOp.EQUAL, value,
            tRowMutations));

    List<TColumnValue> columnValuesA = new ArrayList<>(1);
    TColumnValue columnValueA = new TColumnValue(family, qualifier, value);
    columnValuesA.add(columnValueA);

    // Put an update 'A'
    handler.put(table, new TPut(row, columnValuesA));

    // Verify that the update is there
    result = handler.get(table, new TGet(row));
    assertEquals(1, result.getColumnValuesSize());
    assertTColumnValueEqual(columnValueA, result.getColumnValues().get(0));

    // checkAndMutate -- condition should pass since we added the value
    assertTrue("Expected condition to pass",
        handler.checkAndMutate(table, row, family, qualifier, TCompareOp.EQUAL, value,
            tRowMutations));

    result = handler.get(table, new TGet(row));
    assertEquals(2, result.getColumnValuesSize());
    assertTColumnValueEqual(columnValueA, result.getColumnValues().get(0));
    assertTColumnValueEqual(columnValueB, result.getColumnValues().get(1));
  }

  @Test
  public void testConsistency() throws Exception {
    byte[] rowName = Bytes.toBytes("testConsistency");
    TGet tGet = new TGet(wrap(rowName));
    tGet.setConsistency(TConsistency.STRONG);
    Get get = getFromThrift(tGet);
    assertEquals(Consistency.STRONG, get.getConsistency());

    tGet.setConsistency(TConsistency.TIMELINE);
    tGet.setTargetReplicaId(1);
    get = getFromThrift(tGet);
    assertEquals(Consistency.TIMELINE, get.getConsistency());
    assertEquals(1, get.getReplicaId());

    TScan tScan = new TScan();
    tScan.setConsistency(TConsistency.STRONG);
    Scan scan = scanFromThrift(tScan);
    assertEquals(Consistency.STRONG, scan.getConsistency());

    tScan.setConsistency(TConsistency.TIMELINE);
    tScan.setTargetReplicaId(1);
    scan = scanFromThrift(tScan);
    assertEquals(Consistency.TIMELINE, scan.getConsistency());
    assertEquals(1, scan.getReplicaId());

    TResult tResult = new TResult();
    assertFalse(tResult.isSetStale());
    tResult.setStale(true);
    assertTrue(tResult.isSetStale());
  }

  @Test
  public void testDDLOpertions() throws Exception {
    String namespace = "testDDLOpertionsNamespace";
    String table = "testDDLOpertionsTable";
    TTableName tTableName = new TTableName();
    tTableName.setNs(Bytes.toBytes(namespace));
    tTableName.setQualifier(Bytes.toBytes(table));
    ThriftHBaseServiceHandler handler = createHandler();
    //create name space
    TNamespaceDescriptor namespaceDescriptor = new TNamespaceDescriptor();
    namespaceDescriptor.setName(namespace);
    namespaceDescriptor.putToConfiguration("key1", "value1");
    namespaceDescriptor.putToConfiguration("key2", "value2");
    handler.createNamespace(namespaceDescriptor);
    //list namespace
    List<TNamespaceDescriptor> namespaceDescriptors = handler.listNamespaceDescriptors();
    // should have 3 namespace, default hbase and testDDLOpertionsNamespace
    assertTrue(namespaceDescriptors.size() == 3);
    //modify namesapce
    namespaceDescriptor.putToConfiguration("kye3", "value3");
    handler.modifyNamespace(namespaceDescriptor);
    //get namespace
    TNamespaceDescriptor namespaceDescriptorReturned = handler.getNamespaceDescriptor(namespace);
    assertTrue(namespaceDescriptorReturned.getConfiguration().size() == 3);
    //create table
    TTableDescriptor tableDescriptor = new TTableDescriptor();
    tableDescriptor.setTableName(tTableName);
    TColumnFamilyDescriptor columnFamilyDescriptor1 = new TColumnFamilyDescriptor();
    columnFamilyDescriptor1.setName(familyAname);
    columnFamilyDescriptor1.setDataBlockEncoding(TDataBlockEncoding.DIFF);
    tableDescriptor.addToColumns(columnFamilyDescriptor1);
    List<ByteBuffer> splitKeys = new ArrayList<>();
    splitKeys.add(ByteBuffer.wrap(Bytes.toBytes(5)));
    handler.createTable(tableDescriptor, splitKeys);
    //modify table
    tableDescriptor.setDurability(TDurability.ASYNC_WAL);
    handler.modifyTable(tableDescriptor);
    //modify column family
    columnFamilyDescriptor1.setInMemory(true);
    handler.modifyColumnFamily(tTableName, columnFamilyDescriptor1);
    //add column family
    TColumnFamilyDescriptor columnFamilyDescriptor2 = new TColumnFamilyDescriptor();
    columnFamilyDescriptor2.setName(familyBname);
    columnFamilyDescriptor2.setDataBlockEncoding(TDataBlockEncoding.PREFIX);
    handler.addColumnFamily(tTableName, columnFamilyDescriptor2);
    //get table descriptor
    TTableDescriptor tableDescriptorReturned = handler.getTableDescriptor(tTableName);
    assertTrue(tableDescriptorReturned.getColumns().size() == 2);
    assertTrue(tableDescriptorReturned.getDurability() ==  TDurability.ASYNC_WAL);
    TColumnFamilyDescriptor columnFamilyDescriptor1Returned = tableDescriptorReturned.getColumns()
        .stream().filter(desc -> Bytes.equals(desc.getName(), familyAname)).findFirst().get();
    assertTrue(columnFamilyDescriptor1Returned.isInMemory() == true);
    //delete column family
    handler.deleteColumnFamily(tTableName, ByteBuffer.wrap(familyBname));
    tableDescriptorReturned = handler.getTableDescriptor(tTableName);
    assertTrue(tableDescriptorReturned.getColumns().size() == 1);
    //disable table
    handler.disableTable(tTableName);
    assertTrue(handler.isTableDisabled(tTableName));
    //enable table
    handler.enableTable(tTableName);
    assertTrue(handler.isTableEnabled(tTableName));
    assertTrue(handler.isTableAvailable(tTableName));
    //truncate table
    handler.disableTable(tTableName);
    handler.truncateTable(tTableName, true);
    assertTrue(handler.isTableAvailable(tTableName));
    //delete table
    handler.disableTable(tTableName);
    handler.deleteTable(tTableName);
    assertFalse(handler.tableExists(tTableName));
    //delete namespace
    handler.deleteNamespace(namespace);
    namespaceDescriptors = handler.listNamespaceDescriptors();
    // should have 2 namespace, default and hbase
    assertTrue(namespaceDescriptors.size() == 2);
  }

  @Test
  public void testGetTableDescriptor() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    TTableDescriptor tableDescriptor = handler
        .getTableDescriptor(ThriftUtilities.tableNameFromHBase(TableName.valueOf(tableAname)));
    TableDescriptor table = ThriftUtilities.tableDescriptorFromThrift(tableDescriptor);
    assertTrue(table.getTableName().equals(TableName.valueOf(tableAname)));
    assertTrue(table.getColumnFamilies().length == 2);
    assertTrue(table.getColumnFamily(familyAname).getMaxVersions() == 3);
    assertTrue(table.getColumnFamily(familyBname).getMaxVersions() == 2);
  }

  @Test
  public void testGetThriftServerType() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    assertEquals(TThriftServerType.TWO, handler.getThriftServerType());
  }

  /**
   * Verify that thrift2 client calling thrift server can get the thrift server type correctly.
   */
  @Test
  public void testGetThriftServerOneType() throws Exception {

    // start a thrift server
    HBaseThriftTestingUtility THRIFT_TEST_UTIL = new HBaseThriftTestingUtility();

    LOG.info("Starting HBase Thrift server One");
    THRIFT_TEST_UTIL.startThriftServer(UTIL.getConfiguration(), ThriftServerType.ONE);
    try (TTransport transport = new TSocket(InetAddress.getLocalHost().getHostName(),
        THRIFT_TEST_UTIL.getServerPort())){
      TProtocol protocol = new TBinaryProtocol(transport);
      // This is our thrift2 client.
      THBaseService.Iface client = new THBaseService.Client(protocol);
      // open the transport
      transport.open();
      assertEquals(TThriftServerType.ONE.name(), client.getThriftServerType().name());
    } finally {
      THRIFT_TEST_UTIL.stopThriftServer();
    }
  }

  @Test
  public void testSlowLogResponses() throws Exception {

    // start a thrift server
    HBaseThriftTestingUtility THRIFT_TEST_UTIL = new HBaseThriftTestingUtility();
    Configuration configuration = UTIL.getConfiguration();
    configuration.setBoolean("hbase.regionserver.slowlog.buffer.enabled", true);

    THRIFT_TEST_UTIL.startThriftServer(configuration, ThriftServerType.ONE);
    ThriftHBaseServiceHandler thriftHBaseServiceHandler =
      new ThriftHBaseServiceHandler(configuration,
        UserProvider.instantiate(configuration));
    Collection<ServerName> serverNames = UTIL.getAdmin().getRegionServers();
    Set<TServerName> tServerNames =
      ThriftUtilities.getServerNamesFromHBase(new HashSet<>(serverNames));
    List<Boolean> clearedResponses =
      thriftHBaseServiceHandler.clearSlowLogResponses(tServerNames);
    clearedResponses.forEach(Assert::assertTrue);
    TLogQueryFilter tLogQueryFilter = new TLogQueryFilter();
    tLogQueryFilter.setLimit(15);
    Assert.assertEquals(tLogQueryFilter.getFilterByOperator(), TFilterByOperator.OR);
    LogQueryFilter logQueryFilter = ThriftUtilities.getSlowLogQueryFromThrift(tLogQueryFilter);
    Assert.assertEquals(logQueryFilter.getFilterByOperator(), LogQueryFilter.FilterByOperator.OR);
    tLogQueryFilter.setFilterByOperator(TFilterByOperator.AND);
    logQueryFilter = ThriftUtilities.getSlowLogQueryFromThrift(tLogQueryFilter);
    Assert.assertEquals(logQueryFilter.getFilterByOperator(), LogQueryFilter.FilterByOperator.AND);
    List<TOnlineLogRecord> tLogRecords =
      thriftHBaseServiceHandler.getSlowLogResponses(tServerNames, tLogQueryFilter);
    assertEquals(tLogRecords.size(), 0);
  }

  public static class DelayingRegionObserver implements RegionCoprocessor, RegionObserver {
    private static final Logger LOG = LoggerFactory.getLogger(DelayingRegionObserver.class);
    // sleep time in msec
    private long delayMillis;

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
      this.delayMillis = e.getConfiguration()
          .getLong("delayingregionobserver.delay", 3000);
    }

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get,
                         List<Cell> results) throws IOException {
      try {
        long start = System.currentTimeMillis();
        TimeUnit.MILLISECONDS.sleep(delayMillis);
        if (LOG.isTraceEnabled()) {
          LOG.trace("Slept for " + (System.currentTimeMillis() - start) + " msec");
        }
      } catch (InterruptedException ie) {
        throw new InterruptedIOException("Interrupted while sleeping");
      }
    }
  }
}

