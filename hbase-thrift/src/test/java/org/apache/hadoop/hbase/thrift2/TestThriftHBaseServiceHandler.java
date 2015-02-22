/*
 *
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.thrift.ThriftMetrics;
import org.apache.hadoop.hbase.thrift2.generated.TAppend;
import org.apache.hadoop.hbase.thrift2.generated.TColumn;
import org.apache.hadoop.hbase.thrift2.generated.TColumnIncrement;
import org.apache.hadoop.hbase.thrift2.generated.TColumnValue;
import org.apache.hadoop.hbase.thrift2.generated.TDelete;
import org.apache.hadoop.hbase.thrift2.generated.TDeleteType;
import org.apache.hadoop.hbase.thrift2.generated.TGet;
import org.apache.hadoop.hbase.thrift2.generated.THBaseService;
import org.apache.hadoop.hbase.thrift2.generated.TIOError;
import org.apache.hadoop.hbase.thrift2.generated.TIllegalArgument;
import org.apache.hadoop.hbase.thrift2.generated.TIncrement;
import org.apache.hadoop.hbase.thrift2.generated.TPut;
import org.apache.hadoop.hbase.thrift2.generated.TResult;
import org.apache.hadoop.hbase.thrift2.generated.TScan;
import org.apache.hadoop.hbase.thrift2.generated.TMutation;
import org.apache.hadoop.hbase.thrift2.generated.TRowMutations;
import org.apache.hadoop.hbase.thrift2.generated.TDurability;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import static org.apache.hadoop.hbase.thrift2.ThriftUtilities.getFromThrift;
import static org.apache.hadoop.hbase.thrift2.ThriftUtilities.putFromThrift;
import static org.apache.hadoop.hbase.thrift2.ThriftUtilities.scanFromThrift;
import static org.apache.hadoop.hbase.thrift2.ThriftUtilities.incrementFromThrift;
import static org.apache.hadoop.hbase.thrift2.ThriftUtilities.deleteFromThrift;
import static org.junit.Assert.*;
import static java.nio.ByteBuffer.wrap;

/**
 * Unit testing for ThriftServer.HBaseHandler, a part of the org.apache.hadoop.hbase.thrift2
 * package.
 */
@Category({ClientTests.class, MediumTests.class})
public class TestThriftHBaseServiceHandler {

  public static final Log LOG = LogFactory.getLog(TestThriftHBaseServiceHandler.class);
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
      assertArrayEquals(a.getFamily(), b.getFamily());
      assertArrayEquals(a.getQualifier(), b.getQualifier());
      assertArrayEquals(a.getValue(), b.getValue());
    }
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    UTIL.startMiniCluster();
    Admin admin = UTIL.getHBaseAdmin();
    HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableAname));
    for (HColumnDescriptor family : families) {
      tableDescriptor.addFamily(family);
    }
    admin.createTable(tableDescriptor);
    admin.close();
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
    byte[] rowName = "testExists".getBytes();
    ByteBuffer table = wrap(tableAname);

    TGet get = new TGet(wrap(rowName));
    assertFalse(handler.exists(table, get));

    List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
    columnValues.add(new TColumnValue(wrap(familyAname), wrap(qualifierAname), wrap(valueAname)));
    columnValues.add(new TColumnValue(wrap(familyBname), wrap(qualifierBname), wrap(valueBname)));
    TPut put = new TPut(wrap(rowName), columnValues);
    put.setColumnValues(columnValues);

    handler.put(table, put);

    assertTrue(handler.exists(table, get));
  }

  @Test
  public void testPutGet() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = "testPutGet".getBytes();
    ByteBuffer table = wrap(tableAname);

    List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
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
    byte[] rowName1 = "testPutGetMultiple1".getBytes();
    byte[] rowName2 = "testPutGetMultiple2".getBytes();

    List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
    columnValues.add(new TColumnValue(wrap(familyAname), wrap(qualifierAname), wrap(valueAname)));
    columnValues.add(new TColumnValue(wrap(familyBname), wrap(qualifierBname), wrap(valueBname)));
    List<TPut> puts = new ArrayList<TPut>();
    puts.add(new TPut(wrap(rowName1), columnValues));
    puts.add(new TPut(wrap(rowName2), columnValues));

    handler.putMultiple(table, puts);

    List<TGet> gets = new ArrayList<TGet>();
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
    byte[] rowName1 = "testDeleteMultiple1".getBytes();
    byte[] rowName2 = "testDeleteMultiple2".getBytes();

    List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
    columnValues.add(new TColumnValue(wrap(familyAname), wrap(qualifierAname), wrap(valueAname)));
    columnValues.add(new TColumnValue(wrap(familyBname), wrap(qualifierBname), wrap(valueBname)));
    List<TPut> puts = new ArrayList<TPut>();
    puts.add(new TPut(wrap(rowName1), columnValues));
    puts.add(new TPut(wrap(rowName2), columnValues));

    handler.putMultiple(table, puts);

    List<TDelete> deletes = new ArrayList<TDelete>();
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
    byte[] rowName = "testDelete".getBytes();
    ByteBuffer table = wrap(tableAname);

    List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
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
    List<TColumn> deleteColumns = new ArrayList<TColumn>();
    TColumn deleteColumn = new TColumn(wrap(familyAname));
    deleteColumn.setQualifier(qualifierAname);
    deleteColumns.add(deleteColumn);
    delete.setColumns(deleteColumns);

    handler.deleteSingle(table, delete);

    TGet get = new TGet(wrap(rowName));
    TResult result = handler.get(table, get);
    assertArrayEquals(rowName, result.getRow());
    List<TColumnValue> returnedColumnValues = result.getColumnValues();
    List<TColumnValue> expectedColumnValues = new ArrayList<TColumnValue>();
    expectedColumnValues.add(columnValueB);
    assertTColumnValuesEqual(expectedColumnValues, returnedColumnValues);
  }

  @Test
  public void testDeleteAllTimestamps() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = "testDeleteAllTimestamps".getBytes();
    ByteBuffer table = wrap(tableAname);

    List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
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
    List<TColumn> deleteColumns = new ArrayList<TColumn>();
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
    byte[] rowName = "testDeleteSingleTimestamp".getBytes();
    ByteBuffer table = wrap(tableAname);

    long timestamp1 = System.currentTimeMillis() - 10;
    long timestamp2 = System.currentTimeMillis();

    List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
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
    List<TColumn> deleteColumns = new ArrayList<TColumn>();
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
  public void testIncrement() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = "testIncrement".getBytes();
    ByteBuffer table = wrap(tableAname);

    List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
    columnValues.add(new TColumnValue(wrap(familyAname), wrap(qualifierAname),
      wrap(Bytes.toBytes(1L))));
    TPut put = new TPut(wrap(rowName), columnValues);
    put.setColumnValues(columnValues);
    handler.put(table, put);

    List<TColumnIncrement> incrementColumns = new ArrayList<TColumnIncrement>();
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
    byte[] rowName = "testAppend".getBytes();
    ByteBuffer table = wrap(tableAname);
    byte[] v1 = Bytes.toBytes("42");
    byte[] v2 = Bytes.toBytes("23");
    List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
    columnValues.add(new TColumnValue(wrap(familyAname), wrap(qualifierAname), wrap(v1)));
    TPut put = new TPut(wrap(rowName), columnValues);
    put.setColumnValues(columnValues);
    handler.put(table, put);

    List<TColumnValue> appendColumns = new ArrayList<TColumnValue>();
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
   *
   * @throws Exception
   */
  @Test
  public void testCheckAndPut() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = "testCheckAndPut".getBytes();
    ByteBuffer table = wrap(tableAname);

    List<TColumnValue> columnValuesA = new ArrayList<TColumnValue>();
    TColumnValue columnValueA = new TColumnValue(wrap(familyAname), wrap(qualifierAname),
      wrap(valueAname));
    columnValuesA.add(columnValueA);
    TPut putA = new TPut(wrap(rowName), columnValuesA);
    putA.setColumnValues(columnValuesA);

    List<TColumnValue> columnValuesB = new ArrayList<TColumnValue>();
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
    List<TColumnValue> expectedColumnValues = new ArrayList<TColumnValue>();
    expectedColumnValues.add(columnValueA);
    expectedColumnValues.add(columnValueB);
    assertTColumnValuesEqual(expectedColumnValues, returnedColumnValues);
  }

  /**
   * check that checkAndDelete fails if the cell does not exist, then put in the cell, then
   * check that the checkAndDelete succeeds.
   *
   * @throws Exception
   */
  @Test
  public void testCheckAndDelete() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = "testCheckAndDelete".getBytes();
    ByteBuffer table = wrap(tableAname);

    List<TColumnValue> columnValuesA = new ArrayList<TColumnValue>();
    TColumnValue columnValueA = new TColumnValue(wrap(familyAname), wrap(qualifierAname),
      wrap(valueAname));
    columnValuesA.add(columnValueA);
    TPut putA = new TPut(wrap(rowName), columnValuesA);
    putA.setColumnValues(columnValuesA);

    List<TColumnValue> columnValuesB = new ArrayList<TColumnValue>();
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
    List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
    columnValues.add(columnValue);
    for (int i = 0; i < 10; i++) {
      TPut put = new TPut(wrap(("testScan" + i).getBytes()), columnValues);
      handler.put(table, put);
    }

    // create scan instance
    TScan scan = new TScan();
    List<TColumn> columns = new ArrayList<TColumn>();
    TColumn column = new TColumn();
    column.setFamily(familyAname);
    column.setQualifier(qualifierAname);
    columns.add(column);
    scan.setColumns(columns);
    scan.setStartRow("testScan".getBytes());
    scan.setStopRow("testScan\uffff".getBytes());

    // get scanner and rows
    int scanId = handler.openScanner(table, scan);
    List<TResult> results = handler.getScannerRows(scanId, 10);
    assertEquals(10, results.size());
    for (int i = 0; i < 10; i++) {
      // check if the rows are returned and in order
      assertArrayEquals(("testScan" + i).getBytes(), results.get(i).getRow());
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
  public void testReverseScan() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    ByteBuffer table = wrap(tableAname);

    // insert data
    TColumnValue columnValue = new TColumnValue(wrap(familyAname), wrap(qualifierAname),
      wrap(valueAname));
    List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
    columnValues.add(columnValue);
    for (int i = 0; i < 10; i++) {
      TPut put = new TPut(wrap(("testReverseScan" + i).getBytes()), columnValues);
      handler.put(table, put);
    }

    // create reverse scan instance
    TScan scan = new TScan();
    scan.setReversed(true);
    List<TColumn> columns = new ArrayList<TColumn>();
    TColumn column = new TColumn();
    column.setFamily(familyAname);
    column.setQualifier(qualifierAname);
    columns.add(column);
    scan.setColumns(columns);
    scan.setStartRow("testReverseScan\uffff".getBytes());
    scan.setStopRow("testReverseScan".getBytes());

    // get scanner and rows
    int scanId = handler.openScanner(table, scan);
    List<TResult> results = handler.getScannerRows(scanId, 10);
    assertEquals(10, results.size());
    for (int i = 0; i < 10; i++) {
      // check if the rows are returned and in order
      assertArrayEquals(("testReverseScan" + (9 - i)).getBytes(), results.get(i).getRow());
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
    List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
    columnValues.add(columnValue);
    for (int i = 0; i < 10; i++) {
      TPut put = new TPut(wrap(("testScanWithFilter" + i).getBytes()), columnValues);
      handler.put(table, put);
    }

    // create scan instance with filter
    TScan scan = new TScan();
    List<TColumn> columns = new ArrayList<TColumn>();
    TColumn column = new TColumn();
    column.setFamily(familyAname);
    column.setQualifier(qualifierAname);
    columns.add(column);
    scan.setColumns(columns);
    scan.setStartRow("testScanWithFilter".getBytes());
    scan.setStopRow("testScanWithFilter\uffff".getBytes());
    // only get the key part
    scan.setFilterString(wrap(("KeyOnlyFilter()").getBytes()));

    // get scanner and rows
    int scanId = handler.openScanner(table, scan);
    List<TResult> results = handler.getScannerRows(scanId, 10);
    assertEquals(10, results.size());
    for (int i = 0; i < 10; i++) {
      // check if the rows are returned and in order
      assertArrayEquals(("testScanWithFilter" + i).getBytes(), results.get(i).getRow());
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

  /**
   * Padding numbers to make comparison of sort order easier in a for loop
   *
   * @param n  The number to pad.
   * @param pad  The length to pad up to.
   * @return The padded number as a string.
   */
  private String pad(int n, byte pad) {
    String res = Integer.toString(n);
    while (res.length() < pad) res = "0" + res;
    return res;
  }

  @Test
  public void testScanWithBatchSize() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    ByteBuffer table = wrap(tableAname);

    // insert data
    List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
    for (int i = 0; i < 100; i++) {
      String colNum = pad(i, (byte) 3);
      TColumnValue columnValue = new TColumnValue(wrap(familyAname),
        wrap(("col" + colNum).getBytes()), wrap(("val" + colNum).getBytes()));
      columnValues.add(columnValue);
    }
    TPut put = new TPut(wrap(("testScanWithBatchSize").getBytes()), columnValues);
    handler.put(table, put);

    // create scan instance
    TScan scan = new TScan();
    List<TColumn> columns = new ArrayList<TColumn>();
    TColumn column = new TColumn();
    column.setFamily(familyAname);
    columns.add(column);
    scan.setColumns(columns);
    scan.setStartRow("testScanWithBatchSize".getBytes());
    scan.setStopRow("testScanWithBatchSize\uffff".getBytes());
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
        assertArrayEquals(("col" + colNumPad).getBytes(), cols.get(y).getQualifier());
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
    List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
    columnValues.add(columnValue);
    for (int i = 0; i < 20; i++) {
      TPut put =
          new TPut(wrap(("testGetScannerResults" + pad(i, (byte) 2)).getBytes()), columnValues);
      handler.put(table, put);
    }

    // create scan instance
    TScan scan = new TScan();
    List<TColumn> columns = new ArrayList<TColumn>();
    TColumn column = new TColumn();
    column.setFamily(familyAname);
    column.setQualifier(qualifierAname);
    columns.add(column);
    scan.setColumns(columns);
    scan.setStartRow("testGetScannerResults".getBytes());

    // get 5 rows and check the returned results
    scan.setStopRow("testGetScannerResults05".getBytes());
    List<TResult> results = handler.getScannerResults(table, scan, 5);
    assertEquals(5, results.size());
    for (int i = 0; i < 5; i++) {
      // check if the rows are returned and in order
      assertArrayEquals(("testGetScannerResults" + pad(i, (byte) 2)).getBytes(), results.get(i)
          .getRow());
    }

    // get 10 rows and check the returned results
    scan.setStopRow("testGetScannerResults10".getBytes());
    results = handler.getScannerResults(table, scan, 10);
    assertEquals(10, results.size());
    for (int i = 0; i < 10; i++) {
      // check if the rows are returned and in order
      assertArrayEquals(("testGetScannerResults" + pad(i, (byte) 2)).getBytes(), results.get(i)
          .getRow());
    }

    // get 20 rows and check the returned results
    scan.setStopRow("testGetScannerResults20".getBytes());
    results = handler.getScannerResults(table, scan, 20);
    assertEquals(20, results.size());
    for (int i = 0; i < 20; i++) {
      // check if the rows are returned and in order
      assertArrayEquals(("testGetScannerResults" + pad(i, (byte) 2)).getBytes(), results.get(i)
          .getRow());
    }

    // reverse scan
    scan = new TScan();
    scan.setColumns(columns);
    scan.setReversed(true);
    scan.setStartRow("testGetScannerResults20".getBytes());
    scan.setStopRow("testGetScannerResults".getBytes());
    results = handler.getScannerResults(table, scan, 20);
    assertEquals(20, results.size());
    for (int i = 0; i < 20; i++) {
      // check if the rows are returned and in order
      assertArrayEquals(("testGetScannerResults" + pad(19 - i, (byte) 2)).getBytes(), results.get(i)
          .getRow());
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
        ThriftHBaseServiceHandler.newInstance(hbaseHandler, metrics);
    byte[] rowName = "testMetrics".getBytes();
    ByteBuffer table = wrap(tableAname);

    TGet get = new TGet(wrap(rowName));
    assertFalse(handler.exists(table, get));

    List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
    columnValues.add(new TColumnValue(wrap(familyAname), wrap(qualifierAname), wrap(valueAname)));
    columnValues.add(new TColumnValue(wrap(familyBname), wrap(qualifierBname),  wrap(valueBname)));
    TPut put = new TPut(wrap(rowName), columnValues);
    put.setColumnValues(columnValues);

    handler.put(table, put);

    assertTrue(handler.exists(table, get));
    metricsHelper.assertCounter("put_num_ops", 1, metrics.getSource());
    metricsHelper.assertCounter( "exists_num_ops", 2, metrics.getSource());
  }

  private static ThriftMetrics getMetrics(Configuration conf) throws Exception {
    ThriftMetrics m = new ThriftMetrics(conf, ThriftMetrics.ThriftServerType.TWO);
    m.getSource().init(); //Clear all the metrics
    return m;
  }

  @Test
  public void testAttribute() throws Exception {
    byte[] rowName = "testAttribute".getBytes();
    byte[] attributeKey = "attribute1".getBytes();
    byte[] attributeValue = "value1".getBytes();
    Map<ByteBuffer, ByteBuffer> attributes = new HashMap<ByteBuffer, ByteBuffer>();
    attributes.put(wrap(attributeKey), wrap(attributeValue));

    TGet tGet = new TGet(wrap(rowName));
    tGet.setAttributes(attributes);
    Get get = getFromThrift(tGet);
    assertArrayEquals(get.getAttribute("attribute1"), attributeValue);

    List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
    columnValues.add(new TColumnValue(wrap(familyAname), wrap(qualifierAname), wrap(valueAname)));
    TPut tPut = new TPut(wrap(rowName) , columnValues);
    tPut.setAttributes(attributes);
    Put put = putFromThrift(tPut);
    assertArrayEquals(put.getAttribute("attribute1"), attributeValue);

    TScan tScan = new TScan();
    tScan.setAttributes(attributes);
    Scan scan = scanFromThrift(tScan);
    assertArrayEquals(scan.getAttribute("attribute1"), attributeValue);

    List<TColumnIncrement> incrementColumns = new ArrayList<TColumnIncrement>();
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
   *
   * @throws Exception
   */
  @Test
  public void testMutateRow() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = "testMutateRow".getBytes();
    ByteBuffer table = wrap(tableAname);

    List<TColumnValue> columnValuesA = new ArrayList<TColumnValue>();
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

    List<TColumnValue> expectedColumnValues = new ArrayList<TColumnValue>();
    expectedColumnValues.add(columnValueA);
    assertTColumnValuesEqual(expectedColumnValues, returnedColumnValues);

    List<TColumnValue> columnValuesB = new ArrayList<TColumnValue>();
    TColumnValue columnValueB = new TColumnValue(wrap(familyAname), wrap(qualifierBname),
        wrap(valueBname));
    columnValuesB.add(columnValueB);
    TPut putB = new TPut(wrap(rowName), columnValuesB);
    putB.setColumnValues(columnValuesB);

    TDelete delete = new TDelete(wrap(rowName));
    List<TColumn> deleteColumns = new ArrayList<TColumn>();
    TColumn deleteColumn = new TColumn(wrap(familyAname));
    deleteColumn.setQualifier(qualifierAname);
    deleteColumns.add(deleteColumn);
    delete.setColumns(deleteColumns);

    List<TMutation> mutations = new ArrayList<TMutation>();
    TMutation mutationA = TMutation.put(putB);
    mutations.add(mutationA);

    TMutation mutationB = TMutation.deleteSingle(delete);
    mutations.add(mutationB);

    TRowMutations tRowMutations = new TRowMutations(wrap(rowName),mutations);
    handler.mutateRow(table,tRowMutations);

    result = handler.get(table, get);
    assertArrayEquals(rowName, result.getRow());
    returnedColumnValues = result.getColumnValues();

    expectedColumnValues = new ArrayList<TColumnValue>();
    expectedColumnValues.add(columnValueB);
    assertTColumnValuesEqual(expectedColumnValues, returnedColumnValues);
  }

  /**
   * Create TPut, TDelete , TIncrement objects, set durability then call ThriftUtility
   * functions to get Put , Delete and Increment respectively. Use getDurability to make sure
   * the returned objects have the appropriate durability setting.
   *
   * @throws Exception
   */
  @Test
  public void testDurability() throws Exception {
    byte[] rowName = "testDurability".getBytes();
    List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
    columnValues.add(new TColumnValue(wrap(familyAname), wrap(qualifierAname), wrap(valueAname)));

    List<TColumnIncrement> incrementColumns = new ArrayList<TColumnIncrement>();
    incrementColumns.add(new TColumnIncrement(wrap(familyAname), wrap(qualifierAname)));

    TDelete tDelete = new TDelete(wrap(rowName));
    tDelete.setDurability(TDurability.SKIP_WAL);
    Delete delete = deleteFromThrift(tDelete);
    assertEquals(delete.getDurability(), Durability.SKIP_WAL);

    tDelete.setDurability(TDurability.ASYNC_WAL);
    delete = deleteFromThrift(tDelete);
    assertEquals(delete.getDurability(), Durability.ASYNC_WAL);

    tDelete.setDurability(TDurability.SYNC_WAL);
    delete = deleteFromThrift(tDelete);
    assertEquals(delete.getDurability(), Durability.SYNC_WAL);

    tDelete.setDurability(TDurability.FSYNC_WAL);
    delete = deleteFromThrift(tDelete);
    assertEquals(delete.getDurability(), Durability.FSYNC_WAL);

    TPut tPut = new TPut(wrap(rowName), columnValues);
    tPut.setDurability(TDurability.SKIP_WAL);
    Put put = putFromThrift(tPut);
    assertEquals(put.getDurability(), Durability.SKIP_WAL);

    tPut.setDurability(TDurability.ASYNC_WAL);
    put = putFromThrift(tPut);
    assertEquals(put.getDurability(), Durability.ASYNC_WAL);

    tPut.setDurability(TDurability.SYNC_WAL);
    put = putFromThrift(tPut);
    assertEquals(put.getDurability(), Durability.SYNC_WAL);

    tPut.setDurability(TDurability.FSYNC_WAL);
    put = putFromThrift(tPut);
    assertEquals(put.getDurability(), Durability.FSYNC_WAL);

    TIncrement tIncrement = new TIncrement(wrap(rowName), incrementColumns);

    tIncrement.setDurability(TDurability.SKIP_WAL);
    Increment increment = incrementFromThrift(tIncrement);
    assertEquals(increment.getDurability(), Durability.SKIP_WAL);

    tIncrement.setDurability(TDurability.ASYNC_WAL);
    increment = incrementFromThrift(tIncrement);
    assertEquals(increment.getDurability(), Durability.ASYNC_WAL);

    tIncrement.setDurability(TDurability.SYNC_WAL);
    increment = incrementFromThrift(tIncrement);
    assertEquals(increment.getDurability(), Durability.SYNC_WAL);

    tIncrement.setDurability(TDurability.FSYNC_WAL);
    increment = incrementFromThrift(tIncrement);
    assertEquals(increment.getDurability(), Durability.FSYNC_WAL);
  }
}

