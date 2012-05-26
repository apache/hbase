/*
 * Copyright 2009 The Apache Software Foundation
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.thrift.ThriftMetrics;
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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.spi.NoEmitMetricsContext;
import org.apache.hadoop.metrics.spi.OutputRecord;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Unit testing for ThriftServer.HBaseHandler, a part of the org.apache.hadoop.hbase.thrift2 package.
 */
@Category(MediumTests.class)
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
      new HColumnDescriptor(familyAname),
      new HColumnDescriptor(familyBname)
          .setMaxVersions(2)
  };

  public void assertTColumnValuesEqual(List<TColumnValue> columnValuesA, List<TColumnValue> columnValuesB) {
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
    HBaseAdmin admin = new HBaseAdmin(UTIL.getConfiguration());
    HTableDescriptor tableDescriptor = new HTableDescriptor(tableAname);
    for (HColumnDescriptor family : families) {
      tableDescriptor.addFamily(family);
    }
    admin.createTable(tableDescriptor);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void setup() throws Exception {

  }

  private ThriftHBaseServiceHandler createHandler() {
    return new ThriftHBaseServiceHandler(UTIL.getConfiguration());
  }

  @Test
  public void testExists() throws TIOError, TException {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = "testExists".getBytes();
    ByteBuffer table = ByteBuffer.wrap(tableAname);

    TGet get = new TGet(ByteBuffer.wrap(rowName));
    assertFalse(handler.exists(table, get));

    List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
    columnValues.add(new TColumnValue(ByteBuffer.wrap(familyAname), ByteBuffer.wrap(qualifierAname), ByteBuffer
        .wrap(valueAname)));
    columnValues.add(new TColumnValue(ByteBuffer.wrap(familyBname), ByteBuffer.wrap(qualifierBname), ByteBuffer
        .wrap(valueBname)));
    TPut put = new TPut(ByteBuffer.wrap(rowName), columnValues);
    put.setColumnValues(columnValues);

    handler.put(table, put);

    assertTrue(handler.exists(table, get));
  }

  @Test
  public void testPutGet() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = "testPutGet".getBytes();
    ByteBuffer table = ByteBuffer.wrap(tableAname);

    List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
    columnValues.add(new TColumnValue(ByteBuffer.wrap(familyAname), ByteBuffer.wrap(qualifierAname), ByteBuffer
        .wrap(valueAname)));
    columnValues.add(new TColumnValue(ByteBuffer.wrap(familyBname), ByteBuffer.wrap(qualifierBname), ByteBuffer
        .wrap(valueBname)));
    TPut put = new TPut(ByteBuffer.wrap(rowName), columnValues);

    put.setColumnValues(columnValues);

    handler.put(table, put);

    TGet get = new TGet(ByteBuffer.wrap(rowName));

    TResult result = handler.get(table, get);
    assertArrayEquals(rowName, result.getRow());
    List<TColumnValue> returnedColumnValues = result.getColumnValues();
    assertTColumnValuesEqual(columnValues, returnedColumnValues);
  }

  @Test
  public void testPutGetMultiple() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    ByteBuffer table = ByteBuffer.wrap(tableAname);
    byte[] rowName1 = "testPutGetMultiple1".getBytes();
    byte[] rowName2 = "testPutGetMultiple2".getBytes();

    List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
    columnValues.add(new TColumnValue(ByteBuffer.wrap(familyAname), ByteBuffer.wrap(qualifierAname), ByteBuffer
        .wrap(valueAname)));
    columnValues.add(new TColumnValue(ByteBuffer.wrap(familyBname), ByteBuffer.wrap(qualifierBname), ByteBuffer
        .wrap(valueBname)));
    List<TPut> puts = new ArrayList<TPut>();
    puts.add(new TPut(ByteBuffer.wrap(rowName1), columnValues));
    puts.add(new TPut(ByteBuffer.wrap(rowName2), columnValues));

    handler.putMultiple(table, puts);

    List<TGet> gets = new ArrayList<TGet>();
    gets.add(new TGet(ByteBuffer.wrap(rowName1)));
    gets.add(new TGet(ByteBuffer.wrap(rowName2)));

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
    ByteBuffer table = ByteBuffer.wrap(tableAname);
    byte[] rowName1 = "testDeleteMultiple1".getBytes();
    byte[] rowName2 = "testDeleteMultiple2".getBytes();

    List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
    columnValues.add(new TColumnValue(ByteBuffer.wrap(familyAname), ByteBuffer.wrap(qualifierAname), ByteBuffer
        .wrap(valueAname)));
    columnValues.add(new TColumnValue(ByteBuffer.wrap(familyBname), ByteBuffer.wrap(qualifierBname), ByteBuffer
        .wrap(valueBname)));
    List<TPut> puts = new ArrayList<TPut>();
    puts.add(new TPut(ByteBuffer.wrap(rowName1), columnValues));
    puts.add(new TPut(ByteBuffer.wrap(rowName2), columnValues));

    handler.putMultiple(table, puts);

    List<TDelete> deletes = new ArrayList<TDelete>();
    deletes.add(new TDelete(ByteBuffer.wrap(rowName1)));
    deletes.add(new TDelete(ByteBuffer.wrap(rowName2)));

    List<TDelete> deleteResults = handler.deleteMultiple(table, deletes);
    // 0 means they were all successfully applies
    assertEquals(0, deleteResults.size());

    assertFalse(handler.exists(table, new TGet(ByteBuffer.wrap(rowName1))));
    assertFalse(handler.exists(table, new TGet(ByteBuffer.wrap(rowName2))));
  }

  @Test
  public void testDelete() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = "testDelete".getBytes();
    ByteBuffer table = ByteBuffer.wrap(tableAname);

    List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
    TColumnValue columnValueA = new TColumnValue(ByteBuffer.wrap(familyAname), ByteBuffer.wrap(qualifierAname),
        ByteBuffer.wrap(valueAname));
    TColumnValue columnValueB = new TColumnValue(ByteBuffer.wrap(familyBname), ByteBuffer.wrap(qualifierBname),
        ByteBuffer.wrap(valueBname));
    columnValues.add(columnValueA);
    columnValues.add(columnValueB);
    TPut put = new TPut(ByteBuffer.wrap(rowName), columnValues);

    put.setColumnValues(columnValues);

    handler.put(table, put);

    TDelete delete = new TDelete(ByteBuffer.wrap(rowName));
    List<TColumn> deleteColumns = new ArrayList<TColumn>();
    TColumn deleteColumn = new TColumn(ByteBuffer.wrap(familyAname));
    deleteColumn.setQualifier(qualifierAname);
    deleteColumns.add(deleteColumn);
    delete.setColumns(deleteColumns);

    handler.deleteSingle(table, delete);

    TGet get = new TGet(ByteBuffer.wrap(rowName));
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
    ByteBuffer table = ByteBuffer.wrap(tableAname);

    List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
    TColumnValue columnValueA = new TColumnValue(ByteBuffer.wrap(familyAname), ByteBuffer.wrap(qualifierAname),
        ByteBuffer.wrap(valueAname));
    columnValueA.setTimestamp(System.currentTimeMillis() - 10);
    columnValues.add(columnValueA);
    TPut put = new TPut(ByteBuffer.wrap(rowName), columnValues);

    put.setColumnValues(columnValues);

    handler.put(table, put);
    columnValueA.setTimestamp(System.currentTimeMillis());
    handler.put(table, put);

    TGet get = new TGet(ByteBuffer.wrap(rowName));
    get.setMaxVersions(2);
    TResult result = handler.get(table, get);
    assertEquals(2, result.getColumnValuesSize());

    TDelete delete = new TDelete(ByteBuffer.wrap(rowName));
    List<TColumn> deleteColumns = new ArrayList<TColumn>();
    TColumn deleteColumn = new TColumn(ByteBuffer.wrap(familyAname));
    deleteColumn.setQualifier(qualifierAname);
    deleteColumns.add(deleteColumn);
    delete.setColumns(deleteColumns);
    delete.setDeleteType(TDeleteType.DELETE_COLUMNS); // This is the default anyway.

    handler.deleteSingle(table, delete);

    get = new TGet(ByteBuffer.wrap(rowName));
    result = handler.get(table, get);
    assertNull(result.getRow());
    assertEquals(0, result.getColumnValuesSize());
  }

  @Test
  public void testDeleteSingleTimestamp() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = "testDeleteSingleTimestamp".getBytes();
    ByteBuffer table = ByteBuffer.wrap(tableAname);

    long timestamp1 = System.currentTimeMillis() - 10;
    long timestamp2 = System.currentTimeMillis();
    
    List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
    TColumnValue columnValueA = new TColumnValue(ByteBuffer.wrap(familyAname), ByteBuffer.wrap(qualifierAname),
        ByteBuffer.wrap(valueAname));
    columnValueA.setTimestamp(timestamp1);
    columnValues.add(columnValueA);
    TPut put = new TPut(ByteBuffer.wrap(rowName), columnValues);

    put.setColumnValues(columnValues);

    handler.put(table, put);
    columnValueA.setTimestamp(timestamp2);
    handler.put(table, put);

    TGet get = new TGet(ByteBuffer.wrap(rowName));
    get.setMaxVersions(2);
    TResult result = handler.get(table, get);
    assertEquals(2, result.getColumnValuesSize());

    TDelete delete = new TDelete(ByteBuffer.wrap(rowName));
    List<TColumn> deleteColumns = new ArrayList<TColumn>();
    TColumn deleteColumn = new TColumn(ByteBuffer.wrap(familyAname));
    deleteColumn.setQualifier(qualifierAname);
    deleteColumns.add(deleteColumn);
    delete.setColumns(deleteColumns);
    delete.setDeleteType(TDeleteType.DELETE_COLUMN);

    handler.deleteSingle(table, delete);

    get = new TGet(ByteBuffer.wrap(rowName));
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
    ByteBuffer table = ByteBuffer.wrap(tableAname);

    List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
    columnValues.add(new TColumnValue(ByteBuffer.wrap(familyAname), ByteBuffer.wrap(qualifierAname), ByteBuffer
        .wrap(Bytes.toBytes(1L))));
    TPut put = new TPut(ByteBuffer.wrap(rowName), columnValues);
    put.setColumnValues(columnValues);
    handler.put(table, put);

    List<TColumnIncrement> incrementColumns = new ArrayList<TColumnIncrement>();
    incrementColumns.add(new TColumnIncrement(ByteBuffer.wrap(familyAname), ByteBuffer.wrap(qualifierAname)));
    TIncrement increment = new TIncrement(ByteBuffer.wrap(rowName), incrementColumns);
    handler.increment(table, increment);

    TGet get = new TGet(ByteBuffer.wrap(rowName));
    TResult result = handler.get(table, get);

    assertArrayEquals(rowName, result.getRow());
    assertEquals(1, result.getColumnValuesSize());
    TColumnValue columnValue = result.getColumnValues().get(0);
    assertArrayEquals(Bytes.toBytes(2L), columnValue.getValue());
  }

  /**
   * check that checkAndPut fails if the cell does not exist, then put in the cell, then check that the checkAndPut
   * succeeds.
   * 
   * @throws Exception
   */
  @Test
  public void testCheckAndPut() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = "testCheckAndPut".getBytes();
    ByteBuffer table = ByteBuffer.wrap(tableAname);

    List<TColumnValue> columnValuesA = new ArrayList<TColumnValue>();
    TColumnValue columnValueA = new TColumnValue(ByteBuffer.wrap(familyAname), ByteBuffer.wrap(qualifierAname),
        ByteBuffer.wrap(valueAname));
    columnValuesA.add(columnValueA);
    TPut putA = new TPut(ByteBuffer.wrap(rowName), columnValuesA);
    putA.setColumnValues(columnValuesA);

    List<TColumnValue> columnValuesB = new ArrayList<TColumnValue>();
    TColumnValue columnValueB = new TColumnValue(ByteBuffer.wrap(familyBname), ByteBuffer.wrap(qualifierBname),
        ByteBuffer.wrap(valueBname));
    columnValuesB.add(columnValueB);
    TPut putB = new TPut(ByteBuffer.wrap(rowName), columnValuesB);
    putB.setColumnValues(columnValuesB);

    assertFalse(handler.checkAndPut(table, ByteBuffer.wrap(rowName), ByteBuffer.wrap(familyAname),
        ByteBuffer.wrap(qualifierAname), ByteBuffer.wrap(valueAname), putB));

    TGet get = new TGet(ByteBuffer.wrap(rowName));
    TResult result = handler.get(table, get);
    assertEquals(0, result.getColumnValuesSize());

    handler.put(table, putA);

    assertTrue(handler.checkAndPut(table, ByteBuffer.wrap(rowName), ByteBuffer.wrap(familyAname),
        ByteBuffer.wrap(qualifierAname), ByteBuffer.wrap(valueAname), putB));

    result = handler.get(table, get);
    assertArrayEquals(rowName, result.getRow());
    List<TColumnValue> returnedColumnValues = result.getColumnValues();
    List<TColumnValue> expectedColumnValues = new ArrayList<TColumnValue>();
    expectedColumnValues.add(columnValueA);
    expectedColumnValues.add(columnValueB);
    assertTColumnValuesEqual(expectedColumnValues, returnedColumnValues);
  }

  /**
   * check that checkAndDelete fails if the cell does not exist, then put in the cell, then check that the
   * checkAndDelete succeeds.
   * 
   * @throws Exception
   */
  @Test
  public void testCheckAndDelete() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    byte[] rowName = "testCheckAndDelete".getBytes();
    ByteBuffer table = ByteBuffer.wrap(tableAname);

    List<TColumnValue> columnValuesA = new ArrayList<TColumnValue>();
    TColumnValue columnValueA = new TColumnValue(ByteBuffer.wrap(familyAname), ByteBuffer.wrap(qualifierAname),
        ByteBuffer.wrap(valueAname));
    columnValuesA.add(columnValueA);
    TPut putA = new TPut(ByteBuffer.wrap(rowName), columnValuesA);
    putA.setColumnValues(columnValuesA);

    List<TColumnValue> columnValuesB = new ArrayList<TColumnValue>();
    TColumnValue columnValueB = new TColumnValue(ByteBuffer.wrap(familyBname), ByteBuffer.wrap(qualifierBname),
        ByteBuffer.wrap(valueBname));
    columnValuesB.add(columnValueB);
    TPut putB = new TPut(ByteBuffer.wrap(rowName), columnValuesB);
    putB.setColumnValues(columnValuesB);

    // put putB so that we know whether the row has been deleted or not
    handler.put(table, putB);

    TDelete delete = new TDelete(ByteBuffer.wrap(rowName));

    assertFalse(handler.checkAndDelete(table, ByteBuffer.wrap(rowName), ByteBuffer.wrap(familyAname),
        ByteBuffer.wrap(qualifierAname), ByteBuffer.wrap(valueAname), delete));

    TGet get = new TGet(ByteBuffer.wrap(rowName));
    TResult result = handler.get(table, get);
    assertArrayEquals(rowName, result.getRow());
    assertTColumnValuesEqual(columnValuesB, result.getColumnValues());

    handler.put(table, putA);

    assertTrue(handler.checkAndDelete(table, ByteBuffer.wrap(rowName), ByteBuffer.wrap(familyAname),
        ByteBuffer.wrap(qualifierAname), ByteBuffer.wrap(valueAname), delete));

    result = handler.get(table, get);
    assertFalse(result.isSetRow());
    assertEquals(0, result.getColumnValuesSize());
  }

  @Test
  public void testScan() throws Exception {
    ThriftHBaseServiceHandler handler = createHandler();
    ByteBuffer table = ByteBuffer.wrap(tableAname);

    TScan scan = new TScan();
    List<TColumn> columns = new ArrayList<TColumn>();
    TColumn column = new TColumn();
    column.setFamily(familyAname);
    column.setQualifier(qualifierAname);
    columns.add(column);
    scan.setColumns(columns);
    scan.setStartRow("testScan".getBytes());

    TColumnValue columnValue = new TColumnValue(ByteBuffer.wrap(familyAname), ByteBuffer.wrap(qualifierAname),
        ByteBuffer.wrap(valueAname));
    List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
    columnValues.add(columnValue);
    for (int i = 0; i < 10; i++) {
      TPut put = new TPut(ByteBuffer.wrap(("testScan" + i).getBytes()), columnValues);
      handler.put(table, put);
    }

    int scanId = handler.openScanner(table, scan);
    List<TResult> results = handler.getScannerRows(scanId, 10);
    assertEquals(10, results.size());
    for (int i = 0; i < 10; i++) {
      assertArrayEquals(("testScan" + i).getBytes(), results.get(i).getRow());
    }

    results = handler.getScannerRows(scanId, 10);
    assertEquals(0, results.size());

    handler.closeScanner(scanId);

    try {
      handler.getScannerRows(scanId, 10);
      fail("Scanner id should be invalid");
    } catch (TIllegalArgument e) {
    }
  }

  @Test
  public void testMetrics() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    ThriftMetrics metrics = getMetrics(conf);
    THBaseService.Iface handler =
        ThriftHBaseServiceHandler.newInstance(conf, metrics);
    byte[] rowName = "testMetrics".getBytes();
    ByteBuffer table = ByteBuffer.wrap(tableAname);

    TGet get = new TGet(ByteBuffer.wrap(rowName));
    assertFalse(handler.exists(table, get));

    List<TColumnValue> columnValues = new ArrayList<TColumnValue>();
    columnValues.add(new TColumnValue(ByteBuffer.wrap(familyAname),
                                      ByteBuffer.wrap(qualifierAname),
                                      ByteBuffer.wrap(valueAname)));
    columnValues.add(new TColumnValue(ByteBuffer.wrap(familyBname),
                                      ByteBuffer.wrap(qualifierBname),
                                      ByteBuffer.wrap(valueBname)));
    TPut put = new TPut(ByteBuffer.wrap(rowName), columnValues);
    put.setColumnValues(columnValues);

    handler.put(table, put);

    assertTrue(handler.exists(table, get));
    logMetrics(metrics);
    verifyMetrics(metrics, "put_num_ops", 1);
    verifyMetrics(metrics, "exists_num_ops", 2);
  }
 
  private static ThriftMetrics getMetrics(Configuration conf) throws Exception {
    setupMetricsContext();
    return new ThriftMetrics(Integer.parseInt(ThriftServer.DEFAULT_LISTEN_PORT),
        conf, THBaseService.Iface.class);
  }
 
  private static void setupMetricsContext() throws IOException {
    ContextFactory factory = ContextFactory.getFactory();
    factory.setAttribute(ThriftMetrics.CONTEXT_NAME + ".class",
        NoEmitMetricsContext.class.getName());
    MetricsUtil.getContext(ThriftMetrics.CONTEXT_NAME)
               .createRecord(ThriftMetrics.CONTEXT_NAME).remove();
  }
 
  private static void logMetrics(ThriftMetrics metrics) throws Exception {
    if (LOG.isDebugEnabled()) {
      return;
    }
    MetricsContext context = MetricsUtil.getContext( 
        ThriftMetrics.CONTEXT_NAME); 
    metrics.doUpdates(context); 
    for (String key : context.getAllRecords().keySet()) {
      for (OutputRecord record : context.getAllRecords().get(key)) {
        for (String name : record.getMetricNames()) {
          LOG.debug("metrics:" + name + " value:" +
              record.getMetric(name).intValue());
        }
      }
    }
  }

  private static void verifyMetrics(ThriftMetrics metrics, String name, int expectValue)
      throws Exception { 
    MetricsContext context = MetricsUtil.getContext( 
        ThriftMetrics.CONTEXT_NAME); 
    metrics.doUpdates(context); 
    OutputRecord record = context.getAllRecords().get( 
        ThriftMetrics.CONTEXT_NAME).iterator().next(); 
    assertEquals(expectValue, record.getMetric(name).intValue()); 
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

